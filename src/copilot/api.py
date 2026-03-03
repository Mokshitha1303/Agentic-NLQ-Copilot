from __future__ import annotations

import os
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, ConfigDict, Field

from .agent import EnterpriseNLQCopilot
from .benchmark import run_spider_benchmark
from .config import CopilotConfig
from .reliability import run_safety_suite


class ServiceInfo(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    version: str
    llm_enabled: bool
    default_model: str
    dataset_root: str
    openapi_url: str
    docs_url: str
    redoc_url: str


class HealthResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")
    status: Literal["ok"]
    uptime_s: float
    loaded_databases: int
    llm_enabled: bool


class DatabaseListResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")
    total: int
    databases: list[str]


class SchemaColumnOut(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    original_name: str
    col_type: str
    is_primary: bool


class SchemaTableOut(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    original_name: str
    columns: list[SchemaColumnOut]


class ForeignKeyOut(BaseModel):
    model_config = ConfigDict(extra="forbid")
    source_table: str
    source_column: str
    target_table: str
    target_column: str


class DatabaseSchemaResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")
    db_id: str
    table_count: int
    foreign_key_count: int
    tables: list[SchemaTableOut]
    foreign_keys: list[ForeignKeyOut]


class QueryRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    db_id: str = Field(..., description="Spider database id, e.g. world_1")
    question: str = Field(..., min_length=2, description="Natural-language question")
    allowed_tables: list[str] | None = Field(
        default=None,
        description="Optional allow-list of tables for guardrails.",
    )
    max_rows: int | None = Field(default=None, ge=1, le=5000)
    timeout_ms: int | None = Field(default=None, ge=50, le=120000)
    skip_explanation: bool = Field(
        default=False,
        description="Skip LLM explanation and return concise deterministic summary.",
    )
    forced_sql: str | None = Field(
        default=None,
        description="Testing/benchmarking override to bypass SQL generation.",
    )


class QueryExecutionOut(BaseModel):
    model_config = ConfigDict(extra="forbid")
    columns: list[str]
    rows: list[list[Any]]
    returned_row_count: int
    truncated: bool
    latency_ms: float
    error: str | None


class QueryAuditEventOut(BaseModel):
    model_config = ConfigDict(extra="forbid")
    step: str
    status: str
    started_at: float
    finished_at: float
    duration_ms: float
    details: dict[str, Any]


class QueryResultOut(BaseModel):
    model_config = ConfigDict(extra="forbid")
    question: str
    db_id: str
    sql: str | None
    blocked: bool
    explanation: str
    errors: list[str]
    execution: QueryExecutionOut | None
    audit: list[QueryAuditEventOut]
    latency_ms: float


class QueryResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")
    request_id: str
    status: Literal["ok", "blocked", "error"]
    result: QueryResultOut


class SafetyRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    allowed_tables: list[str] | None = None
    max_rows: int = Field(default=200, ge=1, le=5000)


class SafetyResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")
    total: int
    passed: int
    pass_rate: float
    records: list[dict[str, Any]]


class BenchmarkRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    split: Literal["train", "dev", "test"] = "dev"
    mode: Literal["agent", "oracle"] = "agent"
    limit: int | None = Field(default=None, ge=1)
    max_rows: int = Field(default=200, ge=1, le=5000)
    timeout_ms: int = Field(default=2500, ge=50, le=120000)
    output_dir: str = "outputs"
    run_safety_checks: bool = True


class BenchmarkResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")
    summary: dict[str, Any]


def _parse_bool_env(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return default


def _resolve_llm_mode(use_llm: bool | None) -> bool:
    if use_llm is not None:
        return use_llm
    return _parse_bool_env(os.getenv("COPILOT_USE_LLM"), default=True)


def _parse_cors_origins(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [origin.strip() for origin in raw.split(",") if origin.strip()]


def _build_copilot(config: CopilotConfig, use_llm: bool) -> EnterpriseNLQCopilot:
    try:
        return EnterpriseNLQCopilot.from_config(config=config, use_llm=use_llm)
    except Exception as exc:
        if not use_llm:
            raise
        fallback_error = exc
        try:
            # If LLM mode cannot initialize, keep API alive in offline mode.
            return EnterpriseNLQCopilot.from_config(config=config, use_llm=False)
        except Exception:
            raise RuntimeError(
                f"Failed to initialize copilot with LLM ({fallback_error}) and without LLM."
            ) from fallback_error


def create_app(
    *,
    config: CopilotConfig | None = None,
    use_llm: bool | None = None,
) -> FastAPI:
    cfg = config or CopilotConfig.from_env()
    llm_mode = _resolve_llm_mode(use_llm)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.started_at = time.perf_counter()
        app.state.config = cfg
        app.state.copilot = _build_copilot(cfg, use_llm=llm_mode)
        app.state.llm_enabled = app.state.copilot.llm.__class__.__name__ != "RuleBasedLLMClient"
        yield

    app = FastAPI(
        title="Enterprise NLQ Copilot API",
        version="0.1.0",
        description=(
            "Agentic text-to-SQL API over Spider with schema retrieval, guardrails, "
            "safe execution, grounded explanations, and benchmark utilities."
        ),
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
        lifespan=lifespan,
    )
    cors_origins = _parse_cors_origins(os.getenv("API_CORS_ORIGINS"))
    if cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=cors_origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    @app.get("/", response_model=ServiceInfo, tags=["platform"])
    def root(request: Request) -> ServiceInfo:
        copilot: EnterpriseNLQCopilot = request.app.state.copilot
        return ServiceInfo(
            name="enterprise-nlq-copilot",
            version=app.version,
            llm_enabled=request.app.state.llm_enabled,
            default_model=copilot.config.model,
            dataset_root=str(copilot.config.dataset_root),
            openapi_url="/openapi.json",
            docs_url="/docs",
            redoc_url="/redoc",
        )

    @app.get("/health", response_model=HealthResponse, tags=["platform"])
    def health(request: Request) -> HealthResponse:
        copilot: EnterpriseNLQCopilot = request.app.state.copilot
        uptime_s = time.perf_counter() - request.app.state.started_at
        return HealthResponse(
            status="ok",
            uptime_s=uptime_s,
            loaded_databases=len(copilot.catalog.schemas),
            llm_enabled=request.app.state.llm_enabled,
        )

    @app.get("/v1/databases", response_model=DatabaseListResponse, tags=["schema"])
    def list_databases(request: Request) -> DatabaseListResponse:
        copilot: EnterpriseNLQCopilot = request.app.state.copilot
        dbs = sorted(copilot.catalog.schemas)
        return DatabaseListResponse(total=len(dbs), databases=dbs)

    @app.get("/v1/databases/{db_id}/schema", response_model=DatabaseSchemaResponse, tags=["schema"])
    def get_schema(db_id: str, request: Request) -> DatabaseSchemaResponse:
        copilot: EnterpriseNLQCopilot = request.app.state.copilot
        try:
            schema = copilot.catalog.get_schema(db_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

        tables = [
            SchemaTableOut(
                name=table.name,
                original_name=table.original_name,
                columns=[
                    SchemaColumnOut(
                        name=column.name,
                        original_name=column.original_name,
                        col_type=column.col_type,
                        is_primary=column.is_primary,
                    )
                    for column in table.columns
                ],
            )
            for table in schema.tables
        ]
        foreign_keys = [
            ForeignKeyOut(
                source_table=edge.source_table,
                source_column=edge.source_column,
                target_table=edge.target_table,
                target_column=edge.target_column,
            )
            for edge in schema.foreign_keys
        ]
        return DatabaseSchemaResponse(
            db_id=schema.db_id,
            table_count=len(schema.tables),
            foreign_key_count=len(schema.foreign_keys),
            tables=tables,
            foreign_keys=foreign_keys,
        )

    @app.post("/v1/query", response_model=QueryResponse, tags=["query"])
    def query(payload: QueryRequest, request: Request) -> QueryResponse:
        copilot: EnterpriseNLQCopilot = request.app.state.copilot
        request_id = uuid4().hex
        try:
            result = copilot.ask(
                question=payload.question,
                db_id=payload.db_id,
                allowed_tables=payload.allowed_tables,
                max_rows=payload.max_rows,
                timeout_ms=payload.timeout_ms,
                forced_sql=payload.forced_sql,
                skip_explanation=payload.skip_explanation,
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Query processing failed: {exc}") from exc

        result_dict = result.to_dict()
        status: Literal["ok", "blocked", "error"]
        if result.blocked:
            status = "blocked"
        elif result.execution is not None and result.execution.error:
            status = "error"
        else:
            status = "ok"

        return QueryResponse(
            request_id=request_id,
            status=status,
            result=QueryResultOut.model_validate(result_dict),
        )

    @app.post("/v1/safety", response_model=SafetyResponse, tags=["reliability"])
    def safety(payload: SafetyRequest, request: Request) -> SafetyResponse:
        copilot: EnterpriseNLQCopilot = request.app.state.copilot
        report = run_safety_suite(
            copilot.guardrails,
            allowed_tables=payload.allowed_tables,
            max_rows=payload.max_rows,
        )
        return SafetyResponse.model_validate(report)

    @app.post("/v1/benchmark", response_model=BenchmarkResponse, tags=["reliability"])
    def benchmark(payload: BenchmarkRequest, request: Request) -> BenchmarkResponse:
        copilot: EnterpriseNLQCopilot = request.app.state.copilot
        try:
            summary = run_spider_benchmark(
                copilot,
                split=payload.split,
                mode=payload.mode,
                limit=payload.limit,
                max_rows=payload.max_rows,
                timeout_ms=payload.timeout_ms,
                output_dir=Path(payload.output_dir),
                run_safety_checks=payload.run_safety_checks,
            )
        except (FileNotFoundError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Benchmark failed: {exc}") from exc
        return BenchmarkResponse(summary=summary)

    return app


def run() -> None:
    host = os.getenv("API_HOST", "0.0.0.0")
    port = int(os.getenv("API_PORT", "8000"))
    reload_enabled = _parse_bool_env(os.getenv("API_RELOAD"), default=False)
    uvicorn.run(
        "copilot.api:create_app",
        factory=True,
        host=host,
        port=port,
        reload=reload_enabled,
    )


app = create_app()
