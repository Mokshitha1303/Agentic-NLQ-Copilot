from __future__ import annotations

import time
from typing import Any, TypedDict

from langgraph.graph import END, START, StateGraph

from .config import CopilotConfig
from .executor import SQLiteExecutor
from .guardrails import SQLGuardrails
from .llm import LLMClient, OpenAILLMClient, RuleBasedLLMClient
from .models import AuditEvent, CopilotResponse, ExecutionResult
from .schema_retriever import SchemaContextRetriever
from .spider import SpiderCatalog


class CopilotState(TypedDict, total=False):
    question: str
    db_id: str
    allowed_tables: list[str] | None
    max_rows: int
    timeout_ms: int
    max_attempts: int
    attempt: int
    forced_sql: str | None
    skip_explanation: bool

    schema_context: str
    candidate_sql: str
    generation_rationale: str
    validated_sql: str
    validation_ok: bool
    validation_errors: list[str]
    blocked: bool
    explanation: str
    errors: list[str]
    execution: ExecutionResult | None
    audit: list[AuditEvent]


class EnterpriseNLQCopilot:
    def __init__(
        self,
        *,
        config: CopilotConfig,
        catalog: SpiderCatalog,
        retriever: SchemaContextRetriever,
        llm: LLMClient,
        guardrails: SQLGuardrails,
        executor: SQLiteExecutor,
    ) -> None:
        self.config = config
        self.catalog = catalog
        self.retriever = retriever
        self.llm = llm
        self.guardrails = guardrails
        self.executor = executor
        self._graph = self._build_graph()

    @classmethod
    def from_config(
        cls,
        config: CopilotConfig | None = None,
        *,
        use_llm: bool = True,
    ) -> "EnterpriseNLQCopilot":
        cfg = config or CopilotConfig.from_env()
        catalog = SpiderCatalog(cfg)
        retriever = SchemaContextRetriever(catalog)
        llm: LLMClient
        if use_llm:
            llm = OpenAILLMClient(
                model=cfg.model,
                temperature=cfg.temperature,
                timeout_s=cfg.request_timeout_s,
            )
        else:
            llm = RuleBasedLLMClient()
        return cls(
            config=cfg,
            catalog=catalog,
            retriever=retriever,
            llm=llm,
            guardrails=SQLGuardrails(),
            executor=SQLiteExecutor(),
        )

    def ask(
        self,
        *,
        question: str,
        db_id: str,
        allowed_tables: list[str] | None = None,
        max_rows: int | None = None,
        timeout_ms: int | None = None,
        forced_sql: str | None = None,
        skip_explanation: bool = False,
    ) -> CopilotResponse:
        request_start = time.perf_counter()
        init_state: CopilotState = {
            "question": question,
            "db_id": db_id,
            "allowed_tables": allowed_tables,
            "max_rows": max_rows if max_rows is not None else self.config.default_max_rows,
            "timeout_ms": timeout_ms if timeout_ms is not None else self.config.default_timeout_ms,
            "max_attempts": max(1, self.config.max_retries + 1),
            "attempt": 0,
            "forced_sql": forced_sql,
            "skip_explanation": skip_explanation,
            "validation_errors": [],
            "errors": [],
            "blocked": False,
            "audit": [],
            "execution": None,
            "explanation": "",
        }
        final_state = self._graph.invoke(init_state)
        latency_ms = (time.perf_counter() - request_start) * 1000.0

        sql = final_state.get("validated_sql") or final_state.get("candidate_sql")
        return CopilotResponse(
            question=question,
            db_id=db_id,
            sql=sql,
            blocked=bool(final_state.get("blocked", False)),
            explanation=final_state.get("explanation", ""),
            errors=list(final_state.get("errors", [])),
            execution=final_state.get("execution"),
            audit=list(final_state.get("audit", [])),
            latency_ms=latency_ms,
        )

    def _build_graph(self):
        graph = StateGraph(CopilotState)
        graph.add_node("retrieve_schema", self._retrieve_schema_node)
        graph.add_node("generate_sql", self._generate_sql_node)
        graph.add_node("validate_sql", self._validate_sql_node)
        graph.add_node("execute_sql", self._execute_sql_node)
        graph.add_node("explain", self._explain_node)
        graph.add_node("blocked", self._blocked_node)

        graph.add_edge(START, "retrieve_schema")
        graph.add_edge("retrieve_schema", "generate_sql")
        graph.add_edge("generate_sql", "validate_sql")
        graph.add_conditional_edges(
            "validate_sql",
            self._route_after_validation,
            {
                "execute_sql": "execute_sql",
                "retry": "generate_sql",
                "blocked": "blocked",
            },
        )
        graph.add_edge("execute_sql", "explain")
        graph.add_edge("explain", END)
        graph.add_edge("blocked", END)
        return graph.compile()

    def _route_after_validation(self, state: CopilotState) -> str:
        if state.get("validation_ok", False):
            return "execute_sql"
        if state.get("attempt", 0) < state.get("max_attempts", 1):
            return "retry"
        return "blocked"

    def _retrieve_schema_node(self, state: CopilotState) -> dict[str, Any]:
        started = time.perf_counter()
        context = self.retriever.retrieve(
            db_id=state["db_id"],
            question=state["question"],
            top_k_tables=self.config.max_schema_tables,
            allowed_tables=state.get("allowed_tables"),
        )
        return {
            "schema_context": context,
            "audit": self._append_audit(
                state,
                step="retrieve_schema",
                status="ok",
                started=started,
                details={"context_chars": len(context)},
            ),
        }

    def _generate_sql_node(self, state: CopilotState) -> dict[str, Any]:
        started = time.perf_counter()
        attempt = int(state.get("attempt", 0)) + 1
        feedback = "; ".join(state.get("validation_errors", [])) or None

        forced_sql = state.get("forced_sql")
        errors = list(state.get("errors", []))
        if forced_sql:
            sql = forced_sql.strip().rstrip(";")
            rationale = "Using forced SQL input."
            status = "ok"
        else:
            try:
                sql, rationale = self.llm.generate_sql(
                    question=state["question"],
                    db_id=state["db_id"],
                    schema_context=state.get("schema_context", ""),
                    feedback=feedback,
                )
                status = "ok" if sql else "error"
                if not sql:
                    errors.append("LLM produced empty SQL.")
            except Exception as exc:
                sql = ""
                rationale = ""
                status = "error"
                errors.append(f"LLM generation failed: {exc}")

        return {
            "attempt": attempt,
            "candidate_sql": sql,
            "generation_rationale": rationale,
            "validation_ok": False,
            "errors": errors,
            "audit": self._append_audit(
                state,
                step="generate_sql",
                status=status,
                started=started,
                details={"attempt": attempt, "forced_sql": bool(forced_sql)},
            ),
        }

    def _validate_sql_node(self, state: CopilotState) -> dict[str, Any]:
        started = time.perf_counter()
        candidate_sql = state.get("candidate_sql", "")
        result = self.guardrails.validate(
            candidate_sql,
            allowed_tables=state.get("allowed_tables"),
            max_rows=state["max_rows"],
        )
        errors = list(state.get("errors", []))
        if not result.is_valid:
            errors.extend(result.errors)

        return {
            "validation_ok": result.is_valid,
            "validated_sql": result.sql or "",
            "validation_errors": result.errors,
            "errors": errors,
            "audit": self._append_audit(
                state,
                step="validate_sql",
                status="ok" if result.is_valid else "error",
                started=started,
                details={
                    "referenced_tables": result.referenced_tables,
                    "error_count": len(result.errors),
                },
            ),
        }

    def _execute_sql_node(self, state: CopilotState) -> dict[str, Any]:
        started = time.perf_counter()
        sql = state.get("validated_sql", "").strip()
        errors = list(state.get("errors", []))
        if not sql:
            errors.append("No validated SQL available for execution.")
            return {
                "execution": ExecutionResult(
                    columns=[],
                    rows=[],
                    returned_row_count=0,
                    truncated=False,
                    latency_ms=0.0,
                    error="No SQL to execute.",
                ),
                "errors": errors,
                "audit": self._append_audit(
                    state,
                    step="execute_sql",
                    status="error",
                    started=started,
                    details={},
                ),
            }

        db_path = self.config.resolve_db_path(state["db_id"])
        execution = self.executor.execute(
            db_path=db_path,
            sql=sql,
            max_rows=state["max_rows"],
            timeout_ms=state["timeout_ms"],
        )
        if execution.error:
            errors.append(execution.error)

        return {
            "execution": execution,
            "errors": errors,
            "audit": self._append_audit(
                state,
                step="execute_sql",
                status="ok" if execution.error is None else "error",
                started=started,
                details={
                    "db_path": str(db_path),
                    "returned_rows": execution.returned_row_count,
                    "truncated": execution.truncated,
                    "execution_latency_ms": execution.latency_ms,
                },
            ),
        }

    def _explain_node(self, state: CopilotState) -> dict[str, Any]:
        started = time.perf_counter()
        execution = state.get("execution")
        sql = state.get("validated_sql") or state.get("candidate_sql", "")

        if execution is None:
            explanation = "No execution payload available."
            status = "error"
        elif execution.error:
            explanation = f"Query execution failed: {execution.error}"
            status = "ok"
        elif state.get("skip_explanation"):
            explanation = (
                f"Returned {execution.returned_row_count} rows "
                f"with columns {execution.columns}."
            )
            status = "ok"
        else:
            try:
                explanation = self.llm.explain_result(
                    question=state["question"],
                    sql=sql,
                    execution=execution,
                )
                status = "ok"
            except Exception as exc:
                explanation = (
                    f"Returned {execution.returned_row_count} rows. "
                    f"Explanation model failed: {exc}"
                )
                status = "error"

        return {
            "explanation": explanation,
            "audit": self._append_audit(
                state,
                step="explain",
                status=status,
                started=started,
                details={},
            ),
        }

    def _blocked_node(self, state: CopilotState) -> dict[str, Any]:
        started = time.perf_counter()
        validation_errors = state.get("validation_errors", [])
        reason = " | ".join(validation_errors) if validation_errors else "Validation failed."
        return {
            "blocked": True,
            "explanation": f"Query blocked by safety guardrails. {reason}",
            "audit": self._append_audit(
                state,
                step="blocked",
                status="ok",
                started=started,
                details={"reason": reason},
            ),
        }

    @staticmethod
    def _append_audit(
        state: CopilotState,
        *,
        step: str,
        status: str,
        started: float,
        details: dict[str, Any],
    ) -> list[AuditEvent]:
        events = list(state.get("audit", []))
        events.append(
            AuditEvent(
                step=step,
                status=status,
                started_at=started,
                finished_at=time.perf_counter(),
                details=details,
            )
        )
        return events
