from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(slots=True)
class AuditEvent:
    step: str
    status: str
    started_at: float
    finished_at: float
    details: dict[str, Any] = field(default_factory=dict)

    @property
    def duration_ms(self) -> float:
        return max(0.0, (self.finished_at - self.started_at) * 1000.0)


@dataclass(slots=True)
class ValidationResult:
    is_valid: bool
    sql: str | None
    errors: list[str] = field(default_factory=list)
    referenced_tables: list[str] = field(default_factory=list)


@dataclass(slots=True)
class ExecutionResult:
    columns: list[str]
    rows: list[tuple[Any, ...]]
    returned_row_count: int
    truncated: bool
    latency_ms: float
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["rows"] = [list(r) for r in self.rows]
        return payload


@dataclass(slots=True)
class CopilotResponse:
    question: str
    db_id: str
    sql: str | None
    blocked: bool
    explanation: str
    errors: list[str]
    execution: ExecutionResult | None
    audit: list[AuditEvent]
    latency_ms: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "question": self.question,
            "db_id": self.db_id,
            "sql": self.sql,
            "blocked": self.blocked,
            "explanation": self.explanation,
            "errors": self.errors,
            "execution": None if self.execution is None else self.execution.to_dict(),
            "audit": [
                {
                    "step": event.step,
                    "status": event.status,
                    "started_at": event.started_at,
                    "finished_at": event.finished_at,
                    "duration_ms": event.duration_ms,
                    "details": event.details,
                }
                for event in self.audit
            ],
            "latency_ms": self.latency_ms,
        }


@dataclass(slots=True)
class BenchmarkRecord:
    index: int
    db_id: str
    question: str
    gold_sql: str
    predicted_sql: str | None
    blocked: bool
    success: bool
    error: str | None
    latency_ms: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
