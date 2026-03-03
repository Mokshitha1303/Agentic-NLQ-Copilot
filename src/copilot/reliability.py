from __future__ import annotations

from dataclasses import dataclass
from statistics import mean

from .guardrails import SQLGuardrails


@dataclass(slots=True)
class SafetyCase:
    name: str
    sql: str
    should_pass: bool


DEFAULT_SAFETY_CASES = [
    SafetyCase(
        name="allow_simple_select",
        sql="SELECT name FROM singer",
        should_pass=True,
    ),
    SafetyCase(
        name="block_delete",
        sql="DELETE FROM singer",
        should_pass=False,
    ),
    SafetyCase(
        name="block_drop",
        sql="DROP TABLE singer",
        should_pass=False,
    ),
    SafetyCase(
        name="block_multi_statement",
        sql="SELECT * FROM singer; DELETE FROM singer",
        should_pass=False,
    ),
]


def run_safety_suite(
    guardrails: SQLGuardrails,
    *,
    allowed_tables: list[str] | None = None,
    max_rows: int = 200,
) -> dict[str, object]:
    records: list[dict[str, object]] = []
    passed = 0

    for case in DEFAULT_SAFETY_CASES:
        result = guardrails.validate(
            case.sql,
            allowed_tables=allowed_tables,
            max_rows=max_rows,
        )
        actual_pass = result.is_valid
        outcome = actual_pass == case.should_pass
        if outcome:
            passed += 1
        records.append(
            {
                "name": case.name,
                "expected_valid": case.should_pass,
                "actual_valid": actual_pass,
                "errors": result.errors,
            }
        )

    return {
        "total": len(DEFAULT_SAFETY_CASES),
        "passed": passed,
        "pass_rate": passed / len(DEFAULT_SAFETY_CASES) if DEFAULT_SAFETY_CASES else 0.0,
        "records": records,
    }


def latency_summary(latencies_ms: list[float]) -> dict[str, float]:
    if not latencies_ms:
        return {"mean_ms": 0.0, "p50_ms": 0.0, "p95_ms": 0.0, "max_ms": 0.0}

    ordered = sorted(latencies_ms)
    p50_idx = int(0.50 * (len(ordered) - 1))
    p95_idx = int(0.95 * (len(ordered) - 1))
    return {
        "mean_ms": mean(ordered),
        "p50_ms": ordered[p50_idx],
        "p95_ms": ordered[p95_idx],
        "max_ms": ordered[-1],
    }
