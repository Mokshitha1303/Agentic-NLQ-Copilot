from __future__ import annotations

import csv
import json
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Iterable

from .agent import EnterpriseNLQCopilot
from .models import BenchmarkRecord, ExecutionResult
from .reliability import latency_summary, run_safety_suite
from .spider import SpiderExample


def _canonical_value(value: object) -> object:
    if isinstance(value, float):
        return round(value, 10)
    return value


def _canonical_rows(rows: list[tuple[object, ...]]) -> list[tuple[object, ...]]:
    return [tuple(_canonical_value(value) for value in row) for row in rows]


def compare_execution_results(
    predicted: ExecutionResult,
    gold: ExecutionResult,
    *,
    order_sensitive: bool,
) -> bool:
    if predicted.error or gold.error:
        return False

    pred_rows = _canonical_rows(predicted.rows)
    gold_rows = _canonical_rows(gold.rows)

    if order_sensitive:
        return pred_rows == gold_rows

    return sorted(pred_rows) == sorted(gold_rows)


def _load_examples(copilot: EnterpriseNLQCopilot, split: str) -> list[SpiderExample]:
    if split == "train":
        return copilot.catalog.load_split("train_spider") + copilot.catalog.load_split("train_others")
    return copilot.catalog.load_split(split)


def _slice_examples(examples: list[SpiderExample], limit: int | None) -> Iterable[SpiderExample]:
    if limit is None:
        return examples
    return examples[: max(0, limit)]


def run_spider_benchmark(
    copilot: EnterpriseNLQCopilot,
    *,
    split: str = "dev",
    mode: str = "agent",
    limit: int | None = None,
    max_rows: int = 200,
    timeout_ms: int = 2500,
    output_dir: Path = Path("outputs"),
    run_safety_checks: bool = True,
) -> dict[str, object]:
    if mode not in {"agent", "oracle"}:
        raise ValueError("mode must be 'agent' or 'oracle'")

    examples = list(_slice_examples(_load_examples(copilot, split), limit))
    records: list[BenchmarkRecord] = []
    correct = 0
    blocked = 0

    for idx, example in enumerate(examples):
        forced_sql = example.query if mode == "oracle" else None
        response = copilot.ask(
            question=example.question,
            db_id=example.db_id,
            max_rows=max_rows,
            timeout_ms=timeout_ms,
            forced_sql=forced_sql,
            skip_explanation=True,
        )
        predicted_sql = response.sql

        if response.blocked:
            blocked += 1

        success = False
        error: str | None = None
        if response.execution is not None and response.execution.error is None and predicted_sql:
            guarded_gold = copilot.guardrails.validate(
                example.query,
                allowed_tables=None,
                max_rows=max_rows,
            )
            if guarded_gold.is_valid and guarded_gold.sql:
                gold_execution = copilot.executor.execute(
                    db_path=copilot.config.resolve_db_path(example.db_id),
                    sql=guarded_gold.sql,
                    max_rows=max_rows,
                    timeout_ms=timeout_ms,
                )
                success = compare_execution_results(
                    predicted=response.execution,
                    gold=gold_execution,
                    order_sensitive=("order by" in example.query.lower()),
                )
                if not success and gold_execution.error:
                    error = f"gold_execution_error={gold_execution.error}"
            else:
                error = "gold query failed guardrails validation"
        else:
            if response.errors:
                error = response.errors[-1]
            elif response.execution and response.execution.error:
                error = response.execution.error

        if success:
            correct += 1

        records.append(
            BenchmarkRecord(
                index=idx,
                db_id=example.db_id,
                question=example.question,
                gold_sql=example.query,
                predicted_sql=predicted_sql,
                blocked=response.blocked,
                success=success,
                error=error,
                latency_ms=response.latency_ms,
            )
        )

    total = len(records)
    latencies = [record.latency_ms for record in records]
    summary: dict[str, object] = {
        "timestamp_utc": datetime.utcnow().isoformat(timespec="seconds"),
        "split": split,
        "mode": mode,
        "total_examples": total,
        "execution_accuracy": (correct / total) if total else 0.0,
        "blocked_rate": (blocked / total) if total else 0.0,
        "latency": latency_summary(latencies),
    }

    if run_safety_checks:
        summary["safety_suite"] = run_safety_suite(
            copilot.guardrails,
            allowed_tables=None,
            max_rows=max_rows,
        )

    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    json_path = output_dir / f"benchmark_{split}_{mode}_{stamp}.json"
    csv_path = output_dir / f"benchmark_{split}_{mode}_{stamp}.csv"

    payload = {
        "summary": summary,
        "records": [record.to_dict() for record in records],
    }
    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=list(asdict(records[0]).keys()) if records else list(BenchmarkRecord.__annotations__.keys()),
        )
        writer.writeheader()
        for record in records:
            writer.writerow(record.to_dict())

    summary["artifacts"] = {"json": str(json_path), "csv": str(csv_path)}
    return summary
