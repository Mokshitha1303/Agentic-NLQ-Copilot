from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .models import ExecutionResult


@dataclass(slots=True)
class SQLiteExecutor:
    progress_handler_steps: int = 2_000

    def execute(
        self,
        *,
        db_path: Path,
        sql: str,
        max_rows: int,
        timeout_ms: int,
    ) -> ExecutionResult:
        start = time.perf_counter()
        deadline = time.perf_counter() + timeout_ms / 1000.0
        uri = f"file:{db_path.as_posix()}?mode=ro"
        connection: sqlite3.Connection | None = None

        try:
            connection = sqlite3.connect(uri, uri=True)
            connection.execute("PRAGMA query_only = 1")

            def _progress_handler() -> int:
                return 1 if time.perf_counter() >= deadline else 0

            connection.set_progress_handler(_progress_handler, self.progress_handler_steps)
            cursor = connection.execute(sql)
            columns = [item[0] for item in cursor.description] if cursor.description else []
            rows = cursor.fetchmany(max_rows + 1)
            truncated = len(rows) > max_rows
            if truncated:
                rows = rows[:max_rows]
            materialized_rows = [self._normalize_row(row) for row in rows]

            return ExecutionResult(
                columns=columns,
                rows=materialized_rows,
                returned_row_count=len(materialized_rows),
                truncated=truncated,
                latency_ms=(time.perf_counter() - start) * 1000.0,
                error=None,
            )
        except sqlite3.OperationalError as exc:
            message = str(exc)
            if "interrupted" in message.lower():
                message = f"Timed out after {timeout_ms} ms."
            return ExecutionResult(
                columns=[],
                rows=[],
                returned_row_count=0,
                truncated=False,
                latency_ms=(time.perf_counter() - start) * 1000.0,
                error=message,
            )
        except Exception as exc:  # pragma: no cover - defensive fallback
            return ExecutionResult(
                columns=[],
                rows=[],
                returned_row_count=0,
                truncated=False,
                latency_ms=(time.perf_counter() - start) * 1000.0,
                error=str(exc),
            )
        finally:
            if connection is not None:
                connection.close()

    @staticmethod
    def _normalize_row(row: Any) -> tuple[Any, ...]:
        if isinstance(row, tuple):
            return row
        if isinstance(row, list):
            return tuple(row)
        try:
            return tuple(row)
        except TypeError:
            return (row,)
