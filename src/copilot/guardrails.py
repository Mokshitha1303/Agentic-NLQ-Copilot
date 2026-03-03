from __future__ import annotations

import re
from dataclasses import dataclass

from .models import ValidationResult

DISALLOWED_KEYWORDS = {
    "insert",
    "update",
    "delete",
    "drop",
    "alter",
    "create",
    "truncate",
    "attach",
    "detach",
    "pragma",
    "vacuum",
    "replace",
    "reindex",
}


@dataclass(slots=True)
class SQLGuardrails:
    """
    Enforces enterprise safety constraints before SQL execution.
    """

    def __post_init__(self) -> None:
        try:
            import sqlglot  # noqa: F401
        except ImportError as exc:
            raise RuntimeError("sqlglot is required for SQLGuardrails.") from exc

    def validate(
        self,
        sql: str,
        *,
        allowed_tables: list[str] | None,
        max_rows: int,
    ) -> ValidationResult:
        from sqlglot import errors, parse
        from sqlglot import expressions as exp

        raw = sql.strip()
        errors_out: list[str] = []
        if not raw:
            return ValidationResult(is_valid=False, sql=None, errors=["Empty SQL output."])

        if raw.endswith(";"):
            raw = raw[:-1].strip()

        lower_sql = raw.lower()
        for keyword in DISALLOWED_KEYWORDS:
            if re.search(rf"\b{keyword}\b", lower_sql):
                errors_out.append(f"Disallowed operation detected: '{keyword}'.")
                break

        first_token = lower_sql.split(None, 1)[0] if lower_sql.split() else ""
        if first_token not in {"select", "with"}:
            errors_out.append("Only SELECT/CTE queries are allowed.")

        try:
            statements = parse(raw, read="sqlite")
        except errors.ParseError as exc:
            errors_out.append(f"SQL parse failed: {exc}")
            statements = []

        if len(statements) != 1:
            errors_out.append("Exactly one SQL statement is required.")
            return ValidationResult(is_valid=False, sql=None, errors=errors_out)

        statement = statements[0]

        # Explicitly block DML/DDL AST nodes.
        blocked_nodes = (exp.Insert, exp.Update, exp.Delete, exp.Create, exp.Drop, exp.Alter)
        for blocked in blocked_nodes:
            if statement.find(blocked):
                errors_out.append(f"Disallowed statement type: {blocked.__name__}.")

        referenced_tables = self._referenced_tables(statement)
        if allowed_tables:
            allowed_lower = {table.lower() for table in allowed_tables}
            unauthorized = [table for table in referenced_tables if table.lower() not in allowed_lower]
            if unauthorized:
                errors_out.append(
                    "Query references tables outside allow-list: " + ", ".join(sorted(set(unauthorized)))
                )

        if errors_out:
            return ValidationResult(
                is_valid=False,
                sql=None,
                errors=errors_out,
                referenced_tables=sorted(referenced_tables),
            )

        limited_sql = self._enforce_limit(raw, max_rows=max_rows)
        return ValidationResult(
            is_valid=True,
            sql=limited_sql,
            errors=[],
            referenced_tables=sorted(referenced_tables),
        )

    def _referenced_tables(self, statement: object) -> set[str]:
        from sqlglot import expressions as exp

        expr = statement
        cte_names = {
            cte.alias_or_name.lower()
            for cte in expr.find_all(exp.CTE)
            if getattr(cte, "alias_or_name", None)
        }
        tables: set[str] = set()
        for table in expr.find_all(exp.Table):
            name = table.name
            if not name:
                continue
            lowered = name.lower()
            if lowered in cte_names:
                continue
            tables.add(name)
        return tables

    def _enforce_limit(self, sql: str, *, max_rows: int) -> str:
        limit_pattern = re.compile(r"\blimit\s+(\d+)\b", re.IGNORECASE)
        matches = list(limit_pattern.finditer(sql))

        if not matches:
            return f"{sql} LIMIT {max_rows}"

        last_match = matches[-1]
        current_limit = int(last_match.group(1))
        if current_limit <= max_rows:
            return sql

        start, end = last_match.span(1)
        return sql[:start] + str(max_rows) + sql[end:]
