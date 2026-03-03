from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Protocol

from .models import ExecutionResult

JSON_RE = re.compile(r"\{[\s\S]*\}")
SQL_BLOCK_RE = re.compile(r"```sql\s*([\s\S]*?)```", re.IGNORECASE)


class LLMClient(Protocol):
    def generate_sql(
        self,
        *,
        question: str,
        db_id: str,
        schema_context: str,
        feedback: str | None = None,
    ) -> tuple[str, str]:
        ...

    def explain_result(
        self,
        *,
        question: str,
        sql: str,
        execution: ExecutionResult,
    ) -> str:
        ...


def _extract_sql(text: str) -> str:
    match = SQL_BLOCK_RE.search(text)
    if match:
        return match.group(1).strip().rstrip(";")

    json_match = JSON_RE.search(text)
    if json_match:
        try:
            payload = json.loads(json_match.group(0))
            sql = str(payload.get("sql", "")).strip()
            if sql:
                return sql.rstrip(";")
        except json.JSONDecodeError:
            pass

    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines:
        return ""
    if len(lines) == 1:
        return lines[0].rstrip(";")

    # Last fallback: take first line that looks like SQL.
    for line in lines:
        if line.lower().startswith(("select", "with")):
            return line.rstrip(";")
    return lines[0].rstrip(";")


@dataclass(slots=True)
class OpenAILLMClient:
    model: str
    temperature: float = 0.0
    timeout_s: int = 45

    def __post_init__(self) -> None:
        try:
            from langchain_openai import ChatOpenAI
        except ImportError as exc:
            raise RuntimeError(
                "langchain-openai is required for OpenAILLMClient. Install project dependencies."
            ) from exc

        self._client = ChatOpenAI(
            model=self.model,
            temperature=self.temperature,
            timeout=self.timeout_s,
        )

    def _invoke(self, *, system_prompt: str, user_prompt: str) -> str:
        response = self._client.invoke(
            [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ]
        )
        return str(response.content)

    def generate_sql(
        self,
        *,
        question: str,
        db_id: str,
        schema_context: str,
        feedback: str | None = None,
    ) -> tuple[str, str]:
        system_prompt = (
            "You are an enterprise SQL copilot.\n"
            "Generate one SQLite query only.\n"
            "Hard constraints:\n"
            "- Read-only query (SELECT/CTE only).\n"
            "- Use only tables/columns visible in schema context.\n"
            "- No comments.\n"
            "- No markdown.\n"
            'Return strict JSON: {"sql": "...", "rationale": "..."}'
        )
        user_prompt = (
            f"Database ID: {db_id}\n\n"
            f"Question:\n{question}\n\n"
            f"Schema context:\n{schema_context}\n\n"
            f"Validator feedback (if any):\n{feedback or 'None'}\n"
        )
        raw = self._invoke(system_prompt=system_prompt, user_prompt=user_prompt)
        sql = _extract_sql(raw)
        rationale = ""
        json_match = JSON_RE.search(raw)
        if json_match:
            try:
                payload = json.loads(json_match.group(0))
                rationale = str(payload.get("rationale", ""))
            except json.JSONDecodeError:
                pass
        return sql, rationale

    def explain_result(
        self,
        *,
        question: str,
        sql: str,
        execution: ExecutionResult,
    ) -> str:
        preview_rows = [list(row) for row in execution.rows[:5]]
        system_prompt = (
            "You are an enterprise analytics assistant. "
            "Explain results only from the provided SQL and rows. "
            "If there is uncertainty, state it explicitly."
        )
        user_prompt = (
            f"Question: {question}\n"
            f"SQL: {sql}\n"
            f"Columns: {execution.columns}\n"
            f"Returned rows: {execution.returned_row_count}\n"
            f"Truncated: {execution.truncated}\n"
            f"Sample rows: {preview_rows}\n"
            "Write a concise grounded explanation (2-5 sentences)."
        )
        return self._invoke(system_prompt=system_prompt, user_prompt=user_prompt).strip()


@dataclass(slots=True)
class RuleBasedLLMClient:
    """
    Fallback for offline/safety testing. It does not generate SQL from NL.
    """

    def generate_sql(
        self,
        *,
        question: str,
        db_id: str,
        schema_context: str,
        feedback: str | None = None,
    ) -> tuple[str, str]:
        raise RuntimeError(
            "RuleBasedLLMClient cannot synthesize SQL from NL questions. "
            "Use OpenAILLMClient or provide forced SQL in benchmark/oracle mode."
        )

    def explain_result(
        self,
        *,
        question: str,
        sql: str,
        execution: ExecutionResult,
    ) -> str:
        if execution.error:
            return f"Query failed: {execution.error}"
        if execution.returned_row_count == 0:
            return "The query returned no rows."
        sample = execution.rows[0]
        return (
            f"The query returned {execution.returned_row_count} rows across "
            f"{len(execution.columns)} columns. First row: {sample}."
        )
