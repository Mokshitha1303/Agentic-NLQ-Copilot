from __future__ import annotations

import re

from .spider import DatabaseSchema, SpiderCatalog

TOKEN_RE = re.compile(r"[A-Za-z0-9_]+")


def _tokenize(text: str) -> set[str]:
    return {token.lower() for token in TOKEN_RE.findall(text)}


class SchemaContextRetriever:
    """
    Lightweight lexical retriever over Spider schema metadata.
    """

    def __init__(self, catalog: SpiderCatalog) -> None:
        self.catalog = catalog

    def retrieve(
        self,
        db_id: str,
        question: str,
        top_k_tables: int = 8,
        allowed_tables: list[str] | None = None,
    ) -> str:
        schema = self.catalog.get_schema(db_id)
        selected_tables = self._select_tables(
            schema=schema,
            question=question,
            top_k_tables=top_k_tables,
            allowed_tables=allowed_tables,
        )
        return self._format_context(schema=schema, selected_tables=selected_tables)

    def _select_tables(
        self,
        schema: DatabaseSchema,
        question: str,
        top_k_tables: int,
        allowed_tables: list[str] | None,
    ) -> list[str]:
        question_tokens = _tokenize(question)
        allowed = {table.lower() for table in allowed_tables} if allowed_tables else None

        scored: list[tuple[float, str]] = []
        for table in schema.tables:
            if allowed and table.name.lower() not in allowed:
                continue

            table_tokens = _tokenize(table.name) | _tokenize(table.original_name)
            overlap = len(table_tokens & question_tokens)
            score = overlap * 2.0

            if table.name.lower() in question.lower():
                score += 2.0

            for column in table.columns:
                column_tokens = _tokenize(column.name) | _tokenize(column.original_name)
                col_overlap = len(column_tokens & question_tokens)
                score += col_overlap * 0.25

            scored.append((score, table.name))

        if not scored:
            return []

        scored.sort(key=lambda item: (-item[0], item[1]))
        selected = [name for _, name in scored[: max(1, top_k_tables)]]
        if all(score <= 0 for score, _ in scored):
            # Fallback for weak lexical signals.
            selected = [table.name for table in schema.tables[: max(1, top_k_tables)]]
        return selected

    def _format_context(self, schema: DatabaseSchema, selected_tables: list[str]) -> str:
        if not selected_tables:
            selected_tables = schema.table_names

        table_map = schema.table_map()
        selected_set = {name.lower() for name in selected_tables}
        lines = [f"Database: {schema.db_id}", "Relevant tables:"]

        for table_name in selected_tables:
            table = table_map.get(table_name.lower())
            if table is None:
                continue
            lines.append(f"- {table.name} (original: {table.original_name})")
            for column in table.columns:
                marker = " [PK]" if column.is_primary else ""
                lines.append(
                    f"  - {column.name} (original: {column.original_name}, type: {column.col_type}){marker}"
                )

        fk_lines: list[str] = []
        for edge in schema.foreign_keys:
            if edge.source_table.lower() in selected_set and edge.target_table.lower() in selected_set:
                fk_lines.append(
                    f"- {edge.source_table}.{edge.source_column} -> "
                    f"{edge.target_table}.{edge.target_column}"
                )

        if fk_lines:
            lines.append("Foreign keys in context:")
            lines.extend(fk_lines)

        return "\n".join(lines)
