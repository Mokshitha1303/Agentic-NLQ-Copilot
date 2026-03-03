from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Iterable

from .config import CopilotConfig


@dataclass(slots=True)
class SchemaColumn:
    index: int
    table_index: int
    name: str
    original_name: str
    col_type: str
    is_primary: bool


@dataclass(slots=True)
class SchemaTable:
    index: int
    name: str
    original_name: str
    columns: list[SchemaColumn] = field(default_factory=list)


@dataclass(slots=True)
class ForeignKeyEdge:
    source_table: str
    source_column: str
    target_table: str
    target_column: str


@dataclass(slots=True)
class DatabaseSchema:
    db_id: str
    tables: list[SchemaTable]
    primary_keys: list[int]
    foreign_keys: list[ForeignKeyEdge]
    raw: dict[str, Any]

    @property
    def table_names(self) -> list[str]:
        return [table.name for table in self.tables]

    def table_map(self) -> dict[str, SchemaTable]:
        return {table.name.lower(): table for table in self.tables}


@dataclass(slots=True)
class SpiderExample:
    db_id: str
    question: str
    query: str
    query_toks: list[str]
    query_toks_no_value: list[str]
    question_toks: list[str]
    sql: dict[str, Any]


class SpiderCatalog:
    """Loads Spider schema metadata and split examples."""

    SPLIT_TO_FILE = {
        "train_spider": "train_spider.json",
        "train_others": "train_others.json",
        "dev": "dev.json",
        "test": "test.json",
    }

    def __init__(self, config: CopilotConfig) -> None:
        self.config = config
        self._schemas: dict[str, DatabaseSchema] = {}
        self._split_cache: dict[str, list[SpiderExample]] = {}
        self._load_schemas()

    @property
    def schemas(self) -> dict[str, DatabaseSchema]:
        return self._schemas

    def _load_schemas(self) -> None:
        tables_path = self.config.tables_path
        if not tables_path.exists():
            raise FileNotFoundError(f"Spider schema file not found: {tables_path}")

        payload = json.loads(tables_path.read_text(encoding="utf-8"))
        for raw_schema in payload:
            schema = self._parse_schema(raw_schema)
            self._schemas[schema.db_id] = schema

    def _parse_schema(self, raw_schema: dict[str, Any]) -> DatabaseSchema:
        db_id = raw_schema["db_id"]
        table_names = raw_schema["table_names"]
        table_names_original = raw_schema["table_names_original"]
        column_names = raw_schema["column_names"]
        column_names_original = raw_schema["column_names_original"]
        column_types = raw_schema["column_types"]
        primary_key_indexes = set(raw_schema["primary_keys"])

        tables = [
            SchemaTable(index=i, name=table_names[i], original_name=table_names_original[i])
            for i in range(len(table_names))
        ]

        columns_by_index: dict[int, SchemaColumn] = {}
        for idx, column_meta in enumerate(column_names):
            table_idx, col_name = column_meta
            if table_idx == -1:
                continue

            column = SchemaColumn(
                index=idx,
                table_index=table_idx,
                name=col_name,
                original_name=column_names_original[idx][1],
                col_type=column_types[idx],
                is_primary=idx in primary_key_indexes,
            )
            tables[table_idx].columns.append(column)
            columns_by_index[idx] = column

        edges: list[ForeignKeyEdge] = []
        for source_idx, target_idx in raw_schema["foreign_keys"]:
            source_col = columns_by_index.get(source_idx)
            target_col = columns_by_index.get(target_idx)
            if source_col is None or target_col is None:
                continue
            source_table = tables[source_col.table_index].name
            target_table = tables[target_col.table_index].name
            edges.append(
                ForeignKeyEdge(
                    source_table=source_table,
                    source_column=source_col.name,
                    target_table=target_table,
                    target_column=target_col.name,
                )
            )

        return DatabaseSchema(
            db_id=db_id,
            tables=tables,
            primary_keys=list(raw_schema["primary_keys"]),
            foreign_keys=edges,
            raw=raw_schema,
        )

    def get_schema(self, db_id: str) -> DatabaseSchema:
        if db_id not in self._schemas:
            available = ", ".join(sorted(self._schemas))
            raise KeyError(f"Unknown db_id '{db_id}'. Available: {available}")
        return self._schemas[db_id]

    def load_split(self, split: str) -> list[SpiderExample]:
        if split in self._split_cache:
            return self._split_cache[split]

        if split not in self.SPLIT_TO_FILE:
            choices = ", ".join(sorted(self.SPLIT_TO_FILE))
            raise ValueError(f"Unsupported split '{split}'. Choices: {choices}")

        split_file = self.config.dataset_root / self.SPLIT_TO_FILE[split]
        if not split_file.exists():
            raise FileNotFoundError(f"Split file not found: {split_file}")

        raw_examples = json.loads(split_file.read_text(encoding="utf-8"))
        parsed = [
            SpiderExample(
                db_id=item["db_id"],
                question=item["question"],
                query=item["query"],
                query_toks=item["query_toks"],
                query_toks_no_value=item["query_toks_no_value"],
                question_toks=item["question_toks"],
                sql=item["sql"],
            )
            for item in raw_examples
        ]
        self._split_cache[split] = parsed
        return parsed

    def iter_split(self, split: str) -> Iterable[SpiderExample]:
        return iter(self.load_split(split))
