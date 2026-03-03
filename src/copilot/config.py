from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(slots=True)
class CopilotConfig:
    dataset_root: Path = Path("Dataset/spider_data")
    tables_filename: str = "test_tables.json"
    databases_dirname: str = "test_database"
    train_databases_dirname: str = "database"
    model: str = "gpt-4o-mini"
    temperature: float = 0.0
    request_timeout_s: int = 45
    max_schema_tables: int = 8
    max_retries: int = 2
    default_max_rows: int = 200
    default_timeout_ms: int = 2500

    @classmethod
    def from_env(cls, dataset_root: str | Path | None = None) -> "CopilotConfig":
        load_dotenv()
        root = Path(dataset_root) if dataset_root is not None else Path(
            os.getenv("SPIDER_DATASET_ROOT", "Dataset/spider_data")
        )
        return cls(
            dataset_root=root,
            tables_filename=os.getenv("SPIDER_TABLES_FILE", "test_tables.json"),
            databases_dirname=os.getenv("SPIDER_DB_DIR", "test_database"),
            train_databases_dirname=os.getenv("SPIDER_TRAIN_DB_DIR", "database"),
            model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
            temperature=float(os.getenv("OPENAI_TEMPERATURE", "0.0")),
            request_timeout_s=int(os.getenv("OPENAI_TIMEOUT_S", "45")),
            max_schema_tables=int(os.getenv("COPILOT_MAX_SCHEMA_TABLES", "8")),
            max_retries=int(os.getenv("COPILOT_MAX_RETRIES", "2")),
            default_max_rows=int(os.getenv("COPILOT_DEFAULT_MAX_ROWS", "200")),
            default_timeout_ms=int(os.getenv("COPILOT_DEFAULT_TIMEOUT_MS", "2500")),
        )

    @property
    def tables_path(self) -> Path:
        return self.dataset_root / self.tables_filename

    @property
    def database_root(self) -> Path:
        return self.dataset_root / self.databases_dirname

    @property
    def train_database_root(self) -> Path:
        return self.dataset_root / self.train_databases_dirname

    def resolve_db_path(self, db_id: str) -> Path:
        candidates = [
            self.database_root / db_id / f"{db_id}.sqlite",
            self.train_database_root / db_id / f"{db_id}.sqlite",
        ]
        for path in candidates:
            if path.exists():
                return path
        joined = "\n".join(str(c) for c in candidates)
        raise FileNotFoundError(f"Could not resolve SQLite DB for '{db_id}'. Tried:\n{joined}")
