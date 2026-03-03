import json
import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from copilot.api import create_app
from copilot.config import CopilotConfig


def _create_temp_spider_dataset(root: Path) -> Path:
    dataset_root = root / "spider_data"
    db_dir = dataset_root / "test_database" / "concert_singer"
    db_dir.mkdir(parents=True, exist_ok=True)

    db_path = db_dir / "concert_singer.sqlite"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE singer (singer_id INTEGER PRIMARY KEY, name TEXT)")
    conn.executemany(
        "INSERT INTO singer(singer_id, name) VALUES (?, ?)",
        [(1, "A"), (2, "B"), (3, "C"), (4, "D"), (5, "E"), (6, "F")],
    )
    conn.commit()
    conn.close()

    tables_payload = [
        {
            "db_id": "concert_singer",
            "table_names": ["singer"],
            "table_names_original": ["singer"],
            "column_names": [[-1, "*"], [0, "singer id"], [0, "name"]],
            "column_names_original": [[-1, "*"], [0, "Singer_ID"], [0, "Name"]],
            "column_types": ["text", "number", "text"],
            "primary_keys": [1],
            "foreign_keys": [],
        }
    ]
    (dataset_root / "test_tables.json").write_text(json.dumps(tables_payload), encoding="utf-8")
    return dataset_root


@pytest.fixture(scope="module")
def client(tmp_path_factory: pytest.TempPathFactory) -> TestClient:
    dataset_root = _create_temp_spider_dataset(tmp_path_factory.mktemp("spider_dataset"))
    app = create_app(
        config=CopilotConfig.from_env(dataset_root),
        use_llm=False,
    )
    with TestClient(app) as test_client:
        yield test_client


def test_health_endpoint(client: TestClient) -> None:
    response = client.get("/health")
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["loaded_databases"] == 1


def test_schema_endpoint(client: TestClient) -> None:
    response = client.get("/v1/databases/concert_singer/schema")
    assert response.status_code == 200
    payload = response.json()
    assert payload["db_id"] == "concert_singer"
    assert payload["table_count"] == 1


def test_query_endpoint_forced_sql(client: TestClient) -> None:
    response = client.post(
        "/v1/query",
        json={
            "db_id": "concert_singer",
            "question": "How many singers are in the database?",
            "forced_sql": "SELECT count(*) FROM singer",
            "skip_explanation": True,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["result"]["blocked"] is False
    assert payload["result"]["execution"]["returned_row_count"] == 1


def test_query_endpoint_blocked_sql(client: TestClient) -> None:
    response = client.post(
        "/v1/query",
        json={
            "db_id": "concert_singer",
            "question": "Delete singer data",
            "forced_sql": "DELETE FROM singer",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "blocked"
    assert payload["result"]["blocked"] is True
