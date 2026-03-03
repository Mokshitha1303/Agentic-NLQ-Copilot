import sqlite3

from copilot.executor import SQLiteExecutor


def _create_test_db(path: str) -> None:
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE singer (id INTEGER PRIMARY KEY, name TEXT)")
    conn.executemany(
        "INSERT INTO singer(id, name) VALUES (?, ?)",
        [(1, "A"), (2, "B"), (3, "C")],
    )
    conn.commit()
    conn.close()


def test_execute_returns_rows_and_truncates(tmp_path) -> None:
    db_path = tmp_path / "test.sqlite"
    _create_test_db(str(db_path))

    executor = SQLiteExecutor()
    result = executor.execute(
        db_path=db_path,
        sql="SELECT id, name FROM singer ORDER BY id",
        max_rows=2,
        timeout_ms=500,
    )

    assert result.error is None
    assert result.columns == ["id", "name"]
    assert result.returned_row_count == 2
    assert result.truncated is True
    assert result.rows == [(1, "A"), (2, "B")]


def test_execute_times_out_on_heavy_query(tmp_path) -> None:
    db_path = tmp_path / "heavy.sqlite"
    _create_test_db(str(db_path))

    executor = SQLiteExecutor(progress_handler_steps=500)
    heavy_sql = """
    WITH RECURSIVE cnt(x) AS (
      SELECT 1
      UNION ALL
      SELECT x + 1 FROM cnt WHERE x < 100000000
    )
    SELECT SUM(x) FROM cnt
    """
    result = executor.execute(
        db_path=db_path,
        sql=heavy_sql,
        max_rows=1,
        timeout_ms=1,
    )

    assert result.error is not None
