from copilot.guardrails import SQLGuardrails


def test_allows_select_and_enforces_limit() -> None:
    guardrails = SQLGuardrails()
    result = guardrails.validate(
        "SELECT name FROM singer",
        allowed_tables=None,
        max_rows=50,
    )
    assert result.is_valid
    assert result.sql is not None
    assert "limit 50" in result.sql.lower()


def test_blocks_dml() -> None:
    guardrails = SQLGuardrails()
    result = guardrails.validate(
        "DELETE FROM singer",
        allowed_tables=None,
        max_rows=50,
    )
    assert not result.is_valid
    assert any("Disallowed" in error or "Only SELECT" in error for error in result.errors)


def test_allowed_tables_are_enforced() -> None:
    guardrails = SQLGuardrails()
    result = guardrails.validate(
        "SELECT * FROM album",
        allowed_tables=["singer"],
        max_rows=50,
    )
    assert not result.is_valid
    assert any("allow-list" in error for error in result.errors)
