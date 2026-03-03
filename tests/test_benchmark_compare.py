from copilot.benchmark import compare_execution_results
from copilot.models import ExecutionResult


def _exec(rows):
    return ExecutionResult(
        columns=["x"],
        rows=rows,
        returned_row_count=len(rows),
        truncated=False,
        latency_ms=1.0,
        error=None,
    )


def test_compare_order_sensitive() -> None:
    gold = _exec([(1,), (2,)])
    pred = _exec([(2,), (1,)])
    assert not compare_execution_results(pred, gold, order_sensitive=True)


def test_compare_order_insensitive() -> None:
    gold = _exec([(1,), (2,)])
    pred = _exec([(2,), (1,)])
    assert compare_execution_results(pred, gold, order_sensitive=False)
