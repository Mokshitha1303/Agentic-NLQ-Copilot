from __future__ import annotations

import argparse
import json
from pathlib import Path

from .agent import EnterpriseNLQCopilot
from .benchmark import run_spider_benchmark
from .config import CopilotConfig
from .reliability import run_safety_suite


def _parse_allowed_tables(value: str | None) -> list[str] | None:
    if not value:
        return None
    parsed = [item.strip() for item in value.split(",") if item.strip()]
    return parsed if parsed else None


def _build_copilot(args: argparse.Namespace, *, use_llm: bool) -> EnterpriseNLQCopilot:
    config = CopilotConfig.from_env(dataset_root=args.dataset_root)
    return EnterpriseNLQCopilot.from_config(config=config, use_llm=use_llm)


def _cmd_ask(args: argparse.Namespace) -> None:
    copilot = _build_copilot(args, use_llm=not args.no_llm)
    response = copilot.ask(
        question=args.question,
        db_id=args.db_id,
        allowed_tables=_parse_allowed_tables(args.allowed_tables),
        max_rows=args.max_rows,
        timeout_ms=args.timeout_ms,
    )
    print(json.dumps(response.to_dict(), indent=2, default=str))


def _cmd_benchmark(args: argparse.Namespace) -> None:
    use_llm = args.mode == "agent" and not args.no_llm
    copilot = _build_copilot(args, use_llm=use_llm)
    summary = run_spider_benchmark(
        copilot,
        split=args.split,
        mode=args.mode,
        limit=args.limit,
        max_rows=args.max_rows,
        timeout_ms=args.timeout_ms,
        output_dir=Path(args.output_dir),
        run_safety_checks=not args.skip_safety,
    )
    print(json.dumps(summary, indent=2, default=str))


def _cmd_safety(args: argparse.Namespace) -> None:
    copilot = _build_copilot(args, use_llm=False)
    report = run_safety_suite(
        copilot.guardrails,
        allowed_tables=_parse_allowed_tables(args.allowed_tables),
        max_rows=args.max_rows,
    )
    print(json.dumps(report, indent=2))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="nlq-copilot",
        description="Enterprise NLQ Copilot for Spider (LangGraph + SQL guardrails).",
    )
    parser.add_argument(
        "--dataset-root",
        default="Dataset/spider_data",
        help="Path to spider_data root directory.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    ask_parser = subparsers.add_parser("ask", help="Ask one NL question against one Spider DB.")
    ask_parser.add_argument("--db-id", required=True, help="Spider db_id, e.g. world_1")
    ask_parser.add_argument("--question", required=True, help="Natural-language question")
    ask_parser.add_argument("--allowed-tables", default=None, help="Comma-separated allow-list")
    ask_parser.add_argument("--max-rows", type=int, default=200)
    ask_parser.add_argument("--timeout-ms", type=int, default=2500)
    ask_parser.add_argument("--no-llm", action="store_true", help="Disable OpenAI calls.")
    ask_parser.set_defaults(func=_cmd_ask)

    benchmark_parser = subparsers.add_parser(
        "benchmark",
        help="Run execution-based benchmark over Spider split.",
    )
    benchmark_parser.add_argument("--split", default="dev", choices=["train", "dev", "test"])
    benchmark_parser.add_argument("--mode", default="agent", choices=["agent", "oracle"])
    benchmark_parser.add_argument("--limit", type=int, default=None)
    benchmark_parser.add_argument("--max-rows", type=int, default=200)
    benchmark_parser.add_argument("--timeout-ms", type=int, default=2500)
    benchmark_parser.add_argument("--output-dir", default="outputs")
    benchmark_parser.add_argument("--skip-safety", action="store_true")
    benchmark_parser.add_argument("--no-llm", action="store_true", help="Force no-LLM mode.")
    benchmark_parser.set_defaults(func=_cmd_benchmark)

    safety_parser = subparsers.add_parser("safety", help="Run safety guardrail suite.")
    safety_parser.add_argument("--allowed-tables", default=None, help="Comma-separated allow-list")
    safety_parser.add_argument("--max-rows", type=int, default=200)
    safety_parser.set_defaults(func=_cmd_safety)

    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
