# Agentic NLQ Copilot (Spider + LangGraph)

This project is an **agentic enterprise NLQ copilot** that converts natural-language business questions into safe, executable SQL over Spider databases.

It targets portfolio alignment with enterprise GenAI roles focused on:

- LLM-powered NLQ
- agent orchestration
- SQL safety guardrails
- execution-grounded responses
- reliability and latency benchmarking

## What It Does

Given a question and `db_id`, the copilot:

1. Retrieves relevant schema context from Spider metadata.
2. Generates SQL with an LLM.
3. Validates SQL with guardrails (read-only policy, allow-list, row limits, single statement).
4. Executes SQL in SQLite read-only mode with timeout protection.
5. Returns results plus a grounded explanation and full audit trail.

## Agent Graph (LangGraph)

The LangGraph state machine runs these nodes:

- `retrieve_schema`
- `generate_sql`
- `validate_sql`
- `execute_sql`
- `explain`
- `blocked`

It retries generation when validation fails, up to a configured retry budget.

## Guardrails

Current guardrails enforce:

- read-only SQL only (`SELECT`/`WITH`)
- disallowed keyword blocking (`DROP`, `DELETE`, `UPDATE`, etc.)
- single-statement policy
- table allow-list support
- output row limit enforcement
- execution timeout via SQLite progress handler

## Spider Dataset Assumptions

Default dataset root:

`Dataset/spider_data`

The project uses:

- `test_tables.json` for schema metadata (206 DBs)
- `test_database/` as the primary SQLite DB root
- fallback to `database/` if needed

## Installation

```bash
python -m pip install -e .
```

Optional dev dependencies:

```bash
python -m pip install -e ".[dev]"
```

Set your OpenAI key for agent mode:

```bash
set OPENAI_API_KEY=your_key_here
```

Optional environment overrides are in `.env.example`.

## API Usage

The project now includes a production-style FastAPI service with OpenAPI docs.

Run API locally:

```bash
python -m pip install -e ".[dev]"
nlq-copilot-api
```

Or:

```bash
python -m uvicorn copilot.api:app --host 0.0.0.0 --port 8000
```

Key URLs:

- `GET /` service metadata
- `GET /health` readiness/liveness
- `GET /docs` interactive Swagger UI
- `GET /redoc` ReDoc docs
- `GET /v1/databases` list Spider DBs
- `GET /v1/databases/{db_id}/schema` schema details
- `POST /v1/query` NLQ -> SQL -> safe execution
- `POST /v1/safety` run guardrail checks
- `POST /v1/benchmark` run evaluation job

Example query request:

```bash
curl -X POST "http://localhost:8000/v1/query" ^
  -H "Content-Type: application/json" ^
  -d "{\"db_id\":\"concert_singer\",\"question\":\"How many singers are there?\"}"
```

## CLI Usage

### 1) Ask a question

```bash
python -m copilot ask --db-id world_1 --question "List the names of all countries in Europe"
```

### 2) Run benchmark on Spider

Agent mode (LLM-generated SQL):

```bash
python -m copilot benchmark --split dev --mode agent --limit 100
```

Oracle mode (gold SQL forced through the same guardrails/execution path):

```bash
python -m copilot benchmark --split dev --mode oracle --limit 100
```

Benchmark artifacts are saved under `outputs/` as JSON + CSV.

### 3) Run safety suite

```bash
python -m copilot safety
```

## Benchmark Metrics

The benchmark outputs:

- execution accuracy (denotation comparison)
- blocked query rate
- latency (`mean`, `p50`, `p95`, `max`)
- safety suite pass rate

## Project Structure

```text
src/copilot/
  agent.py          # LangGraph orchestration
  benchmark.py      # Spider benchmark harness
  cli.py            # CLI entrypoint
  config.py         # Runtime config
  executor.py       # SQLite read-only execution with timeout
  guardrails.py     # SQL validation and safety policies
  llm.py            # OpenAI LLM adapter and fallback client
  reliability.py    # Safety suite + latency stats
  schema_retriever.py
  spider.py         # Spider loaders and schema model
tests/
```

## Notes

- For strict reproducibility and model-free checks, use `--mode oracle`.
- In offline mode, pass `--no-llm` and use oracle benchmark/safety checks.
