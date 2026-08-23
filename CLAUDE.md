# Sankalp

Durable saga orchestrator for money movement. FastAPI + PostgreSQL 16 + Redis 7, Python 3.14.
Schema and execution flows: `docs/spec.md` — read it before touching the engine or migrations.

**The guarantee:** kill any process at any instant — workflows resume from the last completed
step, and no step's side effect executes twice. Every change must preserve this.

## Non-negotiable rules

**One transaction per transition.** A state transition and its side-effect record commit in the
same Postgres transaction. If you catch yourself writing "update the DB, then do X" — stop.
That's a dual write and it loses data on crash.

**Async all the way.** `asyncpg` only. Never `psycopg2`, never a blocking driver inside
`async def`, never `time.sleep` / `requests` / sync file I/O on the event loop.

**Idempotent by construction.** A step is done *iff* a row exists in `step_outputs`
(PK: `workflow_id, step_name, kind`). Never add a boolean `completed` flag — the row is the
checkpoint and the idempotency guard in one.

**Never write "exactly-once delivery."** Not in code, comments, docstrings, or docs. We give
exactly-once *effects* via at-least-once execution plus idempotency. Say it that way.

**Types everywhere.** Full type hints on every function. Pydantic v2 for API models
(`model_config`, `field_validator` — not v1 `Config` / `@validator`).

**No new dependencies without asking.** The retry logic, queue, backoff, circuit breaker, and
migrations are built here on purpose — that *is* the project. Never introduce Celery,
SQLAlchemy, Alembic, tenacity, or any orchestration library. Ask first and explain what breaks
without it. The rule targets libraries that would *replace* the parts that are the project;
plumbing that no one would claim credit for (config loading, linting) is not that. Set:
fastapi, uvicorn, asyncpg, pydantic, pydantic-settings, redis, opentelemetry-*, pytest,
pytest-asyncio, pytest-repeat, ruff.

**Every feature ships with a test that proves it.** pytest + pytest-asyncio against a real
Postgres — never mocked, never a container per session: isolation comes from the truncate
fixture, because the crash test runs 20x and the soak runs 1000 workflows. One container on
**5432**, two databases — `sankalp` (dev) and `sankalp_test` (pytest). The fixture asserts it is
connected to `sankalp_test` before truncating; without that check a soak run wipes dev data.
Crash-recovery tests must really kill a worker.

## Layout

```
src/sankalp/
  api/            FastAPI routes, Pydantic request/response models
  engine/         worker loop, dequeue, step execution, retry/compensation
  workflows/      workflow definitions (ordered steps + compensations)
  storage/        asyncpg pool, queries, repositories
  resilience/     backoff, circuit breaker, fencing/resource guard
  observability/  OpenTelemetry spans, metrics, structured logs
migrations/         raw numbered .sql — no ORM, no Alembic
tests/  docker/    docker/ is grafana + prometheus provisioning ONLY
docker-compose.yml  repo root — Postgres (5432, both DBs), Redis
```

## Conventions

- **Migrations** are `NNN_description.sql`, forward-only, never edited once committed. Schema
  changes are new files, applied to both databases. SQL lives in `storage/`, not ORM models.
- **Statuses** are exactly six: `PENDING RUNNING SUCCESS COMPENSATING COMPENSATED
  FAILED_DIRTY`. `FAILED_DIRTY` means compensation failed — alert a human. Don't add a seventh.
- **Claiming work** uses the dequeue query in `docs/spec.md` (`FOR UPDATE SKIP LOCKED`, lease
  expiry, fencing token). Expired leases make rows claimable — don't add a recovery daemon.
- **Before committing a step output**, verify ownership in the same statement (`WHERE id = $1
  AND owner_id = $2 AND fencing_token = $3`). Zero rows means preempted: abort, drop the work.
- **Backoff** is exponential with jitter: `min(2 ** attempt, 60) * (0.5 + random())`. Never
  remove the jitter — it stops a thundering herd when a downstream recovers.
- **Money is integer minor units** — `BIGINT` (`ledger_entries.amount_minor`), `int` in Python.
  Paise, not rupees. Never float, never `Decimal`. Display strings only at the API boundary.
- **Errors** distinguish `RetryableError` from terminal failures. Retryable goes back to the
  queue; terminal goes to `COMPENSATING`. Getting this wrong double-charges or strands money.

## Commands

```bash
docker compose up -d                      # Postgres 5432 (sankalp + sankalp_test), Redis
pytest                                    # runs against sankalp_test
pytest tests/test_crash.py --count=20     # crash recovery, 20x loop
uvicorn sankalp.api.main:app --reload     # dev server, uses sankalp
```
