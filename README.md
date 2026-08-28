# Sankalp

A durable saga orchestrator for money movement. Kill any worker process at any
instant — mid-step, no chance to clean up — and workflows resume from their last
committed checkpoint with **no side effect executing twice**.

Built to survive the failure mode that breaks most "reliable" job systems: a process
that dies between doing the work and recording that it did.

> **Guarantee, stated precisely:** exactly-once *effects*, achieved through
> at-least-once *execution* plus idempotency — never "exactly-once delivery," which is
> impossible across a process boundary. Every guarantee in this repo is backed by a
> test that was observed to fail when its mechanism was removed.

---

## What it is

Sankalp runs multi-step financial workflows (sagas) as durable state machines in
Postgres. Each step is checkpointed; a failed saga is unwound in reverse with
compensating actions; and every state change that needs to reach the outside world is
published reliably through a transactional outbox.

It is deliberately **not** a general-purpose task queue. The design choices —
integer money, append-only ledger, fencing tokens, semantic compensation — are the
choices a payments system makes.

## Core guarantees

- **Crash recovery.** A `SIGKILL`'d worker's in-flight work is resumed by another
  worker once its lease expires. Completed steps replay as checkpoint lookups, not
  re-executions.
- **Exactly-once effects.** A step may *execute* more than once (a crash between the
  side effect and its checkpoint), but its *effect* lands once, because a committed
  `step_outputs` row is both the checkpoint and the idempotency guard.
- **No zombie writes.** Every ownership-scoped write carries a fencing token; a stalled
  worker that wakes up holding a stale token writes zero rows. This is the Redlock
  zombie-writer problem, closed.
- **Sagas unwind cleanly.** On terminal failure, completed steps are compensated in
  reverse order. Compensations are idempotent and crash-safe, checkpointed the same way
  forward steps are.
- **Reliable eventing.** A state change and its event are written in a single Postgres
  transaction — the dual-write problem solved — then drained to a stream at-least-once.

## Three ideas worth reading closely

These are the design decisions the whole system rests on.

### `SKIP LOCKED` is a throughput device, not a safety device

Workers claim runnable rows with `SELECT ... FOR UPDATE SKIP LOCKED`. It's tempting to
think this is what prevents two workers from grabbing the same saga. It isn't.

Under `READ COMMITTED`, even plain `FOR UPDATE` never double-claims: the loser of a race
blocks, re-evaluates the row's predicate after the winner commits, finds it no longer
matches, and moves on. Removing `SKIP LOCKED` is therefore a **performance** regression
(workers convoy behind one row), not a correctness bug. Safety rests on the
`step_outputs` primary key and the fencing token — not on the lock.

### Exactly-once *effects*, not exactly-once *delivery*

A worker can be killed after a step's side effect commits but before its checkpoint
does. On resume, that step runs again — at-least-once execution is unavoidable. What
makes the *effect* exactly-once is that completed steps carry a `step_outputs` row that
the resume replays as a lookup, and side-effecting steps are idempotent by construction.
Durability protects the checkpoints of *completed* steps; the killed step re-runs.

### Fencing tokens close the Redlock gap

A distributed lock (Redlock and friends) does not, by itself, stop a stalled lock-holder
from writing after its lease has been reassigned. Sankalp increments a `fencing_token` on
every claim and guards every ownership-scoped write with
`WHERE id = $1 AND owner_id = $2 AND fencing_token = $3`. A preempted worker's write
matches zero rows and is abandoned. The lock coordinates; the token enforces.

## Architecture

Work is **pulled, not pushed.** Workers claim runnable rows from Postgres; there is no
scheduler or load balancer routing work to them. Adding workers self-balances, with
backpressure for free (a worker only claims what its concurrency budget allows).

```
  ┌──────────┐   POST /workflows      ┌──────────────┐
  │ FastAPI  │ ─────────────────────▶ │  workflows   │ ◀── the queue + saga state
  │  ingest  │   (Idempotency-Key)    │  (Postgres)  │
  └──────────┘                        └──────┬───────┘
                                             │  claim: FOR UPDATE SKIP LOCKED
                                             ▼
                                    ┌───────────────────┐
                                    │     Worker(s)     │  capacity-bounded,
                                    │  ├─ executor      │  lease-renewed
                                    │  └─ drain (opt.)  │
                                    └─────────┬─────────┘
                                              │  outbox INSERT in the SAME
                                              │  transaction as the checkpoint
                                              ▼
  ┌──────────────┐   SKIP LOCKED drain    ┌───────────────┐
  │    outbox    │ ──── XADD ───────────▶ │ Redis Stream  │
  │  (Postgres)  │   then mark published  │  (transport)  │
  └──────────────┘                        └───────────────┘
```

Postgres is the source of truth. The Redis Stream is lossy transport — if it drops
events, the drain republishes from `outbox`.

### Tables

| Table            | Role |
|------------------|------|
| `workflows`      | Saga state machine: status, `owner_id` + `fencing_token`, `lease_expires_at`, `attempt`, `run_after`. |
| `step_outputs`   | Durable checkpoints **and** the idempotency guard, in one table. A step is done iff a row exists. PK includes `kind` (`FORWARD` \| `COMPENSATION`), so forward execution and compensation share one replay rule. |
| `outbox`         | Events written in the same transaction as the step checkpoint; partial index on `published_at IS NULL`. |
| `ledger_entries` | Append-only, double-entry money movement. A `BEFORE UPDATE OR DELETE` trigger raises on any mutation; a UNIQUE `(workflow_id, step_name, account_id, direction)` makes double-posting impossible. |

(`side_effects`, `step_attempts`, `crash_gates` exist only to instrument the crash
tests — they let a test kill a process at a chosen instant and then prove, from
committed rows, that a step ran twice but took effect once. They are not part of the
runtime.)

Migrations are forward-only (`001_core_schema.sql`, `002_crash_gate.sql`,
`003_saga.sql`), checksum-verified, applied to both the dev and test databases.

## Saga lifecycle

Six states. The interesting one is the last.

```mermaid
stateDiagram-v2
    [*] --> PENDING
    PENDING --> RUNNING: claimed
    RUNNING --> SUCCESS: all steps committed
    RUNNING --> PENDING: retryable failure (backoff)
    RUNNING --> COMPENSATING: terminal / unclassified failure
    COMPENSATING --> COMPENSATED: unwound cleanly
    COMPENSATING --> FAILED_DIRTY: a compensation exhausted its retries
    SUCCESS --> [*]
    COMPENSATED --> [*]
    FAILED_DIRTY --> [*]: human intervention required
```

An **unclassified** failure is treated as terminal on purpose: an unrecognised error is
not evidence that re-running the step is safe, and compensation is idempotent by
contract, so the loud path (unwind) is safer than the quiet one (retry into a possible
double-charge).

`FAILED_DIRTY` is deliberate. When a compensation can't be made to succeed, the saga
stops at a clean boundary — everything above the failure unwound, everything below
untouched — and pages a human rather than guessing. Money is in a known-inconsistent
state; the engine has exhausted what it can safely do. Compensations run in reverse
`seq` order precisely because later steps depend on earlier ones, so unwinding out of
order can make the mess worse.

## How correctness is proven

Every safety claim has a test that was **watched failing without its mechanism**, then
restored (files verified byte-identical). A few examples:

- Remove the lease-expiry clause from the claim query → crash recovery times out.
- Skip a completed step's checkpoint → a killed-and-resumed saga double-executes it.
- Move the outbox INSERT out of the checkpoint transaction → a rolled-back step leaves
  an orphan event.
- Invert the drain's publish/mark order → the crash test's stream assertion fails,
  proving the test actually pins XADD-before-mark.

Three real-process crash gates run under repetition (each `SIGKILL`s an actual worker or
drain subprocess, not a cancelled task): forward recovery, compensation recovery, and
drain at-least-once.

## Running it

Requires Docker (Postgres 16 + Redis 7) and Python 3.14.

<!-- TODO(dhanush): confirm these against your Makefile — run `grep -E "^[a-z-]+:" Makefile` -->

```bash
# bring up Postgres + Redis
docker compose up -d

# apply migrations to both databases
make migrate

# run the full test suite
make test

# the crash gates (real SIGKILL, 20 repetitions each)
make test-crash

# run a worker (also drains the outbox by default)
make run-worker            # <!-- TODO: confirm target name -->

# or run the drain as its own process
make drain
```

Config is via `SANKALP_*` environment variables; see `.env.example`.

## Non-goals and known limits

This section is deliberately as prominent as the guarantees. Knowing where a system's
edges are is the point.

- **At-least-once delivery, not exactly-once.** The drain can publish an event twice (a
  crash between XADD and marking it published). Consumers **must** dedupe on the event's
  `id`. There is no exactly-once delivery here because there can't be.
- **Compensation is semantic, not a rollback.** Sagas don't roll back committed
  transactions across services; they run compensating actions (refund, release) that may
  themselves be observable. `FAILED_DIRTY` exists because some inconsistencies can only
  be resolved by a human.
- **Single-region, single-primary Postgres.** Coordination correctness relies on
  linearizable writes to one primary. This buys `SKIP LOCKED`, advisory locks, and
  fencing without a consensus layer — at the cost of a single-region ceiling.
- **No benchmarks.** There are no performance numbers in this README because none have
  been measured. The design has a known throughput ceiling (WAL group-commit on a single
  queue), but a real figure requires a real load test.
- **Ledger immutability is enforced at two layers.** A row-level trigger blocks
  `UPDATE`/`DELETE` on `ledger_entries`. It cannot see `TRUNCATE` — that's not a row
  operation, so it never fires a row trigger — so the second layer is the `sankalp_app`
  grant set (`migrations/004_restricted_role.sql`), which has `SELECT`/`INSERT` only and
  no `TRUNCATE` on the table. The test suite's own truncate fixture still connects as the
  owning `sankalp` role, which is deliberately unrestricted (see
  `tests/test_ledger.py::test_truncate_is_deliberately_not_blocked`).
- **The `sankalp_app` password in `004_restricted_role.sql` is a dev-only literal**,
  matching the `sankalp`/`sankalp` convention already in `docker-compose.yml`. Production
  would inject it via environment or a secret manager, not commit it in migration SQL.

## Roadmap

Phases 1 and 2 (above) are complete, tested, and the shippable core. Planned:

- **Resilience (Phase 3):** Redis token-bucket rate limiting and adaptive concurrency.
  (The restricted `sankalp_app` database role is done — see "Ledger immutability is
  enforced at two layers" above.)
- **Observability (Phase 4):** OpenTelemetry tracing threaded through the outbox's
  `trace_context` column, and Prometheus metrics for queue depth, lease churn, and drain
  lag.

## Build notes

`docs/build-log-day1.md`, `docs/build-log-phase1.md`, and `docs/build-log-phase2.md`
record the design decisions and the bugs found while building — including a real
concurrency bug the compensator exposed, where a worker could re-claim its own
in-progress unwind and run compensations concurrently (a double-refund with no crash
involved), fixed at the claim layer.
