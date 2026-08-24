# Sankalp — Schema & Execution Flows (All 4 Phases)

A durable saga orchestrator for money movement, built on FastAPI + PostgreSQL + Redis.

**Design invariant that governs everything below:** every state transition and its side-effect record land in the *same* Postgres transaction. If you ever find yourself writing "update the DB, then do X," stop — that's a dual write and it will lose data on crash.

---

## Global State Machine

```
                      ┌──────────────────────────────┐
                      │                              │
   PENDING ──────► RUNNING ──────► SUCCESS           │
                      │                              │
                      │ step fails (retryable)       │
                      └──────► (lease expires) ──────┘
                      │
                      │ step fails (terminal)
                      ▼
                 COMPENSATING ──────► COMPENSATED
                      │
                      │ compensation exhausted
                      ▼
                 FAILED_DIRTY   ← needs human intervention; alert on this
```

Six statuses, no more. `FAILED_DIRTY` is deliberate: it's the state where you tried to undo and couldn't. Real payment systems have this state and page a human. Having it in your design shows you've thought past the happy path.

---

# PHASE 1 — Core Durable Engine

**Goal:** kill a worker mid-workflow; another worker resumes from the exact step it stopped at, and no completed step re-executes its side effect.

## Schema

```sql
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TYPE workflow_status AS ENUM (
    'PENDING', 'RUNNING', 'SUCCESS',
    'COMPENSATING', 'COMPENSATED', 'FAILED_DIRTY'
);

CREATE TABLE workflows (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_type     TEXT NOT NULL,              -- 'payment_transfer', etc.
    idempotency_key   TEXT NOT NULL,
    status            workflow_status NOT NULL DEFAULT 'PENDING',

    input             JSONB NOT NULL,
    output            JSONB,
    error             TEXT,

    -- execution position
    current_step      TEXT,
    attempt           INT NOT NULL DEFAULT 0,
    max_attempts      INT NOT NULL DEFAULT 5,

    -- lease / ownership (this is what makes crash recovery safe)
    owner_id          TEXT,
    lease_expires_at  TIMESTAMPTZ,
    fencing_token     BIGINT NOT NULL DEFAULT 0,

    -- scheduling (retry backoff writes into this)
    run_after         TIMESTAMPTZ NOT NULL DEFAULT now(),

    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_idempotency UNIQUE (workflow_type, idempotency_key)
);

-- The dequeue index. Partial, so it only covers rows that are actually claimable.
CREATE INDEX idx_workflows_claimable
    ON workflows (run_after, id)
    WHERE status IN ('PENDING', 'RUNNING', 'COMPENSATING');

CREATE TABLE step_outputs (
    workflow_id   UUID NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    step_name     TEXT NOT NULL,
    seq           INT  NOT NULL,              -- order within the workflow
    kind          TEXT NOT NULL DEFAULT 'FORWARD'
                       CHECK (kind IN ('FORWARD', 'COMPENSATION')),
    output        JSONB,
    started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (workflow_id, step_name, kind)
);
```

**Why `PRIMARY KEY (workflow_id, step_name, kind)` is the whole trick:** it's the checkpoint *and* the idempotency guard in one. A step is "already done" if and only if a row exists. Replay logic is a lookup, not a flag you have to remember to set.

**Why `fencing_token` lives on the workflow row:** every time a worker claims a workflow it increments. A worker that stalled (GC pause, network partition) and wakes up holding token 7 tries to write while the current owner holds token 8 — and Postgres rejects it. Phase 3 wires this into the resource guard; Phase 1 just needs the counter to exist and increment.

### Gate instrumentation (`migrations/002_crash_gate.sql`)

Three tables exist only so the Phase 1 Gate below can be *measured*. A SIGKILLed process takes its in-memory call counters with it, so "did this step run twice?" has to be answered by rows the dead process already committed.

```sql
side_effects  (id BIGSERIAL PK, workflow_id, step_name, executed_at)
step_attempts (id BIGSERIAL PK, workflow_id, step_name, owner_id, pid, attempted_at)
crash_gates   (workflow_id, step_name, released_at, PK (workflow_id, step_name))
```

`side_effects` counts effects that actually **committed**; `step_attempts` counts every time a step **started**, and carries the pid so the test can kill the process that is genuinely mid-step. Read together they state the guarantee as two integers — *attempted twice, took effect once*. `crash_gates` lets the test decide when a blocked step may finish, so the kill is aimed rather than timed against a sleep.

`side_effects` must never gain a unique constraint and the demo steps must never write it with `ON CONFLICT DO NOTHING`. Idempotency by construction is Phase 2; if a duplicate insert were swallowed here, "exactly one row" would hold whether or not crash recovery worked and the gate would assert nothing.

## The Dequeue Query

```sql
UPDATE workflows w
-- The ::workflow_status cast is required, not decoration. Both CASE branches are
-- untyped literals, so the CASE resolves to text, and Postgres refuses to assign
-- text to an enum column. Drop it and nothing claims at all.
SET status           = (CASE WHEN w.status = 'COMPENSATING'
                             THEN 'COMPENSATING' ELSE 'RUNNING' END)::workflow_status,
    owner_id         = $1,
    lease_expires_at = now() + ($2 || ' seconds')::interval,
    fencing_token    = w.fencing_token + 1,
    attempt          = w.attempt + 1,
    updated_at       = now()
FROM (
    SELECT id
    FROM workflows
    WHERE run_after <= now()
      AND (
            status IN ('PENDING', 'COMPENSATING')
            OR (status = 'RUNNING' AND lease_expires_at < now())
          )
    ORDER BY run_after
    FOR UPDATE SKIP LOCKED
    LIMIT $3
) AS claimed
WHERE w.id = claimed.id
RETURNING w.*;
```

Three things doing real work here:

- `FOR UPDATE SKIP LOCKED` — N workers poll simultaneously and never collide. Without it they all grab row 1 and serialize.
- `status = 'RUNNING' AND lease_expires_at < now()` — this *is* the crash recovery. There's no separate recovery daemon; a dead worker's rows simply become claimable again. Fewer moving parts, fewer bugs.
- `ORDER BY run_after` inside the subquery — the scan is bounded by the partial index.

## Execution Flow

```
worker loop:
  1. claim batch (query above)  → returns workflow rows + fencing_token
  2. for each workflow:
       spawn asyncio task, tracked in a bounded set
  3. per workflow:
       load definition by workflow_type → ordered [step_1..step_n]
       load existing step_outputs (one query, build a dict)

       for step in steps:
           if step.name in completed:
               ctx[step.name] = completed[step.name].output   # SKIP — replay
               continue

           renew_lease_if_needed()
           result = await step.fn(ctx, input)

           BEGIN
             INSERT INTO step_outputs (workflow_id, step_name, seq, output)
             UPDATE workflows SET current_step = step.name, updated_at = now()
           COMMIT

           ctx[step.name] = result

       BEGIN
         UPDATE workflows SET status='SUCCESS', output=..., owner_id=NULL
       COMMIT
```

### Retry vs. compensate — the branch that matters

```
on exception from step.fn:
    if RetryableError and attempt < max_attempts:
        UPDATE workflows
        SET status = 'PENDING',
            owner_id = NULL,
            run_after = now() + backoff(attempt),   -- exponential + jitter
            error = str(e)
        -- workflow returns to the queue; completed steps stay checkpointed
    else:
        UPDATE workflows SET status='COMPENSATING', error=str(e)
        -- Phase 2 picks it up
```

Backoff: `min(2 ** attempt, 60) * (0.5 + random())` seconds. The jitter matters — without it, everything that failed during a downstream outage retries in lockstep and knocks the downstream over again the moment it recovers.

### Lease renewal

A step that takes longer than the lease duration will have its workflow stolen mid-execution. Two defenses, use both:

1. Background task renews the lease every `lease_duration / 3` while a step runs.
2. Before committing a step output, verify you still own it:

```sql
UPDATE workflows
SET current_step = $2, updated_at = now()
WHERE id = $1 AND owner_id = $3 AND fencing_token = $4;
-- 0 rows affected → you were preempted. Abort the transaction, drop the work.
```

## Phase 1 API

| Method | Path | Behavior |
|---|---|---|
| `POST` | `/workflows` | Submit. `Idempotency-Key` header required. Returns 201 + id, or 200 + existing state on duplicate. |
| `GET` | `/workflows/{id}` | Status, current step, completed steps, error. |
| `POST` | `/workflows/{id}/cancel` | Sets `COMPENSATING`. |

Submit handler, in full:

```
BEGIN
  INSERT INTO workflows (workflow_type, idempotency_key, input, status)
  VALUES (...)
  ON CONFLICT (workflow_type, idempotency_key) DO NOTHING
  RETURNING id
COMMIT

if no row returned:
    SELECT existing row by (workflow_type, idempotency_key) → return 200
```

Note `DO NOTHING` + re-select rather than `DO UPDATE`. A duplicate submit must never mutate an in-flight workflow.

## Phase 1 Gate

Implemented as `tests/test_crash.py` (`make test-crash`), against the `demo_crash` workflow in `src/sankalp/workflows/demo.py`.

Launch three real `python -m sankalp.engine.worker` **OS processes**, submit a workflow, and `SIGKILL` the one running step 2. SIGKILL specifically, not SIGTERM and not cancellation: the point is a process with zero chance to clean up. SIGTERM runs the drain and *lets in-flight work finish*, and `task.cancel()` unwinds through the executor's `except asyncio.CancelledError` — a worker that gets to run its handlers is not what the guarantee is about.

The kill is aimed, not timed. Step 2 commits a `step_attempts` row carrying its own pid and then blocks, holding an **uncommitted** `side_effects` INSERT open. The test waits for that row and kills that pid, so the crash lands inside step 2 by construction and the killed transaction rolls back. (This replaces the original sketch of "sleeps 10 seconds, `docker kill` at second 5" — a sleep makes every repetition cost its own length and makes the kill a guess. One variant still blocks on a real ~1s sleep, so the gate also covers being killed inside ordinary work.)

Assert:
- Another **process** resumes step 2 within `lease_duration` — a different pid and a different `owner_id`. An expired lease is the only recovery mechanism, so that is the crash-recovery latency bound.
- `step_attempts` reads exactly `{step 1: 1, step 2: 2, step 3: 1}` — the killed step was attempted twice, and no other step was re-attempted. **Without this the test can pass by killing a worker that never reached step 2**: the workflow would still succeed and every side-effect count would still read 1, having proven nothing about resuming mid-step.
- `side_effects` reads exactly 1 per step. Step 1 did not re-execute; step 2's killed attempt left nothing behind.
- Exactly one `FORWARD` `step_outputs` row per step, and `workflows.attempt = 2` (the signature of one recovery re-claim).
- Workflow reaches `SUCCESS`.

Run it hundreds of times: `pytest tests/test_crash.py --count=20`.

**The gate has been observed to fail.** Two mechanisms were removed one at a time and the test went red for the right reason each time:
- Delete `OR (status = 'RUNNING' AND lease_expires_at < now())` from the dequeue query → both variants time out waiting for another worker to resume step 2. Nothing recovers a dead worker's rows.
- Skip step 1's `commit_step_output` → step 1 shows 2 attempts and **2 `side_effects` rows**. The checkpoint is what makes the resume replay a completed step instead of re-executing it.

A gate that has never been seen to fail is not a gate. Re-run both proofs after any change to the claim query, the checkpoint write, or the lease.

---

# PHASE 2 — Sagas, Outbox, Ledger

**Goal:** a workflow that fails at step 3 unwinds steps 2 and 1, and a reconciliation query proves the ledger nets to zero.

## Schema Additions

```sql
CREATE TABLE outbox (
    id             BIGSERIAL PRIMARY KEY,
    workflow_id    UUID NOT NULL REFERENCES workflows(id),
    event_type     TEXT NOT NULL,
    payload        JSONB NOT NULL,
    trace_context  JSONB,                      -- W3C traceparent; Phase 4 reads this
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at   TIMESTAMPTZ,
    attempts       INT NOT NULL DEFAULT 0
);

-- Partial index: shrinks to near-zero once the drain keeps up.
CREATE INDEX idx_outbox_unpublished
    ON outbox (id) WHERE published_at IS NULL;

CREATE TABLE ledger_entries (
    id             BIGSERIAL PRIMARY KEY,
    transfer_id    UUID NOT NULL,              -- groups the debit/credit pair
    workflow_id    UUID NOT NULL REFERENCES workflows(id),
    step_name      TEXT NOT NULL,
    account_id     TEXT NOT NULL,
    direction      TEXT NOT NULL CHECK (direction IN ('DEBIT','CREDIT')),
    amount_minor   BIGINT NOT NULL CHECK (amount_minor > 0),
    currency       CHAR(3) NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- The idempotency guard: a given step can post a given entry exactly once.
    CONSTRAINT uq_ledger_entry
        UNIQUE (workflow_id, step_name, account_id, direction)
);

CREATE INDEX idx_ledger_account ON ledger_entries (account_id, created_at);
```

**Ledger rules, non-negotiable:** append-only. No `UPDATE`, no `DELETE`, ever. A refund is a *new pair of entries in the opposite direction*, not a mutation. Balance is a `SUM`, optionally materialized into a snapshot table later. This is how real ledgers work and it makes the reconciliation invariant checkable at any instant.

## Compensation Model

Compensations reuse `step_outputs` via the `kind` column — no new table. A compensation is "already done" by exactly the same rule as a forward step: a row exists.

```
COMPENSATING flow:
  load completed FORWARD steps, ordered by seq DESC
  load completed COMPENSATION steps into a set

  for step in reversed(completed_forward_steps):
      if step.name in completed_compensations:  continue
      if step has no compensation defined:      continue   # read-only step

      renew_lease_if_needed()
      await step.compensate(ctx, forward_output)

      BEGIN
        INSERT INTO step_outputs (workflow_id, step_name, kind='COMPENSATION', ...)
      COMMIT

  UPDATE workflows SET status='COMPENSATED', owner_id=NULL
```

Two rules for compensation functions:

1. **They must be idempotent themselves.** You may crash after running the compensation but before committing the checkpoint. On replay it runs again. `refund_if_not_already_refunded`, not `refund`.
2. **They must not fail permanently.** If a compensation exhausts its retries, the workflow goes to `FAILED_DIRTY` and you alert. Money is in an inconsistent state and only a human can resolve it. Model this honestly rather than pretending compensations always succeed.

## The Outbox

Events are written **in the same transaction as the step output**:

```
BEGIN
  INSERT INTO step_outputs (...)
  INSERT INTO outbox (workflow_id, event_type, payload, trace_context)
  UPDATE workflows SET current_step = ...
COMMIT
```

That single transaction is the entire answer to the dual-write problem. Either both the state change and the event exist, or neither does.

### Drain loop

```sql
-- Claim
SELECT id, event_type, payload, trace_context
FROM outbox
WHERE published_at IS NULL
ORDER BY id
FOR UPDATE SKIP LOCKED
LIMIT 100;

-- publish to Redis Stream (XADD), then:
UPDATE outbox SET published_at = now() WHERE id = ANY($1);
```

**This is at-least-once, and you should say so plainly.** Crash between XADD and the UPDATE → republish. Consumers must dedupe on `outbox.id`. Anyone who claims exactly-once *delivery* here is wrong; you have exactly-once *effects*, which is the achievable and correct guarantee.

Scale-up path worth documenting in your README: replace polling with Postgres logical replication (`wal_level=logical`, publication on `outbox`, `pgoutput`) — this is what Debezium does. Note the operational hazard too: an unconsumed replication slot pins WAL forever and will fill the disk and take down the primary. Cap it with `max_slot_wal_keep_size`.

## Reconciliation

The invariant that makes your Phase 2 gate a *proof* rather than an assertion:

```sql
-- Every transfer must net to zero.
SELECT transfer_id,
       SUM(CASE WHEN direction='DEBIT'  THEN amount_minor ELSE 0 END) AS debits,
       SUM(CASE WHEN direction='CREDIT' THEN amount_minor ELSE 0 END) AS credits
FROM ledger_entries
GROUP BY transfer_id
HAVING SUM(CASE WHEN direction='DEBIT'  THEN amount_minor ELSE 0 END)
    <> SUM(CASE WHEN direction='CREDIT' THEN amount_minor ELSE 0 END);
-- Must return 0 rows. Always. Run it in CI after every chaos test.
```

## Phase 2 Gate

Run 1,000 workflows with a mock gateway configured to fail ~30% of the time at step 3, while randomly killing workers. Then assert:
- Reconciliation query returns 0 rows.
- No workflow is stuck in `RUNNING` (all terminal).
- `COUNT(*) FROM outbox WHERE published_at IS NULL` drains to 0.
- Mock gateway's per-idempotency-key call log shows no duplicated *effects*.

---

# PHASE 3 — Resilience Layer

**Goal:** drive 5× measured capacity at the system and watch P99 stay bounded instead of collapsing, while proving the distributed lock is safe under a simulated stalled-worker scenario.

## Schema Additions

```sql
-- Guarded mutable resources. Fencing tokens are enforced HERE, not in Redis.
CREATE TABLE resources (
    resource_id         TEXT PRIMARY KEY,       -- 'wallet:U123', 'sku:ABC'
    balance_minor       BIGINT NOT NULL DEFAULT 0,
    version             BIGINT NOT NULL DEFAULT 0,
    last_fencing_token  BIGINT NOT NULL DEFAULT 0,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT non_negative CHECK (balance_minor >= 0)
);

CREATE TABLE workers (
    id              TEXT PRIMARY KEY,
    last_heartbeat  TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    in_flight       INT NOT NULL DEFAULT 0,
    concurrency_limit INT NOT NULL DEFAULT 32
);
```

Rate limiter state lives in Redis only — no Postgres table. Deliberate: it's ephemeral, high-churn, and losing it on restart is harmless (you fail open).

## Restricted Application Role

**Status: not built.** Everything — API, workers, migration runner, pytest — connects as `sankalp`, which is `POSTGRES_USER` in `docker-compose.yml`: a superuser, and the owner of both databases. `migrations/003_saga.sql` points its TRUNCATE note here, so this is the task it points at.

The consequence to be honest about: `ledger_entries` is append-only against `UPDATE` and `DELETE` — a statement-level trigger enforces that, and it has no bypass — but **TRUNCATE is not covered and currently nothing else covers it either.** The trigger deliberately excludes TRUNCATE so `tests/conftest.py` can empty the table between tests. A superuser bypasses privilege checks outright and a table's owner may TRUNCATE regardless of what has been revoked, so today's single role has no control on it at all.

The fix is a grant, not another trigger:

```sql
CREATE ROLE sankalp_app LOGIN PASSWORD '...';   -- NOT a superuser, NOT the table owner
GRANT CONNECT ON DATABASE sankalp TO sankalp_app;
GRANT USAGE ON SCHEMA public TO sankalp_app;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO sankalp_app;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO sankalp_app;

-- The ledger is the exception: append and read, nothing else.
REVOKE TRUNCATE, DELETE, UPDATE ON ledger_entries FROM sankalp_app;
```

Both conditions on the role are load-bearing. A `REVOKE` against a superuser is ignored, and a `REVOKE` against the table's owner is ignored — so a `sankalp_app` that is either one is theatre, and worse than nothing because it reads like a control.

Scope beyond the SQL: the API and workers move to a `sankalp_app` DSN while migrations keep the owning role (DDL needs it), which means two connection strings in `config.py` rather than one. The test fixture keeps the owning role, since its per-test `TRUNCATE` is the suite's isolation mechanism. That split is why this is its own change and not a line in a migration.

## Fenced Resource Mutation

The lock is a Redis optimization. The **correctness** is this UPDATE:

```sql
UPDATE resources
SET balance_minor      = balance_minor - $2,
    version            = version + 1,
    last_fencing_token = $3,
    updated_at         = now()
WHERE resource_id = $1
  AND last_fencing_token < $3       -- reject stale token holders
  AND balance_minor >= $2;          -- reject overdraft
-- 0 rows → either you're a zombie holder, or insufficient funds. Distinguish
--          by re-selecting; raise the right error for each.
```

This is the answer to "prove your distributed lock is safe." Redlock alone isn't — a worker can hold a valid-looking lock while stalled past its TTL (GC pause, VM freeze, network partition) and another worker acquires it legitimately. Both believe they hold it. The monotonic fencing token, checked by the storage layer, is what makes the second writer win and the zombie lose. Kleppmann's critique of Redlock is exactly that it "lacks a facility for generating fencing tokens."

## Token Bucket (Redis Lua)

```lua
-- KEYS[1]=bucket key  ARGV: capacity, refill_per_sec, now_ms, cost
local capacity  = tonumber(ARGV[1])
local refill    = tonumber(ARGV[2])
local now_ms    = tonumber(ARGV[3])
local cost      = tonumber(ARGV[4])

local b       = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens  = tonumber(b[1])
local last_ts = tonumber(b[2])

if tokens == nil then tokens = capacity; last_ts = now_ms end

local elapsed = math.max(0, now_ms - last_ts) / 1000.0
tokens = math.min(capacity, tokens + elapsed * refill)

local allowed = 0
if tokens >= cost then
    tokens  = tokens - cost
    allowed = 1
end

redis.call('HMSET', KEYS[1], 'tokens', tokens, 'ts', now_ms)
redis.call('PEXPIRE', KEYS[1], math.ceil((capacity / refill) * 1000 * 2))

return { allowed, tokens }
```

Atomic refill-and-consume in one round trip, two numbers of state per key regardless of traffic volume. **Fail open** if Redis is unreachable — a dead rate limiter must not become a dead API.

## Adaptive Concurrency (Gradient / TCP-Vegas style)

Static RPS caps are wrong because your real capacity changes with payload size, DB load, and downstream latency. Measure instead:

```
every window (e.g. 1s):
    rtt_min = min RTT ever observed (decayed slowly)
    rtt_avg = avg RTT this window

    gradient   = clamp(rtt_min / rtt_avg, 0.5, 1.0)
    new_limit  = limit * gradient + sqrt(limit)     # queue-size allowance
    limit      = clamp(new_limit, min_limit, max_limit)
```

When latency rises above baseline, the gradient shrinks the limit *before* queues explode. Enforce with `asyncio.Semaphore(limit)`; when it can't be acquired immediately, shed:

| Condition | Response |
|---|---|
| Rate limit exceeded | `429` + `Retry-After` |
| Concurrency limit exceeded, low-criticality | `503` immediately |
| Concurrency limit exceeded, high-criticality | brief bounded wait, then `503` |

Criticality-based shedding (drop `bulk_settlement` before `user_payment`) is Google-SRE practice and worth the extra 20 lines — it turns "I added a semaphore" into "I reasoned about which traffic deserves to survive."

## Hot Key Splitting

A single hot counter serializes every writer on one row. Split it:

```
write:  shard = hash(request_id) % N        → UPDATE resources
                                              WHERE resource_id = 'sku:ABC#' || shard
read:   SELECT SUM(balance_minor) FROM resources
        WHERE resource_id LIKE 'sku:ABC#%'
```

N=16 turns one hot row into 16 warm rows. The trade-off to name out loud: reads get more expensive and you lose a single-row atomic check, so "is there stock left" becomes approximate under contention — acceptable for inventory display, not for the final decrement.

## Cache Stampede (XFetch)

When a hot key expires, every concurrent reader misses simultaneously and stampedes the DB. Probabilistic early recompute, from Vattani et al. (VLDB 2015):

```
value, delta, expiry = cache_get(key)     # delta = last recompute duration
if value is None or (now - delta * beta * log(random())) >= expiry:
    recompute_and_set(key)
return value
```

Each reader independently rolls dice, weighted so the probability of early refresh rises as expiry approaches. No locks, no coordination, and the herd spreads itself out. `beta > 1` refreshes earlier/more eagerly.

## Phase 3 Gate

1. **Overload:** ramp to 5× measured capacity. P99 stays within target; excess returns 429/503. Show the Grafana panel where concurrency limit drops as latency climbs.
2. **Zombie writer:** claim a workflow (token 7), `SIGSTOP` the worker, let the lease expire, let another worker claim (token 8) and commit, then `SIGCONT` the first. Assert its write is rejected and the balance is correct.
3. **Redis down:** kill Redis, confirm the API stays up (fails open) and workflows still complete.

---

# PHASE 4 — Observability & Proof

**Goal:** produce evidence. Anyone can claim fault tolerance; you're going to show a trace.

## Schema Additions

Minimal — the point is linkage, not storage:

```sql
ALTER TABLE workflows
    ADD COLUMN trace_id TEXT,          -- root trace for the whole workflow
    ADD COLUMN span_id  TEXT;

CREATE INDEX idx_workflows_trace ON workflows (trace_id);
```

Everything else goes to OTLP.

## Trace Context Propagation

Automatic propagation works within a request. It does **not** cross the outbox — that's a durable, asynchronous handoff, and the context must travel as data. This is the detail most implementations miss:

```
# Producer side (inside the step transaction)
carrier = {}
TraceContextTextMapPropagator().inject(carrier)
INSERT INTO outbox (..., trace_context) VALUES (..., $carrier)

# Consumer side (drain loop)
ctx = TraceContextTextMapPropagator().extract(row.trace_context)
with tracer.start_as_current_span("outbox.publish", context=ctx, links=[...]):
    ...
```

Same for the workflow itself: store `trace_id` on submit, and when a *different* worker resumes after a crash, start its span with a **link** to the original trace rather than as a child. Linked spans are semantically right for "same logical operation, discontinuous execution," and it renders beautifully — you can literally see the crash and the pickup.

## Span Structure

```
POST /workflows                          (auto, FastAPI instrumentor)
└─ workflow.submit
   ├─ ratelimit.check                    (Redis)
   └─ db.insert workflow+outbox          (auto, asyncpg)

workflow.execute {type, id, attempt}     (worker; linked to submit trace)
├─ step.debit_wallet
│  ├─ resource.fenced_update             (fencing_token as attribute)
│  └─ db.commit checkpoint
├─ step.call_gateway
│  └─ HTTP POST /charge                  (auto, httpx)
└─ step.write_ledger

workflow.compensate                      (linked to execute)
└─ compensation.debit_wallet
```

Keep span **names** low-cardinality (`step.debit_wallet`, never `step.debit_wallet.U12345`). IDs go in attributes.

## Metrics

**RED** for request paths, **USE** for resources:

```
# Rate / Errors / Duration
sankalp_workflows_submitted_total{workflow_type, result}
sankalp_workflow_duration_seconds{workflow_type, status}   # histogram
sankalp_steps_executed_total{step_name, result}
sankalp_compensations_total{step_name, result}
sankalp_http_requests_total{route, status}

# Utilization / Saturation / Errors
sankalp_queue_depth{status}              # gauge: PENDING backlog
sankalp_outbox_lag_seconds               # gauge: now() - oldest unpublished
sankalp_worker_in_flight{worker_id}
sankalp_concurrency_limit{worker_id}     # watch this move under load
sankalp_db_pool_in_use / _size
sankalp_ratelimit_rejected_total{reason}
sankalp_load_shed_total{criticality}

# The correctness metrics — these should be flat zero
sankalp_workflows_failed_dirty_total
sankalp_reconciliation_mismatch_total
sankalp_fencing_rejections_total         # >0 means it's working, not broken
```

Enable **exemplars** on the duration histogram so a slow bucket in Grafana links straight to the trace that caused it.

## Dashboards

| Panel | Shows |
|---|---|
| Workflow throughput + status breakdown | the headline number |
| P50 / P95 / P99 duration, with exemplars | latency behavior |
| Queue depth + outbox lag | saturation — the leading indicator |
| Concurrency limit vs. RTT | adaptive limiter reacting in real time |
| Load shed / 429 rate by criticality | graceful degradation working |
| `FAILED_DIRTY` count | should be zero; alert if not |

## Load Testing Methodology

Two rules, and they matter more than the numbers themselves:

1. **Constant arrival rate, not constant concurrency.** k6 `constant-arrival-rate` or Locust with correction. Closed-loop generators suffer coordinated omission: when the server stalls, the generator stops sending, so the worst samples are never recorded and P99 looks *best* exactly when the system is worst.
2. **Load generator on a separate machine.** Same-box generation inflates results and steals CPU from the thing you're measuring. A second cheap VPS, or GitHub Actions.

Aggregate with HDR histograms. Never average percentiles across windows or hosts — that's mathematically meaningless and a sharp reviewer will call it.

## Chaos Suite

Script these as one command (`make chaos`) so results are reproducible:

| Fault | Tool | Assert |
|---|---|---|
| Worker killed mid-step | `docker kill` | resumes, no duplicate effects |
| DB latency +500ms | Toxiproxy | concurrency limit shrinks, no cascade |
| DB connection cut | Toxiproxy | workflows retry, none lost |
| Redis down | `docker stop` | API stays up (fails open) |
| Network partition worker↔DB | Toxiproxy | lease expires, another worker takes over |
| Gateway 500s / timeouts | mock config | retries then compensates cleanly |
| Zombie worker | SIGSTOP/SIGCONT | fenced write rejected |

After every run: reconciliation query returns 0 rows, no workflow stuck non-terminal, outbox fully drained.

## Phase 4 Gate

A README containing: architecture diagram, a Grafana screenshot spanning an injected-chaos window, a trace screenshot showing a crash-and-resume with linked spans, and a results table of reproducible numbers with the hardware stated.

---

# Cross-Phase Reference

## Table Summary

| Table | Phase | Purpose |
|---|---|---|
| `workflows` | 1 | State, lease, fencing token, scheduling |
| `step_outputs` | 1 | Checkpoints — the idempotency guard |
| `outbox` | 2 | Events, atomic with state changes |
| `ledger_entries` | 2 | Append-only double-entry money record |
| `resources` | 3 | Fenced mutable state (balances, stock) |
| `workers` | 3 | Heartbeats, per-worker concurrency |

## Operational Notes

- **Autovacuum:** `workflows` and `outbox` are high-churn UPDATE/DELETE tables and will bloat. Tune aggressively (`autovacuum_vacuum_scale_factor = 0.02` on those tables) and consider partitioning `outbox` by day with a drop-old-partitions job.
- **Connection pool sizing:** size to Postgres cores × ~2–4, *not* to HTTP concurrency. A pool of ~16 serves thousands of concurrent async requests. Oversized pools make Postgres slower, not faster.
- **Never mix sync and async drivers.** An `async def` route calling a blocking driver stalls the entire event loop and is worse than a plain `def` route.
- **The single-queue ceiling.** Even with `SKIP LOCKED`, throughput on one queue table caps out on WAL group-commit flushing. When you hit it, partition the queue by hash and have each worker poll a subset. Discovering this yourself and fixing it is a far better story than reading about it.