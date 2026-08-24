# Sankalp — Day 1 Build Log

Everything set up on day one, and the reasoning behind each decision. Written so that
in an interview you can explain *why*, not just *what*.

Two commits on `main`:
- `295ddcf` — scaffold: config, migration runner, core schema, docker compose
- `a81981a` — docs: design rationale in core schema

---

## 1. Project definition

**Sankalp** — a durable saga orchestrator for money movement. FastAPI + PostgreSQL 16 +
Redis 7 on Python 3.14.

The guarantee, stated in one sentence: *kill any process at any instant, and workflows
resume from the last completed step with no step's side effect executing twice.*

That sentence is the project. Every architectural decision below either delivers it or
protects it.

### Why this domain
Payments is the one place where "eventually consistent, probably fine" isn't acceptable.
It forces genuine distributed-systems work — idempotency, compensating transactions,
crash recovery, distributed locking — without needing a Kafka cluster or a from-scratch
Raft implementation to justify itself. It's also the domain most legible to the Indian
high-scale market, where payments and fintech dominate the engineering culture.

### Why Postgres as the coordination layer, not etcd/ZooKeeper/Raft
A single-primary Postgres gives linearizable writes, ACID transactions,
`SELECT ... FOR UPDATE SKIP LOCKED` for contention-free work distribution, and advisory
locks for leader election. That's consensus-grade correctness for a single region without
implementing Paxos.

**The interview challenge and the answer:** "That's a single point of failure and it won't
scale writes past one primary." Correct on both counts. The mitigations are Postgres HA
(streaming replication + failover) for the first, and queue partitioning for the second —
and the honest position is that the write ceiling exists and you know roughly where it is.
A known, measured ceiling is a stronger answer than an unmeasured system that claims not
to have one.

---

## 2. Environment — and why the platform mattered

### Migrated Windows → WSL2 Ubuntu
Started scaffolding on native Windows, then moved everything to WSL2 before writing engine
code.

**Why this wasn't optional.** The Phase 1 gate kills a worker mid-step. The Phase 3
zombie-writer test needs `SIGSTOP`/`SIGCONT` to freeze a process and wake it after its
lease expires. Windows has no real POSIX signals — `signal.SIGSTOP` doesn't exist in
Python there, and asyncio's `SIGTERM` handling differs. Graceful shutdown and the two most
important tests in the project all depend on this.

You can approximate a crash with `asyncio.Task.cancel()`, but that cancels a coroutine,
not a process. "I simulated the crash in-process" is a visibly weaker claim than "I killed
the container."

### Repo on the Linux filesystem, not `/mnt/c`
Cross-boundary filesystem I/O in WSL is roughly an order of magnitude slower. The soak
test does thousands of DB round trips; that difference compounds into minutes per run.

### Python 3.14, not 3.12
Ubuntu "resolute" ships 3.14 by default. The risk was `asyncpg` and `pydantic-core` —
both ship compiled extensions, and prebuilt wheels for a brand-new Python sometimes lag,
which would have forced a source build (asyncpg needs a C toolchain, pydantic-core needs
Rust). Verified before committing to it: all packages installed cleanly from wheels.

Then fixed the version pins that would have blocked it: `requires-python = ">=3.12,<3.14"`
excluded 3.14 entirely, and ruff's `target-version` was still `py312`.

### Docker via Desktop's WSL integration
`docker ps` initially failed with a socket permission error — the user wasn't in the
`docker` group. Fixed with `usermod -aG docker`, which requires a full `wsl --shutdown`
to take effect since group membership is only read at login.

---

## 3. Git hygiene — three fixes that would have caused real problems

### `.gitattributes` with `* text=auto eol=lf`
Files copied from Windows carry CRLF line endings. On Linux that produces
`bad interpreter: /bin/bash^M` on scripts and silently failing Makefile targets.

**The project-specific reason it matters more here:** the migration runner checksums `.sql`
files to detect edits after they've been applied. A line-ending flip changes the checksum
and would surface as a false "migration was modified after it was applied" error — a
confusing failure with no obvious cause.

### File modes normalized to 644
NTFS → ext4 carried every file across as mode 755. Git records the executable bit, so the
first commit would have marked `.md`, `.sql`, and `.toml` files as executable. Verified
first that no file had a shebang, so nothing legitimately needed 755.

### Wrong-repo incident
Staged files in `C:\Users\Dhanush\sankalp-engine` (the abandoned Windows copy) instead of
`~/sankalp-engine`. Caught it from the CRLF warnings Git emitted — the LF files were about
to be converted back. Reset the staging and renamed the Windows directory.

Tell-apart rule: WSL shows `dhanush@LAPTOP-...:~/sankalp-engine$`, Windows shows `PS C:\`.

---

## 4. `CLAUDE.md` — project memory

Under 80 lines, instructions rather than documentation, with the *reason* attached to each
rule (a bare "don't do X" gets rationalized around).

### The eight non-negotiables

| Rule | Why |
|---|---|
| One transaction per transition | State change + side-effect record commit together, or it's a dual write that loses data on crash |
| Async all the way (`asyncpg` only) | A blocking driver inside `async def` stalls the entire event loop |
| Idempotent by construction | A step is done *iff* a row exists in `step_outputs` — never a boolean flag |
| Never write "exactly-once delivery" | We provide exactly-once *effects* via at-least-once execution + idempotency. Conflating these loses interviews |
| Types everywhere, Pydantic v2 | — |
| No new dependencies without asking | Prevents importing away the parts that *are* the project |
| Every feature ships with a test | Against real Postgres, never mocked |
| Money is integer minor units | `BIGINT` paise, never float, never `Decimal` |

### Two rules that came out of live decisions

**The money rule.** Claude Code proposed "NUMERIC/Decimal, never float." Right instinct,
wrong conclusion — the spec defines `amount_minor` as `BIGINT`, i.e. integer paise.
Integers are *stronger* than NUMERIC: no decimal to mishandle, no rounding mode to get
wrong. It's what Stripe and most real payment systems do. Two competing money rules would
have produced inconsistent code.

**The dependency rule, scoped correctly.** `pydantic-settings` was blocking startup and
technically violated the no-new-deps rule. Allowed it, because the rule targets
*orchestration libraries* (Celery, SQLAlchemy, Alembic, tenacity) that would replace the
parts of the project that *are* the project. Nobody in an interview cares whether you
hand-rolled env parsing. The clause was written into CLAUDE.md so a future session doesn't
re-litigate it.

---

## 5. Infrastructure

### `docker-compose.yml`
Postgres 16 on 5432 with healthchecks, Redis 7 on 6379, a named `pgdata` volume.

**One container, two databases** — `sankalp` (dev) and `sankalp_test` (pytest) — created
by an init script that runs on a fresh volume. Not two containers on 5432/5433.

Rejected testcontainers (a fresh container per test session) because the crash test runs
20× in a loop and the soak runs 1,000 workflows; per-session startup makes iteration
painful at exactly the moment you're chasing a flaky race. Isolation comes from a truncate
fixture instead. Trade-off accepted: less hermetic, much faster.

Durability flags deliberately left at safe defaults — the crash gate depends on real
`fsync` behavior. `UNLOGGED` tables would be the tempting wrong answer here: faster, and
it deletes the durability that is the entire point.

### `migrations/001_core_schema.sql`
Raw numbered SQL, forward-only, no ORM, no Alembic.

### Migration runner (`storage/migrate.py`)
Checksum verification, out-of-order detection, an advisory lock so two concurrent runs
can't race, `CREATE DATABASE` bootstrap, `--dry-run`, and a transaction per migration.

Applies to **both** databases so dev and test schemas can't drift.

---

## 6. The schema — three load-bearing decisions

### `workflows` — the queue entry *and* the execution state

There is deliberately **no separate queue table and no recovery daemon**. A worker claims
a row by stamping `owner_id` and `lease_expires_at` on it. A dead worker's rows become
claimable again for exactly one reason: `lease_expires_at` drifts into the past and the
dequeue query's `status = 'RUNNING' AND lease_expires_at < now()` branch picks them up.

**Crash recovery *is* that predicate.** A reaper process would only duplicate it and add a
second thing that can be wrong.

### `fencing_token` — the column most likely to look unused

Monotonically increments on every claim, per workflow row. The scenario it exists for:

> A worker stalls mid-step (GC pause, VM freeze, network partition) holding token 7. Its
> lease expires. A second worker legitimately claims the row, gets token 8, and completes
> the step. The first worker wakes, still believing it owns the workflow, and issues its
> write. That write **must lose**.

No lock settles this alone — both workers hold what each sees as a valid claim. What
settles it is that every ownership-scoped statement carries
`AND owner_id = $x AND fencing_token = $y`, so the zombie's `UPDATE` matches zero rows and
it learns it was preempted.

**Why the token lives on the workflow row, not in Redis:** the store that accepts the
write must be the same store that rejects the stale writer. Check in Redis and write in
Postgres and the race is simply re-created one statement lower down.

This is Martin Kleppmann's core objection to Redlock — that it *"lacks a facility for
generating fencing tokens."* Phase 3 extends the same mechanism to
`resources.last_fencing_token`, which accepts a write only from a strictly higher token.

### `step_outputs` — `PRIMARY KEY (workflow_id, step_name, kind)`

The single most important choice in the schema, and the obvious "improvement" — a
`completed BOOLEAN` — is exactly what it rules out.

A step is done **iff** a row exists. Checkpoint and idempotency guard are therefore the
same fact, established by one `INSERT`, committed in the same transaction as the workflow
state change. Replay after a crash is a lookup, never a flag some code path has to
remember to set.

**Why a boolean can't work.** It splits one fact into two writes — perform the effect,
then mark it done — and the gap between them is precisely the crash window that
double-charges. No ordering closes it:

- Mark first, crash → real work silently skipped
- Mark second, crash → side effect repeats

The row-as-checkpoint has no gap, because there's only one write, and a second attempt is
a primary key violation rather than a second effect.

`kind` is in the key so compensations reuse this table instead of needing their own — the
forward run and the undo of the same step are two rows under the identical rule. `seq`
records forward order so the `COMPENSATING` pass can walk completed steps in reverse.

### `idx_workflows_claimable` — partial on purpose

```sql
CREATE INDEX idx_workflows_claimable ON workflows (run_after, id)
    WHERE status IN ('PENDING', 'RUNNING', 'COMPENSATING');
```

Terminal rows (`SUCCESS`, `COMPENSATED`, `FAILED_DIRTY`) accumulate forever and are never
claimable. Indexing them grows the dequeue index without bound while the rows it actually
serves stay roughly the size of the live backlog — every claim would walk a structure made
mostly of history. With the predicate, the index stays proportional to work in flight, and
rows drop out of it for free as they go terminal.

Two properties must hold or it silently stops being used:

1. The predicate must remain a **superset** of the dequeue query's status filter, or the
   planner can't prove the index applies and falls back to a sequential scan.
2. `run_after` must stay the **leading column**, matching the query's `ORDER BY run_after`,
   so a claim is an ordered walk that stops at `LIMIT` rather than sorting the whole
   backlog.

### Six statuses, no more

```
PENDING → RUNNING → SUCCESS
RUNNING → COMPENSATING → COMPENSATED
COMPENSATING → FAILED_DIRTY   (compensation itself failed — alert a human)
```

`FAILED_DIRTY` is deliberate. It's the state where you tried to undo and couldn't. Real
payment systems have this state and page someone. Modelling it honestly — rather than
pretending compensations always succeed — is the difference between a design that has
thought past the happy path and one that hasn't.

---

## 7. The checksum incident — the guard working correctly

Added explanatory comments to `001_core_schema.sql` *after* it had been applied. The
migration runner hashes the entire file body, comments included, so `make migrate` went
red with "was modified after it was applied."

**Three ways to reconcile, and why the choice matters:**

| Option | Verdict |
|---|---|
| Reset the volume (`down -v`, re-apply) | **Chosen.** Both databases held zero rows, so it cost nothing |
| `UPDATE schema_migrations SET checksum = ...` | Rejected — forging the audit record is the exact thing the guard exists to catch |
| Put the comments in a new `002_*.sql` | Rejected — someone opening `001` to "improve" the PK would never see them |

Before resetting, verified the DDL was genuinely unchanged **two ways**: comment-stripped
statements diffed byte-identical against the original, and applying the edited file to a
throwaway database produced a schema that diffed clean against the live one.

**The lasting consequence:** `down -v` was free exactly once, because the data was
worthless. From Phase 2 onward there'll be ledger rows that matter, and migrations are
genuinely frozen — every schema change is a new numbered file, no exceptions.

---

## 8. Verified state at end of day

| Check | Result |
|---|---|
| `docker compose up -d --wait` | Both containers healthy |
| `make migrate` | 001 applied to both DBs, checksum `6fc1faa29768` in each |
| Re-run `make migrate` | Clean no-op — the guard is armed |
| Schema in both DBs | 2 core tables, partial claimable index, 6 statuses |
| DDL reproducibility | Proven by applying to a throwaway DB and diffing |
| Package import | `sankalp` importable without `PYTHONPATH` (editable install) |
| Lint | `ruff check src/` clean on `py314` |
| Git | 2 commits, 20 files, all 644, all LF |

---

## 9. What's built vs. what isn't

**Built:** environment, docker-compose, migration runner, config, core schema.

**Not built:** `storage/queue.py`, the engine (executor, worker loop, retry, compensation),
the API, `tests/` (entire directory), migrations 002+ (outbox, ledger, resources, workers),
observability, README.

Next: **Prompt 1.2** — the dequeue query and the first tests. That's where the engineering
starts.

---

## 10. Interview material already banked

Three answers you can give from day one's work alone:

**"Why Postgres instead of a real consensus system?"** — Single-primary Postgres provides
linearizable writes and ACID transactions; `SKIP LOCKED` gives contention-free work
distribution. That's consensus-grade correctness for single-region without implementing
Raft. The ceiling is a single primary's write throughput, and queue partitioning is the
documented path past it.

**"Why not a `completed` boolean on the step?"** — Because it splits one fact into two
writes, and the gap between them is the crash window that double-charges. Mark first and a
crash skips real work; mark second and a crash repeats a side effect. The row-as-checkpoint
has no gap.

**"How do you know your lease-based ownership is safe?"** — Leases alone aren't; a stalled
worker can wake believing it still owns the row. Fencing tokens are what settle it, and
critically the token must be checked by the same store that accepts the write. That's
Kleppmann's objection to Redlock in one line.
