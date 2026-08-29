# Sankalp — Phase 3 Build Log (Restricted Role, Secure-by-Default, and the API Surface)

Everything built in Phase 3 so far, with the reasoning an interviewer will probe. The code is
in git; this is the *why*, which only lives here.

**Commits so far (merged to `main`):**
- `a281d6a` — `004_restricted_role.sql`: the `sankalp_app` role
- `542f8c4` — worker/drain onto `sankalp_app`; `create_pool` defaults to the restricted role
- `e1320fa` — minimal API surface (submit / get / cancel), on the restricted role by
  construction

**What these three commits close, together:** every process that used to connect as
`sankalp` — the superuser that also owns every table — now connects as a role that is neither.
The ledger's append-only guarantee, which Phase 2 shipped as a trigger, gets its second layer.
`create_pool`'s default flips from fail-open to fail-closed. And the first real API routes are
the first code in this repo that was *born* running under the restricted role, rather than
migrated onto it.

---

## 1. The restricted `sankalp_app` role — closing what the trigger structurally cannot see

`003_saga.sql`'s `ledger_entries_append_only()` trigger is `BEFORE UPDATE OR DELETE` — it
fires on exactly those two statement types. `TRUNCATE` is not a row operation. It never
produces a row for a `BEFORE` row-trigger to inspect, so a trigger — any trigger, written any
way — structurally cannot see it. Phase 2's build log left this named as a known gap
(`tests/test_ledger.py::test_truncate_is_deliberately_not_blocked` documented it rather than
hiding it), and closing it needs a different layer entirely: privileges, not triggers.

**Why a `REVOKE` only works if the role is neither a superuser nor the table's owner.** A
superuser bypasses every privilege check unconditionally. A table's owner keeps implicit `ALL`
regardless of what has been `REVOKE`d from them explicitly — ownership is its own grant, sitting
underneath and prior to the ACL. `REVOKE` against either is theatre: the SQL runs, the catalog
even shows the change, and the role can still do the thing anyway. `sankalp_app` has to fail
*both* tests to be a real control, and the migration is deliberate that both are load-bearing
independently, not redundantly.

**What `004_restricted_role.sql` actually grants** (`sankalp_app`, applied identically to
`sankalp` and `sankalp_test` — `CREATE ROLE` is cluster-wide but each database's `GRANT`s are
not):

- `CONNECT` on the database, `USAGE` on `public`.
- `SELECT, INSERT, UPDATE` on `workflows`, `step_outputs`, `outbox` — exactly what the running
  engine does: claim/update workflow state, write step checkpoints, write and mark outbox
  events.
- `USAGE, SELECT` on `outbox_id_seq` and `ledger_entries_id_seq` only — the two `BIGSERIAL`
  primary keys in the schema. `workflows.id` is `gen_random_uuid()` and `step_outputs`' PK is
  composite; neither has a sequence, so neither needs this grant.
- `ledger_entries`: `REVOKE ALL` first (a defensive no-op — nothing above touched this table —
  so the SELECT/INSERT-only intent reads as a stated pair, not an absence someone could widen
  later without anything here saying otherwise), then `GRANT SELECT, INSERT`. No `UPDATE`, no
  `DELETE`, no `TRUNCATE`.
- **Nothing at all** on `side_effects`, `step_attempts`, `crash_gates` — the crash-gate
  instrumentation tables from `002_crash_gate.sql`. That omission is deliberate and it matters
  for Section 2 below: `workflows/_instrumentation.py`'s pool stays on the owning role,
  untouched by this migration, on purpose.

**The introspection, as proof.** Not a description of what the migration is supposed to do —
what the live catalog actually says, run against the running `sankalp` database:

```
$ psql -U sankalp -d sankalp -c "SELECT rolname, rolsuper, rolcreaterole, rolcreatedb, rolbypassrls FROM pg_roles WHERE rolname='sankalp_app';"
   rolname   | rolsuper | rolcreaterole | rolcreatedb | rolbypassrls
-------------+----------+---------------+-------------+--------------
 sankalp_app | f        | f             | f           | f

$ psql -U sankalp -d sankalp -c "SELECT r.rolname AS member, m.rolname AS member_of FROM pg_auth_members am JOIN pg_roles r ON am.member=r.oid JOIN pg_roles m ON am.roleid=m.oid WHERE r.rolname='sankalp_app';"
 member | member_of
--------+-----------
(0 rows)

$ psql -U sankalp -d sankalp -c "SELECT tablename, tableowner FROM pg_tables WHERE schemaname='public';"
     tablename     | tableowner
--------------------+------------
 schema_migrations  | sankalp
 workflows          | sankalp
 step_outputs       | sankalp
 side_effects       | sankalp
 step_attempts      | sankalp
 crash_gates        | sankalp
 outbox             | sankalp
 ledger_entries     | sankalp
(8 rows)

$ psql -U sankalp -d sankalp -c "\z"
                                         Access privileges
 Schema |         Name          |   Type   |    Access privileges    | Column privileges | Policies
--------+-----------------------+----------+-------------------------+-------------------+----------
 public | crash_gates           | table    |                         |                   |
 public | ledger_entries        | table    | sankalp=arwdDxt/sankalp+|                   |
        |                       |          | sankalp_app=ar/sankalp  |                   |
 public | ledger_entries_id_seq | sequence | sankalp=rwU/sankalp    +|                   |
        |                       |          | sankalp_app=rU/sankalp  |                   |
 public | outbox                | table    | sankalp=arwdDxt/sankalp+|                   |
        |                       |          | sankalp_app=arw/sankalp |                   |
 public | outbox_id_seq         | sequence | sankalp=rwU/sankalp    +|                   |
        |                       |          | sankalp_app=rU/sankalp  |                   |
 public | schema_migrations     | table    |                         |                   |
 public | side_effects          | table    |                         |                   |
 public | side_effects_id_seq   | sequence |                         |                   |
 public | step_attempts         | table    |                         |                   |
 public | step_attempts_id_seq  | sequence |                         |                   |
 public | step_outputs          | table    | sankalp=arwdDxt/sankalp+|                   |
        |                       |          | sankalp_app=arw/sankalp |                   |
 public | workflows             | table    | sankalp=arwdDxt/sankalp+|                   |
        |                       |          | sankalp_app=arw/sankalp |                   |
(12 rows)
```

Read the `ledger_entries` line by ACL letter: `sankalp_app=ar/sankalp` is `a` (INSERT) and `r`
(SELECT) — no `w` (UPDATE), no `d` (DELETE), no `D` (TRUNCATE). No bare `=...` entry anywhere in
that output means no grant to `PUBLIC` either. `rolsuper=f` and zero owned tables confirm both
load-bearing conditions from above hold in the actual running system, not just in the migration
source. `crash_gates`/`side_effects`/`step_attempts` show blank access-privilege columns
entirely — the default, meaning only the owner (`sankalp`) has any rights at all.

**This piece doesn't get a fail-proof in the crash-gate sense, and that's not a gap.** The
crash gates (Phase 1, Phase 2) exist because their failure mode is *intermittent* — a race that
only shows up under real timing, which is why they run `--count=20` and get restored-and-diffed
after being deliberately broken. A privilege check has no timing dimension: either the grant
exists in the catalog or it doesn't, and it is exactly as true on the one-thousandth connection
as the first. There's no repetition to run and no flakiness to shake out. The introspection
above — read directly off the live catalog, not inferred from what one client library's
exception type happened to say — is the proof that actually fits this kind of claim.
(`tests/test_ledger.py::test_sankalp_app_cannot_update_delete_or_truncate_ledger_entries` pins
the same fact from `asyncpg`'s side, for the suite; the two are independent confirmations of
one deterministic fact, not two different things being proven.)

---

## 2. The `create_pool` flip — from fail-open to fail-closed

Before this piece, `create_pool()` with no explicit `dsn` defaulted to
`settings.active_database_url` — the owning `sankalp` role. `sankalp_app` existed
(`004_restricted_role.sql` had already shipped) but using it required *remembering* to pass it
explicitly at every call site. That is a footgun with exactly one shape: forget the override
once, in one process, and that process silently runs with full owner access — past every grant
in `004`, including the `ledger_entries` `REVOKE` that Section 1 just proved. The mistake is
invisible until someone goes looking for it, because everything still works; it just works with
more privilege than it needed.

`542f8c4` flips the default:

```python
# storage/pool.py
return await asyncpg.create_pool(
    dsn or settings.active_app_database_url,   # was: settings.active_database_url
    ...
)
```

`engine/worker.py` and `engine/drain.py` now call `create_pool(settings.active_app_database_url,
settings=settings)` explicitly — redundant with the new default, and left explicit anyway,
because a call site that states its own DSN doesn't depend on nobody having changed the default
out from under it later.

**The one place that genuinely needs the owning role is now the only opt-out, and it's a visible
one.** `workflows/_instrumentation.py`'s pool writes to `side_effects`, `step_attempts`, and
`crash_gates` — tables `004` deliberately grants `sankalp_app` nothing on (Section 1). Before
the flip this pool's `create_pool()` call looked identical to every other call site; the elevated
access was implicit. After the flip it has to say so:

```python
# workflows/_instrumentation.py
settings = get_settings()
_pool = await create_pool(settings.active_database_url, settings=settings)  # owning role, on purpose
```

**The failure mode is now fail-closed instead of fail-open.** Forget to override the new
default anywhere and the resulting pool is *more* restricted than intended, not less — a
missing `sankalp_app` grant surfaces immediately as `InsufficientPrivilegeError` on the very
first write that needs it, rather than silently succeeding with privilege nobody meant to hand
out. That is a strictly better place for a mistake to land: loud and immediate beats silent and
permanent.

**Proof: all three crash gates, unchanged, at `--count=20`.** Every repetition of every gate
exercises both role paths in the same run — the worker/executor pool (now `sankalp_app`,
writing `workflows`/`step_outputs`/`outbox`) and the instrumentation pool
(`_instrumentation.py`, still the owning role, writing `side_effects`/`step_attempts`/
`crash_gates`). If the opt-out in `_instrumentation.py` had been left off, or had picked up the
new restricted default by accident, the very first `INSERT INTO step_attempts` in the very
first repetition would raise `InsufficientPrivilegeError` — there is no partial-credit outcome
here, no gate would get past its first step. The gates staying green at full count is direct
evidence both pools are wired to the role they're supposed to be. Measured on `542f8c4` itself
— the commit that performed the flip:

```
crash gate:           40/40  (20 repeats x 2 collected tests, tests/test_crash.py)
compensation-crash:   20/20  (tests/test_compensation_crash.py)
drain-crash:          20/20  (tests/test_drain_crash.py)
= 80 passes across three gates at --count=20
```

Not re-run on `e1320fa`: that commit only added `api/main.py`, `storage/workflows.py`'s new
functions, and `tests/test_api.py` — it doesn't touch `worker.py`, `drain.py`, `executor.py`, or
any pool default, so the role wiring this proves is unchanged since `542f8c4` measured it.

---

## 3. The API surface — submit / get / cancel, on the restricted role by construction

`docs/spec.md`'s Phase 1 API, built for the first time this phase, and deliberately the first
code in the repo that never ran on anything but `sankalp_app` — `api/main.py`'s lifespan calls
`create_pool()` with no explicit `dsn`, so the restricted-by-default behavior from Section 2 is
what actually runs; passing the DSN explicitly here would have quietly stopped testing that the
default is safe.

### Idempotent submit needs no retry loop, because Postgres already arbitrates the race

`POST /workflows` is `INSERT ... ON CONFLICT (workflow_type, idempotency_key) DO NOTHING
RETURNING ...`, and only on a conflict, a re-select — never `ON CONFLICT ... DO UPDATE`, because
a duplicate submit must never mutate a workflow that may already be `RUNNING`
(`storage/workflows.py::submit_workflow`).

The concurrent case is the one worth being precise about, because the wrong mental model here
("what if the loser reads the row before the winner commits?") describes a bug that Postgres's
own arbitration for `ON CONFLICT` doesn't allow to happen. When two transactions race the same
`(workflow_type, idempotency_key)`, the loser's `INSERT` statement *blocks* on the winner's
uncommitted speculative-insertion lock — it does not read "no conflict yet" and proceed. When
the winner commits, the loser's blocked `INSERT` unblocks, the conflict is now visible,
`DO NOTHING` fires, and `RETURNING` yields zero rows. (If the winner rolls back instead, the
loser's blocked insert simply proceeds and becomes the winner itself.) The follow-up `SELECT`
then runs as a separate statement in the *same* transaction — under READ COMMITTED, Postgres's
default and also asyncpg's `conn.transaction()` default — which takes a fresh snapshot per
statement. Because that `SELECT` only starts after the `INSERT` unblocked, i.e. strictly after
the winner's commit, its fresh snapshot is guaranteed to see the winner's row.

This depends on staying at READ COMMITTED. At REPEATABLE READ or SERIALIZABLE, the loser's
blocked insert doesn't resolve gracefully into `DO NOTHING` — it raises a serialization failure
instead, and the whole "no retry loop needed" property disappears. Nothing in this codebase
raises the isolation level on this transaction; that absence is load-bearing, not an oversight.

`tests/test_api.py::test_concurrent_duplicate_submits_produce_exactly_one_row` fires ten
concurrent identical submits and asserts exactly one `201`, nine `200`s, one row, one `id`
shared by all ten responses — run 15 times locally with zero flakes, which is what you'd expect
from a guaranteed outcome rather than a statistically likely one.

### The cancel bound: no false SUCCESS, ever — just a possibly delayed unwind

`POST /workflows/{id}/cancel` is guarded entirely in SQL:

```sql
UPDATE workflows
SET status = 'COMPENSATING', error = COALESCE(error, 'cancelled by user'),
    run_after = now(), updated_at = now()
WHERE id = $1 AND status IN ('PENDING', 'RUNNING')
```

It does not touch `owner_id` or `fencing_token`. It can't: every other write in
`storage/workflows.py` carries those two columns as an ownership guard because it's a worker
presenting proof of a live claim, and the API holds no lease and has no fencing token to
present. Writing `owner_id = NULL` here to make the row "look free" would be actively wrong — it
would let a second worker claim a row the current owner is still executing, the exact
double-claim the fencing token exists to prevent.

The honest consequence: a worker executing forward does not observe this cancel. Every per-step
write it makes (`commit_step_output` → `_CHECKPOINT_POSITION_SQL`) guards on `id`/`owner_id`/
`fencing_token` only, never `status` — so once cancel has flipped the row to `COMPENSATING`, the
worker keeps checkpointing every remaining step in `definition.steps` as if nothing happened,
because nothing in that loop rechecks `status` between steps. It is only the very last write of
a forward run, `finish_success` → `_FINISH_SUCCESS_SQL`, that requires `status = 'RUNNING'`; that
`UPDATE` matches zero rows, `finish_success` returns `False`, and `execute_workflow` raises
`PreemptedError` immediately — no additional wait. The executor reads that as preemption, drops
the work, and the row — `COMPENSATING`, `owner_id` still set — falls back to the same
lease-expiry recovery path every crashed worker's row already goes through, and then unwinds
normally.

So the two delays are alternatives, not additive. If the worker that held the row when it was
cancelled is still alive, the bound is the runtime of every step still left to execute in that
forward run (not just the one in flight) — `finish_success` fails synchronously the moment it's
reached, with no lease wait tacked on. The lease duration only enters if that worker dies before
reaching `finish_success` at all (a crash, not a live cancel): then nothing writes
`finish_success` or anything else, and recovery has to fall back on the lease expiring, the same
as any other dead worker's row.

So the bound is: **a cancel can be delayed by up to the runtime of the rest of that forward run
if the worker stays alive, or up to one lease duration if it doesn't — never both, and never
silently ignored into a false SUCCESS.** That's stated as a designed property,
not confessed as a limitation — the alternative (having cancel steal the ownership columns to
force immediate recognition) would reintroduce exactly the mid-unwind claim race Phase 2's
build log spent a full section on, for a route that isn't the lease holder and has no business
presenting fencing proof it doesn't hold.

### What's not built yet

No rate limiting. No adaptive concurrency. No observability — no tracing, no metrics, on the
API or anywhere else. The three routes are exactly `docs/spec.md`'s Phase 1 API and nothing
past it: no middleware, no new tables. Those are later pieces, not implied by anything shipped
here.

---

## State at end of these two pieces

- `sankalp_app`: neither superuser nor table owner, confirmed by direct catalog introspection
  as well as by test. `ledger_entries` immutability is now two independent layers — trigger
  blocks `UPDATE`/`DELETE`, grants block `TRUNCATE` — closing the gap Phase 2 named and left
  open.
- `create_pool` defaults to the restricted role everywhere; the one legitimate exception
  (crash-gate instrumentation) is now an explicit, visible opt-out instead of an implicit
  default. All three crash gates still pass at `--count=20`, which exercises both role paths on
  every single repetition.
- The Phase 1 API exists: `POST /workflows` (idempotent, race-free under concurrency without a
  retry loop), `GET /workflows/{id}`, `POST /workflows/{id}/cancel` (bounded-delay, never a
  false `SUCCESS`) — all three running on the restricted role from the start. 16 new tests;
  full non-crash suite (148 tests) green; ruff clean.
- Not built: rate limiting, adaptive concurrency, observability (Phase 3's remaining pieces and
  Phase 4).

---

## 4. The rate limiter — fail-open, the breaker that makes it fast, and the clock

`2870880` — a Redis-backed token bucket (`resilience/ratelimit.py`) fronted by a circuit
breaker (`resilience/circuit.py`), enforced as ASGI middleware (`api/middleware.py`) in front
of all three routes from Section 3. One tier only: every check is a Redis round trip; there is
no in-process L1 cache underneath it, and that absence matters below.

### Fail-open is the correct stance here, not a weakened one

When the breaker is open, or Redis answers with a transport failure, `check()` returns
`admitted=True, enforced=False` — every request goes through, unconditionally. Not a smaller
limit, not a cached last-known-good decision: no admission control at all, because there is no
L1 tier to fall back to.

That's deliberate, and the reasoning is the same one that lets Section 3's cancel route leave a
worker temporarily unaware it's been cancelled: the limiter is a protective control, not a
correctness control. Nothing about the money guarantee depends on it. `POST /workflows` is
idempotent under concurrent duplicates by construction (Section 3); every step commits its
effect exactly once regardless of how many times it's attempted (the project's core guarantee,
stated in `CLAUDE.md`). A flood during a Redis outage costs throughput — it cannot make a step
execute twice. Failing closed would take an outage of one dependency this system has never
promised durability from, and turn it into a total outage of money movement over a dependency
whose entire job is shedding load, not holding state. That's a strictly worse failure mode than
the one being defended against.

The scope caveat is worth stating plainly rather than leaving implicit: this reasoning holds
only because the limiter enforces an aggregate protective budget, not an entitlement. If a
route class's limit ever becomes a per-tenant contractual quota — "this customer is guaranteed
no more than N/sec, full stop" — fail-open becomes wrong, because now unlimited admission
during an outage isn't a throughput cost, it's the SLA itself being silently violated. That's a
different feature with a different failure mode, and it isn't what's built here.

Fail-open alone is not sufficient, and the breaker is why: without it, every request during an
outage still pays a full connect/read timeout against a socket that will never answer. That
doesn't just remove rate limiting — under a constant arrival rate it makes the API slow enough
that the request queue grows without bound, which is its own outage. The breaker is what turns
"fail open" into "fail open *fast*": once it trips, `allow()` returns `False` synchronously,
before any socket is touched, and the answer comes back in microseconds instead of at the
timeout budget. Fail-open decides what the answer is when Redis is unreachable; the breaker
decides how fast that answer arrives — they're solving two different halves of the same outage.

### The breaker counts transport failures, never application answers

`_run`'s exception handling is the one thing in this module most worth reading closely before
changing it, because the three outcomes it distinguishes look similar from the outside and
must not be merged:

- `ResponseError` (which includes `NoScriptError`) — Redis answered, promptly and correctly, and
  the answer happened to be an error. Never fed to the breaker.
- `TimeoutError | RedisError | OSError` — Redis (or the socket to it) did not answer. Fed to
  the breaker.

A `NOSCRIPT` means the script cache is cold — a restart, a `SCRIPT FLUSH`, a failover onto a
replica that never got `SCRIPT LOAD`'d. Redis is not merely up, it's telling the caller exactly
what to do next (`EVAL` once, and the cache repopulates as a side effect of that call). Counting
it as a breaker failure would trip the breaker at the exact moment Redis is healthiest —
answering fast and correctly — and a real Redis restart makes this failure mode certain rather
than rare: every API process's local script cache goes cold at once, so without the special
case, a routine restart would produce a burst of `NOSCRIPT`s across the whole fleet
simultaneously, tripping every breaker on a Redis that has just come back. `_eval_with_retry`
catches `NoScriptError` and retries once with a full `EVAL`, entirely inside `_run`, before the
transport-failure classification ever runs — the breaker never even learns a `NOSCRIPT`
happened.

A script bug — today, only `refill_per_sec <= 0`, rejected via `redis.error_reply` — is also a
`ResponseError`, and deliberately handled the same way: fail open, but log at `ERROR` and leave
the breaker `CLOSED`. This is a permanent programming error (a caller passed a bad config), not
a transient outage, and disguising it as one would be actively misleading — it would make the
breaker open and later self-heal on its own cooldown, which reads as "Redis recovered" when
what actually happened is "someone shipped a bad value and never got paged for it." Loud and
un-self-healing is the right shape for a bug; quiet and self-healing is the right shape for an
outage. Conflating them loses the ability to tell which one is happening from the outside.

### The clock: whose, and why the choice looks locally wrong and is globally right

`check()` computes `now_ms = int(self._clock() * 1000)` in the caller's process and passes it
into the Lua script as `ARGV[3]`, rather than letting the script call Redis's own `TIME`.

**The failure mode this creates, and the one line that bounds it.** Every bucket's state is
keyed off whatever `now_ms` the *last* caller happened to pass. If a caller's clock is skewed —
behind the value already stored for that key, whether from NTP drift, a VM pause, or simple
cross-instance disagreement — naive arithmetic computes a negative `elapsed`, and a negative
elapsed multiplied by `refill` *subtracts* tokens instead of adding them. Enough skew, or enough
callers hitting the same key with different clocks, drives `tokens` arbitrarily negative — and
because nothing subsequently pulls it back up except real elapsed time at the configured refill
rate, the bucket can end up needing a very long time to refill back past zero. Run the numbers
on the exact skew `test_a_caller_whose_clock_lags_never_drains_the_bucket_below_zero` exercises
— `capacity=10`, `refill_per_second=5.0`, a 50,000-second backward clock jump — through the
pre-clamp arithmetic:

```
Call 1 (clock=100_000.0s -> now_ms=100_000_000): first write to this key, so tokens=capacity=10.
  cost=1 -> tokens=9, allowed. Stored: tokens=9, ts=100_000_000.

Call 2 (clock=50_000.0s -> now_ms=50_000_000), no clamp:
  elapsed = (50_000_000 - 100_000_000) / 1000 = -50_000 s
  tokens  = min(10, 9 + (-50_000 * 5)) = min(10, -249_991) = -249_991
  denied (-249_991 < cost). Stored: tokens=-249_991.

Recovery, assuming no further skew:
  tokens needed to reach cost=1: 249_991 + 1 = 249_992
  at refill=5 tokens/sec: 249_992 / 5 = 49_998.4 s ~= 13h 53m
```

One skewed call drives `tokens` to −249,991, and recovering to the single token a request needs
costs 49,998 seconds — **just under 14 hours** — of real time at the configured refill rate.
That's not a slower bucket — it's every request against that key denied for a duration with no
relationship to how large the clock skew actually was.

`math.max(0, now_ms - last_ts)` (`ratelimit.py`'s Lua, the line called out in its own comment)
is the fix, and the shape of what it changes is the whole point: it doesn't remove skew, it
bounds its consequence. A lagging clock now contributes exactly zero refill instead of negative
refill — no worse than a caller who happened not to call in for a while. A fast clock can still
over-refill, but only up to `capacity`, via the `math.min` two lines down — a one-time bounded
burst, never an unbounded one. Skew degrades to "one bucket briefly acts as if slightly more or
less time passed than it did," never to "this bucket is now permanently wrong" or "this bucket
is now deadlocked." That's the entire difference between a clamp being defensive padding and
being load-bearing: remove it, and the failure mode isn't a slightly worse limiter, it's an
outage shaped like a rate limiter.

**Why the caller's clock instead of Redis's `TIME`, honestly.** `TIME` would actually be *more*
correct for this specific bucket: one authoritative clock, called from inside the script, means
no cross-instance skew to bound in the first place — the failure mode above wouldn't exist. And
the classical objection to `TIME` — that it isn't deterministic and so breaks replication —
doesn't apply here: Redis has replicated by *effect* (propagating the writes a script produced)
rather than by verbatim command since Redis 5, and this is Redis 7. So the honest reason to keep
the caller's clock isn't correctness; it's testability. It's the injection seam that lets
`test_atomicity_under_200_concurrent_callers_admits_exactly_the_capacity` pin every caller to
the same instant and assert exactly 50-of-200 admitted as an equality, not a tolerance band
around real wall-clock jitter — the same trade `compute_backoff` already made in taking an
injectable `rng` instead of reaching for the module's own random state. On genuinely
unsynchronized hosts, this is a one-line change: swap the `ARGV[3]` clock for `redis.call('TIME')`
inside the script and drop the parameter. Nothing else about the bucket's shape changes.

### Three bugs found in `docs/spec.md`'s reference script, fixed here

Implementing the spec's Lua surfaced three places where the reference script is wrong, not just
stylistically dated:

1. **Truncated tokens, but a correct `retry_after_ms`.** Redis truncates a Lua number to an
   integer on the RESP reply, so the spec's `return { allowed, tokens }` silently hands the
   caller `9` for a true count of `9.5` — a fractional token, lost with no signal it happened.
   Truncation itself is fine for the token count; the bug would be computing `retry_after_ms`
   *from* the already-truncated value. This script computes it in Lua, before the return, while
   `tokens` and the deficit are still floats — the client's `Retry-After` header reflects the
   real deficit, not one rounded down first.
2. **`HSET`, not `HMSET`.** The spec's script uses `HMSET`, deprecated by Redis since 4.0.
3. **`refill <= 0` rejected explicitly**, via `redis.error_reply`, before it reaches the
   `PEXPIRE` line's division. The spec's version divides by it unguarded — a caller bug turns
   into a Lua runtime error at a different line than the one that caused it, with no message
   pointing at the actual mistake. Guarding it explicitly turns "someone passed a bad config"
   into a clean `ResponseError` that — per the breaker rules above — fails open and logs loudly
   without touching the breaker. (`Settings` also constrains `refill_per_second > 0`
   independently; this is the second, inner check, for anything that reaches the script by a
   path the outer one didn't cover.)

### What proves it

Five fail-proofs, each one built by breaking the mechanism first and watching the test catch it:

- Feed the script the clock-lag test's own values (`capacity=10`, `refill=5.0/s`, a
  50,000-second backward jump) without the clamp: the arithmetic above drives the bucket to
  −249,991 tokens and a ~13.9-hour (49,998-second) recovery from a single skewed call — not a
  degraded limiter, an outage. With `math.max(0, ...)` in place, the same skew costs one
  bucket's worth of under-refill and nothing more.
- Route a `NOSCRIPT` through the plain transport-failure path instead of the dedicated retry:
  the breaker trips on a Redis that just answered correctly.
- Remove the breaker's short-circuit and blackhole Redis: 20 sockets get opened against a
  hung Redis that should never have been touched once the breaker was open — `test_
  breaker_opens_after_the_threshold_and_stops_touching_the_socket` asserts `connections == 0`
  for exactly this reason.
- Race 50 concurrent callers through `HALF_OPEN` without the single-synchronous-call state
  flip: more than one probe reaches Redis. With `allow()`'s state transition and its "yes"
  happening inside the same non-`await`ing call, exactly one of the 50 gets `enforced=True`
  and `connections` advances by at most one.
- `test_atomicity_under_200_concurrent_callers_admits_exactly_the_capacity` at `--count=20`:
  260/260 clean runs of an exact-equality assertion (50-of-200 admitted, pinned clock, no
  tolerance band) — the kind of assertion that only stays green by accident if the underlying
  atomicity is real.

Crash gates unaffected — this piece touches no worker, dequeue, or checkpoint path, so
`tests/test_crash.py` and friends weren't expected to move and didn't.

275 lines in `resilience/ratelimit.py`, roughly 650 across `test_ratelimit.py` and
`test_circuit.py` combined. That ratio is not padding — it's what "the proof is the point"
looks like in line counts on a module whose entire job is behaving correctly under exactly the
concurrent and adversarial conditions that are hardest to exercise by hand.

### What's not built yet

No adaptive concurrency (a gradient limiter that scales worker concurrency to observed
latency, not anything about Redis) — the breaker here only ever answers admit-or-don't. No
load testing: everything above is a unit-level proof of the mechanism (atomicity, fail-open,
breaker timing, clock-skew bounding) under controlled, synthetic conditions — a real
production overload, with real network variance and real client behavior, hasn't been measured
against this. The fail-proofs show the mechanism does what it claims when deliberately broken;
they are not evidence of how it behaves under organic load.
