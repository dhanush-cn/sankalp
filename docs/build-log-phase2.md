# Sankalp — Phase 2 Build Log (Compensation, and the Race It Uncovered)

Everything built in Phase 2, with the reasoning an interviewer will probe. The code is in
git; this is the *why*, which only lives here.

**Phase 2 commits (merged to `main` from `phase-2/compensation`):**
- `5267fb9` — `003_saga.sql`: outbox and append-only `ledger_entries`
- `c391a6f` — prove `003_saga.sql`: append-only, idempotency guard, reconciliation
- `901890d` — compensation execution + fix mid-unwind claim race

**The Phase 2 guarantee, proven:** a saga that fails partway unwinds its committed steps in
reverse, each compensation running exactly once — and SIGKILL the worker mid-unwind and the
next one resumes without re-running an undo that already committed. Demonstrated by
`tests/test_compensation_crash.py` at `--count=20` (20/20) alongside the Phase 1 gate still at
40/40, and observed to fail when any of its three mechanisms is removed.

**The headline of this phase is not the compensator.** It is the bug the compensator exposed:
a worker could steal its own in-progress unwind and refund twice, with no crash involved, and
the Phase 1 scaffolding had been hiding it the whole time. Section 3 is the long one.

---

## 1. The unwind — the forward loop, read backwards

`execute_workflow` dispatches on `claimed.status`. COMPENSATING goes to `_compensate`, which
loads the committed checkpoints in reverse `seq` and reverses each one:

```
load FORWARD checkpoints ORDER BY seq DESC, and the COMPENSATION names as a set
for record in forward:
    if record.step_name in compensated:   continue   # already undone
    if step.compensation is None:         continue   # read-only step
    renew_lease_if_needed()
    run the compensation
    BEGIN: guarded UPDATE workflows; INSERT step_outputs kind='COMPENSATION' COMMIT
UPDATE workflows SET status='COMPENSATED', owner_id=NULL
```

Deliberately the *same* rules as the forward direction rather than a second set. A compensation
is done if and only if a row exists for it — the `(workflow_id, step_name, kind)` primary key
means a step's forward run and its undo are two distinct rows idempotent under one rule. No new
table, no `completed` boolean, no migration: `kind` and `seq` were already there, put there in
`001_core_schema.sql` for exactly this.

Ordering comes from the persisted `seq`, not from `definition.steps`. A definition edited under
an in-flight saga would otherwise silently reorder the reversal of money that has already
moved. The row says what order it ran in; the code doesn't get a second opinion.

### FAILED_DIRTY stops the unwind where it broke
A compensation that can't be made to succeed ends the unwind at that step. The steps below it
are left alone, because reverse `seq` is a *dependency* order — step 1's undo may assume step
2's has already run, so carrying on past a failure runs an undo whose precondition was just
violated, and can make the inconsistency worse rather than smaller. The committed COMPENSATION
rows are the record of exactly how far it got, which is what the human resolving it needs.

That's the whole reason `FAILED_DIRTY` exists as a status rather than being modelled as a
retry: it is the state where you tried to undo and couldn't, and only a person can finish it.

---

## 2. Two retry regimes, and why one counter can't serve both

A compensation gets its own budget (`Settings.compensation_max_attempts`), counted **in memory**
inside a single claim. `workflows.attempt` is never touched by the unwind. That looks like
duplication until you try to share the counter, at which point it fails three separate ways.

**The budget is already spent by the time you need it.** A workflow reaches COMPENSATING one of
two ways, and the common one is *retries exhausted* — meaning `attempt == max_attempts` at the
exact moment compensation begins. A shared counter therefore gives every retry-exhausted saga
**zero** compensation retries: the first transient blip in an undo goes straight to
`FAILED_DIRTY`. The regime that most needs a retry budget is the one guaranteed not to have any
left.

**The counter measures claims, not failures.** The dequeue query does `attempt = attempt + 1`
as part of claiming, and a COMPENSATING row is re-claimed on crash recovery. So a shared budget
is consumed by *worker deaths during the unwind*, not by compensations failing. Kill a worker
three times mid-unwind and the saga goes `FAILED_DIRTY` and pages someone, though no
compensation ever failed once. Counting in memory means the resuming worker starts a fresh
budget — which is right, because it is measuring something else.

**Resetting `attempt` at `begin_compensation` doesn't rescue it.** That buys a clean budget by
destroying the forward run's history, which is the first thing anyone reads off the row when
asking how a saga got here. Considered and rejected; the test
`test_the_unwind_leaves_workflows_attempt_alone` pins it.

The two also cost different things, which is why the defaults differ (`max_attempts=5`,
`compensation_max_attempts=3`). A forward retry *releases the row* and costs nothing while it
waits. A compensation retry happens inside a live claim, holding a lease and a concurrency slot
through every backoff.

### The policy inside a compensation is inverted, too
Forward: an unclassified exception is **terminal**, because an unrecognised failure is not
evidence that re-running the step is safe — it may already have moved money. Compensations are
idempotent *by contract*, so re-running one is safe, and the expensive mistake runs the other
way: giving up on a transient failure strands a saga in `FAILED_DIRTY` and pages someone for a
blip that cleared a second later. So the unwind retries everything except an explicit
`TerminalError` — the compensation itself saying waiting won't help.

---

## 3. The mid-unwind claim race — the real find of this phase

### What the Phase 1 stub was hiding

Phase 1 had no compensator. A worker claiming a COMPENSATING row called `_defer_compensation`,
which released ownership and pushed `run_after` out by a backoff. Correct for its purpose, and
it had a side effect nobody had reason to notice: **a COMPENSATING row was never held by anyone
for more than a few milliseconds.**

Meanwhile the dequeue query's `WHERE` read:

```sql
status IN ('PENDING', 'COMPENSATING')
OR (status = 'RUNNING' AND lease_expires_at < now())
```

Look at the COMPENSATING branch. It has **no lease test at all**. A COMPENSATING row was
claimable *no matter who held it or whether their lease was live*. In Phase 1 that was
unobservable: nothing ever held such a row long enough to be stolen from. The moment a real
unwind started holding one for the duration of several compensations, it became a free-for-all.

This is the interesting shape of the bug. It was not introduced by the compensator. It was
**already in the spec's query**, latent, protected from observation by a stub whose entire job
was to be temporary. Removing the scaffolding is what made the defect reachable.

### How it surfaced: distrusting a fast test

The first run of the new crash gate failed like this:

```
E  AssertionError: 'charge:compensate' was resumed by pid 4309, the process this test killed.
E  assert 4309 != 4309
1 failed in 0.79s
```

The assertion message was misleading — it reads as "recovery came from the wrong process." The
useful signal was **0.79s**. That test launches three Python interpreters and waits for each to
log that it is polling; it cannot finish in under a second. The number was evidence that the
test had never reached the state it thought it was testing, and that the assertion firing was a
downstream symptom, not the fault.

So: don't debug the assertion, instrument the system. A throwaway probe drove one
`demo_unwind` workflow through a real worker fleet and printed `step_attempts`, status, and the
ownership columns every 250 ms. The answer came back immediately:

```
[0.00s] status=COMPENSATING
        charge:compensate  pid=4351   <- same worker
        charge:compensate  pid=4351   <- same worker, again
[0.25s] charge:compensate  pid=4351   <- and again
        charge:compensate  pid=4352   <- and now a second worker
```

Then, cut down to a **single** worker to eliminate contention as an explanation:

```
ROW: status=COMPENSATING  attempt=4  fencing_token=4
charge:compensate attempts: 3
WARNING sankalp.worker: lost the lease on workflow ...; re-claimed past fencing token 2
WARNING sankalp.worker: lost the lease on workflow ...; re-claimed past fencing token 3
```

One worker, polling every 50 ms, re-claimed the same row four times and ran three concurrent
unwinds of the same saga — **racing itself**. Each claim bumped the fencing token and preempted
the previous one mid-compensation.

### The measurement that matters

With the buggy predicate, one worker, and *no kill at all*:

```
side_effects  {'charge:compensate': 2, 'reserve:compensate': 2}
fencing_token: 4
final status:  COMPENSATED
```

Two committed refunds and two committed reserve-releases on a single saga, and the workflow
reported `COMPENSATED` as though everything were fine. No crash, no contention, no adversarial
timing — just a worker polling at its configured interval.

### The honest framing

This is the part worth stating precisely, because the sloppy version overclaims in one
direction and the defensive version underclaims in the other.

**Fencing did its job.** Every ownership-guarded write carries `AND owner_id = $x AND
fencing_token = $y`, so of the racing workers only one ever committed a checkpoint. The
persisted state stayed correct throughout: one `kind='COMPENSATION'` row per step, no
duplicates, correct final status. If you audit `step_outputs` you see a clean unwind. In the
narrow sense of "committed state is exactly-once," the invariant held.

**And that was not enough, because state was never the thing at risk.** Fencing rejects a
*write*. It cannot reject a *side effect that already happened*. Each racing worker had already
executed the refund by the time its checkpoint was refused — the guard turned four concurrent
unwinds into one recorded unwind and three silent duplicate refunds. Exactly-once effects
depend on two things, and only one of them was actually holding: the guard kept the *ledger*
honest while the *money* moved twice.

**What was left carrying the weight was idempotency alone.** `docs/spec.md` requires
compensations to be idempotent — `refund_if_not_already_refunded`, not `refund` — precisely so
that at-least-once execution is survivable. That contract is a backstop for the unavoidable
window (crash after the undo, before its checkpoint). It was instead silently absorbing
*routine, engine-caused* concurrent execution on every single unwind. A backstop doing a
primary's job is a design defect even while nothing visibly breaks, because it means the system
is one non-idempotent compensation away from real loss and nothing tells you.

The demo workflow's compensations are deliberately non-idempotent — plain `INSERT`, no
`ON CONFLICT` — for exactly this reason. Idempotent instrumentation would have made the gate
pass whether or not the engine was correct. The counts have to measure what the engine did.

### The fix, at the claim layer

Mutual exclusion belongs where claiming happens, not inside the unwind. Spec first, per
`CLAUDE.md`:

```sql
status = 'PENDING'
OR (status IN ('RUNNING', 'COMPENSATING')
    AND (lease_expires_at IS NULL OR lease_expires_at < now()))
```

The framing that makes it obvious in review: **a COMPENSATING row under a live lease is an
owned row being actively unwound.** It is not a queue entry waiting for someone — it is work in
progress, exactly as a `RUNNING` row is, and it gets identical ownership protection. Forward and
backward execution become the same kind of thing: a worker holds the row, performs side effects,
checkpoints them, and nobody else may touch it meanwhile. The row becomes re-claimable for
exactly one reason, the same as forward — the owner's lease expired, i.e. the unwinding worker
died — and recovery is then the same path.

`lease_expires_at IS NULL` is what keeps this from costing anything. `begin_compensation` nulls
the lease and sets `run_after = now()`, so a freshly-compensating row is still *instantly*
claimable and the property that mattered — an unwind never sits out a backoff, because money is
already committed in the steps it has to reverse — survives untouched.

Two things that make this cheap rather than invasive: the partial index
`idx_workflows_claimable` predicate stays a superset of the new filter, so the planner can still
prove the index applies and **no migration is needed**; and the alternative — having `_compensate`
push `run_after` forward as its first act — was rejected because it adds a write per unwind and
still leaves a race between the claim and that write.

### Proven by reproducing the double-refund on command

The fix is only worth as much as the demonstration that it is load-bearing. Restoring the old
predicate reproduces the failure deterministically:

- `tests/test_claim.py` — both new claim tests go red.
- The double-refund reappears: `side_effects {'charge:compensate': 2, 'reserve:compensate': 2}`,
  one worker, no kill.
- Restore the predicate → green. `sha256sum -c` confirms the file is byte-identical to its
  pre-experiment state.

Three regression tests pin it, and they pin *both* directions — the second one matters as much
as the first, because an exclusion that strands a dead worker's saga is a worse bug than the one
it fixes:

1. Worker A holds a COMPENSATING row with a live lease → worker B **cannot** claim it, and the
   rejected claim does not bump the fencing token.
2. Let that lease expire → worker B **can** claim it, still COMPENSATING, with a higher token.
   Recovery still works.
3. A freshly-compensating row with a `NULL` lease is claimable immediately — no backoff crept in.

---

## 4. `defer_compensation` survived, narrowed

The Phase 1 stub is gone, but a function of that name remains, reachable from exactly one place:
a worker claiming a COMPENSATING row whose `workflow_type` its build doesn't import, and which
therefore cannot resolve the definition listing the compensations.

Raising there — what the forward path does — is wrong, and for a reason that is now familiar:
`begin_compensation` sets `run_after = now()`, so the same ignorant worker re-claims the row
every lease, forever. `FAILED_DIRTY` would page a human for a rolling deploy that fixes itself
in thirty seconds. And "compensate as best we can" isn't available without the definition. So it
hands the row back with a jittered backoff and writes no status, no error, no checkpoint.

Its docstring says all of this at length, because the next reader will otherwise delete it as
leftover scaffolding — it *looks* exactly like the thing that was just removed.

---

## 5. Fail-proofs, recorded

Every correctness claim in this phase was observed to fail without its mechanism, restored, and
checksum-verified.

| Mechanism removed | What went red | Evidence |
|---|---|---|
| Lease test on COMPENSATING claims | 2 claim tests + the unwind gate | `side_effects {charge:compensate: 2, reserve:compensate: 2}` — one worker, no kill |
| `ORDER BY seq DESC` → `ASC` | 9 tests | `compensations ran as ['refund_wallet', 'void_gateway']` — forward order |
| The `kind='COMPENSATION'` checkpoint write | the unwind gate | `charge:compensate: 2` committed, 0 checkpoint rows |

"A gate that has never been seen to fail is not a gate."

### One correction the third proof forced
The gate originally killed the worker inside the **first** compensation to run. That proves an
unwind resumes, but proves nothing about the idempotency guard — an interrupted undo has no
checkpoint to lose, so the resume would re-run it either way. Moving the kill to the **second**
undo puts a completed, checkpointed refund behind the crash, and the assertion that matters
becomes "the already-compensated step was **not** attempted again."

This is the same correction Phase 1 arrived at from the other direction: durability protects the
*completed* work's checkpoints, not the interrupted one's. Same insight, mirrored.

### The gates also had to be made unable to arm in production
The demo workflows live in `src/sankalp/workflows/` and are imported by every worker, so the
crash-gate mechanism is loaded in a production process. A gate blocking on a `crash_gates` row
that no test will ever insert is a compensation stopped mid-flight with money on one side of it
— presenting as a hung worker, not as a misconfiguration. So arming requires **two** independent
facts (`environment == "test"` **and** `crash_gate_enabled`), never either alone, and an unarmed
gate is a no-op that logs loudly.

---

## 6. The transactional outbox: producer and drain

The table and its rationale shipped with `003_saga.sql`; nothing wrote to it or read from it.
This closes that gap in two halves that deliberately solve different problems.

### The producer half: one transaction, made real

`StepContext.emit(event_type, payload)` buffers events on the context a step or compensation
is running under. Nothing is written to `outbox` at `emit()` time — the payload is only
serialised there, eagerly, so a value that cannot be encoded raises with a traceback pointing
at the step that emitted it rather than surfacing later inside a shielded commit with no way
to tell which buffered event was the bad one.

The write happens in `storage/workflows.py::commit_step_output` /
`commit_compensation_output`, which already opened the one transaction the checkpoint needs.
The `INSERT INTO outbox` joins it, after the ownership-guarded `UPDATE workflows` that takes
the row lock — so a preempted worker returns having written nothing, and "nothing" now
includes its events. That single fact is the entire answer to the dual-write problem: either
the state change and its event both exist, or neither does. There is no third case to reason
about, which is the whole point of putting them in one transaction instead of two writes in
some careful order.

**The retry wrinkle, and why it needed two different fixes.** A forward retry and a
compensation retry look similar from the outside — both re-run something that failed — but
they arrive at the event buffer completely differently, and treating them the same would have
been wrong in one direction or the other:

- A forward retry releases the row (`schedule_retry`) and returns; the next attempt is an
  entirely new claim, building a fresh `StepContext` from scratch. The buffer is safe here by
  construction — but only as an accident of control flow, which is why
  `StepContext.take_pending_events()` also *empties* the buffer on every call. A second commit
  against the same context can then only ever find it empty, which keeps the property true
  even if some future refactor hoists the per-attempt context construction out of the
  executor's loop and this accident stops holding.
- A compensation retry is not like that at all: `_run_compensation` retries **in place**,
  against the *same* `StepContext`, up to `compensation_max_attempts` times. Without an
  explicit `ctx.discard_pending_events()` at the top of each attempt, an attempt that emitted
  and then failed would leave its events sitting in the buffer, and the attempt that finally
  succeeds would commit them alongside its own — one undo, two events. This is not a corner
  case; it is the *ordinary* shape of a compensation retry, so leaving it unhandled would have
  meant a duplicate on every recovered compensation failure, not just an unlucky crash.

Both are proved directly: a step that emits, fails retryably, then succeeds on a second real
claim produces exactly one `outbox` row (`test_a_retried_step_emits_one_event_not_two`); a
compensation that fails once and succeeds on retry produces exactly one
(`test_a_failed_compensation_attempt_does_not_ship_its_events_twice`). Removing either guard
was run and observed to fail before being restored — see the table below.

### The drain half: a different problem, an honestly different guarantee

There is no state change for the drain to be atomic with — only a broker that might be down.
So the drain does not try to extend the producer's atomicity across a process boundary; it
gives up exactly-once *delivery* and keeps exactly-once *effects*, stated plainly rather than
implied: `SELECT ... FOR UPDATE SKIP LOCKED`, publish, `UPDATE ... SET published_at`, all
inside one transaction. A crash between the publish and the mark republishes the batch — the
row is still `published_at IS NULL` — and the only thing that makes that survivable is that
consumers dedupe on `event_id` (`OutboxEvent.id`, i.e. `outbox.id`), which is deliberately
*not* the Redis stream entry ID: a republish gets a fresh stream ID every time, which is
exactly why a separate, stable identifier has to ride along in the payload.

**Why the Postgres transaction stays open across the `XADD`.** `FOR UPDATE SKIP LOCKED` locks
live exactly as long as their transaction does. Commit before publishing and a second drainer
claims the same rows in the gap and double-publishes on the very first pass — no crash
required, no race window to get unlucky in, it simply happens. Holding the transaction open is
what makes N concurrent drainers safe (proved directly:
`test_two_concurrent_drainers_do_not_double_publish` drives two claims against genuinely
separate connections and asserts the claimed id sets are disjoint), and it is also the
operational cost worth naming rather than discovering later: a Redis call that never returns
pins that transaction — and its `xmin` horizon — open indefinitely, on a table `003_saga.sql`
already flags as high-churn. `storage/redis.py` bounds this with a socket timeout
(`outbox_redis_timeout_seconds`), and `outbox_batch_size` bounds how much work sits behind any
one lock.

**A self-deadlock found and fixed during this work, worth recording because it will look
obviously wrong in hindsight and wasn't.** The first version of `drain_once` recorded a failed
publish's `attempts` count from *inside* the `except` block, while the claim's transaction was
still open — i.e. from a second pool connection, while the first was still holding
`FOR UPDATE` locks on the very rows that second connection's `UPDATE outbox SET attempts = ...`
needed to touch. The two connections deadlocked against each other's locks, and the test
proving the failure path hung rather than failed. The fix is `storage/outbox.py`'s
`record_publish_failure` running *after* the exception has propagated out of the `async with`
block that owns the transaction — only once that rollback has actually released the locks is a
second connection safe to write through. The lesson generalises past this one bug: a recovery
write triggered by a failure inside a still-open transaction has to ask what locks that
transaction is still holding, not just what data it needs to change.

**The gate.** `_GatedPublisher` in `engine/drain.py` is built only by `run_drain` — the
composition root — and only when `settings.crash_gate_armed` (the same two-fact contract as
the other two gates: `environment == "test"` *and* `crash_gate_enabled`, never either alone).
It performs the real `XADD` through the publisher it wraps, then records a `step_attempts` row
under the step name `"outbox.drain"` and blocks on `crash_gates`, reusing the exact
instrumentation `002_crash_gate.sql` and `workflows/_instrumentation.py` already built for the
other two gates. `DrainLoop.drain_once` itself carries no branch for any of this — the
production code path a real drain runs is identical whether or not a test is watching.

`tests/test_drain_crash.py` SIGKILLs a real `python -m sankalp.engine.drain` process between
the `XADD` and the mark, at `--count=20` (20/20): the row is confirmed still unpublished after
the kill, a second clean drain republishes it, and the stream ends up holding two entries under
the identical `event_id` — the at-least-once boundary, demonstrated rather than asserted.
`tests/fleet.py::WorkerFleet.launch` gained `module=` and `ready_marker=` parameters so this
reuses the same subprocess/readiness/kill machinery as the other two gates rather than
duplicating it, plus a fix so that launching a second batch of processes on one fleet does not
mistake an already-killed victim from the first batch for one that "exited before it started".

### Fail-proofs, recorded

| Mechanism removed | What went red | Evidence |
|---|---|---|
| The outbox INSERT taken out of `commit_step_output`'s transaction | `test_a_rolled_back_checkpoint_leaves_no_event` | an orphaned event survives a rollback that should have taken it with it |
| `discard_pending_events()` removed from `_run_compensation`'s retry loop | `test_a_failed_compensation_attempt_does_not_ship_its_events_twice` | two events committed for one compensation |
| `take_pending_events()` returning a copy instead of emptying | `test_a_retried_step_emits_one_event_not_two` | the retried step ships two events |
| `FOR UPDATE SKIP LOCKED` removed from the claim | `test_two_concurrent_drainers_do_not_double_publish` | claimed id sets overlap |
| The mark moved outside the claim transaction | `test_a_crash_between_the_xadd_and_the_mark_republishes`, the SIGKILL gate | both stop proving anything -- a second drainer can claim before the first's mark lands |

"A gate that has never been seen to fail is not a gate."

---

## 7. The interview answers banked this phase

1. **"Your fencing tokens worked and you still had a bug — explain."** Fencing rejects a write;
   it cannot un-execute a side effect that already ran. Four workers raced the same unwind, the
   guard let exactly one checkpoint commit, and the ledger looked perfect while three duplicate
   refunds went out. Exactly-once *effects* needs the guard **and** something that stops
   redundant execution in the first place. I had the first and was leaning on compensation
   idempotency — a backstop meant for the unavoidable crash window — to absorb routine
   engine-caused concurrency.

2. **"How did you find it?"** A test failed in 0.79 seconds when it had to launch three
   processes first. The assertion message pointed somewhere else; the *duration* was the real
   signal that the test had never reached the state it claimed to test. I stopped debugging the
   assertion and instrumented the system instead, then cut to one worker to rule out contention
   — which is what showed a worker racing *itself*.

3. **"Why can't forward and compensation share a retry budget?"** Because a saga usually reaches
   COMPENSATING *by exhausting* the forward budget, so sharing gives compensation zero retries
   exactly when it needs them. And `attempt` is incremented by the dequeue query, so it counts
   claims, not failures — a shared budget would be consumed by worker deaths during the unwind
   and page a human though no compensation ever failed.

---

## 8. State at end of Phase 2

- 135 tests green, ruff clean.
- All three crash gates at `--count=20`: forward 40/40, compensation 20/20, outbox drain 20/20.
- Compensation executes end-to-end: reverse `seq`, exactly once per step, `COMPENSATED` or
  `FAILED_DIRTY`, every write ownership- and status-guarded.
- The claim layer gives forward and backward execution one ownership rule and one recovery
  path.
- The outbox is real, not just a table: `StepContext.emit` buffers events that commit in the
  same transaction as a step's or compensation's checkpoint, and a drain loop (`sankalp-drain`,
  or inline in `sankalp-worker` behind `outbox_drain_in_worker`) claims unpublished rows with
  `FOR UPDATE SKIP LOCKED`, `XADD`s them to a Redis Stream, and marks them published --
  at-least-once, with consumers expected to dedupe on `event_id`.
- All fail-experiments reverted; `sha256sum -c` clean on every file touched by them.
- Not built: ledger integration in a real workflow, the 1,000-workflow soak (`make test-soak`
  still selects nothing), resilience layer, observability. Those close Phase 2 and open
  Phases 3–4.
