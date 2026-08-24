# Sankalp — Phase 1 Build Log (The Durable Engine)

Append to `docs/build-log.md` after the day-1 section. Everything built in Phase 1, with
the reasoning an interviewer will probe. The code is in git; this is the *why*, which only
lives here.

**Phase 1 commits (on `main`):**
- `fe6093f` — dequeue query + claim tests
- (definition API + errors)
- `f8f40ed` — executor, worker, lease with fencing-guarded writes
- `5d24844` — worker tests + fix 30s hang on external cancel
- `30ec791` — crash-gate migration
- `42219b6` — crash-gate: demo workflow, test, spec

**The Phase 1 guarantee, proven:** SIGKILL the process holding a workflow mid-step — no
`finally`, no drain, no cleanup — and another process resumes from the last committed
checkpoint with no step's side effect executing twice. Demonstrated by
`tests/test_crash.py` passing 40/40 at `--count=20`, and observed to *fail* when either of
its two recovery mechanisms is removed.

---

## 1. The dequeue query — claiming is recovery

One `UPDATE ... FROM (SELECT ... FOR UPDATE SKIP LOCKED) RETURNING` claims a batch of
runnable workflows. The whole of crash recovery lives in one clause of its `WHERE`:

```sql
status IN ('PENDING', 'COMPENSATING')
OR (status = 'RUNNING' AND lease_expires_at < now())
```

A dead worker's rows become claimable again for exactly one reason: their lease drifts into
the past. **There is no reaper daemon** — a separate recovery process would only duplicate
this predicate and add a second thing that can be wrong.

### The interview answer: SKIP LOCKED is a scalability device, not a safety device
Two workers hit the same first row at the same instant. Postgres row locks live in the
tuple header (`xmax`), acquired by a compare-and-set on the buffer page, so one wins with no
tie. What the loser does is the whole question:

- **With `SKIP LOCKED`:** the lock acquisition is conditional; it fails, the row drops from
  the result, the worker moves to the next. It never waits.
- **Without it (plain `FOR UPDATE`):** the loser calls `XactLockTableWait`, sleeps until the
  winner's transaction ends, then — because we're at READ COMMITTED — runs EvalPlanQual,
  re-fetches the now-`RUNNING` row, re-evaluates the qual, finds it no longer matches, and
  moves on.

So removing `SKIP LOCKED` is a **performance** bug, not a correctness one. No double-claim
happens either way — the READ COMMITTED qual recheck prevents it. What you lose is
throughput: N workers convoy behind one row. The no-double-execution guarantee doesn't rest
on `SKIP LOCKED` at all — it rests on the `step_outputs` primary key and the fencing token.

**The caveat that shows depth:** the qual recheck only saves you *because the lease is
positive*. With `lease_seconds = 0`, the re-evaluated row would satisfy
`lease_expires_at < now()` immediately and the loser would re-claim the winner's work.
`Settings.lease_duration_seconds` is `ge=1`, which is what keeps that hypothetical.

---

## 2. The definition API — retry is opt-in, never opt-out

`@workflow(type)` / `@step(seq=...)` / `@<step>.compensate`. A registry maps `workflow_type`
to an ordered step list, built at import time.

### Unrecognized exceptions default to terminal (compensate), not retryable
The two mistakes aren't symmetric, and only one is silent:

- **A wrong retry** re-runs the step body. If the step failed *after* its side effect landed
  (a gateway call that timed out on the response), retrying moves money twice and the
  workflow still ends `SUCCESS`. Nothing pages anyone; you find it in reconciliation days
  later. Unbounded and invisible.
- **A wrong compensation** unwinds a workflow that might have succeeded on a second try. But
  compensations are idempotent by contract (`refund_if_not_already_refunded`), so undoing a
  step that did nothing is a no-op. The outcome is visible — a `COMPENSATED` row, or
  `FAILED_DIRTY` which pages by design. Bounded and discoverable.

The loud wrong answer beats the quiet one. And nobody can know whether an arbitrary
exception is safe to re-run — only the step's author knows which of *their* failures are.
So steps opt into retrying (`retry_on=`), never out of it.

### seq is validated at import, and it must be contiguous
A duplicate `seq` doesn't just scramble forward order — it destroys the *unwind* order.
`step_outputs.seq` is the only record of execution order that survives the process, read
back `ORDER BY seq DESC` in a different process hours later. A tie has no tiebreaker in that
query, so Postgres may return them in either order — and it need not mirror the order they
actually ran. You could release a reservation before undoing the transfer drawn against it.
Saga correctness *is* "undo in reverse of what happened"; a tie erases the one fact needed
to reconstruct that.

Checked at import because it's a property of the code, knowable with no input. Deferring it
to first-run means the failure lands after a worker has claimed real money-moving work, in
the retry/compensate branch, where a definition bug is neither retryable nor safely
compensable — a typo becomes `FAILED_DIRTY` and a page, hours after a green deploy. At
import, every worker refuses to boot uniformly, before owning any work.

---

## 3. The executor — five writes, five guards

`execute_workflow` loads committed `step_outputs` in one query, replays any step whose row
already exists (a lookup, not a re-execution), and runs the rest, committing each step's
checkpoint and the workflow's new position in a single transaction.

### Every write is ownership-guarded, including both failure paths
All five writes — checkpoint, `finish_success`, `schedule_retry`, `begin_compensation`,
`renew_lease` — end in `WHERE id = $1 AND owner_id = $2 AND fencing_token = $3`, and every
call site acts on the zero-rows answer (raise `PreemptedError`, or return `PREEMPTED`).

The bug this prevents, and the one most likely to be missed: it's obvious to guard the
*step-output commit*. The easy mistake is forgetting that the *failure*-path status writes
need the identical guard. If a preempted zombie could flip a workflow back to `PENDING`
(retry) or `COMPENSATING` after a live worker already moved it forward, you'd get the exact
double-execution the phase exists to prevent.

### Three deliberate placements in the try/except
- `get_definition` sits **outside** the try. An unknown `workflow_type` is a fact about
  *this deployment* (this build didn't import that definition), not a workflow failure.
  Compensating it would unwind real money because a worker was running stale code.
- `except asyncio.CancelledError` **re-raises before** the generic handler. A hard shutdown
  mid-step must leave the row alone so its lease expires and another worker resumes. Marking
  it failed would compensate a workflow whose only problem is that this process was told to
  stop.
- The guarded `UPDATE` runs **before** the `INSERT` inside the checkpoint transaction, so a
  preempted worker returns before it can collide on the `step_outputs` primary key.

### The shielded checkpoint commit — and why shield alone is wrong
By the time the checkpoint is being written, the step's side effect has already happened;
only the record of it is outstanding. A cancellation landing in that gap discards the
checkpoint, so the resume re-executes a step that already moved money. Idempotency-by-
construction is Phase 2, so today that's a *real* double-execution.

The fix is shield-*and-await*, not shield alone:

```python
commit = asyncio.ensure_future(commit_step_output(...))
try:
    return await asyncio.shield(commit)
except asyncio.CancelledError:
    committed = await commit   # <-- this line is the point
    ...
    raise
```

Bare `asyncio.shield` detaches the write but lets the coroutine unwind immediately — the
worker's drain would then return and *close the pool* with the write still in flight. The
second `await commit` in the handler is what makes the drain wait, because the wait happens
*inside the task the drain is already gathering*.

**Proven, not asserted:** `test_a_cancel_while_checkpointing_still_writes_the_checkpoint`
holds the write open deterministically — a second connection takes `SELECT ... FOR UPDATE`
on the row, so the checkpoint's guarded UPDATE parks on that lock and the cancel lands
squarely inside the write. Verified both ways: shield removed → red on
`'debit_wallet' in {}`; shield restored → green. A test watched to fail for the right reason
is worth ten only ever seen green.

---

## 4. The worker — and a self-correction worth more than the fix

Capacity-bounded claiming (`asyncio.Semaphore`), one lease renewer per in-flight workflow,
SIGTERM graceful shutdown (stop claiming, let in-flight commit within a grace period).

### The `_drain` investigation
The claim going in was that `_drain` orphans in-flight tasks on external cancel. Reproducing
it proved that *wrong*: `Task.cancel()` delivers `CancelledError` exactly once, at the
suspension point, and by the time `run()`'s `finally` executes that delivery is spent — so
the drain's cancel-and-gather runs to completion. The shield's protection was never void.

The *real* defect the probes found: an external cancel was being honoured as a graceful
stop, so `_drain` waited out the full `worker_shutdown_grace_seconds` before cancelling
anything — a **30-second hang** on `cancel()` with the default setting. Fix: a `_cancelled`
flag set in the `except CancelledError` branch makes the drain forgo grace on cancel while
still cancelling and awaiting every task. Regression-proven: grace unconditional → test
takes 5.01s (red); with the fix → passes.

The lesson to keep: "I investigated, found my assumption wrong, and found the actual bug" is
a stronger story than "I fixed the bug I set out to fix." Reproduce before you fix.

### test_worker.py — the assertion that isn't trivially satisfiable
`test_a_worker_does_not_claim_more_than_it_can_run` is the one that matters. A semaphore-cap
test (`high_water <= 2`) is satisfied by a *serial* worker and proves nothing. Proving that
at concurrency 1 with 8 queued, exactly one row is *owned* at any instant, proves the worker
doesn't greedily claim work it can't run — which is what protects the DB under load. The
SIGTERM test uses real `os.kill` and waits for the step to start before signalling, so the
handler is installed and the signal can't fall through to killing pytest.

---

## 5. The crash gate — the milestone, and its central insight

`tests/test_crash.py`: launch three real `python -m sankalp.engine.worker` OS processes,
submit a workflow, `SIGKILL` the one running step 2, assert the guarantee held. 40/40 at
`--count=20`.

### Why SIGKILL specifically
Every other crash simulation in the suite is cooperative. `task.cancel()` unwinds through
`except CancelledError`. SIGTERM runs the drain, which *lets in-flight work finish*. A worker
that gets to run its handlers is not what the guarantee is about. SIGKILL is a process that
stops existing between two instructions — no `finally`, no drain, no flush.

### The two-table mechanism that makes the assertion exact
- `side_effects` — written **uncommitted**, inside the step's transaction, commits last. A
  SIGKILL aborts that transaction, so the killed attempt leaves **zero** rows. This is the
  *effect*.
- `step_attempts` — written on a **separate connection, committed immediately**, before the
  slow work. A kill leaves the row. This is the *evidence it ran*.

`step_attempts = 2, side_effects = 1` means "attempted twice, took effect once" — provable,
not hoped for. Without the attempt count the test could pass by killing a worker that never
reached step 2. Separating *evidence it ran* from *the effect itself* is the technique.

`side_effects` deliberately has **no** unique constraint and the steps use no `ON CONFLICT` —
documented in the migration and the spec. A swallowed duplicate would make "exactly one row"
hold whether or not recovery worked, and the gate would assert nothing.

### The central insight (from a fail-experiment that corrected itself)
Setting out to prove double-execution by skipping the *killed step's* checkpoint changed
nothing — the killed attempt never reaches its checkpoint anyway, so the replay runs it once
regardless. The checkpoint that actually prevents a re-execution is **step 1's** — the
*completed* step the resume must replay as a lookup.

So: **durability protects the completed steps' checkpoints, not the killed step's.** The
killed step re-runs (at-least-once); its predecessors don't, because their checkpoints
replay as lookups. That's "resume from where it stopped," stated more precisely than the
spec had it — and there's test evidence: skip `reserve_funds`' checkpoint → 0 checkpoints →
2 attempts → 2 side effects; skip `hold`'s → no change.

### Both fail-proofs, recorded in the spec
- Delete the lease-expiry clause from the dequeue query → both variants **time out** waiting
  for another worker. Nothing recovers a dead worker's rows.
- Skip a completed step's checkpoint → that step shows 2 attempts and **2 side_effects
  rows**. The checkpoint is what makes the resume replay instead of re-execute.

"A gate that has never been seen to fail is not a gate."

### The honest limit, stated in the test docstring
For steps 1 and 3 the side effect commits *just before* `commit_step_output`, so a kill in
that microsecond window would produce two rows on replay. That's at-least-once execution
working as designed — and exactly why the guarantee is exactly-once **effects** via
at-least-once execution plus idempotency, never exactly-once delivery. Phase 2's transactional
outbox and idempotent steps close that window.

---

## 6. The three interview answers banked this phase

1. **"Why is SKIP LOCKED safe?"** — It isn't a safety device, it's a scalability one.
   Removing it is a performance bug; the READ COMMITTED qual recheck prevents double-claim
   either way. Safety rests on the `step_outputs` PK and the fencing token. (Caveat: the
   recheck only holds because the lease is positive.)

2. **"How do you get exactly-once effects?"** — At-least-once execution plus idempotency. The
   killed step re-runs; completed steps replay from checkpoints as lookups. Fencing tokens on
   every ownership-scoped write mean a preempted zombie's writes match zero rows. It's
   exactly-once *effects*, not delivery — and I can show you the microsecond window where a
   step runs twice, which Phase 2 closes.

3. **"Prove your recovery works."** — A test that SIGKILLs a real process mid-step, 20×, and
   goes red the instant I remove the lease-expiry claim or a completed step's checkpoint. The
   guarantee is two integers: attempted twice, took effect once.

---

## 7. State at end of Phase 1

- 68 tests green, ruff clean.
- Engine: claim, definition, executor, worker, lease — all writes fencing-guarded, every
  correctness claim backed by a test observed to fail without its mechanism.
- `main` clean; `git diff src/` byte-identical to HEAD after all fail-experiments reverted.
- Not built: outbox, ledger, compensation execution end-to-end, resilience layer,
  observability. Those are Phases 2–4.
