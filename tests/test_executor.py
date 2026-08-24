"""The execution flow, proved against a real database. See docs/spec.md, "Execution Flow".

These are the assertions the Phase 1 guarantee reduces to:

  * every step runs once, in seq order, and each one's output is checkpointed with it;
  * a step that already has a ``step_outputs`` row is **not called again** -- it is replayed
    from its committed output, which is what makes a resume after a crash identical to never
    having stopped;
  * a worker that has been preempted writes nothing at all, including the checkpoint for the
    step it just finished;
  * a retryable failure returns the workflow to the queue with its checkpoints intact, and
    the next attempt resumes at the step that failed;
  * anything else -- terminal, unclassified, or retries exhausted -- goes to COMPENSATING.
"""

from __future__ import annotations

import asyncio
import json
import uuid

import pytest

from sankalp.config import Settings
from sankalp.engine.definition import StepContext, clear_registry, step, workflow
from sankalp.engine.errors import RetryableError, TerminalError
from sankalp.engine.executor import ExecutionResult, execute_workflow
from sankalp.engine.lease import Lease
from sankalp.storage.queue import claim_workflows

LEASE = 30
WORKFLOW_TYPE = "payment_transfer"


@pytest.fixture(autouse=True)
def isolated_registry():
    """Each test registers its own throwaway definition into an empty registry."""
    clear_registry()
    yield
    clear_registry()


@pytest.fixture
def settings() -> Settings:
    return Settings(max_attempts=5, backoff_cap_seconds=60, lease_duration_seconds=LEASE)


# ---------------------------------------------------------------------------
# Fixtures of the domain: a three-step transfer that records what it ran.
# ---------------------------------------------------------------------------


def define_transfer(calls: list[str], **failures) -> type:
    """debit -> gateway -> ledger, appending to ``calls`` as each step executes.

    ``calls`` is a closure rather than instance state on purpose: the engine instantiates a
    fresh workflow object per execution, so anything recorded on ``self`` would vanish
    exactly where these tests need to look -- across a resume.

    ``failures`` maps a step name to an exception to raise instead of returning.
    """

    def maybe_fail(name: str) -> None:
        exc = failures.get(name)
        if exc is not None:
            raise exc

    @workflow(WORKFLOW_TYPE)
    class PaymentTransfer:
        @step(seq=1)
        async def debit_wallet(self, ctx: StepContext) -> dict[str, int]:
            calls.append("debit_wallet")
            maybe_fail("debit_wallet")
            return {"debited_minor": ctx.input["amount_minor"]}

        @step(seq=2)
        async def call_gateway(self, ctx: StepContext) -> dict[str, str]:
            calls.append("call_gateway")
            maybe_fail("call_gateway")
            return {"reference": f"gw-{ctx.output_of('debit_wallet')['debited_minor']}"}

        @step(seq=3)
        async def write_ledger(self, ctx: StepContext) -> dict[str, str]:
            calls.append("write_ledger")
            maybe_fail("write_ledger")
            return {"posted": ctx.output_of("call_gateway")["reference"]}

    return PaymentTransfer


async def claim_one(pool, owner: str = "worker-a", lease: int = LEASE):
    """Claim exactly one workflow, the way the worker does."""
    async with pool.acquire() as conn:
        claimed = await claim_workflows(conn, owner, lease, 1)
    assert len(claimed) == 1, f"expected one claimable workflow, got {len(claimed)}"
    return claimed[0]


async def steal(pool, workflow_id: uuid.UUID, owner: str = "worker-b") -> None:
    """Another worker claims the row: new owner, higher fencing token.

    Exactly what the dequeue query does to a lease that expired while its holder was
    stalled -- which is the situation every ownership guard in the engine exists for.
    """
    await pool.execute(
        """
        UPDATE workflows
        SET owner_id = $2,
            fencing_token = fencing_token + 1,
            lease_expires_at = now() + interval '30 seconds'
        WHERE id = $1
        """,
        workflow_id,
        owner,
    )


async def row(pool, workflow_id: uuid.UUID):
    return await pool.fetchrow("SELECT * FROM workflows WHERE id = $1", workflow_id)


async def checkpoints(pool, workflow_id: uuid.UUID) -> dict[str, dict]:
    records = await pool.fetch(
        "SELECT step_name, seq, kind, output FROM step_outputs WHERE workflow_id = $1",
        workflow_id,
    )
    return {r["step_name"]: dict(r) for r in records}


# ---------------------------------------------------------------------------
# 1. The happy path.
# ---------------------------------------------------------------------------


async def test_every_step_runs_in_seq_order_and_is_checkpointed(pool, insert_workflow, settings):
    calls: list[str] = []
    define_transfer(calls)
    workflow_id = await insert_workflow(input={"amount_minor": 25_000})

    claimed = await claim_one(pool)
    result = await execute_workflow(pool, claimed, settings=settings)

    assert result is ExecutionResult.SUCCESS
    assert calls == ["debit_wallet", "call_gateway", "write_ledger"]

    saved = await checkpoints(pool, workflow_id)
    assert set(saved) == {"debit_wallet", "call_gateway", "write_ledger"}
    assert [saved[n]["seq"] for n in calls] == [1, 2, 3]
    assert all(r["kind"] == "FORWARD" for r in saved.values())
    assert json.loads(saved["debit_wallet"]["output"]) == {"debited_minor": 25_000}


async def test_success_releases_the_lease_and_records_the_output(pool, insert_workflow, settings):
    calls: list[str] = []
    define_transfer(calls)
    workflow_id = await insert_workflow(input={"amount_minor": 700})

    await execute_workflow(pool, await claim_one(pool), settings=settings)

    final = await row(pool, workflow_id)
    assert final["status"] == "SUCCESS"
    # Released, so nothing about this row looks like work in progress any more.
    assert final["owner_id"] is None
    assert final["lease_expires_at"] is None
    assert final["error"] is None
    assert json.loads(final["output"]) == {
        "debit_wallet": {"debited_minor": 700},
        "call_gateway": {"reference": "gw-700"},
        "write_ledger": {"posted": "gw-700"},
    }


# ---------------------------------------------------------------------------
# 2. Replay. The property the whole engine is built to have.
# ---------------------------------------------------------------------------


async def test_a_checkpointed_step_is_replayed_not_re_executed(pool, insert_workflow, settings):
    """A worker died after step 1 committed. The next one must not debit the wallet twice.

    The checkpointed output is deliberately *different* from what a re-run would produce, so
    "step 2 saw the committed value" cannot be satisfied by accidentally re-executing step 1.
    """
    calls: list[str] = []
    define_transfer(calls)
    workflow_id = await insert_workflow(input={"amount_minor": 25_000})
    await pool.execute(
        "INSERT INTO step_outputs (workflow_id, step_name, seq, output) "
        "VALUES ($1, 'debit_wallet', 1, $2::jsonb)",
        workflow_id,
        json.dumps({"debited_minor": 111}),
    )

    result = await execute_workflow(pool, await claim_one(pool), settings=settings)

    assert result is ExecutionResult.SUCCESS
    assert calls == ["call_gateway", "write_ledger"], "a checkpointed step re-executed"
    # Step 2 read the committed output, not a fresh one.
    saved = await checkpoints(pool, workflow_id)
    assert json.loads(saved["call_gateway"]["output"]) == {"reference": "gw-111"}
    assert len(saved) == 3


async def test_a_step_whose_output_was_null_is_still_done(pool, insert_workflow, settings):
    """Replay tests membership, never truthiness.

    A step that legitimately returns nothing is checkpointed with a JSON null. Treat that as
    "not done" and it re-executes on every resume -- a duplicated side effect produced by
    testing a dict value instead of a key.
    """
    calls: list[str] = []
    define_transfer(calls)
    workflow_id = await insert_workflow()
    await pool.execute(
        "INSERT INTO step_outputs (workflow_id, step_name, seq, output) "
        "VALUES ($1, 'debit_wallet', 1, 'null'::jsonb)",
        workflow_id,
    )

    await execute_workflow(pool, await claim_one(pool), settings=settings)

    assert "debit_wallet" not in calls


async def test_a_fully_checkpointed_workflow_runs_no_steps_at_all(pool, insert_workflow, settings):
    """Killed between the last checkpoint and the SUCCESS transition -- the narrowest window
    there is. The resume must finish the workflow without executing anything."""
    calls: list[str] = []
    define_transfer(calls)
    workflow_id = await insert_workflow()
    for seq, name in enumerate(["debit_wallet", "call_gateway", "write_ledger"], start=1):
        await pool.execute(
            "INSERT INTO step_outputs (workflow_id, step_name, seq, output) "
            "VALUES ($1, $2, $3, $4::jsonb)",
            workflow_id,
            name,
            seq,
            json.dumps({"replayed": name}),
        )

    result = await execute_workflow(pool, await claim_one(pool), settings=settings)

    assert result is ExecutionResult.SUCCESS
    assert calls == []
    assert (await row(pool, workflow_id))["status"] == "SUCCESS"


# ---------------------------------------------------------------------------
# 3. Preemption. A worker that lost the row must write nothing.
# ---------------------------------------------------------------------------


async def test_a_preempted_worker_abandons_the_workflow_without_writing(
    pool, insert_workflow, settings
):
    """Step 1 finishes; by then another worker owns the row. The checkpoint must roll back.

    Committing it would let a stalled worker checkpoint a step against a workflow the new
    owner is already replaying -- and the guarded UPDATE and the INSERT share one
    transaction precisely so that cannot half-happen.
    """
    calls: list[str] = []
    workflow_id = await insert_workflow()

    @workflow(WORKFLOW_TYPE)
    class Stalling:
        @step(seq=1)
        async def debit_wallet(self, ctx: StepContext) -> dict[str, int]:
            calls.append("debit_wallet")
            await steal(pool, ctx.workflow_id)  # the lease lapsed while we were working
            return {"debited_minor": 1}

        @step(seq=2)
        async def call_gateway(self, ctx: StepContext) -> dict[str, str]:
            calls.append("call_gateway")
            return {"reference": "gw-1"}

    claimed = await claim_one(pool)
    result = await execute_workflow(pool, claimed, settings=settings)

    assert result is ExecutionResult.PREEMPTED
    assert calls == ["debit_wallet"], "execution continued after being preempted"
    assert await checkpoints(pool, workflow_id) == {}, "a preempted worker wrote a checkpoint"

    # And the row still belongs entirely to the worker that took it.
    final = await row(pool, workflow_id)
    assert final["owner_id"] == "worker-b"
    assert final["status"] == "RUNNING"
    assert final["fencing_token"] == claimed.fencing_token + 1


async def test_a_lost_lease_stops_the_next_step_before_it_runs(pool, insert_workflow, settings):
    """The second defense: renew before each step, and never start one on a lease we lost.

    Distinct from ``test_a_step_that_renews_a_lost_lease_learns_it_was_preempted`` below,
    which drives ``Lease.renew_or_raise`` from inside a step that is already running. This
    one drives ``Lease.renew_if_needed`` -- the threshold branch the executor calls *before*
    invoking a step -- and the difference is the whole point: here the side effect never
    happens at all, rather than happening and then being refused a checkpoint.

    The timing is one-directional on purpose. The lease is given a 50ms duration and then
    left to sit for 150ms, so ``seconds_remaining`` is not merely under the renewal threshold
    (a third of 50ms) but past zero. A slow machine can only make it more expired, never
    less, so there is no schedule under which this test flickers.
    """
    calls: list[str] = []
    define_transfer(calls)
    workflow_id = await insert_workflow()

    claimed = await claim_one(pool)
    await steal(pool, workflow_id)  # our fencing token is now one behind the row's
    spent = Lease(pool, claimed, duration_seconds=0.05, renew_divisor=3)
    await asyncio.sleep(0.15)
    assert spent.seconds_remaining < 0, "the lease under test has not actually run out"

    result = await execute_workflow(pool, claimed, lease=spent, settings=settings)

    assert result is ExecutionResult.PREEMPTED
    assert calls == [], "a step ran on a lease this worker no longer held"
    assert spent.lost, "the renewal was accepted despite a stale fencing token"
    assert await checkpoints(pool, workflow_id) == {}

    # The rejection came from the ownership guard, not from the clock: the row is intact and
    # still belongs to the worker that took it.
    final = await row(pool, workflow_id)
    assert final["owner_id"] == "worker-b"
    assert final["fencing_token"] == claimed.fencing_token + 1


# ---------------------------------------------------------------------------
# 4. Retry: back to the queue, checkpoints intact.
# ---------------------------------------------------------------------------


async def test_a_retryable_failure_returns_the_workflow_to_the_queue(
    pool, insert_workflow, settings
):
    calls: list[str] = []
    define_transfer(calls, call_gateway=RetryableError("gateway timed out"))
    workflow_id = await insert_workflow()

    result = await execute_workflow(pool, await claim_one(pool), settings=settings)

    assert result is ExecutionResult.RETRY_SCHEDULED
    final = await row(pool, workflow_id)
    assert final["status"] == "PENDING"
    assert final["owner_id"] is None
    assert final["lease_expires_at"] is None
    assert "RetryableError" in final["error"] and "call_gateway" in final["error"]
    # attempt is incremented by the claim, not here -- doing both would halve max_attempts.
    assert final["attempt"] == 1

    # The step that succeeded stays checkpointed; the one that failed did not.
    assert set(await checkpoints(pool, workflow_id)) == {"debit_wallet"}


async def test_the_retry_is_scheduled_into_the_future(pool, insert_workflow, settings):
    """run_after is what keeps a backed-off workflow out of the next dequeue batch."""
    define_transfer([], debit_wallet=RetryableError("down"))
    workflow_id = await insert_workflow()

    await execute_workflow(pool, await claim_one(pool), settings=settings)

    final = await row(pool, workflow_id)
    assert final["run_after"] > await pool.fetchval("SELECT now()")
    # And it really is invisible to a claimer until then.
    async with pool.acquire() as conn:
        assert await claim_workflows(conn, "worker-b", LEASE, 10) == []


async def test_the_next_attempt_resumes_at_the_step_that_failed(pool, insert_workflow, settings):
    """Retry and replay together -- the whole reason a retry is cheaper than a restart."""
    calls: list[str] = []
    fail_once = {"call_gateway": RetryableError("gateway timed out")}
    workflow_id = await insert_workflow()

    @workflow(WORKFLOW_TYPE)
    class FlakyTransfer:
        @step(seq=1)
        async def debit_wallet(self, ctx: StepContext) -> dict[str, int]:
            calls.append("debit_wallet")
            return {"debited_minor": ctx.input["amount_minor"]}

        @step(seq=2, retry_on=RetryableError)
        async def call_gateway(self, ctx: StepContext) -> dict[str, str]:
            calls.append("call_gateway")
            exc = fail_once.pop("call_gateway", None)
            if exc is not None:
                raise exc
            return {"reference": "gw-ok"}

    assert await execute_workflow(pool, await claim_one(pool), settings=settings) is (
        ExecutionResult.RETRY_SCHEDULED
    )
    # Serve out the backoff.
    await pool.execute("UPDATE workflows SET run_after = now() WHERE id = $1", workflow_id)

    result = await execute_workflow(pool, await claim_one(pool), settings=settings)

    assert result is ExecutionResult.SUCCESS
    assert calls == ["debit_wallet", "call_gateway", "call_gateway"], (
        "the debit re-executed on retry -- the wallet was charged twice"
    )
    assert (await row(pool, workflow_id))["attempt"] == 2


# ---------------------------------------------------------------------------
# 5. Compensate: terminal, unclassified, or out of attempts.
# ---------------------------------------------------------------------------


async def test_a_terminal_failure_compensates_with_attempts_still_left(
    pool, insert_workflow, settings
):
    define_transfer([], call_gateway=TerminalError("account frozen"))
    workflow_id = await insert_workflow()

    result = await execute_workflow(pool, await claim_one(pool), settings=settings)

    assert result is ExecutionResult.COMPENSATING
    final = await row(pool, workflow_id)
    assert final["status"] == "COMPENSATING"
    assert final["owner_id"] is None
    assert "account frozen" in final["error"]
    assert final["attempt"] < final["max_attempts"], "this was not about running out of tries"


async def test_an_unclassified_exception_is_treated_as_terminal(pool, insert_workflow, settings):
    """The default points at compensation on purpose: an unrecognised failure is not evidence
    that re-running the step is safe, and compensation is idempotent by contract."""
    define_transfer([], call_gateway=RuntimeError("something nobody predicted"))
    workflow_id = await insert_workflow()

    result = await execute_workflow(pool, await claim_one(pool), settings=settings)

    assert result is ExecutionResult.COMPENSATING
    assert "RuntimeError" in (await row(pool, workflow_id))["error"]


async def test_exhausted_retries_compensate(pool, insert_workflow, settings):
    """attempt 5 of 5 fails retryably: there is nothing left to retry with."""
    define_transfer([], call_gateway=RetryableError("still timing out"))
    # The claim increments attempt, so 4 -> 5, which is max_attempts.
    workflow_id = await insert_workflow(attempt=4)

    claimed = await claim_one(pool)
    assert claimed.attempt == claimed.max_attempts

    result = await execute_workflow(pool, claimed, settings=settings)

    assert result is ExecutionResult.COMPENSATING
    final = await row(pool, workflow_id)
    assert final["status"] == "COMPENSATING"
    # The completed step keeps its checkpoint -- it is what the unwind has to undo.
    assert set(await checkpoints(pool, workflow_id)) == {"debit_wallet"}


async def test_compensation_is_claimable_immediately(pool, insert_workflow, settings):
    """An unwind must not sit out a backoff left over from the retry before it: money is
    already committed in the steps it has to reverse."""
    workflow_id = await insert_workflow()
    # One definition whose failure changes between attempts -- a second @workflow on the same
    # type is rejected at registration, and rightly so.
    failure: list[Exception] = [RetryableError("down")]

    @workflow(WORKFLOW_TYPE)
    class Failing:
        @step(seq=1)
        async def debit_wallet(self, ctx: StepContext) -> dict[str, bool]:
            raise failure[0]

    # Attempt 1 fails retryably, which pushes run_after out by a backoff...
    assert await execute_workflow(pool, await claim_one(pool), settings=settings) is (
        ExecutionResult.RETRY_SCHEDULED
    )
    assert (await row(pool, workflow_id))["run_after"] > await pool.fetchval("SELECT now()")

    # ...and attempt 2 fails terminally, so the row carries that stale future run_after into
    # COMPENSATING unless the transition resets it.
    await pool.execute("UPDATE workflows SET run_after = now() WHERE id = $1", workflow_id)
    failure[0] = TerminalError("rejected")
    assert await execute_workflow(pool, await claim_one(pool), settings=settings) is (
        ExecutionResult.COMPENSATING
    )

    async with pool.acquire() as conn:
        reclaimed = await claim_workflows(conn, "worker-b", LEASE, 10)
    assert [w.id for w in reclaimed] == [workflow_id]
    assert reclaimed[0].status == "COMPENSATING"


async def test_a_non_serialisable_output_is_terminal(pool, insert_workflow, settings):
    """The output has to survive a process boundary -- it is handed to the compensation on
    replay, possibly hours later and somewhere else."""
    workflow_id = await insert_workflow()

    @workflow(WORKFLOW_TYPE)
    class ReturnsAnObject:
        @step(seq=1)
        async def debit_wallet(self, ctx: StepContext) -> object:
            return object()

    result = await execute_workflow(pool, await claim_one(pool), settings=settings)

    assert result is ExecutionResult.COMPENSATING
    assert "JSON-serialisable" in (await row(pool, workflow_id))["error"]
    assert await checkpoints(pool, workflow_id) == {}


async def test_the_replay_context_cannot_be_mutated_by_a_step(pool, insert_workflow, settings):
    """A step writing into ctx.outputs would change what a later step reads without that ever
    reaching step_outputs -- so a clean run and a resume would diverge."""
    workflow_id = await insert_workflow()

    @workflow(WORKFLOW_TYPE)
    class Meddling:
        @step(seq=1)
        async def debit_wallet(self, ctx: StepContext) -> dict[str, int]:
            ctx.outputs["smuggled"] = {"not": "checkpointed"}  # type: ignore[index]
            return {}

    result = await execute_workflow(pool, await claim_one(pool), settings=settings)

    assert result is ExecutionResult.COMPENSATING
    assert "TypeError" in (await row(pool, workflow_id))["error"]


async def test_a_cancel_while_checkpointing_still_writes_the_checkpoint(
    pool, connect, insert_workflow, settings
):
    """Shutdown grace running out must not throw away a step that has already run.

    By the time the checkpoint is being written the side effect has happened and only the
    record of it is outstanding. Without the shield in ``_commit_finished_step`` the
    cancellation aborts that write, no ``step_outputs`` row appears, and the resume
    re-executes a step that already moved money. That is a real double-execution today:
    idempotent-by-construction steps are Phase 2, so nothing downstream would absorb it.

    The window is held open deterministically rather than by luck -- another connection locks
    the workflow row, so the checkpoint's guarded UPDATE parks on that lock and the cancel
    lands squarely inside the write.
    """
    workflow_id = await insert_workflow()
    ran = asyncio.Event()

    @workflow(WORKFLOW_TYPE)
    class SideEffecting:
        @step(seq=1)
        async def debit_wallet(self, ctx: StepContext) -> dict[str, int]:
            ran.set()
            return {"debited_minor": 500}

    # Claim before locking: the dequeue query uses SKIP LOCKED and would skip a locked row.
    claimed = await claim_one(pool)
    blocker = await connect()
    lock = blocker.transaction()
    await lock.start()
    await blocker.execute("SELECT 1 FROM workflows WHERE id = $1 FOR UPDATE", workflow_id)

    task = asyncio.create_task(execute_workflow(pool, claimed, settings=settings))
    await ran.wait()
    await asyncio.sleep(0.1)  # the checkpoint's UPDATE is now parked on the row lock

    task.cancel()
    await asyncio.sleep(0.1)  # the cancellation has been delivered, and shielded
    await lock.rollback()  # release the row -- the shielded write can now finish

    with pytest.raises(asyncio.CancelledError):
        await task

    saved = await checkpoints(pool, workflow_id)
    assert "debit_wallet" in saved, "the checkpoint was discarded along with the cancellation"
    assert json.loads(saved["debit_wallet"]["output"]) == {"debited_minor": 500}


# ---------------------------------------------------------------------------
# 6. The lease, from inside a step.
# ---------------------------------------------------------------------------


async def test_a_step_can_extend_its_own_lease(pool, insert_workflow, settings):
    """A long step that checkpoints its own progress pushes the lease out itself, rather than
    being stolen mid-flight by a worker that saw it expire."""
    await insert_workflow()
    observed: list[object] = []

    @workflow(WORKFLOW_TYPE)
    class SlowStep:
        @step(seq=1)
        async def debit_wallet(self, ctx: StepContext) -> dict[str, bool]:
            observed.append(
                await pool.fetchval("SELECT lease_expires_at FROM workflows WHERE id = $1",
                                    ctx.workflow_id)
            )
            await asyncio.sleep(0.05)
            await ctx.renew_lease()
            observed.append(
                await pool.fetchval("SELECT lease_expires_at FROM workflows WHERE id = $1",
                                    ctx.workflow_id)
            )
            return {"ok": True}

    result = await execute_workflow(pool, await claim_one(pool), settings=settings)

    assert result is ExecutionResult.SUCCESS
    before, after = observed
    assert after > before, "ctx.renew_lease() did not move lease_expires_at"


async def test_a_step_that_renews_a_lost_lease_learns_it_was_preempted(
    pool, insert_workflow, settings
):
    """PreemptedError out of ctx.renew_lease() -- so a long step stops working for a workflow
    whose result would be rejected, instead of finding out minutes later."""
    workflow_id = await insert_workflow()
    reached_the_end = []

    @workflow(WORKFLOW_TYPE)
    class Stalled:
        @step(seq=1)
        async def debit_wallet(self, ctx: StepContext) -> dict[str, bool]:
            await steal(pool, ctx.workflow_id)
            await ctx.renew_lease()
            reached_the_end.append(True)
            return {"ok": True}

    result = await execute_workflow(pool, await claim_one(pool), settings=settings)

    assert result is ExecutionResult.PREEMPTED
    assert reached_the_end == []
    assert await checkpoints(pool, workflow_id) == {}


# ---------------------------------------------------------------------------
# 7. What the executor refuses to decide.
# ---------------------------------------------------------------------------


async def test_an_unregistered_workflow_type_leaves_the_row_untouched(
    pool, insert_workflow, settings
):
    """This worker's build does not import the definition. That is a fact about the worker,
    not a failure of the workflow -- compensating it here would unwind real money because a
    process was stale. Let the lease expire and a worker that knows the type take it."""
    workflow_id = await insert_workflow(workflow_type="type_this_build_never_heard_of")
    claimed = await claim_one(pool)

    with pytest.raises(KeyError, match="no workflow registered"):
        await execute_workflow(pool, claimed, settings=settings)

    final = await row(pool, workflow_id)
    assert final["status"] == "RUNNING"
    assert final["owner_id"] == "worker-a"
    assert final["error"] is None
