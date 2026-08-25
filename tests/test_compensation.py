"""The unwind, proved against a real database. See docs/spec.md, "Compensation Model".

These are the assertions the COMPENSATING path reduces to:

  * compensations run in reverse ``seq`` order over the steps that actually completed, and a
    step with no compensation declared -- a read-only one -- is skipped rather than failed;
  * a compensation runs **exactly once**, because a ``step_outputs`` row with
    ``kind = 'COMPENSATION'`` is the checkpoint and the idempotency guard in one, exactly as
    the FORWARD row is on the way in;
  * a compensation that ran but whose checkpoint did not commit runs **again** on the resume.
    That is not a bug being tolerated, it is the contract: the window cannot be closed, which
    is why compensations are required to be idempotent;
  * a worker that has been preempted writes nothing at all, including that checkpoint;
  * a compensation that cannot be made to succeed lands the workflow in ``FAILED_DIRTY`` and
    stops the unwind there, leaving the steps below it un-reversed for a human;
  * none of this touches ``workflows.attempt``. The unwind's retry budget is counted in
    memory, so the forward run's history survives for whoever has to read the row later.
"""

from __future__ import annotations

import logging
import uuid

import pytest

from sankalp.config import Settings
from sankalp.engine.definition import StepContext, clear_registry, step, workflow
from sankalp.engine.errors import RetryableError, TerminalError
from sankalp.engine.executor import ExecutionResult, execute_workflow
from sankalp.storage import workflows as workflow_writes
from sankalp.storage.queue import claim_workflows

LEASE = 30
WORKFLOW_TYPE = "payment_transfer"

#: Three tries per compensation, and a backoff cap of one second so the two sleeps between
#: them cost ~1-3s rather than ~9s. The cap is the only lever: the jitter is not optional
#: (CLAUDE.md) and a test that stubbed it out would stop exercising the real backoff call.
COMPENSATION_ATTEMPTS = 3


@pytest.fixture(autouse=True)
def isolated_registry():
    """Each test registers its own throwaway definition into an empty registry."""
    clear_registry()
    yield
    clear_registry()


@pytest.fixture
def settings() -> Settings:
    return Settings(
        max_attempts=5,
        backoff_cap_seconds=1,
        lease_duration_seconds=LEASE,
        compensation_max_attempts=COMPENSATION_ATTEMPTS,
    )


# ---------------------------------------------------------------------------
# A three-step saga that records every forward step and every undo it runs.
# ---------------------------------------------------------------------------


def define_saga(events: list[str], **compensation_failures: list[Exception | None]) -> type:
    """debit -> gateway -> ledger, where the ledger step always fails terminally.

    So the forward run always ends in COMPENSATING with steps 1 and 2 checkpointed, which is
    the position every test here starts the unwind from -- and is exactly the case in the
    brief: a workflow failing at step 3 must unwind 2 and then 1.

    ``events`` is a closure rather than instance state on purpose: the engine instantiates a
    fresh workflow object per execution, so anything recorded on ``self`` would vanish exactly
    where these tests need to look -- across a re-claim.

    ``compensation_failures`` maps a compensation's name to a queue of outcomes, one per
    invocation: an exception to raise, or ``None`` to succeed. A shorter queue than the number
    of invocations means every later call succeeds, which is how "fails twice, then works" is
    written without a counter in the test body.
    """

    def outcome(name: str) -> None:
        queue = compensation_failures.get(name)
        if queue:
            exc = queue.pop(0)
            if exc is not None:
                raise exc

    @workflow(WORKFLOW_TYPE)
    class PaymentTransfer:
        @step(seq=1)
        async def debit_wallet(self, ctx: StepContext) -> dict[str, int]:
            events.append("debit_wallet")
            return {"debited_minor": ctx.input["amount_minor"]}

        @debit_wallet.compensate
        async def refund_wallet(self, ctx: StepContext, forward_output: dict) -> None:
            events.append("refund_wallet")
            outcome("refund_wallet")

        @step(seq=2)
        async def call_gateway(self, ctx: StepContext) -> dict[str, str]:
            events.append("call_gateway")
            return {"reference": f"gw-{ctx.output_of('debit_wallet')['debited_minor']}"}

        @call_gateway.compensate
        async def void_gateway(self, ctx: StepContext, forward_output: dict) -> None:
            events.append("void_gateway")
            outcome("void_gateway")

        @step(seq=3)
        async def write_ledger(self, ctx: StepContext) -> dict[str, str]:
            events.append("write_ledger")
            raise TerminalError("ledger rejected the posting")

    return PaymentTransfer


async def claim_one(pool, owner: str = "worker-a", lease: int = LEASE):
    """Claim exactly one workflow, the way the worker does."""
    async with pool.acquire() as conn:
        claimed = await claim_workflows(conn, owner, lease, 1)
    assert len(claimed) == 1, f"expected one claimable workflow, got {len(claimed)}"
    return claimed[0]


async def claim_one_or_none(pool, owner: str, lease: int = LEASE):
    """Try to claim; None when nothing is claimable. For asserting a live lease excludes."""
    async with pool.acquire() as conn:
        claimed = await claim_workflows(conn, owner, lease, 1)
    return claimed[0] if claimed else None


async def steal(pool, workflow_id: uuid.UUID, owner: str = "worker-z") -> None:
    """Another worker claims the row: new owner, higher fencing token."""
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


async def compensations(pool, workflow_id: uuid.UUID) -> list[str]:
    """Compensated step names, in the order their checkpoints committed."""
    records = await pool.fetch(
        """
        SELECT step_name FROM step_outputs
        WHERE workflow_id = $1 AND kind = 'COMPENSATION'
        ORDER BY completed_at, step_name
        """,
        workflow_id,
    )
    return [r["step_name"] for r in records]


async def forward_steps(pool, workflow_id: uuid.UUID) -> set[str]:
    records = await pool.fetch(
        "SELECT step_name FROM step_outputs WHERE workflow_id = $1 AND kind = 'FORWARD'",
        workflow_id,
    )
    return {r["step_name"] for r in records}


async def run_forward_to_compensating(pool, settings, events: list[str]) -> None:
    """Drive the forward run that fails at step 3, leaving the row COMPENSATING."""
    result = await execute_workflow(pool, await claim_one(pool), settings=settings)
    assert result is ExecutionResult.COMPENSATING
    assert events == ["debit_wallet", "call_gateway", "write_ledger"]
    events.clear()


# ---------------------------------------------------------------------------
# 1. The order, and the shape of a completed unwind.
# ---------------------------------------------------------------------------


async def test_a_workflow_failing_at_step_3_unwinds_steps_2_and_1_in_that_order(
    pool, insert_workflow, settings
):
    """The headline case. Reverse seq is the unwind order, and it is not negotiable.

    Step 2's undo releases what step 2 took; step 1's undo assumes step 2 has already been
    released. Run them the other way round and each undo is operating on a state its author
    never anticipated.
    """
    workflow_id = await insert_workflow(input={"amount_minor": 25_000})
    events: list[str] = []
    define_saga(events)

    await run_forward_to_compensating(pool, settings, events)

    result = await execute_workflow(pool, await claim_one(pool, owner="worker-b"),
                                    settings=settings)

    assert result is ExecutionResult.COMPENSATED
    assert events == ["void_gateway", "refund_wallet"], (
        f"compensations ran as {events}; step 2's undo must run before step 1's"
    )
    assert await compensations(pool, workflow_id) == ["call_gateway", "debit_wallet"], (
        "the COMPENSATION checkpoints must have committed in reverse seq order too -- the "
        "durable record is what a resume after a crash reads, not the in-memory list"
    )
    final = await row(pool, workflow_id)
    assert final["status"] == "COMPENSATED"
    assert final["owner_id"] is None
    assert final["lease_expires_at"] is None
    assert final["error"] is not None, (
        "the failure that sent the workflow here is the most useful thing to read off a "
        "COMPENSATED row afterwards; the unwind must not clear it"
    )


async def test_a_step_that_never_completed_is_not_compensated(
    pool, insert_workflow, settings
):
    """``write_ledger`` failed, so it has no FORWARD row and nothing to undo."""
    workflow_id = await insert_workflow()
    events: list[str] = []
    define_saga(events)

    await run_forward_to_compensating(pool, settings, events)
    await execute_workflow(pool, await claim_one(pool, owner="worker-b"), settings=settings)

    assert await forward_steps(pool, workflow_id) == {"debit_wallet", "call_gateway"}
    assert "write_ledger" not in await compensations(pool, workflow_id)


async def test_a_read_only_step_with_no_compensation_is_skipped(
    pool, insert_workflow, settings
):
    """A balance check has nothing to undo. Skipping it must not disturb the order."""
    workflow_id = await insert_workflow()
    events: list[str] = []

    @workflow(WORKFLOW_TYPE)
    class WithReadOnlyStep:
        @step(seq=1)
        async def debit_wallet(self, ctx: StepContext) -> dict[str, bool]:
            return {"debited": True}

        @debit_wallet.compensate
        async def refund_wallet(self, ctx: StepContext, forward_output: dict) -> None:
            events.append("refund_wallet")

        @step(seq=2)
        async def check_fraud(self, ctx: StepContext) -> dict[str, bool]:
            # No @check_fraud.compensate: it read something, it changed nothing.
            return {"clean": True}

        @step(seq=3)
        async def call_gateway(self, ctx: StepContext) -> dict[str, bool]:
            return {"charged": True}

        @call_gateway.compensate
        async def void_gateway(self, ctx: StepContext, forward_output: dict) -> None:
            events.append("void_gateway")

        @step(seq=4)
        async def write_ledger(self, ctx: StepContext) -> dict[str, bool]:
            raise TerminalError("ledger rejected the posting")

    assert await execute_workflow(pool, await claim_one(pool), settings=settings) is (
        ExecutionResult.COMPENSATING
    )
    result = await execute_workflow(pool, await claim_one(pool, owner="worker-b"),
                                    settings=settings)

    assert result is ExecutionResult.COMPENSATED
    assert events == ["void_gateway", "refund_wallet"]
    assert await compensations(pool, workflow_id) == ["call_gateway", "debit_wallet"], (
        "check_fraud must not get a COMPENSATION row: it declares no compensation, and "
        "writing one would claim an undo happened that never did"
    )


async def test_a_compensation_receives_the_output_of_the_step_it_undoes(
    pool, insert_workflow, settings
):
    """Read back from step_outputs, not carried in memory -- that is what makes an unwind
    possible in a process that never ran the forward step."""
    await insert_workflow(input={"amount_minor": 7_500})
    seen: dict[str, object] = {}

    @workflow(WORKFLOW_TYPE)
    class CarriesOutput:
        @step(seq=1)
        async def debit_wallet(self, ctx: StepContext) -> dict[str, int]:
            return {"debited_minor": ctx.input["amount_minor"]}

        @debit_wallet.compensate
        async def refund_wallet(self, ctx: StepContext, forward_output: dict) -> None:
            seen["forward_output"] = forward_output
            seen["sibling"] = dict(ctx.outputs)
            seen["step_name"] = ctx.step_name

        @step(seq=2)
        async def write_ledger(self, ctx: StepContext) -> dict[str, bool]:
            raise TerminalError("ledger rejected the posting")

    await execute_workflow(pool, await claim_one(pool), settings=settings)
    await execute_workflow(pool, await claim_one(pool, owner="worker-b"), settings=settings)

    assert seen["forward_output"] == {"debited_minor": 7_500}
    assert seen["sibling"] == {"debit_wallet": {"debited_minor": 7_500}}
    assert seen["step_name"] == "debit_wallet", (
        "the context must name the forward step being undone, so a compensation that keys "
        "anything on it agrees with the checkpoint the engine is about to write"
    )


# ---------------------------------------------------------------------------
# 2. Exactly once -- the COMPENSATION row is the guard.
# ---------------------------------------------------------------------------


async def test_an_already_compensated_step_is_not_compensated_again(
    pool, insert_workflow, settings
):
    """A resume after a crash mid-unwind. The row that exists is the reason it is skipped."""
    workflow_id = await insert_workflow()
    events: list[str] = []
    define_saga(events)

    await run_forward_to_compensating(pool, settings, events)
    # Stand in for "a previous worker got this far before it died".
    await pool.execute(
        """
        INSERT INTO step_outputs (workflow_id, step_name, seq, kind)
        VALUES ($1, 'call_gateway', 2, 'COMPENSATION')
        """,
        workflow_id,
    )

    result = await execute_workflow(pool, await claim_one(pool, owner="worker-b"),
                                    settings=settings)

    assert result is ExecutionResult.COMPENSATED
    assert events == ["refund_wallet"], (
        f"the unwind ran {events}; call_gateway already had a COMPENSATION row and re-running "
        "its undo would be a second refund"
    )
    assert await compensations(pool, workflow_id) == ["call_gateway", "debit_wallet"]


async def test_a_compensation_whose_checkpoint_did_not_commit_runs_again(
    pool, insert_workflow, settings
):
    """The contract, stated as a test: crash after the undo, before its checkpoint -> it repeats.

    This is the in-process twin of the SIGKILL gate. A steal makes the ownership-guarded
    checkpoint write match zero rows, so the undo has genuinely happened and left no record of
    itself -- which is precisely why docs/spec.md requires compensations to be idempotent
    (``refund_if_not_already_refunded``, not ``refund``) rather than pretending the window
    can be closed.
    """
    workflow_id = await insert_workflow()
    events: list[str] = []
    define_saga(events)

    await run_forward_to_compensating(pool, settings, events)

    claimed = await claim_one(pool, owner="worker-b")
    await steal(pool, workflow_id, owner="worker-c")
    result = await execute_workflow(pool, claimed, settings=settings)

    assert result is ExecutionResult.PREEMPTED
    assert events == ["void_gateway"], "the undo ran before the guard rejected its checkpoint"
    assert await compensations(pool, workflow_id) == [], (
        "a preempted worker must write nothing at all -- a COMPENSATION row here would tell "
        "the real owner that an undo it never saw had already happened"
    )
    assert (await row(pool, workflow_id))["status"] == "COMPENSATING"

    # worker-c holds a live lease, so nobody may take the unwind off it -- a COMPENSATING row
    # under a live lease is an owned row being actively unwound (tests/test_claim.py). Model
    # worker-c dying, which is the only thing that hands the row on.
    assert await claim_one_or_none(pool, owner="worker-d") is None, (
        "worker-c's live lease must exclude worker-d, or two workers unwind the same saga"
    )
    await pool.execute(
        "UPDATE workflows SET lease_expires_at = now() - interval '1 second' WHERE id = $1",
        workflow_id,
    )

    # The successor resumes: void_gateway runs a second time, which the idempotency contract
    # permits, and this time its checkpoint commits.
    events.clear()
    resumed = await execute_workflow(
        pool, await claim_one(pool, owner="worker-d"), settings=settings
    )

    assert resumed is ExecutionResult.COMPENSATED
    assert events == ["void_gateway", "refund_wallet"]
    assert await compensations(pool, workflow_id) == ["call_gateway", "debit_wallet"], (
        "exactly one COMPENSATION row per step, even though void_gateway ran twice"
    )


async def test_a_completed_unwind_is_not_claimable(pool, insert_workflow, settings):
    """COMPENSATED is terminal and falls out of idx_workflows_claimable."""
    await insert_workflow()
    define_saga([])

    await execute_workflow(pool, await claim_one(pool), settings=settings)
    await execute_workflow(pool, await claim_one(pool, owner="worker-b"), settings=settings)

    async with pool.acquire() as conn:
        assert await claim_workflows(conn, "worker-c", LEASE, 10) == []


# ---------------------------------------------------------------------------
# 3. When a compensation will not succeed.
# ---------------------------------------------------------------------------


async def test_a_compensation_that_recovers_on_a_retry_completes_the_unwind(
    pool, insert_workflow, settings
):
    """Two transient failures then success. Giving up on these would page someone for a blip."""
    workflow_id = await insert_workflow()
    events: list[str] = []
    define_saga(events, void_gateway=[RetryableError("gateway 503"), RuntimeError("reset")])

    await run_forward_to_compensating(pool, settings, events)
    result = await execute_workflow(pool, await claim_one(pool, owner="worker-b"),
                                    settings=settings)

    assert result is ExecutionResult.COMPENSATED
    assert events == ["void_gateway", "void_gateway", "void_gateway", "refund_wallet"], (
        f"ran {events}; the retry budget is per compensation, and an unclassified exception "
        "must be retried -- compensations are idempotent by contract, so re-running is safe"
    )
    assert await compensations(pool, workflow_id) == ["call_gateway", "debit_wallet"]


async def test_an_exhausted_compensation_lands_the_workflow_in_failed_dirty(
    pool, insert_workflow, settings, caplog
):
    """The state that pages a human, and the log line that tells them."""
    workflow_id = await insert_workflow()
    events: list[str] = []
    define_saga(events, void_gateway=[RuntimeError("gateway is gone")] * 10)

    await run_forward_to_compensating(pool, settings, events)
    with caplog.at_level(logging.ERROR, logger="sankalp.executor"):
        result = await execute_workflow(pool, await claim_one(pool, owner="worker-b"),
                                        settings=settings)

    assert result is ExecutionResult.FAILED_DIRTY
    assert events == ["void_gateway"] * COMPENSATION_ATTEMPTS, (
        f"ran {events}; the compensation gets exactly compensation_max_attempts tries"
    )
    final = await row(pool, workflow_id)
    assert final["status"] == "FAILED_DIRTY"
    assert "gateway is gone" in final["error"]
    assert final["owner_id"] is None
    assert any(record.levelno >= logging.ERROR for record in caplog.records), (
        "FAILED_DIRTY that nobody was told about is indistinguishable from money quietly "
        "going missing -- it must log at ERROR"
    )


async def test_failed_dirty_stops_the_unwind_and_keeps_what_already_succeeded(
    pool, insert_workflow, settings
):
    """Reverse seq is a dependency order, so the unwind stops where it broke.

    ``call_gateway`` was reversed and stays reversed; ``debit_wallet``'s undo is never
    attempted, because it would be running against a state its precondition -- the gateway
    call being voided -- no longer describes. The COMPENSATION rows are the record of exactly
    how far it got, which is what the human resolving this needs.
    """
    workflow_id = await insert_workflow()
    events: list[str] = []
    define_saga(events, refund_wallet=[RuntimeError("wallet service is down")] * 10)

    await run_forward_to_compensating(pool, settings, events)
    result = await execute_workflow(pool, await claim_one(pool, owner="worker-b"),
                                    settings=settings)

    assert result is ExecutionResult.FAILED_DIRTY
    assert events == ["void_gateway"] + ["refund_wallet"] * COMPENSATION_ATTEMPTS
    assert await compensations(pool, workflow_id) == ["call_gateway"], (
        "the undo that did succeed must keep its checkpoint: re-running it later would be a "
        "second void, and losing the record is how that happens"
    )
    assert (await row(pool, workflow_id))["status"] == "FAILED_DIRTY"


async def test_a_terminal_compensation_failure_does_not_spend_the_retry_budget(
    pool, insert_workflow, settings
):
    """A compensation raising TerminalError is saying waiting will not help. Believe it."""
    await insert_workflow()
    events: list[str] = []
    define_saga(events, void_gateway=[TerminalError("account closed permanently")] * 10)

    await run_forward_to_compensating(pool, settings, events)
    result = await execute_workflow(pool, await claim_one(pool, owner="worker-b"),
                                    settings=settings)

    assert result is ExecutionResult.FAILED_DIRTY
    assert events == ["void_gateway"], (
        f"ran {events}; an explicit TerminalError must not be retried {COMPENSATION_ATTEMPTS}x"
    )


async def test_a_failed_dirty_workflow_is_not_claimable(pool, insert_workflow, settings):
    """Nothing retries a dirty saga automatically. Cycling the row would hide it."""
    await insert_workflow()
    events: list[str] = []
    define_saga(events, void_gateway=[RuntimeError("gone")] * 10)

    await run_forward_to_compensating(pool, settings, events)
    await execute_workflow(pool, await claim_one(pool, owner="worker-b"), settings=settings)

    async with pool.acquire() as conn:
        assert await claim_workflows(conn, "worker-c", LEASE, 10) == []


# ---------------------------------------------------------------------------
# 4. The unwind's budget is its own, and the forward history survives.
# ---------------------------------------------------------------------------


async def test_the_unwind_leaves_workflows_attempt_alone(pool, insert_workflow, settings):
    """The compensation retry budget is counted in memory, never on the row.

    ``workflows.attempt`` is the forward run's history -- how many times this workflow was
    claimed and executed -- and it is what someone reads to understand how a saga got here.
    Spending it on compensation retries would overwrite that, and resetting it would erase it.
    """
    workflow_id = await insert_workflow()
    events: list[str] = []
    define_saga(events, void_gateway=[RuntimeError("transient")])

    await run_forward_to_compensating(pool, settings, events)
    attempt_after_forward = (await row(pool, workflow_id))["attempt"]
    assert attempt_after_forward == 1

    await execute_workflow(pool, await claim_one(pool, owner="worker-b"), settings=settings)

    final = await row(pool, workflow_id)
    assert final["status"] == "COMPENSATED"
    assert final["attempt"] == attempt_after_forward + 1, (
        f"attempt is {final['attempt']}: it must move by exactly one -- the dequeue query's "
        "increment for the claim that ran the unwind -- and not by the two extra tries the "
        "failing compensation took"
    )


# ---------------------------------------------------------------------------
# 5. The SQL guards on their own, independent of the executor.
# ---------------------------------------------------------------------------


async def test_finish_compensated_is_ownership_guarded(pool, insert_workflow):
    """False means preempted, and nothing is written -- the rule every write there follows."""
    workflow_id = await insert_workflow(status="COMPENSATING")
    own = workflow_writes.Ownership.of(await claim_one(pool))
    await steal(pool, workflow_id)

    assert await workflow_writes.finish_compensated(pool, own) is False
    assert (await row(pool, workflow_id))["status"] == "COMPENSATING"


async def test_finish_compensated_refuses_a_workflow_that_is_not_compensating(
    pool, insert_workflow
):
    """The status predicate is part of the guard: only an unwind can become COMPENSATED."""
    workflow_id = await insert_workflow()
    claimed = await claim_one(pool)
    assert claimed.status == "RUNNING"

    own = workflow_writes.Ownership.of(claimed)
    assert await workflow_writes.finish_compensated(pool, own) is False
    assert (await row(pool, workflow_id))["status"] == "RUNNING"


async def test_fail_dirty_is_ownership_guarded(pool, insert_workflow):
    workflow_id = await insert_workflow(status="COMPENSATING")
    own = workflow_writes.Ownership.of(await claim_one(pool))
    await steal(pool, workflow_id)

    assert await workflow_writes.fail_dirty(pool, own, error="nope") is False
    stolen = await row(pool, workflow_id)
    assert stolen["status"] == "COMPENSATING"
    assert stolen["owner_id"] == "worker-z", "the stale worker released someone else's row"


async def test_fail_dirty_refuses_a_workflow_that_is_not_compensating(pool, insert_workflow):
    workflow_id = await insert_workflow()
    own = workflow_writes.Ownership.of(await claim_one(pool))

    assert await workflow_writes.fail_dirty(pool, own, error="nope") is False
    assert (await row(pool, workflow_id))["status"] == "RUNNING"


async def test_commit_compensation_output_is_ownership_guarded(pool, insert_workflow):
    """Zero rows on the guard means the INSERT is never reached and nothing is written."""
    workflow_id = await insert_workflow(status="COMPENSATING")
    own = workflow_writes.Ownership.of(await claim_one(pool))
    await steal(pool, workflow_id)

    committed = await workflow_writes.commit_compensation_output(
        pool, own, step_name="debit_wallet", seq=1, duration_seconds=0.1
    )
    assert committed is False
    assert await compensations(pool, workflow_id) == []


async def test_commit_compensation_output_refuses_a_workflow_that_is_not_compensating(
    pool, insert_workflow
):
    """A RUNNING workflow has no unwind in progress, so it cannot record an undo."""
    workflow_id = await insert_workflow()
    own = workflow_writes.Ownership.of(await claim_one(pool))

    committed = await workflow_writes.commit_compensation_output(
        pool, own, step_name="debit_wallet", seq=1, duration_seconds=0.1
    )
    assert committed is False
    assert await compensations(pool, workflow_id) == []


async def test_load_unwind_state_separates_the_two_kinds(pool, insert_workflow):
    """The read the unwind is built on: forward rows in reverse seq, compensations as a set."""
    workflow_id = await insert_workflow(status="COMPENSATING")
    await pool.execute(
        """
        INSERT INTO step_outputs (workflow_id, step_name, seq, kind, output) VALUES
            ($1, 'debit_wallet', 1, 'FORWARD', '{"debited_minor": 100}'::jsonb),
            ($1, 'call_gateway', 2, 'FORWARD', 'null'::jsonb),
            ($1, 'call_gateway', 2, 'COMPENSATION', NULL)
        """,
        workflow_id,
    )

    forward, compensated = await workflow_writes.load_unwind_state(pool, workflow_id)

    assert [record.step_name for record in forward] == ["call_gateway", "debit_wallet"]
    assert forward[1].output == {"debited_minor": 100}
    assert compensated == {"call_gateway"}
    assert "call_gateway" in compensated, (
        "membership, never truthiness: a COMPENSATION row carries a NULL output, so a "
        "truthiness test here would re-run every undo in the workflow"
    )
    # A step whose forward output is a JSON null is still completed and still compensable.
    assert forward[0].output is None
