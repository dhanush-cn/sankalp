"""The transactional outbox's producer half: an event and its checkpoint commit together.

See docs/spec.md, "The Outbox": ``INSERT INTO outbox`` rides in the same transaction as the
step's checkpoint (``storage/workflows.py::commit_step_output`` /
``commit_compensation_output``). These assertions are what that buys:

  * a step that emits, and finishes cleanly, commits its event with its checkpoint;
  * a step whose checkpoint rolls back -- because it was preempted, or because it raised after
    emitting -- leaves no orphaned event. The rollback takes the event with it;
  * a step retried across a fresh claim emits its event exactly once, never once per attempt;
  * a compensation's events follow the identical rule, including across its own in-place
    retries, where a failed attempt's events must not ride along with the one that succeeds.
"""

from __future__ import annotations

import json
import uuid

import pytest

from sankalp.config import Settings
from sankalp.engine.definition import StepContext, clear_registry, step, workflow
from sankalp.engine.errors import RetryableError, TerminalError
from sankalp.engine.executor import ExecutionResult, execute_workflow
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
    return Settings(
        max_attempts=5, backoff_cap_seconds=1, lease_duration_seconds=LEASE,
        compensation_max_attempts=3,
    )


async def claim_one(pool, owner: str = "worker-a", lease: int = LEASE):
    async with pool.acquire() as conn:
        claimed = await claim_workflows(conn, owner, lease, 1)
    assert len(claimed) == 1, f"expected one claimable workflow, got {len(claimed)}"
    return claimed[0]


async def steal(pool, workflow_id: uuid.UUID, owner: str = "worker-b") -> None:
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


async def checkpoints(pool, workflow_id: uuid.UUID) -> dict[str, dict]:
    records = await pool.fetch(
        "SELECT step_name, kind, output FROM step_outputs "
        "WHERE workflow_id = $1 AND kind = 'FORWARD'",
        workflow_id,
    )
    return {r["step_name"]: dict(r) for r in records}


async def compensation_checkpoints(pool, workflow_id: uuid.UUID) -> dict[str, dict]:
    records = await pool.fetch(
        "SELECT step_name, kind, output FROM step_outputs "
        "WHERE workflow_id = $1 AND kind = 'COMPENSATION'",
        workflow_id,
    )
    return {r["step_name"]: dict(r) for r in records}


async def outbox_rows(pool, workflow_id: uuid.UUID) -> list[dict]:
    records = await pool.fetch(
        "SELECT event_type, payload, published_at FROM outbox "
        "WHERE workflow_id = $1 ORDER BY id",
        workflow_id,
    )
    return [dict(r) for r in records]


# ---------------------------------------------------------------------------
# 1-2. Atomicity: the headline property.
# ---------------------------------------------------------------------------


async def test_an_event_and_its_checkpoint_commit_together(pool, insert_workflow, settings):
    workflow_id = await insert_workflow(input={"amount_minor": 25_000})

    @workflow(WORKFLOW_TYPE)
    class Transfer:
        @step(seq=1)
        async def debit_wallet(self, ctx: StepContext) -> dict[str, int]:
            ctx.emit("wallet.debited", {"amount_minor": ctx.input["amount_minor"]})
            return {"debited_minor": ctx.input["amount_minor"]}

    result = await execute_workflow(pool, await claim_one(pool), settings=settings)

    assert result is ExecutionResult.SUCCESS
    saved = await checkpoints(pool, workflow_id)
    assert set(saved) == {"debit_wallet"}

    events = await outbox_rows(pool, workflow_id)
    assert len(events) == 1
    assert events[0]["event_type"] == "wallet.debited"
    assert json.loads(events[0]["payload"]) == {"amount_minor": 25_000}
    assert events[0]["published_at"] is None, "the drain, not the checkpoint, marks published"


async def test_a_rolled_back_checkpoint_leaves_no_event(pool, insert_workflow, settings):
    """The headline case. A step emits, then this worker turns out to be preempted.

    The guarded UPDATE inside commit_step_output takes the row lock and fails to match, so
    the whole transaction -- checkpoint AND event -- rolls back together. No orphan event, and
    (the other half of the same guarantee) no lost checkpoint either: neither exists.
    """
    workflow_id = await insert_workflow()

    @workflow(WORKFLOW_TYPE)
    class Transfer:
        @step(seq=1)
        async def debit_wallet(self, ctx: StepContext) -> dict[str, int]:
            ctx.emit("wallet.debited", {"amount_minor": 1})
            await steal(pool, ctx.workflow_id)  # this worker's fencing token is now stale
            return {"debited_minor": 1}

    claimed = await claim_one(pool)
    result = await execute_workflow(pool, claimed, settings=settings)

    assert result is ExecutionResult.PREEMPTED
    assert await checkpoints(pool, workflow_id) == {}, "a preempted worker wrote a checkpoint"
    assert await outbox_rows(pool, workflow_id) == [], (
        "a preempted worker's event survived the rollback that should have taken it with it"
    )


async def test_a_step_that_raises_after_emitting_writes_no_event(pool, insert_workflow, settings):
    """A step that fails never reaches the checkpoint transaction at all -- so its buffered
    event was never given anywhere to go, and none is written."""
    workflow_id = await insert_workflow()

    @workflow(WORKFLOW_TYPE)
    class Transfer:
        @step(seq=1)
        async def debit_wallet(self, ctx: StepContext) -> dict[str, int]:
            ctx.emit("wallet.debited", {"amount_minor": 1})
            raise TerminalError("the gateway rejected the debit")

    result = await execute_workflow(pool, await claim_one(pool), settings=settings)

    assert result is ExecutionResult.COMPENSATING
    assert await outbox_rows(pool, workflow_id) == []


# ---------------------------------------------------------------------------
# 3. The forward retry: a fresh claim per attempt, so the event ships exactly once.
# ---------------------------------------------------------------------------


async def test_a_retried_step_emits_one_event_not_two(pool, insert_workflow, settings):
    """Emit, fail retryably on attempt 1, succeed on attempt 2 -- driven through the real
    executor twice, exactly as a live retry is. Exactly one outbox row must result: a
    duplicate created here is a duplicate at the source, which consumer dedupe cannot repair.
    """
    workflow_id = await insert_workflow()
    fail_once = {"call_gateway": RetryableError("gateway timed out")}

    @workflow(WORKFLOW_TYPE)
    class Transfer:
        @step(seq=1)
        async def debit_wallet(self, ctx: StepContext) -> dict[str, int]:
            return {"debited_minor": 1}

        @step(seq=2, retry_on=RetryableError)
        async def call_gateway(self, ctx: StepContext) -> dict[str, str]:
            ctx.emit("gateway.called", {"attempt": ctx.attempt})
            exc = fail_once.pop("call_gateway", None)
            if exc is not None:
                raise exc
            return {"reference": "gw-ok"}

    first = await execute_workflow(pool, await claim_one(pool), settings=settings)
    assert first is ExecutionResult.RETRY_SCHEDULED
    assert await outbox_rows(pool, workflow_id) == [], (
        "a step that raised must not have committed the event it emitted before failing"
    )

    await pool.execute("UPDATE workflows SET run_after = now() WHERE id = $1", workflow_id)
    second = await execute_workflow(pool, await claim_one(pool), settings=settings)

    assert second is ExecutionResult.SUCCESS
    events = await outbox_rows(pool, workflow_id)
    assert len(events) == 1, f"expected exactly one event across both attempts, got {events}"
    assert json.loads(events[0]["payload"]) == {"attempt": 2}


# ---------------------------------------------------------------------------
# 4. Compensations: same transaction, plus the in-place-retry wrinkle.
# ---------------------------------------------------------------------------


async def test_a_compensation_emits_in_its_own_transaction(pool, insert_workflow, settings):
    workflow_id = await insert_workflow()

    @workflow(WORKFLOW_TYPE)
    class Transfer:
        @step(seq=1)
        async def debit_wallet(self, ctx: StepContext) -> dict[str, int]:
            return {"debited_minor": 1}

        @debit_wallet.compensate
        async def refund_wallet(self, ctx: StepContext, forward_output: dict) -> None:
            ctx.emit("wallet.refunded", {"debited_minor": forward_output["debited_minor"]})

        @step(seq=2)
        async def write_ledger(self, ctx: StepContext) -> dict[str, str]:
            raise TerminalError("ledger rejected the posting")

    forward = await execute_workflow(pool, await claim_one(pool), settings=settings)
    assert forward is ExecutionResult.COMPENSATING

    unwind = await execute_workflow(pool, await claim_one(pool, owner="worker-b"),
                                     settings=settings)
    assert unwind is ExecutionResult.COMPENSATED

    saved = await compensation_checkpoints(pool, workflow_id)
    assert "debit_wallet" in saved

    events = await outbox_rows(pool, workflow_id)
    assert len(events) == 1
    assert events[0]["event_type"] == "wallet.refunded"
    assert json.loads(events[0]["payload"]) == {"debited_minor": 1}


async def test_a_failed_compensation_attempt_does_not_ship_its_events_twice(
    pool, insert_workflow, settings
):
    """The unwind retries a compensation IN PLACE, against one StepContext -- unlike the
    forward path, which gets a fresh context per attempt via a fresh claim. Without
    ``ctx.discard_pending_events()`` at the top of each attempt (executor._run_compensation),
    the events an attempt buffered before failing would ride along with the retry that
    succeeds, and one undo would ship two events.
    """
    workflow_id = await insert_workflow()
    failures: list[Exception | None] = [RuntimeError("wallet service is down"), None]

    @workflow(WORKFLOW_TYPE)
    class Transfer:
        @step(seq=1)
        async def debit_wallet(self, ctx: StepContext) -> dict[str, int]:
            return {"debited_minor": 1}

        @debit_wallet.compensate
        async def refund_wallet(self, ctx: StepContext, forward_output: dict) -> None:
            ctx.emit("wallet.refund_attempted", {})
            exc = failures.pop(0)
            if exc is not None:
                raise exc

        @step(seq=2)
        async def write_ledger(self, ctx: StepContext) -> dict[str, str]:
            raise TerminalError("ledger rejected the posting")

    forward = await execute_workflow(pool, await claim_one(pool), settings=settings)
    assert forward is ExecutionResult.COMPENSATING

    unwind = await execute_workflow(pool, await claim_one(pool, owner="worker-b"),
                                     settings=settings)
    assert unwind is ExecutionResult.COMPENSATED
    assert failures == [], "the compensation was not retried the expected number of times"

    events = await outbox_rows(pool, workflow_id)
    assert len(events) == 1, (
        f"expected exactly one event from the successful attempt, got {len(events)}: {events}"
    )


# ---------------------------------------------------------------------------
# 5. emit() itself.
# ---------------------------------------------------------------------------


async def test_emit_rejects_an_unserialisable_payload(pool, insert_workflow, settings):
    workflow_id = await insert_workflow()

    @workflow(WORKFLOW_TYPE)
    class Transfer:
        @step(seq=1)
        async def debit_wallet(self, ctx: StepContext) -> dict[str, int]:
            ctx.emit("wallet.debited", {"bad": object()})
            return {"debited_minor": 1}

    result = await execute_workflow(pool, await claim_one(pool), settings=settings)

    # emit() raises TerminalError, uncaught by any retry_on -- so the step fails terminally
    # and the workflow is sent to compensate, exactly as an unclassified failure would.
    assert result is ExecutionResult.COMPENSATING
    final = await pool.fetchrow("SELECT error FROM workflows WHERE id = $1", workflow_id)
    assert "not JSON-serialisable" in final["error"]
    assert await outbox_rows(pool, workflow_id) == []
