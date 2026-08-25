"""``demo_unwind`` -- the saga the Phase 2 crash gate kills a worker in the middle of *undoing*.

``demo.py`` proves the forward guarantee: kill a worker mid-step and the step is attempted
twice but takes effect once. This module proves the same thing about the other direction, which
is the half a saga orchestrator is actually judged on -- an unwind that loses its place
double-refunds, and an unwind that forgets where it was leaves money moved.

The shape is deliberate, and it mirrors ``demo.py`` step for step.

**The forward run always fails, at the last step.** ``settle`` raises ``TerminalError``
unconditionally, so ``reserve`` and ``charge`` are committed to ``step_outputs`` and the
workflow lands in COMPENSATING with exactly two steps to reverse. No flakiness, no timing: the
gate gets the same starting position every repetition.

**The kill lands in the SECOND undo, with the first already checkpointed.** ``refund_charge``
runs, commits, and gets its ``kind = 'COMPENSATION'`` row. Only then does ``release_reserve``
open a transaction, INSERT into ``side_effects``, and wait. A SIGKILL during that wait aborts
it, so the killed attempt leaves *zero* committed releases and the resume produces exactly one
-- and, the assertion that actually matters, the resume must **not** run ``refund_charge``
again. Nothing stops it doing so except that committed COMPENSATION row. Kill inside the first
undo instead and this proves far less: an interrupted undo has no checkpoint to lose, so the
idempotency guard is never consulted.

**The compensation announces itself before it becomes killable**, on a separate connection that
commits immediately, carrying its pid -- so the test kills at a chosen instant rather than
racing a sleep, and afterwards can prove the undo was attempted twice while it took effect once.

**Compensations record under their own name.** ``charge``'s undo writes ``step_attempts`` and
``side_effects`` rows named ``'charge:compensate'`` (:func:`compensation_of`), never
``'charge'``. Sharing the name would make the forward and reverse counts un-separable, and
would make this compensation's ``crash_gates`` row release the *forward* step instead.

Nothing here is idempotent, on purpose -- the same reasoning as ``demo.py``. A compensation
written ``ON CONFLICT DO NOTHING`` would make the gate pass whether or not the engine's
checkpointing worked. Real compensations must be idempotent (docs/spec.md, "Compensation
Model"); these are instruments, and they have to measure what the engine actually did.
"""

from __future__ import annotations

import asyncio
from typing import Any

import asyncpg

from sankalp.engine.definition import StepContext, step, workflow
from sankalp.engine.errors import TerminalError
from sankalp.workflows._instrumentation import (
    await_gate,
    compensation_of,
    get_pool,
    record_attempt,
    record_side_effect,
)

__all__ = [
    "DemoUnwind",
    "WORKFLOW_TYPE",
    "STEP_NAMES",
    "UNWIND_ORDER",
    "FAILING_STEP",
    "GATED_COMPENSATION",
    "COMPLETED_COMPENSATION",
    "compensation_of",
]

#: The ``workflows.workflow_type`` this definition is registered under. A wire identifier.
WORKFLOW_TYPE = "demo_unwind"

#: Forward step names in ``seq`` order. Exported so tests assert against these rather than
#: against string literals that could drift out of step with the definition.
STEP_NAMES = ("reserve", "charge", "settle")

#: The step that always fails, sending the workflow to COMPENSATING with the two before it
#: checkpointed. It has no compensation: it never completed, so there is nothing to undo.
FAILING_STEP = "settle"

#: The order the compensations must run in -- reverse ``seq`` over the *completed* steps.
#: ``settle`` is absent because it never committed a FORWARD row.
UNWIND_ORDER = ("charge", "reserve")

#: The compensation the gate kills inside of -- the *second* one to run, and that choice is
#: the whole point. Killing inside the first undo proves only that an unwind resumes; the undo
#: that was interrupted has no checkpoint to lose, so nothing about the idempotency guard is
#: under test. Killing inside the second one puts a **completed, checkpointed** undo behind the
#: crash, and the resume must skip it. That is the reverse-direction twin of demo.py, where
#: `reserve_funds` is completed and must not re-run while `hold` is the step being killed.
GATED_COMPENSATION = "reserve"

#: The undo that must have finished before the kill lands, and must NOT run again after it.
#: Its ``kind = 'COMPENSATION'`` row is the only thing preventing a second refund.
COMPLETED_COMPENSATION = "charge"


@workflow(WORKFLOW_TYPE)
class DemoUnwind:
    """Two compensable steps and a third that always fails. The gate kills inside an undo."""

    # -- forward ------------------------------------------------------------------------

    @step(seq=1)
    async def reserve(self, ctx: StepContext) -> dict[str, Any]:
        """Reserve funds. Lowest seq, so its undo runs *last* -- and is the one that is killed."""
        pool = await get_pool()
        async with pool.acquire() as conn:
            await record_attempt(conn, ctx.workflow_id, "reserve", ctx.owner_id)
            async with conn.transaction():
                await record_side_effect(conn, ctx.workflow_id, "reserve")
        return {"reserved_minor": int(ctx.input.get("amount_minor", 0))}

    @step(seq=2)
    async def charge(self, ctx: StepContext) -> dict[str, Any]:
        """Charge the gateway. Its undo runs first and must survive the crash as a checkpoint.

        The returned reference is what ``refund_charge`` reads back -- from ``step_outputs``,
        in a different process, after the one that charged is gone. That round trip is why a
        step output has to be plain JSON.
        """
        pool = await get_pool()
        async with pool.acquire() as conn:
            await record_attempt(conn, ctx.workflow_id, "charge", ctx.owner_id)
            async with conn.transaction():
                await record_side_effect(conn, ctx.workflow_id, "charge")
        reserved = ctx.output_of("reserve")["reserved_minor"]
        return {"reference": f"gw-{reserved}", "charged_minor": reserved}

    @step(seq=3)
    async def settle(self, ctx: StepContext) -> dict[str, Any]:
        """Always fails, terminally. This is the trigger, not a step under test.

        Terminal rather than retryable so the workflow goes straight to COMPENSATING instead
        of spending ``max_attempts`` first: the gate is about the unwind, and making it sit
        through a retry ladder every repetition would cost minutes and prove nothing extra.

        It records an attempt but no side effect -- it never completed, which is exactly why
        it has no compensation and why the unwind must skip it.
        """
        pool = await get_pool()
        async with pool.acquire() as conn:
            await record_attempt(conn, ctx.workflow_id, "settle", ctx.owner_id)
        raise TerminalError(
            f"workflow {ctx.workflow_id}: settlement rejected (demo_unwind always fails here, "
            "so that the compensation path is what gets exercised)"
        )

    # -- reverse ------------------------------------------------------------------------

    @charge.compensate
    async def refund_charge(
        self, ctx: StepContext, forward_output: dict[str, Any]
    ) -> None:
        """Undo the charge. Runs first, completes, and is checkpointed before the kill lands.

        This is the undo under test, even though nothing kills it. Its role is to be the one
        that must NOT run again: by the time the gate stops the unwind it has a committed
        ``kind = 'COMPENSATION'`` row, so the resume has to read that row and skip it. Delete
        the checkpoint write and this refund happens twice.

        Nothing here waits, and its side effect commits promptly, because the crash window is
        the next compensation's problem, not this one's.
        """
        pool = await get_pool()
        async with pool.acquire() as conn:
            name = compensation_of("charge")
            await record_attempt(conn, ctx.workflow_id, name, ctx.owner_id)
            async with conn.transaction():
                await record_side_effect(conn, ctx.workflow_id, name)

    @reserve.compensate
    async def release_reserve(
        self, ctx: StepContext, forward_output: dict[str, Any]
    ) -> None:
        """Release the reservation, holding it uncommitted across a killable wait.

        Runs second -- reverse ``seq`` -- so the kill lands with the refund above already
        checkpointed and this undo still outstanding. Two connections, and which does what is
        the point. ``marker`` commits the attempt immediately so the test can see this undo
        running and learn the pid. ``effect`` holds the transaction that a SIGKILL must be able
        to roll back -- so it must not be the same connection, or the attempt row would die
        with it and the test would be blind.
        """
        name = compensation_of("reserve")
        pool = await get_pool()
        async with pool.acquire() as marker, pool.acquire() as effect:
            await record_attempt(marker, ctx.workflow_id, name, ctx.owner_id)
            # Everything from here until this block exits is inside one transaction. A SIGKILL
            # anywhere in it leaves no committed release -- which is precisely the window the
            # Phase 2 gate kills in.
            async with effect.transaction():
                await record_side_effect(effect, ctx.workflow_id, name)
                await self._wait(marker, ctx, name)

    async def _wait(self, conn: asyncpg.Connection, ctx: StepContext, name: str) -> None:
        """The killable window. Waits on the gate, or on a real sleep -- never blocking.

        Polled on ``conn``, not on the connection holding the open transaction: read committed
        would make either work, but keeping the transaction's connection idle means the only
        statement that can be in flight when the kill lands is the INSERT that must roll back.

        Both branches ``await``. The worker renews this workflow's lease from a background task
        (``engine/worker.py``, ``_renew_until_done``) and a compensation that refused to yield
        would starve that renewer and lose the lease it is supposed to be holding -- the
        recovering attempt would then be preempted mid-undo and the gate would fail for a
        reason that has nothing to do with the crash.
        """
        mode = ctx.input.get("mode", "gate")
        if mode == "gate":
            await await_gate(conn, ctx.workflow_id, name)
        elif mode == "sleep":
            await asyncio.sleep(float(ctx.input.get("sleep_seconds", 1.0)))
        else:
            raise TerminalError(
                f"workflow {ctx.workflow_id} was submitted with mode {mode!r}; "
                "demo_unwind understands 'gate' and 'sleep'"
            )
