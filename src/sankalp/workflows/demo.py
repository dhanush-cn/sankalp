"""``demo_crash`` -- the workflow the Phase 1 crash gate kills a worker in the middle of.

This is not an illustration of how to write a workflow. It is instrumentation: three steps
that record what they did in Postgres so a test can count real executions *after* the
process that performed them has been SIGKILLed. A killed process takes its in-memory call
counters with it, so the "your mock's call counter still reads 1" half of the gate
(docs/spec.md, Phase 1 Gate) has to be a committed row instead of a closure.

The shape of step 2 is the whole design, and it is deliberate in two independent ways.

**The side effect is uncommitted while the step is killable.** ``hold`` opens a transaction,
INSERTs into ``side_effects``, does its work, and only then returns -- letting the
transaction commit. A SIGKILL during the work aborts that transaction, so the killed attempt
leaves *zero* committed side effects and the replay can be asserted to produce exactly one.
Insert-and-commit-then-work would instead leave one row behind per kill, and "exactly one"
would be measuring nothing.

**The step announces itself before it becomes killable.** ``step_attempts`` is written on a
*separate* connection and committed immediately, carrying this process's pid. That is what
lets the test kill at a chosen instant rather than racing a sleep, and -- because the row
survives the kill -- what lets it afterwards prove the step was attempted twice while its
side effect happened once. That pair is the guarantee in CLAUDE.md, stated as two integers.

Nothing here is idempotent, on purpose. Making the inserts ``ON CONFLICT DO NOTHING`` would
be Phase 2's idempotency-by-construction, and it would make the gate pass whether or not
crash recovery worked. The counts have to measure what the engine did.

How ``hold`` waits is chosen by the workflow input, because the gate needs both:

``{"mode": "gate"}``
    Block until the test inserts a ``crash_gates`` row. The kill lands the instant the step
    is confirmed running and the *recovering* attempt returns immediately, so a repetition
    costs a few seconds instead of the length of a sleep. This is the default.
``{"mode": "sleep", "sleep_seconds": 1.0}``
    Block on a real ``asyncio.sleep``, so the gate also covers being killed in the middle of
    genuine uninterruptible-looking work rather than only inside a coordination loop.

Both wait with ``await``, never a blocking call. The worker renews this workflow's lease from
a background task (``engine/worker.py``, ``_renew_until_done``) and a step that refused to
yield would starve that renewer and lose the lease it is supposed to be holding -- the
recovering attempt would then be preempted mid-step and the gate would fail for a reason
that has nothing to do with the crash.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

import asyncpg

from sankalp.engine.definition import StepContext, step, workflow
from sankalp.engine.errors import TerminalError
from sankalp.storage.pool import create_pool

__all__ = ["DemoCrash", "WORKFLOW_TYPE", "STEP_NAMES", "GATED_STEP"]

#: The ``workflows.workflow_type`` this definition is registered under. A wire identifier --
#: the crash test writes it into the row it submits.
WORKFLOW_TYPE = "demo_crash"

#: Forward step names in ``seq`` order. Exported so the test asserts against these rather
#: than against string literals that could drift out of step with the definition.
STEP_NAMES = ("reserve_funds", "hold", "settle")

#: The step the gate kills inside of.
GATED_STEP = "hold"

#: Ceiling on how long ``hold`` waits for its gate. Purely a safety net: the test always
#: releases the gate, so hitting this means something is wrong, and failing loudly beats a
#: repetition that hangs until pytest is killed by hand.
_GATE_TIMEOUT_SECONDS = 60.0

#: How often the gate is polled. LISTEN/NOTIFY would avoid the poll, but it would also make
#: the step's wait depend on a connection staying healthy across the very kill this exists to
#: study; a cheap indexed lookup has no such failure mode.
_GATE_POLL_SECONDS = 0.02

# A pool of this module's own, because the engine hands a step a StepContext and not a
# connection -- by design (engine/definition.py: the definition "holds no connection, opens
# no transaction"). It points at settings.active_database_url, so a worker launched with
# SANKALP_ENVIRONMENT=test records into sankalp_test like everything else in the run.
_pool: asyncpg.Pool | None = None
_pool_lock = asyncio.Lock()


async def _get_pool() -> asyncpg.Pool:
    """This module's pool, opened on first use.

    Double-checked under a lock: several steps can start concurrently on one worker, and two
    of them racing here would open two pools and leak one.
    """
    global _pool
    if _pool is not None:
        return _pool
    async with _pool_lock:
        if _pool is None:
            _pool = await create_pool()
    return _pool


async def _record_attempt(conn: asyncpg.Connection, ctx: StepContext) -> None:
    """Commit "this process is starting this step, right now", with the pid to kill.

    Written before the step does anything the test might kill it during, and committed on its
    own rather than joined to the side effect -- an attempt that was interrupted still
    happened, and the gate's ability to distinguish "attempted twice, took effect once" from
    "never got there" depends on this row outliving the process that wrote it.
    """
    await conn.execute(
        """
        INSERT INTO step_attempts (workflow_id, step_name, owner_id, pid)
        VALUES ($1, $2, $3, $4)
        """,
        ctx.workflow_id,
        ctx.step_name,
        ctx.owner_id or "unknown",
        os.getpid(),
    )


async def _record_side_effect(conn: asyncpg.Connection, ctx: StepContext) -> None:
    """Record one side effect. Plain INSERT -- no ON CONFLICT; see the module docstring."""
    await conn.execute(
        "INSERT INTO side_effects (workflow_id, step_name) VALUES ($1, $2)",
        ctx.workflow_id,
        ctx.step_name,
    )


async def _await_gate(conn: asyncpg.Connection, ctx: StepContext) -> None:
    """Block until the test releases this step, or fail loudly at the timeout."""
    deadline = asyncio.get_running_loop().time() + _GATE_TIMEOUT_SECONDS
    while True:
        released = await conn.fetchval(
            "SELECT 1 FROM crash_gates WHERE workflow_id = $1 AND step_name = $2",
            ctx.workflow_id,
            ctx.step_name,
        )
        if released is not None:
            return
        if asyncio.get_running_loop().time() >= deadline:
            raise TerminalError(
                f"step {ctx.step_name!r} of workflow {ctx.workflow_id} waited "
                f"{_GATE_TIMEOUT_SECONDS:.0f}s for its crash_gates row and it never arrived. "
                "The crash gate releases it right after the kill -- a timeout here means the "
                "test never got that far, not that the step is slow."
            )
        await asyncio.sleep(_GATE_POLL_SECONDS)


@workflow(WORKFLOW_TYPE)
class DemoCrash:
    """Three steps that leave a countable trace. Step 2 is the one the gate kills inside."""

    @step(seq=1)
    async def reserve_funds(self, ctx: StepContext) -> dict[str, Any]:
        """Record a side effect and commit. Nothing here is meant to be interrupted.

        Its role in the gate is to be the step that must NOT run again: it is checkpointed
        before step 2 starts, so the resume has to replay it from ``step_outputs`` as a
        lookup rather than re-execute it.
        """
        pool = await _get_pool()
        async with pool.acquire() as conn:
            await _record_attempt(conn, ctx)
            async with conn.transaction():
                await _record_side_effect(conn, ctx)
        return {"reserved": int(ctx.input.get("amount_minor", 0))}

    @step(seq=2)
    async def hold(self, ctx: StepContext) -> dict[str, Any]:
        """Announce, then hold an uncommitted side effect open across a killable wait.

        Two connections, and which one does what is the point. ``marker`` commits the attempt
        immediately so the test can see this step running and learn the pid. ``effect`` holds
        the transaction that a SIGKILL must be able to roll back -- so it must not be the
        same connection, or the attempt row would die with it and the test would be blind.
        """
        pool = await _get_pool()
        async with pool.acquire() as marker, pool.acquire() as effect:
            await _record_attempt(marker, ctx)
            # Everything from here until this block exits is inside one transaction. A
            # SIGKILL anywhere in it leaves no committed side effect -- which is precisely
            # the window the Phase 1 gate kills in.
            async with effect.transaction():
                await _record_side_effect(effect, ctx)
                await self._wait(marker, ctx)
        return {"held": True, "attempt": ctx.attempt}

    async def _wait(self, conn: asyncpg.Connection, ctx: StepContext) -> None:
        """The killable window. Waits on the gate, or on a real sleep -- never blocking.

        Polled on ``conn``, not on the connection holding the open transaction: read
        committed would make either work, but keeping the transaction's connection idle means
        the only statement that can be in flight when the kill lands is the INSERT that must
        roll back.
        """
        mode = ctx.input.get("mode", "gate")
        if mode == "gate":
            await _await_gate(conn, ctx)
        elif mode == "sleep":
            await asyncio.sleep(float(ctx.input.get("sleep_seconds", 1.0)))
        else:
            raise TerminalError(
                f"workflow {ctx.workflow_id} was submitted with mode {mode!r}; "
                "demo_crash understands 'gate' and 'sleep'"
            )

    @step(seq=3)
    async def settle(self, ctx: StepContext) -> dict[str, Any]:
        """The step after the crash. Its side effect proves the workflow ran to the end."""
        pool = await _get_pool()
        async with pool.acquire() as conn:
            await _record_attempt(conn, ctx)
            async with conn.transaction():
                await _record_side_effect(conn, ctx)
        return {"settled": True}
