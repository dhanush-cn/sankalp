"""Committed traces that outlive the process being killed. Shared by the crash gates.

A SIGKILLed worker takes its in-memory call counters with it, so a test that asks "did this
side effect run twice?" cannot count in a closure -- the counting has to be rows, written by
the process doing the work and read back afterwards by a test that outlived it. These four
helpers are that instrumentation, against the three tables in migrations/002_crash_gate.sql.

They live in their own module because both gates need them: ``demo.py`` kills a worker inside a
forward step, ``unwind.py`` kills one inside a *compensation*, and the second must be able to
record under a different ``step_name`` than the step it is undoing. So every helper takes the
name explicitly rather than reading ``ctx.step_name`` -- a compensation's context still names
the forward step, and letting the two share a name would make the gate's counts un-separable
and, worse, make a compensation's ``crash_gates`` row collide with its forward step's.

Nothing here is idempotent, on purpose. Making the inserts ``ON CONFLICT DO NOTHING`` would
make a gate pass whether or not crash recovery worked; the counts have to measure what the
engine actually did.
"""

from __future__ import annotations

import asyncio
import logging
import os
from uuid import UUID

import asyncpg

from sankalp.config import get_settings
from sankalp.engine.errors import TerminalError
from sankalp.storage.pool import create_pool

__all__ = [
    "compensation_of",
    "get_pool",
    "record_attempt",
    "record_side_effect",
    "await_gate",
]

log = logging.getLogger("sankalp.crash_gate")

#: Ceiling on how long a gated step or compensation waits. Purely a safety net: the test always
#: releases the gate, so hitting this means something is wrong, and failing loudly beats a
#: repetition that hangs until pytest is killed by hand.
_GATE_TIMEOUT_SECONDS = 60.0

#: How often the gate is polled. LISTEN/NOTIFY would avoid the poll, but it would also make the
#: wait depend on a connection staying healthy across the very kill this exists to study; a
#: cheap indexed lookup has no such failure mode.
_GATE_POLL_SECONDS = 0.02

# A pool of these modules' own, because the engine hands a step a StepContext and not a
# connection -- by design (engine/definition.py: the definition "holds no connection, opens no
# transaction"). It points at settings.active_database_url, so a worker launched with
# SANKALP_ENVIRONMENT=test records into sankalp_test like everything else in the run.
_pool: asyncpg.Pool | None = None
_pool_lock = asyncio.Lock()


def compensation_of(step_name: str) -> str:
    """The name a step's *compensation* records under: ``'charge'`` -> ``'charge:compensate'``.

    One function rather than an f-string at each site so the forward name and the undo name
    cannot drift apart between the workflow that writes the rows and the test that counts them.
    """
    return f"{step_name}:compensate"


async def get_pool() -> asyncpg.Pool:
    """This package's instrumentation pool, opened on first use.

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


async def record_attempt(
    conn: asyncpg.Connection, workflow_id: UUID, step_name: str, owner_id: str | None
) -> None:
    """Commit "this process is starting this work, right now", with the pid to kill.

    Written before anything the test might kill it during, and committed on its own rather
    than joined to the side effect -- an attempt that was interrupted still happened, and a
    gate's ability to distinguish "attempted twice, took effect once" from "never got there"
    depends on this row outliving the process that wrote it.
    """
    await conn.execute(
        """
        INSERT INTO step_attempts (workflow_id, step_name, owner_id, pid)
        VALUES ($1, $2, $3, $4)
        """,
        workflow_id,
        step_name,
        owner_id or "unknown",
        os.getpid(),
    )


async def record_side_effect(
    conn: asyncpg.Connection, workflow_id: UUID, step_name: str
) -> None:
    """Record one side effect. Plain INSERT -- no ON CONFLICT; see the module docstring."""
    await conn.execute(
        "INSERT INTO side_effects (workflow_id, step_name) VALUES ($1, $2)",
        workflow_id,
        step_name,
    )


async def await_gate(conn: asyncpg.Connection, workflow_id: UUID, step_name: str) -> None:
    """Block until the test releases this work, or fail loudly at the timeout.

    **Fails safe.** This is the one function in the package that can make a step or a
    compensation stop and wait indefinitely, and the modules that call it are imported by every
    worker process -- ``workflows/__init__.py`` is what registers definitions, so a production
    worker has this code loaded. If the gate is not *demonstrably* armed
    (:attr:`Settings.crash_gate_armed`: ``environment == "test"`` **and**
    ``crash_gate_enabled``, never either on its own) it returns immediately and the caller
    proceeds as if there were no gate at all.

    That default matters more than the convenience of arming it. A gate that blocked on a
    single leaked env var would park a compensation on a ``crash_gates`` row no test is going
    to insert, and it would present as a hung worker rather than as a misconfiguration: an
    unwind stopped mid-flight with money committed on one side of it.

    Not being armed is logged at WARNING rather than passed over in silence, because the other
    way for this to be wrong is a *test* that forgot to arm it -- and a gate that quietly did
    not gate turns the crash tests into tests of nothing, which is the failure mode that does
    not announce itself.
    """
    if not get_settings().crash_gate_armed:
        log.warning(
            "crash gate for %r on workflow %s is NOT armed (environment=%r, "
            "crash_gate_enabled=%s); continuing without waiting. Arming needs both "
            "SANKALP_ENVIRONMENT=test and SANKALP_CRASH_GATE_ENABLED=1.",
            step_name,
            workflow_id,
            get_settings().environment,
            get_settings().crash_gate_enabled,
        )
        return

    deadline = asyncio.get_running_loop().time() + _GATE_TIMEOUT_SECONDS
    while True:
        released = await conn.fetchval(
            "SELECT 1 FROM crash_gates WHERE workflow_id = $1 AND step_name = $2",
            workflow_id,
            step_name,
        )
        if released is not None:
            return
        if asyncio.get_running_loop().time() >= deadline:
            raise TerminalError(
                f"{step_name!r} of workflow {workflow_id} waited {_GATE_TIMEOUT_SECONDS:.0f}s "
                "for its crash_gates row and it never arrived. The crash gate releases it "
                "right after the kill -- a timeout here means the test never got that far, "
                "not that the work is slow."
            )
        await asyncio.sleep(_GATE_POLL_SECONDS)
