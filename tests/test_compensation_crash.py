"""The Phase 2 gate: SIGKILL a real worker while it is *undoing* a saga.

``test_crash.py`` proves the forward half of the guarantee in CLAUDE.md. This proves the half
that a saga orchestrator actually lives or dies on, because the forward half has a safety net
the reverse half does not: a forward step that runs twice can often be absorbed downstream,
while an undo that runs twice is a second refund, and an undo that is skipped is money that
stayed moved. The unwind has no third option to fall back to.

Same instrument, same discipline as the forward gate: real OS processes, ``SIGKILL``, no
``finally``, no drain, no shielded checkpoint. The process stops existing between two
instructions.

**How the kill is aimed.** ``demo_unwind``'s forward run always fails at ``settle``, so the
workflow reaches COMPENSATING with ``reserve`` and ``charge`` checkpointed and two undos owed.
The first undo to run is ``charge``'s -- reverse ``seq`` -- and it commits a
``step_attempts`` row carrying its own pid, then blocks holding an *uncommitted*
``side_effects`` INSERT open. The test waits for that row and kills that pid, so the crash
lands inside a compensation by construction rather than by timing a sleep. The killed
transaction rolls back, which is what makes "exactly one refund" an exact assertion rather
than a likely one.

**Why the counts cannot be faked.** ``step_attempts`` for ``'charge:compensate'`` must read
exactly **2** and ``side_effects`` for it exactly **1**: attempted twice, took effect once.
Without the attempt count the test would pass by killing a worker that had not yet started the
undo -- the saga would still unwind and every side-effect count would still read 1, having
proven nothing about resuming mid-compensation.

**And the order must survive the crash.** The two ``kind = 'COMPENSATION'`` rows are asserted
in ``completed_at`` order, so a resume that restarted the unwind from the wrong end, or walked
it forwards, fails here even though every count would still be 1.

Run it the way it is meant to be run::

    pytest tests/test_compensation_crash.py --count=20    # or: make test-unwind-crash
"""

from __future__ import annotations

import time
import uuid

import asyncpg
from fleet import (
    COMPLETION_TIMEOUT_SECONDS,
    LEASE_SECONDS,
    RECOVERY_MARGIN_SECONDS,
    STARTUP_TIMEOUT_SECONDS,
    WorkerFleet,
    wait_for,
)

from sankalp.workflows.unwind import (
    COMPLETED_COMPENSATION,
    GATED_COMPENSATION,
    STEP_NAMES,
    UNWIND_ORDER,
    WORKFLOW_TYPE,
    compensation_of,
)

#: The step_attempts / side_effects names the compensations record under. Not the same as the
#: steps they undo -- see workflows/_instrumentation.py.
GATED_NAME = compensation_of(GATED_COMPENSATION)

#: The undo that finished and was checkpointed *before* the kill. Its count is the assertion
#: this whole file exists for: it must read 1, and only its COMPENSATION row makes that true.
COMPLETED_NAME = compensation_of(COMPLETED_COMPENSATION)


# ---------------------------------------------------------------------------
# Observing the unwind from outside the workers
# ---------------------------------------------------------------------------


async def _attempts(conn: asyncpg.Connection, workflow_id: uuid.UUID, name: str):
    """Every recorded attempt at one step or compensation, oldest first."""
    return await conn.fetch(
        """
        SELECT owner_id, pid, attempted_at
        FROM step_attempts
        WHERE workflow_id = $1 AND step_name = $2
        ORDER BY id
        """,
        workflow_id,
        name,
    )


async def _tally(conn: asyncpg.Connection, workflow_id: uuid.UUID, table: str) -> dict[str, int]:
    records = await conn.fetch(
        f"SELECT step_name, count(*) AS n FROM {table} "  # noqa: S608 - fixed literals below
        "WHERE workflow_id = $1 GROUP BY step_name",
        workflow_id,
    )
    return {r["step_name"]: r["n"] for r in records}


async def _checkpoints(
    conn: asyncpg.Connection, workflow_id: uuid.UUID, kind: str
) -> list[str]:
    """Checkpointed step names of one kind, in the order they committed."""
    records = await conn.fetch(
        """
        SELECT step_name FROM step_outputs
        WHERE workflow_id = $1 AND kind = $2
        ORDER BY completed_at, step_name
        """,
        workflow_id,
        kind,
    )
    return [r["step_name"] for r in records]


async def _release_gate(conn: asyncpg.Connection, workflow_id: uuid.UUID) -> None:
    """Let the *recovering* attempt of the gated compensation finish immediately."""
    await conn.execute(
        "INSERT INTO crash_gates (workflow_id, step_name) VALUES ($1, $2)",
        workflow_id,
        GATED_NAME,
    )


# ---------------------------------------------------------------------------
# The gate
# ---------------------------------------------------------------------------


async def test_sigkill_mid_compensation_resumes_without_repeating_the_undo(
    conn: asyncpg.Connection, insert_workflow, workers: WorkerFleet
) -> None:
    """Kill a worker inside a compensation; the unwind resumes and no undo runs twice."""
    workers.launch()
    workflow_id = await insert_workflow(
        workflow_type=WORKFLOW_TYPE, input={"amount_minor": 250_000, "mode": "gate"}
    )

    # 1. Wait until the first compensation is genuinely running, and learn which process has it.
    #    Getting here at all means the forward run failed at settle and the row was re-claimed
    #    as COMPENSATING -- the unwind is under way.
    started = await wait_for(
        f"{GATED_NAME!r} to start on some worker",
        lambda: _attempts(conn, workflow_id, GATED_NAME),
        workers,
        give_up_after=STARTUP_TIMEOUT_SECONDS,
    )
    victim_pid = started[0]["pid"]
    victim_owner = started[0]["owner_id"]

    # 2. SIGKILL it. No finally, no drain, no flush -- its open refund transaction dies with it.
    workers.kill(victim_pid)
    killed_at = time.monotonic()

    # 3. Release the gate so the recovering attempt returns at once. Ordering matters: the gate
    #    is opened only after the victim is already dead, so it can never be what let the
    #    victim finish.
    await _release_gate(conn, workflow_id)

    # 4. Another *process* must resume the unwind, within the lease.
    async def taken_over():
        rows = await _attempts(conn, workflow_id, GATED_NAME)
        return rows if len(rows) >= 2 else None

    attempts = await wait_for(
        f"another worker to resume {GATED_NAME!r}",
        taken_over,
        workers,
        give_up_after=LEASE_SECONDS + RECOVERY_MARGIN_SECONDS,
    )
    recovery_seconds = time.monotonic() - killed_at
    assert attempts[1]["pid"] != victim_pid, (
        f"{GATED_NAME!r} was resumed by pid {attempts[1]['pid']}, the process this test "
        "killed. Recovery must come from a different process."
    )
    assert attempts[1]["owner_id"] != victim_owner, (
        f"the resuming worker reported owner_id {attempts[1]['owner_id']!r}, the same as the "
        "killed worker -- owner_id must be unique per process or the ownership guard is blind."
    )
    assert recovery_seconds <= LEASE_SECONDS + RECOVERY_MARGIN_SECONDS, (
        f"recovery took {recovery_seconds:.2f}s, beyond the {LEASE_SECONDS}s lease "
        f"(+{RECOVERY_MARGIN_SECONDS:.0f}s margin). An expired lease is the only recovery "
        "mechanism, so this bound is the crash-recovery latency."
    )

    # 5. The saga must finish unwinding.
    final = await wait_for(
        "the workflow to reach COMPENSATED",
        lambda: conn.fetchrow(
            "SELECT status, attempt, error FROM workflows WHERE id = $1 "
            "AND status = 'COMPENSATED'",
            workflow_id,
        ),
        workers,
        give_up_after=COMPLETION_TIMEOUT_SECONDS,
    )

    tries = await _tally(conn, workflow_id, "step_attempts")
    effects = await _tally(conn, workflow_id, "side_effects")
    undone = await _checkpoints(conn, workflow_id, "COMPENSATION")
    forward = await _checkpoints(conn, workflow_id, "FORWARD")

    # The heart of it: the killed compensation was attempted twice and took effect once, while
    # the other compensation ran exactly once and no forward step ran again at all.
    assert tries == {
        STEP_NAMES[0]: 1,
        STEP_NAMES[1]: 1,
        STEP_NAMES[2]: 1,
        COMPLETED_NAME: 1,
        GATED_NAME: 2,
    }, (
        f"attempts were {tries}, expected the killed compensation to be attempted twice and "
        f"everything else exactly once. If {GATED_NAME!r} shows 1, the kill landed before the "
        f"undo started and this run proved nothing. If {COMPLETED_NAME!r} shows 2, the resume "
        "re-ran an undo that had already committed its COMPENSATION checkpoint -- that is a "
        "second refund, and it is the exact failure this file exists to catch."
        f"{workers.diagnostics()}"
    )
    assert effects == {
        STEP_NAMES[0]: 1,
        STEP_NAMES[1]: 1,
        COMPLETED_NAME: 1,
        GATED_NAME: 1,
    }, (
        f"side effects were {effects}, expected exactly one each. More than one for "
        f"{COMPLETED_NAME!r} means an already-checkpointed undo ran a second time; more than "
        f"one for {GATED_NAME!r} means the killed attempt's transaction committed after all. "
        f"{STEP_NAMES[2]!r} must appear nowhere: it never completed."
        f"{workers.diagnostics()}"
    )

    # And the order survived the crash. Reverse seq is a dependency order, so a resume that
    # restarted from the wrong end would still show every count as 1 and still be wrong.
    assert undone == list(UNWIND_ORDER), (
        f"COMPENSATION checkpoints committed in the order {undone}, expected "
        f"{list(UNWIND_ORDER)} -- reverse seq. The unwind resumed in the wrong direction."
        f"{workers.diagnostics()}"
    )
    assert len(undone) == len(set(undone)), (
        f"duplicate COMPENSATION rows in {undone}: the checkpoint log is the idempotency "
        "guard and cannot carry duplicates"
    )
    assert forward == [STEP_NAMES[0], STEP_NAMES[1]], (
        f"FORWARD checkpoints were {forward}; {STEP_NAMES[2]!r} never completed and the "
        "unwind must not have added or replayed any."
    )
    assert final["error"] is not None, (
        "the terminal failure that sent this saga to COMPENSATING is the most useful thing to "
        "read off the row afterwards; the unwind must not clear it"
    )
