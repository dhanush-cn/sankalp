"""The Phase 1 gate: SIGKILL a real worker process mid-step and prove the guarantee.

CLAUDE.md states it as: *kill any process at any instant -- workflows resume from the last
completed step, and no step's side effect executes twice.* Every other crash simulation in
this suite is cooperative and therefore does not test that. ``expire_lease`` forges the
symptom with an UPDATE. ``task.cancel()`` unwinds through ``except asyncio.CancelledError``
in both the executor and the worker. SIGTERM runs the drain in ``worker.py``, which
deliberately lets in-flight work *finish*. A worker that gets to run its handlers is not the
thing the guarantee is about.

So this file uses real OS processes and ``SIGKILL``: no ``finally``, no drain, no shielded
checkpoint, no flush. The process stops existing between two instructions.

**How the kill is aimed.** ``demo_crash``'s step 2 commits a ``step_attempts`` row carrying
its own pid and then blocks, holding an *uncommitted* ``side_effects`` INSERT open. The test
waits for that row and kills that pid, so the crash lands inside step 2 by construction
rather than by timing a sleep. The killed transaction rolls back, which is what makes
"exactly one side effect for step 2" an exact assertion instead of a likely one.

**Why the counts cannot be faked.** ``step_attempts`` for step 2 must read exactly **2** and
``side_effects`` for it exactly **1**: attempted twice, took effect once. Without the attempt
count the test would pass by killing a worker that had not yet reached step 2 -- the workflow
would still succeed and every side-effect count would still read 1, having proven nothing
about resuming mid-step.

**What this does not claim.** For steps 1 and 3 the side effect commits just before
``commit_step_output``, so a kill in that microsecond window would produce two rows on
replay. That is at-least-once execution behaving exactly as designed, and it is why CLAUDE.md
says exactly-once *effects* via at-least-once execution plus idempotency -- never
exactly-once delivery. This test kills in the window the spec's gate describes and asserts
what is actually guaranteed there.

Run it the way it is meant to be run::

    pytest tests/test_crash.py --count=20        # or: make test-crash
"""

from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

import asyncpg
import pytest

from sankalp.workflows.demo import GATED_STEP, STEP_NAMES, WORKFLOW_TYPE

REPO_ROOT = Path(__file__).resolve().parents[1]

#: Short enough that 20 repetitions are minutes rather than an afternoon, long enough that a
#: healthy worker on a loaded machine renews comfortably (the renewer ticks at lease/3).
#: Recovery latency is bounded by this and nothing else -- there is no reaper.
LEASE_SECONDS = 5

#: Three, so that after one is killed there is still more than one candidate to take over --
#: a single survivor would also prove recovery, but not that recovery survives contention.
WORKER_COUNT = 3

#: Allowance on top of the lease for claim, poll and process scheduling. Generous enough not
#: to flake on a loaded machine, tight enough that "recovered within lease_duration" is still
#: a real bound rather than a formality.
RECOVERY_MARGIN_SECONDS = 3.0

#: Ceiling on waiting for the whole workflow to finish after the kill.
COMPLETION_TIMEOUT_SECONDS = LEASE_SECONDS + 20.0

#: Worker startup: interpreter boot, imports, first poll.
STARTUP_TIMEOUT_SECONDS = 30.0

_POLL_SECONDS = 0.02


# ---------------------------------------------------------------------------
# Worker subprocesses
# ---------------------------------------------------------------------------


class WorkerFleet:
    """Real ``python -m sankalp.engine.worker`` processes, and the log tails to explain them.

    Output goes to temporary files rather than ``subprocess.PIPE``: a worker logs on every
    claim and every recovery, and nothing in this test reads the pipes while it waits, so a
    PIPE would fill its buffer and block the child -- stalling the very process whose
    liveness is under test.
    """

    def __init__(self) -> None:
        self._procs: dict[int, subprocess.Popen[bytes]] = {}
        self._logs: dict[int, Path] = {}

    @property
    def pids(self) -> set[int]:
        return set(self._procs)

    def launch(self, count: int = WORKER_COUNT) -> None:
        """Start ``count`` workers and block until each has reached its polling loop."""
        for index in range(count):
            handle, path = tempfile.mkstemp(prefix=f"crash-worker-{index}-", suffix=".log")
            proc = subprocess.Popen(  # noqa: S603 - fixed argv, never shell=True
                [sys.executable, "-m", "sankalp.engine.worker"],
                env=self._env(index),
                cwd=REPO_ROOT,
                stdout=handle,
                stderr=subprocess.STDOUT,
            )
            os.close(handle)
            self._procs[proc.pid] = proc
            self._logs[proc.pid] = Path(path)
        self._await_ready()

    @staticmethod
    def _env(index: int) -> dict[str, str]:
        return {
            **os.environ,
            # The suite's own guard (config.py validates the name ends in _test) plus this is
            # what keeps a worker that claims real work pointed at sankalp_test: create_pool
            # resolves active_database_url from it.
            "SANKALP_ENVIRONMENT": "test",
            # owner_id must be unique per process -- it is half of every ownership guard, so
            # two workers sharing one would each accept the other's writes as their own.
            "SANKALP_WORKER_ID": f"crash-worker-{index}-{uuid.uuid4().hex[:8]}",
            "SANKALP_LEASE_DURATION_SECONDS": str(LEASE_SECONDS),
            "SANKALP_POLL_INTERVAL_SECONDS": "0.05",
            "SANKALP_LOG_LEVEL": "INFO",
        }

    def _await_ready(self) -> None:
        """Wait for every worker to log that it is polling, or fail with its output.

        A worker that dies on import would otherwise show up much later as an unexplained
        timeout waiting for a workflow nobody ever claimed.
        """
        deadline = time.monotonic() + STARTUP_TIMEOUT_SECONDS
        pending = set(self._procs)
        while pending:
            for pid in list(pending):
                proc = self._procs[pid]
                if proc.poll() is not None:
                    raise AssertionError(
                        f"worker pid {pid} exited with {proc.returncode} before it started "
                        f"polling.{self.diagnostics()}"
                    )
                if "polling:" in self._read(pid):
                    pending.discard(pid)
            if not pending:
                return
            if time.monotonic() >= deadline:
                raise AssertionError(
                    f"workers {sorted(pending)} did not start polling within "
                    f"{STARTUP_TIMEOUT_SECONDS:.0f}s.{self.diagnostics()}"
                )
            time.sleep(_POLL_SECONDS)

    def kill(self, pid: int) -> None:
        """SIGKILL one worker -- the whole point of this file.

        Refuses any pid this fleet did not spawn. The pid is read out of a database row
        written by another process, and signalling something we did not start on the strength
        of that would be indefensible however well the query is scoped.
        """
        if pid not in self._procs:
            raise AssertionError(
                f"refusing to SIGKILL pid {pid}: not a worker this test started "
                f"(started {sorted(self._procs)})"
            )
        os.kill(pid, signal.SIGKILL)
        self._procs[pid].wait(timeout=10)

    def _read(self, pid: int) -> str:
        try:
            return self._logs[pid].read_text(encoding="utf-8", errors="replace")
        except OSError:
            return ""

    def diagnostics(self) -> str:
        """Worker logs, for the failure message. Tests that fail silently are worthless."""
        parts = ["", "--- worker logs ---"]
        for pid, proc in self._procs.items():
            state = "running" if proc.poll() is None else f"exited {proc.returncode}"
            tail = "\n".join(self._read(pid).splitlines()[-25:]) or "(no output)"
            parts.append(f"[pid {pid}, {state}]\n{tail}")
        return "\n".join(parts)

    def shutdown(self) -> None:
        """Kill and reap every worker, then remove its log. Runs even when a test fails."""
        for proc in self._procs.values():
            if proc.poll() is None:
                proc.kill()
        for proc in self._procs.values():
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:  # pragma: no cover - a worker ignoring SIGKILL
                pass
        for path in self._logs.values():
            path.unlink(missing_ok=True)


@pytest.fixture
def workers(truncate_tables: None) -> WorkerFleet:
    """A fleet of real worker processes, always torn down.

    Depends on ``truncate_tables`` so no worker is running while the tables are emptied: a
    worker that claimed a row from the previous repetition would be writing into a schema
    this one is about to wipe.
    """
    fleet = WorkerFleet()
    try:
        yield fleet
    finally:
        fleet.shutdown()


# ---------------------------------------------------------------------------
# Observing the workflow from outside the workers
# ---------------------------------------------------------------------------


async def _wait_for(what: str, probe, fleet: WorkerFleet, give_up_after: float):
    """Poll ``probe`` until it returns something truthy, or fail naming what was awaited.

    Named ``give_up_after`` rather than ``timeout`` to match ``test_worker.wait_for`` -- and
    because ruff's ASYNC109 is right that a ``timeout`` argument on an async function should
    usually be ``asyncio.timeout``. It cannot be here: the failure message has to include the
    worker logs, and a ``TimeoutError`` raised out of a context manager cannot reach them.
    """
    deadline = time.monotonic() + give_up_after
    while True:
        result = await probe()
        if result:
            return result
        if time.monotonic() >= deadline:
            raise AssertionError(
                f"timed out after {give_up_after:.1f}s waiting for {what}."
                f"{fleet.diagnostics()}"
            )
        await asyncio.sleep(_POLL_SECONDS)


async def _attempts(conn: asyncpg.Connection, workflow_id: uuid.UUID, step_name: str):
    """Every recorded attempt at one step, oldest first."""
    return await conn.fetch(
        """
        SELECT owner_id, pid, attempted_at
        FROM step_attempts
        WHERE workflow_id = $1 AND step_name = $2
        ORDER BY id
        """,
        workflow_id,
        step_name,
    )


async def _counts_by_step(
    conn: asyncpg.Connection, workflow_id: uuid.UUID
) -> tuple[dict[str, int], dict[str, int], dict[str, int]]:
    """(side effects, attempts, forward checkpoints) per step name."""

    async def tally(sql: str) -> dict[str, int]:
        return {r["step_name"]: r["n"] for r in await conn.fetch(sql, workflow_id)}

    return (
        await tally(
            "SELECT step_name, count(*) AS n FROM side_effects "
            "WHERE workflow_id = $1 GROUP BY step_name"
        ),
        await tally(
            "SELECT step_name, count(*) AS n FROM step_attempts "
            "WHERE workflow_id = $1 GROUP BY step_name"
        ),
        await tally(
            "SELECT step_name, count(*) AS n FROM step_outputs "
            "WHERE workflow_id = $1 AND kind = 'FORWARD' GROUP BY step_name"
        ),
    )


async def _release_gate(conn: asyncpg.Connection, workflow_id: uuid.UUID) -> None:
    """Let the *recovering* attempt of the gated step finish immediately."""
    await conn.execute(
        "INSERT INTO crash_gates (workflow_id, step_name) VALUES ($1, $2)",
        workflow_id,
        GATED_STEP,
    )


# ---------------------------------------------------------------------------
# The gate
# ---------------------------------------------------------------------------


async def _run_crash_gate(
    conn: asyncpg.Connection,
    insert_workflow,
    fleet: WorkerFleet,
    workflow_input: dict[str, object],
    *,
    release_gate: bool,
) -> None:
    """Submit, kill mid-step-2, and assert the guarantee held.

    Shared by both variants because the assertions are the point and they are identical: what
    step 2 blocks on changes nothing about what must be true afterwards.
    """
    fleet.launch()
    workflow_id = await insert_workflow(workflow_type=WORKFLOW_TYPE, input=workflow_input)

    # 1. Wait until step 2 is genuinely running, and learn which process is running it.
    started = await _wait_for(
        f"{GATED_STEP!r} to start on some worker",
        lambda: _attempts(conn, workflow_id, GATED_STEP),
        fleet,
        give_up_after=STARTUP_TIMEOUT_SECONDS,
    )
    victim_pid = started[0]["pid"]
    victim_owner = started[0]["owner_id"]

    # 2. SIGKILL it. No finally, no drain, no flush -- its open transaction dies with it.
    fleet.kill(victim_pid)
    killed_at = time.monotonic()

    # 3. Release the gate so the recovering attempt returns at once. Ordering matters: the
    #    gate is opened only after the victim is already dead, so it can never be what let
    #    the victim finish.
    if release_gate:
        await _release_gate(conn, workflow_id)

    # 4. Another *process* must pick the workflow up, within the lease.
    async def taken_over():
        rows = await _attempts(conn, workflow_id, GATED_STEP)
        return rows if len(rows) >= 2 else None

    attempts = await _wait_for(
        f"another worker to resume {GATED_STEP!r}",
        taken_over,
        fleet,
        give_up_after=LEASE_SECONDS + RECOVERY_MARGIN_SECONDS,
    )
    recovery_seconds = time.monotonic() - killed_at
    assert attempts[1]["pid"] != victim_pid, (
        f"{GATED_STEP!r} was resumed by pid {attempts[1]['pid']}, the process this test "
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

    # 5. The workflow must finish.
    row = await _wait_for(
        "the workflow to reach SUCCESS",
        lambda: conn.fetchrow(
            "SELECT status, attempt, current_step FROM workflows WHERE id = $1 "
            "AND status = 'SUCCESS'",
            workflow_id,
        ),
        fleet,
        give_up_after=COMPLETION_TIMEOUT_SECONDS,
    )

    effects, tries, checkpoints = await _counts_by_step(conn, workflow_id)

    # The heart of it: step 2 was attempted twice and took effect once, while the step before
    # it was neither re-attempted nor re-executed.
    assert tries == {STEP_NAMES[0]: 1, GATED_STEP: 2, STEP_NAMES[2]: 1}, (
        f"attempts per step were {tries}, expected the killed step to be attempted twice and "
        f"every other step once. If {GATED_STEP!r} shows 1, the kill landed before the step "
        f"started and this run proved nothing; if {STEP_NAMES[0]!r} shows 2, the resume "
        "re-ran a completed step instead of replaying its checkpoint."
        f"{fleet.diagnostics()}"
    )
    assert effects == {name: 1 for name in STEP_NAMES}, (
        f"side effects per step were {effects}, expected exactly one each. More than one for "
        f"{GATED_STEP!r} means the killed attempt's transaction committed after all -- its "
        "side effect ran twice, which is the failure this engine exists to prevent."
        f"{fleet.diagnostics()}"
    )
    assert checkpoints == {name: 1 for name in STEP_NAMES}, (
        f"FORWARD step_outputs rows were {checkpoints}, expected exactly one per step -- the "
        "checkpoint log is the idempotency guard and cannot carry duplicates."
    )
    assert row["attempt"] == 2, (
        f"workflows.attempt is {row['attempt']}, expected 2. The dequeue query increments it "
        "on every claim, so 2 is the signature of exactly one recovery re-claim; 1 would mean "
        "the workflow was never re-claimed and the kill did not exercise recovery."
    )
    assert row["current_step"] == STEP_NAMES[-1]


async def test_sigkill_mid_step_resumes_without_repeating_side_effects(
    conn: asyncpg.Connection, insert_workflow, workers: WorkerFleet
) -> None:
    """The gate. Step 2 blocks on a gate this test holds, so the kill is exactly aimed.

    This is the variant that runs 20x: with nothing to sleep through, a repetition costs the
    lease and little else.
    """
    await _run_crash_gate(
        conn,
        insert_workflow,
        workers,
        {"amount_minor": 250_000, "mode": "gate"},
        release_gate=True,
    )


async def test_sigkill_during_real_work_resumes_without_repeating_side_effects(
    conn: asyncpg.Connection, insert_workflow, workers: WorkerFleet
) -> None:
    """The same gate, killed inside a real ``asyncio.sleep`` rather than a coordination loop.

    Not luck: step 2 commits its ``step_attempts`` row *before* the sleep and this test polls
    at 20ms, so the kill lands tens of milliseconds into a 1000ms window. What it adds over
    the gated variant is that the killed step was doing ordinary work, with nothing in it
    that exists for the test's benefit.
    """
    await _run_crash_gate(
        conn,
        insert_workflow,
        workers,
        {"amount_minor": 250_000, "mode": "sleep", "sleep_seconds": 1.0},
        release_gate=False,
    )
