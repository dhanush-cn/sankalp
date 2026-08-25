"""Real worker processes, and the log tails needed to explain them when a gate fails.

Extracted from ``test_crash.py`` unchanged when a second gate needed it: ``test_crash.py``
SIGKILLs a worker inside a forward step, ``test_compensation_crash.py`` inside a compensation,
and both need the same three things -- processes that are genuinely separate from pytest, a
readiness check so a worker that dies on import is reported as that rather than as a mysterious
timeout, and a kill that refuses any pid this harness did not spawn.

Everything here is deliberately *not* cooperative. ``expire_lease`` forges a crash's symptom
with an UPDATE, ``task.cancel()`` unwinds through ``except asyncio.CancelledError``, and
SIGTERM runs the drain in ``worker.py``, which lets in-flight work finish. A worker that gets
to run its handlers is not the thing the guarantee is about. These processes stop existing
between two instructions.
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
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

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

POLL_SECONDS = 0.02


class WorkerFleet:
    """Real ``python -m sankalp.engine.worker`` processes, and the log tails to explain them.

    Output goes to temporary files rather than ``subprocess.PIPE``: a worker logs on every
    claim and every recovery, and nothing in these tests reads the pipes while it waits, so a
    PIPE would fill its buffer and block the child -- stalling the very process whose liveness
    is under test.
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
            # The crash gates fail safe and need BOTH facts to arm (config.py,
            # Settings.crash_gate_armed). Setting only this one would leave every gate a
            # no-op, the kill would land after the step had already finished, and the gate
            # tests would pass while proving nothing -- which is why the workers log loudly
            # when they find a gate unarmed.
            "SANKALP_CRASH_GATE_ENABLED": "1",
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
            time.sleep(POLL_SECONDS)

    def kill(self, pid: int) -> None:
        """SIGKILL one worker -- the whole point of these files.

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


async def wait_for(
    what: str,
    probe: Callable[[], Awaitable[Any]],
    fleet: WorkerFleet,
    give_up_after: float,
) -> Any:
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
        await asyncio.sleep(POLL_SECONDS)
