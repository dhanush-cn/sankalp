"""The third crash gate: SIGKILL a real ``sankalp-drain`` process between the XADD and the mark.

``tests/test_outbox_drain.py::test_a_crash_between_the_xadd_and_the_mark_republishes`` proves
the same property with an in-process fault injection -- a publisher that performs the real
XADD and then raises. This file proves it against a process that stops existing between two
instructions, the way ``tests/test_crash.py`` and ``tests/test_compensation_crash.py`` prove
the forward and compensation guarantees. Run it the way it is meant to be run::

    pytest tests/test_drain_crash.py --count=20        # or: make test-drain-crash

**How the kill is aimed.** ``engine/drain.py::_GatedPublisher`` -- built only by ``run_drain``,
and only when the crash gate is armed -- performs the real ``XADD`` and only then records a
``step_attempts`` row (carrying its pid, under the step name ``"outbox.drain"``) and blocks on
``crash_gates``, reusing the exact instrumentation ``migrations/002_crash_gate.sql`` and
``workflows/_instrumentation.py`` built for the other two gates. The test waits for that row,
SIGKILLs the pid it names, and only then releases the gate -- so the kill provably lands after
the event reached Redis and before ``published_at`` was ever going to be stamped.
"""

from __future__ import annotations

import time
import uuid

import asyncpg
from fleet import (
    COMPLETION_TIMEOUT_SECONDS,
    STARTUP_TIMEOUT_SECONDS,
    WorkerFleet,
)
from fleet import wait_for as _wait_for


async def _attempts(conn: asyncpg.Connection, workflow_id: uuid.UUID):
    return await conn.fetch(
        "SELECT owner_id, pid FROM step_attempts WHERE workflow_id = $1 AND step_name = "
        "'outbox.drain' ORDER BY id",
        workflow_id,
    )


async def _release_gate(conn: asyncpg.Connection, workflow_id: uuid.UUID) -> None:
    await conn.execute(
        "INSERT INTO crash_gates (workflow_id, step_name) VALUES ($1, 'outbox.drain')",
        workflow_id,
    )


async def test_sigkill_between_the_xadd_and_the_mark_republishes(
    conn: asyncpg.Connection, insert_workflow, workers: WorkerFleet, redis_client, event_stream
) -> None:
    workflow_id = await insert_workflow()
    await conn.execute(
        "INSERT INTO outbox (workflow_id, event_type, payload) "
        "VALUES ($1, 'step.completed', '{}'::jsonb)",
        workflow_id,
    )

    # 1. A real `sankalp-drain` process, pointed at this test's own stream key so it cannot
    #    collide with anything another test left behind.
    workers.launch(
        count=1,
        module="sankalp.engine.drain",
        ready_marker="draining:",
        extra_env={"SANKALP_OUTBOX_STREAM": event_stream},
    )

    # 2. Wait until the gate is holding it -- which happens only AFTER the real XADD.
    started = await _wait_for(
        "the drain to publish and gate on 'outbox.drain'",
        lambda: _attempts(conn, workflow_id),
        workers,
        give_up_after=STARTUP_TIMEOUT_SECONDS,
    )
    assert (
        await redis_client.xlen(event_stream) == 1
    ), "the gate fired before the XADD -- the kill would land in the wrong place"
    victim_pid = started[0]["pid"]

    # 3. SIGKILL it. No finally, no drain-of-the-drain, no flush.
    workers.kill(victim_pid)
    killed_at = time.monotonic()

    # 4. The mark must never have run: the row is exactly as unpublished as it was before the
    #    gate fired, because the claim transaction died with the process that held it.
    unpublished = await conn.fetchval(
        "SELECT published_at FROM outbox WHERE workflow_id = $1", workflow_id
    )
    assert unpublished is None, (
        "published_at was stamped despite the process being killed before the mark could run"
    )
    assert await redis_client.xlen(event_stream) == 1, (
        "the killed attempt's XADD must still be the only entry -- nothing has republished yet"
    )

    # 5. Release the gate (a no-op for the dead process) and bring up a second, clean drain to
    #    republish. Ordering matters: the release happens only after the victim is confirmed
    #    dead, so it can never be what let the victim finish.
    await _release_gate(conn, workflow_id)
    workers.launch(
        count=1,
        module="sankalp.engine.drain",
        ready_marker="draining:",
        extra_env={"SANKALP_OUTBOX_STREAM": event_stream},
    )

    published = await _wait_for(
        "the workflow's event to be marked published",
        lambda: conn.fetchval(
            "SELECT published_at FROM outbox WHERE workflow_id = $1", workflow_id
        ),
        workers,
        give_up_after=COMPLETION_TIMEOUT_SECONDS,
    )
    recovery_seconds = time.monotonic() - killed_at
    assert published is not None

    # 6. The at-least-once proof: TWO stream entries, because the killed attempt's XADD really
    #    happened, but they must carry the SAME event_id -- the only thing that lets a
    #    consumer collapse a republish back to one effect.
    entries = await redis_client.xrange(event_stream)
    assert len(entries) == 2, (
        f"expected the crashed attempt's XADD plus the republish, got {len(entries)}"
        f"{workers.diagnostics()}"
    )
    event_ids = {fields["event_id"] for _, fields in entries}
    assert event_ids == {"1"}, (
        f"the two stream entries must carry the SAME event_id, got {event_ids} -- a consumer "
        "cannot dedupe a republish that shows up under a different identity"
        f"{workers.diagnostics()}"
    )
    assert recovery_seconds < COMPLETION_TIMEOUT_SECONDS
