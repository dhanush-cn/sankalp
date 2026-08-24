"""The worker loop, proved against a real database and real signals.

Four properties, each of which is a way the worker can quietly lose work:

  * the semaphore actually caps in-flight workflows, and the worker never claims more than
    it has room to run -- a claimed workflow carries a lease nobody is renewing while it
    waits in a local queue, so over-claiming gets work stolen mid-flight;
  * SIGTERM stops claiming but lets in-flight workflows commit, which is what separates a
    rolling deploy that finishes its work from one that abandons a lease-duration of it;
  * a cancellation of ``run()`` stops *now* rather than sitting out the grace period, and
    still cancels and awaits everything in flight;
  * the background renewer keeps a step that outlives its lease from being stolen.

No mocks and no fakes: a real Postgres, a real ``Worker``, and a real ``os.kill``. The
signal test is the only one that touches the pytest process itself, and it waits until the
worker has installed its handlers before firing so the signal cannot escape to the default
disposition and kill the run.
"""

from __future__ import annotations

import asyncio
import os
import signal
import time
import uuid

import pytest

from sankalp.config import Settings
from sankalp.engine.definition import StepContext, clear_registry, step, workflow
from sankalp.engine.worker import Worker
from sankalp.storage.queue import claim_workflows

WORKFLOW_TYPE = "payment_transfer"


@pytest.fixture(autouse=True)
def isolated_registry():
    clear_registry()
    yield
    clear_registry()


def make_settings(**overrides) -> Settings:
    """Worker settings tuned for a test: poll fast, keep the lease short, fail fast."""
    base = dict(
        worker_concurrency=4,
        dequeue_batch_size=10,
        poll_interval_seconds=0.02,
        lease_duration_seconds=30,
        lease_renew_divisor=3,
        worker_shutdown_grace_seconds=5.0,
        max_attempts=3,
    )
    return Settings(**(base | overrides))


async def wait_for(predicate, *, give_up_after: float = 10.0, interval: float = 0.02) -> bool:
    """Poll ``predicate`` until it is true or ``give_up_after`` seconds elapse.

    Everything here is driven by a worker running concurrently, so the tests wait on
    observable state rather than on a sleep long enough to "probably" be sufficient.
    """
    deadline = time.monotonic() + give_up_after
    while time.monotonic() < deadline:
        if await predicate():
            return True
        await asyncio.sleep(interval)
    return False


async def statuses(pool) -> list[str]:
    return [r["status"] for r in await pool.fetch("SELECT status FROM workflows ORDER BY id")]


async def status_of(pool, workflow_id: uuid.UUID) -> str:
    return await pool.fetchval("SELECT status FROM workflows WHERE id = $1", workflow_id)


async def stop(worker: Worker, task: asyncio.Task) -> None:
    """Ask a worker to stop and wait for it, failing loudly rather than hanging the suite."""
    worker.request_shutdown()
    await asyncio.wait_for(task, timeout=15)


# ---------------------------------------------------------------------------
# 1. The baseline: a worker empties the queue.
# ---------------------------------------------------------------------------


async def test_a_worker_drains_the_queue(pool, insert_workflow):
    executed: list[uuid.UUID] = []

    @workflow(WORKFLOW_TYPE)
    class Transfer:
        @step(seq=1)
        async def debit_wallet(self, ctx: StepContext) -> dict[str, int]:
            executed.append(ctx.workflow_id)
            return {"debited_minor": ctx.input["amount_minor"]}

    for _ in range(5):
        await insert_workflow()

    worker = Worker(pool, settings=make_settings(), owner_id="worker-a")
    task = asyncio.create_task(worker.run())
    try:
        drained = await wait_for(lambda: _all_success(pool))
    finally:
        await stop(worker, task)

    assert drained, f"queue did not drain: {await statuses(pool)}"
    assert len(executed) == 5


async def _all_success(pool) -> bool:
    return await pool.fetchval("SELECT count(*) FROM workflows WHERE status <> 'SUCCESS'") == 0


# ---------------------------------------------------------------------------
# 2. The concurrency bound.
# ---------------------------------------------------------------------------


async def test_the_semaphore_caps_in_flight_workflows(pool, insert_workflow):
    """Concurrency 2 against 8 workflows: never three at once, and genuinely two at once.

    Both halves matter. The upper bound alone is satisfied by a worker that runs everything
    serially, which would prove nothing about the semaphore -- so the test also asserts the
    limit is actually reached.
    """
    concurrent = 0
    high_water = 0

    @workflow(WORKFLOW_TYPE)
    class Slow:
        @step(seq=1)
        async def debit_wallet(self, ctx: StepContext) -> dict[str, bool]:
            nonlocal concurrent, high_water
            concurrent += 1
            high_water = max(high_water, concurrent)
            try:
                await asyncio.sleep(0.1)
            finally:
                concurrent -= 1
            return {"ok": True}

    for _ in range(8):
        await insert_workflow()

    worker = Worker(pool, settings=make_settings(worker_concurrency=2), owner_id="worker-a")
    task = asyncio.create_task(worker.run())
    try:
        drained = await wait_for(lambda: _all_success(pool))
    finally:
        await stop(worker, task)

    assert drained, f"queue did not drain: {await statuses(pool)}"
    assert high_water <= 2, f"{high_water} workflows ran at once against a limit of 2"
    assert high_water == 2, "the limit was never reached -- this proves nothing about it"


async def test_a_worker_does_not_claim_more_than_it_can_run(pool, insert_workflow):
    """Slots are taken before the dequeue query, so the batch is sized to real capacity.

    A workflow claimed but not started carries a lease this worker is not renewing, so it
    would be stolen mid-wait by another worker seeing an expired lease. With concurrency 1
    and eight workflows queued, exactly one may be owned at any instant.
    """
    running = asyncio.Event()
    release = asyncio.Event()

    @workflow(WORKFLOW_TYPE)
    class Blocking:
        @step(seq=1)
        async def debit_wallet(self, ctx: StepContext) -> dict[str, bool]:
            running.set()
            await release.wait()
            return {"ok": True}

    for _ in range(8):
        await insert_workflow()

    worker = Worker(pool, settings=make_settings(worker_concurrency=1), owner_id="worker-a")
    task = asyncio.create_task(worker.run())
    try:
        await asyncio.wait_for(running.wait(), timeout=10)
        # Give the poll loop room to over-claim if it were going to.
        await asyncio.sleep(0.2)
        owned = await pool.fetchval(
            "SELECT count(*) FROM workflows WHERE owner_id = 'worker-a'"
        )
        assert owned == 1, f"worker claimed {owned} workflows while able to run 1"
    finally:
        release.set()
        await stop(worker, task)


# ---------------------------------------------------------------------------
# 3. SIGTERM: stop claiming, let in-flight work commit.
# ---------------------------------------------------------------------------


async def test_sigterm_stops_claiming_and_lets_in_flight_work_commit(pool, insert_workflow):
    """A real SIGTERM to this process, handled by the worker's own handler.

    The in-flight workflow must reach SUCCESS -- its checkpoint and its terminal transition
    both committed after the signal arrived -- while the queued one must be left untouched
    for the next worker, rather than claimed and then abandoned holding a lease.
    """
    running = asyncio.Event()
    release = asyncio.Event()

    @workflow(WORKFLOW_TYPE)
    class Blocking:
        @step(seq=1)
        async def debit_wallet(self, ctx: StepContext) -> dict[str, int]:
            running.set()
            await release.wait()
            return {"debited_minor": ctx.input["amount_minor"]}

    first = await insert_workflow()
    second = await insert_workflow()

    worker = Worker(pool, settings=make_settings(worker_concurrency=1), owner_id="worker-a")
    task = asyncio.create_task(worker.run())
    try:
        # Wait for the step: by then run() has installed the handler, so the signal cannot
        # reach the default disposition and take down pytest.
        await asyncio.wait_for(running.wait(), timeout=10)
        in_flight_id = await pool.fetchval(
            "SELECT id FROM workflows WHERE owner_id = 'worker-a'"
        )

        os.kill(os.getpid(), signal.SIGTERM)
        await asyncio.sleep(0.2)  # the loop would have claimed again by now if it were going to

        release.set()
        await asyncio.wait_for(task, timeout=15)
    finally:
        release.set()
        if not task.done():
            worker.request_shutdown()
            await asyncio.wait_for(task, timeout=15)

    assert await status_of(pool, in_flight_id) == "SUCCESS", (
        "the in-flight workflow did not commit after SIGTERM"
    )
    # The other one was never touched: still queued, unowned, and claimable by anyone.
    queued = second if in_flight_id == first else first
    untouched = await pool.fetchrow("SELECT * FROM workflows WHERE id = $1", queued)
    assert untouched["status"] == "PENDING"
    assert untouched["owner_id"] is None
    assert untouched["attempt"] == 0, "the queued workflow was claimed and then abandoned"

    async with pool.acquire() as conn:
        assert [w.id for w in await claim_workflows(conn, "worker-b", 30, 10)] == [queued]


# ---------------------------------------------------------------------------
# 4. Cancellation: stop now, but still drain.
# ---------------------------------------------------------------------------


async def test_an_external_cancel_drains_without_waiting_out_the_grace(pool, insert_workflow):
    """Regression: ``_drain`` used to sit out the full grace period on a cancellation.

    A cancellation is an order to stop now. Waiting the grace first turned a supervisor's
    ``task.cancel()`` into a hang of up to ``worker_shutdown_grace_seconds`` -- measured at
    1.95s against a 1s grace before the fix, and it scales with the setting, so the 30s
    default meant a 30s hang.

    Both halves are asserted: that it returns promptly, and that promptness did not come at
    the cost of abandoning the in-flight task. It must still be cancelled and awaited, which
    is what the executor's shielded checkpoint write depends on.
    """
    running = asyncio.Event()
    step_saw_cancel = asyncio.Event()

    @workflow(WORKFLOW_TYPE)
    class LongStep:
        @step(seq=1)
        async def debit_wallet(self, ctx: StepContext) -> dict[str, bool]:
            running.set()
            try:
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                step_saw_cancel.set()
                raise
            return {"ok": True}

    await insert_workflow()
    settings = make_settings(worker_concurrency=2, worker_shutdown_grace_seconds=5.0)
    worker = Worker(pool, settings=settings, owner_id="worker-a")
    task = asyncio.create_task(worker.run())

    await asyncio.wait_for(running.wait(), timeout=10)
    began = time.monotonic()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=15)
    elapsed = time.monotonic() - began

    assert elapsed < 2.0, (
        f"cancel took {elapsed:.2f}s: the drain sat out the "
        f"{settings.worker_shutdown_grace_seconds}s grace instead of stopping now"
    )
    assert step_saw_cancel.is_set(), "the in-flight workflow was abandoned, not cancelled"
    await asyncio.sleep(0)  # let the tasks' done-callbacks run
    assert worker.in_flight == 0, "a task outlived the drain"


async def test_a_cancelled_workflow_is_left_for_its_lease(pool, insert_workflow):
    """Cancelled work is not lost, and not mislabelled either.

    The row keeps its checkpoints and its lease; it is recovered by the same expired-lease
    path that recovers a killed process. What must NOT happen is a state transition -- the
    workflow did not fail, this process was told to stop.
    """
    running = asyncio.Event()

    @workflow(WORKFLOW_TYPE)
    class LongStep:
        @step(seq=1)
        async def debit_wallet(self, ctx: StepContext) -> dict[str, bool]:
            running.set()
            await asyncio.sleep(30)
            return {"ok": True}

    workflow_id = await insert_workflow()
    worker = Worker(pool, settings=make_settings(), owner_id="worker-a")
    task = asyncio.create_task(worker.run())

    await asyncio.wait_for(running.wait(), timeout=10)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=15)

    final = await pool.fetchrow("SELECT * FROM workflows WHERE id = $1", workflow_id)
    assert final["status"] == "RUNNING", "a cancelled workflow was recorded as failed"
    assert final["error"] is None
    assert final["owner_id"] == "worker-a"

    # And it becomes claimable again the moment the lease lapses -- no reaper involved.
    await pool.execute(
        "UPDATE workflows SET lease_expires_at = now() - interval '1 second' WHERE id = $1",
        workflow_id,
    )
    async with pool.acquire() as conn:
        assert [w.id for w in await claim_workflows(conn, "worker-b", 30, 10)] == [workflow_id]


# ---------------------------------------------------------------------------
# 5. The background lease renewer.
# ---------------------------------------------------------------------------


async def test_the_renewer_keeps_a_step_that_outlives_its_lease(pool, insert_workflow):
    """A step longer than the lease must not be stolen out from under the worker running it.

    The lease is one second and the step takes about two, so without the renewer the row
    would become claimable mid-step and the eventual checkpoint would be rejected by the
    ownership guard. Three things are asserted: the stamp keeps moving, no other worker can
    claim the row while the step runs, and the workflow still reaches SUCCESS.
    """
    running = asyncio.Event()

    @workflow(WORKFLOW_TYPE)
    class SlowStep:
        @step(seq=1)
        async def debit_wallet(self, ctx: StepContext) -> dict[str, bool]:
            running.set()
            await asyncio.sleep(2.0)
            return {"ok": True}

    workflow_id = await insert_workflow()
    settings = make_settings(lease_duration_seconds=1, lease_renew_divisor=3)
    worker = Worker(pool, settings=settings, owner_id="worker-a")
    task = asyncio.create_task(worker.run())
    try:
        await asyncio.wait_for(running.wait(), timeout=10)

        async def lease_stamp():
            return await pool.fetchval(
                "SELECT lease_expires_at FROM workflows WHERE id = $1", workflow_id
            )

        first = await lease_stamp()
        # Longer than the whole lease: without renewal the row is claimable by now.
        await asyncio.sleep(1.2)
        second = await lease_stamp()

        assert second > first, "the background renewer never extended the lease"
        async with pool.acquire() as conn:
            poached = await claim_workflows(conn, "worker-b", 30, 10)
        assert poached == [], "another worker stole a workflow whose lease was being renewed"

        assert await wait_for(lambda: _all_success(pool), give_up_after=10), (
            f"workflow did not finish: {await statuses(pool)}"
        )
    finally:
        await stop(worker, task)

    final = await pool.fetchrow("SELECT * FROM workflows WHERE id = $1", workflow_id)
    assert final["status"] == "SUCCESS"
    assert final["attempt"] == 1, "the workflow was re-claimed, so a renewal was missed"
