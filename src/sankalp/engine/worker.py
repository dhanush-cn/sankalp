"""The worker: claim work, run it under a bounded concurrency, renew its leases, shut down.

The loop is deliberately small, because almost everything that would otherwise live in a
worker is a property of the schema instead. There is no recovery daemon (an expired lease
makes a row claimable again), no in-memory queue (the ``workflows`` row *is* the queue
entry), and no progress tracking (``step_outputs`` is the progress). What is left:

**Claim only what there is room to run.** Slots are taken from the semaphore *before* the
dequeue query, and the batch is sized to the number taken. Claiming ahead of capacity would
be strictly harmful: a claimed workflow carries a lease that this worker would not be
renewing while it sat in a local queue, so it would be stolen by another worker halfway
through waiting -- and the queue is already durable and shared, which is the whole reason
not to keep a private copy of it.

**Renew while the work runs.** One background task per in-flight workflow ticks at
``lease_duration / lease_renew_divisor``. This is the timer defense from docs/spec.md; the
executor's check before each step is the second one, and both drive the same
:class:`~sankalp.engine.lease.Lease`.

**Shut down without stranding anything.** SIGTERM stops claiming and lets in-flight
workflows finish, which is the difference between a rolling deploy that completes its work
and one that leaves a lease-duration hole in throughput. A second signal, the grace period
running out, or ``run()`` being cancelled outright cancels what is left -- that last one
skips the waiting entirely, because a cancellation means stop *now* and honouring it with a
grace period would make a supervisor wait out a delay it never asked for. Cancelled work is
not lost either: it is recovered by the same expired-lease path as a crash. That is the
point of having only one recovery mechanism -- the graceful path and the ``docker kill``
path end up in the same place.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal

import asyncpg

from sankalp.config import Settings, get_settings
from sankalp.engine.executor import execute_workflow
from sankalp.engine.lease import Lease
from sankalp.storage.queue import ClaimedWorkflow, claim_workflows

__all__ = ["Worker", "run_worker", "main"]

log = logging.getLogger("sankalp.worker")


class Worker:
    """One polling worker. Owns its concurrency, its leases, and its shutdown.

    ``owner_id`` is written to ``workflows.owner_id`` and must be unique per process: it is
    half of the ownership guard on every write, so two processes sharing one would each
    accept the other's writes as their own and the fencing token would be doing the work
    alone.
    """

    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        settings: Settings | None = None,
        owner_id: str | None = None,
    ) -> None:
        self._pool = pool
        self._settings = settings or get_settings()
        self._owner_id = owner_id or self._settings.worker_id
        self._slots = asyncio.Semaphore(self._settings.worker_concurrency)
        self._in_flight: set[asyncio.Task[None]] = set()
        self._stopping = asyncio.Event()
        self._handled_signals: list[signal.Signals] = []
        #: Set when run() is unwinding because it was cancelled, rather than because it was
        #: asked to stop. The drain reads it to decide whether a grace period is owed.
        self._cancelled = False

    # -- lifecycle ----------------------------------------------------------------------

    @property
    def owner_id(self) -> str:
        return self._owner_id

    @property
    def in_flight(self) -> int:
        """Workflows currently executing. Never exceeds ``worker_concurrency``."""
        return len(self._in_flight)

    def request_shutdown(self) -> None:
        """Stop claiming new work. In-flight workflows run to completion.

        Safe to call from a signal handler and safe to call twice; :meth:`run` bounds the
        wait with ``worker_shutdown_grace_seconds`` regardless.
        """
        if not self._stopping.is_set():
            self._stopping.set()

    async def run(self) -> None:
        """Poll until shutdown is requested, then drain."""
        self._install_signal_handlers()
        log.info(
            "worker %s polling: concurrency=%d batch=%d lease=%ds",
            self._owner_id,
            self._settings.worker_concurrency,
            self._settings.dequeue_batch_size,
            self._settings.lease_duration_seconds,
        )
        try:
            await self._poll_forever()
        except asyncio.CancelledError:
            # Cancelled from outside rather than asked to stop. That is an order to stop
            # *now*, so the drain below forgoes the grace period -- see _drain.
            self._cancelled = True
            raise
        finally:
            self._remove_signal_handlers()
            await self._drain()
            log.info("worker %s stopped", self._owner_id)

    # -- the loop -----------------------------------------------------------------------

    async def _poll_forever(self) -> None:
        while not self._stopping.is_set():
            slots = await self._acquire_slots(self._settings.dequeue_batch_size)
            if self._stopping.is_set():
                self._release_slots(slots)
                return

            try:
                claimed = await self._claim(slots)
            except (asyncpg.PostgresError, OSError):
                # The database is unreachable or unhappy. Nothing was claimed, so nothing is
                # stranded; back off for a poll interval and try again rather than spinning.
                log.exception("claim failed; retrying after the poll interval")
                self._release_slots(slots)
                await self._wait(self._settings.poll_interval_seconds)
                continue

            # Hand one slot to each workflow and give the rest straight back, so an
            # under-full batch does not shrink this worker's capacity until it restarts.
            self._release_slots(slots - len(claimed))
            if not claimed:
                await self._wait(self._settings.poll_interval_seconds)
                continue

            log.debug("worker %s claimed %d workflow(s)", self._owner_id, len(claimed))
            for workflow in claimed:
                self._spawn(workflow)

    async def _claim(self, batch_size: int) -> list[ClaimedWorkflow]:
        async with self._pool.acquire() as conn:
            return await claim_workflows(
                conn,
                self._owner_id,
                self._settings.lease_duration_seconds,
                batch_size,
            )

    def _spawn(self, claimed: ClaimedWorkflow) -> None:
        task = asyncio.create_task(self._run_one(claimed), name=f"workflow:{claimed.id}")
        self._in_flight.add(task)
        # discard, not remove: _drain may cancel and collect a task before its callback runs.
        task.add_done_callback(self._in_flight.discard)

    async def _run_one(self, claimed: ClaimedWorkflow) -> None:
        """Execute one workflow with its lease kept alive underneath it."""
        lease = Lease(
            self._pool,
            claimed,
            duration_seconds=self._settings.lease_duration_seconds,
            renew_divisor=self._settings.lease_renew_divisor,
        )
        renewer = asyncio.create_task(self._renew_until_done(lease), name=f"lease:{claimed.id}")
        try:
            result = await execute_workflow(
                self._pool, claimed, lease=lease, settings=self._settings
            )
            log.debug(
                "workflow %s (%s) -> %s", claimed.id, claimed.workflow_type, result.value
            )
        except asyncio.CancelledError:
            # Shutdown ran out of grace. The row keeps its lease and its checkpoints; when
            # the lease expires another worker resumes it from the last completed step.
            log.warning(
                "workflow %s cancelled mid-execution; another worker will resume it from its "
                "last checkpoint once the lease expires",
                claimed.id,
            )
            raise
        except Exception:
            # Could not execute it at all -- an unregistered workflow_type, a database that
            # went away mid-transition. Leave the row untouched for the same reason: its
            # lease expiring is the recovery path, and writing a state we cannot justify
            # would be worse than writing nothing.
            log.exception(
                "workflow %s could not be executed; leaving it to its lease", claimed.id
            )
        finally:
            renewer.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await renewer
            self._slots.release()

    async def _renew_until_done(self, lease: Lease) -> None:
        """Extend one workflow's lease on a timer until cancelled or preempted.

        Renews unconditionally on each tick rather than checking how much is left: it wakes
        with two thirds of the lease still unused, so a conditional renewal would decline
        every time and this defense would never actually fire.
        """
        while True:
            await asyncio.sleep(lease.renew_interval_seconds)
            try:
                if not await lease.renew():
                    # Another worker holds the row. Stop renewing and let the execution find
                    # out for itself at its next guarded write -- cancelling it from here
                    # would interrupt a step that is already mid-side-effect.
                    log.warning(
                        "lost the lease on workflow %s; it has been re-claimed past fencing "
                        "token %d",
                        lease.workflow_id,
                        lease.fencing_token,
                    )
                    return
            except (asyncpg.PostgresError, OSError):
                # A blip must not kill the renewer -- that would guarantee the loss it is
                # trying to prevent. Try again next tick; there is still lease left.
                log.warning(
                    "lease renewal for workflow %s failed; retrying in %.1fs",
                    lease.workflow_id,
                    lease.renew_interval_seconds,
                    exc_info=True,
                )

    # -- concurrency --------------------------------------------------------------------

    async def _acquire_slots(self, maximum: int) -> int:
        """Block for one slot, then take whatever else is free right now. Returns the count.

        The blocking first acquire is the throttle: at full concurrency this loop simply
        stops asking the database for work until a workflow finishes. It also means shutdown
        requested while the worker is saturated is noticed when the next slot frees rather
        than instantly -- which costs nothing, since shutdown is waiting for exactly those
        in-flight workflows anyway.

        The opportunistic part is safe without a lock: ``Semaphore.acquire`` returns without
        yielding when the semaphore is not locked, so nothing else can run between the check
        and the acquire that follows it, and neither call can block.
        """
        await self._slots.acquire()
        taken = 1
        while taken < maximum and not self._slots.locked():
            await self._slots.acquire()
            taken += 1
        return taken

    def _release_slots(self, count: int) -> None:
        for _ in range(count):
            self._slots.release()

    async def _wait(self, seconds: float) -> None:
        """Sleep, but wake immediately if shutdown is requested."""
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._stopping.wait(), seconds)

    # -- shutdown -----------------------------------------------------------------------

    async def _drain(self) -> None:
        """Let in-flight workflows finish, then cancel and collect whatever is left.

        The grace period is owed to a *requested* shutdown -- SIGTERM, ``request_shutdown()``
        -- where the caller is asking the worker to stop when it can. A cancellation is not
        that: it says stop now, and sitting out the grace first would turn a supervisor's
        ``task.cancel()`` into a wait of up to ``worker_shutdown_grace_seconds`` while this
        worker politely finished work nobody asked it to finish.

        Either way every task is cancelled and then **awaited**, never abandoned. That await
        is what the executor's shielded checkpoint write depends on: it keeps a step that has
        already run from losing its ``step_outputs`` row, and it holds off the pool close in
        ``run_worker`` until the write has actually landed.
        """
        if not self._in_flight:
            return
        grace = 0.0 if self._cancelled else self._settings.worker_shutdown_grace_seconds
        pending = set(self._in_flight)
        if self._cancelled:
            log.info("cancelled: stopping %d in-flight workflow(s) now", len(pending))
        else:
            log.info("draining %d in-flight workflow(s), up to %.0fs", len(pending), grace)

        _, still_running = await asyncio.wait(pending, timeout=grace)
        if not still_running:
            log.info("all in-flight workflows finished")
            return

        log.warning(
            "cancelling %d workflow(s) still running after %.1fs. Their leases expire in at "
            "most %ds and another worker resumes them from their last checkpoint",
            len(still_running),
            grace,
            self._settings.lease_duration_seconds,
        )
        for task in still_running:
            task.cancel()
        await asyncio.gather(*still_running, return_exceptions=True)

    def _install_signal_handlers(self) -> None:
        """Route SIGTERM/SIGINT into the loop. Ignored where the platform has no support.

        ``loop.add_signal_handler`` rather than ``signal.signal``: the callback runs on the
        event loop instead of interrupting whatever coroutine happened to be executing, so
        setting the shutdown event cannot land in the middle of a step's ``await``.
        """
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, self._on_signal, sig)
            except (NotImplementedError, RuntimeError, ValueError):
                log.debug("cannot install a handler for %s on this platform", sig.name)
                continue
            self._handled_signals.append(sig)

    def _remove_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        while self._handled_signals:
            with contextlib.suppress(NotImplementedError, RuntimeError, ValueError):
                loop.remove_signal_handler(self._handled_signals.pop())

    def _on_signal(self, sig: signal.Signals) -> None:
        if self._stopping.is_set():
            # Someone is asking again because the first request is taking too long. Escalate
            # rather than ignore it: an operator's second SIGTERM should not need a SIGKILL,
            # and cancelled workflows are recovered by their leases like any other crash.
            log.warning(
                "%s again -- cancelling %d in-flight workflow(s) now",
                sig.name,
                len(self._in_flight),
            )
            for task in list(self._in_flight):
                task.cancel()
            return
        log.info(
            "%s received; finishing %d in-flight workflow(s) and claiming no more",
            sig.name,
            len(self._in_flight),
        )
        self.request_shutdown()


async def run_worker(settings: Settings | None = None) -> None:
    """Open a pool, run one worker until it is asked to stop, close the pool."""
    from sankalp.storage.pool import create_pool

    settings = settings or get_settings()
    pool = await create_pool(settings=settings)
    try:
        await Worker(pool, settings=settings).run()
    finally:
        await pool.close()


def main() -> int:
    """Console entry point: ``sankalp-worker``."""
    settings = get_settings()
    logging.basicConfig(
        level=settings.log_level.upper(),
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )
    # Importing the package is what registers the definitions -- @workflow runs at import
    # time, and get_definition can only resolve a workflow_type this process has imported.
    import sankalp.workflows  # noqa: F401

    asyncio.run(run_worker(settings))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
