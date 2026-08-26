"""The outbox drain: move committed events from Postgres to Redis, at least once.

``StepContext.emit`` and the checkpoint transaction (``storage/workflows.py``) are the
producer half of the outbox: an event becomes durable in the same transaction as the state
change that caused it. This module is the consumer half, and it is a genuinely different
problem -- there is no state change to be atomic *with* here, only a broker that might be down
and a network that might drop the response after the write already landed.

The drain therefore gives up exactly-once *delivery* and keeps exactly-once *effects*
(CLAUDE.md): each batch is claimed with ``FOR UPDATE SKIP LOCKED`` inside one transaction,
published, and only then marked ``published_at`` in that same transaction. A crash between the
publish and the mark republishes the batch on the next pass -- the row is still
``published_at IS NULL`` -- and the *only* thing that makes that survivable is that consumers
dedupe on ``event_id`` (``storage/outbox.py``: ``OutboxEvent.id``, unchanged across
republishes, as opposed to the Redis stream entry ID, which is fresh every time).

**Why the Postgres transaction stays open across the publish.** ``FOR UPDATE SKIP LOCKED``
locks live exactly as long as their transaction does. Commit before publishing and a second
drainer claims the same rows in the gap and double-publishes on the very first pass, with no
crash required. Holding it open is what makes N concurrent drainers safe, and it is also the
cost: a Redis call that never returns pins this transaction -- and its xmin horizon -- open
indefinitely, which is why the client (``storage/redis.py``) carries a socket timeout and
``outbox_batch_size`` bounds how much work is behind any one lock.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
from collections.abc import Sequence

import asyncpg

from sankalp.config import Settings, get_settings
from sankalp.engine.publisher import Publisher, RedisStreamPublisher
from sankalp.resilience.backoff import compute_backoff
from sankalp.storage.outbox import (
    OutboxEvent,
    claim_unpublished,
    mark_published,
    record_publish_failure,
)

__all__ = ["DrainLoop", "run_drain", "main"]

log = logging.getLogger("sankalp.drain")


class DrainLoop:
    """Poll the outbox and publish, until :meth:`stop`.

    Deliberately shaped like ``engine.worker.Worker``'s poll loop -- the interruptible sleep,
    the same two asyncpg exception hierarchies caught around the claim, the same "log the
    readiness marker on start" pattern the test harness waits on -- because it is solving the
    identical problem (poll a Postgres table, back off on trouble, stop promptly) for a
    different table.
    """

    def __init__(
        self,
        pool: asyncpg.Pool,
        publisher: Publisher,
        *,
        settings: Settings | None = None,
    ) -> None:
        self._pool = pool
        self._publisher = publisher
        self._settings = settings or get_settings()
        self._stopping = asyncio.Event()
        #: Consecutive failed batches, for backoff -- reset to 0 by any successful pass
        #: (including an empty one), so a blip does not leave the loop needlessly slow once
        #: the trouble has cleared.
        self._consecutive_failures = 0

    def stop(self) -> None:
        """Ask the loop to stop after its current batch. Safe to call more than once."""
        if not self._stopping.is_set():
            self._stopping.set()

    async def run(self) -> None:
        """Poll until :meth:`stop`, publishing whatever batches ``drain_once`` finds."""
        log.info(
            "draining: batch=%d stream=%r interval=%.2fs",
            self._settings.outbox_batch_size,
            self._settings.outbox_stream,
            self._settings.outbox_poll_interval_seconds,
        )
        while not self._stopping.is_set():
            try:
                published = await self.drain_once()
            except (asyncpg.PostgresError, asyncpg.InterfaceError, OSError):
                # Named explicitly rather than caught as asyncpg.Error, which does not exist:
                # PostgresError and InterfaceError are sibling hierarchies under Exception (see
                # worker.py::_poll_forever). Uncaught, either kills this loop silently while the
                # rest of the process looks healthy.
                self._consecutive_failures += 1
                delay = compute_backoff(
                    self._consecutive_failures, cap_seconds=self._settings.backoff_cap_seconds
                )
                log.exception(
                    "drain batch failed (%d consecutive); retrying in %.1fs",
                    self._consecutive_failures,
                    delay,
                )
                await self._wait(delay)
                continue

            self._consecutive_failures = 0
            if not published:
                await self._wait(self._settings.outbox_poll_interval_seconds)
        log.info("drain stopped")

    async def drain_once(self) -> int:
        """Claim, publish, and mark one batch. Returns the number of rows published.

        The whole mechanism lives in this one transaction: claim (which takes the row locks),
        publish, mark. A publish (or mark) that raises is allowed to propagate out of the
        ``async with`` below, which rolls the transaction back -- the rows return to
        ``published_at IS NULL`` exactly as if they had never been claimed.

        :func:`~sankalp.storage.outbox.record_publish_failure` runs in the ``except`` clause
        **outside** that ``async with``, deliberately -- not merely because it needs its own
        transaction (the one that just rolled back cannot carry a durable write out of it), but
        because of *when* it runs. Called from inside the ``async with``, it would try to
        ``UPDATE`` the very rows the still-open claim transaction holds ``FOR UPDATE`` locks on,
        over a second connection -- a self-deadlock, since that lock is not released until the
        rollback completes, and the rollback does not complete until this coroutine returns.
        Only once the exception has unwound past the ``async with`` -- rolling the claim back
        and freeing those locks -- is it safe to write through a different connection.
        """
        event_ids: list[int] = []
        try:
            async with self._pool.acquire() as conn, conn.transaction():
                events = await claim_unpublished(conn, self._settings.outbox_batch_size)
                if not events:
                    return 0
                event_ids = [event.id for event in events]
                await self._publisher.publish(events)
                await mark_published(conn, event_ids)
                return len(events)
        except Exception:
            if event_ids:
                log.exception(
                    "publishing %d outbox event(s) failed; they remain unpublished and will "
                    "be retried",
                    len(event_ids),
                )
                await record_publish_failure(self._pool, event_ids)
            raise

    async def _wait(self, seconds: float) -> None:
        """Sleep, but wake immediately once :meth:`stop` is called."""
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._stopping.wait(), seconds)


class _GatedPublisher:
    """Wraps a real publisher so a crash test can SIGKILL between the XADD and the mark.

    Composition-root only: built by :func:`run_drain`, never by :class:`DrainLoop` or
    ``drain_once`` itself, so the mechanism a production process actually runs -- ``DrainLoop``
    -- carries no branch for this at all. The gate lives entirely in the layer that decides
    *which* publisher to construct.

    Fails safe on the same two-fact contract as ``workflows/_instrumentation.py``:
    ``settings.crash_gate_armed`` requires both ``environment == "test"`` and
    ``crash_gate_enabled``, never either alone, so a leaked env var cannot park a production
    drain on a ``crash_gates`` row no test will ever insert.

    Gates on the first event's ``workflow_id`` under the step name ``"outbox.drain"`` -- the
    crash test that uses this publishes exactly one event per batch, so that identifies the
    batch uniquely. A batch spanning several workflows would only gate on the first; nothing
    here needs it to do more.
    """

    def __init__(self, inner: Publisher, *, settings: Settings) -> None:
        self._inner = inner
        self._settings = settings

    async def publish(self, events: Sequence[OutboxEvent]) -> None:
        await self._inner.publish(events)
        if not events or not self._settings.crash_gate_armed:
            return
        from sankalp.workflows._instrumentation import await_gate, get_pool, record_attempt

        workflow_id = events[0].workflow_id
        pool = await get_pool()
        async with pool.acquire() as conn:
            await record_attempt(conn, workflow_id, "outbox.drain", "outbox-drain")
            await await_gate(conn, workflow_id, "outbox.drain")


async def run_drain(settings: Settings | None = None) -> None:
    """Open a pool and a Redis client, run one drain loop until stopped, close both.

    Installs SIGTERM/SIGINT handlers around the loop, same rationale as
    ``worker.py::Worker._install_signal_handlers``: ``loop.add_signal_handler`` runs the
    callback on the event loop rather than interrupting whatever coroutine is mid-``await``,
    so a signal cannot land inside a claim transaction. There is no in-flight work to drain
    here the way the worker has -- the loop simply stops claiming new batches once its current
    one finishes.

    This is also the composition root that decides whether the drain runs behind the crash
    gate (:class:`_GatedPublisher`) -- see its docstring for why that decision belongs here
    and not inside :class:`DrainLoop`.
    """
    from sankalp.storage.pool import create_pool
    from sankalp.storage.redis import create_redis

    settings = settings or get_settings()
    pool = await create_pool(settings=settings)
    redis = create_redis(settings=settings)
    try:
        publisher: Publisher = RedisStreamPublisher(
            redis,
            stream=settings.outbox_stream,
            maxlen=settings.outbox_stream_maxlen,
        )
        if settings.crash_gate_armed:
            publisher = _GatedPublisher(publisher, settings=settings)
        loop_ = DrainLoop(pool, publisher, settings=settings)
        installed: list[signal.Signals] = []
        running_loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                running_loop.add_signal_handler(sig, loop_.stop)
            except (NotImplementedError, RuntimeError, ValueError):
                continue
            installed.append(sig)
        try:
            await loop_.run()
        finally:
            for sig in installed:
                with contextlib.suppress(NotImplementedError, RuntimeError, ValueError):
                    running_loop.remove_signal_handler(sig)
    finally:
        await redis.aclose()
        await pool.close()


def main() -> int:
    """Console entry point: ``sankalp-drain``."""
    settings = get_settings()
    logging.basicConfig(
        level=settings.log_level.upper(),
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )
    asyncio.run(run_drain(settings))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
