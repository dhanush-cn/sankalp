"""The drain, proved against real Postgres and real Redis. See docs/spec.md, "Drain loop".

  * every unpublished row is published, exactly once, on a clean run;
  * a second drain over the same rows publishes nothing;
  * two drains racing over the same backlog do not double-publish -- SKIP LOCKED holds;
  * a failure before the publish leaves the rows unpublished, with ``attempts`` bumped;
  * a crash between the publish and the mark republishes the row under the SAME ``event_id``
    -- the at-least-once boundary the spec draws, proved in-process with a publisher that
    performs the real XADD and then raises. The real-process version of this, SIGKILLing a
    real ``sankalp-drain`` between the XADD and the mark, is ``tests/test_drain_crash.py``.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Sequence

import pytest

from sankalp.config import Settings
from sankalp.engine.drain import DrainLoop
from sankalp.engine.publisher import RedisStreamPublisher
from sankalp.storage.outbox import OutboxEvent


async def insert_events(
    pool, workflow_id: uuid.UUID, n: int, *, event_type: str = "step.completed"
):
    for i in range(n):
        await pool.execute(
            "INSERT INTO outbox (workflow_id, event_type, payload) VALUES ($1, $2, $3::jsonb)",
            workflow_id,
            event_type,
            json.dumps({"i": i}),
        )


async def unpublished_count(pool) -> int:
    return await pool.fetchval("SELECT count(*) FROM outbox WHERE published_at IS NULL")


class RaisingPublisher:
    """Fails every batch, before doing anything -- proves the failure path leaves rows alone."""

    async def publish(self, events: Sequence[OutboxEvent]) -> None:
        raise ConnectionError("redis is unreachable")


class ExplodingPublisher:
    """Performs the REAL XADD via ``inner``, then raises -- the in-process crash proof.

    This is the exact shape of "a crash between the XADD and the mark": by the time this
    raises, the events are genuinely sitting in the Redis stream, and the drain's transaction
    is about to roll back without stamping ``published_at``. A second, healthy drain then
    republishes them -- with the same ``event_id`` each time, which is what a consumer's
    dedupe keys off.
    """

    def __init__(self, inner: RedisStreamPublisher) -> None:
        self._inner = inner

    async def publish(self, events: Sequence[OutboxEvent]) -> None:
        await self._inner.publish(events)
        raise ConnectionResetError("connection dropped after the XADD landed")


def make_settings(**overrides) -> Settings:
    base = dict(outbox_batch_size=100, backoff_cap_seconds=1)
    return Settings(**(base | overrides))


# ---------------------------------------------------------------------------
# 1-3. The clean drain.
# ---------------------------------------------------------------------------


async def test_the_drain_publishes_every_unpublished_event_once(
    pool, insert_workflow, redis_client, event_stream
):
    workflow_id = await insert_workflow()
    await insert_events(pool, workflow_id, 5)

    publisher = RedisStreamPublisher(redis_client, stream=event_stream, maxlen=1000)
    loop = DrainLoop(pool, publisher, settings=make_settings())

    published = await loop.drain_once()

    assert published == 5
    assert await unpublished_count(pool) == 0

    entries = await redis_client.xrange(event_stream)
    assert len(entries) == 5
    event_ids = [fields["event_id"] for _, fields in entries]
    assert len(set(event_ids)) == 5, "each entry must carry a distinct event_id"


async def test_a_second_drain_publishes_nothing(pool, insert_workflow, redis_client, event_stream):
    workflow_id = await insert_workflow()
    await insert_events(pool, workflow_id, 3)
    publisher = RedisStreamPublisher(redis_client, stream=event_stream, maxlen=1000)
    loop = DrainLoop(pool, publisher, settings=make_settings())

    await loop.drain_once()
    before = await redis_client.xlen(event_stream)

    again = await loop.drain_once()

    assert again == 0
    assert await redis_client.xlen(event_stream) == before


async def test_two_concurrent_drainers_do_not_double_publish(
    pool, insert_workflow, redis_client, event_stream
):
    """SKIP LOCKED proved: two claims racing over the same backlog partition it, not repeat it.

    Both transactions open before either claims -- otherwise the second would see the first's
    rows as already published rather than as locked -- so the claim itself is driven directly
    rather than through ``drain_once``, which claims and publishes and marks as one call.
    """
    workflow_id = await insert_workflow()
    await insert_events(pool, workflow_id, 10)
    publisher = RedisStreamPublisher(redis_client, stream=event_stream, maxlen=1000)

    from sankalp.storage.outbox import claim_unpublished, mark_published

    conn_a = await pool.acquire()
    conn_b = await pool.acquire()
    try:
        tx_a = conn_a.transaction()
        tx_b = conn_b.transaction()
        await tx_a.start()
        await tx_b.start()

        claimed_a = await claim_unpublished(conn_a, 6)
        claimed_b = await claim_unpublished(conn_b, 6)

        ids_a = {e.id for e in claimed_a}
        ids_b = {e.id for e in claimed_b}
        assert ids_a.isdisjoint(ids_b), "the two claims overlapped -- SKIP LOCKED did not hold"
        assert ids_a | ids_b == set(range(1, 11))

        await publisher.publish(claimed_a)
        await mark_published(conn_a, [e.id for e in claimed_a])
        await publisher.publish(claimed_b)
        await mark_published(conn_b, [e.id for e in claimed_b])

        await tx_a.commit()
        await tx_b.commit()
    finally:
        await pool.release(conn_a)
        await pool.release(conn_b)

    assert await unpublished_count(pool) == 0
    entries = await redis_client.xrange(event_stream)
    assert len(entries) == 10
    event_ids = [fields["event_id"] for _, fields in entries]
    assert len(set(event_ids)) == 10, "every row was published exactly once, by exactly one drainer"


# ---------------------------------------------------------------------------
# 4. Failure before the publish.
# ---------------------------------------------------------------------------


async def test_a_failure_before_the_xadd_leaves_the_rows_unpublished(pool, insert_workflow):
    workflow_id = await insert_workflow()
    await insert_events(pool, workflow_id, 2)
    loop = DrainLoop(pool, RaisingPublisher(), settings=make_settings())

    with pytest.raises(ConnectionError):
        await loop.drain_once()

    assert await unpublished_count(pool) == 2
    attempts = await pool.fetch(
        "SELECT attempts FROM outbox WHERE workflow_id = $1 ORDER BY id", workflow_id
    )
    assert [r["attempts"] for r in attempts] == [1, 1], (
        "a failed publish attempt must still be counted, in its own transaction -- the "
        "claim's transaction rolled back and cannot carry the increment out with it"
    )


# ---------------------------------------------------------------------------
# 5. The at-least-once boundary, in process.
# ---------------------------------------------------------------------------


async def test_a_crash_between_the_xadd_and_the_mark_republishes(
    pool, insert_workflow, redis_client, event_stream
):
    workflow_id = await insert_workflow()
    await insert_events(pool, workflow_id, 1)
    real_publisher = RedisStreamPublisher(redis_client, stream=event_stream, maxlen=1000)
    exploding = ExplodingPublisher(real_publisher)
    crashing_loop = DrainLoop(pool, exploding, settings=make_settings())

    with pytest.raises(ConnectionResetError):
        await crashing_loop.drain_once()

    # The row is still unpublished -- the mark never ran, and the claim's transaction rolled
    # back -- but the event genuinely reached Redis, because the XADD happened for real before
    # the raise. This is the crash window the spec describes, reproduced without a process kill.
    assert await unpublished_count(pool) == 1
    assert await redis_client.xlen(event_stream) == 1

    clean_loop = DrainLoop(pool, real_publisher, settings=make_settings())
    republished = await clean_loop.drain_once()

    assert republished == 1
    assert await unpublished_count(pool) == 0

    entries = await redis_client.xrange(event_stream)
    assert len(entries) == 2, "the crashed attempt's XADD plus the republish"
    event_ids = {fields["event_id"] for _, fields in entries}
    seen = [f["event_id"] for _, f in entries]
    assert len(event_ids) == 1, (
        f"both stream entries must carry the SAME event_id -- {seen} -- which is the only "
        "thing that lets a consumer collapse them back to one effect"
    )


# ---------------------------------------------------------------------------
# 6. Batch size.
# ---------------------------------------------------------------------------


async def test_the_drain_respects_the_batch_size(pool, insert_workflow, redis_client, event_stream):
    workflow_id = await insert_workflow()
    await insert_events(pool, workflow_id, 7)
    publisher = RedisStreamPublisher(redis_client, stream=event_stream, maxlen=1000)
    loop = DrainLoop(pool, publisher, settings=make_settings(outbox_batch_size=3))

    first = await loop.drain_once()
    assert first == 3
    assert await unpublished_count(pool) == 4

    second = await loop.drain_once()
    assert second == 3
    assert await unpublished_count(pool) == 1

    third = await loop.drain_once()
    assert third == 1
    assert await unpublished_count(pool) == 0
