"""Publishing outbox rows to Redis: the consumer-facing half of the drain.

:class:`Publisher` is a ``Protocol`` rather than a base class so a test can hand the drain
something that fails at a chosen instant -- mid-batch, after a real XADD, whatever the test
needs -- without touching Redis at all. :class:`RedisStreamPublisher` is the only
implementation the engine ships.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from redis.asyncio import Redis

from sankalp.storage.outbox import OutboxEvent

__all__ = ["Publisher", "RedisStreamPublisher"]


class Publisher(Protocol):
    """Something that can move a batch of claimed outbox rows to a broker."""

    async def publish(self, events: Sequence[OutboxEvent]) -> None:
        """Publish every event in ``events``. Raise on any failure, and raise before
        returning -- the drain calls this while still holding the rows' locks, and it decides
        whether to mark them published purely from whether this returned or raised."""
        ...


class RedisStreamPublisher:
    """XADDs each event to one Redis Stream, with ``event_id`` as the dedupe key.

    Each entry's stream ID (``*``, server-assigned) is deliberately **not** how consumers tell
    events apart -- a republished event gets a fresh stream ID every time. ``event_id`` is
    ``str(outbox.id)``, the same number named in the drain's SQL and in docs/spec.md's "dedupe
    on ``outbox.id``" -- so the field a consumer actually reads and the row the database
    reasons about are the same identifier under two names, not two identifiers that might
    drift apart.

    Entries go out in one pipeline, not one transaction: a partial failure midway through a
    batch leaves some entries published and the rest not, which is at-least-once behaving
    exactly as designed -- the drain only marks a row ``published_at`` after this coroutine
    returns without raising, so an entry that never made it out is retried on the next pass.
    """

    def __init__(self, redis: Redis, *, stream: str, maxlen: int) -> None:
        self._redis = redis
        self._stream = stream
        self._maxlen = maxlen

    async def publish(self, events: Sequence[OutboxEvent]) -> None:
        if not events:
            return
        pipe = self._redis.pipeline(transaction=False)
        for event in events:
            fields = {
                "event_id": str(event.id),
                "event_type": event.event_type,
                "workflow_id": str(event.workflow_id),
                "payload": event.payload_json,
                "created_at": event.created_at.isoformat(),
            }
            if event.trace_context_json is not None:
                fields["trace_context"] = event.trace_context_json
            pipe.xadd(self._stream, fields, maxlen=self._maxlen, approximate=True)
        await pipe.execute()
