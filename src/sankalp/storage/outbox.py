"""The outbox: events that become true in the same transaction as the state change.

Two halves, and the whole design is the seam between them.

**The write** (:func:`insert_events`) takes a *connection*, never a pool. That is the entire
point of this module: the INSERT has to run inside a transaction somebody else opened -- the
one in :func:`sankalp.storage.workflows.commit_step_output` that also writes the step's
checkpoint. Either the step is durably done and its event exists, or neither is true. Every
other function here takes a pool or a connection according to whose transaction it belongs in,
and that distinction is load-bearing rather than stylistic.

The alternative everyone writes first -- commit the step, then publish -- is a dual write, and
it fails in both directions: crash after the commit and the event is lost forever; publish
first and crash before the commit and you have announced something that never happened. No
ordering of the two saves it, because the failure is that they are two.

**The drain** (:func:`claim_unpublished` / :func:`mark_published`) moves committed rows to
Redis afterwards. It is **at-least-once**: a crash between the publish and the mark republishes
the row, and consumers dedupe on the event id. What the system provides is exactly-once
*effects*, never exactly-once delivery (CLAUDE.md).

JSON is bound as *text* with an explicit ``::jsonb`` cast, and read back as text, for the same
reason as everywhere else in this package (see ``storage/pool.py``): these functions take
pools and connections they do not own -- including bare ``asyncpg.connect`` handles in tests --
so they cannot assume ``set_type_codec`` has been called. With a codec registered, passing a
str would double-encode; without one, passing a dict fails outright. Text plus a cast is the
shape that is correct either way.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

import asyncpg

# Nothing is imported from ``storage.workflows`` here, deliberately: that module imports *this*
# one so the outbox INSERT can join its checkpoint transaction, and the dependency has to run in
# exactly that direction. Hence the two write helpers below return None rather than borrowing
# its ``rows_affected`` -- the drain already knows how many ids it handed over, and it holds the
# row locks meanwhile, so there is no count here anyone could learn anything from.

__all__ = [
    "PendingEvent",
    "OutboxEvent",
    "insert_events",
    "claim_unpublished",
    "mark_published",
    "record_publish_failure",
]


@dataclass(frozen=True, slots=True)
class PendingEvent:
    """One event a step has emitted, encoded and waiting for the step's checkpoint.

    The payload arrives here already serialised. :meth:`sankalp.engine.definition.StepContext.emit`
    does that at the call site on purpose: a value that cannot be encoded must fail with a
    traceback pointing at the step that emitted it, not later inside a shielded write with no
    idea which of the buffered events was the bad one. Same rule as ``executor._encode_output``.
    """

    event_type: str
    payload_json: str
    trace_context_json: str | None = None


@dataclass(frozen=True, slots=True)
class OutboxEvent:
    """One committed row, claimed by the drain and on its way to Redis.

    ``id`` is the row's ``BIGSERIAL`` primary key and it is *the dedupe key* -- it travels to
    consumers as the ``event_id`` field. It is deliberately not the Redis stream entry id: a
    republished event gets a fresh stream id, which is precisely why this one has to ride along
    in the payload.
    """

    id: int
    workflow_id: UUID
    event_type: str
    payload_json: str
    trace_context_json: str | None
    created_at: datetime


_INSERT_EVENT_SQL = """
INSERT INTO outbox (workflow_id, event_type, payload, trace_context)
VALUES ($1, $2, $3::jsonb, $4::jsonb)
"""

#: The claim. ``WHERE published_at IS NULL`` matches ``idx_outbox_unpublished``'s predicate
#: exactly and ``ORDER BY id`` matches its leading column, so the planner can prove the partial
#: index applies and this is an ordered walk that stops at LIMIT rather than a sort of the whole
#: backlog (003_saga.sql says so at length; keeping the two in step is the reader's job).
#:
#: ``FOR UPDATE SKIP LOCKED`` is what makes concurrent drains safe, and it is also why the
#: caller must hold the transaction open across the publish: these row locks live exactly as
#: long as the transaction does. Commit before publishing and a second drainer claims the same
#: rows in the gap.
_CLAIM_UNPUBLISHED_SQL = """
SELECT id,
       workflow_id,
       event_type,
       payload::text        AS payload,
       trace_context::text  AS trace_context,
       created_at
FROM outbox
WHERE published_at IS NULL
ORDER BY id
FOR UPDATE SKIP LOCKED
LIMIT $1
"""

#: Runs in the *same* transaction as the claim above, after the publish returned. ``attempts``
#: is bumped here as well as on the failure path, so the column counts publish attempts rather
#: than publish successes: ``attempts = 3`` reads as "failed twice, then went out".
_MARK_PUBLISHED_SQL = """
UPDATE outbox
SET published_at = now(), attempts = attempts + 1
WHERE id = ANY($1::bigint[])
"""

#: Runs in its OWN transaction, after the claim's transaction has already rolled back -- which
#: is the whole reason it exists as a separate statement. A counter incremented only inside the
#: claim transaction would be rolled back along with everything else on exactly the occasions it
#: is meant to record, leaving ``attempts`` able to describe successes and nothing else.
_RECORD_FAILURE_SQL = """
UPDATE outbox
SET attempts = attempts + 1
WHERE id = ANY($1::bigint[])
"""


async def insert_events(
    conn: asyncpg.Connection,
    workflow_id: UUID,
    events: Sequence[PendingEvent],
) -> int:
    """Write buffered events on ``conn``, inside whatever transaction it is already in.

    Takes a connection rather than a pool so it *cannot* be called except as part of someone
    else's transaction. A pool parameter here would silently acquire a second connection and
    commit on its own, which is the dual write this table exists to abolish -- the signature is
    the guard.

    Returns the number of rows written, so a caller can assert on it; zero events is a no-op
    rather than an error, because most steps emit nothing.
    """
    if not events:
        return 0
    await conn.executemany(
        _INSERT_EVENT_SQL,
        [
            (workflow_id, event.event_type, event.payload_json, event.trace_context_json)
            for event in events
        ],
    )
    return len(events)


async def claim_unpublished(conn: asyncpg.Connection, limit: int) -> list[OutboxEvent]:
    """Lock up to ``limit`` unpublished rows for this transaction, skipping anyone else's.

    The caller **must** already be in a transaction and must keep it open until the publish has
    happened and :func:`mark_published` has run. See ``_CLAIM_UNPUBLISHED_SQL``.
    """
    records = await conn.fetch(_CLAIM_UNPUBLISHED_SQL, limit)
    return [
        OutboxEvent(
            id=record["id"],
            workflow_id=record["workflow_id"],
            event_type=record["event_type"],
            payload_json=record["payload"],
            trace_context_json=record["trace_context"],
            created_at=record["created_at"],
        )
        for record in records
    ]


async def mark_published(conn: asyncpg.Connection, event_ids: Sequence[int]) -> None:
    """Stamp ``published_at``. Same transaction as the claim -- see the module docstring."""
    if not event_ids:
        return
    await conn.execute(_MARK_PUBLISHED_SQL, list(event_ids))


async def record_publish_failure(pool: asyncpg.Pool, event_ids: Sequence[int]) -> None:
    """Count a failed publish attempt, in a transaction of its own.

    Takes a pool, not a connection, and that is deliberate: by the time this is called the
    claim's transaction has rolled back, taking any increment made inside it with it. A row
    that keeps failing to publish has to become *visible* -- that is the only reason
    ``outbox.attempts`` exists (003_saga.sql) -- and it cannot do that from inside the
    transaction that keeps being undone.
    """
    if not event_ids:
        return
    await pool.execute(_RECORD_FAILURE_SQL, list(event_ids))
