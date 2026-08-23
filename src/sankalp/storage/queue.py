"""Claiming work from the queue.

There is no queue table and no recovery daemon. The ``workflows`` row *is* the queue
entry: a worker claims it by stamping ``owner_id`` and ``lease_expires_at`` on it, and a
dead worker's rows become claimable again for exactly one reason -- ``lease_expires_at``
drifts into the past and the dequeue query's ``status = 'RUNNING' AND lease_expires_at <
now()`` branch picks them up. Crash recovery IS that predicate (docs/spec.md, Phase 1).

The SQL below is transcribed verbatim from docs/spec.md ("The Dequeue Query"). It is not
to be rewritten here; if the query needs to change, it changes in the spec first.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg

#: Verbatim from docs/spec.md. Three clauses are load-bearing:
#:
#:   * ``FOR UPDATE SKIP LOCKED`` -- N workers poll simultaneously and never collide.
#:     Without it they all queue behind the same row and claiming serialises.
#:   * ``status = 'RUNNING' AND lease_expires_at < now()`` -- this is the whole of crash
#:     recovery. Remove it and a killed worker's workflows are stranded forever.
#:   * ``ORDER BY run_after`` inside the subquery -- matches the leading column of the
#:     partial index ``idx_workflows_claimable``, so a claim is an ordered walk that stops
#:     at LIMIT rather than a sort of the entire backlog.
#:
#: The CASE on status exists so a COMPENSATING workflow stays COMPENSATING when it is
#: re-claimed; only PENDING work becomes RUNNING.
#:
#: The ``::workflow_status`` cast on that CASE is required, not decoration. Both branches
#: are untyped literals, so the CASE resolves to text, and Postgres refuses to assign text
#: to an enum column. Drop it and nothing claims at all. (The spec carries the same cast
#: and the same note -- an earlier revision of it did not, and the query as printed there
#: would not run.)
_CLAIM_SQL = """
UPDATE workflows w
SET status           = (CASE WHEN w.status = 'COMPENSATING'
                             THEN 'COMPENSATING' ELSE 'RUNNING' END)::workflow_status,
    owner_id         = $1,
    lease_expires_at = now() + ($2 || ' seconds')::interval,
    fencing_token    = w.fencing_token + 1,
    attempt          = w.attempt + 1,
    updated_at       = now()
FROM (
    SELECT id
    FROM workflows
    WHERE run_after <= now()
      AND (
            status IN ('PENDING', 'COMPENSATING')
            OR (status = 'RUNNING' AND lease_expires_at < now())
          )
    ORDER BY run_after
    FOR UPDATE SKIP LOCKED
    LIMIT $3
) AS claimed
WHERE w.id = claimed.id
RETURNING w.*
"""


def _decode_json(value: str | bytes | dict[str, Any] | None) -> dict[str, Any] | None:
    """Decode a JSONB column whether or not the caller registered a json codec.

    ``claim_workflows`` takes a connection it does not own, so it cannot assume
    ``set_type_codec('jsonb', ...)`` has been called on it. Handle both shapes.
    """
    if value is None or isinstance(value, dict):
        return value
    return json.loads(value)


@dataclass(frozen=True, slots=True)
class ClaimedWorkflow:
    """One workflow row, freshly claimed by this worker.

    ``owner_id``, ``lease_expires_at`` and ``fencing_token`` are nullable columns but are
    non-optional here: the UPDATE that produced this row just stamped all three.

    ``fencing_token`` is the value every subsequent ownership-scoped write must carry
    (``WHERE id = $1 AND owner_id = $2 AND fencing_token = $3``). Zero rows affected means
    a newer claimer holds a higher token and this worker was preempted.
    """

    id: UUID
    workflow_type: str
    idempotency_key: str
    status: str
    input: dict[str, Any]
    output: dict[str, Any] | None
    error: str | None
    current_step: str | None
    attempt: int
    max_attempts: int
    owner_id: str
    lease_expires_at: datetime
    fencing_token: int
    run_after: datetime
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_record(cls, record: asyncpg.Record) -> ClaimedWorkflow:
        return cls(
            id=record["id"],
            workflow_type=record["workflow_type"],
            idempotency_key=record["idempotency_key"],
            status=record["status"],
            input=_decode_json(record["input"]) or {},
            output=_decode_json(record["output"]),
            error=record["error"],
            current_step=record["current_step"],
            attempt=record["attempt"],
            max_attempts=record["max_attempts"],
            owner_id=record["owner_id"],
            lease_expires_at=record["lease_expires_at"],
            fencing_token=record["fencing_token"],
            run_after=record["run_after"],
            created_at=record["created_at"],
            updated_at=record["updated_at"],
        )


async def claim_workflows(
    conn: asyncpg.Connection,
    owner_id: str,
    lease_seconds: int,
    batch_size: int,
) -> list[ClaimedWorkflow]:
    """Claim up to ``batch_size`` runnable workflows for ``owner_id``.

    Returns the claimed rows with their new ``fencing_token`` and lease, or an empty list
    if nothing is runnable. Claimable means: ``run_after`` has passed, and the workflow is
    either waiting (PENDING / COMPENSATING) or is RUNNING under an expired lease -- the
    latter being a worker that died holding it.

    No transaction is opened. The statement is a single UPDATE and therefore already
    atomic; wrapping it would only hold the row locks longer and starve other claimers.
    """
    records = await conn.fetch(
        _CLAIM_SQL,
        owner_id,
        # Bound as text, not int: in ``($2 || ' seconds')::interval`` Postgres resolves the
        # parameter through the || operator and infers text. An int bind fails outright.
        str(lease_seconds),
        batch_size,
    )
    return [ClaimedWorkflow.from_record(r) for r in records]
