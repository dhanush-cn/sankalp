"""Every write a worker makes to a workflow it has claimed. SQL lives here, not in the engine.

Two rules shape this whole module.

**One transaction per transition.** :func:`commit_step_output` writes the checkpoint and the
workflow's new position in a single transaction. There is deliberately no "record the step,
then update the workflow" pair anywhere below -- that is the dual write, and the gap between
the two writes is exactly the crash window that re-executes a side effect or loses a
checkpoint.

**Every write carries the ownership guard.** ``WHERE id = $1 AND owner_id = $2 AND
fencing_token = $3``. A worker that stalled -- GC pause, VM freeze, partition -- and wakes up
still believing it owns the workflow has had its row re-claimed by someone else, which bumped
``fencing_token``. Its UPDATE therefore matches zero rows and it learns, at the only moment
that matters, that it was preempted. That is why each function here returns ``bool`` rather
than ``None``: **False means preempted**, and the caller must drop the work rather than
carry on. Never "fix" one of these by dropping the guard or by ignoring the return value.

The one read, :func:`load_forward_outputs`, is what makes replay a lookup instead of a flag:
a step is done if and only if a row exists for it (migrations/001_core_schema.sql).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg

from sankalp.storage.queue import ClaimedWorkflow

#: What comes back out of a JSONB column. The engine round-trips step outputs through
#: ``step_outputs.output`` without inspecting them -- only the step that wrote one and the
#: compensation that reads it know its shape -- so this is genuinely unknown here rather
#: than an annotation someone left unfinished.
JsonValue = Any

__all__ = [
    "Ownership",
    "rows_affected",
    "load_forward_outputs",
    "commit_step_output",
    "finish_success",
    "schedule_retry",
    "begin_compensation",
    "renew_lease",
]


@dataclass(frozen=True, slots=True)
class Ownership:
    """The three columns that together say "this worker, this claim, right now".

    Bundled rather than passed as a loose triple because they are only ever correct
    together: the id alone identifies the row, but it is ``owner_id`` *and* ``fencing_token``
    that distinguish the live owner from a zombie holding a stale claim.
    """

    workflow_id: UUID
    owner_id: str
    fencing_token: int

    @classmethod
    def of(cls, claimed: ClaimedWorkflow) -> Ownership:
        return cls(
            workflow_id=claimed.id,
            owner_id=claimed.owner_id,
            fencing_token=claimed.fencing_token,
        )


def rows_affected(command_tag: str) -> int:
    """Rows touched, from asyncpg's command tag (``'UPDATE 1'``, ``'INSERT 0 1'``).

    This is the ownership guard's entire return channel. Zero means the ``WHERE`` clause
    matched nothing, which for the statements in this module means one thing only: a newer
    claimer holds a higher fencing token.
    """
    return int(command_tag.rsplit(" ", 1)[-1])


# ---------------------------------------------------------------------------
# Read: the replay set.
# ---------------------------------------------------------------------------

_LOAD_FORWARD_OUTPUTS_SQL = """
SELECT step_name, output
FROM step_outputs
WHERE workflow_id = $1 AND kind = 'FORWARD'
"""


def _decode(value: str | bytes | None) -> JsonValue:
    """Decode a JSONB column read over a connection with no json codec registered.

    Deliberately mirrors ``queue._decode_json``: these functions take a pool they do not own
    and cannot assume ``set_type_codec`` has been called on its connections. For the same
    reason every write below binds JSON as *text* with an explicit ``::jsonb`` cast -- with
    a codec registered, passing a str would double-encode, and passing a dict without one
    would fail outright. Text plus a cast is the shape that is correct either way.
    """
    if value is None or not isinstance(value, str | bytes):
        return value
    return json.loads(value)


async def load_forward_outputs(pool: asyncpg.Pool, workflow_id: UUID) -> dict[str, JsonValue]:
    """Committed FORWARD outputs for a workflow, keyed by step name -- in one query.

    One query for the whole workflow, not one per step: the replay check runs on every step
    of every execution, and the per-step version turns an N-step workflow into N round trips
    before it does any work.

    A step with a JSON ``null`` output is present in the returned dict with value ``None``.
    Callers must therefore test membership (``name in outputs``), never truthiness -- a step
    that legitimately returned nothing is still done, and re-running it would be a second
    side effect.
    """
    records = await pool.fetch(_LOAD_FORWARD_OUTPUTS_SQL, workflow_id)
    return {r["step_name"]: _decode(r["output"]) for r in records}


# ---------------------------------------------------------------------------
# Writes. All ownership-guarded; all return False when preempted.
# ---------------------------------------------------------------------------

#: Taken FIRST inside the transaction, before the checkpoint is inserted, and that order is
#: load-bearing rather than stylistic. It proves ownership *and* takes the row lock, so a
#: concurrent claimer's ``FOR UPDATE SKIP LOCKED`` skips this row for the rest of the
#: transaction. A worker that has already been preempted therefore stops here, having
#: written nothing -- it can never reach the INSERT and collide on the step_outputs primary
#: key. Insert first and that collision becomes reachable, and the error it raises says
#: "duplicate key" rather than "you were preempted".
_CHECKPOINT_POSITION_SQL = """
UPDATE workflows
SET current_step = $2, updated_at = now()
WHERE id = $1 AND owner_id = $3 AND fencing_token = $4
"""

#: ``started_at`` is derived from the server's ``now()`` minus the measured duration rather
#: than sent as a Python timestamp, so the two ends of a step are stamped by one clock and
#: the pair stays consistent under host/container skew.
_INSERT_STEP_OUTPUT_SQL = """
INSERT INTO step_outputs (workflow_id, step_name, seq, kind, output, started_at, completed_at)
VALUES ($1, $2, $3, 'FORWARD', $4::jsonb, now() - make_interval(secs => $5::float8), now())
"""

_FINISH_SUCCESS_SQL = """
UPDATE workflows
SET status           = 'SUCCESS',
    output           = $2::jsonb,
    error            = NULL,
    owner_id         = NULL,
    lease_expires_at = NULL,
    updated_at       = now()
WHERE id = $1 AND owner_id = $3 AND fencing_token = $4
"""

#: Back to PENDING with the lease released and ``run_after`` pushed out by the backoff.
#: ``attempt`` is untouched: the dequeue query increments it as part of claiming, so
#: bumping it here would count every retry twice and halve max_attempts.
_SCHEDULE_RETRY_SQL = """
UPDATE workflows
SET status           = 'PENDING',
    error            = $2,
    owner_id         = NULL,
    lease_expires_at = NULL,
    run_after        = now() + make_interval(secs => $3::float8),
    updated_at       = now()
WHERE id = $1 AND owner_id = $4 AND fencing_token = $5
"""

#: ``run_after`` is reset to now() because the row may still be carrying a future value from
#: the retry that preceded this failure, and an unwind must not sit in the queue waiting out
#: a backoff -- money is committed in the steps it has to undo.
_BEGIN_COMPENSATION_SQL = """
UPDATE workflows
SET status           = 'COMPENSATING',
    error            = $2,
    owner_id         = NULL,
    lease_expires_at = NULL,
    run_after        = now(),
    updated_at       = now()
WHERE id = $1 AND owner_id = $3 AND fencing_token = $4
"""

_RENEW_LEASE_SQL = """
UPDATE workflows
SET lease_expires_at = now() + make_interval(secs => $2::float8),
    updated_at       = now()
WHERE id = $1 AND owner_id = $3 AND fencing_token = $4
"""


async def commit_step_output(
    pool: asyncpg.Pool,
    own: Ownership,
    *,
    step_name: str,
    seq: int,
    output_json: str,
    duration_seconds: float,
) -> bool:
    """Checkpoint one completed step and advance the workflow's position, atomically.

    The step's side effect has already happened by the time this is called; this is the
    single write that makes it *durably done*, so that a crash one instruction later replays
    the workflow without re-running the step. Returns False if the ownership guard matched
    zero rows, in which case nothing is written -- the whole thing rolls back together.

    ``output_json`` is pre-serialised by the caller, on purpose: a value that cannot be
    encoded must fail before the transaction opens, not with a half-written one held open
    while ``json.dumps`` raises.
    """
    async with pool.acquire() as conn, conn.transaction():
        tag = await conn.execute(
            _CHECKPOINT_POSITION_SQL,
            own.workflow_id,
            step_name,
            own.owner_id,
            own.fencing_token,
        )
        if rows_affected(tag) == 0:
            return False
        await conn.execute(
            _INSERT_STEP_OUTPUT_SQL,
            own.workflow_id,
            step_name,
            seq,
            output_json,
            duration_seconds,
        )
        return True


async def finish_success(pool: asyncpg.Pool, own: Ownership, *, output_json: str) -> bool:
    """Every step is checkpointed: mark the workflow SUCCESS and release the lease."""
    tag = await pool.execute(
        _FINISH_SUCCESS_SQL, own.workflow_id, output_json, own.owner_id, own.fencing_token
    )
    return rows_affected(tag) > 0


async def schedule_retry(
    pool: asyncpg.Pool, own: Ownership, *, error: str, delay_seconds: float
) -> bool:
    """Return the workflow to the queue after ``delay_seconds``.

    The checkpoints stay exactly where they are -- that is the point of retrying rather than
    unwinding: the next attempt replays the completed steps as lookups and resumes at the one
    that failed.
    """
    tag = await pool.execute(
        _SCHEDULE_RETRY_SQL,
        own.workflow_id,
        error,
        delay_seconds,
        own.owner_id,
        own.fencing_token,
    )
    return rows_affected(tag) > 0


async def begin_compensation(pool: asyncpg.Pool, own: Ownership, *, error: str) -> bool:
    """Hand the workflow to the unwind: COMPENSATING, lease released, immediately claimable."""
    tag = await pool.execute(
        _BEGIN_COMPENSATION_SQL, own.workflow_id, error, own.owner_id, own.fencing_token
    )
    return rows_affected(tag) > 0


async def renew_lease(pool: asyncpg.Pool, own: Ownership, *, duration_seconds: float) -> bool:
    """Push the lease out by ``duration_seconds``. False means the workflow is no longer ours.

    Guarded like every other write here, which makes renewal the cheapest way a stalled
    worker discovers it was preempted -- before it starts the next step, rather than after
    that step has already moved money.
    """
    tag = await pool.execute(
        _RENEW_LEASE_SQL, own.workflow_id, duration_seconds, own.owner_id, own.fencing_token
    )
    return rows_affected(tag) > 0
