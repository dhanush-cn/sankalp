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

The two reads, :func:`load_forward_outputs` and :func:`load_unwind_state`, are what make
replay a lookup instead of a flag: a step is done if and only if a row exists for it, and its
undo is done if and only if a second row exists with ``kind = 'COMPENSATION'``
(migrations/001_core_schema.sql). One rule, applied twice.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg

from sankalp.storage.outbox import PendingEvent, insert_events
from sankalp.storage.queue import ClaimedWorkflow

#: What comes back out of a JSONB column. The engine round-trips step outputs through
#: ``step_outputs.output`` without inspecting them -- only the step that wrote one and the
#: compensation that reads it know its shape -- so this is genuinely unknown here rather
#: than an annotation someone left unfinished.
JsonValue = Any

__all__ = [
    "CompletedStep",
    "Ownership",
    "WorkflowRecord",
    "rows_affected",
    "load_forward_outputs",
    "load_unwind_state",
    "commit_step_output",
    "commit_compensation_output",
    "finish_success",
    "finish_compensated",
    "fail_dirty",
    "schedule_retry",
    "begin_compensation",
    "defer_compensation",
    "renew_lease",
    "submit_workflow",
    "get_workflow",
    "get_completed_steps",
    "cancel_workflow",
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


#: Both kinds in one query, because the unwind needs both and they live in one table. The
#: ``ORDER BY seq DESC`` is the unwind order itself (docs/spec.md, "Compensation Model"), and
#: it is taken from the persisted ``seq`` rather than from ``definition.steps`` on purpose: a
#: definition edited under an in-flight saga would otherwise silently reorder the reversal of
#: money that has already moved. The row says what order it ran in; the code does not get a
#: second opinion.
_LOAD_UNWIND_STATE_SQL = """
SELECT step_name, seq, kind, output
FROM step_outputs
WHERE workflow_id = $1
ORDER BY seq DESC
"""


@dataclass(frozen=True, slots=True)
class CompletedStep:
    """One committed FORWARD checkpoint, as the unwind reads it back.

    ``output`` is what the forward step returned and is handed straight to its compensation --
    which is the entire reason an unwind is possible after a crash that lost every scrap of
    in-memory state.
    """

    step_name: str
    seq: int
    output: JsonValue


async def load_unwind_state(
    pool: asyncpg.Pool, workflow_id: UUID
) -> tuple[list[CompletedStep], set[str]]:
    """Everything the COMPENSATING pass needs, in one round trip.

    Returns the completed forward steps in reverse ``seq`` order -- the order their
    compensations must run in -- and the names of the steps whose compensation has already
    committed.

    That second set is the idempotency guard, and it is the same rule as the forward replay
    check: a compensation is done if and only if a row exists. Membership, never truthiness --
    a compensation stores no output, so every one of these rows has ``output IS NULL`` and a
    truthiness test would re-run every undo in the workflow.
    """
    records = await pool.fetch(_LOAD_UNWIND_STATE_SQL, workflow_id)
    forward = [
        CompletedStep(step_name=r["step_name"], seq=r["seq"], output=_decode(r["output"]))
        for r in records
        if r["kind"] == "FORWARD"
    ]
    compensated = {r["step_name"] for r in records if r["kind"] == "COMPENSATION"}
    return forward, compensated


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

#: ``AND status = 'RUNNING'`` is a guard, not a filter. Only a forward run reaches SUCCESS,
#: and a forward run is RUNNING by definition -- so the predicate costs nothing on the happy
#: path and makes one specific catastrophe unrepresentable: a COMPENSATING workflow being
#: promoted to SUCCESS. That transition would mark a saga complete while the forward steps it
#: was unwinding stay un-unwound, i.e. money moved and the row says everything is fine.
#: The executor also routes a COMPENSATING claim to the unwind rather than the forward loop,
#: which means this predicate should never be the thing that fires. Keep both: that dispatch
#: is a Python branch one edit away from being lost, and this is the lock on the door itself.
_FINISH_SUCCESS_SQL = """
UPDATE workflows
SET status           = 'SUCCESS',
    output           = $2::jsonb,
    error            = NULL,
    owner_id         = NULL,
    lease_expires_at = NULL,
    updated_at       = now()
WHERE id = $1 AND owner_id = $3 AND fencing_token = $4 AND status = 'RUNNING'
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

#: The compensation half of ``_CHECKPOINT_POSITION_SQL``, and the extra ``AND status =
#: 'COMPENSATING'`` is the same kind of guard as the one on ``_FINISH_SUCCESS_SQL``: an undo
#: may only be recorded against a workflow that is actually unwinding. ``current_step`` is
#: moved to the step being reversed so an operator watching the row sees the unwind walk
#: backwards through it, rather than the row appearing frozen at the step that failed.
_CHECKPOINT_COMPENSATION_POSITION_SQL = """
UPDATE workflows
SET current_step = $2, updated_at = now()
WHERE id = $1 AND owner_id = $3 AND fencing_token = $4
  AND status = 'COMPENSATING'
"""

#: The undo's checkpoint. ``output`` is NULL because a compensation returns nothing -- the row
#: exists to say "this was undone", and that fact is the whole payload. Same shape as the
#: forward insert otherwise, including deriving ``started_at`` from the server's ``now()``
#: minus the measured duration so both ends of the compensation are stamped by one clock.
_INSERT_COMPENSATION_OUTPUT_SQL = """
INSERT INTO step_outputs (workflow_id, step_name, seq, kind, output, started_at, completed_at)
VALUES ($1, $2, $3, 'COMPENSATION', NULL, now() - make_interval(secs => $4::float8), now())
"""

#: Every compensation is committed: the saga is fully reversed.
#:
#: ``error`` is pointedly absent from the SET list. It still holds the failure that sent the
#: workflow here, and that is the single most useful thing to read off a COMPENSATED row
#: afterwards -- clearing it would leave a successfully unwound saga with no record of what it
#: was unwinding from.
#:
#: ``AND status = 'COMPENSATING'`` is part of the guard, not decoration: only an unwinding
#: workflow can become COMPENSATED, so a row that moved on since it was claimed is untouchable.
_FINISH_COMPENSATED_SQL = """
UPDATE workflows
SET status           = 'COMPENSATED',
    owner_id         = NULL,
    lease_expires_at = NULL,
    updated_at       = now()
WHERE id = $1 AND owner_id = $2 AND fencing_token = $3 AND status = 'COMPENSATING'
"""

#: The state a human has to resolve. Reached only when a compensation could not be made to
#: succeed, which means money is committed somewhere it should not be and no automated path
#: remains -- so this deliberately does NOT release the row back onto the queue in a claimable
#: status. FAILED_DIRTY falls out of ``idx_workflows_claimable`` (001_core_schema.sql), so the
#: row is terminal and no worker will pick it up and quietly try again.
#:
#: ``error`` IS replaced here, unlike in the COMPENSATED case: the compensation failure is now
#: the actionable fact, and the original step failure is already in the logs.
_FAIL_DIRTY_SQL = """
UPDATE workflows
SET status           = 'FAILED_DIRTY',
    error            = $2,
    owner_id         = NULL,
    lease_expires_at = NULL,
    updated_at       = now()
WHERE id = $1 AND owner_id = $3 AND fencing_token = $4 AND status = 'COMPENSATING'
"""

#: Put a COMPENSATING workflow back on the queue, later, without changing what it is.
#:
#: ``status`` is pointedly absent from the SET list. The row is COMPENSATING and stays
#: COMPENSATING -- this releases ownership and pushes ``run_after`` out, nothing more. The
#: alternative would be a seventh status meaning "waiting for a worker that can read this
#: workflow's type", and the six are exactly six on purpose.
#:
#: ``AND status = 'COMPENSATING'`` is part of the ownership guard rather than decoration: it
#: makes the statement unable to touch a row that has moved on since it was claimed.
_DEFER_COMPENSATION_SQL = """
UPDATE workflows
SET owner_id         = NULL,
    lease_expires_at = NULL,
    run_after        = now() + make_interval(secs => $2::float8),
    updated_at       = now()
WHERE id = $1 AND owner_id = $3 AND fencing_token = $4
  AND status = 'COMPENSATING'
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
    events: Sequence[PendingEvent] = (),
) -> bool:
    """Checkpoint one completed step, advance the workflow, and emit its events -- atomically.

    The step's side effect has already happened by the time this is called; this is the
    single write that makes it *durably done*, so that a crash one instruction later replays
    the workflow without re-running the step. Returns False if the ownership guard matched
    zero rows, in which case nothing is written -- the whole thing rolls back together.

    **This transaction is the answer to the dual-write problem.** The step's events go into
    ``outbox`` right here, beside the checkpoint, so there is no instant at which one exists
    without the other. The alternative -- commit the step, then publish -- fails in both
    directions: crash after the commit and the event is lost; publish first and crash before
    the commit and you have announced something that never happened. Ordering does not save
    it, because the failure is that they are two writes. A separate drain loop moves the
    committed rows to Redis afterwards, and *that* handoff is at-least-once (storage/outbox.py).

    ``output_json`` and the events' payloads are pre-serialised by the caller, on purpose: a
    value that cannot be encoded must fail before the transaction opens, not with a
    half-written one held open while ``json.dumps`` raises.
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
        # Deliberately after the guarded UPDATE, which has already proved ownership and taken
        # the row lock. A preempted worker returns above having written nothing -- and
        # "nothing" now includes its events, which is what makes an orphaned event describing
        # a checkpoint that was rolled back impossible rather than merely rare.
        await insert_events(conn, own.workflow_id, events)
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


async def commit_compensation_output(
    pool: asyncpg.Pool,
    own: Ownership,
    *,
    step_name: str,
    seq: int,
    duration_seconds: float,
    events: Sequence[PendingEvent] = (),
) -> bool:
    """Checkpoint one completed compensation, atomically with the workflow's position.

    Exactly :func:`commit_step_output`'s shape, and for exactly its reasons. The undo's side
    effect has already happened by the time this is called; this single write is what makes it
    *durably undone*, so a crash one instruction later resumes the unwind without running the
    compensation again. Events a compensation emitted ride in this transaction on the same
    terms -- an undo is a state change like any other and its event must not be able to
    outlive a checkpoint that rolled back.

    The guarded UPDATE is taken first inside the transaction, before the INSERT, and that
    order is load-bearing rather than stylistic: it proves ownership *and* takes the row lock,
    so a concurrent claimer's ``FOR UPDATE SKIP LOCKED`` skips this row for the rest of the
    transaction. A worker that has already been preempted therefore stops here having written
    nothing, and can never reach the INSERT and collide on the ``step_outputs`` primary key.

    Returns False if the guard matched zero rows -- preempted, or no longer COMPENSATING -- in
    which case nothing is written and the whole thing rolls back together.
    """
    async with pool.acquire() as conn, conn.transaction():
        tag = await conn.execute(
            _CHECKPOINT_COMPENSATION_POSITION_SQL,
            own.workflow_id,
            step_name,
            own.owner_id,
            own.fencing_token,
        )
        if rows_affected(tag) == 0:
            return False
        await conn.execute(
            _INSERT_COMPENSATION_OUTPUT_SQL,
            own.workflow_id,
            step_name,
            seq,
            duration_seconds,
        )
        await insert_events(conn, own.workflow_id, events)
        return True


async def finish_compensated(pool: asyncpg.Pool, own: Ownership) -> bool:
    """Every compensation is checkpointed: mark the saga fully reversed and release the lease."""
    tag = await pool.execute(
        _FINISH_COMPENSATED_SQL, own.workflow_id, own.owner_id, own.fencing_token
    )
    return rows_affected(tag) > 0


async def fail_dirty(pool: asyncpg.Pool, own: Ownership, *, error: str) -> bool:
    """A compensation could not be made to succeed: park the workflow for a human.

    Terminal on purpose. The row is released but lands in a status no dequeue query claims, so
    nothing retries it automatically -- money is committed somewhere it should not be, the
    engine has exhausted what it can do about that, and quietly cycling the row would hide it.
    The committed ``kind = 'COMPENSATION'`` rows are the record of how far the unwind got.
    """
    tag = await pool.execute(
        _FAIL_DIRTY_SQL, own.workflow_id, error, own.owner_id, own.fencing_token
    )
    return rows_affected(tag) > 0


async def defer_compensation(
    pool: asyncpg.Pool, own: Ownership, *, delay_seconds: float
) -> bool:
    """Hand a COMPENSATING workflow back to the queue -- for **one** narrow reason.

    **Read this before deleting it as leftover.** An earlier build had a function of this name
    that every COMPENSATING claim went through, because nothing could unwind a saga yet. That
    stub is gone; :func:`~sankalp.engine.executor._compensate` replaced it and is what runs an
    unwind now. This is a different thing wearing the same name, and it is reachable from
    exactly one place: a worker that claimed a COMPENSATING row whose ``workflow_type`` its
    build does not import, and which therefore cannot resolve the definition it would need to
    know what the compensations even are.

    That is a fact about *this worker*, not about the workflow -- a rolling deploy, an old
    binary, a definition module someone forgot to import. The honest response is to give the
    row back untouched and let a worker that knows the type claim it. Note what the
    alternatives cost:

    * Raising instead would leave the row claimed until its lease expired, so the same
      ignorant worker would pick it up again a lease later, and again, spinning on a workflow
      it can never make progress on while ``begin_compensation``'s ``run_after = now()`` keeps
      it permanently at the front of the queue.
    * FAILED_DIRTY would page a human for a deployment that will fix itself in thirty seconds.
    * Compensating "as best we can" is not available: without the definition there is no list
      of compensations to run.

    So this writes no status, no error, and no checkpoint. It releases ownership and pushes
    ``run_after`` out by a jittered backoff, which is the only difference from doing nothing at
    all -- and it is what keeps a partially-deployed fleet from busy-looping on the row.

    Returns False if the ownership guard matched zero rows, or if the workflow is no longer
    COMPENSATING -- either way somebody else has it and this worker must drop the work.
    """
    tag = await pool.execute(
        _DEFER_COMPENSATION_SQL,
        own.workflow_id,
        delay_seconds,
        own.owner_id,
        own.fencing_token,
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


# ---------------------------------------------------------------------------
# API reads/writes. Not ownership-guarded -- the API holds no lease and no
# fencing token; these touch only the columns a submitter or a caller reading
# workflow state is entitled to.
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class WorkflowRecord:
    """A ``workflows`` row as the API reads and returns it."""

    id: UUID
    workflow_type: str
    status: str
    input: JsonValue
    output: JsonValue
    error: str | None
    current_step: str | None
    attempt: int


_WORKFLOW_RECORD_COLUMNS = """
    id, workflow_type, status, input, output, error, current_step, attempt
"""


def _to_record(r: asyncpg.Record) -> WorkflowRecord:
    return WorkflowRecord(
        id=r["id"],
        workflow_type=r["workflow_type"],
        status=r["status"],
        input=_decode(r["input"]),
        output=_decode(r["output"]),
        error=r["error"],
        current_step=r["current_step"],
        attempt=r["attempt"],
    )


#: ``RETURNING`` on the insert first, ``DO NOTHING`` on conflict, only falling back to the
#: SELECT below when it returns no row. This is the shape docs/spec.md's "Submit handler, in
#: full" specifies, and the ordering is load-bearing: Postgres's speculative-insertion
#: arbitration makes a concurrent racer's INSERT *block* on this one until it commits (or
#: proceed itself if this one rolls back) rather than read "no conflict yet". Under the
#: default READ COMMITTED isolation (Postgres's default, and asyncpg's ``conn.transaction()``
#: default), the follow-up SELECT -- run only after this statement returns, in the same
#: transaction -- takes a fresh snapshot that is guaranteed to see whichever row won. Never
#: raise this transaction's isolation level: at REPEATABLE READ or SERIALIZABLE, the loser's
#: blocked insert raises a serialization failure instead of resolving into DO NOTHING.
_SUBMIT_INSERT_SQL = f"""
INSERT INTO workflows (workflow_type, idempotency_key, input, status)
VALUES ($1, $2, $3::jsonb, 'PENDING')
ON CONFLICT (workflow_type, idempotency_key) DO NOTHING
RETURNING {_WORKFLOW_RECORD_COLUMNS}
"""

_SUBMIT_SELECT_SQL = f"""
SELECT {_WORKFLOW_RECORD_COLUMNS}
FROM workflows
WHERE workflow_type = $1 AND idempotency_key = $2
"""


async def submit_workflow(
    pool: asyncpg.Pool, *, workflow_type: str, idempotency_key: str, input_json: str
) -> tuple[WorkflowRecord, bool]:
    """Insert a new workflow, or return the one a prior identical submit already created.

    Returns ``(record, created)`` -- ``created`` is True only for the request that actually
    inserted the row, which is what tells the route whether to answer 201 or 200. Never
    ``ON CONFLICT ... DO UPDATE``: a duplicate submit must never mutate a workflow that may
    already be RUNNING (docs/spec.md, Phase 1 API). See the SQL comments above for why the
    two statements below are race-free under concurrent duplicate submits without a retry loop.
    """
    async with pool.acquire() as conn, conn.transaction():
        row = await conn.fetchrow(_SUBMIT_INSERT_SQL, workflow_type, idempotency_key, input_json)
        if row is not None:
            return _to_record(row), True
        row = await conn.fetchrow(_SUBMIT_SELECT_SQL, workflow_type, idempotency_key)
        assert row is not None, (
            "submit lost the race twice: INSERT found a conflict but the SELECT that follows "
            "it, in the same transaction and after the INSERT returned, found no row"
        )
        return _to_record(row), False


_GET_WORKFLOW_SQL = f"""
SELECT {_WORKFLOW_RECORD_COLUMNS}
FROM workflows
WHERE id = $1
"""


async def get_workflow(pool: asyncpg.Pool, workflow_id: UUID) -> WorkflowRecord | None:
    """A single workflow's current state, or None if no such id exists."""
    row = await pool.fetchrow(_GET_WORKFLOW_SQL, workflow_id)
    return _to_record(row) if row is not None else None


_GET_COMPLETED_STEPS_SQL = """
SELECT step_name
FROM step_outputs
WHERE workflow_id = $1 AND kind = 'FORWARD'
ORDER BY seq
"""


async def get_completed_steps(pool: asyncpg.Pool, workflow_id: UUID) -> list[str]:
    """Names of the forward steps this workflow has checkpointed, in execution order."""
    records = await pool.fetch(_GET_COMPLETED_STEPS_SQL, workflow_id)
    return [r["step_name"] for r in records]


#: Only PENDING/RUNNING may be cancelled -- a SUCCESS or COMPENSATED workflow is done, and a
#: workflow already COMPENSATING or FAILED_DIRTY is already past the point cancellation means
#: anything. Deliberately does not touch owner_id/fencing_token: the API is not the lease
#: holder and has no fencing token to present. A worker mid-step will not observe this
#: transition -- its per-step writes guard on ownership, not status -- but it cannot reach
#: SUCCESS afterward either: ``_FINISH_SUCCESS_SQL`` requires ``status = 'RUNNING'``, so that
#: UPDATE matches zero rows, the executor treats it as preempted, and the row (COMPENSATING,
#: owner_id still set until its lease expires) falls back to the ordinary lease-expiry
#: recovery path into an unwind.
_CANCEL_SQL = """
UPDATE workflows
SET status     = 'COMPENSATING',
    error      = COALESCE(error, 'cancelled by user'),
    run_after  = now(),
    updated_at = now()
WHERE id = $1 AND status IN ('PENDING', 'RUNNING')
RETURNING id
"""


async def cancel_workflow(pool: asyncpg.Pool, workflow_id: UUID) -> bool:
    """Move a PENDING/RUNNING workflow to COMPENSATING. False if it can't be cancelled now.

    False covers two different callers' cases the route must tell apart -- the id does not
    exist, or it exists but is already SUCCESS/COMPENSATING/COMPENSATED/FAILED_DIRTY -- by
    design this function does not distinguish them; the route re-reads the row with
    :func:`get_workflow` only on this path to decide 404 vs 409, so the common (successful)
    case stays one round trip.
    """
    tag = await pool.execute(_CANCEL_SQL, workflow_id)
    return rows_affected(tag) > 0
