"""Running one claimed workflow to its next resting state.

This is the loop docs/spec.md calls "Execution Flow", and the guarantee in CLAUDE.md is
stated entirely in terms of what happens here: kill the process at any instant and the
workflow resumes from the last completed step, with no step's side effect executed twice.

That reduces to three properties, and every design choice below serves one of them.

**Replay is a lookup.** Before the first step runs, one query loads the ``step_outputs``
rows already committed for this workflow. A step whose name is in that set is not executed;
its committed output is put into the context and the loop moves on. There is no "completed"
flag to have forgotten to set and no in-memory progress to have lost -- the row is the
checkpoint, so the question "did this already run?" has the same answer in the process that
ran it and in the process that picks it up forty seconds after that one was killed.

**A step and its checkpoint commit together.** :func:`~sankalp.storage.workflows.commit_step_output`
writes the checkpoint and the workflow's new position in one transaction. The crash window
that would double-execute a step is not narrowed here, it is absent: either the checkpoint
is durable or it is not, and if it is not, the step runs again.

**Nothing is written without proving ownership.** Every write carries ``AND owner_id = $x
AND fencing_token = $y``. Zero rows means another worker re-claimed this workflow while we
were stalled, and the response is to stop immediately and write nothing else -- not to
retry, not to compensate, not to record an error. The new owner is already replaying from
the last checkpoint; anything we wrote after losing the row would be a second, stale worker
editing a workflow it does not own.

The failure branch is the other half of the file, and getting it wrong is how money moves
twice or gets stranded (src/sankalp/engine/errors.py):

    RetryableError, attempts left  ->  PENDING, released, run_after = now() + backoff
    anything else                  ->  COMPENSATING, released, unwound by Phase 2

Note which way the default points. An exception the step did not classify is treated as
terminal, because an unrecognised failure is not evidence that re-running the step is safe,
while compensation is idempotent by contract.

Until Phase 2 exists, "unwound by Phase 2" needs one more thing to be true: a Phase 1 worker
that claims a COMPENSATING row must refuse it rather than run it forward. Nothing in the
schema stops it claiming one -- an unwind is deliberately claimable with no backoff -- so the
refusal lives here, at the top of :func:`execute_workflow`, ahead of any step invocation.
Without it a failed workflow is re-executed forward on every claim, and one whose step
happens to succeed on a later attempt is carried to SUCCESS with its committed side effects
never unwound.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from enum import StrEnum
from types import MappingProxyType
from typing import Any

import asyncpg

from sankalp.config import Settings, get_settings
from sankalp.engine.definition import Step, StepContext, StepOutput, get_definition
from sankalp.engine.errors import PreemptedError, TerminalError
from sankalp.engine.lease import Lease
from sankalp.resilience.backoff import compute_backoff
from sankalp.storage import workflows as workflow_writes
from sankalp.storage.queue import ClaimedWorkflow

__all__ = ["ExecutionResult", "execute_workflow"]

log = logging.getLogger("sankalp.executor")

#: Errors are stored in an unbounded TEXT column on a table that is UPDATEd on every claim
#: and every lease renewal. A driver traceback pasted into a stack of a thousand rows bloats
#: the hottest table in the schema to no one's benefit; the full detail belongs in the logs.
_MAX_ERROR_CHARS = 2000


class ExecutionResult(StrEnum):
    """Where one execution left the workflow. Not a workflow status.

    ``PREEMPTED`` in particular is not a state any row is ever in: it says this *worker*
    stopped because it lost the row, and the workflow itself is untouched and already in
    somebody else's hands.
    """

    SUCCESS = "SUCCESS"
    RETRY_SCHEDULED = "RETRY_SCHEDULED"
    COMPENSATING = "COMPENSATING"
    PREEMPTED = "PREEMPTED"
    #: Claimed a workflow that needs unwinding, in a build with nothing to unwind it. Like
    #: PREEMPTED, this names what this *worker* did, not a state the row is in -- the row is
    #: COMPENSATING before and after. Phase 2 removes it.
    COMPENSATION_DEFERRED = "COMPENSATION_DEFERRED"


async def execute_workflow(
    conn_pool: asyncpg.Pool,
    claimed: ClaimedWorkflow,
    *,
    lease: Lease | None = None,
    settings: Settings | None = None,
) -> ExecutionResult:
    """Run ``claimed`` from its last checkpoint to success, retry, or compensation.

    ``lease`` is supplied by the worker, which is also running a background renewer against
    the same object; without one, a lease is created here so the function is usable on its
    own (tests, a one-shot runner) with only the inline renewal defense.

    Raises rather than returns when the workflow cannot be executed at all -- an unregistered
    ``workflow_type``, a database that has gone away. Those leave the row exactly as claimed,
    so its lease expires and a worker that can execute it picks it up. Recording a state
    transition for "this process could not run it" would be a lie about the workflow.
    """
    settings = settings or get_settings()

    # PHASE 1 ONLY -- delete this branch when Phase 2 wires the compensator, and delete
    # storage.workflows.defer_compensation with it.
    #
    # A COMPENSATING row is claimable by the same dequeue query as everything else (that is
    # deliberate: an unwind must not sit out a backoff). Phase 2 owns the unwind, and until it
    # exists this worker is the only thing that ever picks such a row up -- so without this
    # branch it would walk the FORWARD loop again. That re-invokes the step that failed, and a
    # step which fails once and then succeeds would carry the workflow all the way to SUCCESS
    # with its earlier side effects never unwound.
    #
    # Checked before get_definition on purpose, so a COMPENSATING row whose type this build
    # does not import also defers cleanly rather than raising and spinning on its lease.
    if claimed.status == "COMPENSATING":
        return await _defer_compensation(conn_pool, claimed, settings=settings)

    # Deliberately outside the try below: an unknown workflow_type means this build does not
    # import the definition, which is a deployment fact about *us*, not a failure of the
    # workflow. Compensating it here would unwind real money because a worker was stale.
    definition = get_definition(claimed.workflow_type)

    if lease is None:
        lease = Lease(
            conn_pool,
            claimed,
            duration_seconds=settings.lease_duration_seconds,
            renew_divisor=settings.lease_renew_divisor,
        )

    own = workflow_writes.Ownership.of(claimed)
    instance = definition.instantiate()
    completed = await workflow_writes.load_forward_outputs(conn_pool, claimed.id)
    outputs: dict[str, Any] = {}
    running: Step | None = None

    try:
        for step in definition.steps:
            # Membership, never truthiness: a step that committed a null output is done, and
            # re-running it would be a second side effect.
            if step.name in completed:
                outputs[step.name] = completed[step.name]
                log.debug("workflow %s replaying %s from checkpoint", claimed.id, step.name)
                continue

            running = step
            if not await lease.renew_if_needed():
                raise PreemptedError(
                    f"workflow {claimed.id} was re-claimed past fencing token "
                    f"{claimed.fencing_token} before {step.name!r} started"
                )

            ctx = _context_for(claimed, step, outputs, lease)
            started = time.monotonic()
            result = await step.invoke(instance, ctx)
            elapsed = time.monotonic() - started

            if not await _commit_finished_step(
                conn_pool, own, step=step, result=result, elapsed=elapsed
            ):
                raise PreemptedError(
                    f"workflow {claimed.id} was re-claimed past fencing token "
                    f"{claimed.fencing_token} while {step.name!r} was running; its "
                    "checkpoint was rolled back and the new owner will re-run it"
                )
            outputs[step.name] = result

        if not await workflow_writes.finish_success(
            conn_pool, own, output_json=json.dumps(outputs)
        ):
            raise PreemptedError(
                f"workflow {claimed.id} was re-claimed past fencing token "
                f"{claimed.fencing_token} before it could be marked SUCCESS"
            )
        log.info(
            "workflow %s (%s) succeeded on attempt %d", claimed.id, claimed.workflow_type,
            claimed.attempt,
        )
        return ExecutionResult.SUCCESS

    except PreemptedError as exc:
        # Nothing to write and nothing wrong: the row belongs to someone else, who is
        # replaying it from the same checkpoints we were reading.
        log.warning("%s", exc)
        return ExecutionResult.PREEMPTED

    except asyncio.CancelledError:
        # A hard shutdown cancelled us mid-step. Leave the row alone: its lease will expire
        # and another worker resumes from the last checkpoint, which is the recovery path
        # this engine is built on. Marking it failed here would compensate a workflow whose
        # only problem is that this process was told to stop.
        log.warning("workflow %s cancelled mid-execution; leaving it to its lease", claimed.id)
        raise

    except Exception as exc:
        return await _handle_step_failure(
            conn_pool, claimed, own, step=running, exc=exc, settings=settings
        )


async def _defer_compensation(
    conn_pool: asyncpg.Pool,
    claimed: ClaimedWorkflow,
    *,
    settings: Settings,
) -> ExecutionResult:
    """Give a COMPENSATING workflow back to the queue, later. PHASE 1 ONLY.

    WARNING rather than DEBUG, and naming the reason, because a deferred workflow and a stuck
    one look identical from the outside -- COMPENSATING, unowned, going nowhere. Whoever finds
    such a row at 3am should learn why from the logs, not have to infer it from the absence of
    a compensator.

    Reuses :func:`compute_backoff` rather than a fixed delay so repeated deferrals spread out
    with the same jitter as everything else; a fixed one would sync every deferred workflow in
    the system onto the same tick.
    """
    delay = compute_backoff(claimed.attempt, cap_seconds=settings.backoff_cap_seconds)
    log.warning(
        "COMPENSATING workflow %s (%s) claimed by a Phase-1 worker with no compensator; "
        "deferring for %.1fs until Phase 2 wires the unwind (attempt %d)",
        claimed.id,
        claimed.workflow_type,
        delay,
        claimed.attempt,
    )
    if not await workflow_writes.defer_compensation(
        conn_pool, workflow_writes.Ownership.of(claimed), delay_seconds=delay
    ):
        log.warning(
            "workflow %s was re-claimed before its compensation could be deferred", claimed.id
        )
        return ExecutionResult.PREEMPTED
    return ExecutionResult.COMPENSATION_DEFERRED


def _context_for(
    claimed: ClaimedWorkflow,
    step: Step,
    outputs: dict[str, Any],
    lease: Lease,
) -> StepContext:
    """Everything the step is allowed to know about the attempt it is running in.

    ``outputs`` is wrapped read-only. The dict is the loop's own progress record, and a step
    that wrote into it would change what a *later* step reads without that ever reaching
    ``step_outputs`` -- so the workflow would behave one way on a clean run and another on
    the replay after a crash, which is the exact divergence this engine exists to rule out.
    """
    return StepContext(
        workflow_id=claimed.id,
        input=claimed.input,
        outputs=MappingProxyType(outputs),
        fencing_token=claimed.fencing_token,
        attempt=claimed.attempt,
        owner_id=claimed.owner_id,
        step_name=step.name,
        renew_lease_callback=lease.renew_or_raise,
    )


async def _commit_finished_step(
    conn_pool: asyncpg.Pool,
    own: workflow_writes.Ownership,
    *,
    step: Step,
    result: StepOutput,
    elapsed: float,
) -> bool:
    """Checkpoint a step that has already run -- and finish that write even if cancelled.

    By the time this is called the step's side effect has happened. All that is left is
    writing it down, and a cancellation landing in that gap is the one case where losing the
    race actually costs something: the checkpoint never appears, so the resume re-executes a
    step that already moved money. Until a step is idempotent by construction (Phase 2, a
    natural key plus ``ON CONFLICT DO NOTHING``) that is a real double-execution, not a
    theoretical one.

    So the commit is shielded and then explicitly awaited on the way out. Shielding alone is
    not enough: it detaches the write but lets this coroutine unwind immediately, and the
    worker's drain would then return -- and close the pool -- with the write still in flight.
    Awaiting ``commit`` in the cancellation handler is what makes the drain wait for it,
    because the wait happens inside the task the drain is already gathering.

    This narrows the window rather than closing it. A SIGKILL, a lost connection, or a
    cancellation arriving *during* the commit still leaves the step uncheckpointed, and the
    engine's answer to that is unchanged: execution is at-least-once and steps must be
    idempotent. What this buys is that an orderly shutdown no longer throws away work it had
    already finished.
    """
    commit = asyncio.ensure_future(
        workflow_writes.commit_step_output(
            conn_pool,
            own,
            step_name=step.name,
            seq=step.seq,
            # Evaluated before the future exists, so a value that cannot be encoded raises
            # here rather than leaving a detached write to shield.
            output_json=_encode_output(step, result),
            duration_seconds=elapsed,
        )
    )
    try:
        return await asyncio.shield(commit)
    except asyncio.CancelledError:
        try:
            committed = await commit
        except Exception:
            log.exception(
                "cancelled while checkpointing %r, and the write did not land; the step will "
                "re-run when another worker resumes this workflow",
                step.name,
            )
        else:
            if committed:
                log.info(
                    "cancelled during %r, but its checkpoint committed -- the resume will "
                    "replay it rather than re-run it",
                    step.name,
                )
            else:
                log.warning(
                    "cancelled during %r, and its checkpoint was rejected: this worker had "
                    "already been preempted",
                    step.name,
                )
        raise


def _encode_output(step: Step, result: StepOutput) -> str:
    """Serialise a step's return value for ``step_outputs.output`` (JSONB).

    Bound as text with an explicit ``::jsonb`` cast rather than as a Python object, so the
    write does not depend on a json codec being registered on a pool this module was handed
    and does not own.
    """
    try:
        return json.dumps(result)
    except (TypeError, ValueError) as exc:
        # The side effect has already happened; only the checkpoint can still fail. Terminal
        # rather than retryable, because every retry would re-run the step and fail here
        # again in exactly the same way.
        raise TerminalError(
            f"step {step.name!r} returned {type(result).__name__}, which is not "
            f"JSON-serialisable ({exc}). A step output is persisted to step_outputs.output "
            "and handed back to the step's compensation on replay, possibly in another "
            "process -- return plain JSON types"
        ) from exc


async def _handle_step_failure(
    conn_pool: asyncpg.Pool,
    claimed: ClaimedWorkflow,
    own: workflow_writes.Ownership,
    *,
    step: Step | None,
    exc: Exception,
    settings: Settings,
) -> ExecutionResult:
    """The branch that decides whether money is retried or unwound.

    ``step`` is None only if the failure happened before any step began -- loading the
    checkpoints, say. There is no step policy to consult then, and no side effect to undo
    either, but the workflow has no way to distinguish that from a step failure, so it takes
    the safe branch and compensates. A compensation with nothing checkpointed to unwind is a
    no-op; a wrong retry is not.
    """
    attempts_left = claimed.attempt < claimed.max_attempts
    retryable = step is not None and step.is_retryable(exc)
    where = f"step {step.name!r}" if step is not None else "execution"
    error = _describe(where, exc)

    if retryable and attempts_left:
        delay = compute_backoff(claimed.attempt, cap_seconds=settings.backoff_cap_seconds)
        log.warning(
            "workflow %s: %s failed retryably on attempt %d/%d, retrying in %.1fs: %s",
            claimed.id, where, claimed.attempt, claimed.max_attempts, delay, exc,
        )
        if not await workflow_writes.schedule_retry(
            conn_pool, own, error=error, delay_seconds=delay
        ):
            log.warning("workflow %s was re-claimed before its retry could be scheduled",
                        claimed.id)
            return ExecutionResult.PREEMPTED
        return ExecutionResult.RETRY_SCHEDULED

    if retryable:
        reason = f"retries exhausted after {claimed.attempt}/{claimed.max_attempts} attempts"
    else:
        reason = "failure is terminal"
    log.error(
        "workflow %s: %s failed on attempt %d, compensating (%s): %s",
        claimed.id, where, claimed.attempt, reason, exc, exc_info=exc,
    )
    if not await workflow_writes.begin_compensation(conn_pool, own, error=error):
        log.warning("workflow %s was re-claimed before compensation could be started",
                    claimed.id)
        return ExecutionResult.PREEMPTED
    return ExecutionResult.COMPENSATING


def _describe(where: str, exc: Exception) -> str:
    """The one-line error stored on the workflow row.

    Carries the exception's *type* as well as its message: whether a failure was a
    ``RetryableError`` or something the step never classified is the first thing anyone asks
    when a workflow lands in COMPENSATING, and the message alone rarely says.
    """
    detail = f"{where}: {type(exc).__name__}: {exc}"
    if len(detail) <= _MAX_ERROR_CHARS:
        return detail
    return detail[: _MAX_ERROR_CHARS - 3] + "..."
