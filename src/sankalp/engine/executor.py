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
    anything else                  ->  COMPENSATING, released, unwound on its next claim

Note which way the default points. An exception the step did not classify is treated as
terminal, because an unrecognised failure is not evidence that re-running the step is safe,
while compensation is idempotent by contract.

**A COMPENSATING claim runs backwards, never forwards.** Nothing in the schema distinguishes
the two -- an unwind is claimable by the same dequeue query, deliberately with no backoff, and
arrives here as an ordinary :class:`ClaimedWorkflow`. So :func:`execute_workflow` dispatches on
``claimed.status`` before it touches a step: COMPENSATING goes to :func:`_compensate`, which
walks the committed checkpoints in reverse ``seq`` and reverses each one. Lose that dispatch
and a failed saga is re-executed forward on every claim, and one whose failing step happens to
succeed on a later attempt is carried all the way to SUCCESS with its committed side effects
never unwound. ``_FINISH_SUCCESS_SQL``'s ``AND status = 'RUNNING'`` is the second lock on that
same door; keep both.

There is one COMPENSATING claim :func:`_compensate` never sees: a workflow whose type this
build does not import, so there is no definition and therefore no list of compensations to
run. :func:`_defer_compensation` hands that row straight back to the queue. It is a small,
narrow fallback about *this worker's* deployment, not a way of putting an unwind off -- do not
confuse the two, and do not route ordinary unwinds through it.

The unwind mirrors the forward loop rather than inventing a second set of rules. A compensation
is done if and only if a ``step_outputs`` row exists for it with ``kind = 'COMPENSATION'``,
committed in one transaction with the workflow's position, ownership-guarded, cancellation-
shielded. What differs is only what it does when a compensation *fails*: there is nowhere left
to fall back to, so it retries in place a bounded number of times
(``settings.compensation_max_attempts``, counted in memory -- ``workflows.attempt`` keeps its
forward history) and then writes FAILED_DIRTY and stops, leaving the remaining steps
un-reversed for a human. Reverse ``seq`` is a dependency order; carrying on past a failure in
it can make the mess worse rather than smaller.
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
from sankalp.engine.definition import (
    Step,
    StepContext,
    StepOutput,
    WorkflowDefinition,
    WorkflowInstance,
    get_definition,
)
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
    #: A forward run failed terminally and handed the workflow to the unwind. The row is
    #: COMPENSATING and immediately claimable; the unwind happens on that next claim, not here.
    COMPENSATING = "COMPENSATING"
    #: An unwind ran to completion: every compensable step has a committed COMPENSATION row.
    COMPENSATED = "COMPENSATED"
    #: An unwind could not finish. Money is in an inconsistent state and a human must resolve
    #: it -- this is the result to alert on.
    FAILED_DIRTY = "FAILED_DIRTY"
    PREEMPTED = "PREEMPTED"
    #: Claimed an unwind whose ``workflow_type`` this build cannot resolve, and gave it back.
    #: Like PREEMPTED, this names what this *worker* did, not a state the row is in -- the row
    #: is COMPENSATING before and after. Not a failure, and not a deferral of the unwind in
    #: general: a worker that *can* read the type runs it on the next claim.
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

    # The dispatch. A COMPENSATING row arrives through the same dequeue query as everything
    # else and looks identical here, so this is the only thing that keeps a failed saga from
    # being replayed forward -- see the module docstring. It must stay ahead of any step
    # invocation.
    #
    # It resolves the definition itself rather than sharing the forward path's call below,
    # because the two want opposite things from an unresolvable workflow_type. A forward run
    # raises and keeps its claim: the row is PENDING or RUNNING, its lease expires, and it
    # waits. An unwind cannot do that -- begin_compensation set run_after = now() so it never
    # sits out a backoff, so this same ignorant worker would re-claim it a lease later and
    # every lease after that. It hands the row back instead.
    if claimed.status == "COMPENSATING":
        try:
            definition = get_definition(claimed.workflow_type)
        except KeyError as exc:
            return await _defer_compensation(conn_pool, claimed, exc, settings=settings)
        return await _compensate(
            conn_pool,
            claimed,
            definition,
            _lease_for(conn_pool, claimed, lease, settings),
            settings=settings,
        )

    # Deliberately outside the try below: an unknown workflow_type means this build does not
    # import the definition, which is a deployment fact about *us*, not a failure of the
    # workflow. Compensating it here would unwind real money because a worker was stale.
    definition = get_definition(claimed.workflow_type)
    lease = _lease_for(conn_pool, claimed, lease, settings)

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


def _lease_for(
    conn_pool: asyncpg.Pool,
    claimed: ClaimedWorkflow,
    lease: Lease | None,
    settings: Settings,
) -> Lease:
    """The worker's lease, or a standalone one so this module is usable without a worker."""
    if lease is not None:
        return lease
    return Lease(
        conn_pool,
        claimed,
        duration_seconds=settings.lease_duration_seconds,
        renew_divisor=settings.lease_renew_divisor,
    )


async def _defer_compensation(
    conn_pool: asyncpg.Pool,
    claimed: ClaimedWorkflow,
    exc: KeyError,
    *,
    settings: Settings,
) -> ExecutionResult:
    """Give an unwind back because this build cannot read its ``workflow_type``.

    The narrow case, and the only one: see
    :func:`sankalp.storage.workflows.defer_compensation` for why handing the row back beats
    raising, failing dirty, or guessing. This is **not** a general "unwind later" path --
    :func:`_compensate` runs the unwind, and every COMPENSATING claim a worker can actually
    resolve goes there.

    WARNING rather than DEBUG, and naming the type, because a deferred workflow and a stuck
    one look identical from the outside -- COMPENSATING, unowned, going nowhere. Whoever finds
    such a row at 3am should learn from the logs that some worker could not read its type,
    not have to infer it. If every worker in the fleet logs this, the definition is missing
    from the deployment and the saga is genuinely stalled.

    Reuses :func:`compute_backoff` rather than a fixed delay so repeated deferrals spread out
    with the same jitter as everything else; a fixed one would sync every deferred workflow in
    the system onto the same tick.
    """
    delay = compute_backoff(claimed.attempt, cap_seconds=settings.backoff_cap_seconds)
    log.warning(
        "COMPENSATING workflow %s claimed by a worker that cannot resolve its type %r "
        "(%s); handing it back for %.1fs so a worker that imports the definition can unwind "
        "it (attempt %d)",
        claimed.id,
        claimed.workflow_type,
        exc,
        delay,
        claimed.attempt,
    )
    if not await workflow_writes.defer_compensation(
        conn_pool, workflow_writes.Ownership.of(claimed), delay_seconds=delay
    ):
        log.warning(
            "workflow %s was re-claimed before its compensation could be handed back",
            claimed.id,
        )
        return ExecutionResult.PREEMPTED
    return ExecutionResult.COMPENSATION_DEFERRED


async def _compensate(
    conn_pool: asyncpg.Pool,
    claimed: ClaimedWorkflow,
    definition: WorkflowDefinition,
    lease: Lease,
    *,
    settings: Settings,
) -> ExecutionResult:
    """Reverse a failed saga: run each completed step's compensation, newest first.

    The whole flow is docs/spec.md, "Compensation Model", and it is the forward loop read
    backwards. One query loads what already happened -- the committed FORWARD checkpoints in
    reverse ``seq`` order, and the set of steps already undone -- and from there the same
    three properties hold as on the way in. A compensation is skipped if a row exists for it;
    it and its checkpoint commit together; nothing is written without proving ownership.

    Two things are skipped rather than run: a step already in ``compensated`` (a resume after
    a crash mid-unwind) and a step with no compensation declared at all, which is a read-only
    step -- a balance check, a fraud lookup -- with nothing to undo.

    A compensation that cannot be made to succeed ends the unwind at that step, in
    FAILED_DIRTY. The steps below it are deliberately left alone: reverse ``seq`` is a
    dependency order, so an undo whose ordering precondition has just been violated can make
    the inconsistency worse rather than smaller. The committed COMPENSATION rows say exactly
    how far it got, which is what the human resolving it needs.
    """
    own = workflow_writes.Ownership.of(claimed)
    instance = definition.instantiate()
    forward, compensated = await workflow_writes.load_unwind_state(conn_pool, claimed.id)
    # Every committed forward output, so a compensation can read a sibling step's result as
    # well as its own -- read-only, for the reason in _context_for.
    outputs: dict[str, Any] = {record.step_name: record.output for record in forward}

    try:
        for record in forward:
            # Membership, never truthiness: a compensation row carries a NULL output, so a
            # truthiness test here would re-run every undo in the workflow.
            if record.step_name in compensated:
                log.debug(
                    "workflow %s: %r is already compensated, skipping",
                    claimed.id, record.step_name,
                )
                continue

            try:
                step = definition.step_by_name(record.step_name)
            except KeyError as exc:
                # The definition changed under an in-flight saga. There is a committed side
                # effect whose undo this build cannot even name, so there is no honest state
                # other than "a human must look at this".
                return await _fail_dirty(
                    conn_pool, own, claimed, error=_describe("unwind", exc), exc=exc
                )

            if step.compensation is None:
                log.debug(
                    "workflow %s: %r declares no compensation (read-only), skipping",
                    claimed.id, step.name,
                )
                continue

            if not await lease.renew_if_needed():
                raise PreemptedError(
                    f"workflow {claimed.id} was re-claimed past fencing token "
                    f"{claimed.fencing_token} before {step.name!r} could be compensated"
                )

            ctx = _context_for(claimed, step, outputs, lease)
            started = time.monotonic()
            failure = await _run_compensation(
                instance, step, ctx, record.output, lease, settings=settings
            )
            elapsed = time.monotonic() - started

            if failure is not None:
                return await _fail_dirty(
                    conn_pool,
                    own,
                    claimed,
                    error=_describe(f"compensation for step {step.name!r}", failure),
                    exc=failure,
                )

            if not await _commit_compensation(conn_pool, own, step=step, elapsed=elapsed):
                raise PreemptedError(
                    f"workflow {claimed.id} was re-claimed past fencing token "
                    f"{claimed.fencing_token} while {step.name!r} was being compensated; its "
                    "checkpoint was rolled back and the new owner will re-run the undo"
                )
            log.info("workflow %s: compensated %r", claimed.id, step.name)

        if not await workflow_writes.finish_compensated(conn_pool, own):
            raise PreemptedError(
                f"workflow {claimed.id} was re-claimed past fencing token "
                f"{claimed.fencing_token} before it could be marked COMPENSATED"
            )
        log.info(
            "workflow %s (%s) fully compensated", claimed.id, claimed.workflow_type
        )
        return ExecutionResult.COMPENSATED

    except PreemptedError as exc:
        # Same as on the way in: the row belongs to someone else, who is resuming the unwind
        # from the same COMPENSATION checkpoints we were reading. Write nothing.
        log.warning("%s", exc)
        return ExecutionResult.PREEMPTED

    except asyncio.CancelledError:
        # A hard shutdown cancelled us mid-unwind. Leave the row COMPENSATING: its lease
        # expires, another worker claims it, and it resumes at the first step without a
        # COMPENSATION row. Marking it FAILED_DIRTY here would page a human because this
        # process was told to stop.
        log.warning(
            "workflow %s cancelled mid-compensation; leaving it to its lease", claimed.id
        )
        raise


async def _run_compensation(
    instance: WorkflowInstance,
    step: Step,
    ctx: StepContext,
    forward_output: StepOutput,
    lease: Lease,
    *,
    settings: Settings,
) -> Exception | None:
    """Run one undo until it works or the budget runs out. Returns the last failure, or None.

    The retry policy here is the **inverse** of the forward one, and deliberately so. A
    forward step that raises something it did not classify is treated as terminal, because an
    unrecognised failure is not evidence that re-running it is safe -- it may already have
    moved money. A compensation is idempotent by contract (docs/spec.md: ``refund_if_not_
    already_refunded``, not ``refund``), so re-running one *is* safe, and the expensive
    mistake is the other way round: giving up on a transient failure strands a saga in
    FAILED_DIRTY and pages someone for a downstream blip that cleared a second later.

    So everything is retried except an explicit ``TerminalError``, which is the compensation
    itself saying that waiting will not help, and ``PreemptedError``, which is not a failure
    of the undo at all -- it means we no longer own the workflow and must write nothing.
    ``asyncio.CancelledError`` propagates for the same reason (it is a BaseException and is
    not caught below).

    The budget is counted here, in memory, rather than on ``workflows.attempt``: that column
    is the forward run's history and stays intact, and counting in memory means the number
    measures *compensation failures* -- a crash mid-unwind gives the resuming worker a fresh
    budget instead of spending someone else's.
    """
    attempts = settings.compensation_max_attempts
    last: Exception | None = None

    for attempt in range(1, attempts + 1):
        try:
            await step.invoke_compensation(instance, ctx, forward_output)
            return None
        except PreemptedError:
            raise
        except TerminalError as exc:
            log.error(
                "workflow %s: compensation for %r failed terminally on attempt %d/%d; "
                "retrying would fail the same way: %s",
                ctx.workflow_id, step.name, attempt, attempts, exc,
            )
            return exc
        except Exception as exc:
            last = exc
            if attempt == attempts:
                break
            delay = compute_backoff(attempt, cap_seconds=settings.backoff_cap_seconds)
            log.warning(
                "workflow %s: compensation for %r failed on attempt %d/%d, retrying in "
                "%.1fs: %s",
                ctx.workflow_id, step.name, attempt, attempts, delay, exc,
            )
            await asyncio.sleep(delay)
            # Renew after the sleep, not before it: the backoff is where this coroutine spends
            # nearly all of its time, and a lease that was healthy going in may not be coming
            # out. Losing it here means the next attempt would run an undo whose checkpoint
            # the ownership guard is already certain to reject.
            if not await lease.renew_if_needed():
                raise PreemptedError(
                    f"workflow {ctx.workflow_id} was re-claimed past fencing token "
                    f"{ctx.fencing_token} while {step.name!r}'s compensation was backing off"
                ) from exc

    return last


async def _fail_dirty(
    conn_pool: asyncpg.Pool,
    own: workflow_writes.Ownership,
    claimed: ClaimedWorkflow,
    *,
    error: str,
    exc: Exception,
) -> ExecutionResult:
    """Park a workflow whose unwind could not finish, and say so loudly.

    ERROR, with the exception attached, because this is the one status in the schema that
    means *stop and fetch a person*: a side effect is committed, the engine has exhausted what
    it can do about that, and nothing will retry it. A FAILED_DIRTY row that nobody was told
    about is indistinguishable from money quietly going missing.
    """
    log.error(
        "workflow %s (%s) is FAILED_DIRTY: %s. Committed side effects have NOT been fully "
        "reversed -- the COMPENSATION rows in step_outputs show how far the unwind got, and "
        "a human must resolve the rest",
        claimed.id,
        claimed.workflow_type,
        error,
        exc_info=exc,
    )
    if not await workflow_writes.fail_dirty(conn_pool, own, error=error):
        log.warning(
            "workflow %s was re-claimed before it could be marked FAILED_DIRTY", claimed.id
        )
        return ExecutionResult.PREEMPTED
    return ExecutionResult.FAILED_DIRTY


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
    step that already moved money. Until a step is idempotent by construction (a natural key
    plus ``ON CONFLICT DO NOTHING``) that is a real double-execution, not a theoretical one.
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
    return await _commit_shielded(commit, f"step {step.name!r}")


async def _commit_compensation(
    conn_pool: asyncpg.Pool,
    own: workflow_writes.Ownership,
    *,
    step: Step,
    elapsed: float,
) -> bool:
    """Checkpoint a compensation that has already run. The undo's half of the pair above.

    Shielded for the same reason and with the same honesty about what it buys: the undo has
    happened, only the record of it is outstanding, and losing that record means the undo runs
    again on the resume. Compensations are required to be idempotent precisely because that
    window cannot be closed -- this only stops an *orderly* shutdown from walking into it.
    """
    commit = asyncio.ensure_future(
        workflow_writes.commit_compensation_output(
            conn_pool,
            own,
            step_name=step.name,
            seq=step.seq,
            duration_seconds=elapsed,
        )
    )
    return await _commit_shielded(commit, f"compensation for step {step.name!r}")


async def _commit_shielded(commit: asyncio.Future[bool], what: str) -> bool:
    """Await a checkpoint write so that a cancellation cannot abandon it half-issued.

    The commit is shielded and then explicitly awaited on the way out. Shielding alone is not
    enough: it detaches the write but lets this coroutine unwind immediately, and the worker's
    drain would then return -- and close the pool -- with the write still in flight. Awaiting
    ``commit`` in the cancellation handler is what makes the drain wait for it, because the
    wait happens inside the task the drain is already gathering.

    This narrows the window rather than closing it. A SIGKILL, a lost connection, or a
    cancellation arriving *during* the commit still leaves the work uncheckpointed, and the
    engine's answer to that is unchanged: execution is at-least-once and both steps and
    compensations must be idempotent. What it buys is that an orderly shutdown no longer
    throws away work it had already finished.
    """
    try:
        return await asyncio.shield(commit)
    except asyncio.CancelledError:
        try:
            committed = await commit
        except Exception:
            log.exception(
                "cancelled while checkpointing %s, and the write did not land; it will run "
                "again when another worker resumes this workflow",
                what,
            )
        else:
            if committed:
                log.info(
                    "cancelled during %s, but its checkpoint committed -- the resume will "
                    "skip it rather than re-run it",
                    what,
                )
            else:
                log.warning(
                    "cancelled during %s, and its checkpoint was rejected: this worker had "
                    "already been preempted",
                    what,
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
