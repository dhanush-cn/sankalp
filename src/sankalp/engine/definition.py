"""The workflow definition API: ``@workflow``, ``@step``, ``@<step>.compensate``.

A workflow definition is a class whose methods are the ordered steps::

    @workflow("payment_transfer")
    class PaymentTransfer:
        @step(seq=1)
        async def debit_wallet(self, ctx: StepContext) -> dict[str, Any]:
            ...

        @debit_wallet.compensate
        async def undo_debit(self, ctx: StepContext, forward_output: dict[str, Any]) -> None:
            ...

The definition is *only* a declaration -- it holds no connection, opens no transaction and
knows nothing about leases. The worker loop walks ``definition.steps`` in ``seq`` order,
skips the ones that already have a ``step_outputs`` row, and commits each output with the
transition that follows it (docs/spec.md, "Execution Flow"). Keeping the two apart is what
lets the crash tests drive the engine over a definition that is a few lines of test code.

Two decisions worth stating, because they are the ones that would be easy to undo:

``seq`` is written down rather than inferred from source order. It is persisted in
``step_outputs.seq`` and it is the order compensations unwind in (``ORDER BY seq DESC``),
so it has to be stable against someone reordering methods or a mixin inserting one. It is
validated at registration -- unique and contiguous from 1 -- so a deleted step is a startup
failure instead of a gap that silently reorders an unwind.

A step's ``name`` is likewise explicit-able and defaults to the method name, because the
name is a primary key column (``workflow_id, step_name, kind``). Renaming a method whose
name reached production would orphan every in-flight checkpoint under the old name and
re-execute the step; pass ``name=`` to keep the persisted identity while renaming the code.
"""

from __future__ import annotations

import inspect
import json
from collections.abc import Awaitable, Callable, Iterator, Mapping
from dataclasses import dataclass, field
from functools import cached_property
from types import MethodType
from typing import Any, TypeVar
from uuid import UUID

from sankalp.engine.errors import RetryableError, TerminalError, WorkflowDefinitionError
from sankalp.storage.outbox import PendingEvent

__all__ = [
    "StepContext",
    "Step",
    "WorkflowDefinition",
    "step",
    "workflow",
    "get_definition",
    "registered_types",
    "clear_registry",
]

#: Whatever a step returns. Genuinely unknown to the engine -- it round-trips through
#: ``step_outputs.output`` (JSONB) and only the step and its own compensation know its
#: shape. Named rather than written as a bare ``Any`` so it reads as "the step decides"
#: rather than as an annotation someone forgot to finish.
StepOutput = Any

#: An instance of a user-defined workflow class. The engine calls nothing on it directly;
#: it only passes it back as ``self`` to the step functions the definition holds.
WorkflowInstance = Any

#: A forward step: ``async def (self, ctx) -> output``. The output is JSON-serialisable
#: because it lands in ``step_outputs.output`` (JSONB) and is handed back to the step's
#: compensation on replay -- possibly in a different process, hours later.
StepFunc = Callable[[Any, "StepContext"], Awaitable[Any]]

#: A compensation: ``async def (self, ctx, forward_output) -> None``. It receives the
#: output the forward step committed, which is what makes an unwind possible after a crash
#: that lost every scrap of in-memory state.
CompensationFunc = Callable[[Any, "StepContext", Any], Awaitable[None]]

WorkflowClass = TypeVar("WorkflowClass", bound=type)


@dataclass(frozen=True, slots=True)
class StepContext:
    """Everything a step is allowed to know about the attempt it is running in.

    ``outputs`` holds the committed outputs of the *prior* steps of this workflow, keyed by
    step name -- read back from ``step_outputs``, not carried in memory. On a replay after a
    crash the values are identical to what the original process saw, which is the property
    that makes resuming mid-workflow indistinguishable from never having stopped.

    ``fencing_token`` is passed through to every ownership-scoped write (``WHERE id = $1 AND
    owner_id = $2 AND fencing_token = $3``). A step that talks to a resource guard hands it
    over so a stalled worker holding an older token cannot act after being preempted.

    ``emit`` buffers events that the engine writes in the *same transaction* as this step's
    checkpoint -- see :meth:`emit`. One context is built per attempt, and the commit that
    consumes the buffer empties it.
    """

    workflow_id: UUID
    input: Mapping[str, Any]
    outputs: Mapping[str, Any]
    fencing_token: int
    attempt: int
    owner_id: str | None = None
    step_name: str | None = None

    #: Supplied by the worker: extends this workflow's lease and resolves once the row is
    #: actually stamped. Not part of the public surface -- steps call :meth:`renew_lease`.
    renew_lease_callback: Callable[[], Awaitable[None]] | None = field(
        default=None, repr=False, compare=False
    )

    #: Events emitted by this attempt, waiting for its checkpoint transaction. A mutable list
    #: on a frozen dataclass is fine and intended: the attribute is never rebound, only the
    #: list it points at is appended to and cleared.
    #:
    #: Excluded from ``repr`` and ``compare`` for the same reason as the callback above -- it
    #: is engine plumbing, not part of what a context *is*, and two contexts do not stop being
    #: equal because one of them has emitted something.
    _pending_events: list[PendingEvent] = field(
        default_factory=list, repr=False, compare=False
    )

    def emit(self, event_type: str, payload: Mapping[str, Any] | None = None) -> None:
        """Buffer an event to be written in the SAME transaction as this step's checkpoint.

        This is the transactional outbox, and the atomicity is the entire point: the engine
        does not publish anything here and does not talk to a broker at all. It appends the
        event to ``outbox`` in the one transaction that also writes ``step_outputs`` and moves
        the workflow's position, so the event and the state change that caused it become true
        together or not at all. A separate drain loop moves committed rows to Redis afterwards.

        Nothing is written if the step goes on to fail, or if this worker turns out to have
        been preempted -- the checkpoint transaction rolls back and takes the events with it.
        That is what stops an event describing a step whose effects were never recorded.

        The payload is serialised **now**, so a value that cannot be encoded raises here, with
        a traceback pointing at the step that emitted it. Deferring it to commit time would
        surface the failure inside a shielded write with no indication of which buffered event
        was the bad one. Terminal rather than retryable for the same reason as
        ``executor._encode_output``: a retry re-encodes the same object and fails identically.
        """
        try:
            payload_json = json.dumps(dict(payload) if payload is not None else {})
        except (TypeError, ValueError) as exc:
            raise TerminalError(
                f"event {event_type!r} emitted by step {self.step_name!r} of workflow "
                f"{self.workflow_id} has a payload that is not JSON-serialisable ({exc}). "
                "An event payload is persisted to outbox.payload (JSONB) and read by a "
                "different process later -- emit plain JSON types"
            ) from exc
        self._pending_events.append(
            # trace_context stays None until Phase 4 wires the W3C traceparent through
            # (docs/spec.md, "Trace Context Propagation"). The column and this field exist so
            # that work is a change at this one line rather than a schema migration.
            PendingEvent(event_type=event_type, payload_json=payload_json)
        )

    def take_pending_events(self) -> list[PendingEvent]:
        """Hand over the buffered events and empty the buffer. Called by the engine, once.

        Emptying is what makes double-emission unrepresentable rather than merely unlikely.
        The buffer is reachable no other way, so a second commit against this context can only
        ever find it empty -- and that stays true even if some later refactor hoists the
        per-attempt ``_context_for`` call out of the executor's loop, which is exactly the
        edit that would otherwise start shipping every event twice.
        """
        taken = list(self._pending_events)
        self._pending_events.clear()
        return taken

    def discard_pending_events(self) -> None:
        """Throw away anything buffered so far, without committing it.

        For a retry that reuses this context: the unwind runs a compensation up to
        ``compensation_max_attempts`` times against one ``StepContext``, and events buffered by
        an attempt that then failed must not ride along with the attempt that succeeds.
        """
        self._pending_events.clear()

    async def renew_lease(self) -> None:
        """Push this workflow's lease further out; call it from inside a long step.

        The worker renews on a timer every ``lease_duration / 3`` while a step runs, so
        most steps never need this. A step that does something slow in one uninterruptible
        chunk -- a big batch, a downstream that blocks -- can call it at its own
        checkpoints as well; renewing twice is harmless.

        Outside a worker (a unit test constructing a context by hand) there is no lease to
        extend and this does nothing, rather than failing a step for the sake of a fixture.
        """
        if self.renew_lease_callback is not None:
            await self.renew_lease_callback()

    def output_of(self, step_name: str) -> StepOutput:
        """Output committed by an earlier step, or a loud error naming what is available.

        ``ctx.outputs[name]`` raises a bare ``KeyError`` that reads identically whether the
        step is misspelled or genuinely has not run yet; those are very different bugs.
        """
        try:
            return self.outputs[step_name]
        except KeyError:
            available = ", ".join(sorted(self.outputs)) or "none"
            raise KeyError(
                f"step {step_name!r} has no committed output for workflow {self.workflow_id}; "
                f"completed so far: {available}. A step can only read steps with a lower seq."
            ) from None


class Step:
    """One forward step, its optional compensation, and its error policy.

    Also a descriptor: ``self.debit_wallet(ctx)`` inside another method calls the function,
    while ``PaymentTransfer.debit_wallet`` gives back this object so the registry and the
    tests can introspect it. Without ``__get__`` the decorator would replace the method with
    something instances cannot call, which is a nasty surprise for anyone writing a
    definition.
    """

    __slots__ = (
        "seq",
        "name",
        "func",
        "compensation",
        "retry_on",
        "terminal_on",
        "_explicit_name",
    )

    def __init__(
        self,
        *,
        seq: int,
        func: StepFunc,
        name: str | None = None,
        retry_on: tuple[type[BaseException], ...] = (),
        terminal_on: tuple[type[BaseException], ...] = (),
    ) -> None:
        self.seq = seq
        self._explicit_name = name is not None
        self.name = name or func.__name__
        self.func = func
        self.compensation: CompensationFunc | None = None
        self.retry_on = retry_on
        self.terminal_on = terminal_on

    # -- descriptor -------------------------------------------------------------------

    def __set_name__(self, owner: type, attr: str) -> None:
        # The attribute name, not the function name, is the default -- they differ when a
        # step is declared via an alias. An explicit name= always wins: overriding it here
        # would defeat the one reason to pass one (renaming code without orphaning
        # checkpoints written under the old step_name).
        if not self._explicit_name:
            self.name = attr

    def __get__(
        self, obj: WorkflowInstance | None, objtype: type | None = None
    ) -> Step | Callable[[StepContext], Awaitable[StepOutput]]:
        if obj is None:
            return self
        return MethodType(self.func, obj)

    def __repr__(self) -> str:
        comp = self.compensation.__name__ if self.compensation else None
        return f"<Step seq={self.seq} name={self.name!r} compensation={comp!r}>"

    # -- declaration ------------------------------------------------------------------

    def compensate(self, func: CompensationFunc) -> CompensationFunc:
        """Attach the undo for this step. Used as ``@<step>.compensate``.

        Compensations are optional: a read-only step (a balance check, a fraud lookup) has
        nothing to undo and the unwind skips it. Returns the function unchanged so the
        compensation stays an ordinary callable method rather than a second Step -- it is
        reached through ``step.compensation``, never scheduled on its own.

        The contract the engine relies on (docs/spec.md, "Compensation Model"): the
        compensation must be idempotent, because a crash between running it and committing
        its ``step_outputs`` row means it runs again on replay; and it must not fail
        permanently, because exhausting its retries lands the workflow in ``FAILED_DIRTY``
        and pages a human.
        """
        _require_coroutine_function(func, f"compensation for step {self.name!r}")
        if self.compensation is not None:
            raise WorkflowDefinitionError(
                f"step {self.name!r} already has compensation "
                f"{self.compensation.__name__!r}; a step unwinds exactly one way"
            )
        self.compensation = func
        return func

    # -- error policy -----------------------------------------------------------------

    def is_retryable(self, exc: BaseException) -> bool:
        """Whether ``exc`` from this step should go back on the queue.

        Precedence: the step's own ``terminal_on``, then its ``retry_on``, then the
        ``TerminalError`` / ``RetryableError`` marker classes, then terminal. Declared types
        beat the markers so a step can call a shared helper that raises ``RetryableError``
        and still classify one specific subclass of it as fatal for its own use.
        """
        if self.terminal_on and isinstance(exc, self.terminal_on):
            return False
        if self.retry_on and isinstance(exc, self.retry_on):
            return True
        if isinstance(exc, TerminalError):
            return False
        return isinstance(exc, RetryableError)

    # -- invocation -------------------------------------------------------------------

    async def invoke(self, instance: WorkflowInstance, ctx: StepContext) -> StepOutput:
        """Run the forward step. The caller commits the output; this does not."""
        return await self.func(instance, ctx)

    async def invoke_compensation(
        self, instance: WorkflowInstance, ctx: StepContext, forward_output: StepOutput
    ) -> None:
        """Run the undo with the output the forward step committed."""
        if self.compensation is None:
            raise WorkflowDefinitionError(
                f"step {self.name!r} has no compensation; the unwind must skip it "
                "rather than call this"
            )
        await self.compensation(instance, ctx, forward_output)


def step(
    *,
    seq: int,
    name: str | None = None,
    retry_on: type[BaseException] | tuple[type[BaseException], ...] = (),
    terminal_on: type[BaseException] | tuple[type[BaseException], ...] = (),
) -> Callable[[StepFunc], Step]:
    """Declare a forward step at position ``seq`` (1-based, contiguous).

    ``retry_on`` / ``terminal_on`` let a step classify exceptions it does not raise itself
    -- an ``asyncio.TimeoutError`` from a client library, say -- without wrapping every call
    in a try/except that re-raises a marker. Anything unclassified is terminal; see
    :mod:`sankalp.engine.errors` for why that default points the way it does.
    """
    retryable = _as_exception_tuple(retry_on, "retry_on")
    terminal = _as_exception_tuple(terminal_on, "terminal_on")
    overlap = {e.__name__ for e in retryable} & {e.__name__ for e in terminal}
    if overlap:
        raise WorkflowDefinitionError(
            f"{', '.join(sorted(overlap))} is in both retry_on and terminal_on; "
            "the classification would depend on precedence rather than on intent"
        )
    if seq < 1:
        raise WorkflowDefinitionError(f"seq must be >= 1, got {seq}")

    def decorate(func: StepFunc) -> Step:
        _require_coroutine_function(func, f"step {name or func.__name__!r}")
        return Step(seq=seq, func=func, name=name, retry_on=retryable, terminal_on=terminal)

    return decorate


@dataclass(frozen=True)
class WorkflowDefinition:
    """A registered workflow: its type string, its class, and its steps in ``seq`` order."""

    workflow_type: str
    cls: type
    steps: tuple[Step, ...]

    @cached_property
    def _by_name(self) -> dict[str, Step]:
        return {s.name: s for s in self.steps}

    def __iter__(self) -> Iterator[Step]:
        return iter(self.steps)

    def __len__(self) -> int:
        return len(self.steps)

    @property
    def step_names(self) -> tuple[str, ...]:
        return tuple(s.name for s in self.steps)

    def step_by_name(self, name: str) -> Step:
        """Look up a step by its persisted name.

        A ``step_outputs`` row whose ``step_name`` no longer resolves means the definition
        changed under an in-flight workflow -- worth an explicit error, since the
        alternative is unwinding a saga while quietly ignoring a step that moved money.
        """
        try:
            return self._by_name[name]
        except KeyError:
            raise KeyError(
                f"workflow {self.workflow_type!r} has no step named {name!r}; "
                f"defined steps: {', '.join(self.step_names)}"
            ) from None

    def instantiate(self) -> WorkflowInstance:
        """Fresh instance per workflow execution.

        The class is a namespace for steps, not a place to keep state: anything a step needs
        on replay has to come from ``ctx``, because the process that runs step 3 may not be
        the one that ran step 2.
        """
        return self.cls()


#: workflow_type -> definition. Populated at import time by @workflow.
_REGISTRY: dict[str, WorkflowDefinition] = {}


def workflow(workflow_type: str) -> Callable[[WorkflowClass], WorkflowClass]:
    """Register a class as the definition for ``workflow_type``.

    ``workflow_type`` is the string stored in ``workflows.workflow_type`` and half of the
    idempotency key, so it is a wire identifier: choose it once and do not rename it.

    The class comes back unchanged -- registration is a side effect, not a transformation --
    so it stays ordinarily importable, subclassable and testable.
    """

    def decorate(cls: WorkflowClass) -> WorkflowClass:
        if not workflow_type or not workflow_type.strip():
            raise WorkflowDefinitionError("workflow_type must be a non-empty string")
        steps = _collect_steps(cls)
        _validate(workflow_type, cls, steps)
        existing = _REGISTRY.get(workflow_type)
        if existing is not None and existing.cls is not cls:
            raise WorkflowDefinitionError(
                f"workflow_type {workflow_type!r} is already registered to "
                f"{existing.cls.__module__}.{existing.cls.__qualname__}; two definitions "
                "sharing a type would make replay pick whichever imported last"
            )
        _REGISTRY[workflow_type] = WorkflowDefinition(
            workflow_type=workflow_type, cls=cls, steps=steps
        )
        return cls

    return decorate


def get_definition(workflow_type: str) -> WorkflowDefinition:
    """Resolve a claimed row's ``workflow_type`` to its definition.

    Unknown type means the row was written by a build that had a definition this process
    does not import. Failing here is right: the workflow stays claimable and a worker that
    does know the type can pick it up.
    """
    try:
        return _REGISTRY[workflow_type]
    except KeyError:
        known = ", ".join(sorted(_REGISTRY)) or "none"
        raise KeyError(
            f"no workflow registered for type {workflow_type!r}; registered: {known}. "
            "Is the module defining it imported by this worker?"
        ) from None


def registered_types() -> tuple[str, ...]:
    """Every registered ``workflow_type``, sorted."""
    return tuple(sorted(_REGISTRY))


def clear_registry() -> None:
    """Drop every registration. Test hook -- tests define throwaway workflows."""
    _REGISTRY.clear()


# ---------------------------------------------------------------------------
# Registration-time validation
# ---------------------------------------------------------------------------


def _collect_steps(cls: type) -> tuple[Step, ...]:
    """Gather the class's steps, including inherited ones, ordered by ``seq``.

    The MRO is walked in reverse so a subclass overriding an inherited step wins, and the
    same Step object bound to two names is counted once -- ``@x.compensate`` returning the
    plain function keeps that case rare, but aliasing a step is not worth a crash.
    """
    found: dict[str, Step] = {}
    for klass in reversed(cls.__mro__):
        for attr, value in vars(klass).items():
            if isinstance(value, Step):
                found[attr] = value
    unique: dict[int, Step] = {id(s): s for s in found.values()}
    return tuple(sorted(unique.values(), key=lambda s: s.seq))


def _validate(workflow_type: str, cls: type, steps: tuple[Step, ...]) -> None:
    where = f"workflow {workflow_type!r} ({cls.__qualname__})"
    if not steps:
        raise WorkflowDefinitionError(f"{where} declares no steps; add at least one @step")

    seqs = [s.seq for s in steps]
    duplicates = sorted({n for n in seqs if seqs.count(n) > 1})
    if duplicates:
        collide = {n: [s.name for s in steps if s.seq == n] for n in duplicates}
        raise WorkflowDefinitionError(
            f"{where} reuses seq values: "
            + "; ".join(f"seq={n} on {', '.join(names)}" for n, names in collide.items())
            + ". seq is the execution order and the unwind order -- it cannot be ambiguous"
        )

    expected = list(range(1, len(steps) + 1))
    if seqs != expected:
        missing = sorted(set(expected) - set(seqs))
        raise WorkflowDefinitionError(
            f"{where} has non-contiguous seq values {seqs}; expected {expected}"
            + (f" (missing {missing})" if missing else "")
            + ". A gap usually means a step was deleted, which silently changes what the "
            "remaining steps read from ctx"
        )

    names = [s.name for s in steps]
    duplicate_names = sorted({n for n in names if names.count(n) > 1})
    if duplicate_names:
        raise WorkflowDefinitionError(
            f"{where} declares more than one step named {', '.join(duplicate_names)}; "
            "step_name is part of the step_outputs primary key, so the second step would "
            "be treated as the first one already having run"
        )


def _require_coroutine_function(func: object, what: str) -> None:
    if not inspect.iscoroutinefunction(func):
        raise WorkflowDefinitionError(
            f"{what} must be declared with 'async def' -- got {func!r}. A blocking function "
            "here stalls the whole worker's event loop, including every other workflow's "
            "lease renewal"
        )


def _as_exception_tuple(
    value: type[BaseException] | tuple[type[BaseException], ...], what: str
) -> tuple[type[BaseException], ...]:
    types = value if isinstance(value, tuple) else (value,)
    for entry in types:
        if not (isinstance(entry, type) and issubclass(entry, BaseException)):
            raise WorkflowDefinitionError(f"{what} must contain exception types, got {entry!r}")
    return tuple(types)
