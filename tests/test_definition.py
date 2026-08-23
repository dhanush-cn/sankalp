"""The definition API, proved: registry order, compensations, and the checks that fire
at registration rather than halfway through moving money.

No database here -- a definition is a declaration and touches nothing. (The autouse
truncate fixture in conftest still needs Postgres up; that is the suite's standing cost,
not this module's.)
"""

from __future__ import annotations

import uuid

import pytest

from sankalp.engine.definition import (
    StepContext,
    clear_registry,
    get_definition,
    registered_types,
    step,
    workflow,
)
from sankalp.engine.errors import RetryableError, TerminalError, WorkflowDefinitionError


@pytest.fixture(autouse=True)
def isolated_registry():
    """Each test defines throwaway workflows into an empty registry.

    The registry is module-global and populated at import time, so without this a test that
    registers 'payment_transfer' would collide with the next one that does.
    """
    clear_registry()
    yield
    clear_registry()


def make_context(**overrides) -> StepContext:
    defaults = dict(
        workflow_id=uuid.uuid4(),
        input={"amount_minor": 25_000, "currency": "INR"},
        outputs={},
        fencing_token=7,
        attempt=1,
    )
    return StepContext(**(defaults | overrides))


# ---------------------------------------------------------------------------
# 1. The headline case: a 3-step workflow resolves in order, with the right
#    compensations attached to the right steps.
# ---------------------------------------------------------------------------


def define_transfer() -> type:
    """A realistic shape: debit, then a read-only check, then credit.

    Declared out of seq order on purpose -- ``check_limits`` is written between the two,
    ``credit_beneficiary`` is declared first -- so the ordering assertions below are about
    ``seq`` and not about source order.
    """

    @workflow("payment_transfer")
    class PaymentTransfer:
        @step(seq=3)
        async def credit_beneficiary(self, ctx: StepContext) -> dict[str, int]:
            debit = ctx.output_of("debit_wallet")
            return {"credited_minor": debit["debited_minor"]}

        @credit_beneficiary.compensate
        async def undo_credit(self, ctx: StepContext, forward_output: dict[str, int]) -> None:
            self.undone.append(("credit_beneficiary", forward_output))

        @step(seq=1)
        async def debit_wallet(self, ctx: StepContext) -> dict[str, int]:
            return {"debited_minor": ctx.input["amount_minor"]}

        @debit_wallet.compensate
        async def undo_debit(self, ctx: StepContext, forward_output: dict[str, int]) -> None:
            self.undone.append(("debit_wallet", forward_output))

        # Read-only: nothing happened downstream, so there is nothing to unwind.
        @step(seq=2)
        async def check_limits(self, ctx: StepContext) -> dict[str, bool]:
            return {"within_limit": True}

        def __init__(self) -> None:
            self.undone: list[tuple[str, dict[str, int]]] = []

    return PaymentTransfer


def test_registry_resolves_steps_in_seq_order_with_compensations():
    cls = define_transfer()

    assert registered_types() == ("payment_transfer",)
    definition = get_definition("payment_transfer")
    assert definition.cls is cls
    assert len(definition) == 3

    # Order is seq order, not the order the methods were written in.
    assert definition.step_names == ("debit_wallet", "check_limits", "credit_beneficiary")
    assert [s.seq for s in definition.steps] == [1, 2, 3]

    debit, check, credit = definition.steps

    # Each compensation is attached to its own step -- and the read-only step has none,
    # which is what tells the unwind to skip it rather than to fail.
    assert debit.compensation is not None
    assert debit.compensation.__name__ == "undo_debit"
    assert credit.compensation is not None
    assert credit.compensation.__name__ == "undo_credit"
    assert check.compensation is None

    assert definition.step_by_name("credit_beneficiary") is credit


async def test_definition_executes_forward_then_unwinds_in_reverse():
    """The registry is only useful if the engine can drive it: run the three steps the way
    the worker loop would, then unwind them the way COMPENSATING would."""
    define_transfer()
    definition = get_definition("payment_transfer")
    instance = definition.instantiate()

    outputs: dict[str, dict[str, int]] = {}
    for s in definition.steps:
        ctx = make_context(outputs=dict(outputs), step_name=s.name)
        outputs[s.name] = await s.invoke(instance, ctx)

    assert outputs == {
        "debit_wallet": {"debited_minor": 25_000},
        "check_limits": {"within_limit": True},
        "credit_beneficiary": {"credited_minor": 25_000},
    }

    # Unwind: reverse seq order, skipping the step with no compensation, each handed back
    # the output its forward step produced.
    for s in reversed(definition.steps):
        if s.compensation is None:
            continue
        await s.invoke_compensation(instance, make_context(outputs=outputs), outputs[s.name])

    assert instance.undone == [
        ("credit_beneficiary", {"credited_minor": 25_000}),
        ("debit_wallet", {"debited_minor": 25_000}),
    ]


def test_instantiate_returns_a_fresh_instance_each_time():
    """State must not leak between executions -- the process that runs step 3 may not be
    the one that ran step 2, so anything remembered on the instance is a lie on replay."""
    define_transfer()
    definition = get_definition("payment_transfer")
    assert definition.instantiate() is not definition.instantiate()


async def test_step_stays_callable_on_an_instance():
    """The decorator must not turn a method into something instances cannot call."""
    define_transfer()
    definition = get_definition("payment_transfer")
    instance = definition.instantiate()

    assert definition.cls.debit_wallet is definition.steps[0]  # class access introspects
    assert await instance.debit_wallet(make_context()) == {"debited_minor": 25_000}


# ---------------------------------------------------------------------------
# 2. Registration-time validation.
# ---------------------------------------------------------------------------


def test_duplicate_seq_is_rejected():
    with pytest.raises(WorkflowDefinitionError, match="reuses seq"):

        @workflow("dupe")
        class _Dupe:
            @step(seq=1)
            async def a(self, ctx: StepContext) -> None: ...

            @step(seq=1)
            async def b(self, ctx: StepContext) -> None: ...

    assert registered_types() == (), "a rejected definition must not land in the registry"


def test_gap_in_seq_is_rejected():
    """A gap normally means a step was deleted, which silently changes what the remaining
    steps read from ctx -- so it fails at import, not at 3am."""
    with pytest.raises(WorkflowDefinitionError, match="non-contiguous"):

        @workflow("gappy")
        class _Gappy:
            @step(seq=1)
            async def a(self, ctx: StepContext) -> None: ...

            @step(seq=3)
            async def c(self, ctx: StepContext) -> None: ...


def test_seq_must_start_at_one():
    with pytest.raises(WorkflowDefinitionError, match="non-contiguous"):

        @workflow("zero_based")
        class _ZeroBased:
            @step(seq=2)
            async def a(self, ctx: StepContext) -> None: ...

            @step(seq=3)
            async def b(self, ctx: StepContext) -> None: ...


def test_duplicate_step_name_is_rejected():
    """step_name is part of the step_outputs primary key: two steps sharing one would make
    the second look like the first had already run."""
    with pytest.raises(WorkflowDefinitionError, match="more than one step named"):

        @workflow("same_name")
        class _SameName:
            @step(seq=1, name="settle")
            async def a(self, ctx: StepContext) -> None: ...

            @step(seq=2, name="settle")
            async def b(self, ctx: StepContext) -> None: ...


def test_workflow_without_steps_is_rejected():
    with pytest.raises(WorkflowDefinitionError, match="declares no steps"):

        @workflow("empty")
        class _Empty:
            pass


def test_blocking_step_function_is_rejected():
    """A def instead of an async def stalls the event loop for every other workflow the
    worker holds, including their lease renewals."""
    with pytest.raises(WorkflowDefinitionError, match="async def"):

        @step(seq=1)
        def blocking(self, ctx: StepContext) -> None: ...


def test_blocking_compensation_is_rejected():
    @step(seq=1)
    async def forward(self, ctx: StepContext) -> None: ...

    with pytest.raises(WorkflowDefinitionError, match="async def"):

        @forward.compensate
        def undo(self, ctx: StepContext, forward_output: None) -> None: ...


def test_second_compensation_on_one_step_is_rejected():
    @step(seq=1)
    async def forward(self, ctx: StepContext) -> None: ...

    @forward.compensate
    async def undo(self, ctx: StepContext, forward_output: None) -> None: ...

    with pytest.raises(WorkflowDefinitionError, match="already has compensation"):

        @forward.compensate
        async def undo_again(self, ctx: StepContext, forward_output: None) -> None: ...


def test_registering_two_classes_under_one_type_is_rejected():
    @workflow("collide")
    class _First:
        @step(seq=1)
        async def a(self, ctx: StepContext) -> None: ...

    with pytest.raises(WorkflowDefinitionError, match="already registered"):

        @workflow("collide")
        class _Second:
            @step(seq=1)
            async def a(self, ctx: StepContext) -> None: ...

    assert get_definition("collide").cls is _First


def test_unknown_workflow_type_names_what_is_registered():
    @workflow("known")
    class _Known:
        @step(seq=1)
        async def a(self, ctx: StepContext) -> None: ...

    with pytest.raises(KeyError, match="known"):
        get_definition("nope")


def test_explicit_name_survives_a_method_rename():
    """The persisted step_name is the checkpoint identity; renaming the method must not
    orphan in-flight workflows' step_outputs rows."""

    @workflow("renamed")
    class _Renamed:
        @step(seq=1, name="debit_wallet")
        async def debit_wallet_v2(self, ctx: StepContext) -> None: ...

    assert get_definition("renamed").step_names == ("debit_wallet",)


def test_inherited_steps_are_collected_and_overrides_win():
    class Base:
        @step(seq=1)
        async def debit(self, ctx: StepContext) -> str:
            return "base"

        @step(seq=2)
        async def credit(self, ctx: StepContext) -> str:
            return "base"

    @workflow("inheriting")
    class Child(Base):
        @step(seq=2)
        async def credit(self, ctx: StepContext) -> str:
            return "child"

    definition = get_definition("inheriting")
    assert definition.step_names == ("debit", "credit")
    assert definition.step_by_name("credit") is not Base.credit


# ---------------------------------------------------------------------------
# 3. Error classification -- the branch that decides retry vs. unwind.
# ---------------------------------------------------------------------------


def test_default_classification_is_terminal():
    """Unrecognised failures unwind rather than retry: an unknown failure mode is no
    evidence that re-running the step is safe."""

    @workflow("classify")
    class _Classify:
        @step(seq=1)
        async def a(self, ctx: StepContext) -> None: ...

    s = get_definition("classify").steps[0]
    assert s.is_retryable(RetryableError("downstream 503")) is True
    assert s.is_retryable(TerminalError("insufficient funds")) is False
    assert s.is_retryable(ValueError("who knows")) is False


def test_declared_types_classify_exceptions_the_step_did_not_raise():
    @workflow("declared")
    class _Declared:
        @step(seq=1, retry_on=(TimeoutError, ConnectionError), terminal_on=PermissionError)
        async def a(self, ctx: StepContext) -> None: ...

    s = get_definition("declared").steps[0]
    assert s.is_retryable(TimeoutError()) is True
    assert s.is_retryable(ConnectionResetError()) is True  # subclass of ConnectionError
    assert s.is_retryable(PermissionError()) is False
    assert s.is_retryable(RuntimeError()) is False


def test_terminal_on_beats_the_retryable_marker():
    class RateLimited(RetryableError):
        pass

    @workflow("precedence")
    class _Precedence:
        @step(seq=1, terminal_on=RateLimited)
        async def a(self, ctx: StepContext) -> None: ...

    s = get_definition("precedence").steps[0]
    assert s.is_retryable(RateLimited()) is False
    assert s.is_retryable(RetryableError()) is True


def test_a_type_in_both_lists_is_rejected():
    with pytest.raises(WorkflowDefinitionError, match="both retry_on and terminal_on"):
        step(seq=1, retry_on=TimeoutError, terminal_on=TimeoutError)


def test_retry_on_must_contain_exception_types():
    with pytest.raises(WorkflowDefinitionError, match="exception types"):
        step(seq=1, retry_on=("TimeoutError",))


# ---------------------------------------------------------------------------
# 4. StepContext.
# ---------------------------------------------------------------------------


async def test_context_exposes_prior_outputs_and_claim_state():
    ctx = make_context(outputs={"debit_wallet": {"debited_minor": 25_000}})

    assert ctx.input["amount_minor"] == 25_000
    assert ctx.output_of("debit_wallet") == {"debited_minor": 25_000}
    assert ctx.fencing_token == 7
    assert ctx.attempt == 1


def test_output_of_names_what_has_completed():
    ctx = make_context(outputs={"debit_wallet": {}})
    with pytest.raises(KeyError, match="completed so far: debit_wallet"):
        ctx.output_of("credit_beneficiary")


async def test_renew_lease_calls_the_worker_callback():
    renewals = 0

    async def renew() -> None:
        nonlocal renewals
        renewals += 1

    ctx = make_context(renew_lease_callback=renew)
    await ctx.renew_lease()
    await ctx.renew_lease()
    assert renewals == 2


async def test_renew_lease_without_a_worker_is_a_no_op():
    """A hand-built context holds no lease, so there is nothing to extend -- a unit test of
    a step should not have to stub this out."""
    await make_context().renew_lease()
