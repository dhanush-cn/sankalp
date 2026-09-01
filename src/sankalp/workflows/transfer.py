"""``demo_transfer`` -- a double-entry transfer against the real ledger (migrations/003_saga.sql).

Unlike ``demo.py`` and ``demo_unwind`` in ``unwind.py``, this workflow is **idempotent by
construction**, the way a real production step must be (docs/spec.md, "Idempotent by
construction"; CLAUDE.md). Its two forward steps write ``ledger_entries`` with
``ON CONFLICT (workflow_id, step_name, account_id, direction) DO NOTHING``, so a replayed
step posts nothing extra. That is the opposite design choice from ``demo.py``'s ``hold`` and
``unwind.py``'s compensations, which are deliberately NOT idempotent so a crash gate can count
raw attempts. This workflow's assertion is therefore **reconciliation** -- every
``transfer_id`` in ``ledger_entries`` nets to zero (``tests/chaos/invariants.py``'s
``check_reconciliation``, the query from docs/spec.md) -- never an execution count. It must
NEVER be cited as evidence of exactly-once *effects*; ``demo_crash`` (``demo.py``) owns that
claim, with committed ``step_attempts``/``side_effects`` rows built to measure it. This module
has no such instrumentation and would not prove anything if it did -- an idempotent step
running twice is supposed to look identical to running once.

**Why the debit and the credit are separate steps, in separate transactions.** A single step
posting both rows would make "transiently unbalanced" impossible to observe -- either both
commit or neither does, and reconciliation could never catch anything. Splitting them means a
crash between ``post_debit``'s commit and ``post_credit``'s attempt leaves exactly one leg
posted, which is the state this workflow exists to be able to create: reconciliation must find
it non-zero until the second leg lands (normally) or the first leg is reversed (after a
COMPENSATING unwind).

**``transfer_id`` is ``ctx.workflow_id``, never a freshly generated UUID.** It has to be
identical across every attempt AND every compensation of one workflow, or the four rows a
compensated transfer produces (debit, credit, and their reversals) would not share a
``transfer_id`` and reconciliation could never net them against each other. Generating a new
UUID inside a step -- or threading one through ``forward_output`` -- would mean a replayed
step could invent a second ``transfer_id`` and post an orphan row that no reversal could ever
balance: exactly the failure this workflow is built to make visible, not to cause.

**Compensation posts a new, opposite-direction entry -- never an UPDATE or DELETE.**
``ledger_entries`` is append-only, enforced by ``trg_ledger_entries_append_only``
(migrations/003_saga.sql) with no bypass, and ``sankalp_app`` holds only ``SELECT``/``INSERT``
on it (no ``UPDATE``, no ``DELETE``) per ``migrations/004_restricted_role.sql``. A reversal is
therefore a normal positive-``amount_minor`` row (the ``CHECK (amount_minor > 0)`` constraint
rules out a signed correction) in the flipped ``direction``, recorded under
``compensation_of(step_name)`` so it does not collide with the forward row it undoes under
``uq_ledger_entry``.

No waits, no gates, no sleeps. This is not a crash-gate instrument like ``demo.py`` /
``unwind.py`` -- it exists to be run under fault injection (Toxiproxy) and read back by
reconciliation, not to be SIGKILLed at a chosen instant.
"""

from __future__ import annotations

import asyncio
from typing import Any

import asyncpg

from sankalp.config import get_settings
from sankalp.engine.definition import StepContext, step, workflow
from sankalp.engine.errors import TerminalError
from sankalp.storage.pool import create_pool
from sankalp.workflows._instrumentation import compensation_of

__all__ = ["DemoTransfer", "WORKFLOW_TYPE", "STEP_NAMES", "compensation_of"]

#: The ``workflows.workflow_type`` this definition is registered under. A wire identifier.
WORKFLOW_TYPE = "demo_transfer"

#: Forward step names in ``seq`` order. Exported so callers assert against these rather than
#: against string literals that could drift out of step with the definition.
STEP_NAMES = ("post_debit", "post_credit")

_INSERT_ENTRY_SQL = """
INSERT INTO ledger_entries (
    transfer_id, workflow_id, step_name, account_id, direction, amount_minor, currency
)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (workflow_id, step_name, account_id, direction) DO NOTHING
"""

# This module's own pool, deliberately separate from workflows/_instrumentation.py's. Same
# double-checked-lock shape as that module's get_pool() -- see _get_pool()'s docstring below
# for why the two must not be the same pool.
_pool: asyncpg.Pool | None = None
_pool_lock = asyncio.Lock()


async def _get_pool() -> asyncpg.Pool:
    """This module's pool, opened on first use, on the RESTRICTED role -- never the owning one.

    workflows/_instrumentation.py's ``get_pool()`` is deliberately on the owning role
    (``settings.active_database_url``), because ``side_effects``, ``step_attempts`` and
    ``crash_gates`` have no ``sankalp_app`` grants at all (migrations/004_restricted_role.sql)
    -- those three tables are crash-gate instrumentation, not production writes, and the
    owning role is the only one that can touch them.

    ``ledger_entries`` is not instrumentation. It is a business table, and ``sankalp_app``
    holds exactly ``SELECT``/``INSERT`` on it (migrations/004_restricted_role.sql) -- enough
    for everything this workflow does, and by design nothing more (no ``UPDATE``, matching the
    append-only trigger; see ``_post_entry``'s docstring). So this workflow must write through
    the RESTRICTED role, the same one a real worker executes on
    (``settings.active_app_database_url``, ``engine/worker.py:399``) -- never
    ``_instrumentation.get_pool()``'s owning-role pool, and not because of a grant it would
    fail on (``sankalp_app`` can INSERT here just fine) but because of which DSN that would
    resolve to.

    That DSN is the consequence that actually matters for the chaos suite.
    ``active_app_database_url`` is exactly what ``tests/fleet.py``'s ``WorkerFleet.launch``
    overrides via ``extra_env={"SANKALP_TEST_APP_DATABASE_URL": ...}`` to route a worker
    through the Toxiproxy Postgres proxy. Using this module's own pool means ``demo_transfer``'s
    ledger writes take the SAME path through the SAME proxy the worker executing it takes.
    Reaching for ``_instrumentation.get_pool()`` instead -- built on ``active_database_url``,
    which chaos tests never override -- would send those writes over a direct, unproxied
    connection: reconciliation would then read a database the injected fault never touched,
    and a chaos scenario could pass green while proving nothing about the fault it injected.
    """
    global _pool
    if _pool is not None:
        return _pool
    async with _pool_lock:
        if _pool is None:
            settings = get_settings()
            _pool = await create_pool(settings.active_app_database_url, settings=settings)
    return _pool


async def _post_entry(
    ctx: StepContext, *, step_name: str, account_id: str, direction: str
) -> None:
    """One idempotent INSERT into ``ledger_entries``, in its own transaction.

    ``transfer_id`` is always ``ctx.workflow_id`` -- see the module docstring for why that is
    load-bearing rather than incidental. ``ON CONFLICT ... DO NOTHING`` is required, not
    defensive: a step commits this INSERT before the engine commits its ``step_outputs``
    checkpoint, so a crash in that window replays the step, and the second attempt must post
    nothing. ``DO UPDATE`` is not an option here: ``sankalp_app`` has no ``UPDATE`` grant on
    this table, and the append-only trigger would reject the UPDATE outright even if it did.
    """
    amount_minor = int(ctx.input.get("amount_minor", 100_00))
    currency = str(ctx.input.get("currency", "INR"))
    pool = await _get_pool()
    async with pool.acquire() as conn, conn.transaction():
        await conn.execute(
            _INSERT_ENTRY_SQL,
            ctx.workflow_id,
            ctx.workflow_id,
            step_name,
            account_id,
            direction,
            amount_minor,
            currency,
        )


def _validate_fail_after(ctx: StepContext) -> None:
    """Reject a ``fail_after`` that could never drive a compensation.

    Only a name in ``STEP_NAMES[:-1]`` -- every step except the last -- has a following step
    to fail *in* (see :func:`_maybe_fail_before`). A ``fail_after`` naming the last step, or a
    typo that matches no step at all, previously returned silently and the workflow ran to
    SUCCESS with a balanced ledger: a caller who asked for a compensation path and got a clean
    success instead, with nothing to say so. That is the same vacuity this workflow exists to
    keep out of ``check_reconciliation`` -- a check that cannot fail is not a check -- so it is
    rejected here as a caller error, loudly, before any money moves, rather than left to be
    silently absorbed as a no-op.

    Checked once, at the very start of the workflow (``post_debit``, seq=1), rather than
    repeated in every step -- one bad value should fail the same way regardless of which step
    would eventually have ignored it.
    """
    fail_after = ctx.input.get("fail_after")
    if fail_after is None:
        return
    usable = STEP_NAMES[:-1]
    if fail_after not in usable:
        raise TerminalError(
            f"workflow {ctx.workflow_id}: fail_after={fail_after!r} is not usable -- it must "
            f"name one of {', '.join(repr(n) for n in usable)} to drive a compensation. "
            f"{STEP_NAMES[-1]!r} (the last step) and any other value have no following step "
            "to fail in, and would otherwise run to SUCCESS silently."
        )


def _maybe_fail_before(ctx: StepContext, step_name: str) -> None:
    """Raise ``TerminalError`` iff ``ctx.input['fail_after']`` names the PRECEDING step.

    Checked at the START of a step, before it does any work -- never after. Failing from
    inside the named step itself, after it has already posted its entry, would mean that
    step's own forward call never returns and the engine never checkpoints it to
    ``step_outputs`` -- so its own compensation would never run, leaving that leg's entry
    orphaned and unreversed. (Caught exactly that way in manual verification: a
    ``fail_after: "post_debit"`` that raised from inside ``post_debit`` after its INSERT
    left ``post_debit`` non-terminal and its compensation never invoked -- reconciliation
    correctly flagged the transfer as unbalanced, which is the bug this ordering fixes.)
    Failing here, at the top of the NEXT step, lets the named step complete and checkpoint
    normally, so the unwind has a real, committed leg to reverse.

    A ``fail_after`` that could never reach a following step (the last step, or a typo) is
    rejected up front by :func:`_validate_fail_after` instead of being silently ignored here.
    """
    previous_index = STEP_NAMES.index(step_name) - 1
    if previous_index < 0:
        return
    if ctx.input.get("fail_after") == STEP_NAMES[previous_index]:
        raise TerminalError(
            f"workflow {ctx.workflow_id}: demo_transfer configured to fail after "
            f"{STEP_NAMES[previous_index]!r} (fail_after), to drive compensation "
            "deterministically"
        )


@workflow(WORKFLOW_TYPE)
class DemoTransfer:
    """A double-entry transfer: debit the source, credit the destination, same amount."""

    @step(seq=1)
    async def post_debit(self, ctx: StepContext) -> dict[str, Any]:
        """Post the DEBIT leg against the source account."""
        _validate_fail_after(ctx)
        _maybe_fail_before(ctx, "post_debit")
        source = str(ctx.input.get("source_account", "acct:source"))
        await _post_entry(ctx, step_name="post_debit", account_id=source, direction="DEBIT")
        return {"account_id": source}

    @step(seq=2)
    async def post_credit(self, ctx: StepContext) -> dict[str, Any]:
        """Post the CREDIT leg against the destination account, same amount and transfer."""
        _maybe_fail_before(ctx, "post_credit")
        destination = str(ctx.input.get("destination_account", "acct:destination"))
        await _post_entry(
            ctx, step_name="post_credit", account_id=destination, direction="CREDIT"
        )
        return {"account_id": destination}

    @post_debit.compensate
    async def reverse_debit(self, ctx: StepContext, forward_output: dict[str, Any]) -> None:
        """Reverse the DEBIT leg: a new CREDIT row against the same account, same amount."""
        name = compensation_of("post_debit")
        await _post_entry(
            ctx, step_name=name, account_id=forward_output["account_id"], direction="CREDIT"
        )

    @post_credit.compensate
    async def reverse_credit(self, ctx: StepContext, forward_output: dict[str, Any]) -> None:
        """Reverse the CREDIT leg: a new DEBIT row against the same account, same amount."""
        name = compensation_of("post_credit")
        await _post_entry(
            ctx, step_name=name, account_id=forward_output["account_id"], direction="DEBIT"
        )
