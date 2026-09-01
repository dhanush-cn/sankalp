"""``demo_transfer``, proved against a real engine and a real database.

See ``src/sankalp/workflows/transfer.py`` for the design; this file exists to prove it holds,
the same way ``tests/test_ledger.py`` proves ``migrations/003_saga.sql``'s claims rather than
just restating them. Cases 1-3 are driven through :func:`execute_workflow` -- claim, then
execute, exactly like a real worker -- never by calling a step method on a bare
``DemoTransfer()`` instance. That is the same idiom ``tests/test_executor.py`` and
``tests/test_compensation.py`` use, and it matters here specifically: ``demo_transfer`` is
idempotent by construction (``ON CONFLICT ... DO NOTHING``), so a test that hand-invoked step
methods could not tell "the engine replayed this correctly" from "the test replayed it".

No ``WorkerFleet`` / SIGKILL here, unlike ``tests/test_crash.py`` and
``tests/test_compensation_crash.py``. Those gate on ``demo_crash`` / ``demo_unwind``, which
carry crash-gate instrumentation (``workflows/_instrumentation.py``) built to be paused and
killed at a chosen instant. ``demo_transfer`` deliberately has none of that (its own module
docstring: "No waits, no gates, no sleeps") -- it is not a crash-gate instrument, so there is
nothing here to pause a subprocess inside of. Its correctness is proved the same way
``test_executor.py`` proves the engine's replay logic in general: in-process, against the real
pool, through the real ``execute_workflow``.

Four properties:

  1. the happy path posts a genuine balanced double entry, and reconciliation sees it as such;
  2. a mid-transfer failure compensates only the leg that actually posted, leaving no trace of
     the leg that never ran -- and the reversal alone is still balanced;
  3. an unusable ``fail_after`` (the last step, or a typo) is rejected loudly, as a distinct
     outcome from both a clean SUCCESS and a real compensation -- COMPENSATED with zero rows,
     not COMPENSATED with a reversed leg;
  4. ``demo_transfer``'s idempotency guard -- ``ON CONFLICT ... DO NOTHING`` -- makes a second
     invocation of ``post_debit`` against the same workflow a no-op. This one does NOT drive
     the second invocation through ``execute_workflow``; see its docstring for why not, and
     for what it does and does not prove as a result.
"""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import AsyncIterator

import pytest
from chaos.invariants import RECONCILE

from sankalp.config import Settings, get_settings
from sankalp.engine.definition import StepContext, get_definition, workflow
from sankalp.engine.executor import ExecutionResult, execute_workflow
from sankalp.storage.queue import claim_workflows
from sankalp.workflows import transfer
from sankalp.workflows.transfer import STEP_NAMES, WORKFLOW_TYPE, DemoTransfer, compensation_of

LEASE = 30


@pytest.fixture(autouse=True)
async def reset_transfer_pool() -> AsyncIterator[None]:
    """Force ``SANKALP_ENVIRONMENT=test``, and give ``transfer._get_pool()`` a fresh pool and
    lock, for every test in this file.

    **Why the environment is forced here too, not just the pool.** ``tests/conftest.py``'s
    ``pool`` fixture is hardcoded to ``test_database_url`` regardless of
    ``SANKALP_ENVIRONMENT`` (deliberately -- a stray env var must not point the suite at dev).
    ``transfer.py``'s ``_get_pool()``, by contrast, resolves ``settings.active_app_database_url``,
    which SWITCHES on ``environment`` (``config.py``): ``test_app_database_url`` when
    ``environment == "test"``, ``app_database_url`` (dev) otherwise. Those two only agree on
    the same database when ``SANKALP_ENVIRONMENT=test`` is actually set in the process --
    which ``make test`` and ``make chaos`` both do, but a bare
    ``pytest tests/test_transfer.py`` does not. Without forcing it here, this file's workflow
    inserts against ``pool`` (``sankalp_test``) while ``transfer._get_pool()`` writes against
    dev ``sankalp`` -- a foreign key violation on ``ledger_entries.workflow_id``, since the
    row the workflow was inserted as lives in a database the ledger pool never touches. Setting
    it in a fixture, restored afterward, makes this file self-contained rather than dependent
    on how it happens to be invoked. ``get_settings`` is ``@lru_cache``d
    (``config.py:355``), which documents ``get_settings.cache_clear()`` as the intended hook
    for exactly this: a test that mutates the environment must clear the cache before AND
    after, or the mutation either does not take effect (a stale cached ``Settings`` from
    before this fixture ran) or outlives this test (a ``Settings`` cached mid-override, read by
    a later test that never asked for ``environment=test``).

    **Why the pool is reset too.** ``transfer.py`` has its own module-level pool (``_pool`` /
    ``_pool_lock``), separate from ``workflows/_instrumentation.py``'s -- deliberately on the
    RESTRICTED role (``settings.active_app_database_url``), the same one a real worker
    executes on, so that ``demo_transfer``'s ledger writes take the same path through
    Toxiproxy a chaos scenario's worker fleet takes (see ``transfer._get_pool()``'s own
    docstring). That pool has exactly the same stale-event-loop hazard
    ``workflows/_instrumentation.py``'s did, for the same reason: it is a module-level global,
    bound to whichever event loop is running the first time ``_get_pool()`` is called, and it
    is never rebuilt after that. That is correct for its real consumer -- a worker process has
    exactly one event loop for its entire lifetime -- but this file drives ``demo_transfer``
    in-process, and pytest gives every test function its OWN fresh event loop
    (``pyproject.toml``'s ``asyncio_default_fixture_loop_scope = "function"``). The module
    global survives across tests within the same pytest process, so the pool this file's first
    test creates stays cached for every test after it, bound to a loop that has since been
    closed -- "Event loop is closed", then a follow-on ``asyncpg.exceptions.InterfaceError``
    trying to roll back on it.

    So both fixes belong here: this test file is what breaks the pool singleton's one
    assumption and what depends on an environment variable it cannot assume the caller set, so
    this test file is what pays to reset both -- not ``transfer.py`` or ``config.py``, which
    are correct as written for their real consumers, and not ``_instrumentation.py``, which
    this fixture no longer even touches now that ``transfer.py`` has stopped sharing its pool.

    The lock, not only the pool, has to be replaced. ``asyncio.Lock()`` binds to a loop the
    first time it is awaited, same as the pool -- resetting ``_pool`` alone while leaving the
    OLD lock in place would trade one stale-loop failure (the pool) for a subtler one the next
    time two steps raced to call ``_get_pool()`` and both awaited the old, dead-loop lock.
    """
    previous_environment = os.environ.get("SANKALP_ENVIRONMENT")
    os.environ["SANKALP_ENVIRONMENT"] = "test"
    get_settings.cache_clear()

    transfer._pool = None
    transfer._pool_lock = asyncio.Lock()
    yield
    pool = transfer._pool
    if pool is not None:
        await pool.close()
    transfer._pool = None

    if previous_environment is None:
        os.environ.pop("SANKALP_ENVIRONMENT", None)
    else:
        os.environ["SANKALP_ENVIRONMENT"] = previous_environment
    get_settings.cache_clear()


@pytest.fixture(autouse=True)
def ensure_demo_transfer_registered() -> None:
    """Guarantee ``get_definition(WORKFLOW_TYPE)`` resolves, regardless of what ran before.

    Unlike ``test_executor.py`` / ``test_compensation.py``, this file drives the REAL,
    production-registered ``demo_transfer`` (imported by every worker via
    ``workflows/__init__.py``), not a throwaway definition -- so it must not
    ``clear_registry()``. But those other files' own ``isolated_registry`` fixtures do call
    it, as part of their own isolation, and since Python caches an already-imported module, a
    later plain ``import sankalp.workflows`` will not re-run ``transfer.py``'s ``@workflow``
    decorator to put ``demo_transfer`` back. Re-registering the same class object here is a
    no-op when it is already present -- ``workflow()`` only rejects a *different* class
    sharing the type (``engine/definition.py``) -- so this is safe to run before every test
    in this file no matter what pytest collected and ran beforehand.
    """
    workflow(WORKFLOW_TYPE)(DemoTransfer)


@pytest.fixture
def settings() -> Settings:
    return Settings(max_attempts=5, backoff_cap_seconds=1, lease_duration_seconds=LEASE)


# ---------------------------------------------------------------------------
# Helpers, in the shape tests/test_executor.py and tests/test_compensation.py use.
# ---------------------------------------------------------------------------


async def claim_one(pool, owner: str = "worker-a", lease: int = LEASE):
    """Claim exactly one workflow, the way the worker does."""
    async with pool.acquire() as conn:
        claimed = await claim_workflows(conn, owner, lease, 1)
    assert len(claimed) == 1, f"expected one claimable workflow, got {len(claimed)}"
    return claimed[0]


async def status_of(pool, workflow_id) -> str:
    return await pool.fetchval("SELECT status FROM workflows WHERE id = $1", workflow_id)


async def ledger_rows(pool, workflow_id) -> list[dict]:
    records = await pool.fetch(
        """
        SELECT transfer_id, step_name, account_id, direction, amount_minor, currency
        FROM ledger_entries
        WHERE workflow_id = $1
        ORDER BY id
        """,
        workflow_id,
    )
    return [dict(r) for r in records]


async def outbox_rows(pool, workflow_id) -> list[dict]:
    """Rows from ``outbox`` for one workflow, with ``payload`` (jsonb) decoded to a dict.

    ``outbox.payload`` is ``jsonb`` (migrations/003_saga.sql, ``storage/outbox.py``'s
    ``_INSERT_EVENT_SQL``), and this pool has no ``jsonb`` codec registered, so it comes back
    as text unless cast -- same reasoning as ``storage/outbox.py``'s own module docstring.
    Cast it to ``::text`` here and decode with ``json.loads``, rather than relying on any
    codec this fixture's pool may or may not have.
    """
    records = await pool.fetch(
        """
        SELECT event_type, payload::text AS payload
        FROM outbox
        WHERE workflow_id = $1
        ORDER BY id
        """,
        workflow_id,
    )
    return [{"event_type": r["event_type"], "payload": json.loads(r["payload"])} for r in records]


# ---------------------------------------------------------------------------
# 1. The happy path: a genuine balanced double entry.
# ---------------------------------------------------------------------------


async def test_happy_path_posts_a_balanced_double_entry_and_reconciles(
    pool, insert_workflow, settings
):
    workflow_id = await insert_workflow(
        workflow_type=WORKFLOW_TYPE,
        input={
            "source_account": "acct:A",
            "destination_account": "acct:B",
            "amount_minor": 250_00,
            "currency": "INR",
        },
    )

    result = await execute_workflow(pool, await claim_one(pool), settings=settings)

    assert result is ExecutionResult.SUCCESS
    assert await status_of(pool, workflow_id) == "SUCCESS"

    rows = await ledger_rows(pool, workflow_id)
    assert len(rows) == 2

    debit = next(r for r in rows if r["direction"] == "DEBIT")
    credit = next(r for r in rows if r["direction"] == "CREDIT")
    assert debit["step_name"] == "post_debit"
    assert debit["account_id"] == "acct:A"
    assert credit["step_name"] == "post_credit"
    assert credit["account_id"] == "acct:B"
    assert debit["amount_minor"] == credit["amount_minor"] == 250_00
    assert debit["transfer_id"] == workflow_id
    assert credit["transfer_id"] == workflow_id

    assert await pool.fetch(RECONCILE) == []


async def test_post_credit_emits_exactly_one_transfer_posted_event(pool, insert_workflow, settings):
    """What this proves: ``post_credit`` writes exactly one ``transfer.posted`` row to
    ``outbox``, in the same transaction as its ledger INSERT, with a payload matching what was
    submitted -- proving ``ctx.emit`` is actually wired to a production workflow rather than
    only exercised by ``tests/test_outbox.py``'s unit-level coverage of the mechanism itself
    (see ``transfer.py``'s ``post_credit`` docstring for why that gap mattered: before this,
    ``tests/chaos/invariants.py``'s ``check_outbox_drained`` ran against a table no real
    workflow had ever written to).
    """
    submitted = {
        "source_account": "acct:G",
        "destination_account": "acct:H",
        "amount_minor": 175_00,
        "currency": "INR",
    }
    workflow_id = await insert_workflow(workflow_type=WORKFLOW_TYPE, input=submitted)

    result = await execute_workflow(pool, await claim_one(pool), settings=settings)
    assert result is ExecutionResult.SUCCESS

    events = await outbox_rows(pool, workflow_id)
    assert len(events) == 1
    event = events[0]
    assert event["event_type"] == "transfer.posted"
    assert event["payload"] == {
        "transfer_id": str(workflow_id),
        "source_account": submitted["source_account"],
        "destination_account": submitted["destination_account"],
        "amount_minor": submitted["amount_minor"],
        "currency": submitted["currency"],
    }


# ---------------------------------------------------------------------------
# 2. Compensation reverses only the leg that actually posted.
# ---------------------------------------------------------------------------


async def test_compensation_reverses_only_the_leg_that_posted_and_stays_balanced(
    pool, insert_workflow, settings
):
    workflow_id = await insert_workflow(
        workflow_type=WORKFLOW_TYPE,
        input={
            "source_account": "acct:C",
            "destination_account": "acct:D",
            "amount_minor": 300_00,
            "fail_after": "post_debit",
        },
    )

    forward = await execute_workflow(pool, await claim_one(pool), settings=settings)
    assert forward is ExecutionResult.COMPENSATING

    unwind = await execute_workflow(
        pool, await claim_one(pool, owner="worker-b"), settings=settings
    )
    assert unwind is ExecutionResult.COMPENSATED
    assert await status_of(pool, workflow_id) == "COMPENSATED"

    rows = await ledger_rows(pool, workflow_id)
    assert len(rows) == 2
    step_names = {r["step_name"] for r in rows}
    assert step_names == {"post_debit", compensation_of("post_debit")}
    assert "post_credit" not in step_names, (
        "post_credit never completed -- it raised before posting anything, so there is "
        "nothing for its compensation to reverse"
    )

    debit = next(r for r in rows if r["step_name"] == "post_debit")
    reversal = next(r for r in rows if r["step_name"] == compensation_of("post_debit"))
    assert debit["direction"] == "DEBIT"
    assert reversal["direction"] == "CREDIT"
    assert debit["account_id"] == reversal["account_id"] == "acct:C"
    assert debit["amount_minor"] == reversal["amount_minor"] == 300_00
    assert debit["transfer_id"] == workflow_id
    assert reversal["transfer_id"] == workflow_id

    assert await pool.fetch(RECONCILE) == []

    assert await outbox_rows(pool, workflow_id) == [], (
        "post_credit never ran -- fail_after='post_debit' raised before it started -- so "
        "no transfer.posted event should exist for this workflow"
    )


# ---------------------------------------------------------------------------
# 3. An unusable fail_after is a caller error, not a silent SUCCESS.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "bad_fail_after",
    [
        pytest.param(STEP_NAMES[-1], id="names-the-last-step"),
        pytest.param("no-such-step", id="unrecognised-name"),
    ],
)
async def test_an_unusable_fail_after_is_rejected_before_anything_posts(
    pool, insert_workflow, settings, bad_fail_after
):
    workflow_id = await insert_workflow(
        workflow_type=WORKFLOW_TYPE,
        input={"amount_minor": 100_00, "fail_after": bad_fail_after},
    )

    forward = await execute_workflow(pool, await claim_one(pool), settings=settings)
    assert forward is ExecutionResult.COMPENSATING

    unwind = await execute_workflow(
        pool, await claim_one(pool, owner="worker-b"), settings=settings
    )
    assert unwind is ExecutionResult.COMPENSATED

    # "Not SUCCESS" alone would also be satisfied by a workflow that never ran at all.
    # COMPENSATED is the same status a legitimate compensation produces (case 2, above) --
    # the row count is what tells a rejected fail_after apart from a real reversal.
    assert await status_of(pool, workflow_id) == "COMPENSATED"
    assert await ledger_rows(pool, workflow_id) == []


# ---------------------------------------------------------------------------
# 4. The idempotency guard makes a second invocation of a step a no-op.
# ---------------------------------------------------------------------------


async def test_invoking_the_same_step_twice_posts_nothing_extra(pool, insert_workflow, settings):
    """What this proves: ``demo_transfer``'s ``ON CONFLICT (workflow_id, step_name,
    account_id, direction) DO NOTHING`` makes a second invocation of ``post_debit`` against
    the same workflow a no-op, at the database level.

    What this does NOT prove: that the ENGINE replays a step correctly. The second
    invocation below is not driven through ``execute_workflow`` -- this test builds the
    ``StepContext`` itself and calls ``Step.invoke`` directly, so the engine's own replay
    decision (whether and when it decides to re-run a step at all) is exactly the part this
    test does not exercise. It only shows that IF the engine ever does invoke this step
    twice, the write it performs is safe.

    Why not a real engine-driven replay: ``execute_workflow``'s loop structurally refuses to
    re-run a step once ``step_outputs`` has a row for it -- "a step whose name is in that set
    is not executed" (engine/executor.py's module docstring) is the guarantee, not a gap to
    route around. Producing a genuine replay requires reproducing the crash window between a
    step's side effect committing and its checkpoint committing, which is exactly what
    ``demo_crash``'s crash-gate instrumentation (``workflows/_instrumentation.py``) exists to
    hold open for a real SIGKILL -- and ``demo_transfer`` deliberately has none of that (see
    the module docstring above). The engine's replay guarantee itself is
    ``tests/test_crash.py``'s claim, proved there against a real killed worker; this test's
    job is narrower and stops at the boundary of what it can actually show without that
    machinery.

    What IS used, for the one invocation this test can make itself: :meth:`Step.invoke`
    (``engine/definition.py``) is the same primitive ``execute_workflow`` calls internally
    at ``result = await step.invoke(instance, ctx)`` -- a public method of the real,
    production-registered ``Step`` object, not a bare call to
    ``DemoTransfer().post_debit(...)``. The ``StepContext`` is built from the SAME claim
    (workflow_id, input, fencing_token, attempt) the first execution used.
    """
    workflow_id = await insert_workflow(
        workflow_type=WORKFLOW_TYPE,
        input={
            "source_account": "acct:E",
            "destination_account": "acct:F",
            "amount_minor": 400_00,
        },
    )

    claimed = await claim_one(pool)
    result = await execute_workflow(pool, claimed, settings=settings)
    assert result is ExecutionResult.SUCCESS

    before = await ledger_rows(pool, workflow_id)
    assert len(before) == 2

    definition = get_definition(WORKFLOW_TYPE)
    post_debit = definition.step_by_name("post_debit")
    replay_ctx = StepContext(
        workflow_id=claimed.id,
        input=claimed.input,
        outputs={},
        fencing_token=claimed.fencing_token,
        attempt=claimed.attempt,
        owner_id=claimed.owner_id,
        step_name="post_debit",
    )

    await post_debit.invoke(definition.instantiate(), replay_ctx)

    after = await ledger_rows(pool, workflow_id)
    assert after == before, "the replayed step's ON CONFLICT DO NOTHING must make it a no-op"
