"""Shared post-fault assertions for the Phase 4 chaos suite.

Every scenario injects a different fault -- a SIGKILLed worker, an unreachable Redis, a
paused Postgres -- waits for quiescence, and then calls exactly these checks. They live
here, once, so no scenario can quietly assert less than its neighbour: an invariant that
only some scenarios check is an invariant the suite will eventually stop finding breaks in.

The five, stated as queries:

  1. reconciliation      -- every transfer nets to zero (docs/spec.md, "Reconciliation")
  2. nothing in flight   -- no workflow left PENDING / RUNNING / COMPENSATING
  3. outbox drained      -- no row left with published_at IS NULL
  4. no double effects   -- no (workflow_id, step_name) twice in side_effects
  5. no FAILED_DIRTY     -- compensation itself did not fail

2 and 5 are deliberately separate checks. FAILED_DIRTY is terminal, so 2 passes on such a
row: it is not stuck, it is finished badly, with money stranded mid-unwind and a human
required. That outcome gets its own line rather than being folded into "still in flight",
because the two demand completely different responses.

4 is an assertion and not a unique constraint on purpose. migrations/002_crash_gate.sql
says why at length: ``step_attempts = 2, side_effects = 1`` is the measurement that proves
at-least-once execution produced exactly-once *effects*. A UNIQUE (workflow_id, step_name)
there -- or an ON CONFLICT DO NOTHING in the demo steps -- would make "one row" true whether
or not recovery worked, and the gate would pass while asserting nothing. So the duplicate
has to be found by looking, here.

Each check has a revert-proof in tests/chaos/test_invariants.py, which breaks the invariant
on purpose and asserts the check says so -- except :func:`check_reconciliation`, whose pair
of proofs live with the constant's original home in tests/test_ledger.py.

Database role
-------------
``side_effects``, ``step_attempts`` and ``crash_gates`` have NO ``sankalp_app`` grants, by
design (CLAUDE.md; migrations/004_restricted_role.sql). Check 4 reads ``side_effects``, so
these checks must run as the OWNING role -- ``test_database_url`` / ``active_database_url``
-- never the restricted one. ``storage.pool.create_pool()`` defaults to the restricted role,
so do not hand a scenario's engine pool to these functions; open the connection with
:func:`owning_connection`, which names the owner DSN explicitly.

The failure mode is loud rather than silent -- the restricted role gets permission denied,
not an empty result -- and :func:`check_all` deliberately does not catch it: a chaos run
that could not read ``side_effects`` has not checked invariant 4, and must not report that
it did.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import asynccontextmanager
from typing import Any, Protocol

import asyncpg

from sankalp.config import get_settings


class Queryable(Protocol):
    """The two methods every check needs.

    Both :class:`asyncpg.Connection` and :class:`asyncpg.Pool` satisfy this, so a scenario
    passes whichever it already holds open -- as long as it is on the owning role.
    """

    async def fetch(self, query: str, *args: Any) -> list[asyncpg.Record]: ...

    async def fetchval(self, query: str, *args: Any) -> Any: ...


#: docs/spec.md, "Reconciliation" -- verbatim. Every transfer must net to zero, so this
#: returns the transfers that do not. It must return no rows, always.
#:
#: Moved here from tests/test_ledger.py so the chaos suite and the ledger tests run the
#: same text. test_ledger.py imports it and still owns the two tests that prove it works in
#: both directions -- balanced returns nothing, unbalanced returns the offending transfer --
#: which is what stops this constant from rotting into a query that happens to return
#: nothing.
RECONCILE = """
SELECT transfer_id,
       SUM(CASE WHEN direction='DEBIT'  THEN amount_minor ELSE 0 END) AS debits,
       SUM(CASE WHEN direction='CREDIT' THEN amount_minor ELSE 0 END) AS credits
FROM ledger_entries
GROUP BY transfer_id
HAVING SUM(CASE WHEN direction='DEBIT'  THEN amount_minor ELSE 0 END)
    <> SUM(CASE WHEN direction='CREDIT' THEN amount_minor ELSE 0 END)
"""

#: Exactly the three statuses in idx_workflows_claimable's predicate
#: (migrations/001_core_schema.sql:108). The other three -- SUCCESS, COMPENSATED,
#: FAILED_DIRTY -- are terminal. Keep the two lists in step: a status added to the index
#: predicate but not here would leave a way to be stuck that nothing checks.
NON_TERMINAL_STATUSES = ("PENDING", "RUNNING", "COMPENSATING")


@asynccontextmanager
async def owning_connection(dsn: str | None = None) -> AsyncIterator[asyncpg.Connection]:
    """Open a connection as the owning role, closed on the way out.

    Defaults to ``test_database_url`` rather than ``active_database_url`` for the same
    reason tests/conftest.py's ``test_dsn`` does: a stray ``SANKALP_ENVIRONMENT`` must not
    be able to point a chaos run at the dev database. Pass ``dsn`` to check another one.

    Built with :func:`asyncpg.connect` and not ``storage.pool.create_pool`` -- that helper
    defaults to ``sankalp_app``, which cannot read ``side_effects`` at all.
    """
    conn = await asyncpg.connect(dsn or str(get_settings().test_database_url))
    try:
        yield conn
    finally:
        await conn.close()


async def unpublished_count(db: Queryable) -> int:
    """Outbox rows the drain has not published.

    Lives here rather than in tests/test_outbox_drain.py, which imports it, for the same
    reason :data:`RECONCILE` does: invariant 3 and the drain's own tests have to be asking
    the database the same question. That file's tests continue to guard it.
    """
    return await db.fetchval("SELECT count(*) FROM outbox WHERE published_at IS NULL")


# ---------------------------------------------------------------------------
# The five checks. Each raises AssertionError naming the offending rows -- an
# invariant that reports only "failed" costs the reader the whole investigation.
# ---------------------------------------------------------------------------


async def check_reconciliation(db: Queryable) -> None:
    """Every transfer's debits equal its credits."""
    rows = await db.fetch(RECONCILE)
    if not rows:
        return
    detail = "\n".join(
        f"    transfer {r['transfer_id']}: debits={r['debits']} credits={r['credits']} "
        f"(delta={r['debits'] - r['credits']} minor units)"
        for r in rows
    )
    raise AssertionError(f"{len(rows)} transfer(s) do not net to zero:\n{detail}")


async def check_no_stuck_workflows(db: Queryable) -> None:
    """No workflow left in a non-terminal status once the system is quiescent."""
    rows = await db.fetch(
        """
        SELECT id, workflow_type, status::text AS status, current_step,
               attempt, max_attempts, owner_id, lease_expires_at, run_after
        FROM workflows
        WHERE status = ANY($1::workflow_status[])
        ORDER BY status, id
        """,
        list(NON_TERMINAL_STATUSES),
    )
    if not rows:
        return
    detail = "\n".join(
        f"    {r['id']} {r['status']:<12} type={r['workflow_type']} "
        f"step={r['current_step']} attempt={r['attempt']}/{r['max_attempts']} "
        f"owner={r['owner_id']} lease_expires_at={r['lease_expires_at']} "
        f"run_after={r['run_after']}"
        for r in rows
    )
    raise AssertionError(
        f"{len(rows)} workflow(s) still non-terminal "
        f"({', '.join(NON_TERMINAL_STATUSES)}):\n{detail}"
    )


async def check_outbox_drained(db: Queryable) -> None:
    """No outbox row left unpublished."""
    count = await unpublished_count(db)
    if count == 0:
        return
    rows = await db.fetch(
        """
        SELECT id, workflow_id, event_type, attempts, created_at
        FROM outbox
        WHERE published_at IS NULL
        ORDER BY id
        LIMIT 20
        """
    )
    detail = "\n".join(
        f"    outbox #{r['id']} workflow={r['workflow_id']} type={r['event_type']} "
        f"attempts={r['attempts']} created_at={r['created_at']}"
        for r in rows
    )
    elided = f"\n    ... and {count - len(rows)} more" if count > len(rows) else ""
    raise AssertionError(f"{count} outbox row(s) never published:\n{detail}{elided}")


async def check_no_duplicate_side_effects(db: Queryable) -> None:
    """A step's side effect committed at most once, however many times it was attempted."""
    rows = await db.fetch(
        """
        SELECT workflow_id, step_name, count(*) AS effects
        FROM side_effects
        GROUP BY workflow_id, step_name
        HAVING count(*) > 1
        ORDER BY count(*) DESC, workflow_id
        """
    )
    if not rows:
        return
    detail = "\n".join(
        f"    {r['workflow_id']} step={r['step_name']}: {r['effects']} side effects"
        for r in rows
    )
    raise AssertionError(
        f"{len(rows)} (workflow_id, step_name) pair(s) took effect more than once -- "
        f"a step's side effect ran twice:\n{detail}"
    )


async def check_no_failed_dirty(db: Queryable) -> None:
    """No workflow whose compensation itself failed.

    Terminal, so :func:`check_no_stuck_workflows` passes on these -- which is exactly why
    this is a check of its own. FAILED_DIRTY means the unwind could not complete: money is
    somewhere unintended and a human has to resolve it.
    """
    rows = await db.fetch(
        """
        SELECT id, workflow_type, current_step, attempt, error, updated_at
        FROM workflows
        WHERE status = 'FAILED_DIRTY'
        ORDER BY updated_at
        """
    )
    if not rows:
        return
    detail = "\n".join(
        f"    {r['id']} type={r['workflow_type']} step={r['current_step']} "
        f"attempt={r['attempt']} at={r['updated_at']} error={r['error']!r}"
        for r in rows
    )
    raise AssertionError(
        f"{len(rows)} workflow(s) in FAILED_DIRTY -- compensation failed, "
        f"a human must resolve these:\n{detail}"
    )


#: Every check, in the order :func:`check_all` runs them. A new invariant is added here
#: and is then part of every scenario, which is the reason this module exists.
ALL_CHECKS: Sequence[Callable[[Queryable], Awaitable[None]]] = (
    check_reconciliation,
    check_no_stuck_workflows,
    check_outbox_drained,
    check_no_duplicate_side_effects,
    check_no_failed_dirty,
)


async def check_all(db: Queryable) -> None:
    """Run all five and report every failure, not just the first.

    Stopping at the first would make each chaos run report one broken invariant and hide
    the rest, so the shape of the damage would take as many runs to learn as there are
    invariants -- and a fault that is hard to reproduce might not survive that many. Three
    broken invariants and one broken invariant are different diagnoses; the run should say
    which it saw.

    Only AssertionError is collected. Anything else -- a permission denied from running on
    the restricted role, a dropped connection -- propagates immediately, because those mean
    the check did not run, not that it passed.
    """
    failures: list[str] = []
    for check in ALL_CHECKS:
        try:
            await check(db)
        except AssertionError as exc:
            failures.append(f"  [{check.__name__}] {exc}")
    if failures:
        raise AssertionError(
            f"{len(failures)} of {len(ALL_CHECKS)} post-fault invariants failed:\n"
            + "\n".join(failures)
        )
