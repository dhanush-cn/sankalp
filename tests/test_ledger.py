"""003_saga.sql, proved. See docs/spec.md, Phase 2.

The migration makes four claims that the database is supposed to enforce on its own,
and a comment asserting them is not enforcement. Each one is checked here by trying to
violate it:

  * ledger_entries is append-only -- no UPDATE, no DELETE, in any shape,
  * UNIQUE (workflow_id, step_name, account_id, direction) makes a replayed step's
    second posting a constraint violation rather than a second debit,
  * a workflow with money posted against it cannot be deleted (no ON DELETE CASCADE),
  * idx_outbox_unpublished is genuinely partial.

Plus the deliberate gap: TRUNCATE is *not* blocked, and that is asserted too, so the
decision is pinned rather than merely remembered.
"""

from __future__ import annotations

import uuid

import asyncpg
import pytest

#: docs/spec.md, "Reconciliation" -- verbatim. Every transfer must net to zero, so this
#: returns the transfers that do not. It must return no rows, always.
RECONCILE = """
SELECT transfer_id,
       SUM(CASE WHEN direction='DEBIT'  THEN amount_minor ELSE 0 END) AS debits,
       SUM(CASE WHEN direction='CREDIT' THEN amount_minor ELSE 0 END) AS credits
FROM ledger_entries
GROUP BY transfer_id
HAVING SUM(CASE WHEN direction='DEBIT'  THEN amount_minor ELSE 0 END)
    <> SUM(CASE WHEN direction='CREDIT' THEN amount_minor ELSE 0 END)
"""


async def _post(conn, workflow_id, **overrides):
    """Insert one ledger entry and return its id. Amounts are minor units -- paise."""
    entry = {
        "transfer_id": uuid.uuid4(),
        "step_name": "debit_source",
        "account_id": "acct:source",
        "direction": "DEBIT",
        "amount_minor": 250_00,
        "currency": "INR",
    } | overrides
    return await conn.fetchval(
        """
        INSERT INTO ledger_entries (
            transfer_id, workflow_id, step_name, account_id,
            direction, amount_minor, currency
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING id
        """,
        entry["transfer_id"],
        workflow_id,
        entry["step_name"],
        entry["account_id"],
        entry["direction"],
        entry["amount_minor"],
        entry["currency"],
    )


# ---------------------------------------------------------------------------
# 1. Append-only, enforced by the database rather than by convention.
# ---------------------------------------------------------------------------


async def test_update_is_rejected_and_the_row_survives(conn, insert_workflow):
    """A committed entry cannot be altered, and the failed attempt changes nothing."""
    workflow_id = await insert_workflow()
    await _post(conn, workflow_id, amount_minor=250_00)

    with pytest.raises(asyncpg.RestrictViolationError) as excinfo:
        await conn.execute("UPDATE ledger_entries SET amount_minor = 1")

    assert "append-only" in str(excinfo.value)
    assert await conn.fetchval("SELECT amount_minor FROM ledger_entries") == 250_00


async def test_delete_is_rejected_and_the_row_survives(conn, insert_workflow):
    workflow_id = await insert_workflow()
    await _post(conn, workflow_id)

    with pytest.raises(asyncpg.RestrictViolationError):
        await conn.execute("DELETE FROM ledger_entries")

    assert await conn.fetchval("SELECT count(*) FROM ledger_entries") == 1


async def test_the_ban_is_on_the_statement_not_the_row(conn, insert_workflow):
    """The reason the trigger is FOR EACH STATEMENT and not FOR EACH ROW.

    A row-level trigger only fires once something has matched, which would make the ban
    conditional on the WHERE clause finding a row: ``DELETE FROM ledger_entries WHERE
    false`` would report DELETE 0 and succeed. At statement level there is no shape of
    UPDATE or DELETE that this table accepts -- not one that matches nothing, and not
    one issued against an empty table.
    """
    workflow_id = await insert_workflow()
    await _post(conn, workflow_id)

    for statement in (
        "UPDATE ledger_entries SET amount_minor = 1 WHERE false",
        "DELETE FROM ledger_entries WHERE false",
        "UPDATE ledger_entries SET amount_minor = 1 WHERE id = -1",
        "DELETE FROM ledger_entries WHERE id = -1",
    ):
        with pytest.raises(asyncpg.RestrictViolationError):
            await conn.execute(statement)

    # ...and on an empty table, where there is no row to protect at all.
    await conn.execute("TRUNCATE ledger_entries")
    with pytest.raises(asyncpg.RestrictViolationError):
        await conn.execute("DELETE FROM ledger_entries")


async def test_the_guard_has_no_conditional_bypass(conn):
    """The trigger is registered on UPDATE and DELETE only, with no WHEN clause.

    Read from the catalog rather than from the migration text, so this fails if someone
    adds a TRUNCATE event, drops to row level, or attaches a condition -- the three edits
    that would each leave the guard looking present while no longer being absolute.
    """
    row = await conn.fetchrow(
        """
        SELECT tgname,
               (tgtype & 1)  = 0  AS is_statement_level,
               (tgtype & 2)  <> 0 AS is_before,
               (tgtype & 16) <> 0 AS on_update,
               (tgtype & 8)  <> 0 AS on_delete,
               (tgtype & 32) <> 0 AS on_truncate,
               (tgtype & 4)  <> 0 AS on_insert,
               tgqual IS NOT NULL AS has_when_clause
        FROM pg_trigger
        WHERE tgrelid = 'ledger_entries'::regclass AND NOT tgisinternal
        """
    )
    assert row is not None, "the append-only trigger is missing from ledger_entries"
    assert row["tgname"] == "trg_ledger_entries_append_only"
    assert row["is_statement_level"] and row["is_before"]
    assert row["on_update"] and row["on_delete"]
    assert not row["on_insert"], "appending must stay legal -- that is the whole table"
    assert not row["has_when_clause"], "a guard with a condition in it has a hole in it"
    # Deliberate, and documented in 003_saga.sql: see the TRUNCATE test below.
    assert not row["on_truncate"]


# ---------------------------------------------------------------------------
# 2. The idempotency guard -- what makes a replayed step safe.
# ---------------------------------------------------------------------------


async def test_replayed_posting_is_a_constraint_violation_not_a_second_debit(
    conn, insert_workflow
):
    """The crash window this constraint exists for.

    A step posts its debit, the process dies before its step_outputs checkpoint commits,
    and the replay posts again. Without uq_ledger_entry the account is debited twice.
    """
    workflow_id = await insert_workflow()
    await _post(conn, workflow_id, step_name="debit_source", account_id="acct:A")

    with pytest.raises(asyncpg.UniqueViolationError) as excinfo:
        # A different transfer_id and amount: the constraint keys on who/what/where,
        # deliberately not on the values, so a replay cannot slip past by differing.
        await _post(
            conn,
            workflow_id,
            step_name="debit_source",
            account_id="acct:A",
            amount_minor=1,
        )

    assert "uq_ledger_entry" in str(excinfo.value)
    assert await conn.fetchval("SELECT count(*) FROM ledger_entries") == 1


async def test_on_conflict_do_nothing_makes_the_replay_a_no_op(conn, insert_workflow):
    """How a step actually writes: the second run is a no-op, not an error to handle."""
    workflow_id = await insert_workflow()
    transfer_id = uuid.uuid4()

    for _ in range(3):
        await conn.execute(
            """
            INSERT INTO ledger_entries (
                transfer_id, workflow_id, step_name, account_id,
                direction, amount_minor, currency
            )
            VALUES ($1, $2, 'debit_source', 'acct:A', 'DEBIT', 250_00, 'INR')
            ON CONFLICT ON CONSTRAINT uq_ledger_entry DO NOTHING
            """,
            transfer_id,
            workflow_id,
        )

    assert await conn.fetchval("SELECT count(*) FROM ledger_entries") == 1
    assert await conn.fetchval("SELECT sum(amount_minor) FROM ledger_entries") == 250_00


async def test_the_other_half_of_the_pair_is_not_blocked(conn, insert_workflow):
    """The constraint must not stop a step posting both sides of its double entry."""
    workflow_id = await insert_workflow()
    transfer_id = uuid.uuid4()

    await _post(
        conn, workflow_id, transfer_id=transfer_id, account_id="acct:A", direction="DEBIT"
    )
    await _post(
        conn, workflow_id, transfer_id=transfer_id, account_id="acct:B", direction="CREDIT"
    )

    assert await conn.fetchval("SELECT count(*) FROM ledger_entries") == 2


# ---------------------------------------------------------------------------
# 3. Column constraints -- sign lives in `direction`, never in the amount.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("amount", [0, -1, -250_00])
async def test_amount_must_be_positive(conn, insert_workflow, amount):
    """A zero or negative amount would put sign in two places at once."""
    workflow_id = await insert_workflow()
    with pytest.raises(asyncpg.CheckViolationError):
        await _post(conn, workflow_id, amount_minor=amount)


@pytest.mark.parametrize("direction", ["debit", "Credit", "TRANSFER", ""])
async def test_direction_is_constrained_to_the_two_valid_values(
    conn, insert_workflow, direction
):
    workflow_id = await insert_workflow()
    with pytest.raises(asyncpg.CheckViolationError):
        await _post(conn, workflow_id, direction=direction)


async def test_ledger_pins_its_workflow(conn, insert_workflow):
    """No ON DELETE CASCADE here, unlike every other table in the schema.

    Cascading would mean a stray DELETE on workflows silently erases the record of a
    transfer that really happened. The FK refusing is the intended behaviour.
    """
    workflow_id = await insert_workflow()
    await _post(conn, workflow_id)

    with pytest.raises(asyncpg.ForeignKeyViolationError):
        await conn.execute("DELETE FROM workflows WHERE id = $1", workflow_id)


# ---------------------------------------------------------------------------
# 4. The outbox's partial index.
# ---------------------------------------------------------------------------


async def test_outbox_unpublished_index_is_partial(conn):
    """Assert the predicate, not merely that an index exists.

    An "index is present" check would still pass if someone dropped the WHERE clause,
    which is exactly the change that makes the index grow with history instead of with
    the backlog.
    """
    indexdef = await conn.fetchval(
        "SELECT indexdef FROM pg_indexes WHERE indexname = 'idx_outbox_unpublished'"
    )
    assert indexdef is not None, "idx_outbox_unpublished is missing"
    assert "WHERE (published_at IS NULL)" in indexdef


async def test_published_rows_leave_the_index(conn, insert_workflow):
    """Stamping published_at is what removes a row from the drain's working set."""
    workflow_id = await insert_workflow()
    for _ in range(3):
        await conn.execute(
            "INSERT INTO outbox (workflow_id, event_type, payload) "
            "VALUES ($1, 'step.completed', '{}'::jsonb)",
            workflow_id,
        )

    unpublished = "SELECT count(*) FROM outbox WHERE published_at IS NULL"
    assert await conn.fetchval(unpublished) == 3

    await conn.execute(
        "UPDATE outbox SET published_at = now() "
        "WHERE id IN (SELECT id FROM outbox ORDER BY id LIMIT 2)"
    )
    assert await conn.fetchval(unpublished) == 1


# ---------------------------------------------------------------------------
# 5. Reconciliation -- the invariant the Phase 2 gate turns on.
# ---------------------------------------------------------------------------


async def test_reconciliation_passes_for_a_balanced_transfer(conn, insert_workflow):
    workflow_id = await insert_workflow()
    transfer_id = uuid.uuid4()

    await _post(
        conn,
        workflow_id,
        transfer_id=transfer_id,
        account_id="acct:A",
        direction="DEBIT",
        amount_minor=250_00,
    )
    await _post(
        conn,
        workflow_id,
        transfer_id=transfer_id,
        account_id="acct:B",
        direction="CREDIT",
        amount_minor=250_00,
    )

    assert await conn.fetch(RECONCILE) == []


async def test_reconciliation_catches_an_unbalanced_transfer(conn, insert_workflow):
    """The other direction, so the query is proved able to fail.

    A reconciliation that has never been seen to return a row is not a reconciliation --
    it is a query that happens to return nothing.
    """
    workflow_id = await insert_workflow()
    transfer_id = uuid.uuid4()

    await _post(
        conn,
        workflow_id,
        transfer_id=transfer_id,
        account_id="acct:A",
        direction="DEBIT",
        amount_minor=250_00,
    )
    await _post(
        conn,
        workflow_id,
        transfer_id=transfer_id,
        account_id="acct:B",
        direction="CREDIT",
        amount_minor=200_00,  # 50.00 short -- money would have vanished
    )

    rows = await conn.fetch(RECONCILE)
    assert len(rows) == 1
    assert rows[0]["transfer_id"] == transfer_id
    assert rows[0]["debits"] == 250_00
    assert rows[0]["credits"] == 200_00


# ---------------------------------------------------------------------------
# 6. The documented gap, pinned.
# ---------------------------------------------------------------------------


async def test_truncate_is_deliberately_not_blocked(conn, insert_workflow):
    """TRUNCATE succeeds, on purpose. This is not an oversight being enshrined.

    The append-only trigger covers the row-mutation corruption paths -- UPDATE and DELETE.
    TRUNCATE is a separate trigger event and is left outside the guard so that
    tests/conftest.py can empty this table between tests, which is where the whole suite's
    isolation comes from. Blocking it would take the suite down with it.

    Asserted here rather than left implicit: if someone adds a BEFORE TRUNCATE trigger,
    this test fails with a message naming the reason, instead of every other test in the
    suite failing inside the fixture for no visible cause.

    The honest state of this gap, per 003_saga.sql: there is no restricted application
    role yet -- everything connects as the `sankalp` superuser, which also owns the table
    -- so nothing currently stops a TRUNCATE. The control is a Phase 3 `sankalp_app` role
    with TRUNCATE, DELETE and UPDATE revoked, and it does not exist yet.
    """
    workflow_id = await insert_workflow()
    await _post(conn, workflow_id)

    await conn.execute("TRUNCATE ledger_entries")

    assert await conn.fetchval("SELECT count(*) FROM ledger_entries") == 0
