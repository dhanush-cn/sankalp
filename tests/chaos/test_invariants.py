"""The invariant checks, proved able to fail.

A check that has never been seen to fail is not a check -- it is a query that happens to
return nothing, and after a chaos run it would report "all clear" for a system in any state
at all. So each check here is run twice: once against the clean, truncated database, where
it must pass, and once against a database with exactly the damage it exists to find, where
it must raise and must name the offending row. The identifier matters as much as the raise:
a chaos scenario's whole value is telling the reader which workflow went wrong.

check_reconciliation is absent on purpose. It already has this pair of proofs, in
tests/test_ledger.py (test_reconciliation_passes_for_a_balanced_transfer and
test_reconciliation_catches_an_unbalanced_transfer), where the query used to live.

Everything here runs on the OWNING role via ``owning_connection``: side_effects has no
sankalp_app grants by design, so the restricted pool cannot write the duplicate row that
makes the fourth test's damage, nor read it back.
"""

from __future__ import annotations

import json

import pytest

from chaos.invariants import (
    check_all,
    check_no_duplicate_side_effects,
    check_no_failed_dirty,
    check_no_stuck_workflows,
    check_outbox_drained,
    owning_connection,
)


async def _insert_side_effect(db, workflow_id, step_name: str) -> None:
    """One row in side_effects -- what a step writes when its effect actually commits."""
    await db.execute(
        "INSERT INTO side_effects (workflow_id, step_name) VALUES ($1, $2)",
        workflow_id,
        step_name,
    )


async def _insert_outbox_event(db, workflow_id, event_type: str = "step.completed") -> None:
    """One unpublished outbox row -- published_at defaults to NULL, which is the damage."""
    await db.execute(
        "INSERT INTO outbox (workflow_id, event_type, payload) VALUES ($1, $2, $3::jsonb)",
        workflow_id,
        event_type,
        json.dumps({"chaos": True}),
    )


async def test_a_workflow_left_running_is_reported_as_stuck(insert_workflow):
    """The fault this catches: quiescence reached with work still in flight."""
    async with owning_connection() as db:
        await check_no_stuck_workflows(db)  # clean database: nothing to report

        workflow_id = await insert_workflow(status="RUNNING", owner_id="worker-gone")

        with pytest.raises(AssertionError) as excinfo:
            await check_no_stuck_workflows(db)

    message = str(excinfo.value)
    assert str(workflow_id) in message
    assert "RUNNING" in message
    assert "worker-gone" in message


async def test_a_step_that_took_effect_twice_is_reported(insert_workflow):
    """The fault this catches: at-least-once execution that produced two effects.

    Note that both INSERTs succeed -- side_effects deliberately has no UNIQUE constraint
    (migrations/002_crash_gate.sql), because a swallowed duplicate would make the crash
    gate's count read 1 whether or not recovery worked. This check is the substitute.
    """
    workflow_id = await insert_workflow(status="SUCCESS")

    async with owning_connection() as db:
        await _insert_side_effect(db, workflow_id, "debit_source")
        await check_no_duplicate_side_effects(db)  # one effect, one step: fine

        await _insert_side_effect(db, workflow_id, "debit_source")

        with pytest.raises(AssertionError) as excinfo:
            await check_no_duplicate_side_effects(db)

    message = str(excinfo.value)
    assert str(workflow_id) in message
    assert "debit_source" in message
    assert "2 side effects" in message


async def test_an_unpublished_outbox_row_is_reported(insert_workflow):
    """The fault this catches: the drain stopped, so a committed event never left."""
    workflow_id = await insert_workflow(status="SUCCESS")

    async with owning_connection() as db:
        await check_outbox_drained(db)  # empty outbox: drained by definition

        await _insert_outbox_event(db, workflow_id, event_type="transfer.completed")

        with pytest.raises(AssertionError) as excinfo:
            await check_outbox_drained(db)

    message = str(excinfo.value)
    assert str(workflow_id) in message
    assert "transfer.completed" in message
    assert "1 outbox row(s) never published" in message


async def test_a_failed_dirty_workflow_is_reported_separately(insert_workflow):
    """The fault this catches: compensation itself failed, so money is stranded.

    The row is terminal, so :func:`check_no_stuck_workflows` is asserted to pass on it here
    -- that is precisely why FAILED_DIRTY needs a check of its own, and this test is what
    stops the two from being merged back together later.
    """
    async with owning_connection() as db:
        await check_no_failed_dirty(db)

        workflow_id = await insert_workflow(status="FAILED_DIRTY")
        await db.execute(
            "UPDATE workflows SET error = $2, current_step = $1 WHERE id = $3",
            "refund_source",
            "refund gateway returned 500 on every attempt",
            workflow_id,
        )

        await check_no_stuck_workflows(db)  # terminal: not stuck, and that is the point

        with pytest.raises(AssertionError) as excinfo:
            await check_no_failed_dirty(db)

    message = str(excinfo.value)
    assert str(workflow_id) in message
    assert "refund_source" in message
    assert "refund gateway returned 500" in message
    assert "a human must resolve" in message


async def test_check_all_reports_every_broken_invariant_not_just_the_first(insert_workflow):
    """check_all's docstring claims it does not stop at the first failure. Proved here.

    Three invariants are broken at once, one of them (the stuck workflow) ordered before
    the other two, so a check_all that returned early would report exactly one.
    """
    stuck_id = await insert_workflow(status="COMPENSATING", owner_id="worker-gone")
    dirty_id = await insert_workflow(status="FAILED_DIRTY")

    async with owning_connection() as db:
        await _insert_outbox_event(db, stuck_id)

        with pytest.raises(AssertionError) as excinfo:
            await check_all(db)

    message = str(excinfo.value)
    assert "3 of 5 post-fault invariants failed" in message
    assert "[check_no_stuck_workflows]" in message
    assert "[check_outbox_drained]" in message
    assert "[check_no_failed_dirty]" in message
    assert str(stuck_id) in message
    assert str(dirty_id) in message
