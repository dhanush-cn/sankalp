"""The dequeue query, proved. See docs/spec.md, "The Dequeue Query".

Five properties have to hold before anything can be built on top of this:

  * two workers never claim the same workflow (SKIP LOCKED),
  * a RUNNING workflow whose lease expired IS claimable -- this is crash recovery,
  * a RUNNING workflow whose lease is live is NOT claimable,
  * fencing_token increments on every claim, monotonically,
  * run_after in the future keeps a row out of the batch (retry backoff depends on it).
"""

from __future__ import annotations

import asyncio

from sankalp.storage.queue import claim_workflows

LEASE = 30


async def _status_of(conn, workflow_id):
    return await conn.fetchval("SELECT status FROM workflows WHERE id = $1", workflow_id)


# ---------------------------------------------------------------------------
# 1. Concurrent claimers never overlap.
# ---------------------------------------------------------------------------


async def test_disjoint_concurrent_claims(conn, connect, insert_workflow):
    """Two claimers, ten rows, five each -- disjoint, and neither goes home empty.

    batch_size is 5 rather than 10 so that "both got work" is guaranteed by the query's
    semantics instead of by scheduling luck: whichever statement takes the row locks first
    can only take five, and SKIP LOCKED sends the other straight past them to the next
    five. A trivial 10/0 split -- which would satisfy disjointness while proving nothing --
    is not a reachable outcome here.
    """
    expected = {await insert_workflow() for _ in range(10)}

    conn_a, conn_b = await connect(), await connect()
    claimed_a, claimed_b = await asyncio.gather(
        claim_workflows(conn_a, "worker-a", LEASE, 5),
        claim_workflows(conn_b, "worker-b", LEASE, 5),
    )

    ids_a = {w.id for w in claimed_a}
    ids_b = {w.id for w in claimed_b}

    assert len(claimed_a) == 5, "worker-a claimed nothing or overran its batch size"
    assert len(claimed_b) == 5, "worker-b claimed nothing or overran its batch size"
    assert not (ids_a & ids_b), f"both workers claimed {ids_a & ids_b}"
    assert ids_a | ids_b == expected

    # And each row carries the owner that actually claimed it.
    assert all(w.owner_id == "worker-a" for w in claimed_a)
    assert all(w.owner_id == "worker-b" for w in claimed_b)


async def test_concurrent_claims_contend_for_the_same_rows(conn, connect, insert_workflow):
    """Both claimers ask for all ten. The split is whatever the race produces -- 10/0 is a
    legitimate outcome -- but no workflow may be handed to two workers."""
    expected = {await insert_workflow() for _ in range(10)}

    conn_a, conn_b = await connect(), await connect()
    claimed_a, claimed_b = await asyncio.gather(
        claim_workflows(conn_a, "worker-a", LEASE, 10),
        claim_workflows(conn_b, "worker-b", LEASE, 10),
    )

    ids_a = {w.id for w in claimed_a}
    ids_b = {w.id for w in claimed_b}

    assert not (ids_a & ids_b), f"both workers claimed {ids_a & ids_b}"
    assert len(ids_a) + len(ids_b) == 10, "a row was claimed twice or lost"
    assert ids_a | ids_b == expected


# ---------------------------------------------------------------------------
# 2 & 3. The lease is what makes a crashed worker's work recoverable.
# ---------------------------------------------------------------------------


async def test_running_with_expired_lease_is_claimable(conn, insert_workflow):
    """A worker died holding this workflow. Its lease lapsed; another worker takes over.

    There is no reaper process involved -- the expiry alone is the recovery mechanism.
    """
    workflow_id = await insert_workflow(
        status="RUNNING",
        owner_id="worker-that-died",
        lease_expires_in_seconds=-5.0,
    )

    claimed = await claim_workflows(conn, "worker-b", LEASE, 10)

    assert [w.id for w in claimed] == [workflow_id]
    assert claimed[0].owner_id == "worker-b"
    # Still RUNNING: it was mid-execution and resumes from its last checkpoint.
    assert claimed[0].status == "RUNNING"
    assert claimed[0].lease_expires_at > await conn.fetchval("SELECT now()")


async def test_running_with_live_lease_is_not_claimable(conn, insert_workflow):
    """The owner is alive and working. Nobody else may touch it."""
    await insert_workflow(
        status="RUNNING",
        owner_id="worker-a",
        lease_expires_in_seconds=60.0,
    )

    assert await claim_workflows(conn, "worker-b", LEASE, 10) == []


# ---------------------------------------------------------------------------
# 4. The fencing token.
# ---------------------------------------------------------------------------


async def test_fencing_token_increments_on_every_claim(conn, insert_workflow, expire_lease):
    """Each claim gets a strictly higher token than the last, forever.

    Monotonicity is the entire mechanism: a stalled worker holding token 7 issues a write
    while the live owner holds 8, and the `AND fencing_token = $n` on every ownership-scoped
    statement makes the stale write match zero rows. Reset the counter and the zombie wins.
    """
    workflow_id = await insert_workflow()

    tokens: list[int] = []
    attempts: list[int] = []
    for _ in range(3):
        claimed = await claim_workflows(conn, "worker-a", LEASE, 10)
        assert [w.id for w in claimed] == [workflow_id]
        tokens.append(claimed[0].fencing_token)
        attempts.append(claimed[0].attempt)
        # Simulate the holder dying, so the row is claimable again.
        await expire_lease(workflow_id)

    assert tokens == [1, 2, 3]
    assert attempts == [1, 2, 3]


# ---------------------------------------------------------------------------
# 5. Scheduling. Retry backoff writes into run_after, so this gates every retry.
# ---------------------------------------------------------------------------


async def test_future_run_after_is_excluded(conn, insert_workflow):
    """A workflow sleeping off its backoff is invisible; a due one alongside it is not."""
    await insert_workflow(run_after_seconds=300.0)
    due_id = await insert_workflow(run_after_seconds=-1.0)

    claimed = await claim_workflows(conn, "worker-a", LEASE, 10)

    assert [w.id for w in claimed] == [due_id]


# ---------------------------------------------------------------------------
# The other branch of the same statement.
# ---------------------------------------------------------------------------


async def test_compensating_workflow_stays_compensating(conn, insert_workflow):
    """Claiming an unwinding workflow must not flip it back to RUNNING -- that would send
    it forward through its steps again instead of continuing to undo them."""
    workflow_id = await insert_workflow(status="COMPENSATING")

    claimed = await claim_workflows(conn, "worker-a", LEASE, 10)

    assert [w.id for w in claimed] == [workflow_id]
    assert claimed[0].status == "COMPENSATING"
    assert await _status_of(conn, workflow_id) == "COMPENSATING"


async def test_batch_size_caps_the_claim(conn, insert_workflow):
    for _ in range(5):
        await insert_workflow()

    assert len(await claim_workflows(conn, "worker-a", LEASE, 2)) == 2


async def test_nothing_to_claim_returns_empty_list(conn):
    assert await claim_workflows(conn, "worker-a", LEASE, 10) == []
