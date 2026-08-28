"""The API surface (docs/spec.md, "Phase 1 API"), proved against a real Postgres.

Three things this file exists to nail down, each because getting it wrong either loses the
idempotency guarantee or corrupts an in-flight workflow:

  * ``POST /workflows`` never mutates an existing row on a duplicate submit -- proved by
    submitting the same key twice with *different* bodies and checking the stored row still
    matches request 1.
  * The duplicate-submit race is actually race-free under real concurrency, not just
    sequentially correct -- proved with concurrent submits, not a mocked race.
  * ``POST /workflows/{id}/cancel`` cannot touch a workflow that isn't PENDING/RUNNING.

No mocks: real FastAPI app, real asyncpg pool, real Postgres. The app's pool is opened
against ``settings.active_app_database_url`` with ``environment`` forced to ``"test"``
explicitly (not inherited from the ambient ``SANKALP_ENVIRONMENT``), so every request in this
file runs over the restricted ``sankalp_app`` role (migrations/004_restricted_role.sql) on
``sankalp_test`` -- which is also what proves 004's grants are sufficient: any missing grant
surfaces as ``InsufficientPrivilegeError`` from an ordinary test, not a bespoke check.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import AsyncIterator

import asyncpg
import httpx
import pytest

from sankalp.config import Settings
from sankalp.engine.definition import StepContext, clear_registry, step, workflow
from sankalp.storage.pool import create_pool

WORKFLOW_TYPE = "api_test_workflow"


@pytest.fixture(autouse=True)
def isolated_registry() -> AsyncIterator[None]:
    """Each test registers its own throwaway workflow type into an empty registry."""
    clear_registry()

    @workflow(WORKFLOW_TYPE)
    class ApiTestWorkflow:
        @step(seq=1)
        async def only_step(self, ctx: StepContext) -> dict[str, str]:
            return {"ok": "true"}

    yield
    clear_registry()


@pytest.fixture
async def app_pool(truncate_tables: None) -> AsyncIterator[asyncpg.Pool]:
    """The restricted-role pool the API actually runs on, forced onto sankalp_test.

    ``environment="test"`` is passed as an explicit field, which pydantic-settings gives
    priority over any ambient ``SANKALP_ENVIRONMENT`` -- so this pool is deterministically
    ``sankalp_app`` on ``sankalp_test`` no matter how this file is invoked.
    """
    settings = Settings(environment="test")
    pool = await create_pool(settings=settings)
    try:
        yield pool
    finally:
        await pool.close()


@pytest.fixture
async def client(app_pool: asyncpg.Pool) -> AsyncIterator[httpx.AsyncClient]:
    """An HTTP client wired directly to the app over the restricted-role pool.

    Deliberately does not run the app's own lifespan (no ``LifespanManager``): assigning
    ``app.state.pool`` directly is what lets this fixture hand the app a pool pinned to
    ``sankalp_test`` regardless of ambient environment, which the lifespan's own
    ``create_pool()`` call (reading ambient settings) cannot guarantee.
    """
    from sankalp.api.main import app

    app.state.pool = app_pool
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


async def _row(
    conn: asyncpg.Connection, workflow_type: str, idempotency_key: str
) -> asyncpg.Record | None:
    return await conn.fetchrow(
        "SELECT * FROM workflows WHERE workflow_type = $1 AND idempotency_key = $2",
        workflow_type,
        idempotency_key,
    )


# ---------------------------------------------------------------------------
# Restricted role: the grants 004 gives sankalp_app are exercised by every test below
# already; this pins down the other direction -- it must NOT be the owning role either.
# ---------------------------------------------------------------------------


async def test_api_pool_cannot_truncate(app_pool: asyncpg.Pool) -> None:
    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        await app_pool.execute("TRUNCATE workflows")


# ---------------------------------------------------------------------------
# POST /workflows
# ---------------------------------------------------------------------------


async def test_submit_requires_idempotency_key(client: httpx.AsyncClient) -> None:
    resp = await client.post("/workflows", json={"workflow_type": WORKFLOW_TYPE, "input": {}})
    assert resp.status_code == 422


async def test_submit_rejects_unregistered_workflow_type(client: httpx.AsyncClient) -> None:
    resp = await client.post(
        "/workflows",
        json={"workflow_type": "not_a_real_type", "input": {}},
        headers={"Idempotency-Key": str(uuid.uuid4())},
    )
    assert resp.status_code == 422


async def test_first_submit_creates_pending_workflow(
    client: httpx.AsyncClient, conn: asyncpg.Connection
) -> None:
    key = str(uuid.uuid4())
    resp = await client.post(
        "/workflows",
        json={"workflow_type": WORKFLOW_TYPE, "input": {"amount_minor": 100}},
        headers={"Idempotency-Key": key},
    )
    assert resp.status_code == 201
    body = resp.json()
    assert body["status"] == "PENDING"
    assert body["input"] == {"amount_minor": 100}

    row = await _row(conn, WORKFLOW_TYPE, key)
    assert row is not None
    assert str(row["id"]) == body["id"]
    assert row["status"] == "PENDING"


async def test_duplicate_submit_never_mutates_the_row(
    client: httpx.AsyncClient, conn: asyncpg.Connection
) -> None:
    """The whole point of ON CONFLICT DO NOTHING + re-select: request 2's body is ignored."""
    key = str(uuid.uuid4())
    headers = {"Idempotency-Key": key}

    first = await client.post(
        "/workflows",
        json={"workflow_type": WORKFLOW_TYPE, "input": {"amount_minor": 1}},
        headers=headers,
    )
    assert first.status_code == 201
    first_id = first.json()["id"]

    second = await client.post(
        "/workflows",
        json={"workflow_type": WORKFLOW_TYPE, "input": {"amount_minor": 999_999}},
        headers=headers,
    )
    assert second.status_code == 200
    assert second.json()["id"] == first_id
    # The duplicate's differing body must not have won.
    assert second.json()["input"] == {"amount_minor": 1}

    row = await _row(conn, WORKFLOW_TYPE, key)
    assert row is not None
    assert json.loads(row["input"]) == {"amount_minor": 1}


async def test_same_key_different_type_is_a_distinct_workflow(
    client: httpx.AsyncClient,
) -> None:
    clear_registry()

    @workflow(WORKFLOW_TYPE)
    class A:
        @step(seq=1)
        async def s(self, ctx: StepContext) -> dict[str, str]:
            return {}

    other_type = "api_test_workflow_other"

    @workflow(other_type)
    class B:
        @step(seq=1)
        async def s(self, ctx: StepContext) -> dict[str, str]:
            return {}

    key = str(uuid.uuid4())
    r1 = await client.post(
        "/workflows",
        json={"workflow_type": WORKFLOW_TYPE, "input": {}},
        headers={"Idempotency-Key": key},
    )
    r2 = await client.post(
        "/workflows",
        json={"workflow_type": other_type, "input": {}},
        headers={"Idempotency-Key": key},
    )
    assert r1.status_code == 201
    assert r2.status_code == 201
    assert r1.json()["id"] != r2.json()["id"]


async def test_concurrent_duplicate_submits_produce_exactly_one_row(
    client: httpx.AsyncClient, conn: asyncpg.Connection
) -> None:
    """The speculative-insertion / re-select race, under real concurrency.

    Every racer's INSERT either wins outright or blocks until the winner commits before its
    own statement (or its follow-up SELECT) resolves -- so this is a guaranteed outcome, not
    a flaky tie, per the mechanics documented in storage/workflows.py::submit_workflow.
    """
    key = str(uuid.uuid4())
    headers = {"Idempotency-Key": key}
    body = {"workflow_type": WORKFLOW_TYPE, "input": {"race": True}}

    responses = await asyncio.gather(
        *(client.post("/workflows", json=body, headers=headers) for _ in range(10))
    )

    statuses = sorted(r.status_code for r in responses)
    assert statuses == [200] * 9 + [201]

    ids = {r.json()["id"] for r in responses}
    assert len(ids) == 1

    count = await conn.fetchval(
        "SELECT count(*) FROM workflows WHERE workflow_type = $1 AND idempotency_key = $2",
        WORKFLOW_TYPE,
        key,
    )
    assert count == 1


# ---------------------------------------------------------------------------
# GET /workflows/{id}
# ---------------------------------------------------------------------------


async def test_get_unknown_workflow_is_404(client: httpx.AsyncClient) -> None:
    resp = await client.get(f"/workflows/{uuid.uuid4()}")
    assert resp.status_code == 404


async def test_get_reports_completed_steps(
    client: httpx.AsyncClient, insert_workflow, conn: asyncpg.Connection
) -> None:
    workflow_id = await insert_workflow(workflow_type=WORKFLOW_TYPE, status="RUNNING")
    await conn.execute(
        """
        INSERT INTO step_outputs (workflow_id, step_name, seq, kind, output)
        VALUES ($1, 'only_step', 1, 'FORWARD', '{}'::jsonb)
        """,
        workflow_id,
    )

    resp = await client.get(f"/workflows/{workflow_id}")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "RUNNING"
    assert body["completed_steps"] == ["only_step"]


# ---------------------------------------------------------------------------
# POST /workflows/{id}/cancel
# ---------------------------------------------------------------------------


async def test_cancel_unknown_workflow_is_404(client: httpx.AsyncClient) -> None:
    resp = await client.post(f"/workflows/{uuid.uuid4()}/cancel")
    assert resp.status_code == 404


async def test_cancel_pending_workflow_moves_to_compensating(
    client: httpx.AsyncClient, insert_workflow
) -> None:
    workflow_id = await insert_workflow(workflow_type=WORKFLOW_TYPE, status="PENDING")
    resp = await client.post(f"/workflows/{workflow_id}/cancel")
    assert resp.status_code == 200
    assert resp.json()["status"] == "COMPENSATING"


async def test_cancel_running_workflow_does_not_touch_owner(
    client: httpx.AsyncClient, insert_workflow, conn: asyncpg.Connection
) -> None:
    """A worker's ownership survives the cancel -- only status/run_after move."""
    workflow_id = await insert_workflow(
        workflow_type=WORKFLOW_TYPE,
        status="RUNNING",
        owner_id="worker-under-test",
        fencing_token=3,
        lease_expires_in_seconds=30.0,
    )
    resp = await client.post(f"/workflows/{workflow_id}/cancel")
    assert resp.status_code == 200
    assert resp.json()["status"] == "COMPENSATING"

    row = await conn.fetchrow(
        "SELECT owner_id, fencing_token FROM workflows WHERE id = $1", workflow_id
    )
    assert row["owner_id"] == "worker-under-test"
    assert row["fencing_token"] == 3


@pytest.mark.parametrize("status", ["SUCCESS", "COMPENSATED", "COMPENSATING", "FAILED_DIRTY"])
async def test_cancel_refuses_terminal_or_already_unwinding_workflows(
    client: httpx.AsyncClient, insert_workflow, status: str
) -> None:
    workflow_id = await insert_workflow(workflow_type=WORKFLOW_TYPE, status=status)
    resp = await client.post(f"/workflows/{workflow_id}/cancel")
    assert resp.status_code == 409
