"""Shared pytest fixtures.

Isolation between tests comes from truncating tables, not from a container per session:
the crash test runs 20x and the soak runs 1,000 workflows, and standing up Postgres for
each would dominate the runtime. The price of that choice is that a misconfigured run
could truncate the *dev* database, so :func:`truncate_tables` asks the server which
database it is actually connected to and refuses to touch anything not named ``*_test``.

Every async fixture is function-scoped on purpose. ``pyproject.toml`` sets
``asyncio_default_fixture_loop_scope = "function"``; a session-scoped connection or pool
would be bound to an event loop that is closed by the time the second test runs, which
breaks exactly the repeated-run tests this suite exists for.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import asyncpg
import pytest

from sankalp.config import get_settings

#: Truncated before every test. step_outputs is listed explicitly even though it would go
#: via CASCADE, so the intent survives someone dropping the FK.
_TABLES = ("workflows", "step_outputs")


@pytest.fixture(scope="session")
def test_dsn() -> str:
    """DSN of the pytest database.

    Always ``test_database_url``, regardless of ``SANKALP_ENVIRONMENT`` -- a stray env var
    must not be able to point the suite at dev. ``Settings`` has already validated that the
    name ends in ``_test`` (src/sankalp/config.py).
    """
    return str(get_settings().test_database_url)


@pytest.fixture(autouse=True)
async def truncate_tables(test_dsn: str) -> AsyncIterator[None]:
    """Empty the test database before each test, after proving which database it is.

    The check is deliberately made against ``current_database()`` rather than against the
    DSN string: the DSN is what we asked for, this is what we got.
    """
    conn = await asyncpg.connect(test_dsn)
    try:
        database = await conn.fetchval("SELECT current_database()")
        if not database.endswith("_test"):
            pytest.fail(
                f"refusing to truncate: connected to database {database!r}, which does not "
                "end in '_test'. Check SANKALP_TEST_DATABASE_URL -- this guard is what stops "
                "a test run from wiping the dev database."
            )
        if await conn.fetchval("SELECT to_regclass('public.workflows')") is None:
            pytest.fail(
                f"database {database!r} has no 'workflows' table. Run `make migrate` "
                "(or `python -m sankalp.storage.migrate --target test`) first."
            )
        await conn.execute(f"TRUNCATE {', '.join(_TABLES)} RESTART IDENTITY CASCADE")
    finally:
        await conn.close()
    yield


@pytest.fixture
async def connect(
    test_dsn: str, truncate_tables: None
) -> AsyncIterator[Callable[[], Awaitable[asyncpg.Connection]]]:
    """Factory for additional connections, all closed at teardown.

    Concurrency tests need genuinely separate sessions -- two claimers sharing one
    connection would serialise and prove nothing about SKIP LOCKED.
    """
    opened: list[asyncpg.Connection] = []

    async def _connect() -> asyncpg.Connection:
        conn = await asyncpg.connect(test_dsn)
        opened.append(conn)
        return conn

    yield _connect

    for conn in opened:
        await conn.close()


@pytest.fixture
async def conn(connect: Callable[[], Awaitable[asyncpg.Connection]]) -> asyncpg.Connection:
    """A single connection to the test database, on an already-truncated schema."""
    return await connect()


@pytest.fixture
async def pool(test_dsn: str, truncate_tables: None) -> AsyncIterator[asyncpg.Pool]:
    """A small pool on the test database -- what the executor and worker are handed.

    Function-scoped like everything else here, because a pool binds its connections to the
    event loop that created them and ``asyncio_default_fixture_loop_scope`` gives each test
    a fresh one. Deliberately built with :func:`asyncpg.create_pool` rather than
    ``storage.pool.create_pool``, which reads ``active_database_url`` -- the suite must point
    at ``sankalp_test`` no matter what ``SANKALP_ENVIRONMENT`` says.
    """
    created = await asyncpg.create_pool(test_dsn, min_size=1, max_size=8)
    try:
        yield created
    finally:
        await created.close()


@pytest.fixture
def insert_workflow(conn: asyncpg.Connection) -> Callable[..., Awaitable[uuid.UUID]]:
    """Insert one workflow row and return its id.

    All times are expressed as offsets from the *server's* ``now()``. The dequeue query
    compares ``run_after`` and ``lease_expires_at`` against ``now()`` inside Postgres, so
    computing these in Python would make the tests sensitive to clock skew between the
    host and the container.
    """

    async def _insert(
        *,
        status: str = "PENDING",
        run_after_seconds: float = -1.0,
        lease_expires_in_seconds: float | None = None,
        owner_id: str | None = None,
        fencing_token: int = 0,
        attempt: int = 0,
        workflow_type: str = "payment_transfer",
        input: dict[str, Any] | None = None,
    ) -> uuid.UUID:
        return await conn.fetchval(
            """
            INSERT INTO workflows (
                workflow_type, idempotency_key, status, input,
                owner_id, fencing_token, attempt,
                run_after,
                lease_expires_at
            )
            VALUES (
                $1, $2, $3::workflow_status, $4::jsonb,
                $5, $6, $7,
                now() + make_interval(secs => $8::float8),
                CASE WHEN $9::float8 IS NULL THEN NULL
                     ELSE now() + make_interval(secs => $9::float8) END
            )
            RETURNING id
            """,
            workflow_type,
            f"key-{uuid.uuid4().hex}",
            status,
            json.dumps(input if input is not None else {"amount_minor": 10_000}),
            owner_id,
            fencing_token,
            attempt,
            run_after_seconds,
            lease_expires_in_seconds,
        )

    return _insert


@pytest.fixture
def expire_lease(conn: asyncpg.Connection) -> Callable[[uuid.UUID], Awaitable[None]]:
    """Push a workflow's lease into the past -- i.e. simulate the worker holding it dying.

    This is the only thing a crashed worker leaves behind that matters to the queue, which
    is the point: recovery needs no signal beyond an expired lease.
    """

    async def _expire(workflow_id: uuid.UUID) -> None:
        await conn.execute(
            "UPDATE workflows SET lease_expires_at = now() - interval '1 second' WHERE id = $1",
            workflow_id,
        )

    return _expire
