"""The asyncpg connection pool.

Sized from :class:`~sankalp.config.Settings`, which sizes it to Postgres cores x 2-4 and
**not** to HTTP or worker concurrency (docs/spec.md, Operational Notes). A pool of ~16
serves thousands of concurrent async callers, because a connection is held only for the
microseconds a statement is actually in flight; making it larger just moves contention into
Postgres, where it is more expensive.

No JSONB codec is registered, on purpose. ``queue.py`` and ``workflows.py`` take pools and
connections they do not own -- including bare ``asyncpg.connect`` handles in tests -- so
they bind JSON as text with an explicit ``::jsonb`` cast and decode reads themselves. That
is correct with or without a codec; registering one here would make the same parameter
binding double-encode and would silently split the codebase into "connections from this
function" and "every other connection".
"""

from __future__ import annotations

import asyncpg

from sankalp.config import Settings, get_settings

__all__ = ["create_pool"]


async def create_pool(
    dsn: str | None = None,
    *,
    settings: Settings | None = None,
    min_size: int | None = None,
    max_size: int | None = None,
) -> asyncpg.Pool:
    """Open a pool against ``dsn``, defaulting to the database this environment selects."""
    settings = settings or get_settings()
    return await asyncpg.create_pool(
        dsn or settings.active_database_url,
        min_size=min_size if min_size is not None else settings.db_pool_min_size,
        max_size=max_size if max_size is not None else settings.db_pool_max_size,
        command_timeout=settings.db_command_timeout_seconds,
        # Disabled because pgbouncer in transaction mode discards prepared statements
        # between checkouts, and a cached statement that the server has forgotten fails at
        # the worst possible time. The queries here are few and cheap to re-plan.
        statement_cache_size=settings.db_statement_cache_size,
    )
