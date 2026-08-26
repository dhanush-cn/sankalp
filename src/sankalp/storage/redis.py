"""The Redis client the drain XADDs through.

Mirrors :func:`sankalp.storage.pool.create_pool`'s shape deliberately -- same optional
overrides, same settings resolution -- so opening and closing a Redis client reads like
opening and closing the Postgres pool it always runs alongside.

``socket_timeout`` matters more here than on an ordinary cache client: the drain holds a
Postgres transaction (and the row locks ``FOR UPDATE SKIP LOCKED`` took) open across the
``XADD``, so a Redis call that hangs forever would pin that transaction open too. Bounding the
call is what turns "Redis is unreachable" into a retried batch instead of a stuck worker.
"""

from __future__ import annotations

from redis.asyncio import Redis

from sankalp.config import Settings, get_settings

__all__ = ["create_redis"]


def create_redis(url: str | None = None, *, settings: Settings | None = None) -> Redis:
    """Build a Redis client against ``url``, defaulting to ``settings.redis_url``.

    Synchronous, like ``redis.asyncio.Redis.from_url`` itself -- there is no I/O to await
    until the first command, so this mirrors ``create_pool`` in spirit without needing to be a
    coroutine. Callers close it with ``await client.aclose()``.
    """
    settings = settings or get_settings()
    return Redis.from_url(
        url or str(settings.redis_url),
        decode_responses=True,
        socket_timeout=settings.outbox_redis_timeout_seconds,
        socket_connect_timeout=settings.outbox_redis_timeout_seconds,
    )
