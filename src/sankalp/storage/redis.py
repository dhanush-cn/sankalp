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


def create_redis(
    url: str | None = None,
    *,
    settings: Settings | None = None,
    socket_timeout_seconds: float | None = None,
) -> Redis:
    """Build a Redis client against ``url``, defaulting to ``settings.redis_url``.

    Synchronous, like ``redis.asyncio.Redis.from_url`` itself -- there is no I/O to await
    until the first command, so this mirrors ``create_pool`` in spirit without needing to be a
    coroutine. Callers close it with ``await client.aclose()``.

    ``socket_timeout_seconds`` defaults to ``settings.outbox_redis_timeout_seconds`` (5s --
    right for a drain batch that can afford to wait), but the rate limiter
    (``resilience/ratelimit.py``) passes ``settings.ratelimit_redis_timeout_seconds`` (50ms)
    instead: that call sits inline on the request path, where the whole point of the circuit
    breaker in front of it is to stop paying a timeout at all once Redis is known to be down,
    and a 5s budget would mean 5s of stalled requests per failure before the breaker even has a
    chance to open.
    """
    settings = settings or get_settings()
    timeout = (
        settings.outbox_redis_timeout_seconds
        if socket_timeout_seconds is None
        else socket_timeout_seconds
    )
    return Redis.from_url(
        url or str(settings.redis_url),
        decode_responses=True,
        socket_timeout=timeout,
        socket_connect_timeout=timeout,
    )
