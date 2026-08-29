"""Rate-limit enforcement as pure ASGI middleware.

**Not** ``@app.middleware("http")`` -- that decorator wraps every request in
``BaseHTTPMiddleware``, which runs the whole request inside an ``anyio`` task group and turns
the response into a stream. It exists to let middleware read/rewrite a streaming response body;
this middleware only needs to reject *before* a route runs, so it is plain ASGI instead --
``__init__(app)`` / ``async def __call__(scope, receive, send)`` -- with none of
``BaseHTTPMiddleware``'s well-documented history of swallowing client disconnects and
interfering with background tasks.

The request body is never read here. ``workflow_type`` lives in the body of ``POST
/workflows``, and reading it in ASGI middleware means buffering ``receive`` and replaying it to
the route -- and per ``resilience/ratelimit.py``'s module docs, the bucket key is the route
class, not anything caller-supplied, so there is nothing in the body this middleware needs.
"""

from __future__ import annotations

import json
import logging

from starlette.types import ASGIApp, Receive, Scope, Send

from sankalp.config import Settings, get_settings
from sankalp.resilience.ratelimit import TokenBucketLimiter

__all__ = ["RateLimitMiddleware"]

log = logging.getLogger("sankalp.api")

#: Deliberately a fixed table rather than a regex or FastAPI route introspection: three routes,
#: all named in api/main.py, and a fixed table can't silently start matching a route added later
#: without someone extending it here too.
_SUBMIT = "submit"
_READ = "read"
_CANCEL = "cancel"

#: route class -> the Settings field naming its per-request token cost. Read fresh via
#: get_settings() on every request rather than captured once, the same reasoning as
#: app.state.limiter below -- Settings is process-wide and cached (config.py's
#: @lru_cache), so this costs a dict lookup and an attribute read, not I/O.
_COST_FIELD = {
    _SUBMIT: "ratelimit_submit_cost",
    _READ: "ratelimit_read_cost",
    _CANCEL: "ratelimit_cancel_cost",
}


def _cost_for(route_class: str, settings: Settings) -> int:
    return getattr(settings, _COST_FIELD[route_class])  # type: ignore[no-any-return]


def _route_class(method: str, path: str) -> str | None:
    """The bucket this request belongs to, or ``None`` for anything not rate-limited.

    Matched on shape rather than exact string, since ``/workflows/{id}`` and
    ``/workflows/{id}/cancel`` both have a path segment that varies per request. Anything that
    doesn't match one of the three known routes (a typo'd path, a future route not yet wired
    in here) is deliberately **not** rate-limited rather than guessed at -- it is the route
    itself (FastAPI's own routing) that decides whether such a path is valid at all; this table
    only prices the ones that exist today.
    """
    segments = [s for s in path.split("/") if s]
    if method == "POST" and segments == ["workflows"]:
        return _SUBMIT
    if method == "GET" and len(segments) == 2 and segments[0] == "workflows":
        return _READ
    if (
        method == "POST"
        and len(segments) == 3
        and segments[0] == "workflows"
        and segments[2] == "cancel"
    ):
        return _CANCEL
    return None


class RateLimitMiddleware:
    """Reject over-budget requests with 429 before they reach a route handler.

    ``app.state.limiter`` is read fresh on every call rather than captured once in
    ``__init__`` (which only ever sees ``app`` before the lifespan has run) -- the lifespan
    assigns it after startup, mirroring how ``api/main.py`` already reads ``app.state.pool``.
    A missing or ``None`` limiter admits every request: the same fail-open stance the limiter
    itself takes when Redis is down, extended to "the limiter isn't wired up at all," which
    matters for ``tests/test_api.py``'s ``client`` fixture -- it does not run the lifespan, so
    unless it explicitly sets ``app.state.limiter`` (see that file), every request there takes
    this path rather than erroring.
    """

    def __init__(self, app: ASGIApp) -> None:
        self._app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return

        limiter: TokenBucketLimiter | None = getattr(scope["app"].state, "limiter", None)
        if limiter is None:
            await self._app(scope, receive, send)
            return

        route_class = _route_class(scope["method"], scope["path"])
        if route_class is None:
            await self._app(scope, receive, send)
            return

        cost = _cost_for(route_class, get_settings())
        decision = await limiter.check(route_class, cost=cost)
        if decision.admitted:
            await self._app(scope, receive, send)
            return

        await _send_429(send, decision.retry_after_seconds)


async def _send_429(send: Send, retry_after_seconds: int) -> None:
    """Emit a 429 in FastAPI's own error shape (``{"detail": ...}``).

    Middleware runs before FastAPI's exception handlers ever see the request, so nothing here
    produces that shape for free the way raising ``HTTPException`` from inside a route does --
    it has to be built by hand, matched deliberately so a client cannot tell a rate-limit
    rejection from any other 4xx this API returns.
    """
    body = json.dumps({"detail": "rate limit exceeded"}).encode("utf-8")
    headers = [
        (b"content-type", b"application/json"),
        (b"content-length", str(len(body)).encode("ascii")),
        (b"retry-after", str(max(1, retry_after_seconds)).encode("ascii")),
    ]
    await send({"type": "http.response.start", "status": 429, "headers": headers})
    await send({"type": "http.response.body", "body": body})
