"""Rate-limit and adaptive-concurrency enforcement as pure ASGI middleware.

**Not** ``@app.middleware("http")`` -- that decorator wraps every request in
``BaseHTTPMiddleware``, which runs the whole request inside an ``anyio`` task group and turns
the response into a stream. It exists to let middleware read/rewrite a streaming response body;
neither middleware here needs that -- ``RateLimitMiddleware`` only needs to reject *before* a
route runs, and ``AdaptiveConcurrencyMiddleware`` only needs to time the route's own execution
-- so both are plain ASGI instead: ``__init__(app)`` / ``async def __call__(scope, receive,
send)``, with none of ``BaseHTTPMiddleware``'s well-documented history of swallowing client
disconnects and interfering with background tasks.

The request body is never read by either. ``workflow_type`` lives in the body of ``POST
/workflows``, and reading it in ASGI middleware means buffering ``receive`` and replaying it to
the route -- and neither middleware needs anything caller-supplied from it: the rate limiter's
bucket key is the route class (``resilience/ratelimit.py``'s module docs), and the concurrency
limiter's only input beyond that is the ``Criticality`` header below.

**Registration order matters** (``api/main.py``), and it is the opposite of what "added first"
suggests: Starlette's ``add_middleware`` inserts at the front of its list, so the *last* call
ends up outermost (hit first by a request) -- verified directly against the app's own built
middleware stack, not assumed. ``AdaptiveConcurrencyMiddleware`` is added first (innermost,
closest to the route handler, so the RTT it measures is purely handler execution time) and
``RateLimitMiddleware`` second (outermost, so a rate-limited request 429s before it ever
reaches the concurrency gate).
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Iterable

from starlette.types import ASGIApp, Receive, Scope, Send

from sankalp.config import Settings, get_settings
from sankalp.resilience.adaptive import AdaptiveConcurrencyLimiter, Criticality
from sankalp.resilience.ratelimit import TokenBucketLimiter

__all__ = ["AdaptiveConcurrencyMiddleware", "RateLimitMiddleware"]

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


#: No configurable header name -- no precedent for that in this codebase (route classes and
#: costs are all fixed tables, not settings). ``Idempotency-Key``-style casing, no ``X-`` prefix.
_CRITICALITY_HEADER = b"criticality"


def _criticality_from_headers(headers: Iterable[tuple[bytes, bytes]]) -> Criticality:
    """Absent or unrecognised -> :attr:`Criticality.LOW` -- the backward-compatible default.

    Argued in full in the build log: before this middleware existed, no request got special
    treatment under overload, so undeclared traffic keeping that same best-effort behaviour is
    the non-breaking choice. ``HIGH`` is something a caller deliberately claims.
    """
    for name, value in headers:
        if name.lower() == _CRITICALITY_HEADER and value.lower() == b"high":
            return Criticality.HIGH
    return Criticality.LOW


class AdaptiveConcurrencyMiddleware:
    """Shed over-saturated requests by criticality; feed real handler RTT back into the gradient.

    ``app.state.concurrency_limiter`` is read fresh per request, same reasoning and same
    fail-open-on-``None`` stance as :class:`RateLimitMiddleware`'s ``app.state.limiter`` above.

    **The RTT timer starts only after admission, never before.** ``acquire()`` yields a
    :class:`~sankalp.resilience.adaptive.ConcurrencyDecision` before this middleware has done
    anything else; the clock starts *after* that yield, wrapping only
    ``self._app(scope, receive, send)`` -- the route's own execution. Starting it any earlier
    would fold a ``HIGH`` caller's admission wait into what the gradient treats as downstream
    latency, which is exactly the runaway feedback loop
    :meth:`~sankalp.resilience.adaptive.AdaptiveConcurrencyLimiter.record_rtt`'s own docstring
    warns about -- proved at this exact call site by a fail-proof in ``tests/test_adaptive.py``,
    not just asserted in a comment.
    """

    def __init__(self, app: ASGIApp) -> None:
        self._app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return

        limiter: AdaptiveConcurrencyLimiter | None = getattr(
            scope["app"].state, "concurrency_limiter", None
        )
        if limiter is None:
            await self._app(scope, receive, send)
            return

        criticality = _criticality_from_headers(scope["headers"])
        async with limiter.acquire(criticality) as decision:
            if not decision.admitted:
                await _send_503(send)
                return
            started = time.monotonic()
            try:
                await self._app(scope, receive, send)
            finally:
                limiter.record_rtt(time.monotonic() - started)


async def _send_503(send: Send) -> None:
    """Emit a 503 in FastAPI's own error shape. The first 503 anywhere in this codebase --
    there is no existing exception-handler precedent to reuse, same reasoning as ``_send_429``.
    """
    body = json.dumps({"detail": "concurrency limit exceeded"}).encode("utf-8")
    headers = [
        (b"content-type", b"application/json"),
        (b"content-length", str(len(body)).encode("ascii")),
    ]
    await send({"type": "http.response.start", "status": 503, "headers": headers})
    await send({"type": "http.response.body", "body": body})
