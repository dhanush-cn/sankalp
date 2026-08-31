"""FastAPI routes and Pydantic v2 request/response models.

Three routes (docs/spec.md, "Phase 1 API"): submit, read, cancel. No new tables. Fronted by two
middleware layers, outermost to innermost:
:class:`~sankalp.api.middleware.RateLimitMiddleware` -- a Redis token bucket per route class,
behind a circuit breaker that fails open (``resilience/ratelimit.py``, ``resilience/circuit.py``)
-- then :class:`~sankalp.api.middleware.AdaptiveConcurrencyMiddleware` -- a gradient/Vegas-style
in-process concurrency limiter guarding this process's own slice of the DB pool
(``resilience/adaptive.py``, docs/spec.md "Adaptive Concurrency").

The app's pool is deliberately opened with no explicit ``dsn`` argument, so
:func:`~sankalp.storage.pool.create_pool`'s default -- ``settings.active_app_database_url``,
the restricted ``sankalp_app`` role (migrations/004_restricted_role.sql) -- is what actually
runs. Passing the DSN explicitly here would silently stop testing that the default is safe.
"""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any
from uuid import UUID

import asyncpg
from fastapi import FastAPI, Header, HTTPException, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from sankalp.api.middleware import AdaptiveConcurrencyMiddleware, RateLimitMiddleware
from sankalp.config import get_settings
from sankalp.engine.definition import registered_types
from sankalp.resilience.adaptive import AdaptiveConcurrencyLimiter
from sankalp.resilience.circuit import CircuitBreaker
from sankalp.resilience.ratelimit import TokenBucketLimiter
from sankalp.storage import workflows as workflow_storage
from sankalp.storage.pool import create_pool
from sankalp.storage.redis import create_redis

log = logging.getLogger("sankalp.api")

__all__ = ["app"]


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    # Importing the package is what registers the definitions -- @workflow runs at import
    # time, and registered_types()/get_definition() can only resolve a workflow_type this
    # process has imported (engine/worker.py's main() does the same, for the same reason).
    # Deferred to here rather than a module-level import so api/main.py stays importable
    # with an empty registry -- tests/test_api.py relies on that to register its own
    # throwaway workflow type without the demo definitions bleeding in.
    import sankalp.workflows  # noqa: F401

    settings = get_settings()
    logging.basicConfig(
        level=settings.log_level.upper(),
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )
    pool = await create_pool()
    app.state.pool = pool

    redis = None
    if settings.ratelimit_enabled:
        redis = create_redis(
            settings=settings, socket_timeout_seconds=settings.ratelimit_redis_timeout_seconds
        )
        limiter = TokenBucketLimiter(
            redis,
            key_prefix=settings.ratelimit_key_prefix,
            capacity=settings.ratelimit_capacity,
            refill_per_second=settings.ratelimit_refill_per_second,
            budget_seconds=settings.ratelimit_redis_timeout_seconds,
            breaker=CircuitBreaker(
                failure_threshold=settings.ratelimit_breaker_failure_threshold,
                cooldown_seconds=settings.ratelimit_breaker_cooldown_seconds,
            ),
        )
        await limiter.load_script()
        app.state.limiter = limiter
        log.info(
            "rate limiting enabled: capacity=%d refill=%.1f/s breaker_threshold=%d",
            settings.ratelimit_capacity,
            settings.ratelimit_refill_per_second,
            settings.ratelimit_breaker_failure_threshold,
        )
    else:
        # Explicit opt-out, logged the same as the enabled path -- a wiring gap should never
        # look identical to a deliberate choice in the logs. RateLimitMiddleware admits every
        # request when app.state.limiter is unset, which is what running with this False does.
        app.state.limiter = None
        log.warning("rate limiting disabled (SANKALP_RATELIMIT_ENABLED=false)")

    if settings.adaptive_concurrency_enabled:
        concurrency_limiter = AdaptiveConcurrencyLimiter(
            initial_limit=settings.adaptive_concurrency_initial_limit,
            min_limit=settings.adaptive_concurrency_min_limit,
            max_limit=settings.adaptive_concurrency_max_limit,
            window_seconds=settings.adaptive_concurrency_window_seconds,
            rtt_min_decay=settings.adaptive_concurrency_rtt_min_decay,
            high_criticality_wait_seconds=settings.adaptive_concurrency_high_wait_seconds,
        )
        app.state.concurrency_limiter = concurrency_limiter
        log.info(
            "adaptive concurrency enabled: initial_limit=%d min_limit=%d max_limit=%d",
            settings.adaptive_concurrency_initial_limit,
            settings.adaptive_concurrency_min_limit,
            settings.adaptive_concurrency_max_limit,
        )
    else:
        # Same reasoning as the rate limiter's disabled branch: an explicit opt-out is logged
        # the same as the enabled path, so a wiring gap never looks like a deliberate choice.
        # AdaptiveConcurrencyMiddleware admits every request when this is unset.
        app.state.concurrency_limiter = None
        log.warning("adaptive concurrency disabled (SANKALP_ADAPTIVE_CONCURRENCY_ENABLED=false)")

    try:
        yield
    finally:
        if redis is not None:
            await redis.aclose()
        await pool.close()


app = FastAPI(title="sankalp", lifespan=_lifespan)
# Order matters, and it is the opposite of what "first added" suggests: Starlette's
# add_middleware() inserts at the front of its list, so the LAST call here ends up OUTERMOST
# (hit first by a request) -- verified directly against this app's own built middleware stack,
# not assumed. AdaptiveConcurrencyMiddleware is added first (innermost, closest to the route
# handler, so the RTT it measures is purely handler execution time) and RateLimitMiddleware
# second (outermost, so a 429 fires before a request ever reaches the concurrency gate).
app.add_middleware(AdaptiveConcurrencyMiddleware)
app.add_middleware(RateLimitMiddleware)


def _pool(app: FastAPI) -> asyncpg.Pool:
    return app.state.pool


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class WorkflowSubmitRequest(BaseModel):
    model_config = {"extra": "forbid"}

    workflow_type: str = Field(min_length=1)
    input: dict[str, Any] = Field(default_factory=dict)


class WorkflowResponse(BaseModel):
    model_config = {"extra": "forbid"}

    id: UUID
    workflow_type: str
    status: str
    input: Any
    output: Any
    error: str | None
    current_step: str | None
    attempt: int
    completed_steps: list[str]


def _to_response(
    record: workflow_storage.WorkflowRecord, completed_steps: list[str]
) -> WorkflowResponse:
    return WorkflowResponse(
        id=record.id,
        workflow_type=record.workflow_type,
        status=record.status,
        input=record.input,
        output=record.output,
        error=record.error,
        current_step=record.current_step,
        attempt=record.attempt,
        completed_steps=completed_steps,
    )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@app.post("/workflows")
async def submit_workflow(
    body: WorkflowSubmitRequest,
    idempotency_key: str = Header(..., alias="Idempotency-Key", min_length=1),
) -> JSONResponse:
    """Submit a workflow. ``Idempotency-Key`` is required.

    ``INSERT ... ON CONFLICT (workflow_type, idempotency_key) DO NOTHING`` then, only on a
    conflict, a re-select -- never ``DO UPDATE`` (docs/spec.md, "Submit handler, in full"). A
    duplicate submit must not mutate a workflow that may already be RUNNING. See
    :func:`sankalp.storage.workflows.submit_workflow` for why this is race-free under
    concurrent duplicate submits with no retry loop.

    201 on the request that actually created the row, 200 for a duplicate returning the
    existing state -- decided by ``created`` below, which is why this returns a
    :class:`~fastapi.responses.JSONResponse` directly rather than declaring a fixed
    ``status_code`` on the route.
    """
    if body.workflow_type not in registered_types():
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                f"workflow_type {body.workflow_type!r} is not registered; "
                f"known types: {', '.join(registered_types()) or 'none'}"
            ),
        )

    record, created = await workflow_storage.submit_workflow(
        _pool(app),
        workflow_type=body.workflow_type,
        idempotency_key=idempotency_key,
        input_json=json.dumps(body.input),
    )
    completed_steps = await workflow_storage.get_completed_steps(_pool(app), record.id)
    payload = _to_response(record, completed_steps).model_dump(mode="json")
    return JSONResponse(
        status_code=status.HTTP_201_CREATED if created else status.HTTP_200_OK,
        content=payload,
    )


@app.get("/workflows/{workflow_id}")
async def get_workflow(workflow_id: UUID) -> WorkflowResponse:
    """Status, current step, completed steps, error."""
    record = await workflow_storage.get_workflow(_pool(app), workflow_id)
    if record is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="workflow not found")
    completed_steps = await workflow_storage.get_completed_steps(_pool(app), workflow_id)
    return _to_response(record, completed_steps)


@app.post("/workflows/{workflow_id}/cancel")
async def cancel_workflow(workflow_id: UUID) -> WorkflowResponse:
    """Move a PENDING/RUNNING workflow to COMPENSATING.

    Guarded in SQL by ``WHERE status IN ('PENDING', 'RUNNING')`` -- a SUCCESS, COMPENSATED,
    already-COMPENSATING, or FAILED_DIRTY workflow cannot be touched by this route
    (:func:`sankalp.storage.workflows.cancel_workflow`).

    **This does not touch ``owner_id``/``fencing_token``: the API is not the lease holder and
    has no fencing token to present.** A worker mid-step will not observe this cancel -- its
    per-step writes guard on ownership, not status -- so it keeps checkpointing steps as if
    nothing happened. It cannot reach SUCCESS afterward, though: the terminal UPDATE requires
    ``status = 'RUNNING'``, so that write matches zero rows, the executor treats it as
    preempted, and the row (COMPENSATING, ``owner_id`` still set until its lease expires)
    falls back to the ordinary lease-expiry recovery path into an unwind.
    """
    cancelled = await workflow_storage.cancel_workflow(_pool(app), workflow_id)
    if not cancelled:
        record = await workflow_storage.get_workflow(_pool(app), workflow_id)
        if record is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="workflow not found"
            )
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"workflow {workflow_id} is {record.status}; cannot be cancelled",
        )
    record = await workflow_storage.get_workflow(_pool(app), workflow_id)
    assert record is not None, "cancel_workflow returned True for a row that no longer exists"
    completed_steps = await workflow_storage.get_completed_steps(_pool(app), workflow_id)
    return _to_response(record, completed_steps)
