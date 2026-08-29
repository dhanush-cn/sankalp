"""The token bucket and its circuit breaker, proved against a real Redis.

Three things this file exists to nail down (docs/build-log-phase3.md, "What proves it"):

  (a) the bucket enforces its rate accurately under concurrent callers -- including atomicity,
      which a naive GET-compute-SET implementation would fail even though it passes the
      sequential refill test;
  (b) killing Redis keeps the limiter (and, by extension, the API) admitting requests, and the
      breaker opens fast rather than every call paying a full timeout;
  (c) the breaker recovers once Redis comes back, and recovery means enforcement actually
      resumes -- not just that the breaker reports CLOSED.

No mocks for (b)/(c): ``tests/ratelimit_proxy.py``'s ``RedisProxy`` is a real TCP proxy in front
of the real Redis this suite already depends on, so killing it means real sockets and real
redis-py exceptions, and it is scoped to one test at a time -- the shared ``sankalp-redis``
container used by ``tests/test_outbox_drain.py`` is never touched. ``tests/test_circuit.py``
already proves the breaker's state machine in isolation with an injected clock and no I/O; this
file is what proves that machine actually gets driven correctly by real Redis outcomes.

Isolation: a per-test key prefix (``sankalp.ratelimit.test.<uuid>``), deleted at teardown --
the same reasoning as ``tests/conftest.py``'s ``event_stream`` fixture: a write to a key no
other test uses cannot damage anything it does not own, so the unique key is the isolation
mechanism and no additional guard is needed.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import AsyncIterator

import pytest
from ratelimit_proxy import RedisProxy
from redis.asyncio import Redis

from sankalp.resilience.circuit import CircuitBreaker, CircuitState
from sankalp.resilience.ratelimit import TokenBucketLimiter
from sankalp.storage.redis import create_redis


@pytest.fixture
async def proxy() -> AsyncIterator[tuple[RedisProxy, int]]:
    p = RedisProxy()
    port = await p.start()
    try:
        yield p, port
    finally:
        await p.stop()


def _key_prefix() -> str:
    return f"sankalp.ratelimit.test.{uuid.uuid4().hex}"


async def _cleanup(redis: Redis, key_prefix: str, route_classes: tuple[str, ...]) -> None:
    await redis.delete(*(f"{key_prefix}:{rc}" for rc in route_classes))
    await redis.aclose()


# ---------------------------------------------------------------------------
# (a) Accurate enforcement, including atomicity
# ---------------------------------------------------------------------------


async def test_refill_arithmetic_is_deterministic_with_a_pinned_clock():
    """Capacity 10, refill 5/s, pinned now_ms: 10 admitted, 11th denied; advance exactly 1000ms
    -> exactly 5 more admitted, 6th denied. No sleeping -- the pinned clock makes this exact."""
    now = [1_000.0]  # seconds; TokenBucketLimiter multiplies by 1000 for now_ms
    redis = create_redis(socket_timeout_seconds=1.0)
    key_prefix = _key_prefix()
    limiter = TokenBucketLimiter(
        redis,
        key_prefix=key_prefix,
        capacity=10,
        refill_per_second=5.0,
        budget_seconds=1.0,
        breaker=CircuitBreaker(),
        clock=lambda: now[0],
    )
    try:
        for _ in range(10):
            decision = await limiter.check("submit")
            assert decision.admitted and decision.enforced
        decision = await limiter.check("submit")
        assert not decision.admitted
        assert decision.enforced
        assert decision.retry_after_seconds >= 1

        now[0] += 1.0  # exactly 1000ms later -> exactly 5 tokens refilled
        for _ in range(5):
            decision = await limiter.check("submit")
            assert decision.admitted
        decision = await limiter.check("submit")
        assert not decision.admitted
    finally:
        await _cleanup(redis, key_prefix, ("submit",))


async def test_atomicity_under_200_concurrent_callers_admits_exactly_the_capacity():
    """The test that actually earns its keep (docs/build-log-phase3.md, "What proves it" (a)).

    All 200 callers share one pinned now_ms, so refill contributes exactly zero -- the only way
    to admit exactly 50 out of 200 is for the consume-and-decrement to be atomic across every
    concurrent caller. A naive GET-compute-SET implementation would race and admit more than
    50 (two callers both reading "49 tokens available" before either writes back the debit).

    Built directly with ``Redis.from_url`` rather than :func:`create_redis`, which does not
    expose ``max_connections``: redis-py 8's default connection-pool cap is 100, well under the
    200 concurrent callers this test needs in flight against one client -- a test-harness limit
    with nothing to do with the limiter's own correctness, so it is raised here rather than
    changing the production factory for a concurrency level only this test reaches.
    """
    redis = Redis.from_url(
        "redis://localhost:6379/0",
        decode_responses=True,
        socket_timeout=2.0,
        socket_connect_timeout=2.0,
        max_connections=250,
    )
    key_prefix = _key_prefix()
    limiter = TokenBucketLimiter(
        redis,
        key_prefix=key_prefix,
        capacity=50,
        refill_per_second=1.0,  # near-zero refill contribution even if a call runs a bit late
        budget_seconds=2.0,
        breaker=CircuitBreaker(),
        clock=lambda: 5_000.0,  # every caller sees the identical instant
    )
    try:
        decisions = await asyncio.gather(*(limiter.check("submit") for _ in range(200)))
        admitted = sum(1 for d in decisions if d.admitted)
        assert admitted == 50, "atomicity broke -- a race let through more than the capacity"
        assert all(d.enforced for d in decisions), "Redis was reachable; nothing should fail open"
    finally:
        await _cleanup(redis, key_prefix, ("submit",))


async def test_retry_after_reflects_the_actual_deficit():
    redis = create_redis(socket_timeout_seconds=1.0)
    key_prefix = _key_prefix()
    limiter = TokenBucketLimiter(
        redis,
        key_prefix=key_prefix,
        capacity=1,
        refill_per_second=2.0,  # a 1-token deficit costs 500ms to refill
        budget_seconds=1.0,
        breaker=CircuitBreaker(),
        clock=lambda: 9_000.0,
    )
    try:
        assert (await limiter.check("submit")).admitted
        decision = await limiter.check("submit")
        assert not decision.admitted
        # RFC 9110 wants whole seconds; 500ms must round up to 1, never down to 0.
        assert decision.retry_after_seconds == 1
    finally:
        await _cleanup(redis, key_prefix, ("submit",))


async def test_a_caller_whose_clock_lags_never_drains_the_bucket_below_zero():
    """math.max(0, ...) inside the Lua script (see ratelimit.py's module docstring) is what
    bounds clock skew between instances to a one-time burst instead of a negative-tokens wedge.
    Simulated here by calling with a *later* now_ms first, then an *earlier* one."""
    redis = create_redis(socket_timeout_seconds=1.0)
    key_prefix = _key_prefix()
    limiter = TokenBucketLimiter(
        redis,
        key_prefix=key_prefix,
        capacity=10,
        refill_per_second=5.0,
        budget_seconds=1.0,
        breaker=CircuitBreaker(),
        clock=lambda: 100_000.0,
    )
    try:
        await limiter.check("submit")  # stamps ts=100_000_000ms in the bucket

        limiter._clock = lambda: 50_000.0  # a second caller, 50000s "behind" -- clock skew
        decision = await limiter.check("submit")
        assert decision.admitted, "a lagging clock must contribute zero refill, not go negative"
    finally:
        await _cleanup(redis, key_prefix, ("submit",))


async def test_noscript_recovers_within_one_retry_and_never_opens_the_breaker():
    """A NOSCRIPT means Redis answered promptly and correctly -- just without the script
    cached. It must not read as a Redis failure (docs/build-log-phase3.md, question 3's table)."""
    redis = create_redis(socket_timeout_seconds=1.0)
    key_prefix = _key_prefix()
    breaker = CircuitBreaker(failure_threshold=1, cooldown_seconds=5.0)
    limiter = TokenBucketLimiter(
        redis,
        key_prefix=key_prefix,
        capacity=10,
        refill_per_second=5.0,
        budget_seconds=1.0,
        breaker=breaker,
        clock=lambda: 1.0,
    )
    try:
        await redis.script_flush()
        decision = await limiter.check("submit")
        assert decision.admitted and decision.enforced, "the bucket must still work post-flush"
        assert breaker.state is CircuitState.CLOSED, "a NOSCRIPT alone must never open the breaker"

        # And it stays recovered -- the retry repopulates the cache, so the next call goes
        # straight through EVALSHA with no further NOSCRIPT.
        decision = await limiter.check("submit")
        assert decision.enforced
        assert breaker.state is CircuitState.CLOSED
    finally:
        await _cleanup(redis, key_prefix, ("submit",))


async def test_a_script_bug_fails_open_and_logged_but_never_opens_the_breaker(caplog):
    """refill_per_second<=0 raises a Lua-level ResponseError -- a caller bug, not a transport
    failure. It must fail open (nothing about the money guarantee depends on this limiter) but
    must NOT be mistaken for "Redis is down" and open the breaker on a healthy Redis."""
    redis = create_redis(socket_timeout_seconds=1.0)
    key_prefix = _key_prefix()
    breaker = CircuitBreaker(failure_threshold=1, cooldown_seconds=5.0)
    limiter = TokenBucketLimiter(
        redis,
        key_prefix=key_prefix,
        capacity=10,
        refill_per_second=0.0,
        budget_seconds=1.0,
        breaker=breaker,
        clock=lambda: 1.0,
    )
    try:
        with caplog.at_level("ERROR", logger="sankalp.ratelimit"):
            decision = await limiter.check("submit")
        assert decision.admitted and not decision.enforced, "must fail open on a script error"
        assert any(r.levelname == "ERROR" for r in caplog.records), "a script bug must be loud"
        assert breaker.state is CircuitState.CLOSED, "a config bug is not a transport failure"
    finally:
        await _cleanup(redis, key_prefix, ("submit",))


# ---------------------------------------------------------------------------
# (b) Redis down: fail open, and fast (via the TCP proxy)
# ---------------------------------------------------------------------------


async def test_blackholed_redis_fails_open_for_every_request(proxy):
    redis_proxy, port = proxy
    redis_proxy.blackhole()
    key_prefix = _key_prefix()
    redis = create_redis(f"redis://127.0.0.1:{port}/0", socket_timeout_seconds=0.1)
    breaker = CircuitBreaker(failure_threshold=3, cooldown_seconds=5.0)
    limiter = TokenBucketLimiter(
        redis,
        key_prefix=key_prefix,
        capacity=1,
        refill_per_second=1.0,
        budget_seconds=0.1,
        breaker=breaker,
        clock=lambda: 1.0,
    )
    try:
        # capacity=1 means the 2nd call onward would be denied by a HEALTHY bucket -- proving
        # every one of these is admitted proves fail-open, not just an unusually generous bucket.
        for _ in range(10):
            decision = await limiter.check("submit")
            assert decision.admitted and not decision.enforced
    finally:
        await redis.aclose()


async def test_breaker_opens_after_the_threshold_and_stops_touching_the_socket(proxy):
    redis_proxy, port = proxy
    redis_proxy.blackhole()
    key_prefix = _key_prefix()
    redis = create_redis(f"redis://127.0.0.1:{port}/0", socket_timeout_seconds=0.1)
    breaker = CircuitBreaker(failure_threshold=3, cooldown_seconds=5.0)
    limiter = TokenBucketLimiter(
        redis,
        key_prefix=key_prefix,
        capacity=10,
        refill_per_second=5.0,
        budget_seconds=0.1,
        breaker=breaker,
        clock=lambda: 1.0,
    )
    try:
        for _ in range(3):
            await limiter.check("submit")
        assert breaker.state is CircuitState.OPEN

        redis_proxy.reset_count()
        for _ in range(20):
            decision = await limiter.check("submit")
            assert decision.admitted and not decision.enforced
        # The latency claim, asserted at the mechanism rather than the symptom: zero new
        # connections means no socket was even attempted, which is what makes this fast --
        # not an inference from wall-clock timing that could flake on a loaded box.
        assert redis_proxy.connections == 0, "the breaker must short-circuit before any I/O"
    finally:
        await redis.aclose()


async def test_open_breaker_answers_orders_of_magnitude_faster_than_the_budget(proxy):
    """Secondary to the connection-count assertion above, but worth having: once OPEN, a check
    must return near-instantly, not merely "before the timeout fires"."""
    redis_proxy, port = proxy
    redis_proxy.blackhole()
    key_prefix = _key_prefix()
    redis = create_redis(f"redis://127.0.0.1:{port}/0", socket_timeout_seconds=0.2)
    breaker = CircuitBreaker(failure_threshold=1, cooldown_seconds=5.0)
    limiter = TokenBucketLimiter(
        redis,
        key_prefix=key_prefix,
        capacity=10,
        refill_per_second=5.0,
        budget_seconds=0.2,
        breaker=breaker,
        clock=lambda: 1.0,
    )
    try:
        start = time.monotonic()
        await limiter.check("submit")  # this one pays the real timeout and opens the breaker
        first_call_seconds = time.monotonic() - start
        assert breaker.state is CircuitState.OPEN
        assert first_call_seconds >= 0.2, "the first failure should have paid the real budget"

        start = time.monotonic()
        await limiter.check("submit")
        assert (time.monotonic() - start) < 0.005, "an open breaker must answer near-instantly"
    finally:
        await redis.aclose()


async def test_connection_refused_also_fails_open(proxy):
    """close() models Redis simply being gone, as opposed to hung -- both must fail open."""
    redis_proxy, port = proxy
    await redis_proxy.stop()
    key_prefix = _key_prefix()
    redis = create_redis(f"redis://127.0.0.1:{port}/0", socket_timeout_seconds=0.2)
    limiter = TokenBucketLimiter(
        redis,
        key_prefix=key_prefix,
        capacity=1,
        refill_per_second=1.0,
        budget_seconds=0.2,
        breaker=CircuitBreaker(),
        clock=lambda: 1.0,
    )
    try:
        decision = await limiter.check("submit")
        assert decision.admitted and not decision.enforced
    finally:
        await redis.aclose()


# ---------------------------------------------------------------------------
# (c) Recovery: half-open, one probe, and enforcement actually resumes
# ---------------------------------------------------------------------------


async def test_breaker_recovers_and_enforcement_actually_resumes(proxy):
    """The assertion that means something: not "the breaker reports CLOSED" but "a request
    that should be limited gets a 429-equivalent (admitted=False) again"."""
    redis_proxy, port = proxy
    redis_proxy.blackhole()
    key_prefix = _key_prefix()
    clock = {"now": 1.0}
    redis = create_redis(f"redis://127.0.0.1:{port}/0", socket_timeout_seconds=0.1)
    breaker = CircuitBreaker(failure_threshold=1, cooldown_seconds=1.0)
    limiter = TokenBucketLimiter(
        redis,
        key_prefix=key_prefix,
        capacity=1,
        refill_per_second=1.0,
        budget_seconds=0.1,
        breaker=breaker,
        clock=lambda: clock["now"],
    )
    try:
        await limiter.check("submit")  # opens the breaker
        assert breaker.state is CircuitState.OPEN

        redis_proxy.open()  # Redis is back
        # Advance the breaker's own real monotonic clock by actually waiting -- the breaker
        # was built with the default (real) clock, so its cooldown is real wall-clock time.
        # Kept tiny (cooldown_seconds=1.0, jittered 0.5-1.5x) so this stays fast.
        await asyncio.sleep(1.6)

        first_decision = await limiter.check("submit")  # the probe
        assert first_decision.enforced, "the probe must actually reach Redis, not fail open"
        assert breaker.state is CircuitState.CLOSED, "a successful probe must close the circuit"

        # Bucket has capacity=1 and the probe already spent it -- the next call MUST be denied.
        # This is the assertion that proves recovery, not just the state label.
        clock["now"] += 0.001
        second_decision = await limiter.check("submit")
        assert not second_decision.admitted, "enforcement did not actually resume"
    finally:
        await redis.aclose()


async def test_exactly_one_probe_escapes_even_under_concurrent_callers(proxy):
    """Preventing a probe stampede onto a just-recovering Redis is the entire reason HALF_OPEN
    exists. Proved at the connection-count level: N concurrent callers, one new connection."""
    redis_proxy, port = proxy
    redis_proxy.blackhole()
    key_prefix = _key_prefix()
    redis = create_redis(f"redis://127.0.0.1:{port}/0", socket_timeout_seconds=0.1)
    breaker = CircuitBreaker(failure_threshold=1, cooldown_seconds=1.0)
    limiter = TokenBucketLimiter(
        redis,
        key_prefix=key_prefix,
        capacity=100,
        refill_per_second=50.0,
        budget_seconds=0.1,
        breaker=breaker,
        clock=lambda: 1.0,
    )
    try:
        await limiter.check("submit")  # opens the breaker
        assert breaker.state is CircuitState.OPEN

        redis_proxy.open()
        await asyncio.sleep(1.6)
        redis_proxy.reset_count()

        decisions = await asyncio.gather(*(limiter.check("submit") for _ in range(50)))
        enforced_count = sum(1 for d in decisions if d.enforced)
        assert enforced_count == 1, "exactly one caller's check should have reached Redis"
        assert redis_proxy.connections <= 1, "at most one new connection during the probe window"
        assert breaker.state is CircuitState.CLOSED
    finally:
        await redis.aclose()


async def test_a_failed_probe_reopens_without_a_stampede(proxy):
    redis_proxy, port = proxy
    redis_proxy.blackhole()
    key_prefix = _key_prefix()
    redis = create_redis(f"redis://127.0.0.1:{port}/0", socket_timeout_seconds=0.1)
    breaker = CircuitBreaker(failure_threshold=1, cooldown_seconds=1.0)
    limiter = TokenBucketLimiter(
        redis,
        key_prefix=key_prefix,
        capacity=10,
        refill_per_second=5.0,
        budget_seconds=0.1,
        breaker=breaker,
        clock=lambda: 1.0,
    )
    try:
        await limiter.check("submit")  # opens the breaker
        await asyncio.sleep(1.6)  # past the cooldown, but Redis is STILL blackholed
        decision = await limiter.check("submit")  # the probe -- fails again
        assert decision.admitted and not decision.enforced
        assert breaker.state is CircuitState.OPEN, "a failed probe must reopen, not stay half-open"
    finally:
        await redis.aclose()
