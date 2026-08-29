"""A Redis token bucket, atomic in one round trip, fronted by a circuit breaker.

**One tier only.** There is no in-process (L1) cache of the decision here, and none is planned
for this piece -- every check is a Redis round trip. A later piece may add one to shave latency
and load off Redis on the hot path; until it exists, do not read this module as though it were
part of a finished two-tier design. Say that plainly rather than let the absence read as an
oversight.

**Key: one bucket per route class, not per caller.** ``docs/spec.md``'s script is keyed by
whatever ``KEYS[1]`` the caller passes; this module always passes
``{key_prefix}:{route_class}`` (``submit`` / ``read`` / ``cancel``), never anything derived from
the request. Per-idempotency-key was rejected because ``Idempotency-Key`` is unique per payment
by construction (``api/main.py``), so it would either never limit anything (a flood of distinct
new payments) or throttle exactly the safe, idempotent retry case it should leave alone.
Per-IP was rejected because this is a server-to-server API behind proxies/NAT/mesh: the peer IP
is either one address for an entire client fleet or a value the caller's own
``X-Forwarded-For`` header controls -- and a limit key the caller can choose is not a limit.
**This means the limiter protects the backend from aggregate load; it does not give any one
caller fairness against another.** One noisy client can exhaust a route class's budget for
everyone sharing it. Per-caller fairness needs an authenticated identity, which this codebase
does not have yet -- when it does, the natural extension is folding that identity into the key
alongside the route class, not replacing the route class (see the module docstring's rejected
per-workflow-type option in the phase-3 build log for why the class axis stays either way).

**Fail-open, load-bearing, not incidental.** If the breaker is open, or Redis answers with a
transport-level failure, :meth:`TokenBucketLimiter.check` returns an *admitted* decision with
``enforced=False`` -- never denies on Redis's account. Nothing about the money guarantee depends
on this limiter (every step commits idempotently regardless of how many times it is attempted),
so refusing traffic because a cache is unreachable would trade a non-durable dependency's outage
for a total outage of the API. See ``docs/build-log-phase3.md``'s rate-limiting section for the
full argument, including what this deliberately gives up while Redis is down: there is no
per-process floor underneath this fail-open (the L1 tier that would provide one is out of scope
here), so an outage means genuinely unlimited admission, not merely degraded limiting.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass

from redis.asyncio import Redis
from redis.exceptions import NoScriptError, RedisError, ResponseError

from sankalp.resilience.circuit import CircuitBreaker

__all__ = ["RateLimitDecision", "TokenBucketLimiter"]

log = logging.getLogger("sankalp.ratelimit")

# KEYS[1]=bucket key  ARGV: capacity, refill_per_sec, now_ms, cost
#
# Follows docs/spec.md's "Token Bucket (Redis Lua)" script, with three deliberate deviations
# from what's written there, each because the spec version breaks in a way worth naming rather
# than silently working around:
#
#   1. Returns integers only. Redis truncates a Lua number to an integer when converting it to
#      a RESP reply -- the spec's `return { allowed, tokens }` silently hands a caller `9` for
#      a true token count of `9.5`. `retry_after_ms` is computed *here*, inside the script,
#      while `tokens` is still a float; deriving it in Python from the already-truncated integer
#      would bake that lost fraction into the Retry-After header the client sees.
#   2. `HSET` instead of the spec's `HMSET`, which Redis deprecated in 4.0.
#   3. `refill <= 0` is rejected with `redis.error_reply` rather than dividing by it in the
#      PEXPIRE line. That turns a caller bug into a Lua-level ResponseError, which by this
#      module's breaker rules (see TokenBucketLimiter.check) does NOT open the breaker and IS
#      logged loudly -- the right handling for "someone passed a bad config," as opposed to
#      "Redis is unreachable." Settings also constrains refill_per_second > 0 independently;
#      this is the second, inner guard.
_SCRIPT = """
local capacity  = tonumber(ARGV[1])
local refill    = tonumber(ARGV[2])
local now_ms    = tonumber(ARGV[3])
local cost      = tonumber(ARGV[4])

if refill <= 0 then
    return redis.error_reply("refill_per_sec must be > 0")
end

local b       = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens  = tonumber(b[1])
local last_ts = tonumber(b[2])

if tokens == nil then tokens = capacity; last_ts = now_ms end

-- max(0, ...) is load-bearing, not defensive padding: without it, a caller whose clock lags
-- the value already stored for this key computes a negative elapsed time, which would
-- *subtract* tokens rather than add them. Clamped to zero, a slow clock contributes no refill
-- (rather than draining the bucket) and a fast clock can only ever over-refill by one bucket's
-- worth, capped by the math.min against capacity two lines down. See docs/build-log-phase3.md,
-- "Clock" -- this is the one line that bounds cross-instance clock skew to a one-time bounded
-- burst instead of a wedged-shut bucket.
local elapsed = math.max(0, now_ms - last_ts) / 1000.0
tokens = math.min(capacity, tokens + elapsed * refill)

local allowed = 0
if tokens >= cost then
    tokens  = tokens - cost
    allowed = 1
end

redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now_ms)
redis.call('PEXPIRE', KEYS[1], math.ceil((capacity / refill) * 1000 * 2))

-- retry_after_ms computed here, not in Python, while `tokens` (and the deficit) are still
-- floats -- see deviation (1) above.
local retry_after_ms = 0
if allowed == 0 then
    retry_after_ms = math.ceil(((cost - tokens) / refill) * 1000)
end

return { allowed, math.floor(tokens), retry_after_ms }
"""

# The SHA Redis would hand back from SCRIPT LOAD is just sha1(script text) -- deterministic, so
# computing it locally means EVALSHA can be attempted with zero round trips up front, and the
# NOSCRIPT recovery path (see _eval_full) never needs to ask Redis what SHA it just cached.
_SCRIPT_SHA = hashlib.sha1(_SCRIPT.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class RateLimitDecision:
    """The answer to one :meth:`TokenBucketLimiter.check` call.

    ``enforced`` is what separates "the bucket said yes" from "we let it through because Redis
    or the breaker was unavailable" -- both are ``admitted=True``, but only the first is
    ``enforced=True``. Tests assert on this distinction directly; a future
    ``sankalp_ratelimit_rejected_total{reason}`` metric (not built -- no metrics exist anywhere
    in this codebase yet, see src/sankalp/observability/) would be labelled from it too.
    """

    admitted: bool
    enforced: bool
    retry_after_seconds: int = 0


class TokenBucketLimiter:
    """One Redis-backed token bucket per route class, guarded by a :class:`CircuitBreaker`.

    Plain parameters, not a ``Settings`` object -- matching ``compute_backoff``'s and
    ``CircuitBreaker``'s convention. The caller (``api/main.py``'s lifespan) is what reads
    ``Settings`` and unpacks it into these.

    ``clock`` is the same kind of injection seam as ``CircuitBreaker``'s -- production passes
    nothing and gets ``time.time`` (wall clock, deliberately: see the module docstring's
    "Clock" discussion in ``docs/build-log-phase3.md`` for why this must be wall time, not
    monotonic). Tests pin it to get the exact assertions the caller-supplied ``now_ms`` design
    exists for -- see ``tests/test_ratelimit.py``'s 200-concurrent-callers test, which pins
    every call to the same instant so refill contributes exactly zero and the admitted count is
    an equality, not a tolerance band.
    """

    def __init__(
        self,
        redis: Redis,
        *,
        key_prefix: str,
        capacity: int,
        refill_per_second: float,
        budget_seconds: float,
        breaker: CircuitBreaker,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._redis = redis
        self._key_prefix = key_prefix
        self._capacity = capacity
        self._refill_per_second = refill_per_second
        self._budget_seconds = budget_seconds
        self._breaker = breaker
        self._clock = clock

    async def load_script(self) -> None:
        """``SCRIPT LOAD`` once, at startup, so the first real request is not what pays for it.

        Not required for correctness -- :meth:`check` recovers from a cold cache on its own via
        the NOSCRIPT path -- purely a warmup. The SHA it returns is not consulted: it always
        equals the locally-computed ``_SCRIPT_SHA`` (SHA1 is deterministic over the same bytes),
        which is what every EVALSHA call actually uses.
        """
        await self._redis.script_load(_SCRIPT)

    async def check(self, route_class: str, *, cost: int = 1) -> RateLimitDecision:
        """Consume ``cost`` tokens from ``route_class``'s bucket, or fail open.

        Three separate reasons all land on the same admitted-but-not-enforced answer, and only
        one of them is fed back to the breaker -- see :meth:`_run` for the classification, which
        is the one thing in this class most worth reading closely before changing it.
        """
        if not self._breaker.allow():
            return RateLimitDecision(admitted=True, enforced=False)

        key = f"{self._key_prefix}:{route_class}"
        now_ms = int(self._clock() * 1000)  # wall clock: see module docs/build-log, "Clock".
        reply = await self._run(key, now_ms, cost)
        if reply is None:
            return RateLimitDecision(admitted=True, enforced=False)

        allowed, tokens, retry_after_ms = reply
        if not allowed:
            log.debug(
                "rate limited: class=%s tokens=%d retry_after_ms=%d",
                route_class,
                tokens,
                retry_after_ms,
            )
        # RFC 9110 Retry-After is whole seconds; ceil rather than truncate so a 300ms deficit
        # is never reported as "retry now" (0), and floor at 1 for the same reason.
        retry_after_seconds = max(1, -(-retry_after_ms // 1000)) if not allowed else 0
        return RateLimitDecision(
            admitted=bool(allowed), enforced=True, retry_after_seconds=retry_after_seconds
        )

    async def _run(self, key: str, now_ms: int, cost: int) -> list[int] | None:
        """Run the script, or ``None`` if the call must fail open.

        The classification here is the entire point of this class (docs/build-log-phase3.md,
        question 3's table) -- ``ResponseError`` is caught **before** the general transport
        tuple below because :class:`~redis.exceptions.NoScriptError` (already resolved inside
        :meth:`_eval_with_retry`) and this module's own ``refill <= 0`` guard are both
        ``ResponseError`` subclasses too, and both mean "Redis answered us, the answer was just
        an error" -- the opposite of "Redis did not answer." Only a genuine transport failure
        (below) is fed back to the breaker; a script bug is a permanent programming error that
        must be loud (ERROR, not WARNING) and must never be disguised as a transient outage.
        """
        try:
            reply = await self._eval_with_retry(key, now_ms, cost)
        except ResponseError as exc:
            log.error("rate limiter script error (not a Redis failure): %s", exc)
            return None
        except (TimeoutError, RedisError, OSError) as exc:
            # RedisError also catches redis.exceptions.TimeoutError; the bare TimeoutError and
            # OSError arms are for asyncio.timeout()'s own expiry and raw socket failures, which
            # are not RedisError subclasses (mirrors the asyncpg PostgresError/InterfaceError
            # two-hierarchy note in engine/worker.py -- there is no single ancestor that covers
            # every way this call can fail transport-wise). asyncio.TimeoutError is the same
            # class as the builtin TimeoutError since Python 3.11.
            self._breaker.record_failure()
            log.warning("rate limiter failing open: %s: %s", type(exc).__name__, exc)
            return None
        self._breaker.record_success()
        return reply

    async def _eval_with_retry(self, key: str, now_ms: int, cost: int) -> list[int]:
        """EVALSHA, falling back to one full EVAL on NOSCRIPT.

        A NOSCRIPT means Redis answered us promptly and correctly, just without our script
        cached (a restart, a ``SCRIPT FLUSH``, a failover onto a cold replica) -- it is not
        evidence of an unhealthy Redis, so it is resolved here rather than left for the caller
        to (mis)classify as a failure.
        """
        try:
            return await self._eval(key, now_ms, cost)
        except NoScriptError:
            return await self._eval_full(key, now_ms, cost)

    async def _eval(self, key: str, now_ms: int, cost: int) -> list[int]:
        async with asyncio.timeout(self._budget_seconds):
            return await self._redis.evalsha(  # type: ignore[no-any-return]
                _SCRIPT_SHA, 1, key, self._capacity, self._refill_per_second, now_ms, cost
            )

    async def _eval_full(self, key: str, now_ms: int, cost: int) -> list[int]:
        """The NOSCRIPT recovery path: one EVAL with the full body.

        Redis caches a script under its SHA as a side effect of EVAL, same as SCRIPT LOAD would
        -- so this one round trip both answers the current call and repopulates the cache for
        every EVALSHA after it. No follow-up SCRIPT LOAD is needed; adding one would spend a
        second round trip fetching back the SHA this module already computed locally.
        """
        async with asyncio.timeout(self._budget_seconds):
            return await self._redis.eval(  # type: ignore[no-any-return]
                _SCRIPT, 1, key, self._capacity, self._refill_per_second, now_ms, cost
            )
