"""A circuit breaker around a single dependency: stop paying its timeout once it is down.

Fail-open on a dead Redis (:mod:`sankalp.resilience.ratelimit`) only works if "dead" is
detected fast. Without this, every request pays a full connect/read timeout against a
socket that will never answer -- so a Redis outage does not just remove rate limiting, it
also makes the API slow, and under a constant arrival rate "slow" means the request queue
grows without bound. Fail-open decides *what* the answer is when Redis is unreachable; this
decides *how fast* that answer comes back.

Three states, the standard shape (Fowler, "CircuitBreaker"):

    CLOSED     -- calls go through. N consecutive failures -> OPEN.
    OPEN       -- calls are rejected immediately, no socket touched. After a cooldown -> HALF_OPEN.
    HALF_OPEN  -- exactly one probe call is let through. It succeeds -> CLOSED. It fails -> OPEN,
                  cooldown restarts (with backoff -- see below).

**Synchronous by design, and that is load-bearing, not a style choice.** There is no ``await``
anywhere in :meth:`CircuitBreaker.allow`, :meth:`record_success`, or :meth:`record_failure`. A
plain function with no await points cannot be pre-empted mid-body on a single event loop, so
these three methods are atomic against each other with no lock. That is what guarantees
*exactly one* caller sees ``allow() -> True`` during ``HALF_OPEN`` even when many callers race
it concurrently -- the transition out of ``HALF_OPEN`` (into "probe in flight") happens inside
the same synchronous call that decided to admit. **Do not add an ``await`` inside these
methods** -- doing so reopens exactly the race this class exists to close.

**Per-process, never shared through Redis.** What this protects against -- *this* process
wasting a timeout on a socket to a dependency that will not answer -- is a per-process problem.
Sharing the state would mean coordinating it through the very dependency that might be down,
which is either impossible (Redis is down) or adds another round trip to every check (Redis is
up, and now slower). Each API process runs its own breaker and discovers Redis's health
independently; a fleet does not open in lockstep unless the outage genuinely reaches all of
them, and even then the probe attempts are decorrelated by the jittered cooldown below.

**Cooldown reuses :func:`~sankalp.resilience.backoff.compute_backoff`**, keyed on the number of
consecutive times the breaker has *opened* (not the failure count that opened it). Without
jitter, every API process that failed at the same instant probes Redis at the same instant --
the thundering herd `backoff.py`'s own docstring already argues against, here applied to the
probe rather than to a workflow retry. A failed probe increments that counter and reopens with a
longer, still-jittered cooldown; a successful probe resets it to zero.
"""

from __future__ import annotations

import enum
import logging
import random as _random
import time
from collections.abc import Callable

from sankalp.resilience.backoff import compute_backoff

__all__ = ["CircuitBreaker", "CircuitState"]

log = logging.getLogger("sankalp.circuit")


class CircuitState(enum.Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """Trip after ``failure_threshold`` consecutive failures; probe after ``cooldown_seconds``.

    Takes plain parameters, not a ``Settings`` object -- the same convention
    :func:`~sankalp.resilience.backoff.compute_backoff` uses (``cap_seconds=``), so a caller in
    ``resilience/`` never has to construct a full ``Settings`` to use a piece of it. ``clock``
    and ``rng`` are the same kind of injection seam ``compute_backoff`` gives ``rng``: tests
    pin them, production passes neither and gets ``time.monotonic`` and the module RNG.

    ``clock`` is deliberately ``time.monotonic``, not ``time.time()``. A cooldown is a
    process-local duration, not a value compared across processes (contrast
    :mod:`sankalp.resilience.ratelimit`'s bucket ``now_ms``, which *is* compared across
    processes and therefore must be the wall clock). ``time.monotonic`` cannot be stepped
    backwards or forwards by NTP or an operator, which matters here: a clock step must never be
    able to expire a cooldown early or extend one indefinitely.
    """

    #: Growth cap on the cooldown multiplier in :meth:`_open` -- 8x the base cooldown is far
    #: enough to spread out a fleet of processes probing a genuinely long outage without making
    #: an operator wait an unbounded time for the first successful probe to matter once Redis is
    #: actually back.
    _COOLDOWN_GROWTH_CAP = 8

    def __init__(
        self,
        *,
        failure_threshold: int = 5,
        cooldown_seconds: float = 5.0,
        clock: Callable[[], float] = time.monotonic,
        rng: _random.Random | None = None,
    ) -> None:
        if failure_threshold < 1:
            raise ValueError(f"failure_threshold must be >= 1, got {failure_threshold}")
        if cooldown_seconds <= 0:
            raise ValueError(f"cooldown_seconds must be > 0, got {cooldown_seconds}")
        self._failure_threshold = failure_threshold
        self._cooldown_seconds = cooldown_seconds
        self._clock = clock
        self._rng = rng or _random.Random()

        self._state = CircuitState.CLOSED
        self._consecutive_failures = 0
        #: Consecutive times the breaker has opened, reset by a successful probe. Feeds
        #: compute_backoff so repeated failed probes back off instead of hammering Redis at a
        #: fixed interval.
        self._consecutive_opens = 0
        self._opened_at: float = 0.0
        self._cooldown: float = cooldown_seconds

    @property
    def state(self) -> CircuitState:
        return self._state

    def allow(self) -> bool:
        """May the caller proceed to the real call? No I/O, no await -- see the class docstring.

        ``CLOSED`` always allows. ``OPEN`` allows once the cooldown has elapsed, transitioning to
        ``HALF_OPEN`` and admitting exactly the caller that made the elapsed-cooldown check true
        -- the state flip and the "yes" happen in the same synchronous call, which is what stops
        a second concurrent caller from also seeing ``HALF_OPEN`` with no probe yet recorded.
        ``HALF_OPEN`` allows nothing further until that one probe reports in.
        """
        if self._state is CircuitState.CLOSED:
            return True
        if self._state is CircuitState.OPEN:
            if self._clock() - self._opened_at < self._cooldown:
                return False
            self._state = CircuitState.HALF_OPEN
            log.info("circuit half-open: admitting one probe after %.2fs cooldown", self._cooldown)
            return True
        # HALF_OPEN: the caller above already consumed the one admitted slot by flipping the
        # state in this same synchronous call. Every other caller -- including one that reaches
        # here microseconds later, before the probe has reported in -- sees state is already
        # HALF_OPEN rather than OPEN, so it falls straight to this branch and is refused. No
        # separate "probe in flight" flag is needed; the state transition itself is the flag.
        return False

    def record_success(self) -> None:
        """The last admitted call succeeded. Closes the circuit and clears all counters."""
        if self._state is not CircuitState.CLOSED:
            log.info("circuit closed: probe succeeded")
        self._state = CircuitState.CLOSED
        self._consecutive_failures = 0
        self._consecutive_opens = 0

    def record_failure(self) -> None:
        """The last admitted call failed. Opens the circuit once the threshold is reached, or
        immediately if the failing call was itself the half-open probe."""
        if self._state is CircuitState.HALF_OPEN:
            self._open()
            return
        self._consecutive_failures += 1
        if self._consecutive_failures >= self._failure_threshold:
            self._open()

    def _open(self) -> None:
        self._state = CircuitState.OPEN
        self._consecutive_failures = 0
        self._consecutive_opens += 1
        self._opened_at = self._clock()
        # compute_backoff(attempt, cap_seconds) == min(2**attempt, cap) * (0.5 + jitter). Used
        # here as a dimensionless multiplier on cooldown_seconds rather than as a count of
        # seconds directly: attempt 0 (the first open) gives a factor in [0.5, 1.5), centered on
        # the configured cooldown; each further open doubles it, capped at
        # _COOLDOWN_GROWTH_CAP x the base -- the same exponential-with-jitter shape backoff.py
        # uses for workflow retries, decorrelating repeated probe attempts across a fleet of API
        # processes the same way it decorrelates workflow retries.
        factor = compute_backoff(
            self._consecutive_opens - 1, cap_seconds=self._COOLDOWN_GROWTH_CAP, rng=self._rng
        )
        self._cooldown = self._cooldown_seconds * factor
        log.warning(
            "circuit open: %d consecutive failure(s); next probe in %.2fs (open #%d)",
            self._failure_threshold,
            self._cooldown,
            self._consecutive_opens,
        )
