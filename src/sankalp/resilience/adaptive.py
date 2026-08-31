"""Adaptive concurrency admission for the API (docs/spec.md, "Adaptive Concurrency").

Built bottom-up: this file starts with :class:`_ResizableSemaphore`, the one hand-rolled
concurrency primitive here, proved in isolation before anything is layered on top of it. Next,
:class:`AdaptiveConcurrencyLimiter`'s gradient/window logic decides *what* to resize it to.
Last, :meth:`AdaptiveConcurrencyLimiter.acquire` -- criticality-based shedding -- decides who
gets admitted when it can't grow fast enough. The API middleware that wires this into real
requests, and records real RTT into it, is still a later commit.

**Why not just `asyncio.Semaphore(limit)`.** The stdlib semaphore has no public resize API, and
its capacity is fixed at construction. A gradient limiter needs to change that capacity every
window without losing whatever is currently blocked on it -- recreating a fresh
``asyncio.Semaphore`` each window would strand any caller already awaiting the old object.

**How resize works without touching `Semaphore`'s private state.** One real
``asyncio.Semaphore`` is kept alive for the whole lifetime of a `_ResizableSemaphore`, and its
capacity is changed only through its own public ``acquire``/``release`` -- never by poking
``_value``:

- **Growing** by ``d`` calls ``release()`` on the real semaphore ``d`` times, right away: more
  permits become available immediately.
- **Shrinking** by ``d`` does *not* touch the real semaphore at all. An in-flight caller keeps
  the permit it already holds -- that is deliberate, and matches the spec's framing of the
  gradient shrinking the limit *before* queues explode, i.e. it throttles future admission, it
  does not evict current work. Instead it records ``d`` as debt. The next ``d`` permits that
  come back through :meth:`_ResizableSemaphore.release` are swallowed -- never handed back to
  the real semaphore -- which is how the real capacity actually shrinks, lazily, as work
  finishes.
- **Growing while debt is still outstanding** cancels the debt first, before releasing anything
  new. Debt that hasn't been paid down yet was never actually removed from the pool, so
  cancelling it costs nothing -- it just means fewer of the next few returned permits get
  swallowed. Only the portion of the grow beyond the cancelled debt turns into a real
  ``release()`` call. This is what makes a shrink immediately followed by an equal-and-opposite
  grow a true no-op: nothing is lost, nothing is double-counted.

The invariant this all rests on: at any instant, the real semaphore's total embodied capacity
(available + currently held) equals ``target + debt``, where ``target`` is the most recently
requested capacity and ``debt`` is how much of a pending shrink hasn't been paid down yet. Every
method below preserves it.
"""

from __future__ import annotations

import asyncio
import enum
import json
import logging
import math
import time
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone

__all__ = [
    "AdaptiveConcurrencyLimiter",
    "ConcurrencyDecision",
    "Criticality",
    "_ResizableSemaphore",
]

log = logging.getLogger("sankalp.adaptive")


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


class _ResizableSemaphore:
    """A concurrency permit pool whose capacity can change at runtime.

    See the module docstring for why this exists and how resize works. ``target`` starts at
    ``initial_capacity`` and only ever changes through :meth:`resize`.
    """

    def __init__(self, initial_capacity: int) -> None:
        if initial_capacity < 0:
            raise ValueError(f"initial_capacity must be >= 0, got {initial_capacity}")
        self._sem = asyncio.Semaphore(initial_capacity)
        self._target = initial_capacity
        self._debt = 0

    @property
    def target(self) -> int:
        """The capacity this pool is converging toward."""
        return self._target

    def resize(self, target: int) -> None:
        """Change the target capacity. Growth is immediate; shrinkage is paid down lazily."""
        if target < 0:
            raise ValueError(f"target must be >= 0, got {target}")
        delta = target - self._target
        self._target = target
        if delta > 0:
            cancel = min(delta, self._debt)
            self._debt -= cancel
            fresh = delta - cancel
            for _ in range(fresh):
                self._sem.release()
        elif delta < 0:
            self._debt += -delta

    async def acquire(self) -> None:
        await self._sem.acquire()

    def locked(self) -> bool:
        """True if a caller would block right now -- see :meth:`acquire`'s non-blocking sibling."""
        return self._sem.locked()

    def release(self) -> None:
        """Return a permit acquired via :meth:`acquire`.

        Swallowed (not handed back to the real semaphore) while debt from a pending shrink is
        still outstanding -- see the module docstring's invariant.
        """
        if self._debt > 0:
            self._debt -= 1
        else:
            self._sem.release()


class Criticality(enum.Enum):
    """A caller's declared importance, used only to decide who sheds first under saturation."""

    HIGH = "high"
    LOW = "low"


@dataclass(frozen=True, slots=True)
class ConcurrencyDecision:
    """The result of :meth:`AdaptiveConcurrencyLimiter.acquire`.

    ``waited_seconds`` is only ever nonzero for :attr:`Criticality.HIGH` -- :attr:`LOW` never
    waits, by construction (see :meth:`AdaptiveConcurrencyLimiter._admit`).
    """

    admitted: bool
    waited_seconds: float = 0.0


class AdaptiveConcurrencyLimiter:
    """Gradient/TCP-Vegas concurrency target, resized into a :class:`_ResizableSemaphore`.

    :meth:`record_rtt`/:attr:`limit` decide *what* the target capacity is;
    :meth:`acquire`/:class:`Criticality` decide *who* gets in when it can't grow fast enough --
    :attr:`Criticality.LOW` sheds immediately rather than wait at all, :attr:`Criticality.HIGH`
    gets a brief bounded wait before it, too, sheds (docs/spec.md's table: 503 immediately vs.
    503 after a brief wait).

    Every ``window_seconds``, the samples handed to :meth:`record_rtt` since the last close are
    reduced to one update, exactly per docs/spec.md::

        gradient   = clamp(rtt_min / rtt_avg, 0.5, 1.0)
        new_limit  = limit * gradient + sqrt(limit)     # queue-size allowance
        limit      = clamp(new_limit, min_limit, max_limit)

    ``rtt_min`` is the part the spec leaves unspecified ("decayed slowly"). A genuinely lower
    minimum is adopted immediately -- real capacity improved, there is no reason to wait on it.
    A window whose minimum is *higher* than the current ``rtt_min`` only nudges it up by
    ``rtt_min_decay`` of the gap, rather than jumping straight there. That is what lets a
    permanent baseline shift (a downstream dependency that is now durably slower, not just
    having one bad window) eventually be recognised as the new normal and let ``gradient``
    return to 1.0, while a single noisy window can't yank the floor up and make the limiter
    permanently pessimistic.

    ``clock`` defaults to ``time.monotonic`` -- window and RTT durations are process-local
    intervals, never compared across processes, the same reasoning
    :class:`~sankalp.resilience.circuit.CircuitBreaker` already gives for the same default.
    """

    def __init__(
        self,
        *,
        initial_limit: int,
        min_limit: int,
        max_limit: int,
        window_seconds: float = 1.0,
        rtt_min_decay: float = 0.05,
        high_criticality_wait_seconds: float = 0.25,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if not (0 < min_limit <= initial_limit <= max_limit):
            raise ValueError(
                f"require 0 < min_limit <= initial_limit <= max_limit, "
                f"got min_limit={min_limit}, initial_limit={initial_limit}, max_limit={max_limit}"
            )
        if window_seconds <= 0:
            raise ValueError(f"window_seconds must be > 0, got {window_seconds}")
        if not (0 < rtt_min_decay <= 1.0):
            raise ValueError(f"rtt_min_decay must be in (0, 1], got {rtt_min_decay}")
        if high_criticality_wait_seconds <= 0:
            raise ValueError(
                f"high_criticality_wait_seconds must be > 0, got {high_criticality_wait_seconds}"
            )

        self._min_limit = min_limit
        self._max_limit = max_limit
        self._window_seconds = window_seconds
        self._rtt_min_decay = rtt_min_decay
        self._high_criticality_wait_seconds = high_criticality_wait_seconds
        self._clock = clock

        self._limit = initial_limit
        self._pool = _ResizableSemaphore(initial_limit)
        self._rtt_min: float | None = None
        self._window_samples: list[float] = []
        self._window_start = clock()

    @property
    def limit(self) -> int:
        """The current adaptive limit -- also emitted by ``_close_window`` as the
        ``adaptive_concurrency.window_closed`` log event on every window close."""
        return self._limit

    @asynccontextmanager
    async def acquire(self, criticality: Criticality) -> AsyncIterator[ConcurrencyDecision]:
        """Admit or shed, by criticality. Always yields -- check ``.admitted`` before doing the
        work it gates. Releases the permit on exit, iff one was actually acquired.

        Deliberately does **not** call :meth:`record_rtt` itself. The caller measures and
        records RTT around only the real work done inside the ``async with`` block, after this
        already yielded -- never around the wait this method itself might have spent getting
        here. See :meth:`record_rtt`'s docstring for why folding admission wait into RTT would
        be a runaway feedback loop, not a formula bug.
        """
        decision = await self._admit(criticality)
        try:
            yield decision
        finally:
            if decision.admitted:
                self._pool.release()

    async def _admit(self, criticality: Criticality) -> ConcurrencyDecision:
        if criticality is Criticality.LOW:
            # Non-blocking: `locked()` and `acquire()` have no `await` between them, so nothing
            # else can run on this single-threaded loop in between -- a permit visible at the
            # `locked()` check is still there at the `acquire()` call.
            if self._pool.locked():
                return ConcurrencyDecision(admitted=False)
            await self._pool.acquire()
            return ConcurrencyDecision(admitted=True)

        started = self._clock()
        try:
            async with asyncio.timeout(self._high_criticality_wait_seconds):
                await self._pool.acquire()
        except TimeoutError:
            return ConcurrencyDecision(admitted=False, waited_seconds=self._clock() - started)
        return ConcurrencyDecision(admitted=True, waited_seconds=self._clock() - started)

    def record_rtt(self, rtt_seconds: float) -> None:
        """Feed one completed request's RTT. Closes and applies the window once it's full.

        The caller decides what "RTT" means -- record only real downstream/handler work, never
        any time spent queued waiting for admission. Queueing time is a symptom of saturation,
        not of downstream latency; folding it in here would read as latency rising, shrink
        ``limit`` further, deepen the queue, and never recover.
        """
        self._window_samples.append(rtt_seconds)
        if self._clock() - self._window_start >= self._window_seconds:
            self._close_window()

    def _close_window(self) -> None:
        if self._window_samples:
            window_min = min(self._window_samples)
            rtt_avg = sum(self._window_samples) / len(self._window_samples)

            if self._rtt_min is None or window_min < self._rtt_min:
                self._rtt_min = window_min
            else:
                self._rtt_min += self._rtt_min_decay * (window_min - self._rtt_min)

            gradient = 1.0 if rtt_avg <= 0 else _clamp(self._rtt_min / rtt_avg, 0.5, 1.0)
            new_limit = self._limit * gradient + math.sqrt(self._limit)
            self._limit = int(_clamp(new_limit, self._min_limit, self._max_limit))
            self._pool.resize(self._limit)

            log.info(
                json.dumps(
                    {
                        "event": "adaptive_concurrency.window_closed",
                        "ts": datetime.now(timezone.utc).isoformat(),
                        "limit": self._limit,
                        "rtt_avg_s": rtt_avg,
                        "rtt_min_s": self._rtt_min,
                        "gradient": gradient,
                    }
                )
            )

        self._window_samples = []
        self._window_start = self._clock()
