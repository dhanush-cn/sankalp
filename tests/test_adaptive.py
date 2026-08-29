"""The adaptive concurrency limiter (docs/spec.md, "Adaptive Concurrency"), proved bottom-up.

This file starts where the implementation does: :class:`_ResizableSemaphore`, the one
hand-rolled concurrency primitive in ``resilience/adaptive.py``, proved in isolation before the
gradient math or criticality shedding layered on top of it. See that module's docstring for the
swallow/cancel-debt technique the first block of tests exercises.

Every ``_ResizableSemaphore`` test measures the *real* ``asyncio.Semaphore`` through the class's
own public API only (:func:`_drain` acquires every immediately-available permit and counts
them) rather than trusting the class's internal bookkeeping -- an independent check, not a
tautology.

The :class:`~sankalp.resilience.adaptive.AdaptiveConcurrencyLimiter` tests below drive it
through an injected :class:`FakeClock` with synthetic RTT sequences -- no sleeps, no real
concurrency -- and were tuned against the real implementation, not hand-derived: every window
count and threshold below was picked by actually running the scenario first (see the session
transcript) so the assertions reflect what the arithmetic really does, not what it was
predicted to do. Each fail-proof asserts at the *first* window where a broken variant and the
real implementation actually diverge, not a later point where a subsequent operation could
happen to paper over the difference -- one draft of these tests initially didn't (see
:func:`test_swallow_then_partial_grow_compose_correctly` above for why that matters), so this
was checked by hand for each one below before it was trusted.
"""

from __future__ import annotations

import asyncio
import time

from starlette.applications import Starlette
from starlette.responses import PlainTextResponse
from starlette.routing import Route

from sankalp.api.middleware import AdaptiveConcurrencyMiddleware
from sankalp.resilience.adaptive import (
    AdaptiveConcurrencyLimiter,
    Criticality,
    _ResizableSemaphore,
)


async def _drain(pool: _ResizableSemaphore) -> int:
    """Acquire every immediately-available permit, returning how many there were."""
    count = 0
    while not pool.locked():
        await pool.acquire()
        count += 1
    return count


def _restore(pool: _ResizableSemaphore, count: int) -> None:
    for _ in range(count):
        pool.release()


async def test_initial_capacity_is_exactly_what_was_constructed():
    pool = _ResizableSemaphore(5)
    acquired = await _drain(pool)
    assert acquired == 5


async def test_grow_makes_new_permits_available_immediately():
    pool = _ResizableSemaphore(3)
    pool.resize(7)
    acquired = await _drain(pool)
    assert acquired == 7


async def test_shrink_does_not_evict_permits_already_held():
    """The gradient shrinks *future* admission, never work already in flight -- module docstring."""
    pool = _ResizableSemaphore(5)
    held = await _drain(pool)
    assert held == 5

    pool.resize(2)
    assert pool.locked(), "shrinking must not evict a permit a caller is already holding"

    _restore(pool, held)
    # The shrink is paid down lazily as those held permits come back -- now it should bite.
    acquired_after = await _drain(pool)
    assert acquired_after == 2


async def test_resize_is_lossless_under_oscillation():
    """Fail-proof: shrink by d, then immediately grow back by d before any permit returns.

    A broken resize that always issues a fresh ``release()`` on grow -- instead of cancelling
    outstanding debt first -- would double-count here: the pool would embody ``target + d``
    permits instead of ``target``, silently exceeding whatever ceiling asked for the shrink in
    the first place. This asserts capacity is restored exactly: no more, no less.
    """
    pool = _ResizableSemaphore(10)
    pool.resize(4)  # shrink by 6 -- pure debt, nothing acquired yet to swallow
    pool.resize(10)  # grow back by 6 -- must cancel the debt, not add 6 fresh permits

    acquired = await _drain(pool)
    assert acquired == 10, "resize leaked or double-counted capacity across an oscillation"


async def test_resize_is_lossless_under_a_partial_oscillation():
    """Harder case: some debt is paid down (via a real release) before the grow arrives."""
    pool = _ResizableSemaphore(10)
    held = await _drain(pool)
    assert held == 10

    pool.resize(4)  # debt = 6
    pool.release()  # one held permit returns -- swallowed, debt now 5
    held -= 1

    pool.resize(10)  # grow back by 6: cancels the remaining 5 debt, 1 fresh release
    _restore(pool, held)  # return the rest of what was originally held

    acquired = await _drain(pool)
    assert acquired == 10, "resize leaked or double-counted capacity across a partial oscillation"


async def test_swallow_then_partial_grow_compose_correctly():
    """The swallow path (release while debt > 0) and the cancel path (grow while debt > 0)
    chained in sequence, ending mid-debt rather than fully paid or fully outstanding.

    Asserts immediately after the grow, before any further release can happen -- a broken grow
    that always releases the full delta instead of cancelling debt first would release 2 fresh
    permits here instead of 1, but that extra permit gets silently swallowed by the *next*
    release() call regardless (debt=1 was never cancelled by the broken code, so the first of
    the 8 remaining returns eats it) -- checking only the final total after those returns would
    mask the bug entirely. The intermediate check is what actually catches it.
    """
    pool = _ResizableSemaphore(10)
    await _drain(pool)  # acquire all 10, semaphore now empty

    pool.resize(7)  # shrink by 3 -> debt=3, nothing evicted yet
    pool.release()
    pool.release()  # return 2 held permits -> both swallowed, debt 3 -> 1

    pool.resize(9)  # grow by 2 while debt=1 -> cancel 1, release 1 fresh

    immediately_available = await _drain(pool)
    assert immediately_available == 1, (
        "expected exactly 1 fresh permit -- the cancelled debt unit must not also be released"
    )

    # _drain() above just acquired (and now holds) that 1 permit too, on top of the 8 still
    # held from before -- all 9 need to come back before the final count means anything.
    _restore(pool, 8 + immediately_available)
    acquired = await _drain(pool)
    assert acquired == 9, "target is 9 -- exactly 9 permits should be acquirable"


# ---------------------------------------------------------------------------
# AdaptiveConcurrencyLimiter -- gradient, window, rtt_min decay
# ---------------------------------------------------------------------------


class FakeClock:
    """A monotonic clock the test drives by hand (same shape as test_circuit.py's)."""

    def __init__(self, start: float = 0.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def _run_window(
    limiter: AdaptiveConcurrencyLimiter,
    clock: FakeClock,
    rtt: float,
    *,
    samples: int = 5,
    window_seconds: float = 1.0,
) -> None:
    """Feed ``samples`` copies of ``rtt`` into one window, then close it.

    The last ``record_rtt`` call happens only after the clock has already been advanced past
    ``window_seconds`` -- that is what triggers the close, per :meth:`record_rtt`'s own check.
    """
    for _ in range(samples - 1):
        limiter.record_rtt(rtt)
    clock.advance(window_seconds)
    limiter.record_rtt(rtt)


def test_limit_shrinks_in_the_very_window_rtt_rises_above_baseline():
    """Fail-proof #1. Break: skip the gradient term entirely (``new_limit = limit +
    sqrt(limit)`` unconditionally, as if gradient were pinned at 1.0) -- limit would only ever
    grow, never shrink, no matter how bad RTT gets. Checked immediately after the one window
    that sees the spike, not after several, so a later window's arithmetic can't mask it.

    Verified against the real implementation: window 1 (baseline, rtt=0.010) settles the limit
    at 24; window 2 (rtt=0.100, 10x) drops it to 16. A broken always-grow variant would instead
    raise it, to 28.
    """
    clock = FakeClock()
    limiter = AdaptiveConcurrencyLimiter(initial_limit=20, min_limit=5, max_limit=64, clock=clock)

    _run_window(limiter, clock, rtt=0.010)  # establishes rtt_min ~= 0.010
    before = limiter.limit

    _run_window(limiter, clock, rtt=0.100)  # a single window, 10x worse
    after = limiter.limit

    assert after < before, "limit did not shrink in the very window RTT rose"


def test_limit_recovers_in_the_very_window_rtt_falls_back_to_baseline():
    """Fail-proof #2. Break: drop the ``+ sqrt(limit)`` growth term (``new_limit =
    limit * gradient`` only) -- once gradient returns to 1.0 the limit would stay exactly flat
    forever, never growing back even though RTT has fully recovered. Checked immediately after
    the one recovery window, not after several.

    Verified against the real implementation: after the shrink in the test above (limit=16),
    one window back at rtt=0.010 grows it to 20. A broken no-growth-term variant would instead
    leave it at exactly 16 -- gradient=1.0 makes ``limit * gradient`` a no-op.
    """
    clock = FakeClock()
    limiter = AdaptiveConcurrencyLimiter(initial_limit=20, min_limit=5, max_limit=64, clock=clock)

    _run_window(limiter, clock, rtt=0.010)  # baseline
    _run_window(limiter, clock, rtt=0.100)  # suppress it
    low = limiter.limit

    _run_window(limiter, clock, rtt=0.010)  # rtt fully recovers
    after = limiter.limit

    assert after > low, "limit did not grow back in the very window RTT recovered"


def test_min_limit_floor_holds_under_sustained_extreme_rtt():
    """Fail-proof #3. Break: remove the final ``clamp(new_limit, min_limit, max_limit)``
    entirely (the inner gradient clamp to [0.5, 1.0] stays intact -- only the outer one on
    ``new_limit`` is removed).

    Verified against the real implementation: with ``min_limit=10`` and sustained rtt=10.0
    (1000x the 0.010 baseline), the real (clamped) limiter settles at exactly 10 by the second
    elevated window and stays there. The unclamped variant keeps falling past it -- 9, 7, 6, 5,
    4 -- converging near 4, the true unclamped equilibrium of a gradient pinned at its 0.5
    floor. Checked after 5 elevated windows, well inside the region where both have already
    settled into their respective steady states.
    """
    clock = FakeClock()
    limiter = AdaptiveConcurrencyLimiter(initial_limit=20, min_limit=10, max_limit=64, clock=clock)

    _run_window(limiter, clock, rtt=0.010)  # baseline
    for _ in range(5):
        _run_window(limiter, clock, rtt=10.0)  # sustained, extreme

    assert limiter.limit == 10, "limit dropped below min_limit under sustained extreme RTT"


def test_max_limit_ceiling_holds_under_sustained_low_rtt():
    """Fail-proof #4. Same clamp removed as #3, probed from the other direction: a perfectly
    stable, low RTT (gradient pinned at 1.0) has no fixed point -- ``limit + sqrt(limit)``
    grows forever with nothing to stop it.

    Verified against the real implementation: with ``max_limit=64`` and rtt=0.010 held
    constant every window, the real (clamped) limiter reaches exactly 64 by window 8 and stays
    there. The unclamped variant keeps climbing straight through it -- 72, 80, 88, ... 126 by
    window 15 and still rising. Checked after 12 windows, comfortably past where the clamped
    version has already settled at the ceiling.
    """
    clock = FakeClock()
    limiter = AdaptiveConcurrencyLimiter(initial_limit=20, min_limit=5, max_limit=64, clock=clock)

    for _ in range(12):
        _run_window(limiter, clock, rtt=0.010)  # perfectly stable baseline every window

    assert limiter.limit == 64, "limit exceeded max_limit under sustained low RTT"


def test_rtt_min_decay_lets_the_limit_fully_recover_after_a_sustained_baseline_shift():
    """Fail-proof #5. Break: remove the decay branch entirely -- adopt a genuinely lower
    ``window_min`` immediately (unchanged), but never nudge ``rtt_min`` up toward a *higher*
    ``window_min``, so a permanently elevated baseline can never be recognised as the new
    normal.

    Verified against the real implementation: rtt=0.010 baseline, then a permanent (not
    growing further) shift to rtt=0.030 sustained for 44 windows. The real (decaying) limiter
    is suppressed for a while (down to 5, the floor) but climbs back to 49 by window 44 as
    ``rtt_min`` catches up to the new normal and gradient returns toward 1.0. A frozen-rtt_min
    variant instead gets stuck at 5 and never leaves it -- checked at the same window count, so
    this is not a "give it more time" difference, it is a permanent difference.
    """
    clock = FakeClock()
    limiter = AdaptiveConcurrencyLimiter(initial_limit=20, min_limit=5, max_limit=64, clock=clock)

    _run_window(limiter, clock, rtt=0.010)  # baseline
    for _ in range(44):
        _run_window(limiter, clock, rtt=0.030)  # a new, stable, higher baseline -- sustained

    assert limiter.limit > 40, (
        "limit never recovered after a sustained (not still-rising) baseline shift -- "
        "rtt_min decay is not letting gradient return toward 1.0"
    )


def test_rtt_must_exclude_queueing_wait_or_the_limiter_spirals():
    """Fail-proof #8. This one doesn't break real code -- the code path it protects (the
    middleware timing a request's RTT starting only *after* ``acquire()`` returns, never
    before) doesn't exist until a later commit. Instead this is a standing comparison: the same
    real :class:`AdaptiveConcurrencyLimiter`, fed two different synthetic RTT sequences by the
    test's own driving loop, modelling what the middleware *would* record under each wiring.

    The scenario: true downstream/handler service time is a constant 0.010s, forever -- it
    never degrades. Arrivals permanently and vastly exceed capacity (1000 vs a ceiling of 64),
    so every window is saturated. A caller that doesn't get an immediate permit would wait up
    to a bounded ceiling (0.25s) before being shed.

    "Correct" feeds the limiter only the true 0.010s service time, regardless of saturation.
    "Broken" adds the full 0.25s wait on top, every saturated window, simulating what happens
    if that queueing wait were folded into RTT. Both start from one identical warm-up window at
    the true baseline (0.010s, unsaturated) -- without that, the very first window would anchor
    ``rtt_min`` to its own elevated value and the gradient math would see no rise at all,
    exactly the failure mode :func:`test_limit_shrinks_in_the_very_window_rtt_rises_above_baseline`
    depends on *not* happening.

    Verified against the real implementation: "correct" grows to and holds at the ceiling, 64,
    by window 8 (of the 15 checked) -- the limiter never learns anything is wrong, because
    nothing is. "Broken" collapses to the floor, 5, by window 7, and is still there at window
    15 -- the runaway feedback loop the design doc warns about: saturation reads as latency,
    gradient shrinks, fewer permits mean more callers hit the wait ceiling, "latency" rises
    further.
    """
    true_service_seconds = 0.010
    shed_wait_ceiling_seconds = 0.25
    persistent_arrivals = 1000
    saturated_windows = 15

    def run(*, include_queueing_wait: bool) -> int:
        clock = FakeClock()
        limiter = AdaptiveConcurrencyLimiter(
            initial_limit=20, min_limit=5, max_limit=64, clock=clock
        )
        _run_window(limiter, clock, rtt=true_service_seconds)  # clean warm-up, unsaturated
        for _ in range(saturated_windows):
            saturated = persistent_arrivals > limiter.limit
            wait = shed_wait_ceiling_seconds if (include_queueing_wait and saturated) else 0.0
            _run_window(limiter, clock, rtt=true_service_seconds + wait)
        return limiter.limit

    correct = run(include_queueing_wait=False)
    broken = run(include_queueing_wait=True)

    assert correct >= 60, "limit degraded even though the true downstream never slowed down"
    assert broken <= 10, (
        "expected the queueing-inclusion feedback loop to collapse the limit toward the floor"
    )


# ---------------------------------------------------------------------------
# AdaptiveConcurrencyLimiter -- criticality-based shedding
# ---------------------------------------------------------------------------


async def test_low_sheds_immediately_high_waits_then_sheds_under_real_saturation():
    """Fail-proof #6. Break: remove the ``locked()`` fast path so ``LOW`` also waits like
    ``HIGH`` (i.e. both go through ``asyncio.timeout``).

    Real asyncio concurrency and real (small) sleeps -- the one place that's unavoidable, since
    this asserts on ``asyncio.timeout()``'s actual behavior, the same exception
    ``test_circuit.py`` already makes for its one "default clock is real" test. No fake clock:
    the limiter's own default (real) clock is used, since ``waited_seconds`` and the timeout
    both need to agree on what "time" means here.
    """
    limiter = AdaptiveConcurrencyLimiter(
        initial_limit=2, min_limit=1, max_limit=10, high_criticality_wait_seconds=0.2
    )

    async def hold_a_permit_forever() -> None:
        async with limiter.acquire(Criticality.HIGH) as decision:
            assert decision.admitted
            await asyncio.sleep(10)  # cancelled below, never actually runs to completion

    holders = [asyncio.create_task(hold_a_permit_forever()) for _ in range(2)]
    await asyncio.sleep(0.05)  # let both holders actually acquire before saturating further

    try:
        started = time.monotonic()
        async with limiter.acquire(Criticality.LOW) as low_decision:
            pass
        low_elapsed = time.monotonic() - started

        started = time.monotonic()
        async with limiter.acquire(Criticality.HIGH) as high_decision:
            pass
        high_elapsed = time.monotonic() - started
    finally:
        for holder in holders:
            holder.cancel()
        await asyncio.gather(*holders, return_exceptions=True)

    assert not low_decision.admitted, "LOW should have been shed under saturation"
    assert low_elapsed < 0.05, "LOW waited instead of shedding immediately"

    assert not high_decision.admitted, "HIGH should have been shed once its wait ran out"
    assert high_elapsed >= 0.15, "HIGH did not wait close to the bounded ceiling before shedding"


# ---------------------------------------------------------------------------
# AdaptiveConcurrencyMiddleware -- fail-proof #8, at the real wiring level
# ---------------------------------------------------------------------------


def _make_app(limiter: AdaptiveConcurrencyLimiter, *, handler_seconds: float) -> Starlette:
    """A throwaway Starlette app -- not ``sankalp.api.main.app`` -- with one route whose
    "true" downstream/handler time is constant, wrapped in the real
    ``AdaptiveConcurrencyMiddleware``. Deliberately not the real app: it needs no Postgres, and
    keeping it minimal is what makes ``handler_seconds`` an honest constant instead of
    something real business logic could confound.
    """

    async def endpoint(request: object) -> PlainTextResponse:
        await asyncio.sleep(handler_seconds)
        return PlainTextResponse("ok")

    app = Starlette(routes=[Route("/work", endpoint)])
    app.state.concurrency_limiter = limiter
    app.add_middleware(AdaptiveConcurrencyMiddleware)
    return app


async def _request(app: Starlette) -> int:
    """One request driven by hand-built raw ASGI scope/receive/send -- not ``httpx``.

    Discovered empirically while building this test: ``httpx.ASGITransport`` adds enough of
    its own real overhead under the concurrency this fail-proof needs (tens to hundreds of
    concurrent callers) that it inflates measured RTT on its own, in *both* the correct and the
    broken variant -- confounding the exact effect this test exists to isolate. Calling the
    app's ASGI callable directly removes that layer; what is left is the real middleware, the
    real route, and real ``asyncio`` scheduling contention, nothing else.
    """
    status: dict[str, int] = {}

    async def receive() -> dict[str, object]:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message: dict[str, object]) -> None:
        if message["type"] == "http.response.start":
            status["code"] = message["status"]  # type: ignore[assignment]

    scope: dict[str, object] = {
        "type": "http",
        "method": "GET",
        "path": "/work",
        "raw_path": b"/work",
        "query_string": b"",
        "headers": [(b"criticality", b"high")],
        "app": app,
        "http_version": "1.1",
        "scheme": "http",
        "server": ("test", 80),
        "client": ("test", 1234),
    }
    await app(scope, receive, send)  # type: ignore[arg-type]
    return status["code"]


async def test_middleware_rtt_excludes_admission_wait_under_real_saturation():
    """Fail-proof #8, now proven at the real wiring level -- the step-2 version
    (:func:`test_rtt_must_exclude_queueing_wait_or_the_limiter_spirals`) was necessarily a
    synthetic comparison, since this call site didn't exist yet. This one breaks the actual
    middleware.

    Break: move ``started = time.monotonic()`` in ``AdaptiveConcurrencyMiddleware.__call__`` to
    *before* ``async with limiter.acquire(criticality)`` instead of after -- folding a HIGH
    caller's real admission wait into what gets recorded as RTT.

    The route's handler sleeps a constant 0.003s -- the "true" downstream time, which never
    changes for the whole test. 100 real concurrent callers hammer it continuously for 1.5s,
    permanently exceeding ``initial_limit=10``. Verified against the real implementation:

    * Correct (this test, as committed): the limit grows and oscillates healthily in the
      high teens to low twenties -- well above ``min_limit=8`` -- because the middleware never
      tells the gradient about the admission wait, only the real (constant) handler time.
    * Broken (moving the timer, confirmed by hand before trusting this test): the limit
      collapses to ``min_limit=8`` within a few windows and stays there for the rest of the
      run -- the runaway feedback loop record_rtt's own docstring warns about, reproduced
      against the real call site, not a stand-in for it.
    """
    limiter = AdaptiveConcurrencyLimiter(
        initial_limit=10,
        min_limit=8,
        max_limit=30,
        window_seconds=0.05,
        high_criticality_wait_seconds=0.08,
    )
    app = _make_app(limiter, handler_seconds=0.003)

    for _ in range(5):  # clean warm-up, anchoring rtt_min at the true baseline
        await _request(app)
    await asyncio.sleep(0.06)  # let that warm-up window close

    deadline = time.monotonic() + 1.5

    async def hammer() -> None:
        while time.monotonic() < deadline:
            await _request(app)

    await asyncio.gather(*(hammer() for _ in range(100)))

    assert limiter.limit > 12, (
        "limit collapsed toward the floor even though the true downstream time never changed"
    )
