"""The circuit breaker, proved without touching Redis: threshold, cooldown, half-open, recovery.

Pure unit tests -- an injected clock and rng, no I/O -- the same split ``tests/test_backoff.py``
uses for ``compute_backoff``: the algorithm is proved here in isolation, and
``tests/test_ratelimit.py`` proves it wired into a real Redis behind a real TCP proxy. Every
timing-sensitive assertion pins the clock rather than sleeping, for the same reason
``test_backoff.py`` pins ``rng`` -- a real wall-clock wait here would either flake under load or
make the suite slow for no reason.
"""

from __future__ import annotations

import random
import time

import pytest

from sankalp.resilience.circuit import CircuitBreaker, CircuitState


class FakeClock:
    """A monotonic clock the test drives by hand."""

    def __init__(self, start: float = 0.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def test_closed_allows_and_stays_closed_on_success():
    breaker = CircuitBreaker(failure_threshold=3, cooldown_seconds=1.0)
    for _ in range(10):
        assert breaker.allow()
        breaker.record_success()
    assert breaker.state is CircuitState.CLOSED


def test_opens_after_exactly_the_threshold_of_consecutive_failures():
    breaker = CircuitBreaker(failure_threshold=3, cooldown_seconds=1.0)
    for _ in range(2):
        assert breaker.allow()
        breaker.record_failure()
    assert breaker.state is CircuitState.CLOSED, "must not open before the threshold is reached"

    assert breaker.allow()
    breaker.record_failure()
    assert breaker.state is CircuitState.OPEN


def test_a_success_in_between_resets_the_failure_count():
    """Only *consecutive* failures count -- one success in the middle must reset to zero."""
    breaker = CircuitBreaker(failure_threshold=3, cooldown_seconds=1.0)
    breaker.record_failure()
    breaker.record_failure()
    breaker.record_success()
    breaker.record_failure()
    breaker.record_failure()
    assert breaker.state is CircuitState.CLOSED, "the reset success must not be forgotten"


def test_open_refuses_every_caller_until_the_cooldown_elapses():
    """The actual cooldown is jittered to [0.5, 1.5) x cooldown_seconds (see
    resilience/circuit.py's docstring -- it reuses compute_backoff's formula), so this asserts
    the documented bounds rather than an exact value."""
    clock = FakeClock()
    breaker = CircuitBreaker(failure_threshold=1, cooldown_seconds=10.0, clock=clock)
    breaker.record_failure()
    assert breaker.state is CircuitState.OPEN

    clock.advance(10.0 * 0.5 - 0.001)
    assert not breaker.allow(), "cannot have elapsed even the minimum possible cooldown yet"

    clock.advance(10.0)  # now comfortably past the maximum possible cooldown (1.5x)
    assert breaker.allow(), "must have elapsed regardless of which way the jitter landed"


def test_exactly_one_probe_escapes_half_open_even_under_a_race():
    """The property the whole class exists for: many concurrent callers, one probe.

    No asyncio needed to prove this -- allow() is synchronous (by design, see the class
    docstring), so calling it N times in a row *is* the concurrent-callers case. If two calls
    to allow() ever both returned True while in HALF_OPEN, a probe storm would hit a Redis that
    has not had a moment to recover.
    """
    clock = FakeClock()
    breaker = CircuitBreaker(failure_threshold=1, cooldown_seconds=5.0, clock=clock)
    breaker.record_failure()
    clock.advance(5.0 * 1.5 + 1.0)  # comfortably past the maximum possible jittered cooldown

    admitted = [breaker.allow() for _ in range(50)]
    assert admitted.count(True) == 1, "exactly one caller may be admitted as the probe"
    assert breaker.state is CircuitState.HALF_OPEN


def test_a_successful_probe_closes_the_circuit_and_resets_everything():
    clock = FakeClock()
    breaker = CircuitBreaker(failure_threshold=1, cooldown_seconds=5.0, clock=clock)
    breaker.record_failure()
    clock.advance(5.0 * 1.5 + 1.0)
    assert breaker.allow()  # the probe

    breaker.record_success()
    assert breaker.state is CircuitState.CLOSED

    # A fresh failure reopens it (threshold=1) -- but the point under test is that
    # record_success cleared _consecutive_opens, so this next open is counted as open #1
    # again, not open #2 with an already-grown cooldown.
    breaker.record_failure()
    assert breaker.state is CircuitState.OPEN
    assert breaker._consecutive_opens == 1, "record_success must have reset the open counter"


def test_a_failed_probe_reopens_immediately_without_needing_the_threshold_again():
    clock = FakeClock()
    breaker = CircuitBreaker(failure_threshold=3, cooldown_seconds=5.0, clock=clock)
    breaker.record_failure()
    breaker.record_failure()
    breaker.record_failure()
    assert breaker.state is CircuitState.OPEN
    clock.advance(5.0 * 1.5 + 1.0)
    assert breaker.allow()  # the probe

    breaker.record_failure()  # the probe itself failed
    assert breaker.state is CircuitState.OPEN, "one failed probe is enough -- not threshold more"


def test_repeated_failed_probes_grow_the_cooldown_with_jitter():
    """Reuses compute_backoff -- see resilience/circuit.py's docstring. Growth must be
    monotonic in expectation and never collapse back to the base after repeated opens."""
    clock = FakeClock()
    breaker = CircuitBreaker(
        failure_threshold=1, cooldown_seconds=1.0, clock=clock, rng=random.Random(0)
    )
    breaker.record_failure()
    first_cooldown = breaker._cooldown

    for _ in range(4):
        clock.advance(first_cooldown + 1000)  # always past cooldown, however much it grew
        assert breaker.allow()
        breaker.record_failure()  # the probe fails every time -- reopens

    later_cooldown = breaker._cooldown
    assert later_cooldown > first_cooldown * 2, "should have grown well past the base by now"
    assert later_cooldown <= 1.0 * breaker._COOLDOWN_GROWTH_CAP * 1.5


def test_probe_attempts_are_decorrelated_across_instances():
    """Without jitter, every breaker that opened at the same instant would probe at the same
    instant -- the fleet-wide thundering herd this class exists to avoid (see its docstring)."""
    clock = FakeClock()
    cooldowns = set()
    for seed in range(50):
        breaker = CircuitBreaker(
            failure_threshold=1, cooldown_seconds=5.0, clock=clock, rng=random.Random(seed)
        )
        breaker.record_failure()
        cooldowns.add(breaker._cooldown)

    assert len(cooldowns) > 40, "cooldowns are repeating -- the jitter has been lost"


def test_state_property_reflects_the_machine_faithfully():
    clock = FakeClock()
    breaker = CircuitBreaker(failure_threshold=1, cooldown_seconds=1.0, clock=clock)
    assert breaker.state is CircuitState.CLOSED
    breaker.record_failure()
    assert breaker.state is CircuitState.OPEN
    clock.advance(1.0 * 1.5 + 1.0)
    breaker.allow()
    assert breaker.state is CircuitState.HALF_OPEN
    breaker.record_success()
    assert breaker.state is CircuitState.CLOSED


def test_construction_rejects_nonsensical_configuration():
    with pytest.raises(ValueError, match="failure_threshold"):
        CircuitBreaker(failure_threshold=0, cooldown_seconds=1.0)
    with pytest.raises(ValueError, match="cooldown_seconds"):
        CircuitBreaker(failure_threshold=1, cooldown_seconds=0.0)


def test_default_clock_is_real_monotonic_time_not_only_the_injected_one():
    """One test that never injects a clock, so the ``time.monotonic`` default path is actually
    exercised -- an injection seam that only tests ever use is a seam that can silently break
    in production. Cooldown is kept tiny so this stays fast."""
    breaker = CircuitBreaker(failure_threshold=1, cooldown_seconds=0.05)
    breaker.record_failure()
    assert not breaker.allow(), "the real cooldown cannot have elapsed yet"

    time.sleep(0.1)
    assert breaker.allow(), "the real cooldown has now elapsed"
