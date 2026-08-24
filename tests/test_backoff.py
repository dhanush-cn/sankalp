"""The retry delay, proved: it grows, it is capped, and it is never the same twice.

The jitter is the property under test here. Without it every workflow that failed during a
downstream outage computes an identical delay and returns to the queue in lockstep, so the
downstream is knocked over again the instant it recovers.
"""

from __future__ import annotations

import random

from sankalp.resilience.backoff import compute_backoff

CAP = 60


def test_delay_grows_with_the_attempt_number():
    """Compared at a pinned jitter, so this is about the exponential and not the dice."""
    delays = [
        compute_backoff(attempt, cap_seconds=CAP, rng=random.Random(0))
        for attempt in range(1, 6)
    ]
    assert delays == sorted(delays)
    assert delays[0] < delays[-1]


def test_the_exponential_is_capped():
    """min(2 ** attempt, cap): attempt 20 would otherwise ask for twelve days."""
    for attempt in (10, 20, 1000):
        delay = compute_backoff(attempt, cap_seconds=CAP)
        assert delay <= CAP * 1.5


def test_jitter_spreads_identical_attempts():
    """The whole point. A thousand workflows failing on the same attempt of the same
    downstream outage must not come back at the same moment."""
    delays = {compute_backoff(3, cap_seconds=CAP) for _ in range(200)}

    assert len(delays) > 190, "delays are repeating -- the jitter has been lost"
    assert max(delays) - min(delays) > 1.0, "the spread is too narrow to break up a herd"


def test_the_delay_stays_within_half_and_one_and_a_half_of_the_exponential():
    """0.5 + random() is bounded on both sides on purpose: full jitter can return ~0, which
    lets a workflow retry instantly and burn an attempt on a downstream that has not had a
    moment to recover."""
    for _ in range(200):
        delay = compute_backoff(4, cap_seconds=CAP)
        assert 8.0 <= delay < 24.0  # 2**4 == 16, times [0.5, 1.5)


def test_delay_is_always_positive():
    for attempt in (0, 1, 5):
        assert compute_backoff(attempt, cap_seconds=CAP) > 0
