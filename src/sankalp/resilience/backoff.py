"""Exponential backoff with jitter: ``min(2 ** attempt, cap) * (0.5 + random())``.

The formula is fixed by docs/spec.md and CLAUDE.md, and the jitter half of it is the part
that looks removable and is not. Without it, every workflow that failed during a downstream
outage computes the *same* delay from the same attempt number and returns to the queue in
lockstep -- so the moment the downstream recovers it is hit by the entire backlog at once
and falls over again. The multiplier spreads that herd across a window instead.

Full jitter (``random() * delay``) spreads even better, but its lower bound is ~0, which
lets a workflow retry almost immediately and burn an attempt against a downstream that has
had no time to recover. ``0.5 + random()`` -- i.e. 0.5x to 1.5x of the exponential -- keeps
a real floor under the delay while still decorrelating the herd.
"""

from __future__ import annotations

import random as _random

__all__ = ["compute_backoff"]

#: ``2 ** attempt`` is computed before the cap is applied, and Python integers do not
#: overflow -- a corrupt attempt counter would otherwise ask for a number with millions of
#: digits and stall the event loop computing it. 2**32 seconds is ~136 years, so clamping
#: the exponent here can never change a result that the cap would not have clamped anyway.
_MAX_EXPONENT = 32


def compute_backoff(
    attempt: int,
    *,
    cap_seconds: int = 60,
    rng: _random.Random | None = None,
) -> float:
    """Seconds to wait before retrying ``attempt``. Always positive, never deterministic.

    ``attempt`` is the 1-based count already stamped on the workflow row -- the dequeue
    query increments it as part of claiming, so the first execution runs with ``attempt=1``
    and a failure there backs off by ~2s rather than by ~1s.

    ``rng`` exists so tests can pin the jitter; production passes nothing and uses the
    module-level generator.
    """
    exponent = min(max(attempt, 0), _MAX_EXPONENT)
    jitter = (rng or _random).random()
    return min(float(2**exponent), float(cap_seconds)) * (0.5 + jitter)
