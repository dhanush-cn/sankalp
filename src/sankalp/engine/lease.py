"""The lease on a claimed workflow, and the two defenses that keep it from being stolen.

A workflow is ours for ``lease_duration_seconds`` after we claim it. When that runs out the
dequeue query's ``status = 'RUNNING' AND lease_expires_at < now()`` branch hands the row to
somebody else -- which is precisely the mechanism that recovers a crashed worker, and
precisely what must not happen to a worker that is merely *slow*. docs/spec.md prescribes
both defenses and says to use both; this class is the shared state they need to agree on:

1. The worker runs a background task that renews every ``lease_duration / divisor`` for as
   long as a step is running (:meth:`renew`).
2. The executor checks before starting each step and renews if less than that same slice is
   left (:meth:`renew_if_needed`), so a workflow never enters a step on a nearly-dead lease.

Both go through one :class:`Lease` so the second is a cheap no-op whenever the first is
keeping up, instead of a second renewal schedule racing the first.

Time is tracked with :func:`time.monotonic`, not by comparing against the timestamp in the
row. Asking the server how much lease is left would be a round trip *per check*, which is
the cost the check exists to avoid; and monotonic time cannot jump backwards when NTP steps
the wall clock mid-step. The DB remains the authority on when the lease actually expires --
this is only a local estimate of when to go ask it to move.
"""

from __future__ import annotations

import asyncio
import time
from uuid import UUID

import asyncpg

from sankalp.engine.errors import PreemptedError
from sankalp.storage import workflows as workflow_writes
from sankalp.storage.queue import ClaimedWorkflow

__all__ = ["Lease"]


class Lease:
    """A claimed workflow's lease: how much is left, and how to extend it.

    Once a renewal comes back having matched zero rows the lease is **lost, permanently**.
    Fencing tokens only ever increase, so a claim that has moved past ours can never move
    back to it, and every later call short-circuits instead of issuing SQL that is now
    guaranteed to touch nothing.
    """

    __slots__ = (
        "_pool",
        "_own",
        "_duration",
        "_renew_after",
        "_deadline",
        "_lock",
        "_lost",
    )

    def __init__(
        self,
        pool: asyncpg.Pool,
        claimed: ClaimedWorkflow,
        *,
        duration_seconds: float,
        renew_divisor: int = 3,
    ) -> None:
        if renew_divisor < 2:
            raise ValueError(
                f"renew_divisor must be >= 2, got {renew_divisor}: renewing only once the "
                "lease is at or past half gone leaves no room for the renewal itself to be "
                "slow, and a renewal that lands late has already lost the row"
            )
        self._pool = pool
        self._own = workflow_writes.Ownership.of(claimed)
        self._duration = float(duration_seconds)
        #: Renew when less than this much is left -- the same slice the worker's background
        #: renewer ticks on, so the two never disagree about what "running low" means.
        self._renew_after = self._duration / renew_divisor
        # The claim stamped the row a round trip ago, so this is optimistic by that round
        # trip. Bounded by milliseconds against a lease measured in tens of seconds, and the
        # renewal threshold sits a third of the lease away from the edge regardless.
        self._deadline = time.monotonic() + self._duration
        # Serialises the background renewer against an inline renewal from a step. Without
        # it both can be in flight at once and the older reply can move the local deadline
        # backwards, making us think we have less lease than the server has given us.
        self._lock = asyncio.Lock()
        self._lost = False

    @property
    def workflow_id(self) -> UUID:
        return self._own.workflow_id

    @property
    def fencing_token(self) -> int:
        return self._own.fencing_token

    @property
    def lost(self) -> bool:
        """True once a renewal proved another worker holds this row."""
        return self._lost

    @property
    def seconds_remaining(self) -> float:
        """Local estimate of the lease left. Negative once it is past due."""
        return self._deadline - time.monotonic()

    @property
    def renew_interval_seconds(self) -> float:
        """How often the background renewer should tick -- ``duration / divisor``."""
        return self._renew_after

    async def renew(self) -> bool:
        """Extend the lease now. Returns False if the workflow is no longer ours.

        The workhorse of the background renewer, which renews on a timer rather than on a
        threshold: it wakes at ``duration / divisor``, when two thirds of the lease is still
        unused, so a conditional renewal would decline every single tick and the timer
        defense would never fire at all.
        """
        if self._lost:
            return False
        async with self._lock:
            if self._lost:
                return False
            held = await workflow_writes.renew_lease(
                self._pool, self._own, duration_seconds=self._duration
            )
            if not held:
                self._lost = True
                return False
            self._deadline = time.monotonic() + self._duration
            return True

    async def renew_if_needed(self) -> bool:
        """Renew only if the lease is running low. Returns False if it is no longer ours.

        Called before each step. A step that starts with a nearly-expired lease will be
        stolen mid-flight and every write it makes afterwards is rejected -- so the work is
        thrown away *after* its side effect has already happened.
        """
        if self._lost:
            return False
        if self.seconds_remaining > self._renew_after:
            return True
        return await self.renew()

    async def renew_or_raise(self) -> None:
        """Renew, raising :class:`PreemptedError` if the workflow has been taken.

        This is what :meth:`sankalp.engine.definition.StepContext.renew_lease` is wired to,
        so a long step that checkpoints its own progress finds out at that checkpoint --
        rather than continuing to work for minutes on a workflow whose result will be
        rejected by the ownership guard.
        """
        if not await self.renew():
            raise PreemptedError(
                f"lease on workflow {self.workflow_id} was lost while a step was running: "
                f"another worker re-claimed it past fencing token {self.fencing_token}"
            )

    def __repr__(self) -> str:
        state = "lost" if self._lost else f"{self.seconds_remaining:.1f}s left"
        return f"<Lease workflow={self.workflow_id} token={self.fencing_token} {state}>"
