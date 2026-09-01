"""Chaos scenario 1: DB latency +500ms (docs/spec.md, chaos table).

    | DB latency +500ms | Toxiproxy | concurrency limit shrinks, no cascade |

Two claims to prove: the API's adaptive concurrency limiter shrinks its admission limit while
Postgres is slow, and the system otherwise survives the fault cleanly -- no stuck workflow, no
duplicated side effect, the outbox drains.

Why this needs a real API process, not just a worker fleet. The adaptive limiter
(src/sankalp/resilience/adaptive.py) is wired ONLY into FastAPI's ASGI middleware stack
(src/sankalp/api/main.py, src/sankalp/api/middleware.py) -- it times the route handler and
calls ``record_rtt`` after each request completes. ``src/sankalp/engine/worker.py`` never
constructs one. A worker fleet alone, however hard it is leaned on, emits zero
``adaptive_concurrency.window_closed`` events. So this scenario runs both: an
:class:`~chaos.conftest.ApiProcess` under continuous HTTP load (whose windows this test reads),
and a :class:`~fleet.WorkerFleet` that actually executes the submitted workflows (whose
quiescence this test's invariant check depends on). Both point their DB traffic through the
same Toxiproxy Postgres proxy, so one ``latency`` toxic degrades both at once -- the way a real
`+500ms` on the database would.

Why ``demo_crash`` *and* ``demo_transfer``, and not the default workflow_type. ``payment_transfer``
(the default in
tests/conftest.py's ``insert_workflow``) is registered ad-hoc, inside pytest processes only
(see tests/test_executor.py) -- a real worker subprocess importing only
``sankalp.workflows`` cannot resolve it. ``demo_crash`` is the one production-registered type
built for exactly this (src/sankalp/workflows/demo.py), submitted here with
``{"mode": "sleep", "sleep_seconds": ...}`` -- the default ``mode: "gate"`` would block forever,
since ``WorkerFleet``'s env arms the crash gate (tests/fleet.py). But ``demo_crash`` alone only
feeds four of the five invariants. It writes ``side_effects`` (via
``workflows/_instrumentation.py``, the only ``INSERT INTO side_effects`` in the repo), so it is
the sole data source for ``check_no_duplicate_side_effects`` -- but it never writes
``ledger_entries`` or emits ``transfer.posted``, so a run submitting only ``demo_crash`` would
make ``check_reconciliation`` and ``check_outbox_drained`` pass vacuously, against an empty
table and an empty outbox. ``demo_transfer`` is the other production-registered type
(src/sankalp/workflows/transfer.py) and is what actually posts ledger entries and emits
``transfer.posted``. Submitting both is what makes all five invariants load-bearing in one run.

This scenario proves reconciliation under fault, not just in the steady state: transfers posted
while the database was running +500ms slow still net to zero once the fault clears and every
workflow reaches a terminal status. That gap closed in commit 085327c (demo_transfer itself) and
the follow-up that made it emit ``transfer.posted`` -- this scenario is what exercises that path
under a real latency fault rather than in isolation.
"""

from __future__ import annotations

import asyncio
import itertools
import statistics
import uuid
from contextlib import suppress
from datetime import UTC, datetime
from typing import Any

import httpx
from fleet import WorkerFleet, wait_for

from chaos.conftest import ApiProcess, ChaosProxies, latency, parse_window_closed
from chaos.invariants import check_all, owning_connection

#: Warm-up before the fault: long enough for several windows to close at a healthy limit
#: (adaptive_concurrency_window_seconds defaults to 1.0s -- config.py) so "baseline" means
#: something.
BASELINE_SECONDS = 6.0

#: The fault's duration. Long enough for several 1s windows to close with the limiter pinned
#: at its gradient floor (adaptive.py: gradient clamped to [0.5, 1.0]), short enough to keep
#: the scenario fast.
TOXIC_SECONDS = 12.0

#: After the toxic is removed, before submission stops -- lets the limiter's recovery show up
#: in the log too, though this scenario does not assert on it.
RECOVERY_SECONDS = 6.0

#: Ceiling on waiting for every submitted workflow to reach a terminal status once load stops.
QUIESCENCE_TIMEOUT_SECONDS = 120.0

#: The two production-registered workflow types this scenario alternates between, each feeding
#: invariants the other can't -- see the module docstring's "Why demo_crash and demo_transfer"
#: paragraph.
SUBMISSION_BODIES: tuple[dict[str, Any], ...] = (
    {
        "workflow_type": "demo_crash",
        "input": {"mode": "sleep", "sleep_seconds": 0.2},
    },
    {
        "workflow_type": "demo_transfer",
        "input": {
            "source_account": "acct:chaos-source",
            "destination_account": "acct:chaos-destination",
            "amount_minor": 250_00,
            "currency": "INR",
        },
    },
)


async def _submit_loop(
    base_url: str, stop: asyncio.Event, submitted: list[str]
) -> None:
    """Post workflows continuously until ``stop`` is set.

    Runs a handful of requests concurrently rather than one at a time, so the limiter sees
    real concurrent admission pressure -- a single in-flight request at a time never contends
    for a permit and the limit would have nothing to shrink against.

    Alternates between ``SUBMISSION_BODIES`` so each batch of four concurrent submissions is
    half ``demo_crash``, half ``demo_transfer``.
    """

    async def _one_submission(client: httpx.AsyncClient, body: dict[str, Any]) -> None:
        headers = {"Idempotency-Key": f"chaos-db-latency-{uuid.uuid4().hex}"}
        try:
            response = await client.post(f"{base_url}/workflows", json=body, headers=headers)
        except httpx.HTTPError:
            # A connection error mid-toxic (e.g. the API's own pool briefly starved) is
            # expected noise during the fault, not a scenario failure -- the invariant checks
            # after quiescence are what actually judge correctness.
            return
        if response.status_code in (200, 201):
            submitted.append(response.json()["id"])
        # 503 (shed by the limiter) is the mechanism working, not a failure -- deliberately not
        # recorded as an error.

    workflow_cycle = itertools.cycle(SUBMISSION_BODIES)

    async with httpx.AsyncClient(timeout=10.0) as client:
        while not stop.is_set():
            await asyncio.gather(
                *(_one_submission(client, next(workflow_cycle)) for _ in range(4))
            )
            await asyncio.sleep(0.05)


def _events_between(
    events: list[dict[str, Any]], start: datetime | None, end: datetime | None
) -> list[dict[str, Any]]:
    def _in_range(event: dict[str, Any]) -> bool:
        ts = datetime.fromisoformat(event["ts"])
        if start is not None and ts < start:
            return False
        return not (end is not None and ts > end)

    return [event for event in events if _in_range(event)]


async def test_db_latency_shrinks_the_limit_without_a_cascade(
    chaos_proxies: ChaosProxies, api_process: ApiProcess, workers: WorkerFleet
) -> None:
    workers.launch(
        extra_env={
            "SANKALP_TEST_APP_DATABASE_URL": chaos_proxies.postgres_app_dsn,
            # fleet.py's WorkerFleet._env defaults this to "0" so worker-fleet tests aren't
            # raced by a drain they didn't ask for -- this scenario asserts the outbox drains,
            # so it needs one running, and turning it on here is simpler than spawning a
            # fourth process just to run `python -m sankalp.engine.drain`.
            "SANKALP_OUTBOX_DRAIN_IN_WORKER": "1",
        }
    )

    stop = asyncio.Event()
    submitted: list[str] = []
    load_task = asyncio.create_task(_submit_loop(api_process.base_url, stop, submitted))

    try:
        await asyncio.sleep(BASELINE_SECONDS)

        toxic_start = datetime.now(UTC)
        async with latency(chaos_proxies.client, chaos_proxies.postgres_name, ms=500):
            await asyncio.sleep(TOXIC_SECONDS)
        toxic_end = datetime.now(UTC)

        await asyncio.sleep(RECOVERY_SECONDS)
    finally:
        stop.set()
        with suppress(asyncio.CancelledError):
            await asyncio.wait_for(load_task, timeout=10.0)

    assert submitted, f"no workflow was ever accepted by the API.{api_process.diagnostics()}"

    # --- The limit-shrink assertion -----------------------------------------------------
    events = parse_window_closed(api_process.log_path)
    baseline_events = _events_between(events, None, toxic_start)
    during_events = _events_between(events, toxic_start, toxic_end)

    assert len(baseline_events) >= 3, (
        f"only {len(baseline_events)} adaptive_concurrency.window_closed event(s) before the "
        f"toxic -- need enough baseline windows to have a reference limit."
        f"{api_process.diagnostics()}"
    )
    assert len(during_events) >= 3, (
        f"only {len(during_events)} adaptive_concurrency.window_closed event(s) during the "
        f"toxic window -- the load generator may have stalled rather than the limiter "
        f"reacting.{api_process.diagnostics()}"
    )

    # The healthy limit CLIMBS from adaptive_concurrency_initial_limit (20, config.py) toward
    # its max, so min(baseline) is just the starting value -- a constant a sick API would
    # report too. The median of the last baseline window is the honest "what was normal"
    # reference; comparing against the whole baseline's minimum would only prove "during < 20",
    # which is not what "shrinks" claims.
    baseline_ref = statistics.median(event["limit"] for event in baseline_events)
    during_min = min(event["limit"] for event in during_events)

    assert during_min < baseline_ref, (
        f"limit during the toxic (min={during_min}) did not drop below the baseline "
        f"reference (median={baseline_ref}).{api_process.diagnostics()}"
    )
    assert during_min <= max(5, baseline_ref // 2), (
        f"limit during the toxic (min={during_min}) dropped, but not by much against the "
        f"baseline reference (median={baseline_ref}) -- expected roughly a halving or "
        f"more.{api_process.diagnostics()}"
    )

    # Fault-landed guard: without this, a toxic that silently did nothing (a typo'd proxy
    # name, a toxic the control API rejected) would pass the assertions above vacuously, since
    # a limit that never moved would just be compared against a baseline that also never moved.
    median_rtt_during = statistics.median(event["rtt_avg_s"] for event in during_events)
    assert median_rtt_during >= 0.25, (
        f"median RTT during the toxic window was only {median_rtt_during:.3f}s -- the +500ms "
        f"latency toxic does not appear to have actually landed on the traffic this API "
        f"served.{api_process.diagnostics()}"
    )

    # --- The no-cascade assertion -------------------------------------------------------
    # One connection reused across the whole poll, not opened fresh each tick: fleet.wait_for
    # polls every POLL_SECONDS (20ms), and up to QUIESCENCE_TIMEOUT_SECONDS of that would mean
    # thousands of asyncpg.connect() calls for a question one held connection can answer.
    async with owning_connection() as db:

        async def _quiescent() -> bool:
            count = await db.fetchval(
                "SELECT count(*) FROM workflows "
                "WHERE status IN ('PENDING', 'RUNNING', 'COMPENSATING')"
            )
            return count == 0

        await wait_for(
            "every submitted workflow to reach a terminal status",
            _quiescent,
            workers,
            QUIESCENCE_TIMEOUT_SECONDS,
        )

        await check_all(db)
