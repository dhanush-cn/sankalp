# Phase 4, piece 1 — raw findings

Scratch record of measurements taken while building `loadtest/`. Bullets, not narrative — no
conclusions beyond what's stated here.

## Measured capacity (baseline sweep, both limiters off)

- Steps 200/400/450/500/550 rps, 60s each. Sample counts exact for 200–500 (11980/24001/27001/30001
  against expected 12000/24000/27000/30000).
- p99 by step: 200rps 40.8ms, 400rps 81.5ms, 450rps 48.0ms, 500rps 43.4ms. Flat band. (550rps
  excluded — it aborted on the dropped_iterations threshold, see below.)
- 550 rps aborted at 15.9s on the dropped_iterations threshold; partial n=8139, p99 1114ms.
  Capacity = 500 rps, corner at 550.
- VUs used at 200–500 stayed at preallocated levels; 550 climbed past its allocation within 16s.
- Little's Law: 500 rps × ~7ms ⇒ ~3.5 in-flight at capacity.

## Adaptive limiter, 750 rps (1.4× capacity), min_limit=2

- Limit converges to 4 — 165 of 180 windows at 4, remainder spread 5–14 during initial descent
  from 20.
- Matches the ~3.5 predicted independently from capacity.
- HIGH 27001 admitted, 0 shed (100%). LOW 62192 admitted, 45809 shed (57%). p95 12ms.

## Adaptive limiter, 2500 rps (5× capacity), min_limit=5 (shipped default)

- Pinned at floor: 176 of 180 windows at 5. Descent 20→14→10→8→6→5 within ~6s. Not convergence
  — the floor sets the operating point.
- HIGH admitted 50.5% then 61.5% across two identical runs; LOW 0.06%.
- RTT p95: HIGH 285ms, LOW 789ms.
- Shipped min_limit=5 sits above the measured equilibrium of 4, masking the convergence.

## as-configured, 2500 rps (both limiters on, as shipped)

- Rate limiter shed 113 of ~450,000 requests (0.025%) — nearly inert. Concurrency limiter did
  the shedding.
- Cause: concurrency limiter pinned at floor rejects requests before the token bucket can drain.

## Generator ceiling control

- 2500 rps against `/openapi.json` (bypasses middleware — `_route_class` returns `None`): 74999
  requests, 0 drops, 0 failures, 248 of 625 VUs used. 2500 rps is achievable on this host.

## Anomalies found and dismissed

- 1h15m durations in the first baseline: host suspend. Span 4845s − freezes (3304+1244+67=4615s)
  = 230s real execution, matching the configured sweep. k6's 60s request timeout never fired, so
  k6's own timer wasn't advancing.
- One −1.7ms duration out of 450,001, on a 503. Measurement artifact.
- 200 rps p99 of 144.6ms in the first sweep did not reproduce (40.8ms second run).
- p99 non-monotonic within the flat band (400 rps worst at 81.5ms) — do not narrate the curve's
  shape.
- Run-to-run variance at 5×: HIGH admission 50.5% vs 61.5%. Limit behaviour was deterministic;
  throughput was not.

## Results directories

- `loadtest/results/baseline-sweep-200-650/` — the capacity sweep
- `loadtest/results/adaptive-isolated-750-min2/` — 750 rps, min_limit=2, the converged run
- `loadtest/results/adaptive-isolated-2500-min5/` — 2500 rps, shipped min_limit=5, the saturated
  run
- `loadtest/results/as-configured-2500-min5/` — 2500 rps, both limiters on

`raw_http.jsonl` and `api.log` are gitignored (528MB per run for the raw stream) and
regenerable by re-running; the committed artifacts are `k6-summary.json`, the derived CSVs, and
the plots.
