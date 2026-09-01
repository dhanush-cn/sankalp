# Sankalp — Phase 4 Build Log (Load-Test Harness and Measured Capacity)

Everything built in Phase 4, piece 1, with the reasoning an interviewer will probe. The code is
in git; this is the *why*, which only lives here.

**Commits so far (merged to `main`):**
- `03ab0a1` — observability: `api/main.py`'s missing `basicConfig` call, and the
  `adaptive_concurrency.window_closed` JSON log line from inside the real recompute path
- `d090a7f` — the harness itself: `loadtest/k6/*`, `run_scenario.sh`, the two parsers
- `1e43f60` — results and the generator-ceiling control
- `c44c859` — the plots
- plus this commit, the raw-findings and build-log docs

**What this closes:** every performance claim in this repo before Phase 4 was asserted, not
measured. The spec named this as the one honest gap. It is now closed for the load-testing
half — the chaos suite and Toxiproxy fault injection are a separate piece, not attempted here.

---

## 1. Method

Open-loop, `constant-arrival-rate`, everywhere — never `ramping-vus`. `ramping-vus` is
closed-loop: the generator's own iteration rate depends on how fast the server answers, so
exactly when the system is struggling, the generator slows down to match and the worst samples
never get recorded. P99 looks best exactly when the system is worst — coordinated omission, by
name, in `docs/spec.md`'s own Load Testing Methodology. A "ramp" here is composed of several
genuinely open-loop `constant-arrival-rate` scenarios run back to back with staggered
`startTime`s, never a single executor that adapts its own rate.

Three scenarios, each isolating a different thing:

- **`baseline.js`** — both limiters (rate limiter, adaptive concurrency) disabled. Finds the
  system's raw processing capacity: what Postgres and the pool can actually do, with nothing
  in front of it shedding load first.
- **`adaptive-isolated.js`** — rate limiter disabled, adaptive concurrency at its real default
  (on). Isolates the gradient controller's own behavior from the Redis token bucket's.
- **`as-configured.js`** — both at their real, unmodified defaults. Measures the system as
  shipped.

`adaptive-isolated.js` and `as-configured.js` are byte-identical files (confirmed with `diff`
and matching md5sums at the time they were written). The only difference between the two
scenarios is which environment variable `run_scenario.sh` sets on the API process before
starting it — nothing about which scenario a script *is* belongs inside the script itself.

---

## 2. The false start

The first baseline run left the rate limiter at its real default (on). It measured the Redis
token bucket's own admission ceiling — not what Postgres could process. 88.62% of requests were
shed as 429s at the rate tested. "5× measured capacity," computed from that number, would have
meant 5× the size of the token bucket, not 5× real capacity — the wrong multiplier feeding every
downstream overload scenario.

The fix was disabling the rate limiter for baseline. But the same reasoning applies one layer
further in: leaving the adaptive concurrency limiter on during baseline would have measured the
gradient controller's own settled concurrency instead of raw capacity — "5× capacity" would then
mean 5× the limiter's own choice, and the entire point of `adaptive-isolated.js` (comparing the
limiter's behavior *against* a capacity measured independently of it) would be near-null by
construction, proving nothing. Both limiters are disabled for `baseline.js`; `adaptive-isolated`
disables only the rate limiter; `as-configured` disables neither.

---

## 3. Two enforcement mechanisms, both revert-proofed

Both env vars that control this (`SANKALP_RATELIMIT_ENABLED`,
`SANKALP_ADAPTIVE_CONCURRENCY_ENABLED`) fail silently on a typo — `pydantic-settings` just falls
back to the field's default rather than erroring. A mistyped name produces a run that still
executes, still produces plausible-looking numbers, and gives no downstream signal that it
measured the wrong thing. Two mechanisms close that, each proven by deliberately breaking it.

**(1) A startup-configuration assertion.** After the readiness poll succeeds and before k6 runs,
`run_scenario.sh` greps the API's own startup log for the `log.warning(...)` lines
`api/main.py` already emits when a flag is disabled, matched on a stable prefix
(`"rate limiting disabled"` / `"adaptive concurrency disabled"`, not the full parenthesized
string naming the env var — a later reformat of that suffix must not silently turn the check
into an always-pass). For each flag independently, the warning must be present when the scenario
disables it and absent when it does not; both directions are checked, because a scenario that's
supposed to run with a limiter on but silently doesn't is the same failure as the reverse.
Proven live: the export was deliberately typo'd to `SANKALP_ADAPTIVE_CONCURRENCY_ENABLE` (missing
the trailing `D`), and the assertion caught it — the limiter came up enabled, the expected
"disabled" warning was absent from the log, and the script exited before k6 ever ran. Reverted
immediately after confirming.

**(2) A `dropped_iterations` threshold.** A step that can't actually offer its stated arrival
rate — VU-starved, silently gone closed-loop — produces a table that looks fine but describes a
lower, unknown rate. It was first written as `count==0`, which is cumulative over the entire
run: a single transient drop anywhere, including during normal VU allocation at a step's start,
fails the run permanently, and `delayAbortEval` only postpones *when* that failure is noticed,
it doesn't excuse the early drops that already tripped it. Changed to `rate<1` (a rate ceiling
of one dropped iteration per second) — genuine VU starvation drops continuously at a rate
comparable to the offered arrival rate, while host-level transients produce a handful over
minutes; a rate threshold separates the two where a cumulative count can't. With `rate<1` in
place, the threshold caught the real capacity corner: the 550rps step aborted at 15.9s into its
60s window, rather than running to completion on data that no longer meant what the table would
have implied.

---

## 4. Measured capacity

`baseline.js`, both limiters off, configured through 200/400/450/500/550/600/650 rps at 60s
each; the `dropped_iterations` threshold aborted the run partway through the 550rps step, so
600 and 650 never ran:

- Sample counts exact for every step that completed: 11980, 24001, 27001, 30001 against expected
  12000, 24000, 27000, 30000.
- p99 by step: 200rps 40.8ms, 400rps 81.5ms, 450rps 48.0ms, 500rps 43.4ms — a flat band.
- 550rps aborted at 15.9s on the `dropped_iterations` threshold: partial n=8139, p99 1114ms.
- VU usage at 200–500rps stayed at the preallocated level for each step; at 550rps it climbed
  past its allocation within 16 seconds — the VU-starvation signature the threshold exists to
  catch, not a slow ramp.

**Capacity = 500 rps; the corner is at 550.** By Little's Law, 500rps at the flat band's ~7ms
average latency implies roughly 3.5 requests in flight at capacity — a number this build log
comes back to independently in Section 5.

---

## 5. The adaptive limiter

**At 1.4× capacity (750rps), with `min_limit` lowered to 2:** the limiter converges to a limit
of 4 — 165 of 180 one-second windows sat exactly there, the remainder spread across 5–14 during
the initial descent from the default initial limit of 20. That 4 matches, independently, the
~3.5 in-flight figure Little's Law predicted from the capacity measurement in Section 4 alone —
two different measurements arriving at the same number without either being derived from the
other. HIGH-criticality traffic: 27001 admitted, 0 shed (100%). LOW: 62192 admitted, 45809 shed
(57%). p95 12ms.

**At 5× capacity (2500rps), with the shipped default `min_limit=5`:** the limit pins at the
floor — 176 of 180 windows at exactly 5, reached via a fast descent (20→14→10→8→6→5) within
about six seconds. **This is not convergence.** The floor is what sets the operating point here,
not the gradient arithmetic finding an equilibrium — the equilibrium it would otherwise find is
4, one below the floor, and the floor never lets it get there. HIGH admission ran 50.5% in one
run and 61.5% in an identical repeat; LOW admission was 0.06% in both. RTT p95: HIGH 285ms, LOW
789ms.

**The shipped `min_limit=5` sits above the measured equilibrium of 4, and that masks the
mechanism.** Left at its default, this floor makes the limiter look like it's holding a fixed
ceiling under overload rather than actively converging to one — the only reason the real
equilibrium (4) was visible at all was deliberately lowering the floor to 2, specifically to test
whether the gradient controller was doing anything beyond sitting at whatever floor it was
configured with. Whether the shipped default should change is **deferred**, and deliberately so:
this measurement is steady-state sustained overload, and the floor's practical cost is paid
during a *transient* spike, not a sustained one — a floor set too low would leave a brief real
spike underprotected while the gradient is still descending toward it. That's a different
experiment than the one run here, and the decision waits on it rather than being made from
steady-state data alone.

---

## 6. The rate limiter is nearly inert under overload

`as-configured`, both limiters on their real defaults, 2500rps: the rate limiter (the Redis
token bucket) shed 113 requests out of roughly 450,000 — 0.025%. Essentially all of the shedding
under this overload was done by the concurrency limiter, not the rate limiter. The cause is
Section 5's own finding: the concurrency limiter, pinned at its floor, rejects requests with a
503 before those requests ever get far enough to drain the token bucket down to where the bucket
itself would start returning 429s. The two limiters are not shedding independently at this rate;
one is upstream of the other in effect, not just in wiring order.

---

## 7. Threats to validity

Stated at full strength, not softened:

- **Same-machine testing.** The load generator (k6) and the system under test (the API,
  Postgres, Redis) ran on the same host for every measurement above. `docs/spec.md`'s own
  methodology names this as a real caveat — same-box generation can inflate results and steal
  CPU from the thing being measured — and nothing here works around that; it's disclosed, not
  fixed.
- **The generator ceiling control, and what it does and doesn't establish.** A separate control
  script drove 2500rps against `/openapi.json` (bypasses all rate-limiting/concurrency
  middleware — `_route_class` returns `None` for that path) and sustained it cleanly: 74999
  requests, 0 drops, 0 failures, using only 248 of 625 allocated VUs. This establishes that the
  generator itself was not the bottleneck at 2500rps against a trivial endpoint on this host.
  It does **not** establish that the generator had equal headroom while the API was under real
  load from the same host's CPU and network stack simultaneously serving Postgres, Redis, and
  the application — that's exactly what same-machine testing can't rule out, and the control
  doesn't reach it.
- **Run-to-run variance at 5×.** HIGH admission measured 50.5% in one run and 61.5% in an
  identical repeat of the same configuration. The limiter's *behavior* was deterministic across
  both runs (pinned at the floor of 5, same descent shape) — the *throughput* that behavior
  produced was not. A single run at this rate is not a stable number; the qualitative finding
  (floor-pinned, not converging) is what's load-bearing, not either specific percentage.
- **Ambient tail latency.** No attempt was made to isolate or control for host-level noise
  (other processes, container/VM scheduling, thermal throttling) during any run. The anomalies
  below are partly a record of that noise showing up in the data.
- **Every anomaly found, and the reasoning for dismissing each:**
  - *1h15m step durations in the very first baseline run.* Diagnosed as a host suspend/resume,
    not a measurement of anything real: the run's total wall-clock span was 4845s; three
    freezes (3304 + 1244 + 67 = 4615s) subtracted from that leave 230s of real execution —
    matching the configured sweep's actual duration. k6's own 60-second per-request timeout
    never fired during the freezes, which is consistent with k6's internal timer itself not
    advancing while the host was suspended, rather than with any request actually hanging for
    over an hour.
  - *One −1.7ms request duration*, out of 450,001 total samples, attached to a 503 response.
    Treated as a measurement artifact (a negative duration is not physically meaningful), not a
    finding about the system.
  - *A 200rps p99 of 144.6ms in the first sweep* did not reproduce on a second run of the same
    configuration (40.8ms). Treated as noise from that specific run, not a property of 200rps
    load.
  - *p99 is non-monotonic within the flat band* — the 400rps step's p99 (81.5ms) is the worst
    of the four completed steps, not the highest-rate one. This is recorded as an observation,
    not narrated as a trend or a curve shape; four points in a flat, noisy band don't support a
    story about *why* 400 was worse than 450 or 500.

---

## 8. Reproducing

Each scenario is one call to `loadtest/scripts/run_scenario.sh <scenario> [TARGET_RATE]
[DURATION] [HIGH_FRACTION]`. `SANKALP_ADAPTIVE_CONCURRENCY_MIN_LIMIT` (and any other
`SANKALP_*` setting) can be exported in the calling shell first — it reaches the spawned API
process through normal environment inheritance, unlike a k6-side variable (see the known gap
below).

```bash
# Capacity sweep (Section 4)
loadtest/scripts/run_scenario.sh baseline
# -> loadtest/results/baseline/  (renamed to baseline-sweep-200-650/ before the next run)

# Adaptive limiter at 1.4x capacity, floor lowered to see real convergence (Section 5)
SANKALP_ADAPTIVE_CONCURRENCY_MIN_LIMIT=2 \
  loadtest/scripts/run_scenario.sh adaptive-isolated 750
# -> loadtest/results/adaptive-isolated/  (renamed to adaptive-isolated-750-min2/)

# Adaptive limiter at 5x capacity, shipped default floor (Section 5)
loadtest/scripts/run_scenario.sh adaptive-isolated 2500
# -> loadtest/results/adaptive-isolated/  (renamed to adaptive-isolated-2500-min5/)

# As shipped, both limiters on, 5x capacity (Section 6)
loadtest/scripts/run_scenario.sh as-configured 2500
# -> loadtest/results/as-configured/  (renamed to as-configured-2500-min5/)
```

`docs/phase4-raw-findings.md`'s "Results directories" section maps each of these to where its
artifacts actually live.

**Three known gaps, not fixed here:**

- `STEPS` and `SLEEP_SECONDS` are read by the k6 scripts via `__ENV`, which only sees values
  passed as explicit `-e VAR=value` flags to `k6 run` — k6 does not read arbitrary process
  environment variables the way the API's `SANKALP_*` settings do. `run_scenario.sh` only ever
  passes `-e BASE_URL`, `-e TARGET_RATE`, `-e DURATION`, and `-e HIGH_FRACTION` through to k6.
  Setting `STEPS=...` or `SLEEP_SECONDS=...` in the calling shell before invoking the script has
  no effect at all — silently, since k6 just falls back to the script's own default.
- **Results directories are keyed by scenario name alone** (`loadtest/results/<scenario>/`),
  not by rate or configuration, so a second run of the same scenario overwrites the first run's
  artifacts in place. This already cost one run's artifacts during this piece's own measurement
  work, which is why the four directories in Section 4/5/6 above exist under renamed,
  rate-and-floor-qualified names rather than the script's own default output paths — the
  renaming is a manual step after each run, not something `run_scenario.sh` does for you.
- **The chaos suite's DB-latency scenario does not prove reconciliation under fault.** The
  Phase 4 Gate (below) requires the chaos suite to show "the reconciliation query still nets
  to zero" under injected faults. `tests/chaos/test_chaos_db_latency.py` calls
  `chaos.invariants.check_all`, which includes the reconciliation check, but nothing in `src/`
  currently writes `ledger_entries` for the `demo_crash` workflow that scenario submits
  (`grep -rln ledger_entries src/` returns nothing outside migrations) — the query runs
  against an empty table and passes vacuously. The scenario's other four invariants (no stuck
  workflow, outbox drained, no duplicate side effects, no `FAILED_DIRTY`) are genuinely
  exercised; reconciliation is not. Closing this needs a workflow whose steps actually post
  balanced double-entry `ledger_entries` rows under the same fault; once one exists, the
  scenario's existing `check_all` call starts asserting reconciliation for real with no test
  change required.
