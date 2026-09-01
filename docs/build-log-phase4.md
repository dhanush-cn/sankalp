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

**Two known gaps, not fixed here:**

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

**Closed:** the chaos suite's DB-latency scenario not proving reconciliation under fault
(formerly the third gap here) is closed by commit `085327c` — see Section 9.

---

## 9. Reconciliation under fault, for real: `demo_transfer`

Closes the gap Section 8 used to list third: `tests/chaos/test_chaos_db_latency.py` called
`chaos.invariants.check_all`, including the reconciliation check, but nothing in `src/` wrote
`ledger_entries` — the query ran against an empty table and passed vacuously. Commit `085327c`
adds `demo_transfer` (`src/sankalp/workflows/transfer.py`), a workflow that actually posts to
the ledger, so that check has something to fail against.

**Two forward steps, in separate transactions, on purpose.** `post_debit` and `post_credit`
each commit their own `ledger_entries` row independently — the same double entry posted in one
step, one transaction, would make a transiently unbalanced ledger unobservable, since either
both rows commit or neither does. Reconciliation would then have nothing to ever catch, which
is exactly the vacuity this workflow exists to remove. Splitting them means a crash (or an
injected fault) between the two leaves exactly one leg posted, which reconciliation must — and
does — see as unbalanced until the second leg lands or the first is reversed.

**Idempotent by construction — the opposite design choice from `demo_crash` and
`demo_unwind`.** Both steps write with `ON CONFLICT ... DO NOTHING` against the pre-existing
`uq_ledger_entry` constraint (`workflow_id, step_name, account_id, direction` —
`migrations/003_saga.sql`), so a replayed step posts nothing extra. `demo_crash` and
`demo_unwind` are deliberately the reverse: their crash gates count raw attempts, and an
idempotent write there would make the gate pass whether or not crash recovery actually worked.
`demo_transfer` must never be cited as evidence of exactly-once *effects* — it proves nothing
about execution counts, only about reconciliation. Compensation follows the same append-only
discipline as the rest of the ledger: a reversal is a new, opposite-direction entry, never an
`UPDATE` or `DELETE`.

**Three revert-proofs, each breaking a different mechanism and failing at the point of
divergence:**

- Deleting the `ON CONFLICT ... DO NOTHING` clause: the first invocation of a step still
  succeeds; a second, real invocation of the same step against the same workflow now raises
  `UniqueViolationError` on `uq_ledger_entry` directly, instead of being silently absorbed.
- Generating `transfer_id` from a fresh `uuid4()` per step instead of using `ctx.workflow_id`:
  `RECONCILE` returned two unbalanced groups for what should have been one balanced transfer —
  `debits=500_00, credits=0` and `debits=0, credits=500_00` — because the debit and credit,
  now under different `transfer_id`s, are read as two independent one-sided transfers instead
  of one balanced pair.
- Changing `reverse_debit` to post `direction="DEBIT"` instead of `"CREDIT"`: the compensation
  still ran and the workflow still reached `COMPENSATED` (the row's
  `(workflow_id, step_name, account_id, direction)` key is distinct from the forward row's, so
  no constraint catches it), but it doubled the
  debit instead of reversing it — caught by the test's explicit assertion on the reversal's
  `direction`, not by reconciliation, since that particular test never reaches the `RECONCILE`
  check.

Each was reverted immediately after confirming the failure. `tests/test_transfer.py`, 5 tests,
all passing; full suite `203 passed, 1 deselected` (the chaos scenario itself, which needs the
Toxiproxy container and is excluded from `make test` by design).

**A separate finding, not part of the gap this section closes.** `transfer.py` opens its own
module-level pool from `settings.active_app_database_url` (`sankalp.storage.pool.create_pool`)
rather than reusing `workflows/_instrumentation.py`'s `get_pool()`. Two reasons, not one.
First, `ledger_entries` is a business table, not instrumentation — `sankalp_app` holds
`SELECT`/`INSERT` on it (`migrations/004_restricted_role.sql`), unlike
`side_effects`/`step_attempts`/`crash_gates`, which have no `sankalp_app` grants at all — so it
should be written through the restricted role the worker actually executes on, not the owning
role `_instrumentation.get_pool()` deliberately uses for the crash-gate tables. Second, and the
reason that actually matters for the chaos suite: `active_app_database_url` is exactly what
`tests/fleet.py`'s `WorkerFleet.launch` overrides via
`extra_env={"SANKALP_TEST_APP_DATABASE_URL": ...}` to route a worker through the Toxiproxy
Postgres proxy. `_instrumentation.get_pool()`'s `active_database_url` is never overridden by a
chaos test — reusing it would have sent `demo_transfer`'s ledger writes over a direct,
unproxied connection, and reconciliation would have reported green against a database the
injected fault never touched. The general hazard this leaves behind: a module-level pool
resolved from `settings` follows `SANKALP_ENVIRONMENT`, not the engine's actual connection —
worth checking explicitly the next time any workflow module opens a pool of its own.

---

## 10. The outbox invariant had the same defect as reconciliation

`check_outbox_drained` (`tests/chaos/invariants.py`) was passing in chaos scenario 1 for the same
reason `check_reconciliation` used to (Section 9, before `demo_transfer`): nothing under
`src/sankalp/workflows/` ever called `ctx.emit`, so no production workflow had ever written an
`outbox` row, and the invariant ran against an empty table. `grep -rn "\.emit(" src tests` found
every call site: `tests/test_outbox.py`'s seven throwaway definitions, and nothing else — the
mechanism had no production caller. `sankalp_test.outbox` was confirmed empty after a full
`make test` run, before this fix.

**The distinction that matters here.** The transactional outbox mechanism itself is not
under-tested — `tests/test_outbox.py` already covers forward steps, retries, compensations, and a
payload-serialisation failure. What was missing was any *production* workflow exercising it,
which is what made the chaos-suite invariant vacuous. A well-tested mechanism and a wired-up one
are different claims, and this repo had the first without the second.

**Fix: `post_credit` now emits `transfer.posted`.** After its ledger `INSERT` succeeds,
`post_credit` (`src/sankalp/workflows/transfer.py`) calls `ctx.emit` with `transfer_id` (`str(
ctx.workflow_id)`), `source_account`, `destination_account`, `amount_minor`, and `currency` —
plain JSON, matching what `engine/definition.py` persists to `outbox.payload` (`jsonb`). Only the
credit leg emits: the event announces a completed double entry, and a debit alone is not a
transfer, so `post_debit` stays silent. The compensation path emits nothing either — whether a
reversal should announce its own event is a separate decision, not made here.

**Revert-proof.** Deleting the `ctx.emit` call from `post_credit` leaves everything else
(docstring, the now-unused local variables) untouched, and fails
`test_post_credit_emits_exactly_one_transfer_posted_event` at the row count — `assert 0 == 1` —
with the other five tests in the file unaffected, since none of them depend on the emitted event.
Reverted immediately after confirming.

`tests/test_transfer.py` is now 6 tests, all passing; full suite `204 passed, 1 deselected`.

**The pattern worth carrying forward.** Two of the five chaos invariants — reconciliation and now
the outbox drain — turned out to be passing against empty tables. Both were found the same way:
by asking what in `src/` actually writes the table each invariant reads, not by reading the
invariant's own query. An invariant that has never been seen to fail is not yet a check; the
other three chaos invariants haven't had this question asked of them yet.

---

## 11. Chaos scenario 1 now alternates `demo_crash` and `demo_transfer`

Sections 9 and 10 gave reconciliation and the outbox drain something real to check, but
`tests/chaos/test_chaos_db_latency.py` was still submitting only `demo_crash` under the fault.
Both fixes were dead weight until the scenario actually submitted the workflow type that
exercises them.

**Why both, not a straight swap.** `demo_crash` is the sole writer of `side_effects` in the
repo — `workflows/_instrumentation.py` holds the only `INSERT INTO side_effects` — so it alone
feeds `check_no_duplicate_side_effects`. Replacing it outright with `demo_transfer` would have
closed two vacuities and opened a third. `demo_transfer` feeds `check_reconciliation` and
`check_outbox_drained`. Alternating via `itertools.cycle` over a two-element tuple
(`SUBMISSION_BODIES`) makes each batch of four concurrent submissions exactly 2/2, rather than
leaving the mix to chance.

**Evidence the alternation actually ran**, queried against `sankalp_test` after a green run: 300
`demo_crash` workflows in `SUCCESS`, 300 `demo_transfer` workflows in `SUCCESS`, 600
`ledger_entries` rows (two per transfer), 300 outbox rows all published. This matters because
`_one_submission` only records an id on a 200/201 response — a rejected `demo_transfer`
submission (a bad payload, a 503 from the limiter landing disproportionately on one type) would
have been silently dropped from `submitted`, and the scenario would have gone green with only
`demo_crash` having actually run, leaving reconciliation vacuous inside the very change meant to
fix it.

**Revert-proof.** Bypassing the latency toxic (replacing the `async with latency(...):` block
with a bare `await asyncio.sleep(TOXIC_SECONDS)`) makes the fault-landed guard fail: median RTT
during the toxic window came back at 0.016s against the 0.25s threshold — a 15x margin. It fails
on that guard specifically, not on the limit-shrink assertions above it, confirming those would
otherwise have compared two numbers that never moved rather than catching the fault's absence.
Reverted immediately after confirming.

**No re-derivation needed for the limit-shrink assertion.** `baseline_ref` is
`statistics.median` over the baseline windows, and both the "shrinks" and "shrinks by roughly
half" thresholds in that assertion are relative to it — a changed load profile (two workflow
types instead of one, different request latencies) moves `baseline_ref` and `during_min`
together, so the assertion needed no adjustment for the alternation.

**Note for future debugging.** Chaos tests read `ledger_entries` and `outbox` through
`owning_connection` directly, not through the truncating fixtures `tests/conftest.py` gives
`pytest`-only tests — so both tables accumulate across chaos runs rather than starting empty
each time. A reconciliation failure investigated later could involve rows left over from an
earlier run, not just the run that failed.
