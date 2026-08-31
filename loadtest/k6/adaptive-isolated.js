// Two concurrent constant-arrival-rate streams (LOW, HIGH) driven to overload. This file is
// intentionally byte-identical to its sibling scenario script (adaptive-isolated.js /
// as-configured.js) -- the only difference between the two scenarios is which env var
// run_scenario.sh sets on the API process before starting it (SANKALP_RATELIMIT_ENABLED=false
// for adaptive-isolated, unset -- real defaults -- for as-configured). Nothing about which
// scenario this is belongs in the script itself.
import { Counter, Trend } from "k6/metrics";
import { submitWorkflow } from "./lib/submit.js";

// A bare number ("15") is k6's own duration shorthand for MILLISECONDS, not seconds -- k6
// rejects "15" outright ("duration must be at least 1s, but is 15ms"), confirmed by a real run.
// TARGET_RATE and HIGH_FRACTION are bare numbers by convention here; DURATION should be able to
// be one too without silently meaning something else, so a digits-only value gets "s" appended.
function normalizeDurationSeconds(raw) {
  return /^\d+$/.test(raw) ? `${raw}s` : raw;
}

const BASE_URL = __ENV.BASE_URL || "http://localhost:8000";
const SLEEP_SECONDS = Number(__ENV.SLEEP_SECONDS || 0.05);
const DURATION = normalizeDurationSeconds(__ENV.DURATION || "180s");
// No default: must be supplied as 5x whatever baseline.js found. A silent default here would
// let this scenario quietly stop being an overload test.
const TARGET_RATE = Number(__ENV.TARGET_RATE);
if (!TARGET_RATE || TARGET_RATE <= 0) {
  throw new Error("TARGET_RATE must be set (5x baseline.js's measured capacity), e.g. -e TARGET_RATE=400");
}
// A minority claims HIGH by default -- realistic (most callers don't), and it leaves the LOW
// stream enough weight to make its higher shed rate obvious.
const HIGH_FRACTION = Number(__ENV.HIGH_FRACTION || 0.2);

const highRate = Math.max(1, Math.round(TARGET_RATE * HIGH_FRACTION));
const lowRate = Math.max(1, TARGET_RATE - highRate);

function vusFor(rate) {
  return { preAllocatedVUs: Math.max(10, rate), maxVUs: Math.max(20, rate * 4) };
}

export const options = {
  scenarios: {
    overload_low: {
      executor: "constant-arrival-rate",
      rate: lowRate,
      timeUnit: "1s",
      duration: DURATION,
      exec: "sendLow",
      ...vusFor(lowRate),
    },
    overload_high: {
      executor: "constant-arrival-rate",
      rate: highRate,
      timeUnit: "1s",
      duration: DURATION,
      exec: "sendHigh",
      ...vusFor(highRate),
    },
  },
};

// Six custom metrics, named explicitly rather than relying on k6's tag-grouped summary output,
// so the LOW-sheds-before-HIGH and admitted-P99-bounded claims read straight off
// k6-summary.json. 503 is AdaptiveConcurrencyMiddleware's own rejection (the concurrency
// limiter shedding); 429 is RateLimitMiddleware's (which has no criticality concept at all and
// treats both streams identically) -- kept as separate counters so a rate-limiter rejection is
// never mistaken for the concurrency limiter's own shedding behavior.
export const lowAdmitted = new Counter("low_admitted");
export const lowShedConcurrency = new Counter("low_shed_concurrency");
export const lowShedRatelimit = new Counter("low_shed_ratelimit");
export const highAdmitted = new Counter("high_admitted");
export const highShedConcurrency = new Counter("high_shed_concurrency");
export const highShedRatelimit = new Counter("high_shed_ratelimit");
export const lowRttAdmitted = new Trend("low_rtt_admitted");
export const highRttAdmitted = new Trend("high_rtt_admitted");
// Anything that isn't 2xx/429/503 (a stray 500, an unexpected 4xx, a res.status of 0 from a
// connection failure) falls through to here. Without this bucket such a response would be
// silently dropped from every counter -- invisible rather than miscounted -- which is worse
// under overload, exactly when something unexpected is most likely to happen.
export const lowOther = new Counter("low_other");
export const highOther = new Counter("high_other");

// Confirmed against a real k6 v2.2.0 run: res.status is a number here (k6's http.Response),
// but the raw --out json export tags it as a STRING ("201", "429", "503") -- String(res.status)
// keeps this function's own comparisons consistent with how parse_rtt_timeseries.py reads the
// same status values back out of the raw JSON dump.
function record(res, admitted, shedConcurrency, shedRatelimit, rttAdmitted, other) {
  const status = String(res.status);
  if (status.startsWith("2")) {
    admitted.add(1);
    rttAdmitted.add(res.timings.duration);
  } else if (status === "503") {
    shedConcurrency.add(1);
  } else if (status === "429") {
    shedRatelimit.add(1);
  } else {
    other.add(1);
  }
}

export function sendLow() {
  const res = submitWorkflow(BASE_URL, SLEEP_SECONDS, "low");
  record(res, lowAdmitted, lowShedConcurrency, lowShedRatelimit, lowRttAdmitted, lowOther);
}

export function sendHigh() {
  const res = submitWorkflow(BASE_URL, SLEEP_SECONDS, "high");
  record(res, highAdmitted, highShedConcurrency, highShedRatelimit, highRttAdmitted, highOther);
}
