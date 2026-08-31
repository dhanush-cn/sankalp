// Control, not a measurement of the system under test: this finds the load generator's own
// maximum sustainable arrival rate on this machine, against an endpoint with no application
// work behind it (GET /openapi.json -- pure route/schema introspection, no DB, no workflow
// execution). Degradation observed in the real scenarios (baseline.js, adaptive-isolated.js,
// as-configured.js) can only be attributed to the system under test if the generator itself is
// known not to be the bottleneck at the rates those scenarios use. If this script can't sustain
// its target rate against a static endpoint, no result at that rate anywhere else in this
// harness is trustworthy.
import http from "k6/http";

const BASE_URL = __ENV.BASE_URL || "http://localhost:8000";
const TARGET_RATE = Number(__ENV.TARGET_RATE || 2500);
const DURATION = __ENV.DURATION || "30s";

// Little's Law (L = lambda * W): VUs needed = arrival rate * per-request latency. A static,
// no-DB endpoint should answer in low single-digit milliseconds, but VUs are still sized
// generously rather than tightly against that assumption -- this script's whole purpose is to
// prove the generator isn't the bottleneck, so under-provisioning VUs here would defeat the
// point in exactly the way it did in baseline.js's first (failed) run.
const WORST_CASE_LATENCY_SECONDS = 1;
const HEADROOM_MULTIPLIER = 3;
const littlesLawVUs = TARGET_RATE * WORST_CASE_LATENCY_SECONDS;

export const options = {
  scenarios: {
    generator_ceiling: {
      executor: "constant-arrival-rate",
      rate: TARGET_RATE,
      timeUnit: "1s",
      duration: DURATION,
      // Modest -- fast startup, not the ceiling. maxVUs carries the ceiling.
      preAllocatedVUs: Math.max(10, Math.ceil(TARGET_RATE * 0.25)),
      maxVUs: Math.ceil(littlesLawVUs * HEADROOM_MULTIPLIER),
    },
  },
  // Same reasoning as baseline.js: count==0 is cumulative over the whole run and would fail
  // permanently on a single transient drop during VU allocation; a real VU-starvation failure
  // drops iterations continuously at a rate comparable to the offered arrival rate, whereas
  // host-level transients produce a handful over minutes -- a rate ceiling of 1 drop/second is
  // what separates the two. delayAbortEval gives allocation a moment before this starts
  // evaluating, so a single scheduling hiccup at t=0 doesn't abort a run that's otherwise fine.
  thresholds: {
    dropped_iterations: [{ threshold: "rate<1", abortOnFail: true, delayAbortEval: "30s" }],
  },
};

export default function () {
  http.get(`${BASE_URL}/openapi.json`);
}
