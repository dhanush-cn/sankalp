// Finds "measured capacity" for the system as configured. A stepped ramp built from
// *sequential* constant-arrival-rate scenarios, staggered by startTime: k6 has no "ramping"
// variant of the open-loop executor, and ramping-vus is explicitly out -- it's closed-loop (the
// generator's own iteration rate depends on how fast the server responds), which is exactly the
// coordinated omission docs/spec.md's Load Testing Methodology warns against. Every step here is
// its own genuinely open-loop constant-arrival-rate scenario; the "ramp" is just several of them
// run back to back, not a single closed-loop executor changing its own rate.
import { submitWorkflow } from "./lib/submit.js";

const BASE_URL = __ENV.BASE_URL || "http://localhost:8000";
const SLEEP_SECONDS = Number(__ENV.SLEEP_SECONDS || 0.05);
// Re-pointed at the band where capacity actually lies, from a real run's own numbers: the
// original coarse sweep (20/50/100/200/400/800 rps) showed p99 flat at 23ms through the 400rps
// step, then collapsed to ~16.8s at the 800rps step with 5885 dropped_iterations -- the
// executor had silently gone closed-loop and delivered ~604rps while claiming 800. The corner
// is somewhere between 400 and 800; this sweep is fine enough to find it. Override with
// STEPS="..." if a different band is needed without editing the script.
const STEP_RATES = __ENV.STEPS
  ? __ENV.STEPS.split(",").map(Number)
  : [200, 400, 450, 500, 550, 600, 650];
// 60s, not the original 30s: near the corner, a 30s window lets warm-up transients dominate the
// percentile.
const STEP_DURATION_SECONDS = Number(__ENV.STEP_DURATION_SECONDS || 60);
// No gap by default -- steps run back-to-back. (Still overridable via GAP_SECONDS if a cooldown
// between steps is ever wanted.)
const GAP_SECONDS = Number(__ENV.GAP_SECONDS || 0);

// Little's Law (L = lambda * W): VUs needed = arrival rate * per-request latency. The previous
// run's failure (Insufficient VUs, 3200 active VUs at the top step, 5885 dropped_iterations)
// was the executor silently degrading to closed-loop because maxVUs was sized as a guess
// (rate * 4) rather than from latency. Sizing off an assumed worst-case 2s per-request latency,
// then adding headroom on top of that, is what actually bounds this: 800-1300 VUs across this
// band from Little's Law alone, doubled again for headroom.
const WORST_CASE_LATENCY_SECONDS = 2;
const HEADROOM_MULTIPLIER = 2;

function scenarioFor(rate, startTimeSeconds) {
  const littlesLawVUs = rate * WORST_CASE_LATENCY_SECONDS;
  return {
    executor: "constant-arrival-rate",
    rate,
    timeUnit: "1s",
    duration: `${STEP_DURATION_SECONDS}s`,
    startTime: `${startTimeSeconds}s`,
    // Modest -- fast startup, not the ceiling. maxVUs carries the ceiling.
    preAllocatedVUs: Math.max(10, Math.ceil(rate * 0.25)),
    maxVUs: Math.ceil(littlesLawVUs * HEADROOM_MULTIPLIER),
    exec: "sendLow",
  };
}

function buildScenarios() {
  const scenarios = {};
  let startTimeSeconds = 0;
  for (const rate of STEP_RATES) {
    scenarios[`step_${rate}rps`] = scenarioFor(rate, startTimeSeconds);
    startTimeSeconds += STEP_DURATION_SECONDS + GAP_SECONDS;
  }
  return scenarios;
}

export const options = {
  scenarios: buildScenarios(),
  // A step that can't actually offer its stated arrival rate (VU-starved, silently gone
  // closed-loop) produces a table that looks fine but describes a lower, unknown rate -- the
  // same enforcement principle as run_scenario.sh's startup-configuration assertion: refuse to
  // report a run that didn't measure what it claims to, rather than warn and continue.
  // count==0 is cumulative over the entire run, so a single transient drop anywhere --
  // including during VU allocation at a step's start -- fails it permanently; delayAbortEval
  // only postpones when that failure is noticed, it doesn't excuse early drops. A real
  // VU-starvation failure drops iterations continuously at a rate comparable to the offered
  // arrival rate (the earlier 800rps run dropped 5885 over 30s, ~196/s), whereas host-level
  // transients produce a handful over minutes. A rate ceiling of 1 drop/second separates the
  // two. delayAbortEval still gives allocation a moment before this starts evaluating, so a
  // single scheduling hiccup at t=0 doesn't abort a run that's otherwise fine.
  thresholds: {
    dropped_iterations: [{ threshold: "rate<1", abortOnFail: true, delayAbortEval: "30s" }],
  },
};

export function sendLow() {
  submitWorkflow(BASE_URL, SLEEP_SECONDS, "low");
}
