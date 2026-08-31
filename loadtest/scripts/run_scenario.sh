#!/usr/bin/env bash
# Start the API with the scenario's env, wait for it to be ready, run the matching k6 script,
# then tear the API down -- writing everything into loadtest/results/<scenario>/.
#
# Usage: run_scenario.sh <baseline|adaptive-isolated|as-configured> [TARGET_RATE] [DURATION] [HIGH_FRACTION]
#   TARGET_RATE is required for adaptive-isolated/as-configured (5x baseline's measured
#   capacity) and ignored for baseline, which drives its own fixed step sweep.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BASE_URL="http://localhost:8000"

usage() {
  echo "usage: $0 <baseline|adaptive-isolated|as-configured> [TARGET_RATE] [DURATION] [HIGH_FRACTION]" >&2
  exit 1
}

SCENARIO="${1:-}"
TARGET_RATE="${2:-}"
DURATION="${3:-180}"
HIGH_FRACTION="${4:-0.2}"

case "$SCENARIO" in
  baseline | adaptive-isolated | as-configured) ;;
  *) usage ;;
esac

if [ "$SCENARIO" != "baseline" ] && [ -z "$TARGET_RATE" ]; then
  echo "error: TARGET_RATE is required for $SCENARIO (5x baseline's measured capacity)" >&2
  usage
fi

RESULTS_DIR="$REPO_ROOT/loadtest/results/$SCENARIO"
mkdir -p "$RESULTS_DIR"
API_LOG="$RESULTS_DIR/api.log"

# Each scenario's env must match what its label claims to measure:
#   baseline           -- rate limiter disabled, adaptive concurrency disabled
#   adaptive-isolated  -- rate limiter disabled, adaptive concurrency at its real default (on)
#   as-configured      -- both at their real defaults (on)
# baseline exists to find the system's raw processing capacity -- the number scenario (b)/(c)
# multiply by 5 for TARGET_RATE. With the rate limiter on, baseline would instead measure the
# Redis token bucket's own admission ceiling (confirmed: ~50/s, ~88% shed as 429 at the rate
# tested), not what Postgres/the pool can actually handle. With the adaptive limiter ALSO on,
# baseline would instead measure the gradient controller's own settled concurrency, so "5x
# capacity" would mean 5x the limiter's own choice, and the adaptive-vs-baseline comparison
# scenario (b) exists to make would be near-null by construction, proving nothing. as-configured
# is the one scenario that deliberately keeps both at their real defaults, because it's the one
# measuring the system as shipped -- everything else about the env here is still unmodified, so
# the API starts exactly as `make api` would on its own, on sankalp_app via
# active_app_database_url's default (no SANKALP_ENVIRONMENT override).
if [ "$SCENARIO" = "baseline" ] || [ "$SCENARIO" = "adaptive-isolated" ]; then
  export SANKALP_RATELIMIT_ENABLED=false
fi
if [ "$SCENARIO" = "baseline" ]; then
  export SANKALP_ADAPTIVE_CONCURRENCY_ENABLED=false
fi

# Resolves the real uvicorn PID(s) via pgrep, never bash's $! -- `make api`'s --reload spawns a
# supervisor plus a worker subprocess, and in practice these can end up in DIFFERENT process
# groups (confirmed directly: one probe run saw pgid(supervisor) != pgid(worker), when a plain
# $! after `setsid make api &` only ever points at the wrapper shell's own PID). Every PGID any
# matching PID belongs to gets SIGTERMed, and teardown waits for pgrep to actually go quiet
# before returning -- a stale API surviving into the next scenario would contaminate its
# numbers, silently.
teardown() {
  local pids pgid pgids_seen=""
  pids="$(pgrep -f "uvicorn sankalp.api.main:app" || true)"
  [ -z "$pids" ] && return 0

  for pid in $pids; do
    # `|| true`: the PID can vanish between pgrep and ps at any moment during teardown -- that's
    # the normal case here (things are actively dying), not an exceptional one, and under
    # `set -e` an unguarded failed substitution would abort this loop mid-way, leaving later
    # PGIDs un-killed.
    pgid="$(ps -o pgid= -p "$pid" 2>/dev/null | tr -d ' ')" || true
    [ -z "$pgid" ] && continue
    case " $pgids_seen " in
      *" $pgid "*) continue ;;
    esac
    pgids_seen="$pgids_seen $pgid"
    kill -TERM -- "-$pgid" 2>/dev/null || true
  done

  for _ in $(seq 1 30); do
    pgrep -f "uvicorn sankalp.api.main:app" >/dev/null 2>&1 || return 0
    sleep 1
  done

  # Still alive after 30s of SIGTERM -- escalate rather than leave it to bleed into the next run.
  for pid in $(pgrep -f "uvicorn sankalp.api.main:app" || true); do
    pgid="$(ps -o pgid= -p "$pid" 2>/dev/null | tr -d ' ')" || true
    [ -n "$pgid" ] && kill -KILL -- "-$pgid" 2>/dev/null || true
  done
}
trap teardown EXIT

echo "starting API (scenario=$SCENARIO) -> $API_LOG"
# PYTHON is passed explicitly rather than relying on the caller's shell having the venv active
# -- the Makefile's own default (PYTHON ?= python) resolves against whatever's on PATH in this
# spawned context, and a missing/wrong `python` there fails `make api` in a way that just looks
# like a slow readiness timeout, not a clear "python not found".
setsid make -C "$REPO_ROOT" PYTHON="$REPO_ROOT/.venv/bin/python" api >"$API_LOG" 2>&1 </dev/null &

# GET /openapi.json is pure route/schema introspection -- touches no DB pool, needs no auth --
# so readiness here proves the ASGI app is serving, not that migrations or Redis are healthy.
echo "waiting for API readiness at $BASE_URL/openapi.json ..."
ready=0
for _ in $(seq 1 30); do
  code="$(curl -s -o /dev/null -w '%{http_code}' "$BASE_URL/openapi.json" || true)"
  if [ "$code" = "200" ]; then
    ready=1
    break
  fi
  sleep 1
done
if [ "$ready" != "1" ]; then
  echo "error: API did not become ready within 30s -- see $API_LOG" >&2
  exit 1
fi
echo "API ready."

# Both env vars fail silently on a typo -- pydantic-settings just falls back to the field's
# default rather than erroring -- so a mistyped SANKALP_RATELIMIT_ENABLED or
# SANKALP_ADAPTIVE_CONCURRENCY_ENABLED produces a run that still executes and still produces
# plausible-looking numbers, with nothing downstream to reveal it measured the wrong thing.
# Checking the API's own startup log -- the same log.warning(...) calls api/main.py emits at
# lines 93/116 when a flag is off -- catches that before k6 ever runs, rather than trusting the
# export above actually took effect. Matched on the stable "<thing> disabled" prefix, not the
# full parenthesized string including the env var name -- reformatting that suffix later must
# not silently turn this into an always-pass check.
RATELIMIT_DISABLED_PREFIX="rate limiting disabled"
ADAPTIVE_DISABLED_PREFIX="adaptive concurrency disabled"

# trap teardown EXIT (above, before the API is even started) is already armed by this point, so
# a failed assertion below still tears down the API via `exit 1`.
check_flag() {
  local label="$1" prefix="$2" expect_disabled="$3" present=0
  if grep -qF "$prefix" "$API_LOG"; then
    present=1
  fi
  if [ "$expect_disabled" = "1" ] && [ "$present" != "1" ]; then
    echo "error: scenario $SCENARIO expects $label DISABLED, but its startup warning is absent from $API_LOG" >&2
    exit 1
  fi
  if [ "$expect_disabled" != "1" ] && [ "$present" = "1" ]; then
    echo "error: scenario $SCENARIO expects $label at its real default (enabled), but its startup warning IS present in $API_LOG" >&2
    exit 1
  fi
}

case "$SCENARIO" in
  baseline)
    check_flag "the rate limiter" "$RATELIMIT_DISABLED_PREFIX" 1
    check_flag "adaptive concurrency" "$ADAPTIVE_DISABLED_PREFIX" 1
    ;;
  adaptive-isolated)
    check_flag "the rate limiter" "$RATELIMIT_DISABLED_PREFIX" 1
    check_flag "adaptive concurrency" "$ADAPTIVE_DISABLED_PREFIX" 0
    ;;
  as-configured)
    check_flag "the rate limiter" "$RATELIMIT_DISABLED_PREFIX" 0
    check_flag "adaptive concurrency" "$ADAPTIVE_DISABLED_PREFIX" 0
    ;;
esac
echo "startup configuration verified for scenario $SCENARIO."

k6_args=(-e "BASE_URL=$BASE_URL")
case "$SCENARIO" in
  baseline)
    k6_script="$REPO_ROOT/loadtest/k6/baseline.js"
    ;;
  adaptive-isolated | as-configured)
    k6_script="$REPO_ROOT/loadtest/k6/$SCENARIO.js"
    k6_args+=(-e "TARGET_RATE=$TARGET_RATE" -e "DURATION=$DURATION" -e "HIGH_FRACTION=$HIGH_FRACTION")
    ;;
esac

echo "running k6: $k6_script"
k6 run \
  "${k6_args[@]}" \
  --out "json=$RESULTS_DIR/raw_http.jsonl" \
  --summary-export="$RESULTS_DIR/k6-summary.json" \
  "$k6_script"

echo "scenario $SCENARIO complete -> $RESULTS_DIR"
