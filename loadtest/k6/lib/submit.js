// Shared submit helper for every scenario. No external/network imports -- only k6's built-in
// k6/http, so `k6 run` needs no network access beyond hitting the API itself.
import http from "k6/http";

export function idempotencyKey() {
  return `${Date.now()}-${__VU}-${__ITER}-${Math.random().toString(36).slice(2)}`;
}

// criticality is tagged on every request (not just "high") -- confirmed against a real k6
// v2.2.0 --out json run that data.tags.criticality survives onto http_req_duration points,
// and parse_rtt_timeseries.py's LOW/HIGH split depends on that tag being present on every point,
// not only the ones claiming HIGH.
export function submitWorkflow(baseUrl, sleepSeconds, criticality) {
  const body = JSON.stringify({
    workflow_type: "demo_crash",
    input: { mode: "sleep", sleep_seconds: sleepSeconds },
  });
  const headers = { "Content-Type": "application/json", "Idempotency-Key": idempotencyKey() };
  if (criticality === "high") headers["Criticality"] = "high";
  return http.post(`${baseUrl}/workflows`, body, {
    headers,
    tags: { criticality },
  });
}
