#!/usr/bin/env python3
"""Parse a k6 --out json dump into per-window RTT percentile CSVs, split by criticality.

Usage: parse_rtt_timeseries.py <raw_http.jsonl> <rtt_timeseries.csv>
"""

from __future__ import annotations

import csv
import json
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

METRIC_NAME = "http_req_duration"


def _window_start(ts: datetime) -> datetime:
    """Floor to the enclosing 1-second window."""
    return ts.replace(microsecond=0)


def parse_samples(raw_path: Path) -> dict[datetime, dict[str, list[float]]]:
    """window_start -> criticality -> RTT samples in that window, in seconds."""
    windows: dict[datetime, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    with raw_path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            point = json.loads(line)

            # k6's --out json emits one "Metric" schema-definition line per metric name
            # (no data.value, no data.tags) alongside the real "Point" samples -- confirmed
            # against a real k6 v2.2.0 run. Skipping this is not optional: treating it as a
            # sample would poison every window with a phantom 0ms, untagged data point.
            if point.get("type") != "Point":
                continue
            if point.get("metric") != METRIC_NAME:
                continue

            data = point["data"]
            ts = datetime.fromisoformat(data["time"]).astimezone(timezone.utc)
            window = _window_start(ts)

            # Confirmed against a real run: k6 tags "status" as a string ("201", "429",
            # "503"), never an int. We don't compare on status here, but "criticality" is
            # read from the same tags dict, so the string-not-int lesson is recorded once,
            # here, for whoever adds a status-based split later.
            criticality = data["tags"].get("criticality", "unknown")
            rtt_seconds = data["value"] / 1000.0  # k6 reports http_req_duration in milliseconds
            windows[window][criticality].append(rtt_seconds)
    return windows


def _percentiles(samples: list[float]) -> tuple[float, float, float]:
    """p50/p95/p99 from this window's own raw samples only -- a legitimate per-window exact
    statistic. Averaging percentiles *across* windows is what docs/spec.md's methodology
    forbids; computing one window's percentile from its own data is not that.

    ``samples`` is never empty: the only caller, write_csv, iterates windows[window].items(),
    and every such entry exists only because parse_samples appended to it in the same
    statement that created it -- there is no way to reach an empty list here.
    """
    if len(samples) == 1:
        return samples[0], samples[0], samples[0]
    q = statistics.quantiles(sorted(samples), n=100, method="inclusive")
    return q[49], q[94], q[98]


def write_csv(windows: dict[datetime, dict[str, list[float]]], out_path: Path) -> None:
    with out_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["window_start_ts", "criticality", "count", "p50_s", "p95_s", "p99_s"])
        for window in sorted(windows):
            for criticality, samples in sorted(windows[window].items()):
                p50, p95, p99 = _percentiles(samples)
                writer.writerow([window.isoformat(), criticality, len(samples), p50, p95, p99])


def main() -> int:
    if len(sys.argv) != 3:
        print(f"usage: {sys.argv[0]} <raw_http.jsonl> <rtt_timeseries.csv>", file=sys.stderr)
        return 1
    raw_path, out_path = Path(sys.argv[1]), Path(sys.argv[2])
    write_csv(parse_samples(raw_path), out_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
