#!/usr/bin/env python3
"""Print per-scenario-step latency percentiles from a k6 raw JSON dump.

Usage: per_step_summary.py [raw_http.jsonl]
  Defaults to loadtest/results/baseline/raw_http.jsonl if no path is given.
"""

from __future__ import annotations

import json
import math
import sys
from pathlib import Path

METRIC_NAME = "http_req_duration"
DEFAULT_PATH = "loadtest/results/baseline/raw_http.jsonl"


def load_samples(path: Path) -> dict[str, list[float]]:
    """scenario tag -> list of http_req_duration samples (ms), in file order."""
    groups: dict[str, list[float]] = {}
    printed_tag_keys = False
    with path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            point = json.loads(line)

            # k6's --out json interleaves one "Metric" schema-definition line per metric name
            # (no data.value) alongside the real "Point" samples -- skipping this is not
            # optional, it's already caused a KeyError twice in this project.
            if point.get("type") != "Point":
                continue
            # "metric" is a TOP-LEVEL key on the point, not nested under "data" -- confirmed
            # against a real k6 v2.2.0 sample earlier in this project. data.get("metric") would
            # always be None and silently filter out every point.
            if point.get("metric") != METRIC_NAME:
                continue

            data = point["data"]
            tags = data.get("tags", {})
            if not printed_tag_keys:
                print(f"tag keys on first matching point: {sorted(tags.keys())}")
                printed_tag_keys = True

            scenario = tags.get("scenario", "NO_SCENARIO_TAG")
            groups.setdefault(scenario, []).append(data["value"])
    return groups


def _numeric_key(name: str) -> tuple[float, str]:
    """Sort by the first run of digits in the group name; groups with none sort last."""
    digits = ""
    for ch in name:
        if ch.isdigit():
            digits += ch
        elif digits:
            break
    return (float(digits) if digits else math.inf, name)


def _percentile_by_index(sorted_samples: list[float], p: float) -> float:
    """Nearest-rank percentile: an actual sample, never an average or interpolation."""
    n = len(sorted_samples)
    rank = max(1, min(n, math.ceil(p / 100 * n)))
    return sorted_samples[rank - 1]


def print_table(groups: dict[str, list[float]]) -> None:
    names = sorted(groups, key=_numeric_key)
    print(f"groups found: {names}")
    print()
    header = f"{'group':<20} {'n':>6} {'p50':>8} {'p95':>8} {'p99':>8} {'max':>8}"
    print(header)
    for name in names:
        samples = sorted(groups[name])
        p50 = _percentile_by_index(samples, 50)
        p95 = _percentile_by_index(samples, 95)
        p99 = _percentile_by_index(samples, 99)
        row_max = samples[-1]
        print(
            f"{name:<20} {len(samples):>6} {p50:>8.1f} {p95:>8.1f} {p99:>8.1f} {row_max:>8.1f}"
        )


def main() -> int:
    raw_path = Path(sys.argv[1] if len(sys.argv) > 1 else DEFAULT_PATH)
    try:
        groups = load_samples(raw_path)
    except FileNotFoundError:
        print(f"error: no such file: {raw_path}", file=sys.stderr)
        return 1

    if not groups:
        print(f"no {METRIC_NAME} Point samples found in {raw_path}", file=sys.stderr)
        return 1

    print_table(groups)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
