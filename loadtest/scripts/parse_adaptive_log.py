#!/usr/bin/env python3
"""Parse the API process log into a CSV of the adaptive concurrency limiter's time series.

Usage: parse_adaptive_log.py <api.log> <adaptive_timeseries.csv>
"""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path
from typing import Any

EVENT = "adaptive_concurrency.window_closed"


def _parse_line(line: str) -> dict[str, Any] | None:
    """None for anything that isn't one of our window-closed events.

    api.log is basicConfig-prefixed ("asctime LEVEL name: message") and shares the stream with
    uvicorn/DB noise and every other log line in the process -- finding the first "{" and
    parsing from there is what recovers the JSON regardless of that prefix; skipping (never
    raising) on a JSONDecodeError or a mismatched "event" is what lets an ordinary log line
    coexist in the same file without aborting the parse.
    """
    brace = line.find("{")
    if brace == -1:
        return None
    try:
        parsed = json.loads(line[brace:])
    except json.JSONDecodeError:
        return None
    if not isinstance(parsed, dict) or parsed.get("event") != EVENT:
        return None
    return parsed


def parse_events(log_path: Path) -> list[dict[str, Any]]:
    with log_path.open() as f:
        events = [event for line in f if (event := _parse_line(line)) is not None]
    return events


def write_csv(events: list[dict[str, Any]], out_path: Path) -> None:
    with out_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["ts", "limit", "rtt_avg_s", "rtt_min_s", "gradient"])
        for event in events:
            writer.writerow(
                [event["ts"], event["limit"], event["rtt_avg_s"], event["rtt_min_s"], event["gradient"]]
            )


def main() -> int:
    if len(sys.argv) != 3:
        print(f"usage: {sys.argv[0]} <api.log> <adaptive_timeseries.csv>", file=sys.stderr)
        return 1
    log_path, out_path = Path(sys.argv[1]), Path(sys.argv[2])
    write_csv(parse_events(log_path), out_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
