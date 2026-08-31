#!/usr/bin/env python3
"""Plot the adaptive concurrency limit against RTT p99 per criticality, for one scenario.

Two panels sharing a wall-clock x-axis: the limit (a step function of the recompute path) on
top, RTT p99 split by criticality on the bottom -- one figure shows both "limit drops as
latency climbs" and "admitted HIGH p99 stays bounded while LOW sheds" at once.

Requires matplotlib (loadtest/requirements.txt) -- not a project dependency, install with
`pip install -r loadtest/requirements.txt` before running this script.

Usage: plot_timeseries.py <adaptive_timeseries.csv> <rtt_timeseries.csv> <timeseries.png>
"""

from __future__ import annotations

import csv
import sys
from datetime import datetime
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # no display available where this runs -- write straight to a file
import matplotlib.pyplot as plt


def _read_adaptive(path: Path) -> tuple[list[datetime], list[int]]:
    ts: list[datetime] = []
    limit: list[int] = []
    with path.open(newline="") as f:
        for row in csv.DictReader(f):
            ts.append(datetime.fromisoformat(row["ts"]))
            limit.append(int(row["limit"]))
    return ts, limit


def _read_rtt(path: Path) -> dict[str, tuple[list[datetime], list[float]]]:
    """criticality -> (timestamps, p99_s), each series sorted by timestamp."""
    series: dict[str, list[tuple[datetime, float]]] = {}
    with path.open(newline="") as f:
        for row in csv.DictReader(f):
            criticality = row["criticality"]
            ts = datetime.fromisoformat(row["window_start_ts"])
            p99 = float(row["p99_s"])
            series.setdefault(criticality, []).append((ts, p99))
    return {
        criticality: (
            [ts for ts, _ in sorted(points)],
            [p99 for _, p99 in sorted(points)],
        )
        for criticality, points in series.items()
    }


def plot(adaptive_csv: Path, rtt_csv: Path, out_png: Path, scenario: str) -> None:
    limit_ts, limit = _read_adaptive(adaptive_csv)
    rtt_by_criticality = _read_rtt(rtt_csv)

    fig, (ax_limit, ax_rtt) = plt.subplots(2, 1, sharex=True, figsize=(10, 6))

    ax_limit.step(limit_ts, limit, where="post", color="tab:blue")
    ax_limit.set_ylabel("adaptive concurrency limit")
    ax_limit.set_title(f"{scenario}: concurrency limit vs. admitted-RTT p99")

    for criticality, (rtt_ts, p99) in sorted(rtt_by_criticality.items()):
        ax_rtt.plot(rtt_ts, p99, marker=".", label=f"{criticality} p99")
    ax_rtt.set_ylabel("RTT p99 (s)")
    ax_rtt.set_xlabel("time (UTC)")
    ax_rtt.legend()

    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(out_png, dpi=150)


def main() -> int:
    if len(sys.argv) != 4:
        print(
            f"usage: {sys.argv[0]} <adaptive_timeseries.csv> <rtt_timeseries.csv> <timeseries.png>",
            file=sys.stderr,
        )
        return 1
    adaptive_csv, rtt_csv, out_png = (Path(a) for a in sys.argv[1:4])
    plot(adaptive_csv, rtt_csv, out_png, scenario=out_png.parent.name)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
