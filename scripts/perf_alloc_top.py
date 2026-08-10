#!/usr/bin/env python3
"""Aggregate the per-source alloc profile JSONL emitted by ``run_profiled_alloc``.

Usage:
    python scripts/perf_alloc_top.py [--limit N] [--since ISO]

Reads ``<data_dir>/perf-profiles/allocations.jsonl`` and prints the top frames
(by cumulative allocation across all sources), plus a per-source bucket summary.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any


def _data_dir() -> Path:
    return Path(os.environ.get("BALUFFO_DATA_DIR") or "_out")


def _load(path: Path, since: str | None) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue
        # The log doesn't carry a birth timestamp; rely on dict ordering as-is.
        rows.append(entry)
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=20, help="how many top frames to print")
    parser.add_argument("--sources", type=int, default=10, help="how many top sources to print")
    args = parser.parse_args()

    rows = _load(_data_dir() / "perf-profiles" / "allocations.jsonl", since=None)
    if not rows:
        print(f"no entries in {_data_dir() / 'perf-profiles' / 'allocations.jsonl'}")
        return 0

    total_by_frame: dict[str, float] = defaultdict(float)
    count_by_frame: dict[str, int] = defaultdict(int)
    source_peaks: dict[str, float] = {}
    source_calls: dict[str, int] = defaultdict(int)

    for entry in rows:
        source = str(entry.get("source") or "")
        peak_mib = float(entry.get("peak_mib") or 0.0)
        if source:
            source_calls[source] += 1
            if peak_mib > source_peaks.get(source, 0.0):
                source_peaks[source] = peak_mib
        for frame_row in entry.get("top_frames") or []:
            frame = str(frame_row.get("frame") or "")
            size_mib = float(frame_row.get("size_mib") or 0.0)
            total_by_frame[frame] += size_mib
            count_by_frame[frame] += 1

    print(f"{len(rows)} profiled invocations across {len(source_peaks)} sources")
    print()
    print("Top alloc frames (cumulative MiB across all sources):")
    ranked = sorted(total_by_frame.items(), key=lambda kv: kv[1], reverse=True)[: args.limit]
    for frame, total_mib in ranked:
        print(f"  {total_mib:>9.1f} MiB  ({count_by_frame[frame]}x)  {frame}")

    print()
    print(f"Top {args.sources} sources by peak-mib:")
    ranked_sources = sorted(source_peaks.items(), key=lambda kv: kv[1], reverse=True)[
        : args.sources
    ]
    for source, peak_mib in ranked_sources:
        print(f"  {peak_mib:>9.1f} MiB  ({source_calls[source]}x)  {source}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
