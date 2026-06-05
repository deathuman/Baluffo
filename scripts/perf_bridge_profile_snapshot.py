#!/usr/bin/env python3
"""Capture a bounded read-only bridge performance sample."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.perf_complete import (
    DEFAULT_OUTPUT_ROOT,
    capture_live_bridge_profile,
    generate_run_token,
    parse_endpoint_sequence,
    parse_timeout_sequence,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Capture read-only bridge route/profile evidence from a running Baluffo bridge."
    )
    parser.add_argument("--bridge-base-url", required=True)
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_ROOT / "live" / generate_run_token()),
    )
    parser.add_argument("--timeout", type=float, default=3.0)
    parser.add_argument(
        "--timeouts",
        default="",
        help="Comma-separated timeout seconds, for example 3,10,30. Overrides --timeout.",
    )
    parser.add_argument(
        "--burst-rounds",
        type=int,
        default=0,
        help="Optional concurrent read-only burst rounds for Ops route contention sampling.",
    )
    parser.add_argument(
        "--burst-concurrency",
        type=int,
        default=4,
        help="Concurrent workers for optional burst sampling.",
    )
    parser.add_argument(
        "--burst-endpoints",
        default="",
        help="Comma-separated endpoints for optional burst sampling.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    output_dir = Path(str(args.output_dir)).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    summary = capture_live_bridge_profile(
        bridge_base_url=str(args.bridge_base_url),
        output_dir=output_dir,
        timeout_s=float(args.timeout),
        timeout_sequence=parse_timeout_sequence(str(args.timeouts), fallback=float(args.timeout))
        if str(args.timeouts or "").strip()
        else None,
        burst_rounds=int(args.burst_rounds or 0),
        burst_concurrency=int(args.burst_concurrency or 1),
        burst_endpoints=parse_endpoint_sequence(str(args.burst_endpoints or "")),
    )
    print(json.dumps(summary, indent=2, ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
