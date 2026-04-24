#!/usr/bin/env python3
"""Run an isolated discovery sanity benchmark under _out/."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from pathlib import Path
from typing import Any


def _ensure_repo_on_path() -> Path:
    root = Path(__file__).resolve().parents[1]
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    return root


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run an isolated discovery sanity benchmark.")
    parser.add_argument(
        "--output-dir",
        default="_out/perf-sanity-discovery",
        help="Isolated data root for benchmark artifacts.",
    )
    parser.add_argument("--timeout", type=int, default=12)
    parser.add_argument("--top", type=int, default=20)
    parser.add_argument("--mode", choices=("dynamic", "static"), default="dynamic")
    parser.add_argument(
        "--include-web-search",
        action="store_true",
        help="Enable web search for this benchmark run.",
    )
    return parser.parse_args(argv)


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def main(argv: list[str] | None = None) -> int:
    root = _ensure_repo_on_path()
    args = parse_args(argv)
    from src.baluffo_config import get_storage_defaults

    data_dir = Path(args.output_dir)
    if not data_dir.is_absolute():
        data_dir = (root / data_dir).resolve()
    data_dir.mkdir(parents=True, exist_ok=True)
    live_data_dir = Path(get_storage_defaults()["data_dir"])
    for name in ("jobs-source-state.json", "source-discovery-config.json"):
        source_path = live_data_dir / name
        target_path = data_dir / name
        if source_path.exists() and not target_path.exists():
            shutil.copy2(source_path, target_path)
    os.environ["BALUFFO_DATA_DIR"] = str(data_dir)

    from src.source_discovery.orchestrator import run_discovery

    report = run_discovery(
        timeout_s=int(args.timeout),
        top_n=int(args.top),
        mode=str(args.mode),
        include_web_search=bool(args.include_web_search),
    )
    summary = _as_dict(report.get("summary"))
    runtime = _as_dict(report.get("runtime"))
    outputs = _as_dict(report.get("outputs"))
    payload = {
        "outputDir": str(data_dir),
        "reportPath": str(outputs.get("report")),
        "queuedCandidateCount": int(summary.get("queuedCandidateCount") or 0),
        "discoverableButDeferredCount": int(summary.get("discoverableButDeferredCount") or 0),
        "failedProbeCount": int(summary.get("failedProbeCount") or 0),
        "queuedByAdapter": dict(summary.get("queuedByAdapter") or {}),
        "deferredByAdapter": dict(summary.get("deferredByAdapter") or {}),
        "healthyButDeferredByAdapter": dict(summary.get("healthyButDeferredByAdapter") or {}),
        "suppressedStaticCount": int(summary.get("suppressedStaticCount") or 0),
        "suppressedStaticByReason": dict(summary.get("suppressedStaticByReason") or {}),
        "suppressedStaticByStage": dict(summary.get("suppressedStaticByStage") or {}),
        "queuedProviderCount": int(summary.get("queuedProviderCount") or 0),
        "queuedStaticCount": int(summary.get("queuedStaticCount") or 0),
        "deferredReasons": dict(summary.get("deferredReasons") or {}),
        "totalDurationMs": int(runtime.get("totalDurationMs") or 0),
    }
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
