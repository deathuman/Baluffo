#!/usr/bin/env python3
"""Run repeated default vs parallel bridge startup probes and summarize the result."""

from __future__ import annotations

import argparse
import json
import os
import sys
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.perf_complete import DEFAULT_OUTPUT_ROOT, generate_run_token
from scripts.run_startup_probe_pair import run_startup_probe_pair

PARALLEL_BRIDGE_ENV = "BALUFFO_STARTUP_PARALLEL_BRIDGE"


@contextmanager
def _startup_mode_env(parallel: bool):
    previous = os.environ.get(PARALLEL_BRIDGE_ENV)
    if parallel:
        os.environ[PARALLEL_BRIDGE_ENV] = "1"
    else:
        os.environ.pop(PARALLEL_BRIDGE_ENV, None)
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop(PARALLEL_BRIDGE_ENV, None)
        else:
            os.environ[PARALLEL_BRIDGE_ENV] = previous


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _median(values: list[int]) -> int:
    ordered = sorted(int(value) for value in values if int(value or 0) >= 0)
    if not ordered:
        return 0
    return ordered[len(ordered) // 2]


def _report_duration_ms(path: Path) -> int:
    report = _read_json(path)
    profile = report.get("startupProfile") if isinstance(report.get("startupProfile"), dict) else {}
    return int(profile.get("firstUsableMs") or 0)


def _summarize_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    cold = [int(row.get("coldDurationMs") or 0) for row in rows if row.get("ok")]
    warm = [int(row.get("warmDurationMs") or 0) for row in rows if row.get("ok")]
    return {
        "sampleCount": len(rows),
        "passedCount": sum(1 for row in rows if row.get("ok")),
        "coldMedianMs": _median(cold),
        "warmMedianMs": _median(warm),
        "coldDurationsMs": cold,
        "warmDurationsMs": warm,
    }


def _decision(default_summary: dict[str, Any], parallel_summary: dict[str, Any]) -> dict[str, Any]:
    default_samples = int(default_summary.get("sampleCount") or 0)
    parallel_samples = int(parallel_summary.get("sampleCount") or 0)
    default_passed = int(default_summary.get("passedCount") or 0)
    parallel_passed = int(parallel_summary.get("passedCount") or 0)
    default_cold = int(default_summary.get("coldMedianMs") or 0)
    default_warm = int(default_summary.get("warmMedianMs") or 0)
    parallel_cold = int(parallel_summary.get("coldMedianMs") or 0)
    parallel_warm = int(parallel_summary.get("warmMedianMs") or 0)
    cold_delta = default_cold - parallel_cold
    warm_delta = default_warm - parallel_warm
    samples_complete = (
        default_samples > 0
        and parallel_samples > 0
        and default_passed == default_samples
        and parallel_passed == parallel_samples
    )
    durations_complete = min(default_cold, default_warm, parallel_cold, parallel_warm) > 0
    promote = (
        samples_complete
        and durations_complete
        and cold_delta >= 250
        and warm_delta >= 250
        and parallel_cold <= default_cold
    )
    if promote:
        reason = "parallel improved cold and warm medians by at least 250ms"
    elif not samples_complete:
        reason = "startup probe failures or missing samples keep parallel startup diagnostic-only"
    elif not durations_complete:
        reason = "incomplete startup durations keep parallel startup diagnostic-only"
    else:
        reason = "parallel bridge startup remains diagnostic-only"
    return {
        "promoteParallelDefault": promote,
        "coldImprovementMs": cold_delta,
        "warmImprovementMs": warm_delta,
        "reason": reason,
    }


def run_bridge_ab(
    *,
    output_dir: Path,
    pairs: int,
    runtime_timeout_s: float,
    pages: list[str],
) -> tuple[dict[str, Any], int]:
    output_dir.mkdir(parents=True, exist_ok=True)
    exit_code = 0
    page_reports: dict[str, Any] = {}
    for page in pages:
        open_path = f"{page}.html" if not page.endswith(".html") else page
        page_key = Path(open_path).stem
        page_report: dict[str, Any] = {"default": [], "parallel": []}
        for mode, parallel in (("default", False), ("parallel", True)):
            for index in range(1, max(1, int(pairs)) + 1):
                artifact_root = output_dir / page_key / mode / f"pair-{index}"
                summary_path = artifact_root / "pair-summary.json"
                with _startup_mode_env(parallel):
                    result = run_startup_probe_pair(
                        runtime_timeout_s=runtime_timeout_s,
                        artifact_root=artifact_root,
                        summary_path=summary_path,
                        open_path=open_path,
                        profile_record_only=True,
                    )
                pair_summary = _read_json(summary_path)
                cold_path = Path(str(pair_summary.get("coldReportPath") or ""))
                warm_path = Path(str(pair_summary.get("warmReportPath") or ""))
                ok = int(result or 0) == 0 and bool(pair_summary.get("ok"))
                page_report[mode].append(
                    {
                        "ok": ok,
                        "pair": index,
                        "summaryPath": str(summary_path),
                        "coldReportPath": str(cold_path),
                        "warmReportPath": str(warm_path),
                        "coldDurationMs": _report_duration_ms(cold_path) if ok else 0,
                        "warmDurationMs": _report_duration_ms(warm_path) if ok else 0,
                    }
                )
                if int(result or 0) != 0:
                    exit_code = int(result or 1)
        default_summary = _summarize_rows(page_report["default"])
        parallel_summary = _summarize_rows(page_report["parallel"])
        page_reports[page_key] = {
            **page_report,
            "summary": {
                "default": default_summary,
                "parallel": parallel_summary,
                "decision": _decision(default_summary, parallel_summary),
            },
        }
    report = {
        "ok": exit_code == 0,
        "generatedAt": datetime.now(UTC).isoformat(),
        "outputDir": str(output_dir),
        "pairs": max(1, int(pairs)),
        "runtimeTimeoutS": float(runtime_timeout_s),
        "pages": page_reports,
    }
    report_path = output_dir / "startup-bridge-ab-summary.json"
    report["reportPath"] = str(report_path)
    report_path.write_text(
        json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    return report, exit_code


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run repeated default vs BALUFFO_STARTUP_PARALLEL_BRIDGE startup probes."
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_ROOT / "startup-bridge-ab" / generate_run_token()),
    )
    parser.add_argument("--pairs", type=int, default=5)
    parser.add_argument("--runtime-timeout", type=float, default=60.0)
    parser.add_argument("--pages", default="jobs,admin")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    pages = [part.strip() for part in str(args.pages or "").split(",") if part.strip()]
    report, exit_code = run_bridge_ab(
        output_dir=Path(str(args.output_dir)).expanduser().resolve(),
        pairs=max(1, int(args.pairs or 1)),
        runtime_timeout_s=float(args.runtime_timeout),
        pages=pages or ["jobs", "admin"],
    )
    print(json.dumps(report, indent=2, ensure_ascii=False), flush=True)
    return int(exit_code or 0)


if __name__ == "__main__":
    raise SystemExit(main())
