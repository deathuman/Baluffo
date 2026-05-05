from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

WARN_REGRESSION_PCT = 5.0
FAIL_REGRESSION_PCT = 15.0


def _extract_json_object(text: str) -> dict[str, Any]:
    stripped = str(text or "").strip()
    if not stripped:
        return {}
    try:
        payload = json.loads(stripped)
    except json.JSONDecodeError:
        start = stripped.find("{")
        end = stripped.rfind("}")
        if start < 0 or end < start:
            return {}
        try:
            payload = json.loads(stripped[start : end + 1])
        except json.JSONDecodeError:
            return {}
    return payload if isinstance(payload, dict) else {}


def load_benchmark_payload(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return _extract_json_object(path.read_text(encoding="utf-8"))


def _int_value(value: Any) -> int:
    try:
        return max(0, int(float(value)))
    except (TypeError, ValueError):
        return 0


def benchmark_duration_ms(payload: dict[str, Any], *, mode: str = "") -> int:
    direct = _int_value(payload.get("totalDurationMs"))
    if direct:
        return direct
    runtime = payload.get("runtime")
    if isinstance(runtime, dict):
        runtime_duration = _int_value(runtime.get("totalDurationMs"))
        if runtime_duration:
            return runtime_duration
    if str(mode or "").strip().lower() == "fetch":
        first = payload.get("firstRun")
        second = payload.get("secondRun")
        total = 0
        for run in (first, second):
            if not isinstance(run, dict):
                continue
            run_runtime = run.get("runtime")
            if isinstance(run_runtime, dict):
                total += _int_value(run_runtime.get("totalDurationMs"))
        if total:
            return total
    return 0


def compare_duration(
    *,
    current_duration_ms: int,
    baseline_duration_ms: int,
    warn_pct: float = WARN_REGRESSION_PCT,
    fail_pct: float = FAIL_REGRESSION_PCT,
) -> dict[str, Any]:
    current = _int_value(current_duration_ms)
    baseline = _int_value(baseline_duration_ms)
    if current <= 0:
        return {
            "status": "failed",
            "deltaPct": 0.0,
            "message": "Current benchmark duration is missing or zero.",
        }
    if baseline <= 0:
        return {
            "status": "baseline_missing",
            "deltaPct": 0.0,
            "message": "No usable baseline duration found; benchmark is informational.",
        }
    delta_pct = ((current - baseline) / baseline) * 100.0
    if delta_pct > float(fail_pct):
        status = "failed"
    elif delta_pct > float(warn_pct):
        status = "warn"
    else:
        status = "passed"
    return {
        "status": status,
        "deltaPct": round(delta_pct, 2),
        "message": f"current={current}ms baseline={baseline}ms delta={delta_pct:+.1f}%",
    }


def compare_benchmark_files(
    *,
    current_path: Path,
    baseline_path: Path,
    mode: str,
    warn_pct: float = WARN_REGRESSION_PCT,
    fail_pct: float = FAIL_REGRESSION_PCT,
) -> dict[str, Any]:
    current_payload = load_benchmark_payload(current_path)
    baseline_payload = load_benchmark_payload(baseline_path)
    current_duration = benchmark_duration_ms(current_payload, mode=mode)
    baseline_duration = benchmark_duration_ms(baseline_payload, mode=mode)
    result = compare_duration(
        current_duration_ms=current_duration,
        baseline_duration_ms=baseline_duration,
        warn_pct=warn_pct,
        fail_pct=fail_pct,
    )
    result.update(
        {
            "mode": str(mode or "").strip(),
            "currentPath": str(current_path),
            "baselinePath": str(baseline_path),
            "currentDurationMs": current_duration,
            "baselineDurationMs": baseline_duration,
        }
    )
    return result


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare a benchmark result with a baseline.")
    parser.add_argument("--mode", required=True)
    parser.add_argument("--current", required=True)
    parser.add_argument("--baseline", required=True)
    parser.add_argument("--warn-pct", type=float, default=WARN_REGRESSION_PCT)
    parser.add_argument("--fail-pct", type=float, default=FAIL_REGRESSION_PCT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    result = compare_benchmark_files(
        current_path=Path(str(args.current)),
        baseline_path=Path(str(args.baseline)),
        mode=str(args.mode),
        warn_pct=float(args.warn_pct),
        fail_pct=float(args.fail_pct),
    )
    print(json.dumps(result, indent=2, ensure_ascii=False))
    if result["status"] == "warn":
        print(f"::warning title=Benchmark regression::{result['message']}", file=sys.stderr)
    return 1 if result["status"] == "failed" else 0


if __name__ == "__main__":
    raise SystemExit(main())
