from __future__ import annotations

import argparse
import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

DEFAULT_BASELINE_DIR = Path("_out/perf-baseline")
DEFAULT_TREND_PATH = Path("_out/perf-trend.ndjson")


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _current_commit_sha() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError:
        return ""
    return result.stdout.strip() if result.returncode == 0 else ""


def _parse_non_negative_int(value: Any, *, field: str) -> int:
    try:
        parsed = int(float(value))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a non-negative integer") from exc
    if parsed < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    return parsed


def build_baseline_record(
    *,
    mode: str,
    total_duration_ms: int,
    status: str = "pass",
    source_count: int | None = None,
    adapter_count: int | None = None,
    wall_clock_ms: int | None = None,
    stage_durations_ms: dict[str, Any] | None = None,
    artifact: str = "",
    commit_sha: str = "",
    timestamp: str = "",
) -> dict[str, Any]:
    normalized_mode = str(mode or "").strip()
    if not normalized_mode:
        raise ValueError("mode is required")
    record: dict[str, Any] = {
        "ts": str(timestamp or _utc_now_iso()),
        "mode": normalized_mode,
        "totalDurationMs": _parse_non_negative_int(total_duration_ms, field="totalDurationMs"),
        "status": str(status or "pass").strip() or "pass",
        "commitSha": str(commit_sha or _current_commit_sha()).strip(),
    }
    if source_count is not None:
        record["sourceCount"] = _parse_non_negative_int(source_count, field="sourceCount")
    if adapter_count is not None:
        record["adapterCount"] = _parse_non_negative_int(adapter_count, field="adapterCount")
    if wall_clock_ms is not None:
        record["wallClockMs"] = _parse_non_negative_int(wall_clock_ms, field="wallClockMs")
    if stage_durations_ms is not None:
        record["stageDurationsMs"] = {
            str(key): _parse_non_negative_int(value, field=f"stageDurationsMs.{key}")
            for key, value in stage_durations_ms.items()
        }
    artifact_text = str(artifact or "").strip()
    if artifact_text:
        record["artifact"] = artifact_text
    return record


def baseline_path_for_mode(mode: str, *, baseline_dir: Path = DEFAULT_BASELINE_DIR) -> Path:
    safe_mode = str(mode or "").strip().replace("\\", "-").replace("/", "-")
    safe_mode = safe_mode or "baseline"
    return baseline_dir / f"{safe_mode}-baseline.json"


def parse_stage_durations_json(value: str) -> dict[str, Any] | None:
    text = str(value or "").strip()
    if not text:
        return None
    payload = json.loads(text)
    if not isinstance(payload, dict):
        raise ValueError("--stage-durations-json must be a JSON object")
    return payload


def write_baseline_record(
    record: dict[str, Any],
    *,
    baseline_dir: Path = DEFAULT_BASELINE_DIR,
    trend_path: Path = DEFAULT_TREND_PATH,
) -> Path:
    baseline_dir.mkdir(parents=True, exist_ok=True)
    trend_path.parent.mkdir(parents=True, exist_ok=True)
    path = baseline_path_for_mode(str(record.get("mode") or ""), baseline_dir=baseline_dir)
    path.write_text(json.dumps(record, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    with trend_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")
    return path


def append_trend_record(
    record: dict[str, Any],
    *,
    trend_path: Path = DEFAULT_TREND_PATH,
) -> Path:
    trend_path.parent.mkdir(parents=True, exist_ok=True)
    with trend_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")
    return trend_path


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Record a Baluffo benchmark baseline row.")
    parser.add_argument("--mode", required=True, help="Benchmark mode, e.g. discovery or fetch.")
    parser.add_argument("--total-duration-ms", required=True, type=int)
    parser.add_argument("--status", default="pass")
    parser.add_argument("--source-count", type=int, default=None)
    parser.add_argument("--adapter-count", type=int, default=None)
    parser.add_argument("--wall-clock-ms", type=int, default=None)
    parser.add_argument("--stage-durations-json", default="")
    parser.add_argument("--artifact", default="")
    parser.add_argument("--baseline-dir", default=str(DEFAULT_BASELINE_DIR))
    parser.add_argument("--trend-path", default=str(DEFAULT_TREND_PATH))
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    record = build_baseline_record(
        mode=str(args.mode),
        total_duration_ms=int(args.total_duration_ms),
        status=str(args.status),
        source_count=args.source_count,
        adapter_count=args.adapter_count,
        wall_clock_ms=args.wall_clock_ms,
        stage_durations_ms=parse_stage_durations_json(str(args.stage_durations_json)),
        artifact=str(args.artifact),
    )
    path = write_baseline_record(
        record,
        baseline_dir=Path(str(args.baseline_dir)),
        trend_path=Path(str(args.trend_path)),
    )
    print(f"Recorded {record['mode']} baseline: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
