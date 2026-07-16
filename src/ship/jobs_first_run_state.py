from __future__ import annotations

from pathlib import Path

from src.shared.json_io import existing_json_candidate, read_json

RUNTIME_FEED_ARTIFACTS = (
    "jobs-unified-light.json",
    "jobs-unified.json",
)

ROW_BEARING_JOBS_ARTIFACTS = (
    "jobs-unified-startup.json",
    *RUNTIME_FEED_ARTIFACTS,
)


def _runtime_report_has_success_metadata(report: object) -> bool:
    if not isinstance(report, dict):
        return False
    if not str(report.get("finishedAt") or "").strip():
        return False
    summary = report.get("summary")
    if not isinstance(summary, dict):
        return False
    for status in (report.get("status"), summary.get("status")):
        if str(status or "").strip().lower() in {"error", "failed"}:
            return False
    try:
        output_count = int(summary.get("outputCount") or 0)
    except (TypeError, ValueError):
        output_count = 0
    return output_count > 0


def _path_has_bytes(path: Path) -> bool:
    try:
        return Path(path).stat().st_size > 0
    except OSError:
        return False


def json_feed_artifact_has_rows(path: Path) -> bool:
    payload = read_json(path, {})
    if isinstance(payload, dict):
        rows = payload.get("jobs")
        return isinstance(rows, list) and len(rows) > 0
    if isinstance(payload, list):
        return len(payload) > 0
    return False


def csv_feed_artifact_has_rows(path: Path) -> bool:
    try:
        text = Path(path).read_text(encoding="utf-8")
    except OSError:
        return False
    lines = [
        line.strip()
        for line in text.splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    return len(lines) > 1


def has_loadable_runtime_feed_artifacts(data_dir: str | Path) -> bool:
    data_path = Path(data_dir)
    for name in RUNTIME_FEED_ARTIFACTS:
        path = data_path / name
        if name.endswith(".json"):
            candidate = existing_json_candidate(path)
            if candidate is None or not json_feed_artifact_has_rows(candidate):
                return False
            continue
        if not path.exists() or not csv_feed_artifact_has_rows(path):
            return False
    return True


def has_plausible_runtime_feed_artifacts_for_static_serving(data_dir: str | Path) -> bool:
    data_path = Path(data_dir)
    for name in RUNTIME_FEED_ARTIFACTS:
        path = data_path / name
        if name.endswith(".json"):
            candidate = existing_json_candidate(path)
            if candidate is None or not _path_has_bytes(candidate):
                return False
            continue
        if not _path_has_bytes(path):
            return False
    return True


def has_successful_runtime_jobs_report(data_dir: str | Path) -> bool:
    data_path = Path(data_dir)
    report = read_json(data_path / "jobs-fetch-report.json", {})
    if not _runtime_report_has_success_metadata(report):
        return False
    return has_loadable_runtime_feed_artifacts(data_path)


def has_successful_runtime_jobs_report_for_static_serving(data_dir: str | Path) -> bool:
    data_path = Path(data_dir)
    report = read_json(data_path / "jobs-fetch-report.json", {})
    if not _runtime_report_has_success_metadata(report):
        return False
    return has_plausible_runtime_feed_artifacts_for_static_serving(data_path)


def jobs_cold_start_required(data_dir: str | Path) -> bool:
    return not has_loadable_runtime_feed_artifacts(data_dir)


def jobs_cold_start_required_for_static_serving(data_dir: str | Path) -> bool:
    return not has_successful_runtime_jobs_report_for_static_serving(data_dir)
