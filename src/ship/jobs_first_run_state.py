from __future__ import annotations

import gzip
import time
from pathlib import Path
from typing import Any

from src.shared.fetch_report_identity import fetch_report_identity
from src.shared.json_io import existing_json_candidate, read_json

FETCH_REPORT_NAME = "jobs-fetch-report.json"
FETCH_REPORT_SUMMARY_NAME = "jobs-fetch-report-summary.json"
FETCH_REPORT_READER_SIDECAR = "summary-sidecar"
FETCH_REPORT_READER_FULL = "full-report"

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


def _fetch_report_summary_metadata(report_path: Path) -> dict[str, Any] | None:
    """Header/summary fields from the finished-run sidecar, when it owns the report.

    The fetch finalizer writes ``jobs-fetch-report-summary.json`` beside the
    report, and it carries exactly the fields the success gate reads. A small
    sidecar read plus a bounded identity read replaces a multi-megabyte report
    parse at desktop start. The sidecar is trusted only when its ``runId`` and
    ``finishedAt`` match the report's (a stale or mid-run sidecar cannot settle
    this gate) and it carries an output count; anything else falls back to the
    full report rather than guessing.

    Run identity is used instead of mtime ordering because filesystem timestamps
    advance in coarse clock ticks: two back-to-back writes of a small report and
    its sidecar can share a timestamp, and a rewritten identical report can keep
    an older one, neither of which says the sidecar is stale.
    """
    sidecar_path = report_path.with_name(FETCH_REPORT_SUMMARY_NAME)
    if not sidecar_path.is_file():
        return None
    summary_artifact = read_json(sidecar_path, {})
    if not isinstance(summary_artifact, dict):
        return None
    summary = summary_artifact.get("summary")
    if not isinstance(summary, dict) or "outputCount" not in summary:
        return None
    progress = summary_artifact.get("taskProgress")
    if isinstance(progress, dict) and progress.get("active"):
        return None
    finished_at = str(summary_artifact.get("finishedAt") or "").strip()
    if not finished_at:
        return None
    identity = fetch_report_identity(report_path)
    if identity is None:
        return None
    run_id = str(summary_artifact.get("runId") or "").strip()
    if identity != (run_id, finished_at):
        return None
    return {
        "runId": summary_artifact.get("runId"),
        "status": summary_artifact.get("status"),
        "finishedAt": finished_at,
        "summary": summary,
    }


def load_jobs_fetch_report_metadata(data_dir: str | Path) -> tuple[dict[str, Any], str]:
    """Report metadata plus the artifact that supplied it.

    The reader is ``summary-sidecar`` when the finished sidecar matched the
    report's run identity and ``full-report`` when the gate had to parse the
    report. Startup diagnostics record it so a silent fallback is visible from a
    packaged launch.
    """
    data_path = Path(data_dir)
    metadata = _fetch_report_summary_metadata(data_path / FETCH_REPORT_NAME)
    if metadata is not None:
        return metadata, FETCH_REPORT_READER_SIDECAR
    report = read_json(data_path / FETCH_REPORT_NAME, {})
    return (report if isinstance(report, dict) else {}), FETCH_REPORT_READER_FULL


def evaluate_runtime_jobs_report(data_dir: str | Path) -> tuple[bool, dict[str, Any]]:
    """Success-gate outcome plus the diagnostics that settled it.

    ``reader`` names the artifact the gate used, ``reportMetadataOk`` and
    ``feedArtifactsOk`` say which half decided the result (the feed check is only
    run when the metadata passed, so it stays ``None`` otherwise), and
    ``durationMs`` is the read cost the startup journal needs to show a fallback.
    """
    started = time.perf_counter()
    data_path = Path(data_dir)
    metadata, reader = load_jobs_fetch_report_metadata(data_path)
    metadata_ok = _runtime_report_has_success_metadata(metadata)
    feed_ok = has_loadable_runtime_feed_artifacts(data_path) if metadata_ok else None
    diagnostics: dict[str, Any] = {
        "reader": reader,
        "hasReport": bool(metadata),
        "reportRunId": str(metadata.get("runId") or ""),
        "reportFinishedAt": str(metadata.get("finishedAt") or ""),
        "reportMetadataOk": metadata_ok,
        "feedArtifactsOk": feed_ok,
        "success": bool(metadata_ok and feed_ok),
        "durationMs": round((time.perf_counter() - started) * 1000, 3),
    }
    return diagnostics["success"], diagnostics


# The desktop launcher asks "is there a feed?" from three cold-start paths, and
# the answer used to cost a full gzip decompression plus JSON parse of a 43 MB
# feed (measured 0.5-1.5 s each). Only the opening token decides the answer, so
# a bounded prefix settles it; anything the prefix cannot settle falls back to
# the full parse rather than guessing.
FEED_PRESENCE_PREFIX_BYTES = 256 * 1024


def _json_prefix_has_rows(prefix: bytes) -> bool | None:
    """Return True/False when the prefix settles row presence, else None."""
    if prefix.startswith(b"\xef\xbb\xbf"):
        prefix = prefix[3:]
    text = prefix.lstrip()
    if not text:
        return None
    if text[:1] == b"[":
        rest = text[1:].lstrip()
        if not rest:
            return None
        return rest[:1] != b"]"
    if text[:1] == b"{":
        key = text.find(b'"jobs"')
        if key < 0:
            return None
        rest = text[key + len(b'"jobs"') :].lstrip()
        if not rest.startswith(b":"):
            return None
        rest = rest[1:].lstrip()
        if not rest.startswith(b"["):
            return None
        rest = rest[1:].lstrip()
        if not rest:
            return None
        return rest[:1] != b"]"
    return None


def json_feed_artifact_has_rows(path: Path) -> bool:
    if str(path).lower().endswith(".gz"):
        try:
            with gzip.open(path, "rb") as handle:
                prefix = handle.read(FEED_PRESENCE_PREFIX_BYTES)
        except (OSError, EOFError):
            prefix = b""
        decided = _json_prefix_has_rows(prefix)
        if decided is not None:
            return decided
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
    return evaluate_runtime_jobs_report(data_dir)[0]


def has_successful_runtime_jobs_report_for_static_serving(data_dir: str | Path) -> bool:
    data_path = Path(data_dir)
    metadata, _reader = load_jobs_fetch_report_metadata(data_path)
    if not _runtime_report_has_success_metadata(metadata):
        return False
    return has_plausible_runtime_feed_artifacts_for_static_serving(data_path)


def jobs_cold_start_required(data_dir: str | Path) -> bool:
    return not has_loadable_runtime_feed_artifacts(data_dir)


def jobs_cold_start_required_for_static_serving(data_dir: str | Path) -> bool:
    return not has_successful_runtime_jobs_report_for_static_serving(data_dir)
