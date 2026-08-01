"""Fetch-report GET route handlers.

AI boundary owns: fetch-report and ops fetch-report GET route response wiring only.
AI boundary implement in: report normalization, jobs report contracts, and source-run helpers.
AI boundary search before contracts: frontend callers, bridge route inventory, API docs.
AI boundary verify: `npm run lint:repo-guardrails` plus focused GET route tests.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Protocol

from src.bridge.fetch_report_review_state import load_fetch_report_with_dedup_review_state
from src.bridge.fetch_report_summary import (
    compact_fetch_report_summary_payload,
    load_fetch_report_summary_artifact,
    load_fetch_task_summary_artifact,
)
from src.bridge.routes.get_fetch_report_sources import (
    FetchReportRouteApi,
    hydrate_fetch_report_sources_from_sqlite,
)
from src.bridge.routes.response_writer import BridgeResponseWriter
from src.bridge.routes.route_payload_helpers import (
    as_dict as _as_dict,
)
from src.bridge.routes.route_payload_helpers import (
    as_list as _as_list,
)
from src.bridge.routes.route_payload_helpers import (
    cached_summary_payload as _cached_summary_payload,
)
from src.bridge.routes.route_payload_helpers import (
    log_chunk_payload_from_path as _log_chunk_payload_from_path,
)
from src.bridge.routes.route_payload_helpers import path_signature as _path_signature
from src.bridge.routes.route_storage_metrics import record_storage_read_metric
from src.shared.partial_json import (
    decode_json_span,
    read_json_prefix,
    top_level_json_field_spans,
)

_FETCH_REPORT_SUMMARY_CACHE: dict[str, Any] = {}
_FETCH_REPORT_SUMMARY_SCAN_MAX_BYTES = 1024 * 1024
_FETCH_REPORT_TERMINAL_STATUSES = {
    "ok",
    "completed",
    "complete",
    "error",
    "failed",
    "aborted",
    "cancelled",
    "canceled",
}


class _FetchReportRouteApi(FetchReportRouteApi, Protocol):
    FETCHER_LOG_PATH: Path


def _summary_payload_is_active(payload: dict[str, Any]) -> bool:
    progress = _as_dict(payload.get("taskProgress"))
    status = str(payload.get("status") or "").strip().lower()
    return bool(progress.get("active")) or status in {"queued", "starting", "running", "active"}


def _summary_payload_is_terminal(payload: dict[str, Any]) -> bool:
    status = str(payload.get("status") or "").strip().lower()
    return (
        bool(str(payload.get("finishedAt") or "").strip())
        or status in _FETCH_REPORT_TERMINAL_STATUSES
    )


def _read_terminal_fetch_report_summary(report_path: Path, *, newer_than_ns: int) -> dict[str, Any]:
    """Read only terminal metadata from the full report when it supersedes hot artifacts.

    The summary and task sidecars are deliberately preferred while a run is active.  A
    final report written immediately before a worker crash is the exception: it is the
    authoritative terminal state even if the last sidecar still says ``running``.
    """
    try:
        report_mtime_ns = int(report_path.stat().st_mtime_ns)
    except OSError:
        return {}
    if report_mtime_ns <= int(newer_than_ns or 0):
        return {}
    text = read_json_prefix(report_path, max_bytes=_FETCH_REPORT_SUMMARY_SCAN_MAX_BYTES)
    if not text:
        return {}
    spans = top_level_json_field_spans(text)
    if not spans:
        return {}
    report = {
        "runId": decode_json_span(text, spans, "runId", ""),
        "status": decode_json_span(text, spans, "status", ""),
        "startedAt": decode_json_span(text, spans, "startedAt", ""),
        "finishedAt": decode_json_span(text, spans, "finishedAt", ""),
        "summary": _as_dict(decode_json_span(text, spans, "summary", {})),
        "taskProgress": _as_dict(decode_json_span(text, spans, "taskProgress", {})),
        "timing": _as_dict(decode_json_span(text, spans, "timing", {}, max_bytes=64 * 1024)),
        "availabilitySummary": _as_dict(decode_json_span(text, spans, "availabilitySummary", {})),
        "availabilityHealth": _as_dict(decode_json_span(text, spans, "availabilityHealth", {})),
    }
    report["ok"] = str(report.get("status") or "").strip().lower() not in {
        "error",
        "failed",
        "aborted",
        "cancelled",
        "canceled",
    }
    if not _summary_payload_is_terminal(report) or _summary_payload_is_active(report):
        return {}
    return compact_fetch_report_summary_payload(
        report,
        detail_level="summary",
        source="fetch-report-prefix-terminal",
    )


def _empty_fetch_report_summary_payload() -> dict[str, Any]:
    return {"ok": True, "summaryView": True, "detailLevel": "summary", "summary": {}}


def _terminal_report_for_summary(
    report_path: Path,
    sidecar: dict[str, Any],
    task_summary: dict[str, Any],
) -> dict[str, Any]:
    sidecar_path = report_path.with_name("jobs-fetch-report-summary.json")
    task_path = report_path.with_name("jobs-fetch-tasks.json")
    sidecar_signature = _path_signature(sidecar_path)
    task_signature = _path_signature(task_path)
    sidecar_mtime_ns = int(sidecar_signature[2]) if sidecar_signature else 0
    task_mtime_ns = int(task_signature[2]) if task_signature else 0
    terminal_report_floor_ns = max(sidecar_mtime_ns, task_mtime_ns)
    # A stale summary sidecar can survive after task-state cleanup. In that
    # case there is no newer task artifact to use as a safe scan boundary;
    # inspect the bounded report metadata and validate the run identity below.
    if sidecar and _summary_payload_is_active(sidecar) and not task_summary:
        terminal_report_floor_ns = 0

    # If the full report was flushed after an active sidecar, prefer its
    # terminal state. This closes the crash window where the sidecar remains
    # ``running`` while the report already records the failed/completed run.
    terminal_report = _read_terminal_fetch_report_summary(
        report_path,
        newer_than_ns=terminal_report_floor_ns,
    )
    if not terminal_report or not sidecar or not _summary_payload_is_active(sidecar):
        return terminal_report
    sidecar_run_id = str(sidecar.get("runId") or "").strip()
    report_run_id = str(terminal_report.get("runId") or "").strip()
    report_started_at = str(terminal_report.get("startedAt") or "").strip()
    sidecar_started_at = str(sidecar.get("startedAt") or "").strip()
    if (
        sidecar_run_id
        and report_run_id
        and sidecar_run_id != report_run_id
        and report_started_at
        and sidecar_started_at
        and report_started_at < sidecar_started_at
    ):
        return {}
    return terminal_report


def _summary_artifact_payload(
    sidecar: dict[str, Any],
    task_summary: dict[str, Any],
) -> dict[str, Any] | None:
    if sidecar and _summary_payload_is_terminal(sidecar):
        return compact_fetch_report_summary_payload(
            sidecar,
            detail_level="summary",
            source="fetch-report-summary-artifact",
        )
    if task_summary and _summary_payload_is_terminal(task_summary):
        return compact_fetch_report_summary_payload(
            task_summary,
            detail_level="summary",
            source="jobs-fetch-tasks",
        )
    if sidecar:
        return compact_fetch_report_summary_payload(
            sidecar,
            detail_level="summary",
            source="fetch-report-summary-artifact",
        )
    if task_summary:
        return compact_fetch_report_summary_payload(
            task_summary,
            detail_level="summary",
            source="jobs-fetch-tasks",
        )
    return None


def _summary_payload_from_report_prefix(report_path: Path) -> dict[str, Any]:
    text = read_json_prefix(
        report_path,
        max_bytes=_FETCH_REPORT_SUMMARY_SCAN_MAX_BYTES,
    )
    if not text:
        return _empty_fetch_report_summary_payload()
    spans = top_level_json_field_spans(text)
    summary = _as_dict(decode_json_span(text, spans, "summary", {}))
    task_progress = _as_dict(decode_json_span(text, spans, "taskProgress", {}))
    timing = _as_dict(decode_json_span(text, spans, "timing", {}, max_bytes=64 * 1024))
    return compact_fetch_report_summary_payload(
        {
            "runId": decode_json_span(text, spans, "runId", ""),
            "status": decode_json_span(text, spans, "status", ""),
            "startedAt": decode_json_span(text, spans, "startedAt", ""),
            "finishedAt": decode_json_span(text, spans, "finishedAt", ""),
            "summary": summary,
            "taskProgress": task_progress,
            "timing": timing,
        },
        detail_level="summary",
        source="fetch-report-prefix",
    )


def _fetch_report_summary_payload_from_file(path: Any) -> dict[str, Any]:
    report_path = Path(path) if path else None
    if not report_path:
        return _empty_fetch_report_summary_payload()
    signature = (
        _path_signature(report_path),
        _path_signature(report_path.with_name("jobs-fetch-report-summary.json")),
        _path_signature(report_path.with_name("jobs-fetch-tasks.json")),
    )
    if all(item is None for item in signature):
        return _empty_fetch_report_summary_payload()

    def _build() -> dict[str, Any]:
        sidecar = load_fetch_report_summary_artifact(report_path)
        task_summary = load_fetch_task_summary_artifact(report_path)
        terminal_report = _terminal_report_for_summary(report_path, sidecar, task_summary)
        return (
            terminal_report
            or _summary_artifact_payload(sidecar, task_summary)
            or _summary_payload_from_report_prefix(report_path)
        )

    return _cached_summary_payload(_FETCH_REPORT_SUMMARY_CACHE, signature, _build)


def _fetch_report_live_payload_from_file(path: Any) -> dict[str, Any]:
    report_path = Path(path) if path else None
    if not report_path:
        return {
            "ok": True,
            "summaryView": True,
            "detailLevel": "live",
            "summary": {},
            "sources": [],
        }
    sidecar = load_fetch_report_summary_artifact(report_path)
    if sidecar:
        return compact_fetch_report_summary_payload(
            sidecar,
            detail_level="live",
            source="fetch-report-summary-artifact",
            include_sources=True,
        )
    task_summary = load_fetch_task_summary_artifact(report_path)
    if task_summary:
        return compact_fetch_report_summary_payload(
            task_summary,
            detail_level="live",
            source="jobs-fetch-tasks",
            include_sources=True,
        )
    payload = _fetch_report_summary_payload_from_file(report_path)
    summary = _as_dict(payload.get("summary"))
    try:
        source_count = max(0, int(payload.get("sourceCount") or summary.get("sourceCount") or 0))
    except (TypeError, ValueError):
        source_count = 0
    result = {
        **payload,
        "detailLevel": "live",
        "sources": [],
        "sourcesTruncated": source_count > 0,
    }
    return result


def _handle_fetch_report_route(
    handler: BridgeResponseWriter,
    *,
    api: _FetchReportRouteApi,
    query: dict[str, list[str]],
) -> bool:
    view = str((query.get("view") or [""])[0] or "").strip().lower()
    if view == "summary":
        started_at = time.perf_counter()
        failed = True
        try:
            handler.send_json(_fetch_report_summary_payload_from_file(api.JOBS_FETCH_REPORT_PATH))
            failed = False
        finally:
            record_storage_read_metric(
                api,
                surface="sourceRuns.reportSummary",
                artifact="jobs-fetch-report.json",
                storage_kind="json",
                started_at=started_at,
                row_count=0,
                failed=failed,
            )
        return True
    if view == "live":
        started_at = time.perf_counter()
        failed = True
        try:
            handler.send_json(_fetch_report_live_payload_from_file(api.JOBS_FETCH_REPORT_PATH))
            failed = False
        finally:
            record_storage_read_metric(
                api,
                surface="sourceRuns.reportLiveSummary",
                artifact="jobs-fetch-report-summary.json",
                storage_kind="json",
                started_at=started_at,
                row_count=0,
                failed=failed,
            )
        return True
    started_at = time.perf_counter()
    source = "json"
    source_count = 0
    failed = True
    try:
        payload, dedup_review_state_warning = load_fetch_report_with_dedup_review_state(
            normalize_fetch_report_contract=api.normalize_fetch_report_contract,
            jobs_fetch_report_path=api.JOBS_FETCH_REPORT_PATH,
            dedup_review_state_path=api.DEDUP_REVIEW_STATE_PATH,
        )
        if dedup_review_state_warning:
            payload["dedupReviewStateReadWarning"] = dedup_review_state_warning
        if view != "live" and isinstance(payload, dict):
            payload = hydrate_fetch_report_sources_from_sqlite(api, payload)
            source = "sqlite" if _as_list(payload.get("sources")) else "json"
        if isinstance(payload, dict):
            source_count = len(_as_list(payload.get("sources")))
        handler.send_json(payload)
        failed = False
    finally:
        record_storage_read_metric(
            api,
            surface="sourceRuns.report",
            artifact="jobs-fetch-report.json",
            storage_kind=source,
            started_at=started_at,
            row_count=source_count,
            failed=failed,
        )
    return True


def handle_fetch_report_routes(
    handler: BridgeResponseWriter,
    *,
    api: _FetchReportRouteApi,
    path: str,
    query: dict[str, list[str]],
) -> bool:
    if path == "/fetcher/log":
        payload, status = _log_chunk_payload_from_path(api.FETCHER_LOG_PATH, query)
        handler.send_json(payload, status=status)
        return True

    if path == "/ops/fetch-report":
        return _handle_fetch_report_route(handler, api=api, query=query)

    return False
