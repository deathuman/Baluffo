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
    fetch_report_sources_payload,
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


class _FetchReportRouteApi(FetchReportRouteApi, Protocol):
    FETCHER_LOG_PATH: Path


def _fetch_report_summary_payload_from_file(path: Any) -> dict[str, Any]:
    report_path = Path(path) if path else None
    if not report_path:
        return {"ok": True, "summaryView": True, "detailLevel": "summary", "summary": {}}
    signature = (
        _path_signature(report_path),
        _path_signature(report_path.with_name("jobs-fetch-report-summary.json")),
        _path_signature(report_path.with_name("jobs-fetch-tasks.json")),
    )
    if signature[0] is None and signature[1] is None and signature[2] is None:
        return {"ok": True, "summaryView": True, "detailLevel": "summary", "summary": {}}

    def _build() -> dict[str, Any]:
        sidecar = load_fetch_report_summary_artifact(report_path)
        if sidecar:
            return compact_fetch_report_summary_payload(
                sidecar,
                detail_level="summary",
                source="fetch-report-summary-artifact",
            )
        task_summary = load_fetch_task_summary_artifact(report_path)
        if task_summary:
            return compact_fetch_report_summary_payload(
                task_summary,
                detail_level="summary",
                source="jobs-fetch-tasks",
            )
        text = read_json_prefix(
            report_path,
            max_bytes=_FETCH_REPORT_SUMMARY_SCAN_MAX_BYTES,
        )
        if not text:
            return {"ok": True, "summaryView": True, "detailLevel": "summary", "summary": {}}
        spans = top_level_json_field_spans(text)
        summary = _as_dict(decode_json_span(text, spans, "summary", {}))
        task_progress = _as_dict(decode_json_span(text, spans, "taskProgress", {}))
        timing = _as_dict(decode_json_span(text, spans, "timing", {}, max_bytes=64 * 1024))
        return {
            **compact_fetch_report_summary_payload(
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
            ),
        }

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
    if source_count:
        result["sourceDetailPath"] = "/ops/fetch-report/sources"
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

    if path == "/ops/fetch-report/sources":
        handler.send_json(fetch_report_sources_payload(api, query))
        return True

    return False
