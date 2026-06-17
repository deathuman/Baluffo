"""Fetch-report GET route handlers."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from src.bridge.api import BridgeApi
from src.bridge.fetch_report_review_state import load_fetch_report_with_dedup_review_state
from src.bridge.routes.get_fetch_report_sources import (
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
    clean_text as _clean_text,
)
from src.bridge.routes.route_payload_helpers import (
    log_chunk_payload as _log_chunk_payload,
)
from src.bridge.routes.route_payload_helpers import (
    path_signature as _path_signature,
)
from src.bridge.routes.route_payload_helpers import (
    read_utf8_log_text as _read_utf8_log_text,
)
from src.bridge.routes.route_storage_metrics import record_storage_read_metric
from src.shared.partial_json import (
    decode_json_span,
    read_json_prefix,
    top_level_json_field_spans,
)

_FETCH_REPORT_SUMMARY_CACHE: dict[str, Any] = {}
_FETCH_REPORT_SUMMARY_SCAN_MAX_BYTES = 16 * 1024 * 1024


def _fetch_report_summary_payload_from_file(path: Any) -> dict[str, Any]:
    report_path = Path(path) if path else None
    signature = _path_signature(report_path)
    if not report_path or signature is None:
        return {"ok": True, "summaryView": True, "detailLevel": "summary", "summary": {}}

    def _build() -> dict[str, Any]:
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
            "ok": True,
            "summaryView": True,
            "detailLevel": "summary",
            "runId": _clean_text(decode_json_span(text, spans, "runId", "")),
            "status": _clean_text(decode_json_span(text, spans, "status", "")),
            "startedAt": _clean_text(decode_json_span(text, spans, "startedAt", "")),
            "finishedAt": _clean_text(decode_json_span(text, spans, "finishedAt", "")),
            "summary": {
                key: summary.get(key)
                for key in (
                    "outputCount",
                    "keptCount",
                    "fetchedCount",
                    "failedSources",
                    "totalSources",
                    "successfulSources",
                    "excludedSources",
                    "durationMs",
                )
                if key in summary
            },
            "taskProgress": {
                "active": bool(task_progress.get("active")),
                "phase": _clean_text(task_progress.get("phase")),
                "label": _clean_text(task_progress.get("label") or task_progress.get("phaseLabel")),
                "percent": task_progress.get("percent", 0),
            },
            "timing": {
                "durationMs": timing.get("durationMs"),
                "fetchAndParseMs": timing.get("fetchAndParseMs"),
            },
        }

    return _cached_summary_payload(_FETCH_REPORT_SUMMARY_CACHE, signature, _build)


def _compact_live_fetch_report_payload(payload: dict[str, Any]) -> dict[str, Any]:
    compact_payload = dict(payload or {})
    sources = _as_list(payload.get("sources"))
    compact_payload["sources"] = [
        {key: value for key, value in row.items() if key != "details"}
        for row in sources
        if isinstance(row, dict)
    ]
    return compact_payload


def _handle_fetch_report_route(
    handler: BridgeResponseWriter,
    *,
    api: BridgeApi,
    query: dict[str, list[str]],
) -> bool:
    view = str((query.get("view") or [""])[0] or "").strip().lower()
    if view == "summary":
        started_at = time.perf_counter()
        failed = False
        try:
            handler.send_json(_fetch_report_summary_payload_from_file(api.JOBS_FETCH_REPORT_PATH))
        except Exception:
            failed = True
            raise
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
    started_at = time.perf_counter()
    source = "json"
    source_count = 0
    failed = False
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
        if view == "live" and isinstance(payload, dict):
            payload = _compact_live_fetch_report_payload(payload)
        if isinstance(payload, dict):
            source_count = len(_as_list(payload.get("sources")))
        handler.send_json(payload)
    except Exception:
        failed = True
        raise
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
    api: BridgeApi,
    path: str,
    query: dict[str, list[str]],
) -> bool:
    if path == "/fetcher/log":
        text = _read_utf8_log_text(api.FETCHER_LOG_PATH)
        payload, status = _log_chunk_payload(text, query)
        handler.send_json(payload, status=status)
        return True

    if path == "/ops/fetch-report":
        return _handle_fetch_report_route(handler, api=api, query=query)

    if path == "/ops/fetch-report/sources":
        handler.send_json(fetch_report_sources_payload(api, query))
        return True

    return False
