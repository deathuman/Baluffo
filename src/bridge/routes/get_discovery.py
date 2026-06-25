"""Discovery GET route handlers.

AI boundary owns: `/discovery/*` GET route response wiring only.
AI boundary implement in: discovery services, reports, and candidate helpers.
AI boundary search before contracts: frontend callers, bridge route inventory, API docs.
AI boundary verify: `npm run lint:repo-guardrails` plus focused GET route tests.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Protocol

from src.bridge.routes.error_boundary import (
    run_route_boundary,
    safe_bridge_log,
    send_json_boundary,
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
    last_items as _last_items,
)
from src.bridge.routes.route_payload_helpers import (
    log_chunk_payload_from_path as _log_chunk_payload_from_path,
)
from src.bridge.routes.route_payload_helpers import (
    path_signature as _path_signature,
)
from src.shared.partial_json import (
    decode_json_span,
    read_json_prefix,
    top_level_json_field_spans,
)
from src.source_registry_io import load_runtime_evidence_array

_DISCOVERY_REPORT_SUMMARY_CACHE: dict[str, Any] = {}


class _DiscoveryRouteApi(Protocol):
    DISCOVERY_CANDIDATES_PATH: Path
    DISCOVERY_LOG_PATH: Path
    DISCOVERY_REPORT_PATH: Path

    def bridge_log(self, level: str, event: str, **fields: Any) -> None: ...

    def get_discovery_config_payload(self) -> dict[str, Any]: ...


def _discovery_report_summary_payload(report: dict[str, Any]) -> dict[str, Any]:
    summary = _as_dict(report.get("summary"))
    runtime = _as_dict(report.get("runtime"))
    task_progress = _as_dict(report.get("taskProgress"))
    registry_finalization = _as_dict(runtime.get("registryFinalization"))
    auto_approval = _as_dict(runtime.get("autoApproval"))
    failures = _as_list(report.get("failures"))
    candidates = _as_list(report.get("candidates"))
    log_rows = (
        _as_list(report.get("log")) or _as_list(report.get("logs")) or _as_list(runtime.get("log"))
    )
    return {
        "ok": True,
        "summaryView": True,
        "detailLevel": "summary",
        "runId": _clean_text(report.get("runId")),
        "status": _clean_text(report.get("status")),
        "startedAt": _clean_text(report.get("startedAt")),
        "finishedAt": _clean_text(report.get("finishedAt")),
        "taskProgress": {
            "active": bool(task_progress.get("active")),
            "phase": _clean_text(task_progress.get("phase")),
            "label": _clean_text(task_progress.get("label")),
            "percent": task_progress.get("percent", 0),
        },
        "summary": {
            key: summary.get(key)
            for key in (
                "endpointCount",
                "foundEndpointCount",
                "probedCount",
                "probedCandidateCount",
                "queuedCount",
                "queuedCandidateCount",
                "candidateCount",
                "deferredCount",
                "discoverableButDeferredCount",
                "failedCount",
                "failedProbeCount",
                "failureCount",
                "pendingCount",
                "activeCount",
                "rejectedCount",
            )
            if key in summary
        },
        "counts": {
            "candidateCount": len(candidates),
            "failureCount": len(failures),
        },
        "runtime": {
            "registryFinalization": {
                "status": _clean_text(registry_finalization.get("status")),
                "activeCount": registry_finalization.get("activeCount"),
                "pendingCount": registry_finalization.get("pendingCount"),
                "rejectedCount": registry_finalization.get("rejectedCount"),
            },
            "autoApproval": {
                "enabled": bool(auto_approval.get("enabled")),
                "status": _clean_text(auto_approval.get("status")),
                "approvedCount": auto_approval.get("approvedCount"),
            },
        },
        "recentLog": _last_items(log_rows, 20),
    }


def _discovery_report_summary_payload_from_file(path: Any) -> dict[str, Any]:
    report_path = Path(path) if path else None
    signature = _path_signature(report_path)
    if not report_path or signature is None:
        return _discovery_report_summary_payload({})

    def _build() -> dict[str, Any]:
        text = read_json_prefix(report_path, max_bytes=512 * 1024)
        if not text:
            return _discovery_report_summary_payload({})
        spans = top_level_json_field_spans(text)
        summary = _as_dict(decode_json_span(text, spans, "summary", {}))
        runtime = _as_dict(decode_json_span(text, spans, "runtime", {}))
        task_progress = _as_dict(decode_json_span(text, spans, "taskProgress", {}))
        registry_finalization = _as_dict(runtime.get("registryFinalization"))
        auto_approval = _as_dict(runtime.get("autoApproval"))
        log_rows = (
            _as_list(decode_json_span(text, spans, "log", [], max_bytes=64 * 1024))
            or _as_list(decode_json_span(text, spans, "logs", [], max_bytes=64 * 1024))
            or _as_list(runtime.get("log"))
        )
        payload = _discovery_report_summary_payload(
            {
                "runId": decode_json_span(text, spans, "runId", ""),
                "status": decode_json_span(text, spans, "status", ""),
                "startedAt": decode_json_span(text, spans, "startedAt", ""),
                "finishedAt": decode_json_span(text, spans, "finishedAt", ""),
                "summary": summary,
                "runtime": {
                    "registryFinalization": registry_finalization,
                    "autoApproval": auto_approval,
                },
                "taskProgress": task_progress,
                "log": log_rows,
            }
        )
        payload["counts"] = {
            "candidateCount": summary.get("candidateCount")
            or summary.get("queuedCandidateCount")
            or summary.get("newCandidateCount")
            or 0,
            "failureCount": summary.get("failureCount")
            or summary.get("failedCount")
            or summary.get("failedProbeCount")
            or 0,
        }
        return payload

    return _cached_summary_payload(_DISCOVERY_REPORT_SUMMARY_CACHE, signature, _build)


def _send_json_bytes(
    handler: BridgeResponseWriter, payload: dict[str, Any], *, status: int
) -> None:
    try:
        text = json.dumps(payload, ensure_ascii=False, default=str)
        body = text.encode("utf-8")
    except UnicodeEncodeError:
        text = json.dumps(payload, ensure_ascii=True, default=str)
        body = text.encode("utf-8")
    handler.send_bytes(
        body,
        content_type="application/json; charset=utf-8",
        status=status,
    )


def _handle_discovery_report_route(
    handler: BridgeResponseWriter,
    *,
    api: _DiscoveryRouteApi,
    query: dict[str, list[str]] | None = None,
) -> bool:
    # This route must never "silently" drop the connection; the admin UI
    # treats network errors as bridge-availability failures.
    view = str(((query or {}).get("view") or ["full"])[0] or "full").strip().lower()
    if view not in {"", "full", "summary"}:
        handler.send_json(
            {"ok": False, "error": f"unsupported discovery report view: {view}"},
            status=400,
        )
        return True

    def _send_discovery_report() -> None:
        from src.source_registry_io import load_runtime_evidence

        reconciler = getattr(api, "reconcile_terminal_discovery_report_from_state", None)
        if view != "summary" and callable(reconciler):
            reconciler()

        if view == "summary":
            payload = _discovery_report_summary_payload_from_file(
                getattr(api, "DISCOVERY_REPORT_PATH", None)
            )
            safe_bridge_log(
                api,
                "info",
                "discovery_report_summary_route_sending",
                summaryType=type(payload.get("summary")).__name__,
            )
        else:
            raw = load_runtime_evidence(getattr(api, "DISCOVERY_REPORT_PATH", None), {})

            normalizer_fn = getattr(api, "normalize_discovery_report_contract", None)
            report = normalizer_fn(raw) if callable(normalizer_fn) else raw

            safe_bridge_log(
                api,
                "info",
                "discovery_report_route_sending",
                reportType=type(report).__name__,
                summaryType=type((report or {}).get("summary", None)).__name__
                if isinstance(report, dict)
                else "",
            )

            payload = _as_dict(report) or {"summary": {}, "candidates": [], "failures": []}
        # Prefer the bytes-writing helper to bypass any unexpected issues
        # in JSON response serialization for edge-case payloads.
        if hasattr(handler, "send_bytes"):
            _send_json_bytes(handler, payload, status=200)
        else:
            handler.send_json(payload)

    def _discovery_report_error(exc: Exception) -> dict[str, Any]:
        safe_bridge_log(api, "error", "discovery_report_route_failed", error=str(exc))
        return {"error": "failed_to_load_discovery_report", "detail": str(exc)}

    if hasattr(handler, "send_bytes"):

        def _send_error(exc: Exception) -> None:
            _send_json_bytes(handler, _discovery_report_error(exc), status=500)

        run_route_boundary(
            handler,
            _send_discovery_report,
            error_status=500,
            error_payload=_discovery_report_error,
            error_sender=_send_error,
        )
    else:
        run_route_boundary(
            handler,
            _send_discovery_report,
            error_status=500,
            error_payload=_discovery_report_error,
        )
    return True


def _handle_discovery_candidates_route(
    handler: BridgeResponseWriter,
    *,
    api: _DiscoveryRouteApi,
) -> bool:
    candidates_path = getattr(api, "DISCOVERY_CANDIDATES_PATH", None)

    def _payload() -> dict[str, Any]:
        if candidates_path is None:
            return {"candidates": [], "count": 0}
        candidates = load_runtime_evidence_array(candidates_path, [])
        return {"candidates": candidates, "count": len(candidates)}

    def _error(exc: Exception) -> dict[str, Any]:
        safe_bridge_log(api, "error", "discovery_candidates_route_failed", error=str(exc))
        return {"error": "failed_to_load_discovery_candidates", "detail": str(exc)}

    send_json_boundary(handler, _payload, error_status=500, error_payload=_error)
    return True


def handle_discovery_routes(
    handler: BridgeResponseWriter,
    *,
    api: _DiscoveryRouteApi,
    path: str,
    query: dict[str, list[str]],
) -> bool:
    if path == "/discovery/report":
        return _handle_discovery_report_route(handler, api=api, query=query)

    if path == "/discovery/candidates":
        return _handle_discovery_candidates_route(handler, api=api)

    if path == "/discovery/log":
        payload, status = _log_chunk_payload_from_path(api.DISCOVERY_LOG_PATH, query)
        handler.send_json(payload, status=status)
        return True

    if path == "/discovery/config":
        handler.send_json(api.get_discovery_config_payload())
        return True

    return False
