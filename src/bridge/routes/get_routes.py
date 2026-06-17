"""GET route surface for the admin bridge.

AI boundary owns: GET route dispatch and response wiring only.
AI boundary implement in: bridge services/leaves behind `BridgeApi`.
AI boundary search before contracts: frontend callers, route inventory, API docs.
AI boundary verify: `npm run lint:repo-guardrails` plus focused GET tests.
"""

from __future__ import annotations

import io
import json
import logging
import os
import re
import time
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pydantic import ValidationError as PydanticValidationError

from src.bridge.admin_bootstrap import get_admin_bootstrap_payload
from src.bridge.api import BridgeApi
from src.bridge.container_mode import is_container_runtime, send_container_unavailable
from src.bridge.fetch_report_review_state import load_fetch_report_with_dedup_review_state
from src.bridge.performance_profile import time_operation
from src.bridge.registry_conflict_adjudication import overlay_adjudication
from src.bridge.registry_conflicts import (
    build_registry_conflicts_summary_cache_key,
    load_registry_conflicts_payload,
    load_registry_conflicts_summary_payload,
    summarize_registry_conflicts_payload,
    write_registry_conflicts_summary_cache,
)
from src.bridge.routes.error_boundary import (
    run_route_boundary,
    send_json_boundary,
)
from src.bridge.routes.get_discovery import handle_discovery_routes
from src.bridge.routes.get_fetch_report_sources import (
    hydrate_fetch_report_sources_from_sqlite,
)
from src.bridge.routes.get_ops_diagnostics import handle_ops_diagnostic_routes
from src.bridge.routes.get_ops_status import handle_ops_status_routes
from src.bridge.routes.get_registry import handle_registry_routes
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
from src.bridge.source_policy_link_backfill import (
    enrich_provider_coverage_link_backfill,
    load_provider_coverage_link_backfill,
    source_policy_soak_report_path,
)
from src.core.schemas import LocalSavedJobRowSchema
from src.jobs.common.contracts_source_policy_recommendations import (
    merge_source_policy_review_state_into_recommendations,
    read_source_policy_recommendations_artifact,
)
from src.jobs.common.contracts_source_policy_review_state import (
    read_source_policy_review_state_artifact,
)
from src.shared.partial_json import (
    decode_json_span,
    read_json_prefix,
    top_level_json_field_spans,
)

_ADMIN_BOOTSTRAP_SMOKE_FAIL_ONCE_CONSUMED = False


def _consume_admin_bootstrap_smoke_fail_once() -> bool:
    if str(os.getenv("BALUFFO_PACKAGED_SMOKE_RUNTIME") or "").strip() != "1":
        return False
    requested = str(os.getenv("BALUFFO_PACKAGED_SMOKE_ADMIN_BOOTSTRAP_FAIL_ONCE") or "")
    if requested.strip().lower() not in {"1", "true", "yes", "on"}:
        return False
    global _ADMIN_BOOTSTRAP_SMOKE_FAIL_ONCE_CONSUMED
    if _ADMIN_BOOTSTRAP_SMOKE_FAIL_ONCE_CONSUMED:
        return False
    _ADMIN_BOOTSTRAP_SMOKE_FAIL_ONCE_CONSUMED = True
    return True


logger = logging.getLogger(__name__)

_FETCH_REPORT_SUMMARY_CACHE: dict[str, Any] = {}
_FETCH_REPORT_SUMMARY_SCAN_MAX_BYTES = 16 * 1024 * 1024


def _sync_status_summary_payload(payload: dict[str, Any]) -> dict[str, Any]:
    config = _as_dict(payload.get("config"))
    runtime = _as_dict(payload.get("runtime"))
    saved_config = _as_dict(payload.get("savedConfig"))
    saved_enabled = (
        saved_config.get("enabled") if "enabled" in saved_config else config.get("enabled")
    )
    last_push = _as_dict(runtime.get("lastPush")) or _as_dict(runtime.get("push"))
    last_pull = _as_dict(runtime.get("lastPull")) or _as_dict(runtime.get("pull"))
    return {
        "ok": bool(payload.get("ok", True)),
        "appVersion": _clean_text(payload.get("appVersion")),
        "summaryView": True,
        "detailLevel": "summary",
        "config": {
            "enabled": bool(config.get("enabled")),
            "state": _clean_text(config.get("state")),
            "ready": bool(config.get("ready")),
            "repo": _clean_text(config.get("repo")),
            "branch": _clean_text(config.get("branch")),
            "path": _clean_text(config.get("path")),
            "missing": [
                _clean_text(item) for item in _as_list(config.get("missing")) if _clean_text(item)
            ][:20],
            "message": _clean_text(config.get("message")),
            "credentialsPackaged": bool(config.get("credentialsPackaged")),
        },
        "savedConfig": {"enabled": bool(saved_enabled)},
        "runtime": {
            "state": _clean_text(runtime.get("state") or runtime.get("code")),
            "message": _clean_text(runtime.get("message")),
            "lastPullAt": _clean_text(runtime.get("lastPullAt")),
            "lastPushAt": _clean_text(runtime.get("lastPushAt")),
            "lastAction": _clean_text(runtime.get("lastAction")),
            "lastResult": _clean_text(runtime.get("lastResult")),
            "lastError": _clean_text(runtime.get("lastError")),
            "lastPull": {
                "result": _clean_text(last_pull.get("result")),
                "finishedAt": _clean_text(last_pull.get("finishedAt")),
                "error": _clean_text(last_pull.get("error")),
            },
            "lastPush": {
                "result": _clean_text(last_push.get("result")),
                "finishedAt": _clean_text(last_push.get("finishedAt")),
                "error": _clean_text(last_push.get("error")),
            },
        },
    }


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


def _ops_tab_badge(
    *,
    count: int = 0,
    tone: str = "neutral",
    title: str = "",
    loaded: bool = True,
    error: str = "",
) -> dict[str, Any]:
    return {
        "count": max(0, int(count or 0)),
        "tone": str(tone or "neutral"),
        "title": str(title or ""),
        "loaded": bool(loaded),
        "error": str(error or ""),
    }


def _source_policy_badge_from_artifacts(api: BridgeApi) -> dict[str, Any]:
    recommendations, recommendation_warning = read_source_policy_recommendations_artifact(
        api.SOURCE_POLICY_RECOMMENDATIONS_PATH
    )
    review_state, review_state_warning = read_source_policy_review_state_artifact(
        api.SOURCE_POLICY_REVIEW_STATE_PATH
    )
    payload = merge_source_policy_review_state_into_recommendations(
        recommendations_artifact=recommendations,
        review_state=review_state,
    )
    pairs = _as_list(_as_dict(payload).get("pairs"))
    needs_action = 0
    for row in pairs:
        row_obj = _as_dict(row)
        review_state_text = _clean_text(row_obj.get("reviewState") or "new").lower()
        if review_state_text in {"new", "acknowledged"}:
            needs_action += 1
    warnings = [warning for warning in (recommendation_warning, review_state_warning) if warning]
    tone = "warning" if needs_action > 0 or warnings else "neutral"
    title = (
        f"{needs_action} source policy review item{'' if needs_action == 1 else 's'}"
        if needs_action > 0
        else "No source policy review items"
    )
    if warnings and needs_action <= 0:
        title = "Source policy review summary has warnings"
    return _ops_tab_badge(count=needs_action, tone=tone, title=title)


def _discovery_review_badge_from_report(api: BridgeApi) -> dict[str, Any]:
    report_path = Path(api.DISCOVERY_REPORT_PATH)
    if _path_signature(report_path) is None:
        return _ops_tab_badge(
            loaded=False,
            title="Discovery review count unavailable",
            error="discovery_report_missing",
        )
    report = _as_dict(api.load_json_object(report_path, {}))
    if not report:
        return _ops_tab_badge(
            loaded=False,
            title="Discovery review count unavailable",
            error="discovery_report_empty",
        )
    normalized = _as_dict(api.normalize_discovery_report_contract(report))
    review = _as_dict(normalized.get("candidateReview"))
    summary = _as_dict(normalized.get("summary"))
    counts = _as_dict(normalized.get("counts"))
    count = _safe_int(
        review.get("totalCandidates")
        or summary.get("queuedCandidateCount")
        or summary.get("candidateCount")
        or summary.get("newCandidateCount")
        or counts.get("queuedCandidates")
        or counts.get("candidateCount")
        or counts.get("generatedCandidates")
        or 0,
        0,
    )
    return _ops_tab_badge(
        count=count,
        tone="warning" if count > 0 else "neutral",
        title=(
            f"{count} discovery review item{'' if count == 1 else 's'}"
            if count > 0
            else "No discovery review items"
        ),
    )


def _registry_conflicts_badge_from_exact_summary(api: BridgeApi) -> dict[str, Any]:
    registry_summary = api.get_registry_summary_payload()
    source_state_path = Path(api.JOBS_FETCH_REPORT_PATH).with_name("jobs-source-state.json")
    adjudication = api.load_registry_conflict_adjudication()
    registry_auto_heal = api.get_registry_auto_heal_report()
    conflicts_payload = load_registry_conflicts_summary_payload(
        registry_summary=registry_summary,
        source_state_path=source_state_path,
        adjudication_payload=adjudication,
        registry_auto_heal=registry_auto_heal,
    )
    if _clean_text(conflicts_payload.get("summaryStatus")).lower() != "ready":
        full_payload = load_registry_conflicts_payload(
            load_state=api.load_state,
            load_json_object=api.load_json_object,
            source_state_path=source_state_path,
            adjudication_payload=adjudication,
        )
        full_payload = overlay_adjudication(full_payload, adjudication)
        full_payload["registrySummary"] = registry_summary
        full_payload["registryAutoHeal"] = registry_auto_heal
        full_payload["ok"] = True
        conflicts_payload = summarize_registry_conflicts_payload(full_payload)
        try:
            cache_key = build_registry_conflicts_summary_cache_key(
                registry_summary=registry_summary,
                source_state_path=source_state_path,
                adjudication_payload=adjudication,
            )
            write_registry_conflicts_summary_cache(
                source_state_path=source_state_path,
                cache_key=cache_key,
                payload=conflicts_payload,
            )
        except OSError:
            logger.debug("Could not write registry conflicts summary cache", exc_info=True)
    summary = _as_dict(conflicts_payload.get("summary"))
    count = _safe_int(summary.get("conflictCount"), 0)
    return _ops_tab_badge(
        count=count,
        tone="warning" if count > 0 else "neutral",
        title=(
            f"{count} registry conflict{'' if count == 1 else 's'}"
            if count > 0
            else "No registry conflicts"
        ),
    )


def _admin_ops_tab_counts_summary(api: BridgeApi) -> dict[str, Any]:
    badges: dict[str, dict[str, Any]] = {}

    try:
        health = _as_dict(api.compute_ops_dashboard_health_summary())
        alerts = _as_list(health.get("alerts"))
        critical_count = sum(
            1
            for alert in alerts
            if _clean_text(_as_dict(alert).get("severity")).lower() == "critical"
        )
        badges["overview"] = _ops_tab_badge(
            count=len(alerts),
            tone="critical" if critical_count else "warning" if alerts else "neutral",
            title=(
                f"{len(alerts)} active alert{'' if len(alerts) == 1 else 's'}"
                if alerts
                else "No active alerts"
            ),
        )
    except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError) as exc:
        badges["overview"] = _ops_tab_badge(
            loaded=False,
            title="Overview count unavailable",
            error=str(exc),
        )

    try:
        badges["discovery"] = _discovery_review_badge_from_report(api)
    except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError) as exc:
        badges["discovery"] = _ops_tab_badge(
            loaded=False,
            title="Discovery review count unavailable",
            error=str(exc),
        )

    try:
        badges["source-policy"] = _source_policy_badge_from_artifacts(api)
    except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError) as exc:
        badges["source-policy"] = _ops_tab_badge(
            loaded=False,
            title="Source policy review count unavailable",
            error=str(exc),
        )

    try:
        badges["registry-conflicts"] = _registry_conflicts_badge_from_exact_summary(api)
    except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError) as exc:
        badges["registry-conflicts"] = _ops_tab_badge(
            loaded=False,
            title="Registry conflict count unavailable",
            error=str(exc),
        )

    badges["dedup"] = _ops_tab_badge(
        loaded=False,
        title="Dedup count loads with dedup diagnostics",
    )

    return {
        "ok": True,
        "summaryView": True,
        "detailLevel": "summary",
        "generatedAt": api.now_iso(),
        "badges": badges,
    }


def _compact_live_fetch_report_payload(payload: dict[str, Any]) -> dict[str, Any]:
    compact_payload = dict(payload or {})
    sources = _as_list(payload.get("sources"))
    compact_payload["sources"] = [
        {key: value for key, value in row.items() if key != "details"}
        for row in sources
        if isinstance(row, dict)
    ]
    return compact_payload


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value or default)
    except (TypeError, ValueError):
        return int(default)


def _empty_suppression_eligibility_payload() -> dict[str, Any]:
    return {
        "readyLinkedProviderCount": 0,
        "selectedLinkedStaticCount": 0,
        "missingLinkedStaticCount": 0,
        "suppressedLinkedStaticCount": 0,
        "missingLinkedStaticRows": [],
    }


def _load_suppression_eligibility(api: BridgeApi) -> tuple[dict[str, Any], str]:
    path = source_policy_soak_report_path(api)
    empty_payload = _empty_suppression_eligibility_payload()
    if not path.exists():
        return empty_payload, ""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return empty_payload, f"source_policy_soak_report_unreadable: {exc}"
    section = _as_dict(_as_dict(payload.get("sections")).get("suppressionEligibility"))
    if not section:
        return empty_payload, ""
    result = {
        key: section.get(key, empty_payload[key])
        for key in (
            "readyLinkedProviderCount",
            "selectedLinkedStaticCount",
            "missingLinkedStaticCount",
            "suppressedLinkedStaticCount",
        )
    }
    result["missingLinkedStaticRows"] = [
        dict(row)
        for row in _as_list(section.get("missingLinkedStaticRows"))
        if isinstance(row, dict)
    ]
    return result, ""


def _json_error(exc: Exception) -> dict[str, Any]:
    return {"ok": False, "error": str(exc)}


def handle_get(
    handler: BridgeResponseWriter, *, api: BridgeApi, path: str, query: dict[str, list[str]]
) -> bool:
    """Handle GET routes for the admin bridge.

    Important: `api` must be the currently running BridgeApi instance.
    """

    if path == "/app/ready":
        with time_operation("app.ready.route_payload"):
            handler.send_json(api.compute_ops_health_ready())
        return True

    if path == "/admin/bootstrap":
        with time_operation("admin.bootstrap.route_payload"):
            if _consume_admin_bootstrap_smoke_fail_once():
                handler.send_json(
                    {
                        "ok": False,
                        "error": "packaged smoke forced admin bootstrap timeout",
                        "smokeFailure": True,
                    },
                    status=504,
                )
                return True
            handler.send_json(get_admin_bootstrap_payload(api))
        return True

    if path == "/admin/ops-tab-counts":
        view = str((query.get("view") or ["summary"])[0] or "summary").strip().lower()
        if view not in {"", "summary"}:
            handler.send_json(
                {"ok": False, "error": f"unsupported ops-tab-counts view: {view}"},
                status=400,
            )
            return True
        with time_operation("admin.ops_tab_counts.summary.route_payload"):
            handler.send_json(_admin_ops_tab_counts_summary(api))
        return True

    if handle_discovery_routes(handler, api=api, path=path, query=query):
        return True

    if path == "/desktop-local-data/session":

        def _payload() -> dict[str, Any]:
            route_started_at = time.perf_counter()
            session_started_at = time.perf_counter()
            desktop_session = api.get_desktop_session_payload()
            session_payload_ms = int((time.perf_counter() - session_started_at) * 1000)
            user_started_at = time.perf_counter()
            current_user = api.desktop_local_data_store().get_current_user()
            current_user_read_ms = int((time.perf_counter() - user_started_at) * 1000)
            payload_build_ms = int((time.perf_counter() - route_started_at) * 1000)
            return {
                "ok": True,
                "user": current_user,
                "lastActivityAt": str(api.DESKTOP_SESSION_ACTIVITY_AT or ""),
                "desktopSession": desktop_session,
                "timing": {
                    "sessionPayloadMs": session_payload_ms,
                    "currentUserReadMs": current_user_read_ms,
                    "payloadBuildMs": payload_build_ms,
                },
            }

        send_json_boundary(handler, _payload, error_status=400, error_payload=_json_error)
        return True

    if path == "/desktop-local-data/profiles":
        send_json_boundary(
            handler,
            lambda: {"ok": True, "profiles": api.desktop_local_data_store().list_profiles()},
            error_status=400,
            error_payload=_json_error,
        )
        return True

    if path == "/desktop-local-data/saved-jobs":

        def _payload() -> dict[str, Any]:
            uid = (query.get("uid") or [""])[0]
            raw_rows = api.desktop_local_data_store().list_saved_jobs(uid)
            rows = []
            for row in raw_rows:
                try:
                    LocalSavedJobRowSchema.model_validate(row)
                    rows.append(row)
                except PydanticValidationError as exc:
                    logger.warning("Saved job row validation failed, skipping: %s", exc)
            return {"ok": True, "rows": rows}

        send_json_boundary(handler, _payload, error_status=400, error_payload=_json_error)
        return True

    if path == "/desktop-local-data/saved-job-keys":

        def _payload() -> dict[str, Any]:
            uid = (query.get("uid") or [""])[0]
            return {"ok": True, "keys": api.desktop_local_data_store().get_saved_job_keys(uid)}

        send_json_boundary(handler, _payload, error_status=400, error_payload=_json_error)
        return True

    if path == "/desktop-local-data/attachments":

        def _payload() -> dict[str, Any]:
            uid = (query.get("uid") or [""])[0]
            job_key = (query.get("jobKey") or [""])[0]
            return {
                "ok": True,
                "rows": api.desktop_local_data_store().list_attachments_for_job(uid, job_key),
            }

        send_json_boundary(handler, _payload, error_status=400, error_payload=_json_error)
        return True

    if path == "/desktop-local-data/attachments/content":

        def _send_attachment() -> None:
            uid = (query.get("uid") or [""])[0]
            job_key = (query.get("jobKey") or [""])[0]
            attachment_id = (query.get("attachmentId") or [""])[0]
            download_flag = str((query.get("download") or [""])[0]).strip().lower()
            body, content_type, filename = api.desktop_local_data_store().get_attachment_blob(
                uid, job_key, attachment_id
            )
            handler.send_bytes(
                body,
                content_type=content_type,
                filename=filename,
                disposition="attachment" if download_flag in {"1", "true", "yes"} else "inline",
            )

        run_route_boundary(handler, _send_attachment, error_status=400, error_payload=_json_error)
        return True

    if path == "/desktop-local-data/backup/export-file":

        def _send_export_file() -> None:
            uid = (query.get("uid") or [""])[0]
            include_files_raw = str((query.get("includeFiles") or ["0"])[0]).strip().lower()
            include_files = include_files_raw in {"1", "true", "yes", "on"}
            payload = api.desktop_local_data_store().export_profile_data(
                uid, include_files=include_files
            )
            date_token = datetime.now(UTC).strftime("%Y-%m-%d")
            safe_uid = (
                re.sub(r"[^a-zA-Z0-9_-]+", "_", str(uid or "profile")).strip("_") or "profile"
            )
            if include_files:
                backup_json = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
                buffer = io.BytesIO()
                with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_STORED) as zf:
                    zf.writestr("backup.json", backup_json)
                body = buffer.getvalue()
                filename = f"baluffo-backup-{safe_uid}-{date_token}.zip"
                handler.send_bytes(
                    body,
                    content_type="application/zip",
                    filename=filename,
                    disposition="attachment",
                )
            else:
                body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
                filename = f"baluffo-backup-{safe_uid}-{date_token}.json"
                handler.send_bytes(
                    body,
                    content_type="application/json; charset=utf-8",
                    filename=filename,
                    disposition="attachment",
                )

        run_route_boundary(handler, _send_export_file, error_status=400, error_payload=_json_error)
        return True

    if path == "/desktop-local-data/activity":

        def _payload() -> dict[str, Any]:
            uid = (query.get("uid") or [""])[0]
            limit = int((query.get("limit") or ["300"])[0])
            return {
                "ok": True,
                "rows": api.desktop_local_data_store().list_activity_for_user(uid, limit),
            }

        send_json_boundary(handler, _payload, error_status=400, error_payload=_json_error)
        return True

    if path == "/desktop-local-data/startup-metrics":

        def _payload() -> dict[str, Any]:
            limit_raw = (query.get("limit") or ["200"])[0]
            try:
                limit = int(limit_raw)
            except ValueError:
                limit = 200
            return {"ok": True, "rows": api.read_startup_metrics(limit)}

        send_json_boundary(handler, _payload, error_status=400, error_payload=_json_error)
        return True

    if path == "/app/update-status":
        if is_container_runtime(api):
            send_container_unavailable(handler)
            return True
        send_json_boundary(
            handler,
            api.get_update_status_payload,
            error_status=500,
            error_payload=_json_error,
        )
        return True

    if handle_registry_routes(handler, api=api, path=path, query=query):
        return True

    if path == "/fetcher/log":
        text = _read_utf8_log_text(api.FETCHER_LOG_PATH)
        payload, status = _log_chunk_payload(text, query)
        handler.send_json(payload, status=status)
        return True

    if handle_ops_status_routes(handler, api=api, path=path, query=query):
        return True

    if handle_ops_diagnostic_routes(handler, api=api, path=path, query=query):
        return True

    if path == "/ops/fetch-report":
        view = str((query.get("view") or [""])[0] or "").strip().lower()
        if view == "summary":
            started_at = time.perf_counter()
            failed = False
            try:
                handler.send_json(
                    _fetch_report_summary_payload_from_file(api.JOBS_FETCH_REPORT_PATH)
                )
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

    if path == "/source-policy/recommendations":
        recommendations, recommendation_warning = read_source_policy_recommendations_artifact(
            api.SOURCE_POLICY_RECOMMENDATIONS_PATH
        )
        review_state, review_state_warning = read_source_policy_review_state_artifact(
            api.SOURCE_POLICY_REVIEW_STATE_PATH
        )
        link_backfill, link_backfill_warning = load_provider_coverage_link_backfill(api)
        suppression_eligibility, suppression_eligibility_warning = _load_suppression_eligibility(
            api
        )
        link_backfill = enrich_provider_coverage_link_backfill(api, link_backfill)
        payload = merge_source_policy_review_state_into_recommendations(
            recommendations_artifact=recommendations,
            review_state=review_state,
        )
        handler.send_json(
            {
                "ok": True,
                "recommendations": payload,
                "reviewState": review_state,
                "providerCoverageLinkBackfill": link_backfill,
                "suppressionEligibility": suppression_eligibility,
                "warnings": [
                    warning
                    for warning in (
                        recommendation_warning,
                        review_state_warning,
                        link_backfill_warning,
                        suppression_eligibility_warning,
                    )
                    if warning
                ],
            }
        )
        return True

    if path == "/registry/conflicts":
        view = str((query.get("view") or [""])[0] or "").strip().lower()
        source_state_path = Path(api.JOBS_FETCH_REPORT_PATH).with_name("jobs-source-state.json")
        adjudication = api.load_registry_conflict_adjudication()
        registry_summary = api.get_registry_summary_payload()
        registry_auto_heal = api.get_registry_auto_heal_report()
        if view == "summary":
            handler.send_json(
                load_registry_conflicts_summary_payload(
                    registry_summary=registry_summary,
                    source_state_path=source_state_path,
                    adjudication_payload=adjudication,
                    registry_auto_heal=registry_auto_heal,
                )
            )
            return True
        state = api.load_state()
        registry_summary = api.get_registry_summary_payload()
        registry_auto_heal = api.get_registry_auto_heal_report()
        payload = load_registry_conflicts_payload(
            load_state=lambda: state,
            load_json_object=api.load_json_object,
            source_state_path=source_state_path,
            adjudication_payload=adjudication,
        )
        payload = overlay_adjudication(payload, adjudication)
        payload["registrySummary"] = api.summarize_state(state)
        payload["registryAutoHeal"] = registry_auto_heal
        payload["ok"] = True
        try:
            cache_key = build_registry_conflicts_summary_cache_key(
                registry_summary=registry_summary,
                source_state_path=source_state_path,
                adjudication_payload=adjudication,
            )
            write_registry_conflicts_summary_cache(
                source_state_path=source_state_path,
                cache_key=cache_key,
                payload=summarize_registry_conflicts_payload(payload),
            )
        except OSError:
            logger.debug("Could not write registry conflicts summary cache", exc_info=True)
        handler.send_json(payload)
        return True

    if path == "/sync/status":
        view = str((query.get("view") or ["full"])[0] or "full").strip().lower()
        if view not in {"", "full", "summary"}:
            handler.send_json(
                {"ok": False, "error": f"unsupported sync status view: {view}"},
                status=400,
            )
            return True
        with time_operation("sync.status.summary" if view == "summary" else "sync.status"):
            runtime_state: dict[str, Any] = {}
            if view == "summary":
                try:
                    runtime_state = _as_dict(api.load_sync_runtime_state())
                except Exception:
                    runtime_state = {}
            payload = (
                _sync_status_summary_payload(
                    {
                        "ok": True,
                        "config": api.sync_config_status(),
                        "savedConfig": {},
                        "runtime": runtime_state,
                    }
                )
                if view == "summary"
                else api.get_sync_status_payload()
            )
        handler.send_json(payload)
        return True

    if path == "/tasks/jobs-pipeline-schedule":
        handler.send_json(api.get_jobs_pipeline_schedule_payload())
        return True

    if path == "/tasks/run-jobs-pipeline-status":
        handler.send_json(api.get_jobs_pipeline_status_payload())
        return True

    return False
