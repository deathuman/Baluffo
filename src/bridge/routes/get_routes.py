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
import re
import time
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pydantic import ValidationError as PydanticValidationError

from src.bridge.api import BridgeApi
from src.bridge.container_mode import is_container_runtime, send_container_unavailable
from src.bridge.discovery_audit_artifacts import get_discovery_audit_artifacts_payload
from src.bridge.fetch_report_review_state import load_fetch_report_with_dedup_review_state
from src.bridge.performance_profile import snapshot_performance_profile, time_operation
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
    safe_bridge_log,
    send_json_boundary,
)
from src.bridge.routes.response_writer import BridgeResponseWriter
from src.bridge.source_policy_migration_links import ADMIN_MIGRATION_LINK_ACTOR
from src.bridge.storage_health import get_storage_store, record_storage_diagnostic
from src.bridge.task_failure_attempts import get_task_failure_attempts_payload
from src.core.schemas import LocalSavedJobRowSchema
from src.jobs.common.contracts_source_policy_recommendations import (
    merge_source_policy_review_state_into_recommendations,
    read_source_policy_recommendations_artifact,
)
from src.jobs.common.contracts_source_policy_review_state import (
    read_source_policy_review_state_artifact,
)
from src.shared.timing_counters import snapshot_counters
from src.source_registry import is_hidden_from_default
from src.source_registry_io import load_runtime_evidence_array
from src.storage import SourceRuntimeStore
from src.storage_metrics import duration_ms, record_storage_read, snapshot_storage_metrics

logger = logging.getLogger(__name__)


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _last_items(value: Any, limit: int) -> list[Any]:
    rows = _as_list(value)
    bounded_limit = max(0, min(50, int(limit or 0)))
    if bounded_limit <= 0:
        return []
    return rows[-bounded_limit:]


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


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
                "probedCount",
                "queuedCandidateCount",
                "deferredCount",
                "failedCount",
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


def _compact_live_fetch_report_payload(payload: dict[str, Any]) -> dict[str, Any]:
    compact_payload = dict(payload or {})
    sources = _as_list(payload.get("sources"))
    compact_payload["sources"] = [
        {key: value for key, value in row.items() if key != "details"}
        for row in sources
        if isinstance(row, dict)
    ]
    return compact_payload


def _source_policy_soak_report_path(api: BridgeApi) -> Path:
    data_dir = Path(api.SOURCE_POLICY_RECOMMENDATIONS_PATH).parent
    return data_dir.parent / "_out" / "source-policy-soak-report.json"


def _storage_metrics_data_dir(api: BridgeApi) -> Path:
    data_dir = getattr(api.runtime_config, "data_dir", None)
    if data_dir:
        return Path(data_dir).expanduser().resolve()
    return Path(api.JOBS_FETCH_REPORT_PATH).expanduser().resolve().parent


def _record_storage_read_metric(
    api: BridgeApi,
    *,
    surface: str,
    artifact: str,
    storage_kind: str,
    started_at: float,
    row_count: int = 0,
    bytes_read: int = 0,
    failed: bool = False,
) -> None:
    record_storage_read(
        surface=surface,
        artifact=artifact,
        storage_kind=storage_kind,
        duration_ms=duration_ms(started_at),
        bytes_read=max(0, int(bytes_read or 0)),
        row_count=max(0, int(row_count or 0)),
        failed=failed,
        data_dir=_storage_metrics_data_dir(api),
    )


def _performance_profile_runtime(api: BridgeApi) -> dict[str, Any]:
    runtime_config = getattr(api, "runtime_config", None)
    owner_mode = str(getattr(runtime_config, "owner_mode", "") or "")
    if is_container_runtime(api):
        runtime_mode = "container"
    elif bool(getattr(runtime_config, "desktop_mode", False)):
        runtime_mode = "desktop"
    else:
        runtime_mode = "bridge"
    return {
        "appVersion": str(getattr(api, "app_version", "") or ""),
        "runtimeMode": runtime_mode,
        "ownerMode": owner_mode,
    }


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value or default)
    except (TypeError, ValueError):
        return int(default)


def _source_parity_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "name": _clean_text(row.get("name")),
            "status": _clean_text(row.get("status")).lower(),
            "adapter": _clean_text(row.get("adapter")),
            "fetchStrategy": _clean_text(row.get("fetchStrategy")),
            "studio": _clean_text(row.get("studio")),
            "fetchedCount": _safe_int(row.get("fetchedCount")),
            "keptCount": _safe_int(row.get("keptCount")),
            "lowConfidenceDropped": _safe_int(row.get("lowConfidenceDropped")),
            "error": _clean_text(row.get("error")),
            "durationMs": _safe_int(row.get("durationMs")),
        }
        for row in rows
        if isinstance(row, dict)
    ]


def _record_source_run_diagnostic(
    api: BridgeApi,
    *,
    code: str,
    ok: bool,
    message: str = "",
    details: dict[str, Any] | None = None,
) -> None:
    record_storage_diagnostic(
        _storage_metrics_data_dir(api),
        surface="sourceRuns",
        code=code,
        ok=ok,
        message=message,
        details=dict(details or {}),
    )


def _source_runtime_store(api: BridgeApi, *, row_limit: int = 500) -> SourceRuntimeStore | None:
    try:
        return SourceRuntimeStore(
            get_storage_store(_storage_metrics_data_dir(api)),
            row_limit=max(1, int(row_limit)),
        )
    except (OSError, RuntimeError, TypeError, ValueError) as exc:
        _record_source_run_diagnostic(
            api,
            code="source_runs_store_unavailable",
            ok=False,
            message=str(exc),
        )
        return None


def _source_runs_mode(runtime_store: SourceRuntimeStore) -> str:
    try:
        modes = runtime_store.store.get_authority_modes()
    except (OSError, RuntimeError, TypeError, ValueError):
        return "json"
    return str((modes or {}).get("sourceRuns") or "json").strip().lower()


def _rollback_source_runs_to_json(
    api: BridgeApi,
    runtime_store: SourceRuntimeStore,
    *,
    code: str,
    message: str,
    details: dict[str, Any] | None = None,
) -> None:
    try:
        runtime_store.store.set_authority_mode("sourceRuns", "json", reason=code)
    except (OSError, RuntimeError, TypeError, ValueError) as exc:
        message = f"{message}; rollback failed: {exc}"
    _record_source_run_diagnostic(
        api,
        code=code,
        ok=False,
        message=message,
        details=dict(details or {}),
    )


def _fetch_report_source_count(payload: dict[str, Any]) -> int:
    summary = _as_dict(payload.get("summary"))
    sources = _as_list(payload.get("sources"))
    return max(0, _safe_int(summary.get("sourceCount")), len(sources))


def _hydrate_fetch_report_sources_from_sqlite(
    api: BridgeApi,
    payload: dict[str, Any],
) -> dict[str, Any]:
    run_id = _clean_text(payload.get("runId"))
    if not run_id:
        return payload
    expected_count = _fetch_report_source_count(payload)
    runtime_store = _source_runtime_store(api, row_limit=max(500, expected_count))
    if runtime_store is None or _source_runs_mode(runtime_store) != "sqlite":
        return payload
    rows = runtime_store.source_runs(run_id=run_id, limit=max(1, expected_count or 500))
    json_sources = [row for row in _as_list(payload.get("sources")) if isinstance(row, dict)]
    if expected_count and len(rows) != expected_count:
        _rollback_source_runs_to_json(
            api,
            runtime_store,
            code="source_runs_read_count_mismatch",
            message="SQLite source_runs count did not match fetch report",
            details={"expectedCount": expected_count, "sqliteCount": len(rows)},
        )
        return payload
    if json_sources and _source_parity_rows(rows) != _source_parity_rows(json_sources):
        _rollback_source_runs_to_json(
            api,
            runtime_store,
            code="source_runs_read_projection_mismatch",
            message="SQLite source_runs projection did not match fetch report",
            details={"jsonCount": len(json_sources), "sqliteCount": len(rows)},
        )
        return payload
    if rows:
        hydrated = dict(payload)
        hydrated["sources"] = rows
        _record_source_run_diagnostic(
            api,
            code="source_runs_read_projection_match",
            ok=True,
            details={"rowCount": len(rows)},
        )
        return hydrated
    return payload


def _fetch_report_sources_payload(
    api: BridgeApi,
    query: dict[str, list[str]],
) -> dict[str, Any]:
    started_at = time.perf_counter()
    source = "json"
    rows: list[dict[str, Any]] = []
    failed = False
    try:
        report, warning = load_fetch_report_with_dedup_review_state(
            normalize_fetch_report_contract=api.normalize_fetch_report_contract,
            jobs_fetch_report_path=api.JOBS_FETCH_REPORT_PATH,
            dedup_review_state_path=api.DEDUP_REVIEW_STATE_PATH,
        )
        run_id = _clean_text((query.get("runId") or [""])[0]) or _clean_text(report.get("runId"))
        status = _clean_text((query.get("status") or [""])[0]).lower()
        limit = max(1, min(500, _safe_int((query.get("limit") or ["100"])[0], 100)))
        offset = max(0, _safe_int((query.get("offset") or ["0"])[0], 0))
        runtime_store = _source_runtime_store(api, row_limit=limit)
        if run_id and runtime_store is not None and _source_runs_mode(runtime_store) == "sqlite":
            rows = runtime_store.source_runs(
                run_id=run_id,
                status=status,
                limit=limit,
                offset=offset,
            )
            if rows:
                source = "sqlite"
            else:
                _rollback_source_runs_to_json(
                    api,
                    runtime_store,
                    code="source_runs_read_empty",
                    message="SQLite source_runs did not contain requested fetch source rows",
                    details={"runId": run_id},
                )
        if not rows:
            json_rows = [row for row in _as_list(report.get("sources")) if isinstance(row, dict)]
            if status:
                json_rows = [
                    row for row in json_rows if _clean_text(row.get("status")).lower() == status
                ]
            rows = json_rows[offset : offset + limit]
        return {
            "ok": True,
            "runId": run_id,
            "sources": rows,
            "count": len(rows),
            "limit": limit,
            "offset": offset,
            "source": source,
            "warning": warning,
        }
    except Exception:
        failed = True
        raise
    finally:
        _record_storage_read_metric(
            api,
            surface="sourceRuns.reportSources",
            artifact="jobs-fetch-report.json",
            storage_kind=source,
            started_at=started_at,
            row_count=len(rows),
            failed=failed,
        )


def _load_provider_coverage_link_backfill(api: BridgeApi) -> tuple[dict[str, Any], str]:
    path = _source_policy_soak_report_path(api)
    empty_payload = {
        "reviewCandidates": [],
        "blockedCandidates": [],
        "linkedCandidates": [],
        "candidateLinkCount": 0,
        "blockedCount": 0,
        "highConfidenceLinkCount": 0,
        "mediumConfidenceLinkCount": 0,
        "blockedReasonCounts": {},
        "disambiguationBlockerCounts": {},
        "blockedExamples": [],
        "disambiguationBlockedExamples": [],
        "activeProviderWithoutMigrationIdentityCount": 0,
    }
    if not path.exists():
        return empty_payload, ""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return empty_payload, f"source_policy_soak_report_unreadable: {exc}"
    section = _as_dict(_as_dict(payload.get("sections")).get("providerCoverageLinkBackfill"))
    if not section:
        return empty_payload, ""
    result = {
        key: section.get(key)
        for key in (
            "activeProviderWithoutMigrationIdentityCount",
            "candidateLinkCount",
            "blockedCount",
            "highConfidenceLinkCount",
            "mediumConfidenceLinkCount",
            "ambiguousProviderCount",
            "ambiguousStaticCandidateCount",
            "resolvedBySourceStateCount",
            "resolvedByAdvisoryIdentityCount",
            "unresolvedAmbiguousCount",
            "blockedReasonCounts",
            "disambiguationBlockerCounts",
        )
        if key in section
    }
    result["reviewCandidates"] = [
        dict(row) for row in _as_list(section.get("reviewCandidates")) if isinstance(row, dict)
    ]
    result["blockedCandidates"] = [
        dict(row) for row in _as_list(section.get("blockedCandidates")) if isinstance(row, dict)
    ]
    result["linkedCandidates"] = [
        dict(row)
        for row in _as_list(section.get("links"))
        if isinstance(row, dict) and _clean_text(row.get("recommendedAction")) == "already_linked"
    ]
    result["blockedExamples"] = [
        dict(row) for row in _as_list(section.get("blockedExamples")) if isinstance(row, dict)
    ]
    result["disambiguationBlockedExamples"] = [
        dict(row)
        for row in _as_list(section.get("disambiguationBlockedExamples"))
        if isinstance(row, dict)
    ]
    return result, ""


def _empty_suppression_eligibility_payload() -> dict[str, Any]:
    return {
        "readyLinkedProviderCount": 0,
        "selectedLinkedStaticCount": 0,
        "missingLinkedStaticCount": 0,
        "suppressedLinkedStaticCount": 0,
        "missingLinkedStaticRows": [],
    }


def _load_suppression_eligibility(api: BridgeApi) -> tuple[dict[str, Any], str]:
    path = _source_policy_soak_report_path(api)
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


def _row_identity_tokens(row: dict[str, Any]) -> set[str]:
    return {
        token.lower()
        for token in (
            _clean_text(row.get("id")),
            _clean_text(row.get("sourceId")),
            _clean_text(row.get("sourceIdentity")),
        )
        if token
    }


def _find_state_row_by_id(
    state: dict[str, list[dict[str, Any]]], source_id: str
) -> tuple[str, dict[str, Any]] | None:
    target = _clean_text(source_id).lower()
    if not target:
        return None
    for bucket in ("active", "pending"):
        for row in state.get(bucket) or []:
            if isinstance(row, dict) and target in _row_identity_tokens(row):
                return bucket, row
    return None


def _source_id(api: BridgeApi, row: dict[str, Any]) -> str:
    for key in ("id", "sourceId", "sourceIdentity"):
        value = _clean_text(row.get(key))
        if value:
            return value
    try:
        return _clean_text(api.source_identity(row))
    except (AttributeError, TypeError, ValueError):
        return ""


def _find_static_row_name(state: dict[str, list[dict[str, Any]]], static_source_id: str) -> str:
    match = _find_state_row_by_id(state, static_source_id)
    if not match:
        return ""
    _bucket, static_row = match
    return _clean_text(static_row.get("name"))


def _provider_coverage_rows(api: BridgeApi) -> list[dict[str, Any]]:
    from src.source_registry_io import load_runtime_evidence

    payload = load_runtime_evidence(api.JOBS_FETCH_REPORT_PATH, {})
    provider_coverage = _as_dict(payload.get("providerCoverage"))
    rows: list[dict[str, Any]] = []
    for key in (
        "validatedProviders",
        "probingProviders",
        "unstableOrFailedProviders",
        "needsReviewProviders",
        "readyLaterProviders",
    ):
        rows.extend(row for row in _as_list(provider_coverage.get(key)) if isinstance(row, dict))
    return rows


def _provider_coverage_for_link(
    coverage_rows: list[dict[str, Any]],
    *,
    provider_row: dict[str, Any] | None = None,
    linked_row: dict[str, Any] | None = None,
    static_source_id: str,
) -> dict[str, Any]:
    provider_name = _clean_text((provider_row or {}).get("name")) or _clean_text(
        (linked_row or {}).get("providerSourceName")
    )
    provider_adapter = _clean_text((provider_row or {}).get("adapter")) or _clean_text(
        (linked_row or {}).get("providerAdapter")
    )
    for row in coverage_rows:
        if _clean_text(row.get("migrationSourceIdentity")) != static_source_id:
            continue
        row_name = _clean_text(row.get("name"))
        row_adapter = _clean_text(row.get("adapter"))
        if provider_name and row_name and provider_name != row_name:
            continue
        if provider_adapter and row_adapter and provider_adapter != row_adapter:
            continue
        return row
    return {}


def _linked_candidate_from_provider_row(
    api: BridgeApi,
    state: dict[str, list[dict[str, Any]]],
    coverage_rows: list[dict[str, Any]],
    *,
    bucket: str,
    provider_row: dict[str, Any],
) -> dict[str, Any] | None:
    static_source_id = _clean_text(provider_row.get("migrationSourceIdentity"))
    linked_by = _clean_text(provider_row.get("migrationLinkedBy"))
    if not static_source_id or not linked_by:
        return None
    provider_id = _source_id(api, provider_row)
    static_name = _clean_text(provider_row.get("migrationSourceName")) or _find_static_row_name(
        state, static_source_id
    )
    coverage = _provider_coverage_for_link(
        coverage_rows,
        provider_row=provider_row,
        static_source_id=static_source_id,
    )
    return {
        "providerBucket": bucket,
        "providerSourceId": provider_id,
        "providerSourceName": _clean_text(provider_row.get("name")) or provider_id,
        "providerAdapter": _clean_text(provider_row.get("adapter")),
        "staticSourceId": static_source_id,
        "selectedStaticSourceId": static_source_id,
        "staticSourceName": static_name or static_source_id,
        "selectedStaticSourceName": static_name or static_source_id,
        "migrationSourceIdentity": static_source_id,
        "migrationSourceName": static_name,
        "migrationLinkedBy": linked_by,
        "adminBackfillOwned": linked_by == ADMIN_MIGRATION_LINK_ACTOR,
        "providerCoverageStatus": _clean_text(coverage.get("providerCoverageStatus")),
        "providerCoverageConsecutiveSuccesses": int(
            coverage.get("providerCoverageConsecutiveSuccesses") or 0
        ),
        "providerCoverageLatestKeptCount": int(
            coverage.get("providerCoverageLatestKeptCount") or 0
        ),
        "providerReplacementReadiness": _clean_text(coverage.get("providerReplacementReadiness")),
        "recommendedAction": "already_linked",
    }


def _linked_candidate_from_soak_row(
    state: dict[str, list[dict[str, Any]]],
    coverage_rows: list[dict[str, Any]],
    row: dict[str, Any],
) -> dict[str, Any] | None:
    provider_id = _clean_text(row.get("providerSourceId"))
    static_source_id = _clean_text(row.get("staticSourceId")) or _clean_text(
        row.get("migrationSourceIdentity")
    )
    if not provider_id or not static_source_id:
        return None
    match = _find_state_row_by_id(state, provider_id)
    bucket = ""
    provider_row: dict[str, Any] = {}
    if match:
        bucket, provider_row = match
    linked_by = _clean_text(provider_row.get("migrationLinkedBy"))
    current_static_id = _clean_text(provider_row.get("migrationSourceIdentity"))
    admin_owned = bool(
        current_static_id == static_source_id and linked_by == ADMIN_MIGRATION_LINK_ACTOR
    )
    coverage = _provider_coverage_for_link(
        coverage_rows,
        provider_row=provider_row,
        linked_row=row,
        static_source_id=static_source_id,
    )
    static_name = (
        _clean_text(provider_row.get("migrationSourceName"))
        or _clean_text(row.get("staticSourceName"))
        or _find_static_row_name(state, static_source_id)
    )
    return {
        "providerBucket": bucket,
        "providerSourceId": provider_id,
        "providerSourceName": _clean_text(row.get("providerSourceName"))
        or _clean_text(provider_row.get("name"))
        or provider_id,
        "providerAdapter": _clean_text(row.get("providerAdapter"))
        or _clean_text(provider_row.get("adapter")),
        "staticSourceId": static_source_id,
        "selectedStaticSourceId": static_source_id,
        "staticSourceName": static_name or static_source_id,
        "selectedStaticSourceName": static_name or static_source_id,
        "migrationSourceIdentity": current_static_id or static_source_id,
        "migrationSourceName": _clean_text(provider_row.get("migrationSourceName")) or static_name,
        "migrationLinkedBy": linked_by,
        "adminBackfillOwned": admin_owned,
        "providerCoverageStatus": _clean_text(coverage.get("providerCoverageStatus")),
        "providerCoverageConsecutiveSuccesses": int(
            coverage.get("providerCoverageConsecutiveSuccesses") or 0
        ),
        "providerCoverageLatestKeptCount": int(
            coverage.get("providerCoverageLatestKeptCount") or 0
        ),
        "providerReplacementReadiness": _clean_text(coverage.get("providerReplacementReadiness")),
        "recommendedAction": "already_linked",
    }


def _linked_candidate_key(row: dict[str, Any]) -> str:
    return "|".join(
        (
            _clean_text(row.get("providerSourceId")).lower(),
            _clean_text(row.get("staticSourceId") or row.get("migrationSourceIdentity")).lower(),
        )
    )


def _provider_link_state(
    state: dict[str, list[dict[str, Any]]], provider_id: str
) -> dict[str, Any]:
    match = _find_state_row_by_id(state, provider_id)
    if not match:
        return {
            "providerBucket": "",
            "migrationSourceIdentity": "",
            "migrationLinkedBy": "",
            "adminBackfillOwned": False,
        }
    bucket, provider_row = match
    migration_source_identity = _clean_text(provider_row.get("migrationSourceIdentity"))
    migration_linked_by = _clean_text(provider_row.get("migrationLinkedBy"))
    return {
        "providerBucket": bucket,
        "migrationSourceIdentity": migration_source_identity,
        "migrationLinkedBy": migration_linked_by,
        "adminBackfillOwned": bool(
            migration_source_identity and migration_linked_by == ADMIN_MIGRATION_LINK_ACTOR
        ),
    }


def _enrich_review_candidates(
    state: dict[str, list[dict[str, Any]]], payload: dict[str, Any]
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for row in _as_list(payload.get("reviewCandidates")):
        if not isinstance(row, dict):
            continue
        candidate = dict(row)
        candidate["currentProviderLinkState"] = _provider_link_state(
            state, _clean_text(candidate.get("providerSourceId"))
        )
        candidates.append(candidate)
    return candidates


def _registry_linked_candidates(
    api: BridgeApi,
    state: dict[str, list[dict[str, Any]]],
    coverage_rows: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    linked_candidates_by_key: dict[str, dict[str, Any]] = {}
    for bucket in ("active", "pending"):
        for provider_row in state.get(bucket) or []:
            if not isinstance(provider_row, dict):
                continue
            linked_candidate = _linked_candidate_from_provider_row(
                api,
                state,
                coverage_rows,
                bucket=bucket,
                provider_row=provider_row,
            )
            if not linked_candidate:
                continue
            key = _linked_candidate_key(linked_candidate)
            if key:
                linked_candidates_by_key[key] = linked_candidate
    return linked_candidates_by_key


def _merge_soak_linked_candidates(
    state: dict[str, list[dict[str, Any]]],
    payload: dict[str, Any],
    coverage_rows: list[dict[str, Any]],
    linked_candidates_by_key: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    for row in _as_list(payload.get("linkedCandidates")):
        if not isinstance(row, dict):
            continue
        linked_candidate = _linked_candidate_from_soak_row(state, coverage_rows, row)
        if not linked_candidate:
            continue
        key = _linked_candidate_key(linked_candidate)
        if key and key not in linked_candidates_by_key:
            linked_candidates_by_key[key] = linked_candidate
    return list(linked_candidates_by_key.values())


def _enrich_link_backfill_review_candidates(
    api: BridgeApi, payload: dict[str, Any]
) -> dict[str, Any]:
    state = api.load_state() or {}
    coverage_rows = _provider_coverage_rows(api)
    enriched = dict(payload)
    enriched["reviewCandidates"] = _enrich_review_candidates(state, payload)
    enriched["linkedCandidates"] = _merge_soak_linked_candidates(
        state,
        payload,
        coverage_rows,
        _registry_linked_candidates(api, state, coverage_rows),
    )
    return enriched


def _source_match_tokens(row: dict[str, Any]) -> set[str]:
    tokens: set[str] = set()
    for key in (
        "id",
        "sourceId",
        "url",
        "sourceUrl",
        "source_url",
        "listingUrl",
        "listing_url",
        "careersUrl",
        "careers_url",
        "feed_url",
        "board_url",
    ):
        value = str(row.get(key) or "").strip().lower().rstrip("/")
        if value:
            tokens.add(f"{key.lower()}:{value}")
            if key.endswith("url") or key.endswith("_url") or key in {"url", "sourceUrl"}:
                tokens.add(f"url:{value}")
    name = str(row.get("name") or "").strip().lower()
    studio = str(row.get("studio") or "").strip().lower()
    adapter = str(row.get("adapter") or "").strip().lower()
    if name and adapter:
        tokens.add(f"name_adapter:{name}|{adapter}")
    if studio and adapter:
        tokens.add(f"studio_adapter:{studio}|{adapter}")
    return tokens


def _read_discovery_candidate_rows(api: BridgeApi) -> list[dict[str, Any]]:
    candidates_path = getattr(api, "DISCOVERY_CANDIDATES_PATH", None)
    if candidates_path is None:
        return []
    return load_runtime_evidence_array(candidates_path, [])


def _overlay_discovery_candidate_fields(
    rows: list[dict[str, Any]],
    candidates: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    by_token: dict[str, dict[str, Any]] = {}
    for candidate in candidates:
        for token in _source_match_tokens(candidate):
            by_token.setdefault(token, candidate)

    evidence_fields = (
        "jobsFound",
        "sampleCount",
        "status",
        "lastProbeError",
        "error",
        "lastProbedAt",
        "deferred",
        "pendingReason",
        "deferReason",
        "quarantineReason",
        "weakSignal",
        "candidateState",
        "confidence",
        "rankScore",
        "rankReasons",
    )
    out: list[dict[str, Any]] = []
    for row in rows:
        match = next(
            (by_token[token] for token in _source_match_tokens(row) if token in by_token), None
        )
        if not match:
            out.append(row)
            continue
        merged = dict(row)
        for field in evidence_fields:
            if field in match:
                merged[field] = match[field]
        out.append(merged)
    return out


def _normalize_pending_discovery_job_counts(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in rows:
        if row.get("jobsFound") is not None or row.get("sampleCount") is not None:
            normalized.append(row)
            continue
        updated = dict(row)
        updated["jobsFound"] = 0
        updated["sampleCount"] = 0
        normalized.append(updated)
    return normalized


def _include_hidden_registry_rows(query: dict[str, list[str]]) -> bool:
    return str((query.get("includeHidden") or [""])[0] or "").strip().lower() in {
        "1",
        "true",
        "yes",
    }


def _pending_registry_payload(api: BridgeApi, query: dict[str, list[str]]) -> dict[str, Any]:
    state = api.load_state()
    return _pending_registry_payload_from_state(api, query, state)


def _pending_registry_payload_from_state(
    api: BridgeApi,
    query: dict[str, list[str]],
    state: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    pending_rows = _normalize_pending_discovery_job_counts(
        _overlay_discovery_candidate_fields(
            state.get("pending") or [],
            _read_discovery_candidate_rows(api),
        )
    )
    hidden_pending_count = sum(1 for row in pending_rows if is_hidden_from_default(row))
    if not _include_hidden_registry_rows(query):
        pending_rows = [row for row in pending_rows if not is_hidden_from_default(row)]
    summary = api.summarize_state(state)
    summary["hiddenPendingCount"] = hidden_pending_count
    return {"sources": pending_rows, "summary": summary}


def _include_hidden_pending_registry_rows(query: dict[str, list[str]]) -> bool:
    return str((query.get("includeHiddenPending") or [""])[0] or "").strip().lower() in {
        "1",
        "true",
        "yes",
    }


def _requested_registry_source_buckets(query: dict[str, list[str]]) -> tuple[list[str], str]:
    raw = str((query.get("buckets") or ["pending,active,rejected"])[0] or "").strip()
    buckets = [item.strip().lower() for item in raw.split(",") if item.strip()]
    if not buckets:
        buckets = ["pending", "active", "rejected"]
    allowed = {"pending", "active", "rejected"}
    invalid = [bucket for bucket in buckets if bucket not in allowed]
    if invalid:
        return [], ", ".join(sorted(set(invalid)))
    deduped: list[str] = []
    for bucket in buckets:
        if bucket not in deduped:
            deduped.append(bucket)
    return deduped, ""


def _registry_authority_mode_from_summary(summary: dict[str, Any]) -> str:
    explicit = str(summary.get("authorityMode") or "").strip().lower()
    if explicit:
        return explicit
    reason = str(summary.get("reason") or "").strip().lower()
    if reason.startswith("sqlite"):
        return "sqlite"
    if reason.startswith("shadow"):
        return "shadow"
    if reason.startswith("json"):
        return "json"
    return ""


def _registry_summary_route_payload(
    api: BridgeApi, query: dict[str, list[str]]
) -> tuple[int, dict[str, Any]]:
    view = str((query.get("view") or [""])[0] or "").strip().lower()
    if view not in ("", "cheap", "storage", "exact"):
        return 400, {
            "ok": False,
            "error": "invalid registry summary view",
            "invalidView": view,
            "allowedViews": ["storage", "exact"],
        }
    started_at = time.perf_counter()
    failed = False
    try:
        if view == "exact":
            summary = _as_dict(api.get_registry_exact_summary_payload())
        else:
            summary = _as_dict(api.get_registry_summary_payload())
    except Exception:
        failed = True
        raise
    finally:
        _record_storage_read_metric(
            api,
            surface="registry.summaryExact" if view == "exact" else "registry.summary",
            artifact="source-registry",
            storage_kind="normalized" if view == "exact" else "storage",
            started_at=started_at,
            failed=failed,
        )
    generated_at = str(
        summary.get("updatedAt") or summary.get("publishedAt") or datetime.now(UTC).isoformat()
    )
    return 200, {
        "ok": True,
        "summary": summary,
        "authorityMode": _registry_authority_mode_from_summary(summary),
        "generatedAt": generated_at,
    }


def _registry_sources_payload(
    api: BridgeApi, query: dict[str, list[str]]
) -> tuple[int, dict[str, Any]]:
    buckets, invalid = _requested_registry_source_buckets(query)
    if invalid:
        return 400, {
            "ok": False,
            "error": "invalid registry bucket",
            "invalidBuckets": invalid,
        }
    started_at = time.perf_counter()
    failed = False
    row_count = 0
    storage_kind = "normalized"
    try:
        state = api.load_state()
        summary = {
            **api.summarize_state(state),
            "summaryExact": True,
            "countBasis": "normalized",
        }
        sources: dict[str, list[dict[str, Any]]] = {}
        if "pending" in buckets:
            pending_query = dict(query)
            pending_query["includeHidden"] = [
                "1" if _include_hidden_pending_registry_rows(query) else "0"
            ]
            pending_payload = _pending_registry_payload_from_state(api, pending_query, state)
            sources["pending"] = [
                dict(row)
                for row in _as_list(pending_payload.get("sources"))
                if isinstance(row, dict)
            ]
            summary = {**summary, **_as_dict(pending_payload.get("summary"))}
        if "active" in buckets:
            sources["active"] = [
                dict(row) for row in state.get("active") or [] if isinstance(row, dict)
            ]
        if "rejected" in buckets:
            sources["rejected"] = [
                dict(row) for row in state.get("rejected") or [] if isinstance(row, dict)
            ]
        row_count = sum(len(rows) for rows in sources.values())
        storage_kind = str(summary.get("authorityMode") or "normalized")
        return 200, {"ok": True, "sources": sources, "summary": summary}
    except Exception:
        failed = True
        raise
    finally:
        _record_storage_read_metric(
            api,
            surface="registry.sources",
            artifact="source-registry",
            storage_kind=storage_kind,
            started_at=started_at,
            row_count=row_count,
            failed=failed,
        )


def _handle_registry_routes(
    handler: BridgeResponseWriter,
    *,
    api: BridgeApi,
    path: str,
    query: dict[str, list[str]],
) -> bool:
    if path == "/registry/active":
        started_at = time.perf_counter()
        state = api.load_state()
        _record_storage_read_metric(
            api,
            surface="registry.active",
            artifact="source-registry",
            storage_kind="normalized",
            started_at=started_at,
            row_count=len(state.get("active") or []),
        )
        handler.send_json({"sources": state["active"], "summary": api.summarize_state(state)})
        return True

    if path == "/registry/pending":
        started_at = time.perf_counter()
        payload = _pending_registry_payload(api, query)
        _record_storage_read_metric(
            api,
            surface="registry.pending",
            artifact="source-registry",
            storage_kind="normalized",
            started_at=started_at,
            row_count=len(_as_list(payload.get("sources"))),
        )
        handler.send_json(payload)
        return True

    if path == "/registry/rejected":
        started_at = time.perf_counter()
        state = api.load_state()
        _record_storage_read_metric(
            api,
            surface="registry.rejected",
            artifact="source-registry",
            storage_kind="normalized",
            started_at=started_at,
            row_count=len(state.get("rejected") or []),
        )
        handler.send_json({"sources": state["rejected"], "summary": api.summarize_state(state)})
        return True

    if path == "/registry/sources":
        with time_operation("registry.sources"):
            status, payload = _registry_sources_payload(api, query)
        handler.send_json(payload, status=status)
        return True

    if path == "/registry/summary":
        view = str((query.get("view") or [""])[0] or "").strip().lower()
        operation_name = "registry.summary.exact" if view == "exact" else "registry.summary.storage"
        with time_operation(operation_name):
            status, payload = _registry_summary_route_payload(api, query)
        handler.send_json(payload, status=status)
        return True

    return False


def _handle_ops_health_route(
    handler: BridgeResponseWriter,
    *,
    api: BridgeApi,
    query: dict[str, list[str]],
) -> bool:
    view = str((query.get("view") or ["full"])[0] or "full").strip().lower()
    if view not in {"", "full", "ready"}:
        handler.send_json(
            {"ok": False, "error": f"unsupported ops health view: {view}"},
            status=400,
        )
        return True
    op_label = "ops.health.ready.route_payload" if view == "ready" else "ops.health.route_payload"
    with time_operation(op_label):
        payload = api.compute_ops_health_ready() if view == "ready" else api.compute_ops_health()
    handler.send_json(payload)
    return True


def _handle_ops_dashboard_health_route(
    handler: BridgeResponseWriter,
    *,
    api: BridgeApi,
    query: dict[str, list[str]],
) -> bool:
    view = str((query.get("view") or ["full"])[0] or "full").strip().lower()
    if view not in {"", "full", "summary"}:
        handler.send_json(
            {"ok": False, "error": f"unsupported dashboard-health view: {view}"},
            status=400,
        )
        return True
    dashboard_health_fn = (
        getattr(api, "compute_ops_dashboard_health_summary", None)
        if view == "summary"
        else getattr(api, "compute_ops_dashboard_health", None)
    )
    op_label = (
        "ops.dashboard_health.summary.route_payload"
        if view == "summary"
        else "ops.dashboard_health.route_payload"
    )
    with time_operation(op_label):
        payload = (
            dashboard_health_fn() if callable(dashboard_health_fn) else api.compute_ops_health()
        )
    handler.send_json(payload)
    return True


def _handle_ops_status_routes(
    handler: BridgeResponseWriter,
    *,
    api: BridgeApi,
    path: str,
    query: dict[str, list[str]],
) -> bool:
    if path == "/ops/health":
        return _handle_ops_health_route(handler, api=api, query=query)

    if path == "/ops/dashboard-health":
        return _handle_ops_dashboard_health_route(handler, api=api, query=query)

    if path == "/ops/history":
        limit_raw = (query.get("limit") or ["30"])[0]
        try:
            limit = max(1, min(200, int(limit_raw)))
        except ValueError:
            limit = 30
        rows = list(api.get_lifecycle_run_history_rows() or [])
        handler.send_json({"runs": rows[-limit:], "count": len(rows)})
        return True

    if path == "/ops/task-state":
        view = str((query.get("view") or [""])[0] or "").strip().lower()
        if view == "summary":
            with time_operation("ops.task_state.summary"):
                payload = api.get_current_task_state_summary_payload()
        else:
            with time_operation("ops.task_state.full"):
                payload = api.get_current_task_state_payload()
        handler.send_json(payload)
        return True

    if path.startswith("/ops/task-live/"):
        task_type = path.removeprefix("/ops/task-live/").strip().lower()
        if task_type not in {"fetch", "discovery", "sync"}:
            handler.send_json(
                {"ok": False, "error": f"unsupported task type: {task_type or 'unknown'}"},
                status=404,
            )
            return True
        handler.send_json(api.get_task_live_payload(task_type))
        return True

    return False


def _handle_ops_diagnostic_routes(
    handler: BridgeResponseWriter,
    *,
    api: BridgeApi,
    path: str,
    query: dict[str, list[str]],
) -> bool:
    if path == "/ops/fetcher-metrics":
        window_raw = (query.get("windowRuns") or ["20"])[0]
        try:
            window_runs = max(1, min(200, int(window_raw)))
        except ValueError:
            window_runs = 20
        handler.send_json(api.compute_fetcher_metrics(window_runs=window_runs))
        return True

    if path == "/ops/perf-counters":
        handler.send_json({"ok": True, "counters": snapshot_counters()})
        return True

    if path == "/ops/performance-profile":
        handler.send_json(snapshot_performance_profile(runtime=_performance_profile_runtime(api)))
        return True

    if path == "/ops/storage-metrics":
        handler.send_json(
            {
                "ok": True,
                "storageMetrics": snapshot_storage_metrics(_storage_metrics_data_dir(api)),
                "routeCounters": snapshot_counters(),
            }
        )
        return True

    if path == "/ops/storage-health":
        handler.send_json(api.get_storage_health_payload())
        return True

    if path == "/ops/discovery-audit-artifacts":
        handler.send_json(get_discovery_audit_artifacts_payload(api))
        return True

    if path == "/ops/task-failure-attempts":
        handler.send_json(get_task_failure_attempts_payload(api))
        return True

    if path == "/ops/fetch-report/sources":
        handler.send_json(_fetch_report_sources_payload(api, query))
        return True

    return False


def _read_utf8_log_text(path: Path) -> str:
    try:
        raw = path.read_bytes()
    except OSError:
        return ""
    if not raw:
        return ""
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        if exc.end == len(raw) and exc.reason == "unexpected end of data":
            return raw[: exc.start].decode("utf-8")
        return raw.decode("utf-8", errors="replace")


def _json_error(exc: Exception) -> dict[str, Any]:
    return {"ok": False, "error": str(exc)}


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
    api: BridgeApi,
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
        if callable(reconciler):
            reconciler()

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

        payload = (
            _discovery_report_summary_payload(_as_dict(report))
            if view == "summary"
            else (_as_dict(report) or {"summary": {}, "candidates": [], "failures": []})
        )
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


def handle_get(
    handler: BridgeResponseWriter, *, api: BridgeApi, path: str, query: dict[str, list[str]]
) -> bool:
    """Handle GET routes for the admin bridge.

    Important: `api` must be the currently running BridgeApi instance.
    """

    if path == "/discovery/report":
        return _handle_discovery_report_route(handler, api=api, query=query)

    if path == "/discovery/candidates":
        candidates_path = getattr(api, "DISCOVERY_CANDIDATES_PATH", None)

        def _payload() -> dict[str, Any]:
            if candidates_path is None:
                return {"candidates": [], "count": 0}
            else:
                candidates = load_runtime_evidence_array(candidates_path, [])
                return {"candidates": candidates, "count": len(candidates)}

        def _error(exc: Exception) -> dict[str, Any]:
            safe_bridge_log(api, "error", "discovery_candidates_route_failed", error=str(exc))
            return {"error": "failed_to_load_discovery_candidates", "detail": str(exc)}

        send_json_boundary(handler, _payload, error_status=500, error_payload=_error)
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

    if _handle_registry_routes(handler, api=api, path=path, query=query):
        return True

    if path == "/discovery/log":
        offset_raw = (query.get("offset") or ["0"])[0]
        try:
            offset = max(0, int(offset_raw))
        except ValueError:
            offset = 0
        text = _read_utf8_log_text(api.DISCOVERY_LOG_PATH)
        chunk = text[offset:]
        next_offset = len(text)
        handler.send_json(
            {"text": chunk, "offset": offset, "nextOffset": next_offset, "hasMore": False}
        )
        return True

    if path == "/fetcher/log":
        offset_raw = (query.get("offset") or ["0"])[0]
        try:
            offset = max(0, int(offset_raw))
        except ValueError:
            offset = 0
        text = _read_utf8_log_text(api.FETCHER_LOG_PATH)
        chunk = text[offset:]
        next_offset = len(text)
        handler.send_json(
            {"text": chunk, "offset": offset, "nextOffset": next_offset, "hasMore": False}
        )
        return True

    if _handle_ops_status_routes(handler, api=api, path=path, query=query):
        return True

    if path == "/discovery/config":
        handler.send_json(api.get_discovery_config_payload())
        return True

    if _handle_ops_diagnostic_routes(handler, api=api, path=path, query=query):
        return True

    if path == "/ops/fetch-report":
        view = str((query.get("view") or [""])[0] or "").strip().lower()
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
                payload = _hydrate_fetch_report_sources_from_sqlite(api, payload)
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
            _record_storage_read_metric(
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
        link_backfill, link_backfill_warning = _load_provider_coverage_link_backfill(api)
        suppression_eligibility, suppression_eligibility_warning = _load_suppression_eligibility(
            api
        )
        link_backfill = _enrich_link_backfill_review_candidates(api, link_backfill)
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
            payload = (
                _sync_status_summary_payload(
                    {
                        "ok": True,
                        "config": api.sync_config_status(),
                        "savedConfig": {},
                        "runtime": {},
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
