from __future__ import annotations

import io
import json
import logging
import re
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pydantic import ValidationError as PydanticValidationError

from src.bridge.api import BridgeApi
from src.bridge.routes.error_boundary import (
    run_route_boundary,
    safe_bridge_log,
    send_json_boundary,
)
from src.bridge.routes.response_writer import BridgeResponseWriter
from src.bridge.source_policy_migration_links import ADMIN_MIGRATION_LINK_ACTOR
from src.core.schemas import LocalSavedJobRowSchema
from src.jobs.common.contracts_source_policy_recommendations import (
    merge_source_policy_review_state_into_recommendations,
    read_source_policy_recommendations_artifact,
)
from src.jobs.common.contracts_source_policy_review_state import (
    read_source_policy_review_state_artifact,
)
from src.source_registry import is_hidden_from_default

logger = logging.getLogger(__name__)


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


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


def _load_provider_coverage_link_backfill(api: BridgeApi) -> tuple[dict[str, Any], str]:
    path = _source_policy_soak_report_path(api)
    empty_payload = {
        "reviewCandidates": [],
        "candidateLinkCount": 0,
        "highConfidenceLinkCount": 0,
        "mediumConfidenceLinkCount": 0,
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
            "highConfidenceLinkCount",
            "mediumConfidenceLinkCount",
            "ambiguousProviderCount",
            "ambiguousStaticCandidateCount",
            "resolvedBySourceStateCount",
            "resolvedByAdvisoryIdentityCount",
            "unresolvedAmbiguousCount",
        )
        if key in section
    }
    result["reviewCandidates"] = [
        dict(row) for row in _as_list(section.get("reviewCandidates")) if isinstance(row, dict)
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


def _enrich_link_backfill_review_candidates(
    api: BridgeApi, payload: dict[str, Any]
) -> dict[str, Any]:
    state = api.load_state() or {}
    enriched = dict(payload)
    candidates: list[dict[str, Any]] = []
    for row in _as_list(payload.get("reviewCandidates")):
        if not isinstance(row, dict):
            continue
        candidate = dict(row)
        provider_id = _clean_text(candidate.get("providerSourceId"))
        match = _find_state_row_by_id(state, provider_id)
        if match:
            bucket, provider_row = match
            migration_source_identity = _clean_text(provider_row.get("migrationSourceIdentity"))
            migration_linked_by = _clean_text(provider_row.get("migrationLinkedBy"))
            admin_owned = bool(
                migration_source_identity and migration_linked_by == ADMIN_MIGRATION_LINK_ACTOR
            )
            link_state = {
                "providerBucket": bucket,
                "migrationSourceIdentity": migration_source_identity,
                "migrationLinkedBy": migration_linked_by,
                "adminBackfillOwned": admin_owned,
            }
        else:
            link_state = {
                "providerBucket": "",
                "migrationSourceIdentity": "",
                "migrationLinkedBy": "",
                "adminBackfillOwned": False,
            }
        candidate["currentProviderLinkState"] = link_state
        candidates.append(candidate)
    enriched["reviewCandidates"] = candidates
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
    try:
        raw = json.loads(Path(candidates_path).read_text(encoding="utf-8"))
    except FileNotFoundError:
        return []
    rows = _as_list(raw)
    return [row for row in rows if isinstance(row, dict)]


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
    pending_rows = _normalize_pending_discovery_job_counts(
        _overlay_discovery_candidate_fields(
            state["pending"],
            _read_discovery_candidate_rows(api),
        )
    )
    hidden_pending_count = sum(1 for row in pending_rows if is_hidden_from_default(row))
    if not _include_hidden_registry_rows(query):
        pending_rows = [row for row in pending_rows if not is_hidden_from_default(row)]
    summary = api.summarize_state(state)
    summary["hiddenPendingCount"] = hidden_pending_count
    return {"sources": pending_rows, "summary": summary}


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


def handle_get(
    handler: BridgeResponseWriter, *, api: BridgeApi, path: str, query: dict[str, list[str]]
) -> bool:
    """Handle GET routes for the admin bridge.

    Important: `api` must be the currently running BridgeApi instance.
    """

    if path == "/discovery/report":
        # This route must never "silently" drop the connection; the admin UI
        # treats network errors as bridge-availability failures.
        def _send_discovery_report() -> None:
            load_fn = getattr(api, "load_json_object", None)
            raw = (
                load_fn(getattr(api, "DISCOVERY_REPORT_PATH", None), {})
                if callable(load_fn)
                else {}
            )

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

    if path == "/discovery/candidates":
        candidates_path = getattr(api, "DISCOVERY_CANDIDATES_PATH", None)

        def _payload() -> dict[str, Any]:
            if candidates_path is None:
                return {"candidates": [], "count": 0}
            else:
                try:
                    raw = json.loads(Path(candidates_path).read_text(encoding="utf-8"))
                except FileNotFoundError:
                    raw = []
                candidates = [row for row in _as_list(raw) if isinstance(row, dict)]
                return {"candidates": candidates, "count": len(candidates)}

        def _error(exc: Exception) -> dict[str, Any]:
            safe_bridge_log(api, "error", "discovery_candidates_route_failed", error=str(exc))
            return {"error": "failed_to_load_discovery_candidates", "detail": str(exc)}

        send_json_boundary(handler, _payload, error_status=500, error_payload=_error)
        return True

    if path == "/desktop-local-data/session":

        def _payload() -> dict[str, Any]:
            desktop_session = api.get_desktop_session_payload()
            return {
                "ok": True,
                "user": api.desktop_local_data_store().get_current_user(),
                "lastActivityAt": str(api.DESKTOP_SESSION_ACTIVITY_AT or ""),
                "desktopSession": desktop_session,
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
        send_json_boundary(
            handler,
            api.get_update_status_payload,
            error_status=500,
            error_payload=_json_error,
        )
        return True

    if path == "/registry/active":
        state = api.load_state()
        handler.send_json({"sources": state["active"], "summary": api.summarize_state(state)})
        return True

    if path == "/registry/pending":
        handler.send_json(_pending_registry_payload(api, query))
        return True

    if path == "/registry/rejected":
        state = api.load_state()
        handler.send_json({"sources": state["rejected"], "summary": api.summarize_state(state)})
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

    if path == "/registry/summary":
        state = api.load_state()
        handler.send_json({"summary": api.summarize_state(state)})
        return True

    if path == "/ops/health":
        handler.send_json(api.compute_ops_health())
        return True

    if path == "/ops/history":
        limit_raw = (query.get("limit") or ["30"])[0]
        try:
            limit = max(1, min(200, int(limit_raw)))
        except ValueError:
            limit = 30
        projection_fn = getattr(api, "get_projected_run_history", None)
        if callable(projection_fn):
            projection = projection_fn()
            rows = list(getattr(projection, "rows", []) or [])
        else:
            rows = api.sync_history_from_reports()
        handler.send_json({"runs": rows[-limit:], "count": len(rows)})
        return True

    if path == "/discovery/config":
        handler.send_json(api.get_discovery_config_payload())
        return True

    if path == "/ops/task-state":
        handler.send_json(api.get_current_task_state_payload())
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

    if path == "/ops/fetcher-metrics":
        window_raw = (query.get("windowRuns") or ["20"])[0]
        try:
            window_runs = max(1, min(200, int(window_raw)))
        except ValueError:
            window_runs = 20
        handler.send_json(api.compute_fetcher_metrics(window_runs=window_runs))
        return True

    if path == "/ops/fetch-report":
        view = str((query.get("view") or [""])[0] or "").strip().lower()
        payload = api.normalize_fetch_report_contract(
            api.load_json_object(api.JOBS_FETCH_REPORT_PATH, {})
        )
        if view == "live" and isinstance(payload, dict):
            payload = _compact_live_fetch_report_payload(payload)
        handler.send_json(payload)
        return True

    if path == "/source-policy/recommendations":
        recommendations, recommendation_warning = read_source_policy_recommendations_artifact(
            api.SOURCE_POLICY_RECOMMENDATIONS_PATH
        )
        review_state, review_state_warning = read_source_policy_review_state_artifact(
            api.SOURCE_POLICY_REVIEW_STATE_PATH
        )
        link_backfill, link_backfill_warning = _load_provider_coverage_link_backfill(api)
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
                "warnings": [
                    warning
                    for warning in (
                        recommendation_warning,
                        review_state_warning,
                        link_backfill_warning,
                    )
                    if warning
                ],
            }
        )
        return True

    if path == "/sync/status":
        handler.send_json(api.get_sync_status_payload())
        return True

    if path == "/tasks/run-jobs-pipeline-status":
        handler.send_json(api.get_jobs_pipeline_status_payload())
        return True

    return False
