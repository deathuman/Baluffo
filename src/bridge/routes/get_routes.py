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
from src.bridge.routes.response_writer import BridgeResponseWriter
from src.core.schemas import LocalSavedJobRowSchema
from src.source_registry import is_hidden_from_default

logger = logging.getLogger(__name__)


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _compact_live_fetch_report_payload(payload: dict[str, Any]) -> dict[str, Any]:
    compact_payload = dict(payload or {})
    sources = _as_list(payload.get("sources"))
    compact_payload["sources"] = [
        {key: value for key, value in row.items() if key != "details"}
        for row in sources
        if isinstance(row, dict)
    ]
    return compact_payload


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


def handle_get(
    handler: BridgeResponseWriter, *, api: BridgeApi, path: str, query: dict[str, list[str]]
) -> bool:
    """Handle GET routes for the admin bridge.

    Important: `api` must be the currently running BridgeApi instance.
    """

    if path == "/discovery/report":
        # This route must never "silently" drop the connection; the admin UI
        # treats network errors as bridge-availability failures.
        try:
            load_fn = getattr(api, "load_json_object", None)
            raw = (
                load_fn(getattr(api, "DISCOVERY_REPORT_PATH", None), {})
                if callable(load_fn)
                else {}
            )

            normalizer_fn = getattr(api, "normalize_discovery_report_contract", None)
            report = normalizer_fn(raw) if callable(normalizer_fn) else raw

            try:
                api.bridge_log(
                    "info",
                    "discovery_report_route_sending",
                    reportType=type(report).__name__,
                    summaryType=type((report or {}).get("summary", None)).__name__
                    if isinstance(report, dict)
                    else "",
                )
            except Exception:  # noqa: BLE001
                pass

            payload = _as_dict(report) or {"summary": {}, "candidates": [], "failures": []}
            # Prefer the bytes-writing helper to bypass any unexpected issues
            # in JSON response serialization for edge-case payloads.
            if hasattr(handler, "send_bytes"):
                try:
                    text = json.dumps(payload, ensure_ascii=False, default=str)
                    body = text.encode("utf-8")
                except UnicodeEncodeError:
                    text = json.dumps(payload, ensure_ascii=True, default=str)
                    body = text.encode("utf-8")
                handler.send_bytes(
                    body,
                    content_type="application/json; charset=utf-8",
                    status=200,
                )
            else:
                handler.send_json(payload)
        except Exception as exc:  # noqa: BLE001
            try:
                api.bridge_log("error", "discovery_report_route_failed", error=str(exc))
            except Exception:  # noqa: BLE001
                pass
            payload = {"error": "failed_to_load_discovery_report", "detail": str(exc)}
            if hasattr(handler, "send_bytes"):
                try:
                    text = json.dumps(payload, ensure_ascii=False, default=str)
                    body = text.encode("utf-8")
                except UnicodeEncodeError:
                    text = json.dumps(payload, ensure_ascii=True, default=str)
                    body = text.encode("utf-8")
                handler.send_bytes(
                    body,
                    content_type="application/json; charset=utf-8",
                    status=500,
                )
            else:
                handler.send_json(payload, status=500)
        return True

    if path == "/discovery/candidates":
        candidates_path = getattr(api, "DISCOVERY_CANDIDATES_PATH", None)
        try:
            if candidates_path is None:
                payload = {"candidates": [], "count": 0}
            else:
                try:
                    raw = json.loads(Path(candidates_path).read_text(encoding="utf-8"))
                except FileNotFoundError:
                    raw = []
                candidates = [row for row in _as_list(raw) if isinstance(row, dict)]
                payload = {"candidates": candidates, "count": len(candidates)}
            handler.send_json(payload)
        except Exception as exc:  # noqa: BLE001
            try:
                api.bridge_log("error", "discovery_candidates_route_failed", error=str(exc))
            except Exception:  # noqa: BLE001
                pass
            handler.send_json(
                {"error": "failed_to_load_discovery_candidates", "detail": str(exc)},
                status=500,
            )
        return True

    if path == "/desktop-local-data/session":
        try:
            desktop_session = api.get_desktop_session_payload()
            handler.send_json(
                {
                    "ok": True,
                    "user": api.desktop_local_data_store().get_current_user(),
                    "lastActivityAt": str(api.DESKTOP_SESSION_ACTIVITY_AT or ""),
                    "desktopSession": desktop_session,
                }
            )
        except Exception as exc:  # noqa: BLE001
            handler.send_json({"ok": False, "error": str(exc)}, status=400)
        return True

    if path == "/desktop-local-data/profiles":
        try:
            handler.send_json(
                {"ok": True, "profiles": api.desktop_local_data_store().list_profiles()}
            )
        except Exception as exc:  # noqa: BLE001
            handler.send_json({"ok": False, "error": str(exc)}, status=400)
        return True

    if path == "/desktop-local-data/saved-jobs":
        try:
            uid = (query.get("uid") or [""])[0]
            raw_rows = api.desktop_local_data_store().list_saved_jobs(uid)
            rows = []
            for row in raw_rows:
                try:
                    LocalSavedJobRowSchema.model_validate(row)
                    rows.append(row)
                except PydanticValidationError as exc:
                    logger.warning("Saved job row validation failed, skipping: %s", exc)
            handler.send_json({"ok": True, "rows": rows})
        except Exception as exc:  # noqa: BLE001
            handler.send_json({"ok": False, "error": str(exc)}, status=400)
        return True

    if path == "/desktop-local-data/saved-job-keys":
        try:
            uid = (query.get("uid") or [""])[0]
            handler.send_json(
                {"ok": True, "keys": api.desktop_local_data_store().get_saved_job_keys(uid)}
            )
        except Exception as exc:  # noqa: BLE001
            handler.send_json({"ok": False, "error": str(exc)}, status=400)
        return True

    if path == "/desktop-local-data/attachments":
        try:
            uid = (query.get("uid") or [""])[0]
            job_key = (query.get("jobKey") or [""])[0]
            handler.send_json(
                {
                    "ok": True,
                    "rows": api.desktop_local_data_store().list_attachments_for_job(uid, job_key),
                }
            )
        except Exception as exc:  # noqa: BLE001
            handler.send_json({"ok": False, "error": str(exc)}, status=400)
        return True

    if path == "/desktop-local-data/attachments/content":
        try:
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
        except Exception as exc:  # noqa: BLE001
            handler.send_json({"ok": False, "error": str(exc)}, status=400)
        return True

    if path == "/desktop-local-data/backup/export-file":
        try:
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
        except Exception as exc:  # noqa: BLE001
            handler.send_json({"ok": False, "error": str(exc)}, status=400)
        return True

    if path == "/desktop-local-data/activity":
        try:
            uid = (query.get("uid") or [""])[0]
            limit = int((query.get("limit") or ["300"])[0])
            handler.send_json(
                {
                    "ok": True,
                    "rows": api.desktop_local_data_store().list_activity_for_user(uid, limit),
                }
            )
        except Exception as exc:  # noqa: BLE001
            handler.send_json({"ok": False, "error": str(exc)}, status=400)
        return True

    if path == "/desktop-local-data/startup-metrics":
        try:
            limit_raw = (query.get("limit") or ["200"])[0]
            try:
                limit = int(limit_raw)
            except ValueError:
                limit = 200
            handler.send_json({"ok": True, "rows": api.read_startup_metrics(limit)})
        except Exception as exc:  # noqa: BLE001
            handler.send_json({"ok": False, "error": str(exc)}, status=400)
        return True

    if path == "/app/update-status":
        try:
            handler.send_json(api.get_update_status_payload())
        except Exception as exc:  # noqa: BLE001
            handler.send_json({"ok": False, "error": str(exc)}, status=500)
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

    if path == "/sync/status":
        handler.send_json(api.get_sync_status_payload())
        return True

    if path == "/tasks/run-jobs-pipeline-status":
        handler.send_json(api.get_jobs_pipeline_status_payload())
        return True

    return False
