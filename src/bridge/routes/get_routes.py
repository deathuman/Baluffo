from __future__ import annotations

import io
import json
import logging
import re
import zipfile
from datetime import UTC, datetime
from typing import Any

from pydantic import ValidationError as PydanticValidationError

from src.bridge.api import BridgeApi
from src.core.schemas import SavedJobSchema

logger = logging.getLogger(__name__)


def handle_get(handler: Any, *, api: BridgeApi, path: str, query: dict[str, list[str]]) -> bool:
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

            payload = report or {"summary": {}, "candidates": [], "failures": []}
            # Prefer the bytes-writing helper to bypass any unexpected issues
            # in `_send_json` for edge-case payloads.
            if hasattr(handler, "_send_bytes"):
                try:
                    text = json.dumps(payload, ensure_ascii=False, default=str)
                    body = text.encode("utf-8")
                except UnicodeEncodeError:
                    text = json.dumps(payload, ensure_ascii=True, default=str)
                    body = text.encode("utf-8")
                handler._send_bytes(  # type: ignore[attr-defined]  # noqa: SLF001
                    body,
                    content_type="application/json; charset=utf-8",
                    status=200,
                )
            else:
                handler._send_json(payload)  # noqa: SLF001
        except Exception as exc:  # noqa: BLE001
            try:
                api.bridge_log("error", "discovery_report_route_failed", error=str(exc))
            except Exception:  # noqa: BLE001
                pass
            payload = {"error": "failed_to_load_discovery_report", "detail": str(exc)}
            if hasattr(handler, "_send_bytes"):
                try:
                    text = json.dumps(payload, ensure_ascii=False, default=str)
                    body = text.encode("utf-8")
                except UnicodeEncodeError:
                    text = json.dumps(payload, ensure_ascii=True, default=str)
                    body = text.encode("utf-8")
                handler._send_bytes(  # type: ignore[attr-defined]  # noqa: SLF001
                    body,
                    content_type="application/json; charset=utf-8",
                    status=500,
                )
            else:
                handler._send_json(payload, status=500)  # noqa: SLF001
        return True

    if path == "/desktop-local-data/session":
        try:
            handler._send_json(  # noqa: SLF001
                {
                    "ok": True,
                    "user": api.desktop_local_data_store().get_current_user(),
                    "lastActivityAt": str(api.DESKTOP_SESSION_ACTIVITY_AT or ""),
                }
            )
        except Exception as exc:  # noqa: BLE001
            handler._send_json({"ok": False, "error": str(exc)}, status=400)  # noqa: SLF001
        return True

    if path == "/desktop-local-data/saved-jobs":
        try:
            uid = (query.get("uid") or [""])[0]
            raw_rows = api.desktop_local_data_store().list_saved_jobs(uid)
            rows = []
            for row in raw_rows:
                try:
                    SavedJobSchema.model_validate(row)
                    rows.append(row)
                except PydanticValidationError as exc:
                    logger.warning("Saved job row validation failed, skipping: %s", exc)
            handler._send_json({"ok": True, "rows": rows})  # noqa: SLF001
        except Exception as exc:  # noqa: BLE001
            handler._send_json({"ok": False, "error": str(exc)}, status=400)  # noqa: SLF001
        return True

    if path == "/desktop-local-data/saved-job-keys":
        try:
            uid = (query.get("uid") or [""])[0]
            handler._send_json(
                {"ok": True, "keys": api.desktop_local_data_store().get_saved_job_keys(uid)}
            )  # noqa: SLF001
        except Exception as exc:  # noqa: BLE001
            handler._send_json({"ok": False, "error": str(exc)}, status=400)  # noqa: SLF001
        return True

    if path == "/desktop-local-data/attachments":
        try:
            uid = (query.get("uid") or [""])[0]
            job_key = (query.get("jobKey") or [""])[0]
            handler._send_json(
                {
                    "ok": True,
                    "rows": api.desktop_local_data_store().list_attachments_for_job(uid, job_key),
                }
            )  # noqa: SLF001
        except Exception as exc:  # noqa: BLE001
            handler._send_json({"ok": False, "error": str(exc)}, status=400)  # noqa: SLF001
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
            handler._send_bytes(  # noqa: SLF001
                body,
                content_type=content_type,
                filename=filename,
                disposition="attachment" if download_flag in {"1", "true", "yes"} else "inline",
            )
        except Exception as exc:  # noqa: BLE001
            handler._send_json({"ok": False, "error": str(exc)}, status=400)  # noqa: SLF001
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
                handler._send_bytes(
                    body,
                    content_type="application/zip",
                    filename=filename,
                    disposition="attachment",
                )  # noqa: SLF001
            else:
                body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
                filename = f"baluffo-backup-{safe_uid}-{date_token}.json"
                handler._send_bytes(
                    body,
                    content_type="application/json; charset=utf-8",
                    filename=filename,
                    disposition="attachment",
                )  # noqa: SLF001
        except Exception as exc:  # noqa: BLE001
            handler._send_json({"ok": False, "error": str(exc)}, status=400)  # noqa: SLF001
        return True

    if path == "/desktop-local-data/activity":
        try:
            uid = (query.get("uid") or [""])[0]
            limit = int((query.get("limit") or ["300"])[0])
            handler._send_json(
                {
                    "ok": True,
                    "rows": api.desktop_local_data_store().list_activity_for_user(uid, limit),
                }
            )  # noqa: SLF001
        except Exception as exc:  # noqa: BLE001
            handler._send_json({"ok": False, "error": str(exc)}, status=400)  # noqa: SLF001
        return True

    if path == "/desktop-local-data/startup-metrics":
        try:
            limit_raw = (query.get("limit") or ["200"])[0]
            try:
                limit = int(limit_raw)
            except ValueError:
                limit = 200
            handler._send_json({"ok": True, "rows": api.read_startup_metrics(limit)})  # noqa: SLF001
        except Exception as exc:  # noqa: BLE001
            handler._send_json({"ok": False, "error": str(exc)}, status=400)  # noqa: SLF001
        return True

    if path == "/app/update-status":
        try:
            handler._send_json(api.get_update_status_payload())  # noqa: SLF001
        except Exception as exc:  # noqa: BLE001
            handler._send_json({"ok": False, "error": str(exc)}, status=500)  # noqa: SLF001
        return True

    if path == "/registry/active":
        state = api.load_state()
        handler._send_json({"sources": state["active"], "summary": api.summarize_state(state)})  # noqa: SLF001
        return True

    if path == "/registry/pending":
        state = api.load_state()
        handler._send_json({"sources": state["pending"], "summary": api.summarize_state(state)})  # noqa: SLF001
        return True

    if path == "/registry/rejected":
        state = api.load_state()
        handler._send_json({"sources": state["rejected"], "summary": api.summarize_state(state)})  # noqa: SLF001
        return True

    if path == "/discovery/log":
        offset_raw = (query.get("offset") or ["0"])[0]
        try:
            offset = max(0, int(offset_raw))
        except ValueError:
            offset = 0
        try:
            text = api.DISCOVERY_LOG_PATH.read_text(encoding="utf-8")
        except OSError:
            text = ""
        chunk = text[offset:]
        next_offset = len(text)
        handler._send_json(
            {"text": chunk, "offset": offset, "nextOffset": next_offset, "hasMore": False}
        )  # noqa: SLF001
        return True

    if path == "/fetcher/log":
        offset_raw = (query.get("offset") or ["0"])[0]
        try:
            offset = max(0, int(offset_raw))
        except ValueError:
            offset = 0
        try:
            text = api.FETCHER_LOG_PATH.read_text(encoding="utf-8")
        except OSError:
            text = ""
        chunk = text[offset:]
        next_offset = len(text)
        handler._send_json(
            {"text": chunk, "offset": offset, "nextOffset": next_offset, "hasMore": False}
        )  # noqa: SLF001
        return True

    if path == "/registry/summary":
        state = api.load_state()
        handler._send_json({"summary": api.summarize_state(state)})  # noqa: SLF001
        return True

    if path == "/ops/health":
        handler._send_json(api.compute_ops_health())  # noqa: SLF001
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
        handler._send_json({"runs": rows[-limit:], "count": len(rows)})  # noqa: SLF001
        return True

    if path == "/discovery/config":
        handler._send_json(api.get_discovery_config_payload())  # noqa: SLF001
        return True

    if path == "/ops/task-state":
        handler._send_json(api.get_current_task_state_payload())  # noqa: SLF001
        return True

    if path == "/ops/fetcher-metrics":
        window_raw = (query.get("windowRuns") or ["20"])[0]
        try:
            window_runs = max(1, min(200, int(window_raw)))
        except ValueError:
            window_runs = 20
        handler._send_json(api.compute_fetcher_metrics(window_runs=window_runs))  # noqa: SLF001
        return True

    if path == "/ops/fetch-report":
        handler._send_json(
            api.normalize_fetch_report_contract(
                api.load_json_object(api.JOBS_FETCH_REPORT_PATH, {})
            )
        )  # noqa: SLF001
        return True

    if path == "/sync/status":
        handler._send_json(api.get_sync_status_payload())  # noqa: SLF001
        return True

    if path == "/tasks/run-jobs-pipeline-status":
        handler._send_json(api.get_jobs_pipeline_status_payload())  # noqa: SLF001
        return True

    return False
