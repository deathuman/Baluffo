"""Desktop local-data GET route handlers.

AI boundary owns: desktop local-data GET route response wiring only.
AI boundary implement in: local data store leaves and desktop local-data services.
AI boundary search before contracts: frontend callers, bridge route inventory, API docs.
AI boundary verify: `npm run lint:repo-guardrails` plus focused GET route tests.
"""

from __future__ import annotations

import io
import json
import logging
import re
import time
import zipfile
from datetime import UTC, datetime
from typing import Any, Protocol

from pydantic import ValidationError as PydanticValidationError

from src.bridge.routes.error_boundary import run_route_boundary, send_json_boundary
from src.bridge.routes.response_writer import BridgeResponseWriter
from src.core.schemas import LocalSavedJobRowSchema

logger = logging.getLogger(__name__)


def _json_error(exc: Exception) -> dict[str, Any]:
    return {"ok": False, "error": str(exc)}


class _DesktopLocalDataStore(Protocol):
    def export_profile_data(self, uid: str, *, include_files: bool = False) -> dict[str, Any]: ...

    def get_attachment_blob(
        self, uid: str, job_key: str, attachment_id: str
    ) -> tuple[bytes, str, str]: ...

    def get_current_user(self) -> dict[str, Any] | None: ...

    def get_saved_job_keys(self, uid: str) -> list[str]: ...

    def list_activity_for_user(self, uid: str, limit: int) -> list[dict[str, Any]]: ...

    def list_attachments_for_job(self, uid: str, job_key: str) -> list[dict[str, Any]]: ...

    def list_profiles(self) -> list[dict[str, Any]]: ...

    def list_saved_jobs(self, uid: str) -> list[dict[str, Any]]: ...


class _LocalDataGetRouteApi(Protocol):
    DESKTOP_SESSION_ACTIVITY_AT: str | None

    def desktop_local_data_store(self) -> _DesktopLocalDataStore: ...

    def get_desktop_session_payload(self) -> dict[str, Any]: ...

    def read_startup_metrics(self, limit: int) -> list[dict[str, Any]]: ...


def _handle_session_route(handler: BridgeResponseWriter, *, api: _LocalDataGetRouteApi) -> bool:
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


def _handle_saved_jobs_route(
    handler: BridgeResponseWriter,
    *,
    api: _LocalDataGetRouteApi,
    query: dict[str, list[str]],
) -> bool:
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


def _handle_attachment_content_route(
    handler: BridgeResponseWriter,
    *,
    api: _LocalDataGetRouteApi,
    query: dict[str, list[str]],
) -> bool:
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


def _handle_backup_export_file_route(
    handler: BridgeResponseWriter,
    *,
    api: _LocalDataGetRouteApi,
    query: dict[str, list[str]],
) -> bool:
    def _send_export_file() -> None:
        uid = (query.get("uid") or [""])[0]
        include_files_raw = str((query.get("includeFiles") or ["0"])[0]).strip().lower()
        include_files = include_files_raw in {"1", "true", "yes", "on"}
        payload = api.desktop_local_data_store().export_profile_data(
            uid, include_files=include_files
        )
        date_token = datetime.now(UTC).strftime("%Y-%m-%d")
        safe_uid = re.sub(r"[^a-zA-Z0-9_-]+", "_", str(uid or "profile")).strip("_") or "profile"
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


def _handle_profiles_route(handler: BridgeResponseWriter, *, api: _LocalDataGetRouteApi) -> bool:
    send_json_boundary(
        handler,
        lambda: {"ok": True, "profiles": api.desktop_local_data_store().list_profiles()},
        error_status=400,
        error_payload=_json_error,
    )
    return True


def _handle_saved_job_keys_route(
    handler: BridgeResponseWriter,
    *,
    api: _LocalDataGetRouteApi,
    query: dict[str, list[str]],
) -> bool:
    def _payload() -> dict[str, Any]:
        uid = (query.get("uid") or [""])[0]
        return {"ok": True, "keys": api.desktop_local_data_store().get_saved_job_keys(uid)}

    send_json_boundary(handler, _payload, error_status=400, error_payload=_json_error)
    return True


def _handle_attachments_route(
    handler: BridgeResponseWriter,
    *,
    api: _LocalDataGetRouteApi,
    query: dict[str, list[str]],
) -> bool:
    def _payload() -> dict[str, Any]:
        uid = (query.get("uid") or [""])[0]
        job_key = (query.get("jobKey") or [""])[0]
        return {
            "ok": True,
            "rows": api.desktop_local_data_store().list_attachments_for_job(uid, job_key),
        }

    send_json_boundary(handler, _payload, error_status=400, error_payload=_json_error)
    return True


def _handle_activity_route(
    handler: BridgeResponseWriter,
    *,
    api: _LocalDataGetRouteApi,
    query: dict[str, list[str]],
) -> bool:
    def _payload() -> dict[str, Any]:
        uid = (query.get("uid") or [""])[0]
        limit = int((query.get("limit") or ["300"])[0])
        return {
            "ok": True,
            "rows": api.desktop_local_data_store().list_activity_for_user(uid, limit),
        }

    send_json_boundary(handler, _payload, error_status=400, error_payload=_json_error)
    return True


def _handle_startup_metrics_route(
    handler: BridgeResponseWriter,
    *,
    api: _LocalDataGetRouteApi,
    query: dict[str, list[str]],
) -> bool:
    def _payload() -> dict[str, Any]:
        limit_raw = (query.get("limit") or ["200"])[0]
        try:
            limit = int(limit_raw)
        except ValueError:
            limit = 200
        return {"ok": True, "rows": api.read_startup_metrics(limit)}

    send_json_boundary(handler, _payload, error_status=400, error_payload=_json_error)
    return True


def handle_local_data_get_routes(
    handler: BridgeResponseWriter,
    *,
    api: _LocalDataGetRouteApi,
    path: str,
    query: dict[str, list[str]],
) -> bool:
    if path == "/desktop-local-data/session":
        return _handle_session_route(handler, api=api)

    if path == "/desktop-local-data/profiles":
        return _handle_profiles_route(handler, api=api)

    if path == "/desktop-local-data/saved-jobs":
        return _handle_saved_jobs_route(handler, api=api, query=query)

    if path == "/desktop-local-data/saved-job-keys":
        return _handle_saved_job_keys_route(handler, api=api, query=query)

    if path == "/desktop-local-data/attachments":
        return _handle_attachments_route(handler, api=api, query=query)

    if path == "/desktop-local-data/attachments/content":
        return _handle_attachment_content_route(handler, api=api, query=query)

    if path == "/desktop-local-data/backup/export-file":
        return _handle_backup_export_file_route(handler, api=api, query=query)

    if path == "/desktop-local-data/activity":
        return _handle_activity_route(handler, api=api, query=query)

    if path == "/desktop-local-data/startup-metrics":
        return _handle_startup_metrics_route(handler, api=api, query=query)

    return False
