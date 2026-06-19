from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol
from urllib.parse import urlsplit

from pydantic import ValidationError as PydanticValidationError

from src.bridge.container_mode import is_container_runtime, send_container_unavailable
from src.bridge.performance_profile import time_operation
from src.bridge.routes.error_boundary import run_route_boundary, send_json_boundary
from src.bridge.routes.response_writer import BridgeResponseWriter
from src.core.schemas import SavedJobSchema


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _json_error(exc: Exception) -> dict[str, Any]:
    return {"ok": False, "error": str(exc)}


def _admin_overview_detail(value: Any) -> str:
    detail = str(value or "full").strip().lower()
    if detail not in {"summary", "full"}:
        raise ValueError("Invalid admin overview detail. Expected 'summary' or 'full'.")
    return detail


class _DesktopLocalDataPostStore(Protocol):
    def add_attachment_for_job(
        self, uid: str, job_key: str, file_meta: dict[str, Any], blob_data_url: str
    ) -> str: ...

    def delete_attachment_for_job(self, uid: str, job_key: str, attachment_id: str) -> None: ...

    def export_profile_data(self, uid: str, include_files: bool = False) -> dict[str, Any]: ...

    def get_admin_overview(self, *, detail: str = "full") -> dict[str, Any]: ...

    def get_current_user(self) -> dict[str, Any] | None: ...

    def import_profile_data(self, uid: str, payload: dict[str, Any]) -> dict[str, Any]: ...

    def remove_saved_job_for_user(self, uid: str, job_key: str) -> None: ...

    def save_job_for_user(self, uid: str, job: dict[str, Any], options: dict[str, Any]) -> str: ...

    def sign_in(self, name: str) -> dict[str, Any]: ...

    def sign_out(self) -> None: ...

    def update_application_status(
        self, uid: str, job_key: str, status: str, options: dict[str, Any]
    ) -> None: ...

    def update_application_tracking(
        self, uid: str, job_key: str, tracking: dict[str, Any], options: dict[str, Any]
    ) -> None: ...

    def update_job_notes(
        self, uid: str, job_key: str, notes: str, options: dict[str, Any]
    ) -> None: ...

    def wipe_account_admin(self, uid: str) -> None: ...


class _LocalDataPostRouteApi(Protocol):
    runtime_config: Any

    def append_startup_metric(self, event: str, payload: dict[str, Any]) -> None: ...

    def desktop_local_data_store(self) -> _DesktopLocalDataPostStore: ...

    def update_desktop_session_lifecycle(
        self,
        *,
        owner_token: str,
        session_id: str,
        page_id: str,
        state: str,
        reason: str,
    ) -> tuple[int, dict[str, Any]]: ...


def _handle_saved_job_tracking_post(
    handler: BridgeResponseWriter,
    *,
    api: _LocalDataPostRouteApi,
    path: str,
    payload_dict: dict[str, Any],
) -> bool:
    route_name = path.rsplit("/", 1)[-1]

    def _payload() -> dict[str, Any]:
        store = api.desktop_local_data_store()
        uid = str(payload_dict.get("uid") or "")
        job_key = str(payload_dict.get("jobKey") or "")
        options = _as_dict(payload_dict.get("options"))
        if route_name == "status":
            store.update_application_status(
                uid,
                job_key,
                str(payload_dict.get("status") or ""),
                options,
            )
        elif route_name == "tracking":
            store.update_application_tracking(
                uid,
                job_key,
                _as_dict(payload_dict.get("tracking")),
                options,
            )
        else:
            store.update_job_notes(
                uid,
                job_key,
                str(payload_dict.get("notes") or ""),
                options,
            )
        return {"ok": True}

    send_json_boundary(handler, _payload, error_status=400, error_payload=_json_error)
    return True


def _send_admin_overview_post(
    handler: BridgeResponseWriter,
    *,
    api: _LocalDataPostRouteApi,
    payload_dict: dict[str, Any],
) -> bool:
    def _payload() -> dict[str, Any]:
        detail = _admin_overview_detail(payload_dict.get("detail"))
        with time_operation(f"localData.adminOverview.{detail}"):
            overview = api.desktop_local_data_store().get_admin_overview(detail=detail)
        return {"ok": True, "overview": overview}

    send_json_boundary(handler, _payload, error_status=400, error_payload=_json_error)
    return True


def _send_desktop_session_lifecycle_post(
    handler: BridgeResponseWriter,
    *,
    api: _LocalDataPostRouteApi,
    payload_dict: dict[str, Any],
) -> bool:
    if is_container_runtime(api):
        send_container_unavailable(handler)
        return True
    status_code, result = api.update_desktop_session_lifecycle(
        owner_token=str(payload_dict.get("ownerToken") or ""),
        session_id=str(payload_dict.get("sessionId") or ""),
        page_id=str(payload_dict.get("pageId") or ""),
        state=str(payload_dict.get("state") or ""),
        reason=str(payload_dict.get("reason") or ""),
    )
    handler.send_json(result, status=status_code)
    return True


def _send_startup_metric_post(
    handler: BridgeResponseWriter,
    *,
    api: _LocalDataPostRouteApi,
    payload_dict: dict[str, Any],
) -> bool:
    def _payload() -> dict[str, Any]:
        event = str(payload_dict.get("event") or "").strip() or "unknown"
        metric_payload = _as_dict(payload_dict.get("payload"))
        api.append_startup_metric(event, metric_payload)
        return {"ok": True}

    send_json_boundary(handler, _payload, error_status=400, error_payload=_json_error)
    return True


def _send_startup_metrics_batch_post(
    handler: BridgeResponseWriter,
    *,
    api: _LocalDataPostRouteApi,
    payload_dict: dict[str, Any],
) -> bool:
    def _payload() -> dict[str, Any]:
        rows = payload_dict.get("metrics")
        if not isinstance(rows, list):
            rows = payload_dict.get("events")
        if not isinstance(rows, list):
            raise ValueError("metrics must be an array")
        accepted = 0
        for row in rows[:200]:
            if not isinstance(row, dict):
                continue
            event = str(row.get("event") or "").strip()
            if not event:
                continue
            metric_payload = _as_dict(row.get("payload"))
            api.append_startup_metric(event, metric_payload)
            accepted += 1
        return {"ok": True, "accepted": accepted}

    send_json_boundary(handler, _payload, error_status=400, error_payload=_json_error)
    return True


def _send_open_url_post(
    handler: BridgeResponseWriter,
    *,
    api: _LocalDataPostRouteApi,
    payload_dict: dict[str, Any],
    open_url: Callable[[str], bool],
) -> bool:
    if is_container_runtime(api):
        send_container_unavailable(handler)
        return True

    def _send_open_url() -> None:
        url = str(payload_dict.get("url") or "").strip()
        parsed = urlsplit(url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            handler.send_json({"ok": False, "error": "Invalid URL"}, status=400)
            return
        if open_url(url):
            handler.send_json({"ok": True})
            return
        handler.send_json(
            {"ok": False, "error": "Unable to open the default browser"},
            status=500,
        )

    run_route_boundary(handler, _send_open_url, error_status=400, error_payload=_json_error)
    return True


def handle_post(
    handler: BridgeResponseWriter,
    *,
    api: _LocalDataPostRouteApi,
    path: str,
    payload: Any,
    open_url: Callable[[str], bool],
) -> bool:
    payload_dict = _as_dict(payload)
    if path == "/desktop-local-data/sign-in":
        send_json_boundary(
            handler,
            lambda: {
                "ok": True,
                "user": api.desktop_local_data_store().sign_in(str(payload_dict.get("name") or "")),
            },
            error_status=400,
            error_payload=_json_error,
        )
        return True

    if path == "/desktop-local-data/sign-out":

        def _payload() -> dict[str, Any]:
            api.desktop_local_data_store().sign_out()
            return {"ok": True}

        send_json_boundary(handler, _payload, error_status=400, error_payload=_json_error)
        return True

    if path == "/desktop-local-data/saved-jobs/save":

        def _payload() -> dict[str, Any]:
            job = _as_dict(payload_dict.get("job"))
            if job:
                SavedJobSchema.model_validate(job)
            job_key = api.desktop_local_data_store().save_job_for_user(
                str(payload_dict.get("uid") or ""),
                job,
                _as_dict(payload_dict.get("options")),
            )
            return {"ok": True, "jobKey": job_key}

        def _error(exc: Exception) -> dict[str, Any]:
            if isinstance(exc, PydanticValidationError):
                details = exc.errors()
                first_msg = details[0].get("msg", str(exc)) if details else str(exc)
                return {
                    "ok": False,
                    "error": f"Invalid saved job shape: {first_msg}",
                    "details": details,
                }
            return {"ok": False, "error": str(exc)}

        send_json_boundary(handler, _payload, error_status=400, error_payload=_error)
        return True

    if path == "/desktop-local-data/saved-jobs/remove":

        def _payload() -> dict[str, Any]:
            api.desktop_local_data_store().remove_saved_job_for_user(
                str(payload_dict.get("uid") or ""),
                str(payload_dict.get("jobKey") or ""),
            )
            return {"ok": True}

        send_json_boundary(handler, _payload, error_status=400, error_payload=_json_error)
        return True

    if path in (
        "/desktop-local-data/saved-jobs/status",
        "/desktop-local-data/saved-jobs/tracking",
        "/desktop-local-data/saved-jobs/notes",
    ):
        return _handle_saved_job_tracking_post(
            handler,
            api=api,
            path=path,
            payload_dict=payload_dict,
        )

    if path == "/desktop-local-data/attachments/add":

        def _payload() -> dict[str, Any]:
            attachment_id = api.desktop_local_data_store().add_attachment_for_job(
                str(payload_dict.get("uid") or ""),
                str(payload_dict.get("jobKey") or ""),
                _as_dict(payload_dict.get("fileMeta")),
                str(payload_dict.get("blobDataUrl") or ""),
            )
            return {"ok": True, "attachmentId": attachment_id}

        send_json_boundary(handler, _payload, error_status=400, error_payload=_json_error)
        return True

    if path == "/desktop-local-data/attachments/delete":

        def _payload() -> dict[str, Any]:
            api.desktop_local_data_store().delete_attachment_for_job(
                str(payload_dict.get("uid") or ""),
                str(payload_dict.get("jobKey") or ""),
                str(payload_dict.get("attachmentId") or ""),
            )
            return {"ok": True}

        send_json_boundary(handler, _payload, error_status=400, error_payload=_json_error)
        return True

    if path == "/desktop-local-data/backup/export":

        def _payload() -> dict[str, Any]:
            result = api.desktop_local_data_store().export_profile_data(
                str(payload_dict.get("uid") or ""),
                bool(_as_dict(payload_dict.get("options")).get("includeFiles")),
            )
            return {"ok": True, "payload": result}

        send_json_boundary(handler, _payload, error_status=400, error_payload=_json_error)
        return True

    if path == "/desktop-local-data/backup/import":

        def _payload() -> dict[str, Any]:
            result = api.desktop_local_data_store().import_profile_data(
                str(payload_dict.get("uid") or ""),
                _as_dict(payload_dict.get("payload")),
            )
            return {"ok": True, "result": result}

        send_json_boundary(handler, _payload, error_status=400, error_payload=_json_error)
        return True

    if path == "/desktop-local-data/admin/overview":
        return _send_admin_overview_post(handler, api=api, payload_dict=payload_dict)

    if path == "/desktop-local-data/admin/wipe":

        def _payload() -> dict[str, Any]:
            api.desktop_local_data_store().wipe_account_admin(
                str(payload_dict.get("uid") or ""),
            )
            return {"ok": True, "user": api.desktop_local_data_store().get_current_user()}

        send_json_boundary(handler, _payload, error_status=400, error_payload=_json_error)
        return True

    if path == "/app/desktop-session-lifecycle":
        return _send_desktop_session_lifecycle_post(handler, api=api, payload_dict=payload_dict)

    if path == "/desktop-local-data/startup-metric":
        return _send_startup_metric_post(handler, api=api, payload_dict=payload_dict)

    if path == "/desktop-local-data/startup-metrics/batch":
        return _send_startup_metrics_batch_post(handler, api=api, payload_dict=payload_dict)

    if path == "/desktop-local-data/open-url":
        return _send_open_url_post(
            handler,
            api=api,
            payload_dict=payload_dict,
            open_url=open_url,
        )

    return False
