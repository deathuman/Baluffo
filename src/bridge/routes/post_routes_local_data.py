from __future__ import annotations

from collections.abc import Callable
from typing import Any
from urllib.parse import urlsplit

from pydantic import ValidationError as PydanticValidationError

from src.bridge.api import BridgeApi
from src.bridge.routes.response_writer import BridgeResponseWriter
from src.core.schemas import SavedJobSchema


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def handle_post(
    handler: BridgeResponseWriter,
    *,
    api: BridgeApi,
    path: str,
    payload: Any,
    open_url: Callable[[str], bool],
) -> bool:
    payload_dict = _as_dict(payload)
    if path == "/desktop-local-data/sign-in":
        try:
            user = api.desktop_local_data_store().sign_in(str(payload_dict.get("name") or ""))
            handler.send_json({"ok": True, "user": user})
        except Exception as exc:  # noqa: BLE001
            handler.send_json({"ok": False, "error": str(exc)}, status=400)
        return True

    if path == "/desktop-local-data/sign-out":
        try:
            api.desktop_local_data_store().sign_out()
            handler.send_json({"ok": True})
        except Exception as exc:  # noqa: BLE001
            handler.send_json({"ok": False, "error": str(exc)}, status=400)
        return True

    if path == "/desktop-local-data/saved-jobs/save":
        try:
            job = _as_dict(payload_dict.get("job"))
            if job:
                SavedJobSchema.model_validate(job)
            job_key = api.desktop_local_data_store().save_job_for_user(
                str(payload_dict.get("uid") or ""),
                job,
                _as_dict(payload_dict.get("options")),
            )
            handler.send_json({"ok": True, "jobKey": job_key})
        except PydanticValidationError as exc:  # noqa: BLE001
            details = exc.errors()
            first_msg = details[0].get("msg", str(exc)) if details else str(exc)
            handler.send_json(
                {
                    "ok": False,
                    "error": f"Invalid saved job shape: {first_msg}",
                    "details": details,
                },
                status=400,
            )
        except Exception as exc:  # noqa: BLE001
            handler.send_json({"ok": False, "error": str(exc)}, status=400)
        return True

    if path == "/desktop-local-data/saved-jobs/remove":
        try:
            api.desktop_local_data_store().remove_saved_job_for_user(
                str(payload_dict.get("uid") or ""),
                str(payload_dict.get("jobKey") or ""),
            )
            handler.send_json({"ok": True})
        except Exception as exc:  # noqa: BLE001
            handler.send_json({"ok": False, "error": str(exc)}, status=400)
        return True

    if path == "/desktop-local-data/saved-jobs/status":
        try:
            api.desktop_local_data_store().update_application_status(
                str(payload_dict.get("uid") or ""),
                str(payload_dict.get("jobKey") or ""),
                str(payload_dict.get("status") or ""),
                _as_dict(payload_dict.get("options")),
            )
            handler.send_json({"ok": True})
        except Exception as exc:  # noqa: BLE001
            handler.send_json({"ok": False, "error": str(exc)}, status=400)
        return True

    if path == "/desktop-local-data/saved-jobs/notes":
        try:
            api.desktop_local_data_store().update_job_notes(
                str(payload_dict.get("uid") or ""),
                str(payload_dict.get("jobKey") or ""),
                str(payload_dict.get("notes") or ""),
            )
            handler.send_json({"ok": True})
        except Exception as exc:  # noqa: BLE001
            handler.send_json({"ok": False, "error": str(exc)}, status=400)
        return True

    if path == "/desktop-local-data/attachments/add":
        try:
            attachment_id = api.desktop_local_data_store().add_attachment_for_job(
                str(payload_dict.get("uid") or ""),
                str(payload_dict.get("jobKey") or ""),
                _as_dict(payload_dict.get("fileMeta")),
                str(payload_dict.get("blobDataUrl") or ""),
            )
            handler.send_json({"ok": True, "attachmentId": attachment_id})
        except Exception as exc:  # noqa: BLE001
            handler.send_json({"ok": False, "error": str(exc)}, status=400)
        return True

    if path == "/desktop-local-data/attachments/delete":
        try:
            api.desktop_local_data_store().delete_attachment_for_job(
                str(payload_dict.get("uid") or ""),
                str(payload_dict.get("jobKey") or ""),
                str(payload_dict.get("attachmentId") or ""),
            )
            handler.send_json({"ok": True})
        except Exception as exc:  # noqa: BLE001
            handler.send_json({"ok": False, "error": str(exc)}, status=400)
        return True

    if path == "/desktop-local-data/backup/export":
        try:
            result = api.desktop_local_data_store().export_profile_data(
                str(payload_dict.get("uid") or ""),
                bool(_as_dict(payload_dict.get("options")).get("includeFiles")),
            )
            handler.send_json({"ok": True, "payload": result})
        except Exception as exc:  # noqa: BLE001
            handler.send_json({"ok": False, "error": str(exc)}, status=400)
        return True

    if path == "/desktop-local-data/backup/import":
        try:
            result = api.desktop_local_data_store().import_profile_data(
                str(payload_dict.get("uid") or ""),
                _as_dict(payload_dict.get("payload")),
            )
            handler.send_json({"ok": True, "result": result})
        except Exception as exc:  # noqa: BLE001
            handler.send_json({"ok": False, "error": str(exc)}, status=400)
        return True

    if path == "/desktop-local-data/admin/overview":
        try:
            handler.send_json(
                {"ok": True, "overview": api.desktop_local_data_store().get_admin_overview()}
            )
        except Exception as exc:  # noqa: BLE001
            handler.send_json({"ok": False, "error": str(exc)}, status=400)
        return True

    if path == "/desktop-local-data/admin/wipe":
        try:
            api.desktop_local_data_store().wipe_account_admin(
                str(payload_dict.get("uid") or ""),
            )
            handler.send_json(
                {"ok": True, "user": api.desktop_local_data_store().get_current_user()}
            )
        except Exception as exc:  # noqa: BLE001
            handler.send_json({"ok": False, "error": str(exc)}, status=400)
        return True

    if path == "/app/desktop-session-lifecycle":
        status_code, result = api.update_desktop_session_lifecycle(
            owner_token=str(payload_dict.get("ownerToken") or ""),
            session_id=str(payload_dict.get("sessionId") or ""),
            page_id=str(payload_dict.get("pageId") or ""),
            state=str(payload_dict.get("state") or ""),
        )
        handler.send_json(result, status=status_code)
        return True

    if path == "/desktop-local-data/startup-metric":
        try:
            event = str(payload_dict.get("event") or "").strip() or "unknown"
            metric_payload = _as_dict(payload_dict.get("payload"))
            api.append_startup_metric(event, metric_payload)
            handler.send_json({"ok": True})
        except Exception as exc:  # noqa: BLE001
            handler.send_json({"ok": False, "error": str(exc)}, status=400)
        return True

    if path == "/desktop-local-data/open-url":
        try:
            url = str(payload_dict.get("url") or "").strip()
            parsed = urlsplit(url)
            if parsed.scheme not in {"http", "https"} or not parsed.netloc:
                handler.send_json({"ok": False, "error": "Invalid URL"}, status=400)
                return True
            if open_url(url):
                handler.send_json({"ok": True})
                return True
            handler.send_json(
                {"ok": False, "error": "Unable to open the default browser"},
                status=500,
            )
        except Exception as exc:  # noqa: BLE001
            handler.send_json({"ok": False, "error": str(exc)}, status=400)
        return True

    return False
