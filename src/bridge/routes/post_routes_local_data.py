from __future__ import annotations

from typing import Any, Callable
from urllib.parse import urlsplit

from pydantic import ValidationError as PydanticValidationError

from src.bridge.api import BridgeApi
from src.core.schemas import SavedJobSchema


def handle_post(
    handler: Any,
    *,
    api: BridgeApi,
    path: str,
    payload: Any,
    open_url: Callable[[str], bool],
) -> bool:
    if path == "/desktop-local-data/sign-in":
        try:
            user = api.desktop_local_data_store().sign_in(str((payload or {}).get("name") or ""))
            handler._send_json({"ok": True, "user": user})  # noqa: SLF001
        except Exception as exc:  # noqa: BLE001
            handler._send_json({"ok": False, "error": str(exc)}, status=400)  # noqa: SLF001
        return True

    if path == "/desktop-local-data/sign-out":
        try:
            api.desktop_local_data_store().sign_out()
            handler._send_json({"ok": True})  # noqa: SLF001
        except Exception as exc:  # noqa: BLE001
            handler._send_json({"ok": False, "error": str(exc)}, status=400)  # noqa: SLF001
        return True

    if path == "/desktop-local-data/saved-jobs/save":
        try:
            job = (payload or {}).get("job") if isinstance((payload or {}).get("job"), dict) else {}
            if job:
                SavedJobSchema.model_validate(job)
            job_key = api.desktop_local_data_store().save_job_for_user(
                str((payload or {}).get("uid") or ""),
                job,
                (payload or {}).get("options")
                if isinstance((payload or {}).get("options"), dict)
                else {},
            )
            handler._send_json({"ok": True, "jobKey": job_key})  # noqa: SLF001
        except PydanticValidationError as exc:  # noqa: BLE001
            details = exc.errors()
            first_msg = details[0].get("msg", str(exc)) if details else str(exc)
            handler._send_json(  # noqa: SLF001
                {
                    "ok": False,
                    "error": f"Invalid saved job shape: {first_msg}",
                    "details": details,
                },
                status=400,
            )
        except Exception as exc:  # noqa: BLE001
            handler._send_json({"ok": False, "error": str(exc)}, status=400)  # noqa: SLF001
        return True

    if path == "/desktop-local-data/saved-jobs/remove":
        try:
            api.desktop_local_data_store().remove_saved_job_for_user(
                str((payload or {}).get("uid") or ""),
                str((payload or {}).get("jobKey") or ""),
            )
            handler._send_json({"ok": True})  # noqa: SLF001
        except Exception as exc:  # noqa: BLE001
            handler._send_json({"ok": False, "error": str(exc)}, status=400)  # noqa: SLF001
        return True

    if path == "/desktop-local-data/saved-jobs/status":
        try:
            api.desktop_local_data_store().update_application_status(
                str((payload or {}).get("uid") or ""),
                str((payload or {}).get("jobKey") or ""),
                str((payload or {}).get("status") or ""),
                (payload or {}).get("options")
                if isinstance((payload or {}).get("options"), dict)
                else {},
            )
            handler._send_json({"ok": True})  # noqa: SLF001
        except Exception as exc:  # noqa: BLE001
            handler._send_json({"ok": False, "error": str(exc)}, status=400)  # noqa: SLF001
        return True

    if path == "/desktop-local-data/saved-jobs/notes":
        try:
            api.desktop_local_data_store().update_job_notes(
                str((payload or {}).get("uid") or ""),
                str((payload or {}).get("jobKey") or ""),
                str((payload or {}).get("notes") or ""),
            )
            handler._send_json({"ok": True})  # noqa: SLF001
        except Exception as exc:  # noqa: BLE001
            handler._send_json({"ok": False, "error": str(exc)}, status=400)  # noqa: SLF001
        return True

    if path == "/desktop-local-data/attachments/add":
        try:
            attachment_id = api.desktop_local_data_store().add_attachment_for_job(
                str((payload or {}).get("uid") or ""),
                str((payload or {}).get("jobKey") or ""),
                (payload or {}).get("fileMeta")
                if isinstance((payload or {}).get("fileMeta"), dict)
                else {},
                str((payload or {}).get("blobDataUrl") or ""),
            )
            handler._send_json({"ok": True, "attachmentId": attachment_id})  # noqa: SLF001
        except Exception as exc:  # noqa: BLE001
            handler._send_json({"ok": False, "error": str(exc)}, status=400)  # noqa: SLF001
        return True

    if path == "/desktop-local-data/attachments/delete":
        try:
            api.desktop_local_data_store().delete_attachment_for_job(
                str((payload or {}).get("uid") or ""),
                str((payload or {}).get("jobKey") or ""),
                str((payload or {}).get("attachmentId") or ""),
            )
            handler._send_json({"ok": True})  # noqa: SLF001
        except Exception as exc:  # noqa: BLE001
            handler._send_json({"ok": False, "error": str(exc)}, status=400)  # noqa: SLF001
        return True

    if path == "/desktop-local-data/backup/export":
        try:
            result = api.desktop_local_data_store().export_profile_data(
                str((payload or {}).get("uid") or ""),
                bool(
                    (
                        ((payload or {}).get("options") or {})
                        if isinstance((payload or {}).get("options"), dict)
                        else {}
                    ).get("includeFiles")
                ),
            )
            handler._send_json({"ok": True, "payload": result})  # noqa: SLF001
        except Exception as exc:  # noqa: BLE001
            handler._send_json({"ok": False, "error": str(exc)}, status=400)  # noqa: SLF001
        return True

    if path == "/desktop-local-data/backup/import":
        try:
            result = api.desktop_local_data_store().import_profile_data(
                str((payload or {}).get("uid") or ""),
                (payload or {}).get("payload")
                if isinstance((payload or {}).get("payload"), dict)
                else {},
            )
            handler._send_json({"ok": True, "result": result})  # noqa: SLF001
        except Exception as exc:  # noqa: BLE001
            handler._send_json({"ok": False, "error": str(exc)}, status=400)  # noqa: SLF001
        return True

    if path == "/desktop-local-data/admin/overview":
        try:
            handler._send_json(
                {"ok": True, "overview": api.desktop_local_data_store().get_admin_overview()}
            )  # noqa: SLF001,E501
        except Exception as exc:  # noqa: BLE001
            handler._send_json({"ok": False, "error": str(exc)}, status=400)  # noqa: SLF001
        return True

    if path == "/desktop-local-data/admin/wipe":
        try:
            api.desktop_local_data_store().wipe_account_admin(
                str((payload or {}).get("uid") or ""),
            )
            handler._send_json(
                {"ok": True, "user": api.desktop_local_data_store().get_current_user()}
            )  # noqa: SLF001
        except Exception as exc:  # noqa: BLE001
            handler._send_json({"ok": False, "error": str(exc)}, status=400)  # noqa: SLF001
        return True

    if path == "/app/desktop-session-lifecycle":
        payload_dict = payload if isinstance(payload, dict) else {}
        status_code, result = api.update_desktop_session_lifecycle(
            owner_token=str(payload_dict.get("ownerToken") or ""),
            session_id=str(payload_dict.get("sessionId") or ""),
            page_id=str(payload_dict.get("pageId") or ""),
            state=str(payload_dict.get("state") or ""),
        )
        handler._send_json(result, status=status_code)  # noqa: SLF001
        return True

    if path == "/desktop-local-data/startup-metric":
        try:
            event = str((payload or {}).get("event") or "").strip() or "unknown"
            details = (
                (payload or {}).get("payload")
                if isinstance((payload or {}).get("payload"), dict)
                else {}
            )
            api.append_startup_metric(event, details)
            handler._send_json({"ok": True})  # noqa: SLF001
        except Exception as exc:  # noqa: BLE001
            handler._send_json({"ok": False, "error": str(exc)}, status=400)  # noqa: SLF001
        return True

    if path == "/desktop-local-data/open-url":
        try:
            url = str((payload or {}).get("url") or "").strip()
            parsed = urlsplit(url)
            if parsed.scheme not in {"http", "https"} or not parsed.netloc:
                handler._send_json({"ok": False, "error": "Invalid URL"}, status=400)  # noqa: SLF001
                return True
            if open_url(url):
                handler._send_json({"ok": True})  # noqa: SLF001
                return True
            handler._send_json(  # noqa: SLF001
                {"ok": False, "error": "Unable to open the default browser"},
                status=500,
            )
        except Exception as exc:  # noqa: BLE001
            handler._send_json({"ok": False, "error": str(exc)}, status=400)  # noqa: SLF001
        return True

    return False
