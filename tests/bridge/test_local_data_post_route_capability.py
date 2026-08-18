from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

from src.bridge.routes.post_routes_local_data import _LocalDataPostRouteApi, handle_post
from tests.helpers.bridge_api import FakeHandler
from tests.helpers.mutation import append_and_return


class MinimalLocalDataPostStore:
    def __init__(self) -> None:
        self.current_user: dict[str, Any] | None = None
        self.jobs: dict[str, dict[str, Any]] = {}
        self.attachments: dict[str, dict[str, Any]] = {}
        self.imports: list[dict[str, Any]] = []
        self.wiped: list[str] = []

    def add_attachment_for_job(
        self, uid: str, job_key: str, file_meta: dict[str, Any], blob_data_url: str
    ) -> str:
        attachment_id = f"attachment-{len(self.attachments) + 1}"
        self.attachments[attachment_id] = {
            "uid": uid,
            "jobKey": job_key,
            "blobDataUrl": blob_data_url,
            **file_meta,
        }
        return attachment_id

    def delete_attachment_for_job(self, uid: str, job_key: str, attachment_id: str) -> None:
        del uid, job_key
        self.attachments.pop(attachment_id, None)

    def export_profile_data(self, uid: str, include_files: bool = False) -> dict[str, Any]:
        return {"uid": uid, "includeFiles": include_files, "jobs": list(self.jobs.values())}

    def get_admin_overview(self, *, detail: str = "full") -> dict[str, Any]:
        return {"detail": detail, "jobCount": len(self.jobs)}

    def get_current_user(self) -> dict[str, Any] | None:
        return self.current_user

    def import_profile_data(self, uid: str, payload: dict[str, Any]) -> dict[str, Any]:
        result = {"uid": uid, "payload": payload}
        self.imports.append(result)
        return result

    def remove_saved_job_for_user(self, uid: str, job_key: str) -> None:
        del uid
        self.jobs.pop(job_key, None)

    def save_job_for_user(self, uid: str, job: dict[str, Any], options: dict[str, Any]) -> str:
        job_key = f"job-{len(self.jobs) + 1}"
        self.jobs[job_key] = {"uid": uid, "jobKey": job_key, "job": job, "options": options}
        return job_key

    def sign_in(self, name: str) -> dict[str, Any]:
        self.current_user = {"uid": "user-1", "name": name}
        return self.current_user

    def sign_out(self) -> None:
        self.current_user = None

    def update_application_status(
        self, uid: str, job_key: str, status: str, options: dict[str, Any]
    ) -> None:
        self.jobs.setdefault(job_key, {"uid": uid, "jobKey": job_key})["status"] = status
        self.jobs[job_key]["statusOptions"] = options

    def update_application_tracking(
        self, uid: str, job_key: str, tracking: dict[str, Any], options: dict[str, Any]
    ) -> None:
        self.jobs.setdefault(job_key, {"uid": uid, "jobKey": job_key})["tracking"] = tracking
        self.jobs[job_key]["trackingOptions"] = options

    def update_job_notes(self, uid: str, job_key: str, notes: str, options: dict[str, Any]) -> None:
        self.jobs.setdefault(job_key, {"uid": uid, "jobKey": job_key})["notes"] = notes
        self.jobs[job_key]["noteOptions"] = options

    def wipe_account_admin(self, uid: str) -> None:
        self.wiped.append(uid)
        self.current_user = None


class MinimalLocalDataPostRouteApi:
    def __init__(self) -> None:
        self.runtime_config = SimpleNamespace(container_mode=False)
        self.store = MinimalLocalDataPostStore()
        self.startup_metrics: list[tuple[str, dict[str, Any]]] = []
        self.lifecycle_calls: list[dict[str, str]] = []

    def append_startup_metric(self, event: str, payload: dict[str, Any]) -> None:
        self.startup_metrics.append((event, payload))

    def desktop_local_data_store(self) -> MinimalLocalDataPostStore:
        return self.store

    def update_desktop_session_lifecycle(
        self,
        *,
        owner_token: str,
        session_id: str,
        page_id: str,
        state: str,
        reason: str,
    ) -> tuple[int, dict[str, Any]]:
        call = {
            "ownerToken": owner_token,
            "sessionId": session_id,
            "pageId": page_id,
            "state": state,
            "reason": reason,
        }
        self.lifecycle_calls.append(call)
        return 202, {"ok": True, "lifecycle": call}


def _post(
    api: MinimalLocalDataPostRouteApi,
    path: str,
    payload: dict[str, Any],
    *,
    opened_urls: list[str] | None = None,
) -> FakeHandler:
    handler = FakeHandler()
    opened = opened_urls if opened_urls is not None else []
    assert (
        handle_post(
            handler,
            api=cast(_LocalDataPostRouteApi, api),
            path=path,
            payload=payload,
            open_url=lambda url: append_and_return(opened, url, True),
        )
        is True
    )
    return handler


def test_local_data_post_routes_accept_minimal_capability_object() -> None:
    api = MinimalLocalDataPostRouteApi()

    sign_in = _post(api, "/desktop-local-data/sign-in", {"name": "User One"})
    assert sign_in.sent[-1]["payload"]["user"]["name"] == "User One"

    save = _post(api, "/desktop-local-data/saved-jobs/save", {"uid": "user-1", "job": {}})
    job_key = save.sent[-1]["payload"]["jobKey"]
    assert job_key == "job-1"

    _post(
        api,
        "/desktop-local-data/saved-jobs/status",
        {"uid": "user-1", "jobKey": job_key, "status": "applied"},
    )
    _post(
        api,
        "/desktop-local-data/saved-jobs/tracking",
        {
            "uid": "user-1",
            "jobKey": job_key,
            "tracking": {"pipelinePhase": "interview"},
        },
    )
    _post(
        api,
        "/desktop-local-data/saved-jobs/notes",
        {"uid": "user-1", "jobKey": job_key, "notes": "Follow up"},
    )
    assert api.store.jobs[job_key]["status"] == "applied"
    assert api.store.jobs[job_key]["tracking"]["pipelinePhase"] == "interview"
    assert api.store.jobs[job_key]["notes"] == "Follow up"

    add_attachment = _post(
        api,
        "/desktop-local-data/attachments/add",
        {
            "uid": "user-1",
            "jobKey": job_key,
            "fileMeta": {"name": "cv.pdf"},
            "blobDataUrl": "data:application/pdf;base64,AA==",
        },
    )
    attachment_id = add_attachment.sent[-1]["payload"]["attachmentId"]
    assert attachment_id in api.store.attachments
    assert api.store.attachments[attachment_id]["blobDataUrl"].startswith("data:")

    _post(
        api,
        "/desktop-local-data/attachments/delete",
        {"uid": "user-1", "jobKey": job_key, "attachmentId": attachment_id},
    )
    assert attachment_id not in api.store.attachments

    export = _post(
        api,
        "/desktop-local-data/backup/export",
        {"uid": "user-1", "options": {"includeFiles": True}},
    )
    assert export.sent[-1]["payload"]["payload"]["includeFiles"] is True

    imported = _post(
        api,
        "/desktop-local-data/backup/import",
        {"uid": "user-1", "payload": {"rows": []}, "options": {"mode": "merge"}},
    )
    assert imported.sent[-1]["payload"]["result"]["payload"] == {"rows": []}

    overview = _post(api, "/desktop-local-data/admin/overview", {"detail": "summary"})
    assert overview.sent[-1]["payload"]["overview"]["detail"] == "summary"

    lifecycle = _post(
        api,
        "/app/desktop-session-lifecycle",
        {
            "ownerToken": "owner-1",
            "sessionId": "session-1",
            "pageId": "page-1",
            "state": "active",
            "reason": "heartbeat",
        },
    )
    assert lifecycle.sent[-1]["status"] == 202
    assert api.lifecycle_calls[-1]["state"] == "active"

    _post(api, "/desktop-local-data/startup-metric", {"event": "ready", "payload": {"ms": 5}})
    batch = _post(
        api,
        "/desktop-local-data/startup-metrics/batch",
        {"metrics": [{"event": "visible", "payload": {"ms": 9}}, {"payload": {}}]},
    )
    assert batch.sent[-1]["payload"]["accepted"] == 1
    assert api.startup_metrics == [
        ("ready", {"ms": 5}),
        ("visible", {"ms": 9}),
    ]

    opened_urls: list[str] = []
    opened = _post(
        api,
        "/desktop-local-data/open-url",
        {"url": "https://example.test/jobs"},
        opened_urls=opened_urls,
    )
    assert opened.sent[-1]["payload"] == {"ok": True}
    assert opened_urls == ["https://example.test/jobs"]

    wipe = _post(api, "/desktop-local-data/admin/wipe", {"uid": "user-1"})
    assert wipe.sent[-1]["payload"] == {"ok": True, "user": None}
