from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from src.bridge import admin_bootstrap
from src.bridge.routes import get_admin_bootstrap
from src.bridge.routes.get_routes import handle_get
from tests.helpers.bridge_api import FakeDesktopLocalDataStore, FakeHandler, make_stub_bridge_api


def _overview_summary(store: FakeDesktopLocalDataStore, detail: str) -> dict[str, Any]:
    users = store.list_profiles()
    return {
        "detailLevel": detail,
        "attachmentSizeBasis": "metadata",
        "users": users,
        "totals": {"users": len(users), "savedJobs": 1},
    }


def _raise(exc: BaseException) -> None:
    raise exc


def test_admin_bootstrap_best_effort_suppresses_expected_fallback_failures() -> None:
    assert (
        admin_bootstrap._best_effort("fallback", lambda: _raise(RuntimeError("offline")))  # noqa: SLF001
        == "fallback"
    )
    assert (
        admin_bootstrap._best_effort("fallback", lambda: _raise(OSError("unavailable")))  # noqa: SLF001
        == "fallback"
    )


def test_admin_bootstrap_best_effort_does_not_hide_unexpected_failures() -> None:
    with pytest.raises(AssertionError, match="unexpected bootstrap bug"):
        admin_bootstrap._best_effort(  # noqa: SLF001
            "fallback",
            lambda: _raise(AssertionError("unexpected bootstrap bug")),
        )


def test_admin_bootstrap_uses_bounded_control_plane_inputs(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    user = store.sign_in("Andrea")
    store.save_job_for_user(user["uid"], {"title": "Designer"}, {})
    store.get_admin_overview = lambda *, detail="full": _overview_summary(store, detail)  # type: ignore[attr-defined]
    api = make_stub_bridge_api(tmp_path, store)
    api.app_version = "9.9.9"
    api.sync_config_status = lambda: {"ready": True, "enabled": True, "credentialsPackaged": True}
    api.load_sync_runtime_state = lambda: {
        "lastPullAt": "2026-06-11T21:39:53Z",
        "lastPushAt": "2026-06-04T17:27:36Z",
        "lastAction": "pull",
        "lastResult": "ok",
    }
    api.compute_ops_health_ready = lambda: {
        "schedule": {
            "pipeline": {
                "enabled": True,
                "intervalHours": 12,
                "nextRunAt": "2026-06-12T03:07:50Z",
                "lastPipelineFinishedAt": "2026-06-11T15:07:50Z",
            }
        }
    }
    api.get_lifecycle_current_runs = lambda: [
        {
            "runId": "fetch_live",
            "taskType": "fetch",
            "status": "running",
            "active": True,
            "startedAt": "2026-06-11T10:00:00Z",
            "heartbeatAt": "2026-06-11T10:00:01Z",
            "taskProgress": {"processed": 10},
        }
    ]
    api.get_lifecycle_recent_runs = lambda: [
        {
            "runId": "old_1",
            "taskType": "sync",
            "status": "succeeded",
            "finishedAt": "2026-06-10T10:00:00Z",
        },
        {
            "runId": "old_2",
            "taskType": "fetch",
            "status": "failed",
            "finishedAt": "2026-06-10T11:00:00Z",
        },
        {
            "runId": "old_3",
            "taskType": "discovery",
            "status": "succeeded",
            "finishedAt": "2026-06-10T12:00:00Z",
        },
    ]
    api.get_jobs_pipeline_status_payload = lambda: {"active": False}

    def forbidden(*_args: object, **_kwargs: object) -> dict[str, object]:
        raise AssertionError("admin bootstrap must not call heavy diagnostic helpers")

    api.get_registry_summary_payload = forbidden
    api.compute_ops_health = forbidden
    api.compute_ops_dashboard_health = forbidden
    api.compute_ops_dashboard_health_summary = forbidden
    api.get_current_task_state_payload = forbidden
    api.get_current_task_state_summary_payload = forbidden
    api.get_sync_status_payload = forbidden
    api.load_state = forbidden

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/admin/bootstrap", query={})

    assert result is True
    payload = handler.sent[-1]["payload"]
    assert handler.sent[-1]["status"] == 200
    assert payload["ok"] is True
    assert payload["app"]["version"] == "9.9.9"
    assert payload["session"]["user"]["name"] == "Andrea"
    assert payload["overview"]["totals"]["users"] == 1
    assert payload["sync"]["config"]["enabled"] is True
    assert payload["sync"]["savedConfig"]["enabled"] is True
    assert payload["sync"]["runtime"]["lastPullAt"] == "2026-06-11T21:39:53Z"
    assert payload["schedule"]["pipeline"]["enabled"] is True
    assert payload["schedule"]["pipeline"]["intervalHours"] == 12
    assert payload["registrySummary"]["deferredDuringActiveRun"] is True
    assert payload["registrySummary"]["detailLevel"] == "deferred"
    assert [row["runId"] for row in payload["tasks"]["current"]] == ["fetch_live"]
    assert [row["runId"] for row in payload["tasks"]["recent"]] == ["old_3", "old_2"]


def test_admin_bootstrap_includes_registry_summary_when_idle(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    store.sign_in("Andrea")
    store.get_admin_overview = lambda *, detail="full": _overview_summary(store, detail)  # type: ignore[attr-defined]
    api = make_stub_bridge_api(tmp_path, store)
    api.get_lifecycle_current_runs = lambda: []
    api.get_lifecycle_recent_runs = lambda: []
    api.get_jobs_pipeline_status_payload = lambda: {"active": False}
    api.get_registry_summary_payload = lambda: {
        "activeCount": 2309,
        "pendingCount": 812,
        "rejectedCount": 0,
        "duplicatePendingCount": 1,
        "summaryExact": False,
        "countBasis": "storage",
    }

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/admin/bootstrap", query={})

    assert result is True
    payload = handler.sent[-1]["payload"]
    assert payload["registrySummary"]["activeCount"] == 2309
    assert payload["registrySummary"]["pendingCount"] == 812
    assert "deferredDuringActiveRun" not in payload["registrySummary"]


def test_admin_bootstrap_smoke_fail_once_is_guarded_and_single_use(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(get_admin_bootstrap, "_ADMIN_BOOTSTRAP_SMOKE_FAIL_ONCE_CONSUMED", False)
    monkeypatch.setenv("BALUFFO_PACKAGED_SMOKE_ADMIN_BOOTSTRAP_FAIL_ONCE", "1")
    monkeypatch.delenv("BALUFFO_PACKAGED_SMOKE_RUNTIME", raising=False)
    store = FakeDesktopLocalDataStore()
    store.sign_in("Andrea")
    store.get_admin_overview = lambda *, detail="full": _overview_summary(store, detail)  # type: ignore[attr-defined]
    api = make_stub_bridge_api(tmp_path, store)
    api.get_lifecycle_current_runs = lambda: []
    api.get_lifecycle_recent_runs = lambda: []
    api.get_jobs_pipeline_status_payload = lambda: {"active": False}

    guarded_handler = FakeHandler()
    assert handle_get(guarded_handler, api=api, path="/admin/bootstrap", query={}) is True
    assert guarded_handler.sent[-1]["status"] == 200
    assert guarded_handler.sent[-1]["payload"]["ok"] is True

    monkeypatch.setenv("BALUFFO_PACKAGED_SMOKE_RUNTIME", "1")
    first_handler = FakeHandler()
    assert handle_get(first_handler, api=api, path="/admin/bootstrap", query={}) is True
    assert first_handler.sent[-1]["status"] == 504
    assert first_handler.sent[-1]["payload"]["smokeFailure"] is True

    second_handler = FakeHandler()
    assert handle_get(second_handler, api=api, path="/admin/bootstrap", query={}) is True
    assert second_handler.sent[-1]["status"] == 200
    assert second_handler.sent[-1]["payload"]["ok"] is True


def test_admin_bootstrap_includes_current_user_shell_when_overview_empty(
    tmp_path: Path,
) -> None:
    store = FakeDesktopLocalDataStore()
    user = store.sign_in("Andrea")
    store.get_admin_overview = lambda *, detail="full": {  # type: ignore[attr-defined]
        "detailLevel": detail,
        "attachmentSizeBasis": "metadata",
        "users": [],
        "totals": {"users": 0, "savedJobs": 0},
    }
    api = make_stub_bridge_api(tmp_path, store)
    api.get_lifecycle_current_runs = lambda: []
    api.get_lifecycle_recent_runs = lambda: []
    api.get_jobs_pipeline_status_payload = lambda: {"active": False}

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/admin/bootstrap", query={})

    assert result is True
    payload = handler.sent[-1]["payload"]
    users = payload["overview"]["users"]
    assert users == [
        {
            "uid": user["uid"],
            "userId": user["uid"],
            "name": "Andrea",
            "displayName": "Andrea",
            "savedJobsCount": 0,
            "notesBytes": 0,
            "attachmentsCount": 0,
            "attachmentsBytes": 0,
            "totalBytes": 0,
            "profileShell": True,
        }
    ]
    assert payload["overview"]["totals"]["users"] == 1
    assert payload["overview"]["totals"]["usersCount"] == 1
