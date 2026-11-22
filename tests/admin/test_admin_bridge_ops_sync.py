import json
from pathlib import Path
from typing import Any

from src.bridge.routes.post_routes_admin import handle_post
from tests.helpers.bridge_api import FakeHandler, build_admin_bridge_api


def _load_run_history(root: Path) -> list[dict[str, object]]:
    history_path = root / "admin-run-history.json"
    if not history_path.exists():
        return []
    return list(json.loads(history_path.read_text(encoding="utf-8")))


def test_sync_status_reports_disabled_when_explicitly_disabled(admin_bridge_entrypoint_root):
    api = build_admin_bridge_api()
    api.update_saved_sync_settings({"enabled": False})

    payload = api.get_sync_status_payload()

    assert payload.get("ok")
    assert str((payload.get("config") or {}).get("state") or "") == "disabled"


def test_update_saved_sync_settings_persists_local_enablement_only(admin_bridge_entrypoint_root):
    api = build_admin_bridge_api()

    result = api.update_saved_sync_settings({"enabled": True})

    assert bool(result.get("enabled"))
    payload = api.get_sync_status_payload()
    saved_payload = payload.get("savedConfig") or {}
    assert bool(saved_payload.get("enabled"))
    assert str((payload.get("config") or {}).get("authMode") or "") == "github_app"


def test_sync_pull_updates_local_registry_counts(admin_bridge_entrypoint_root, monkeypatch):
    api = build_admin_bridge_api()
    api.update_saved_sync_settings({"enabled": True})
    api.save_json_atomic(admin_bridge_entrypoint_root / "source-registry-active.json", [])
    api.save_json_atomic(admin_bridge_entrypoint_root / "source-registry-pending.json", [])
    api.save_json_atomic(admin_bridge_entrypoint_root / "source-registry-rejected.json", [])

    monkeypatch.setattr(
        "src.admin_bridge.source_sync_module.pull_and_merge_sources",
        lambda _cfg, _state, **_kwargs: {
            "changed": True,
            "remoteFound": True,
            "remoteSha": "abc",
            "remoteGeneratedAt": "2026-05-12T10:00:00+00:00",
            "snapshotFormat": "sharded-v3",
            "mergedState": {
                "active": [{"adapter": "static", "listing_url": "https://a.com/jobs"}],
                "pending": [{"adapter": "teamtailor", "name": "Foo"}],
                "rejected": [],
            },
        },
    )

    result = api.sync_pull_sources()

    assert result.get("ok")
    assert result.get("changed")
    summary = result.get("summary") or {}
    assert int(summary.get("activeCount") or 0) == 1
    assert int(summary.get("pendingCount") or 0) == 1


def test_sync_push_serializes_expected_snapshot_counts(admin_bridge_entrypoint_root, monkeypatch):
    api = build_admin_bridge_api()
    api.update_saved_sync_settings({"enabled": True})
    api.save_json_atomic(
        admin_bridge_entrypoint_root / "source-registry-active.json",
        [{"id": "static:a", "adapter": "static", "listing_url": "https://a.com/jobs"}],
    )
    api.save_json_atomic(
        admin_bridge_entrypoint_root / "source-registry-pending.json",
        [{"id": "teamtailor:foo", "adapter": "teamtailor", "name": "Foo"}],
    )
    api.save_json_atomic(
        admin_bridge_entrypoint_root / "source-registry-rejected.json",
        [{"id": "lever:bar", "adapter": "lever", "company": "Bar"}],
    )

    monkeypatch.setattr(
        "src.admin_bridge.source_sync_module.push_sources_snapshot",
        lambda _cfg, local_state, **_kwargs: {
            "pushed": True,
            "remotePreviouslyExisted": True,
            "remoteSha": "newsha",
            "snapshotFormat": "sharded-v3",
            "shardCount": 2,
            "changedShardCount": 2,
            "shardsPushedBytes": 4096,
            "manifestSizeBytes": 512,
            "shardCapBytes": 10 * 1024 * 1024,
            "shardHashes": {"shard-path": "sha"},
            "snapshot": {
                "active": list(local_state.get("active") or []),
                "pending": list(local_state.get("pending") or []),
                "rejected": list(local_state.get("rejected") or []),
            },
        },
    )

    result = api.sync_push_sources()

    assert result.get("ok")
    counts = result.get("counts") or {}
    assert int(counts.get("active") or 0) == 1
    assert int(counts.get("pending") or 0) == 1
    assert int(counts.get("rejected") or 0) == 1
    assert result["snapshotFormat"] == "sharded-v3"
    assert result["shardCount"] == 2
    assert result["changedShardCount"] == 2
    assert result["shardHashes"] == {"shard-path": "sha"}


def test_start_sync_task_creates_started_lifecycle_row(admin_bridge_entrypoint_root, monkeypatch):
    api = build_admin_bridge_api()
    api.update_saved_sync_settings({"enabled": True})

    class _NoStartThread:
        def __init__(self, target=None, args=(), kwargs=None, name=None, daemon=None):  # noqa: ANN001
            self.target = target
            self.args = args
            self.kwargs = dict(kwargs or {})
            self.name = name
            self.daemon = daemon

        def start(self):
            return None

    monkeypatch.setattr("src.bridge.sync_service.threading.Thread", _NoStartThread)

    result = api.start_sync_task("pull")

    assert result.get("started")
    assert str(result.get("task") or "") == "source_sync"
    assert str(result.get("action") or "") == "pull"
    projection = api.get_projected_run_history()
    started = [
        row
        for row in projection.rows
        if str(row.get("type") or "") == "sync"
        and str(row.get("lifecycleStatus") or "") == "running"
    ]
    assert len(started) >= 1
    assert str((started[-1].get("summary") or {}).get("action") or "") == "pull"
    assert _load_run_history(admin_bridge_entrypoint_root) == []


def test_sync_push_preview_forwards_dry_run(admin_bridge_entrypoint_root, monkeypatch):
    api = build_admin_bridge_api()
    api.update_saved_sync_settings({"enabled": True})
    calls: dict[str, Any] = {}

    def _push(_cfg, local_state, **kwargs):
        calls.update(kwargs)
        return {
            "pushed": False,
            "dryRun": True,
            "wouldChange": True,
            "remotePreviouslyExisted": True,
            "remoteSha": "abc",
            "skipped": True,
            "skipReason": "dryRun",
            "snapshot": local_state,
        }

    monkeypatch.setattr("src.admin_bridge.source_sync_module.push_sources_snapshot", _push)

    result = api.sync_push_sources(dry_run=True)

    assert result["dryRun"] is True
    assert result["wouldChange"] is True
    assert result["pushed"] is False
    assert calls["dry_run"] is True


class _PreviewPushRouteApi:
    def __init__(self) -> None:
        self.dry_run_calls: list[bool] = []

    def sync_push_sources(self, *, dry_run: bool = False) -> dict[str, Any]:
        self.dry_run_calls.append(dry_run)
        return {"ok": True, "dryRun": dry_run}

    def sync_pull_sources(self) -> dict[str, Any]:
        return {"ok": True}

    def sync_config_status(self) -> dict[str, Any]:
        return {}

    def set_sync_status(self, **_kwargs: Any) -> None:
        return None

    def get_storage_rebuild_payload(self) -> dict[str, Any]:
        return {"ok": True, "storageRebuild": None}


def test_sync_push_route_reads_dry_run_from_body(admin_bridge_entrypoint_root):
    api = _PreviewPushRouteApi()
    handler = FakeHandler()

    handled = handle_post(handler, api=api, path="/sync/push", payload={"dryRun": True})

    assert handled is True
    assert api.dry_run_calls == [True]
    assert handler.sent[-1]["status"] == 200
    assert handler.sent[-1]["payload"] == {"ok": True, "dryRun": True}


class _AsyncPushRouteApi(_PreviewPushRouteApi):
    def __init__(self, *, started: bool = True) -> None:
        super().__init__()
        self.started = started
        self.start_calls: list[tuple[str, str, bool]] = []

    def start_sync_task(
        self, action: str, *, reason: str = "", automatic: bool = True
    ) -> dict[str, Any]:
        self.start_calls.append((action, reason, automatic))
        return {"started": self.started, "task": "source_sync", "action": action}


class _InlinePushRouteApi(_AsyncPushRouteApi):
    def sync_push_sources(self, *, dry_run: bool = False) -> dict[str, Any]:
        self.dry_run_calls.append(dry_run)
        return {"ok": True, "pushed": True, "dryRun": dry_run}


def test_sync_push_route_starts_the_task_by_default(admin_bridge_entrypoint_root):
    api = _AsyncPushRouteApi()
    handler = FakeHandler()

    handled = handle_post(handler, api=api, path="/sync/push", payload={})

    assert handled is True
    assert api.start_calls == [("push", "manual_push", False)]
    assert handler.sent[-1]["status"] == 200
    assert handler.sent[-1]["payload"] == {
        "started": True,
        "task": "source_sync",
        "action": "push",
    }


def test_sync_push_route_reports_a_running_task_as_conflict(admin_bridge_entrypoint_root):
    api = _AsyncPushRouteApi(started=False)
    handler = FakeHandler()

    handled = handle_post(handler, api=api, path="/sync/push", payload={})

    assert handled is True
    assert handler.sent[-1]["status"] == 409
    assert handler.sent[-1]["payload"]["started"] is False


def test_sync_push_route_runs_inline_when_requested(admin_bridge_entrypoint_root):
    api = _InlinePushRouteApi()
    handler = FakeHandler()

    handled = handle_post(handler, api=api, path="/sync/push", payload={"inline": True})

    assert handled is True
    assert api.start_calls == []
    assert handler.sent[-1]["status"] == 200
    assert handler.sent[-1]["payload"] == {"ok": True, "pushed": True, "dryRun": False}


class _AsyncPullRouteApi(_AsyncPushRouteApi):
    def __init__(self, *, started: bool = True) -> None:
        super().__init__(started=started)
        self.pull_calls = 0

    def sync_pull_sources(self) -> dict[str, Any]:
        self.pull_calls += 1
        return {"ok": True, "pulled": True}


def test_sync_pull_route_starts_the_task_by_default(admin_bridge_entrypoint_root):
    api = _AsyncPullRouteApi()
    handler = FakeHandler()

    handled = handle_post(handler, api=api, path="/sync/pull", payload={})

    assert handled is True
    assert api.start_calls == [("pull", "manual_pull", False)]
    assert api.pull_calls == 0
    assert handler.sent[-1]["status"] == 200
    assert handler.sent[-1]["payload"] == {
        "started": True,
        "task": "source_sync",
        "action": "pull",
    }


def test_sync_pull_route_reports_a_running_task_as_conflict(admin_bridge_entrypoint_root):
    api = _AsyncPullRouteApi(started=False)
    handler = FakeHandler()

    handled = handle_post(handler, api=api, path="/sync/pull", payload={})

    assert handled is True
    assert api.pull_calls == 0
    assert handler.sent[-1]["status"] == 409
    assert handler.sent[-1]["payload"]["started"] is False


class _RaisingStartSyncTaskApi(_AsyncPullRouteApi):
    def start_sync_task(
        self, action: str, *, reason: str = "", automatic: bool = True
    ) -> dict[str, Any]:
        raise RuntimeError("start failed")


def test_sync_pull_route_returns_500_payload_when_start_sync_task_raises(
    admin_bridge_entrypoint_root,
):
    api = _RaisingStartSyncTaskApi()
    handler = FakeHandler()

    handled = handle_post(handler, api=api, path="/sync/pull", payload={})

    assert handled is True
    assert api.pull_calls == 0
    assert handler.sent[-1]["status"] == 500
    assert handler.sent[-1]["payload"] == {
        "started": False,
        "task": "source_sync",
        "action": "pull",
        "error": "start failed",
    }


def test_sync_pull_route_runs_inline_when_requested(admin_bridge_entrypoint_root):
    api = _AsyncPullRouteApi()
    handler = FakeHandler()

    handled = handle_post(handler, api=api, path="/sync/pull", payload={"inline": True})

    assert handled is True
    assert api.start_calls == []
    assert api.pull_calls == 1
    assert handler.sent[-1]["status"] == 200
    assert handler.sent[-1]["payload"] == {"ok": True, "pulled": True}


class _RaisingInlinePushRouteApi(_InlinePushRouteApi):
    def __init__(self) -> None:
        super().__init__()
        self.status_calls: list[dict[str, Any]] = []

    def sync_push_sources(self, *, dry_run: bool = False) -> dict[str, Any]:
        self.dry_run_calls.append(dry_run)
        raise RuntimeError("push exploded")

    def set_sync_status(self, **kwargs: Any) -> None:
        self.status_calls.append(kwargs)


def test_sync_push_route_records_sync_status_error_when_inline_action_raises(
    admin_bridge_entrypoint_root,
):
    api = _RaisingInlinePushRouteApi()
    handler = FakeHandler()

    handled = handle_post(handler, api=api, path="/sync/push", payload={"inline": True})

    assert handled is True
    assert api.dry_run_calls == [False]
    assert api.status_calls == [
        {
            "action": "push",
            "result": "error",
            "error": "push exploded",
            "pulled": False,
            "pushed": False,
        }
    ]
    assert handler.sent[-1]["status"] == 500
    assert handler.sent[-1]["payload"] == {
        "ok": False,
        "error": "push exploded",
        "config": {},
    }


def test_sync_push_route_dry_run_wins_over_inline(admin_bridge_entrypoint_root):
    api = _InlinePushRouteApi()
    handler = FakeHandler()

    handled = handle_post(
        handler,
        api=api,
        path="/sync/push",
        payload={"inline": True, "dryRun": True},
    )

    assert handled is True
    assert api.start_calls == []
    assert api.dry_run_calls == [True]
    assert handler.sent[-1]["status"] == 200
    assert handler.sent[-1]["payload"] == {"ok": True, "pushed": True, "dryRun": True}
