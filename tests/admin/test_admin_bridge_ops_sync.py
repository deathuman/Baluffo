import json
from pathlib import Path

from tests.helpers.bridge_api import build_admin_bridge_api


def _load_run_history(root: Path) -> list[dict[str, object]]:
    history_path = root / "admin-run-history.json"
    if not history_path.exists():
        return []
    return json.loads(history_path.read_text(encoding="utf-8"))


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
        lambda _cfg, _state: {
            "changed": True,
            "remoteFound": True,
            "remoteSha": "abc",
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
        [{"adapter": "static", "listing_url": "https://a.com/jobs"}],
    )
    api.save_json_atomic(
        admin_bridge_entrypoint_root / "source-registry-pending.json",
        [{"adapter": "teamtailor", "name": "Foo"}],
    )
    api.save_json_atomic(
        admin_bridge_entrypoint_root / "source-registry-rejected.json",
        [{"adapter": "lever", "company": "Bar"}],
    )

    monkeypatch.setattr(
        "src.admin_bridge.source_sync_module.push_sources_snapshot",
        lambda _cfg, local_state: {
            "pushed": True,
            "remotePreviouslyExisted": True,
            "remoteSha": "newsha",
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


def test_start_sync_task_creates_started_history_row(admin_bridge_entrypoint_root, monkeypatch):
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
    rows = _load_run_history(admin_bridge_entrypoint_root)
    started = [
        row
        for row in rows
        if str(row.get("type") or "") == "sync" and str(row.get("status") or "") == "started"
    ]
    assert len(started) >= 1
    assert str((started[-1].get("summary") or {}).get("action") or "") == "pull"


def test_sync_history_discards_stale_sync_started_row_without_run_id(admin_bridge_entrypoint_root):
    api = build_admin_bridge_api()
    api.save_json_atomic(
        admin_bridge_entrypoint_root / "admin-run-history.json",
        [
            {
                "id": "sync_stale_1",
                "type": "sync",
                "status": "started",
                "startedAt": "2026-03-09T12:00:00+00:00",
                "finishedAt": "",
                "durationMs": 0,
                "summary": {"action": "push"},
            }
        ],
    )

    rows = api.sync_history_from_reports()

    assert not any(str(row.get("id") or "") == "sync_stale_1" for row in rows)
