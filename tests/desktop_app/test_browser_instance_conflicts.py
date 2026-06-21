import json
from pathlib import Path
from unittest import mock

from src.ship import desktop_app
from tests.helpers.temp_paths import workspace_tmpdir


def test_acquire_instance_lock_reclaims_stale_lock() -> None:
    with workspace_tmpdir("desktop-app") as tmp:
        root = Path(tmp) / "session-root"
        root.mkdir(parents=True, exist_ok=True)
        lock_path = root / "desktop-instance.lock"
        lock_path.write_text("not-json", encoding="utf-8")

        with (
            mock.patch.object(desktop_app, "resolve_instance_lock_path", return_value=lock_path),
            mock.patch.object(desktop_app, "_process_identity_matches", return_value=False),
        ):
            lock = desktop_app.acquire_instance_lock(timeout_s=0.5)

        assert lock is not None
        assert lock is not None
        desktop_app.release_instance_lock(lock)


def test_diagnose_instance_conflict_reclaims_stale_owner() -> None:
    with workspace_tmpdir("desktop-app") as tmp:
        root = Path(tmp) / "session-root"
        root.mkdir(parents=True, exist_ok=True)
        lock_path = root / "desktop-instance.lock"
        lock_path.write_text(
            json.dumps(
                {
                    "pid": 999,
                    "createdAt": "2026-03-12T14:00:00+00:00",
                    "launcherToken": "abc",
                    "exePath": "C:/stale/Baluffo.exe",
                    "sessionRoot": str(root),
                    "state": "launching",
                }
            ),
            encoding="utf-8",
        )
        session_path = root / "desktop-session.json"
        session_path.write_text(
            json.dumps({"launcherPid": 999, "bridgePort": 8877}), encoding="utf-8"
        )

        def _fake_reclaim_stale_instance_artifacts(*, data_dir, stale_state, env=None):
            assert data_dir == root
            assert stale_state == {"launcherPid": 999, "bridgePort": 8877}
            assert env is None
            lock_path.unlink()
            session_path.unlink()
            return {"blocked": False}

        with (
            mock.patch.object(desktop_app, "resolve_instance_lock_path", return_value=lock_path),
            mock.patch.object(desktop_app, "resolve_session_state_path", return_value=session_path),
            mock.patch.object(desktop_app, "_process_identity_matches", return_value=False),
            mock.patch.object(
                desktop_app,
                "_reclaim_stale_instance_artifacts",
                side_effect=_fake_reclaim_stale_instance_artifacts,
            ) as reclaim_mock,
            mock.patch.object(desktop_app, "_append_startup_trace"),
        ):
            result = desktop_app.diagnose_instance_conflict(data_dir=root, timeout_s=0.5)

        assert result.get("action") == "reclaimed"
        reclaim_mock.assert_called_once()
        assert not (lock_path.exists())
        assert not (session_path.exists())


def test_diagnose_instance_conflict_blocks_when_stale_runtime_cleanup_fails() -> None:
    with workspace_tmpdir("desktop-app") as tmp:
        root = Path(tmp) / "session-root"
        root.mkdir(parents=True, exist_ok=True)
        lock_path = root / "desktop-instance.lock"
        lock_path.write_text(
            json.dumps(
                {
                    "pid": 999,
                    "createdAt": "2026-03-12T14:00:00+00:00",
                    "launcherToken": "abc",
                    "exePath": "C:/stale/Baluffo.exe",
                    "sessionRoot": str(root),
                    "state": "launching",
                }
            ),
            encoding="utf-8",
        )

        with (
            mock.patch.object(desktop_app, "resolve_instance_lock_path", return_value=lock_path),
            mock.patch.object(desktop_app, "_process_identity_matches", return_value=False),
            mock.patch.object(
                desktop_app,
                "_reclaim_stale_instance_artifacts",
                return_value={
                    "blocked": True,
                    "reason": "stale_bridge_cleanup_failed",
                    "target": "bridge",
                },
            ),
            mock.patch.object(desktop_app, "_append_startup_trace"),
        ):
            result = desktop_app.diagnose_instance_conflict(data_dir=root, timeout_s=0.5)

    assert result["action"] == "blocked"
    assert result["reason"] == "stale_bridge_cleanup_failed"
    assert result["target"] == "bridge"
