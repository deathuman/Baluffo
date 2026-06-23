import json
import os
from pathlib import Path
from unittest import mock

from src.ship import desktop_app
from tests.helpers.temp_paths import workspace_tmpdir


def test_save_session_state_writes_valid_json_atomically() -> None:
    with workspace_tmpdir("desktop-app-session") as tmp:
        session_path = Path(tmp) / "session-root" / "desktop-session.json"
        payload = {
            "launcherPid": 123,
            "bridgePort": 8877,
            "launcherToken": "token",
        }

        with mock.patch.object(
            desktop_app, "resolve_session_state_path", return_value=session_path
        ):
            result = desktop_app.save_session_state(payload)

        assert result == session_path
        assert json.loads(session_path.read_text(encoding="utf-8")) == payload


def test_save_session_state_failed_replace_preserves_existing_session_file() -> None:
    with workspace_tmpdir("desktop-app-session") as tmp:
        session_path = Path(tmp) / "session-root" / "desktop-session.json"
        session_path.parent.mkdir(parents=True, exist_ok=True)
        original = {"launcherPid": 111, "bridgePort": 8877}
        session_path.write_text(json.dumps(original), encoding="utf-8")

        with (
            mock.patch.object(desktop_app, "resolve_session_state_path", return_value=session_path),
            mock.patch(
                "src.ship.desktop_app.session.os.replace",
                side_effect=OSError(13, "replace denied"),
            ),
        ):
            try:
                desktop_app.save_session_state({"launcherPid": 222, "bridgePort": 8878})
            except OSError:
                pass
            else:
                raise AssertionError("save_session_state should surface replace failures")

        assert json.loads(session_path.read_text(encoding="utf-8")) == original


def test_lock_backoff_delay_is_capped_and_deterministic_without_jitter() -> None:
    with mock.patch("src.ship.desktop_app.session.random.uniform", return_value=0.0):
        assert desktop_app._lock_backoff_delay(0) == 0.05
        assert desktop_app._lock_backoff_delay(1) == 0.1
        assert desktop_app._lock_backoff_delay(3) == 0.25


def test_acquire_instance_lock_waits_for_recent_invalid_lock_payload() -> None:
    with workspace_tmpdir("desktop-app") as tmp:
        root = Path(tmp) / "session-root"
        root.mkdir(parents=True, exist_ok=True)
        lock_path = root / "desktop-instance.lock"
        lock_path.write_text("", encoding="utf-8")

        with (
            mock.patch.object(desktop_app, "resolve_instance_lock_path", return_value=lock_path),
            mock.patch.object(desktop_app, "_lock_path_is_recent", return_value=True),
            mock.patch.object(desktop_app, "_sleep_for_lock_retry") as sleep_mock,
            mock.patch(
                "src.ship.desktop_app.session.time.monotonic",
                side_effect=[100.0, 100.0, 100.3],
            ),
        ):
            lock = desktop_app.acquire_instance_lock(timeout_s=0.2)

        assert lock is None
        assert lock_path.exists()
        sleep_mock.assert_called_once()


def test_acquire_instance_lock_reclaims_stale_invalid_lock_payload() -> None:
    with workspace_tmpdir("desktop-app") as tmp:
        root = Path(tmp) / "session-root"
        root.mkdir(parents=True, exist_ok=True)
        lock_path = root / "desktop-instance.lock"
        lock_path.write_text("", encoding="utf-8")
        stale_ts = 1_700_000_000
        os.utime(lock_path, (stale_ts, stale_ts))

        with (
            mock.patch.object(desktop_app, "resolve_instance_lock_path", return_value=lock_path),
            mock.patch.object(desktop_app, "_lock_path_is_recent", return_value=False),
            mock.patch.object(desktop_app, "_process_identity_matches", return_value=False),
        ):
            lock = desktop_app.acquire_instance_lock(timeout_s=0.5)

        assert lock is not None
        desktop_app.release_instance_lock(lock)


def test_acquire_instance_lock_retries_after_initial_payload_write_failure() -> None:
    with workspace_tmpdir("desktop-app") as tmp:
        root = Path(tmp) / "session-root"
        root.mkdir(parents=True, exist_ok=True)
        lock_path = root / "desktop-instance.lock"
        original_write = desktop_app._write_lock_payload_to_handle
        write_attempts = 0

        def _flaky_write(handle: int, payload: dict[str, object]) -> None:
            nonlocal write_attempts
            write_attempts += 1
            if write_attempts == 1:
                raise OSError(5, "payload write failed")
            original_write(handle, payload)

        with (
            mock.patch.object(desktop_app, "resolve_instance_lock_path", return_value=lock_path),
            mock.patch.object(
                desktop_app, "_write_lock_payload_to_handle", side_effect=_flaky_write
            ),
            mock.patch.object(desktop_app, "_sleep_for_lock_retry") as sleep_mock,
        ):
            lock = desktop_app.acquire_instance_lock(timeout_s=0.5)

        assert lock is not None
        assert write_attempts == 2
        sleep_mock.assert_called_once()
        assert lock_path.exists()
        assert lock_path.stat().st_size > 0
        desktop_app.release_instance_lock(lock)


def test_acquire_instance_lock_traces_initial_payload_write_failure() -> None:
    with workspace_tmpdir("desktop-app") as tmp:
        data_dir = Path(tmp) / "data"
        root = Path(tmp) / "session-root"
        root.mkdir(parents=True, exist_ok=True)
        lock_path = root / "desktop-instance.lock"
        original_write = desktop_app._write_lock_payload_to_handle
        write_attempts = 0

        def _flaky_write(handle: int, payload: dict[str, object]) -> None:
            nonlocal write_attempts
            write_attempts += 1
            if write_attempts == 1:
                raise OSError(13, "payload denied")
            original_write(handle, payload)

        with (
            mock.patch.object(desktop_app, "resolve_instance_lock_path", return_value=lock_path),
            mock.patch.object(
                desktop_app, "_write_lock_payload_to_handle", side_effect=_flaky_write
            ),
            mock.patch.object(desktop_app, "_sleep_for_lock_retry"),
        ):
            lock = desktop_app.acquire_instance_lock(
                timeout_s=0.5,
                env={"BALUFFO_DATA_DIR": str(data_dir)},
            )

        assert lock is not None
        rows = desktop_app.read_startup_metrics(data_dir)
        event = next(row for row in rows if row.get("event") == "desktop_lock_payload_write_failed")
        fields = event["fields"]
        assert fields["path"] == str(lock_path)
        assert fields["errno"] == 13
        desktop_app.release_instance_lock(lock)


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
