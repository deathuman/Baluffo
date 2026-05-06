import base64
import json
import os
import subprocess
from pathlib import Path
from unittest import mock
from urllib.request import Request, urlopen

import pytest

from src import packaged_desktop_smoke as smoke
from src import source_sync
from tests.helpers.temp_paths import workspace_tmpdir

from ._helpers import _write_packaged_sync_bundle_config

pytestmark = [pytest.mark.packaging, pytest.mark.slow]


def test_run_packaged_smoke_can_run_desktop_update_rehearsal_mode() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        report_path = root / "data" / "latest.json"
        artifacts_dir = root / "artifacts"
        exe_path = root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        args = smoke.parse_args(
            [
                "--exe-path",
                str(exe_path),
                "--report-path",
                str(report_path),
                "--artifacts-dir",
                str(artifacts_dir),
                "--desktop-update-rehearsal",
            ]
        )
        with (
            mock.patch.object(smoke, "ensure_portable_exe", return_value=exe_path),
            mock.patch.object(
                smoke,
                "collect_packaged_smoke_env_diagnostics",
                return_value={"tmp": "C:/tmp", "temp": "C:/tmp", "isElevated": False},
            ),
            mock.patch.object(
                smoke,
                "run_desktop_update_rehearsal",
                return_value={
                    "name": "Packaged desktop updater rehearsal",
                    "slug": "desktop-update-rehearsal",
                    "status": "passed",
                    "durationMs": 1500,
                    "error": "",
                    "details": {
                        "helperStdoutLog": str(artifacts_dir / "helper.stdout.log"),
                        "helperStderrLog": str(artifacts_dir / "helper.stderr.log"),
                        "helperDiagnosticsLog": str(artifacts_dir / "helper.diagnostics.jsonl"),
                    },
                },
            ) as rehearsal_mock,
        ):
            payload = smoke.run_packaged_smoke(args)
        assert payload["ok"] is True
        assert payload["scenarios"][0]["slug"] == "desktop-update-rehearsal"
        assert payload["artifacts"]["helperStdout"] == str(artifacts_dir / "helper.stdout.log")
        assert payload["artifacts"]["helperStderr"] == str(artifacts_dir / "helper.stderr.log")
        assert payload["artifacts"]["helperDiagnostics"] == str(
            artifacts_dir / "helper.diagnostics.jsonl"
        )
        rehearsal_mock.assert_called_once()
        saved = json.loads(report_path.read_text(encoding="utf-8"))
        assert saved["ok"] is True


def test_packaged_sync_rehearsal_server_serves_fake_github_app_flow() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        portable_root = Path(tmp) / "portable"
        config_path = _write_packaged_sync_bundle_config(portable_root)
        loaded_path, _raw_payload, packaged_config = (
            smoke._load_portable_packaged_sync_rehearsal_config(  # noqa: SLF001
                portable_root
            )
        )
        assert loaded_path == config_path
        base_url, _stats, server, thread = smoke._start_packaged_sync_rehearsal_server(  # noqa: SLF001
            packaged_config=packaged_config,
            snapshot_payload={
                "schemaVersion": source_sync.SYNC_SCHEMA_VERSION,
                "generatedAt": "2026-04-19T12:00:00+00:00",
                "source": {"name": "packaged_sync_rehearsal"},
                "active": [],
                "pending": [],
                "rejected": [],
            },
        )
        try:
            token_request = Request(
                f"{base_url}/app/installations/999999/access_tokens",
                data=b"{}",
                headers={"Authorization": "Bearer rehearsal-jwt"},
                method="POST",
            )
            with urlopen(token_request, timeout=5) as response:  # noqa: S310
                token_payload = json.loads(response.read().decode("utf-8"))
            assert token_payload["token"] == "packaged-sync-rehearsal-token"

            content_request = Request(
                f"{base_url}/repos/owner/repo/contents/baluffo/source-sync.json?ref=main",
                headers={"Authorization": "Bearer packaged-sync-rehearsal-token"},
            )
            with urlopen(content_request, timeout=5) as response:  # noqa: S310
                content_payload = json.loads(response.read().decode("utf-8"))
            decoded = json.loads(base64.b64decode(content_payload["content"]).decode("utf-8"))
            assert content_payload["sha"] == "packaged-sync-rehearsal-sha"
            assert decoded["source"]["name"] == "packaged_sync_rehearsal"
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2.0)


def test_load_portable_packaged_sync_rehearsal_config_rejects_machine_key_derivation() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        portable_root = Path(tmp) / "portable"
        _write_packaged_sync_bundle_config(portable_root, key_derivation="machine")
        with pytest.raises(RuntimeError, match="keyDerivation=machine"):
            smoke._load_portable_packaged_sync_rehearsal_config(portable_root)  # noqa: SLF001


def test_run_packaged_smoke_can_run_sync_rehearsal_mode() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        report_path = root / "data" / "latest.json"
        artifacts_dir = root / "artifacts"
        exe_path = root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        args = smoke.parse_args(
            [
                "--exe-path",
                str(exe_path),
                "--report-path",
                str(report_path),
                "--artifacts-dir",
                str(artifacts_dir),
                "--sync-rehearsal",
            ]
        )
        with (
            mock.patch.object(smoke, "ensure_portable_exe", return_value=exe_path),
            mock.patch.object(
                smoke,
                "collect_packaged_smoke_env_diagnostics",
                return_value={"tmp": "C:/tmp", "temp": "C:/tmp", "isElevated": False},
            ),
            mock.patch.object(
                smoke,
                "run_packaged_sync_rehearsal",
                return_value={
                    "name": "Packaged sync rehearsal",
                    "slug": "packaged-sync-rehearsal",
                    "status": "passed",
                    "durationMs": 1200,
                    "error": "",
                    "details": {
                        "runtimeStdout": str(artifacts_dir / "sync.stdout.log"),
                        "runtimeStderr": str(artifacts_dir / "sync.stderr.log"),
                    },
                },
            ) as rehearsal_mock,
        ):
            payload = smoke.run_packaged_smoke(args)
        assert payload["ok"] is True
        assert payload["scenarios"][0]["slug"] == "packaged-sync-rehearsal"
        assert payload["artifacts"]["syncRehearsalStdout"] == str(artifacts_dir / "sync.stdout.log")
        assert payload["artifacts"]["syncRehearsalStderr"] == str(artifacts_dir / "sync.stderr.log")
        rehearsal_mock.assert_called_once()
        saved = json.loads(report_path.read_text(encoding="utf-8"))
        assert saved["ok"] is True


def test_select_packaged_browser_job_browser_enables_edge_when_needed() -> None:
    with mock.patch.object(
        smoke,
        "select_startup_probe_browser",
        side_effect=[
            RuntimeError("No supported managed Chromium probe browser available."),
            {"browserName": "msedge", "browserPath": "C:/Edge/msedge.exe"},
        ],
    ):
        selected, env_overrides = smoke._select_packaged_browser_job_browser({})

    assert selected == {"browserName": "msedge", "browserPath": "C:/Edge/msedge.exe"}
    assert env_overrides == {
        smoke.desktop_app_mod.PREFERRED_BROWSER_PATH_ENV: "C:/Edge/msedge.exe",
        "BALUFFO_DESKTOP_ALLOW_EDGE_APP_MODE": "1",
    }


def test_launch_packaged_desktop_child_uses_ship_root_layout() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        portable_root = Path(tmp) / "portable"
        ship_root = portable_root / "ship"
        ship_root.mkdir(parents=True, exist_ok=True)
        exe_path = portable_root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        launch_mock = mock.Mock(
            return_value=(mock.Mock(spec=subprocess.Popen), mock.Mock(), mock.Mock())
        )

        with mock.patch.object(smoke, "launch_packaged_command", launch_mock):
            smoke.launch_packaged_desktop_child(
                exe_path,
                mode="site",
                port=8080,
                stdout_path=portable_root / "site.stdout.log",
                stderr_path=portable_root / "site.stderr.log",
            )
            smoke.launch_packaged_desktop_child(
                exe_path,
                mode="bridge",
                port=8877,
                owner_token="owner-token",
                desktop_session_id="desktop-session",
                stdout_path=portable_root / "bridge.stdout.log",
                stderr_path=portable_root / "bridge.stderr.log",
            )

    site_args = launch_mock.call_args_list[0].kwargs["args"]
    bridge_args = launch_mock.call_args_list[1].kwargs["args"]

    assert site_args[0:4] == ["__child_site__", "--root", str(ship_root), "--port"]
    assert bridge_args[0:4] == ["__child_bridge__", "--root", str(ship_root), "--bind-host"]
    assert bridge_args[bridge_args.index("--data-dir") + 1] == str(ship_root / "data")


def test_select_browser_shutdown_proof_falls_back_to_live_window_pid() -> None:
    rows = [
        {"event": "desktop_browser_job_attached", "fields": {"pid": 333}},
        {
            "event": "desktop_shell_window_shown",
            "fields": {"observed": True, "windowPid": 444},
        },
    ]

    with mock.patch.object(
        smoke.desktop_app_mod,
        "is_process_alive",
        side_effect=lambda pid: int(pid) == 444,
    ):
        proof = smoke._select_browser_shutdown_proof(rows)

    assert proof == {
        "proofSource": "window-pid",
        "proofPid": 444,
        "attachedPid": 333,
        "windowPid": 444,
    }


def test_select_browser_shutdown_proof_fails_without_live_attached_or_window_pid() -> None:
    rows = [
        {"event": "desktop_browser_job_attached", "fields": {"pid": 333}},
        {"event": "desktop_shell_window_shown_inferred", "fields": {"observed": False}},
    ]

    with mock.patch.object(smoke.desktop_app_mod, "is_process_alive", return_value=False):
        with pytest.raises(RuntimeError, match="live attached PID or visible window PID"):
            smoke._select_browser_shutdown_proof(rows)


def test_wait_for_packaged_runtime_with_port_pivot_prefers_env_scoped_session_root() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        expected_data_dir = root / "run-data"
        expected_data_dir.mkdir(parents=True, exist_ok=True)
        global_env = {"LOCALAPPDATA": str(root / "global-localappdata")}
        global_session_root = smoke.desktop_update_mod.resolve_desktop_session_root(global_env)
        global_session_root.mkdir(parents=True, exist_ok=True)
        (global_session_root / smoke.DESKTOP_SESSION_STATE_FILE).write_text(
            json.dumps({"sitePort": 7001, "bridgePort": 7002, "dataDir": str(root / "wrong-data")}),
            encoding="utf-8",
        )
        run_env = {"LOCALAPPDATA": str(root / "run-localappdata")}
        run_session_root = smoke.desktop_update_mod.resolve_desktop_session_root(run_env)
        run_session_root.mkdir(parents=True, exist_ok=True)
        (run_session_root / smoke.DESKTOP_SESSION_STATE_FILE).write_text(
            json.dumps({"sitePort": 9001, "bridgePort": 9002, "dataDir": str(expected_data_dir)}),
            encoding="utf-8",
        )
        process = mock.Mock(spec=subprocess.Popen)
        process.poll.return_value = None

        def fake_fetch_json(url: str, timeout_s: float = 10.0):  # noqa: ANN001
            if url == "http://127.0.0.1:9002/ops/health":
                return {"desktopMode": True, "startupReady": True}
            if url == "http://127.0.0.1:9002/desktop-local-data/session":
                return {"sitePort": 9001, "bridgePort": 9002}
            raise AssertionError(f"unexpected url {url}")

        with (
            mock.patch.dict(os.environ, global_env, clear=False),
            mock.patch.object(smoke, "fetch_json", side_effect=fake_fetch_json) as fetch_mock,
            mock.patch.object(
                smoke,
                "fetch_startup_metrics",
                return_value=[
                    {"event": "desktop_browser_process_spawn_started"},
                    {
                        "event": "desktop_browser_launch_selected",
                        "fields": {"mode": "chromium-app"},
                    },
                    {"event": "desktop_runtime_port_retry"},
                ],
            ),
            mock.patch.object(smoke, "_packaged_runtime_page_ready", return_value=True),
            mock.patch.object(smoke, "_required_startup_event_present", return_value=True),
        ):
            runtime_state = smoke.wait_for_packaged_runtime_with_port_pivot(
                process,
                requested_site_port=8080,
                requested_bridge_port=8877,
                expected_data_dir=expected_data_dir,
                timeout_s=0.2,
                env=run_env,
            )

        assert runtime_state["actualSitePort"] == 9001
        assert runtime_state["actualBridgePort"] == 9002
        assert runtime_state["portRetryObserved"] is True
        fetch_mock.assert_any_call("http://127.0.0.1:9002/ops/health", timeout_s=1.0)


def test_run_packaged_browser_job_rehearsal_passes_with_attached_pid_proof() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        artifacts_dir = root / "artifacts"
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        exe_path = root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        runtime_process = mock.Mock(spec=subprocess.Popen)
        stdout_handle = mock.Mock()
        stderr_handle = mock.Mock()

        with (
            mock.patch.object(smoke.os, "name", "nt"),
            mock.patch.object(
                smoke,
                "_select_packaged_browser_job_browser",
                return_value=(
                    {"browserName": "chrome", "browserPath": "C:/Chrome/chrome.exe"},
                    {smoke.desktop_app_mod.PREFERRED_BROWSER_PATH_ENV: "C:/Chrome/chrome.exe"},
                ),
            ),
            mock.patch.object(smoke, "choose_free_port", side_effect=[8080, 8877]),
            mock.patch.object(
                smoke,
                "launch_packaged_exe",
                return_value=(runtime_process, stdout_handle, stderr_handle),
            ) as launch_mock,
            mock.patch.object(
                smoke,
                "wait_for_packaged_runtime_with_port_pivot",
                return_value={
                    "actualSitePort": 8080,
                    "actualBridgePort": 8877,
                    "portRetryObserved": False,
                    "startupMetrics": [
                        {"event": "desktop_browser_process_spawn_started"},
                        {"event": "desktop_browser_job_attached", "fields": {"pid": 333}},
                        {"event": "desktop_browser_launch_accepted"},
                        {
                            "event": "desktop_browser_launch_selected",
                            "fields": {"mode": "chromium-app"},
                        },
                    ],
                },
            ),
            mock.patch.object(
                smoke,
                "_select_browser_shutdown_proof",
                return_value={
                    "proofSource": "attached-browser-pid",
                    "proofPid": 333,
                    "attachedPid": 333,
                    "windowPid": 0,
                },
            ),
            mock.patch.object(smoke.desktop_app_mod, "is_process_alive", return_value=True),
            mock.patch.object(smoke, "terminate_process_only") as terminate_only_mock,
            mock.patch.object(smoke, "_wait_for_pid_exit") as wait_pid_exit_mock,
            mock.patch.object(smoke, "terminate_process_tree"),
            mock.patch.object(smoke, "cleanup_orphaned_desktop_ports_nt"),
        ):
            payload = smoke.run_packaged_browser_job_rehearsal(
                exe_path=exe_path,
                artifacts_dir=artifacts_dir,
                runtime_timeout_s=5.0,
            )

    assert payload["status"] == "passed"
    assert payload["slug"] == "packaged-browser-job-rehearsal"
    assert payload["details"]["proofSource"] == "attached-browser-pid"
    assert payload["details"]["selectedBrowserName"] == "chrome"
    assert payload["details"]["attachedPid"] == 333
    assert payload["details"]["portRetryObserved"] is False
    assert launch_mock.call_args.kwargs["env"][
        smoke.desktop_app_mod.PREFERRED_BROWSER_PATH_ENV
    ] == ("C:/Chrome/chrome.exe")
    terminate_only_mock.assert_called_once_with(runtime_process)
    wait_pid_exit_mock.assert_called_once_with(333, timeout_s=15.0)


def test_run_packaged_browser_job_rehearsal_fails_when_attach_metric_is_missing() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        artifacts_dir = root / "artifacts"
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        exe_path = root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        runtime_process = mock.Mock(spec=subprocess.Popen)
        stdout_handle = mock.Mock()
        stderr_handle = mock.Mock()

        with (
            mock.patch.object(smoke.os, "name", "nt"),
            mock.patch.object(
                smoke,
                "_select_packaged_browser_job_browser",
                return_value=(
                    {"browserName": "chrome", "browserPath": "C:/Chrome/chrome.exe"},
                    {smoke.desktop_app_mod.PREFERRED_BROWSER_PATH_ENV: "C:/Chrome/chrome.exe"},
                ),
            ),
            mock.patch.object(smoke, "choose_free_port", side_effect=[8080, 8877]),
            mock.patch.object(
                smoke,
                "launch_packaged_exe",
                return_value=(runtime_process, stdout_handle, stderr_handle),
            ),
            mock.patch.object(
                smoke,
                "wait_for_packaged_runtime_with_port_pivot",
                return_value={
                    "actualSitePort": 9001,
                    "actualBridgePort": 9002,
                    "portRetryObserved": True,
                    "startupMetrics": [
                        {"event": "desktop_browser_process_spawn_started"},
                        {"event": "desktop_browser_launch_accepted"},
                        {
                            "event": "desktop_browser_launch_selected",
                            "fields": {"mode": "chromium-app"},
                        },
                        {"event": "desktop_runtime_port_retry"},
                    ],
                },
            ),
            mock.patch.object(smoke, "terminate_process_tree"),
            mock.patch.object(smoke, "cleanup_orphaned_desktop_ports_nt"),
        ):
            payload = smoke.run_packaged_browser_job_rehearsal(
                exe_path=exe_path,
                artifacts_dir=artifacts_dir,
                runtime_timeout_s=5.0,
            )

    assert payload["status"] == "failed"
    assert "desktop_browser_job_attached" in payload["error"]
    assert payload["details"]["portRetryObserved"] is True
    assert payload["details"]["actualBridgePort"] == 9002


def test_run_packaged_smoke_can_run_browser_job_rehearsal_mode() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        report_path = root / "data" / "latest.json"
        artifacts_dir = root / "artifacts"
        exe_path = root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        args = smoke.parse_args(
            [
                "--exe-path",
                str(exe_path),
                "--report-path",
                str(report_path),
                "--artifacts-dir",
                str(artifacts_dir),
                "--browser-job-rehearsal",
            ]
        )
        with (
            mock.patch.object(smoke, "ensure_portable_exe", return_value=exe_path),
            mock.patch.object(
                smoke,
                "collect_packaged_smoke_env_diagnostics",
                return_value={"tmp": "C:/tmp", "temp": "C:/tmp", "isElevated": False},
            ),
            mock.patch.object(
                smoke,
                "run_packaged_browser_job_rehearsal",
                return_value={
                    "name": "Packaged browser job rehearsal",
                    "slug": "packaged-browser-job-rehearsal",
                    "status": "passed",
                    "durationMs": 1200,
                    "error": "",
                    "details": {
                        "runtimeStdout": str(artifacts_dir / "browser-job-runtime.stdout.log"),
                        "runtimeStderr": str(artifacts_dir / "browser-job-runtime.stderr.log"),
                        "startupMetrics": str(artifacts_dir / "browser-job.startup-metrics.json"),
                    },
                },
            ) as rehearsal_mock,
        ):
            payload = smoke.run_packaged_smoke(args)

    assert payload["ok"] is True
    assert payload["scenarios"][0]["slug"] == "packaged-browser-job-rehearsal"
    assert payload["artifacts"]["browserJobRehearsalRuntimeStdout"] == str(
        artifacts_dir / "browser-job-runtime.stdout.log"
    )
    assert payload["artifacts"]["browserJobRehearsalStartupMetrics"] == str(
        artifacts_dir / "browser-job.startup-metrics.json"
    )
    rehearsal_mock.assert_called_once()


def test_run_packaged_orphan_reclaim_rehearsal_passes_when_metrics_prove_reclaim() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        artifacts_dir = root / "artifacts"
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        exe_path = root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        stale_site_process = mock.Mock(spec=subprocess.Popen)
        stale_site_process.pid = 101
        stale_bridge_process = mock.Mock(spec=subprocess.Popen)
        stale_bridge_process.pid = 202
        runtime_process = mock.Mock(spec=subprocess.Popen)
        handle_a = mock.Mock()
        handle_b = mock.Mock()
        handle_c = mock.Mock()
        handle_d = mock.Mock()
        handle_e = mock.Mock()
        handle_f = mock.Mock()

        with (
            mock.patch.object(smoke, "choose_free_port", side_effect=[8080, 8877]),
            mock.patch.object(
                smoke,
                "launch_packaged_desktop_child",
                side_effect=[
                    (stale_site_process, handle_a, handle_b),
                    (stale_bridge_process, handle_c, handle_d),
                ],
            ),
            mock.patch.object(smoke, "wait_for_packaged_child_runtime"),
            mock.patch.object(
                smoke,
                "launch_packaged_exe",
                return_value=(runtime_process, handle_e, handle_f),
            ),
            mock.patch.object(
                smoke,
                "wait_for_packaged_runtime_with_port_pivot",
                return_value={
                    "actualSitePort": 8080,
                    "actualBridgePort": 8877,
                    "portRetryObserved": False,
                    "startupMetrics": [
                        {"event": "desktop_stale_runtime_reclaim_started"},
                        {
                            "event": "desktop_stale_runtime_reclaim_result",
                            "fields": {"target": "bridge", "outcome": "killed"},
                        },
                        {
                            "event": "desktop_stale_runtime_reclaim_result",
                            "fields": {"target": "site", "outcome": "killed"},
                        },
                    ],
                },
            ),
            mock.patch.object(smoke, "_wait_for_process_exit") as wait_exit_mock,
            mock.patch.object(smoke, "terminate_process_tree"),
            mock.patch.object(smoke, "cleanup_orphaned_desktop_ports_nt"),
        ):
            payload = smoke.run_packaged_orphan_reclaim_rehearsal(
                exe_path=exe_path,
                artifacts_dir=artifacts_dir,
                runtime_timeout_s=5.0,
            )

    assert payload["status"] == "passed"
    assert payload["slug"] == "packaged-orphan-reclaim-rehearsal"
    assert payload["details"]["actualSitePort"] == 8080
    assert payload["details"]["actualBridgePort"] == 8877
    assert payload["details"]["portRetryObserved"] is False
    wait_exit_mock.assert_any_call(stale_site_process, timeout_s=15.0)
    wait_exit_mock.assert_any_call(stale_bridge_process, timeout_s=15.0)


def test_run_packaged_orphan_reclaim_rehearsal_fails_when_site_reclaim_metric_is_missing() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        artifacts_dir = root / "artifacts"
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        exe_path = root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        stale_site_process = mock.Mock(spec=subprocess.Popen)
        stale_site_process.pid = 101
        stale_bridge_process = mock.Mock(spec=subprocess.Popen)
        stale_bridge_process.pid = 202
        runtime_process = mock.Mock(spec=subprocess.Popen)
        handle_a = mock.Mock()
        handle_b = mock.Mock()
        handle_c = mock.Mock()
        handle_d = mock.Mock()
        handle_e = mock.Mock()
        handle_f = mock.Mock()

        with (
            mock.patch.object(smoke, "choose_free_port", side_effect=[8080, 8877]),
            mock.patch.object(
                smoke,
                "launch_packaged_desktop_child",
                side_effect=[
                    (stale_site_process, handle_a, handle_b),
                    (stale_bridge_process, handle_c, handle_d),
                ],
            ),
            mock.patch.object(smoke, "wait_for_packaged_child_runtime"),
            mock.patch.object(
                smoke,
                "launch_packaged_exe",
                return_value=(runtime_process, handle_e, handle_f),
            ),
            mock.patch.object(
                smoke,
                "wait_for_packaged_runtime_with_port_pivot",
                return_value={
                    "actualSitePort": 8080,
                    "actualBridgePort": 8877,
                    "portRetryObserved": False,
                    "startupMetrics": [
                        {"event": "desktop_stale_runtime_reclaim_started"},
                        {
                            "event": "desktop_stale_runtime_reclaim_result",
                            "fields": {"target": "bridge", "outcome": "killed"},
                        },
                    ],
                },
            ),
            mock.patch.object(smoke, "_wait_for_process_exit"),
            mock.patch.object(smoke, "terminate_process_tree"),
            mock.patch.object(smoke, "cleanup_orphaned_desktop_ports_nt"),
        ):
            payload = smoke.run_packaged_orphan_reclaim_rehearsal(
                exe_path=exe_path,
                artifacts_dir=artifacts_dir,
                runtime_timeout_s=5.0,
            )

    assert payload["status"] == "failed"
    assert "site reclaim" in payload["error"]


def test_run_packaged_orphan_reclaim_rehearsal_fails_when_runtime_retries_ports() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        artifacts_dir = root / "artifacts"
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        exe_path = root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        stale_site_process = mock.Mock(spec=subprocess.Popen)
        stale_site_process.pid = 101
        stale_bridge_process = mock.Mock(spec=subprocess.Popen)
        stale_bridge_process.pid = 202
        runtime_process = mock.Mock(spec=subprocess.Popen)
        handle_a = mock.Mock()
        handle_b = mock.Mock()
        handle_c = mock.Mock()
        handle_d = mock.Mock()
        handle_e = mock.Mock()
        handle_f = mock.Mock()

        with (
            mock.patch.object(smoke, "choose_free_port", side_effect=[8080, 8877]),
            mock.patch.object(
                smoke,
                "launch_packaged_desktop_child",
                side_effect=[
                    (stale_site_process, handle_a, handle_b),
                    (stale_bridge_process, handle_c, handle_d),
                ],
            ),
            mock.patch.object(smoke, "wait_for_packaged_child_runtime"),
            mock.patch.object(
                smoke,
                "launch_packaged_exe",
                return_value=(runtime_process, handle_e, handle_f),
            ),
            mock.patch.object(
                smoke,
                "wait_for_packaged_runtime_with_port_pivot",
                return_value={
                    "actualSitePort": 9001,
                    "actualBridgePort": 9002,
                    "portRetryObserved": True,
                    "startupMetrics": [
                        {"event": "desktop_stale_runtime_reclaim_started"},
                        {
                            "event": "desktop_stale_runtime_reclaim_result",
                            "fields": {"target": "bridge", "outcome": "killed"},
                        },
                        {
                            "event": "desktop_stale_runtime_reclaim_result",
                            "fields": {"target": "site", "outcome": "killed"},
                        },
                    ],
                },
            ),
            mock.patch.object(smoke, "_wait_for_process_exit"),
            mock.patch.object(smoke, "terminate_process_tree"),
            mock.patch.object(smoke, "cleanup_orphaned_desktop_ports_nt"),
        ):
            payload = smoke.run_packaged_orphan_reclaim_rehearsal(
                exe_path=exe_path,
                artifacts_dir=artifacts_dir,
                runtime_timeout_s=5.0,
            )

    assert payload["status"] == "failed"
    assert "requested ports" in payload["error"]


def test_run_packaged_smoke_can_run_orphan_reclaim_rehearsal_mode() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        report_path = root / "data" / "latest.json"
        artifacts_dir = root / "artifacts"
        exe_path = root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        args = smoke.parse_args(
            [
                "--exe-path",
                str(exe_path),
                "--report-path",
                str(report_path),
                "--artifacts-dir",
                str(artifacts_dir),
                "--orphan-reclaim-rehearsal",
            ]
        )
        with (
            mock.patch.object(smoke, "ensure_portable_exe", return_value=exe_path),
            mock.patch.object(
                smoke,
                "collect_packaged_smoke_env_diagnostics",
                return_value={"tmp": "C:/tmp", "temp": "C:/tmp", "isElevated": False},
            ),
            mock.patch.object(
                smoke,
                "run_packaged_orphan_reclaim_rehearsal",
                return_value={
                    "name": "Packaged orphan reclaim rehearsal",
                    "slug": "packaged-orphan-reclaim-rehearsal",
                    "status": "passed",
                    "durationMs": 1200,
                    "error": "",
                    "details": {
                        "runtimeStdout": str(artifacts_dir / "orphan-runtime.stdout.log"),
                        "runtimeStderr": str(artifacts_dir / "orphan-runtime.stderr.log"),
                        "staleSiteStdout": str(artifacts_dir / "orphan-site.stdout.log"),
                        "staleSiteStderr": str(artifacts_dir / "orphan-site.stderr.log"),
                        "staleBridgeStdout": str(artifacts_dir / "orphan-bridge.stdout.log"),
                        "staleBridgeStderr": str(artifacts_dir / "orphan-bridge.stderr.log"),
                    },
                },
            ) as rehearsal_mock,
        ):
            payload = smoke.run_packaged_smoke(args)
        assert payload["ok"] is True
        assert payload["scenarios"][0]["slug"] == "packaged-orphan-reclaim-rehearsal"
        assert payload["artifacts"]["orphanRehearsalRuntimeStdout"] == str(
            artifacts_dir / "orphan-runtime.stdout.log"
        )
        assert payload["artifacts"]["orphanRehearsalSiteStdout"] == str(
            artifacts_dir / "orphan-site.stdout.log"
        )
        assert payload["artifacts"]["orphanRehearsalBridgeStdout"] == str(
            artifacts_dir / "orphan-bridge.stdout.log"
        )
        rehearsal_mock.assert_called_once()
        saved = json.loads(report_path.read_text(encoding="utf-8"))
        assert saved["ok"] is True


def test_wait_for_relaunched_runtime_prefers_explicit_session_env_over_global_state() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        expected_data_dir = root / "portable" / "ship" / "data"
        expected_data_dir.mkdir(parents=True, exist_ok=True)
        global_env = {"LOCALAPPDATA": str(root / "global-localappdata")}
        global_session_root = smoke.desktop_update_mod.resolve_desktop_session_root(global_env)
        (global_session_root / smoke.DESKTOP_SESSION_STATE_FILE).write_text(
            json.dumps({"bridgePort": 8877, "dataDir": str(root / "wrong-data")}),
            encoding="utf-8",
        )
        run_env = {"LOCALAPPDATA": str(root / "run-localappdata")}
        run_session_root = smoke.desktop_update_mod.resolve_desktop_session_root(run_env)
        (run_session_root / smoke.DESKTOP_SESSION_STATE_FILE).write_text(
            json.dumps({"bridgePort": 4567, "dataDir": str(expected_data_dir)}),
            encoding="utf-8",
        )

        with (
            mock.patch.dict(os.environ, global_env, clear=False),
            mock.patch.object(
                smoke,
                "fetch_json",
                return_value={
                    "desktopMode": True,
                    "startupReady": True,
                    "appVersion": "0.1.22",
                },
            ) as fetch_mock,
        ):
            relaunched = smoke._wait_for_relaunched_runtime(
                expected_data_dir=expected_data_dir,
                expected_version="0.1.22",
                timeout_s=0.1,
                env=run_env,
            )

        assert relaunched["session"]["bridgePort"] == 4567
        fetch_mock.assert_called_once_with("http://127.0.0.1:4567/ops/health", timeout_s=5.0)


def test_run_desktop_update_rehearsal_clears_session_state_only_after_runtime_exit() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        portable_root = root / "portable"
        (portable_root / "ship" / "data").mkdir(parents=True, exist_ok=True)
        exe_path = portable_root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        (portable_root / "BaluffoUpdater.exe").write_text("helper", encoding="utf-8")
        paths = smoke.desktop_update_mod.DesktopUpdatePaths.from_data_dir(
            root / "artifacts" / "portable-install" / "ship" / "data"
        )
        process = mock.Mock()
        process.poll.return_value = None
        stdout_handle = mock.Mock()
        stderr_handle = mock.Mock()
        captured_env: dict[str, str] = {}
        session_state_path: Path | None = None

        def fake_archive_portable_dir(_portable_root: Path, output_path: Path) -> Path:
            output_path.write_bytes(b"portable-update")
            return output_path

        def fake_launch_packaged_exe(*args, **kwargs):  # noqa: ANN002, ANN003
            captured_env.update(kwargs.get("env") or {})
            return process, stdout_handle, stderr_handle

        def fake_wait_for_packaged_runtime(*args, **kwargs):  # noqa: ANN002, ANN003
            nonlocal session_state_path
            session_root = smoke.desktop_update_mod.resolve_desktop_session_root(captured_env)
            session_root.mkdir(parents=True, exist_ok=True)
            session_state_path = session_root / smoke.DESKTOP_SESSION_STATE_FILE
            session_state_path.write_text(
                json.dumps({"launcherPid": 6060, "launcherToken": "token"}),
                encoding="utf-8",
            )
            return {}

        def fake_wait_for_process_exit(*args, **kwargs):  # noqa: ANN002, ANN003
            assert session_state_path is not None
            assert session_state_path.exists()
            assert paths.handoff_request_path.exists()

        def fake_wait_for_relaunched_runtime(*args, **kwargs):  # noqa: ANN002, ANN003
            assert session_state_path is not None
            assert not session_state_path.exists()
            return {"session": {"launcherPid": 7001, "bridgePort": 7002, "sitePort": 7003}}

        def fake_request_json(url: str, *, timeout_s: float = 10.0, **kwargs):  # noqa: ANN001, ANN003
            assert "/app/update-status" in url
            paths.handoff_request_path.parent.mkdir(parents=True, exist_ok=True)
            paths.handoff_request_path.write_text("{}", encoding="utf-8")
            return 200, {
                "downloadState": "downloaded",
                "installState": "handoff_requested",
                "installStage": "preparing",
            }

        with (
            mock.patch.object(smoke, "_inject_desktop_update_public_keys"),
            mock.patch.object(smoke, "_seed_rehearsal_local_data", return_value={}),
            mock.patch.object(
                smoke,
                "_archive_portable_dir",
                side_effect=fake_archive_portable_dir,
            ),
            mock.patch.object(
                smoke,
                "_start_desktop_update_release_server",
                return_value=("http://127.0.0.1:63092", mock.Mock(), mock.Mock()),
            ),
            mock.patch.object(
                smoke,
                "packaged_runtime_env_overrides",
                return_value={"LOCALAPPDATA": str(root / "desktop-localappdata")},
            ),
            mock.patch.object(smoke, "_preferred_desktop_browser_env", return_value={}),
            mock.patch.object(smoke, "clear_packaged_desktop_session_state"),
            mock.patch.object(smoke, "choose_free_port", side_effect=[63093, 63094]),
            mock.patch.object(
                smoke,
                "launch_packaged_exe",
                side_effect=fake_launch_packaged_exe,
            ),
            mock.patch.object(
                smoke,
                "wait_for_packaged_runtime",
                side_effect=fake_wait_for_packaged_runtime,
            ),
            mock.patch.object(
                smoke,
                "post_json",
                side_effect=[
                    (
                        200,
                        {"status": {"updateAvailable": True, "availability": "available"}},
                    ),
                    (
                        200,
                        {
                            "started": True,
                            "status": {"downloadState": "downloaded", "installState": "ready"},
                        },
                    ),
                    (200, {"started": True, "exitRequested": True}),
                ],
            ),
            mock.patch.object(smoke, "request_json", side_effect=fake_request_json),
            mock.patch.object(
                smoke, "_wait_for_process_exit", side_effect=fake_wait_for_process_exit
            ),
            mock.patch.object(
                smoke,
                "_wait_for_relaunched_runtime",
                side_effect=fake_wait_for_relaunched_runtime,
            ),
            mock.patch.object(smoke, "_verify_rehearsal_local_data"),
            mock.patch.object(smoke, "_assert_desktop_update_helper_succeeded"),
            mock.patch.object(smoke, "terminate_process_tree"),
            mock.patch.object(smoke, "cleanup_orphaned_desktop_ports_nt"),
        ):
            result = smoke.run_desktop_update_rehearsal(
                exe_path=exe_path,
                artifacts_dir=root / "artifacts",
                runtime_timeout_s=5.0,
            )

        assert result["status"] == "passed"
        assert captured_env["BALUFFO_DESKTOP_UPDATER_NO_DIALOG"] == "1"
        assert captured_env["BALUFFO_DESKTOP_UPDATER_VERIFY_TIMEOUT_S"] == "10"


def test_assert_desktop_update_helper_succeeded_rejects_failed_helper_stdout() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = smoke.desktop_update_mod.DesktopUpdatePaths.from_data_dir(data_dir)
        paths.helper_stdout_log_path.parent.mkdir(parents=True, exist_ok=True)
        paths.helper_stdout_log_path.write_text(
            json.dumps({"ok": False, "error": "boom"}),
            encoding="utf-8",
        )

        with pytest.raises(RuntimeError, match="Update helper reported failure"):
            smoke._assert_desktop_update_helper_succeeded(
                paths=paths,
                relaunch_bridge_port=0,
            )


def test_assert_desktop_update_helper_succeeded_ignores_malformed_diagnostics_lines() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = smoke.desktop_update_mod.DesktopUpdatePaths.from_data_dir(data_dir)
        paths.helper_diagnostics_log_path.parent.mkdir(parents=True, exist_ok=True)
        paths.helper_diagnostics_log_path.write_text(
            "\n".join(
                [
                    '{"event": "helper_main_started"}',
                    "}}",
                    '{"event": "helper_main_succeeded"}',
                ]
            ),
            encoding="utf-8",
        )

        smoke._assert_desktop_update_helper_succeeded(
            paths=paths,
            relaunch_bridge_port=0,
        )


def test_run_packaged_smoke_classifies_spawn_failure_from_node_runner() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        report_path = root / "data" / "latest.json"
        artifacts_dir = root / "artifacts"
        exe_path = root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        process = mock.Mock()
        process.pid = 999
        process.poll.return_value = None
        stdout_handle = mock.Mock()
        stderr_handle = mock.Mock()
        args = smoke.parse_args(
            [
                "--exe-path",
                str(exe_path),
                "--report-path",
                str(report_path),
                "--artifacts-dir",
                str(artifacts_dir),
            ]
        )
        with (
            mock.patch.object(smoke, "ensure_portable_exe", return_value=exe_path),
            mock.patch.object(
                smoke, "launch_packaged_exe", return_value=(process, stdout_handle, stderr_handle)
            ),
            mock.patch.object(
                smoke,
                "wait_for_packaged_runtime",
                return_value={
                    "health": {"ok": True},
                    "session": {"ok": True},
                    "startupMetrics": [],
                },
            ),
            mock.patch.object(
                smoke,
                "capture_runtime_snapshot",
                return_value={},
            ),
            mock.patch.object(
                smoke,
                "run_packaged_node_smoke",
                return_value={
                    "exitCode": 1,
                    "reportPath": str(artifacts_dir / "smoke-report.json"),
                    "outputDir": str(artifacts_dir / "smoke-output"),
                    "scenarios": [],
                    "failureCategory": "node_process_spawn_blocked",
                    "runnerError": "spawn EPERM",
                    "environment": {"tmp": "C:/tmp", "temp": "C:/tmp", "isElevated": True},
                },
            ),
            mock.patch.object(smoke, "terminate_process_tree"),
        ):
            payload = smoke.run_packaged_smoke(args)
        assert not payload["ok"]
        assert payload["failure"]["step"] == "playwright"
        assert payload["failure"]["category"] == "node_process_spawn_blocked"
        assert payload["failure"]["message"] == "spawn EPERM"
        assert payload["environment"]["isElevated"] is True


def test_run_packaged_smoke_fails_when_embedded_probe_fails() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        report_path = root / "data" / "latest.json"
        artifacts_dir = root / "artifacts"
        exe_path = root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        args = smoke.parse_args(
            [
                "--exe-path",
                str(exe_path),
                "--report-path",
                str(report_path),
                "--artifacts-dir",
                str(artifacts_dir),
                "--embedded-probes",
            ]
        )
        failing_probe = {
            "name": "Embedded Jobs Ready",
            "status": "failed",
            "durationMs": 2500,
            "error": "Missing embedded runtime events: jobs_auth_ready",
            "startupProfile": {},
        }
        with (
            mock.patch.object(smoke, "ensure_portable_exe", return_value=exe_path),
            mock.patch.object(smoke, "run_embedded_runtime_probe", return_value=failing_probe),
            mock.patch.object(smoke, "terminate_process_tree") as terminate_mock,
        ):
            payload = smoke.run_packaged_smoke(args)
        assert not payload["ok"]
        assert payload["scenarios"] == [failing_probe, failing_probe, failing_probe]
        assert payload["failure"]["step"] == "runner"
        assert "Embedded Jobs Ready failed" in payload["failure"]["message"]
        terminate_mock.assert_called_once_with(None)


def test_classify_startup_probe_failure_uses_explicit_handoff_failure_category() -> None:
    rows = [
        {
            "event": "desktop_browser_launch_selected",
            "fields": {
                "browser": "chrome",
                "browserPath": "C:/Chrome/chrome.exe",
                "mode": "chromium-app",
            },
        },
        {"event": "desktop_browser_watchdog_handoff_failed", "fields": {}},
    ]

    classification, category = smoke.classify_startup_probe_failure(
        rows,
        error_message="startup markers never arrived",
        summary={"missingEvents": ["jobs_first_render", "jobs_first_interactive"]},
    )

    assert classification == "browser handoff/runtime startup failed"
    assert category == "browser_handoff_runtime_startup_failed"


def test_classify_startup_probe_failure_treats_confirmed_handoff_then_bridge_loss_as_runtime_failure() -> (
    None
):
    rows = [
        {
            "event": "desktop_browser_launch_selected",
            "fields": {
                "browser": "chrome",
                "browserPath": "C:/Chrome/chrome.exe",
                "mode": "chromium-app",
            },
        },
        {
            "event": "desktop_browser_watchdog_handoff_confirmed",
            "fields": {"evidence": "startup_metric"},
        },
        {"event": "desktop_window_closed", "fields": {"reason": "bridge_exit"}},
    ]

    classification, category = smoke.classify_startup_probe_failure(
        rows,
        error_message="[WinError 10054] An existing connection was forcibly closed",
        summary={"missingEvents": ["jobs_first_render", "jobs_first_interactive"]},
    )

    assert classification == "browser runtime startup failed"
    assert category == "browser_runtime_startup_failed"
