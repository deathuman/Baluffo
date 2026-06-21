"""Packaged desktop rehearsal tests for completion classification."""

from ._rehearsal_shared import (
    Path,
    json,
    mock,
    os,
    pytest,
    smoke,
    workspace_tmpdir,
)

pytestmark = [pytest.mark.packaging, pytest.mark.slow]


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


@pytest.mark.windows
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

        def fake_post_json(url: str, *args, **kwargs):  # noqa: ANN002, ANN003
            if "/app/check-for-update" in url:
                return 200, {"status": {"updateAvailable": True, "availability": "available"}}
            if "/app/download-update" in url:
                return 200, {
                    "started": True,
                    "status": {"downloadState": "downloaded", "installState": "ready"},
                }
            assert "/app/install-update" in url
            paths.handoff_request_path.parent.mkdir(parents=True, exist_ok=True)
            paths.handoff_request_path.write_text("{}", encoding="utf-8")
            paths.install_state_path.write_text(
                json.dumps(
                    {
                        "downloadState": "downloaded",
                        "installState": "handoff_requested",
                        "installStage": "preparing",
                    }
                ),
                encoding="utf-8",
            )
            raise ConnectionResetError("[WinError 10054] reset after handoff")

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
                return_value={
                    "APPDATA": str(root / "desktop-appdata"),
                    "LOCALAPPDATA": str(root / "desktop-localappdata"),
                },
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
                side_effect=fake_post_json,
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
            mock.patch.object(smoke, "_wait_for_desktop_update_helper_completion") as wait_helper,
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
        wait_helper.assert_called_once()
        assert wait_helper.call_args.kwargs["paths"].install_state_path == paths.install_state_path
        assert wait_helper.call_args.kwargs["timeout_s"] >= 30.0
        assert captured_env["APPDATA"] == str(root / "desktop-appdata")
        assert captured_env["LOCALAPPDATA"] == str(root / "desktop-localappdata")
        assert captured_env["BALUFFO_DESKTOP_UPDATER_NO_DIALOG"] == "1"
        assert captured_env["BALUFFO_DESKTOP_UPDATER_VERIFY_TIMEOUT_S"] == "30"


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


def test_wait_for_desktop_update_helper_completion_accepts_success_diagnostics() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = smoke.desktop_update_mod.DesktopUpdatePaths.from_data_dir(data_dir)
        paths.helper_diagnostics_log_path.parent.mkdir(parents=True, exist_ok=True)
        paths.helper_diagnostics_log_path.write_text(
            json.dumps({"event": "helper_main_succeeded", "fields": {"installedVersion": "1.4.0"}}),
            encoding="utf-8",
        )

        smoke._wait_for_desktop_update_helper_completion(paths=paths, timeout_s=1.0)


def test_wait_for_desktop_update_helper_completion_rejects_failed_diagnostics() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = smoke.desktop_update_mod.DesktopUpdatePaths.from_data_dir(data_dir)
        paths.helper_diagnostics_log_path.parent.mkdir(parents=True, exist_ok=True)
        paths.helper_diagnostics_log_path.write_text(
            json.dumps(
                {
                    "event": "helper_main_failed",
                    "fields": {"error": "Updated desktop app did not report startup readiness"},
                }
            ),
            encoding="utf-8",
        )

        with pytest.raises(
            RuntimeError,
            match="Updated desktop app did not report startup readiness",
        ):
            smoke._wait_for_desktop_update_helper_completion(paths=paths, timeout_s=1.0)


def test_wait_for_desktop_update_helper_completion_rejects_failed_install_state() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = smoke.desktop_update_mod.DesktopUpdatePaths.from_data_dir(data_dir)
        paths.install_state_path.parent.mkdir(parents=True, exist_ok=True)
        paths.install_state_path.write_text(
            json.dumps(
                {
                    "installState": "failed",
                    "installStage": "failed",
                    "lastError": "desktop_update_relaunch_verification_failed",
                }
            ),
            encoding="utf-8",
        )

        with pytest.raises(RuntimeError, match="failed updater state"):
            smoke._wait_for_desktop_update_helper_completion(paths=paths, timeout_s=1.0)


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
