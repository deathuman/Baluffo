"""Packaged desktop rehearsal tests for browser launch."""

from ._rehearsal_shared import (
    ADMIN_BRIDGE_TEST_PORT,
    Path,
    mock,
    pytest,
    runtime_process,
    smoke,
    subprocess,
    workspace_tmpdir,
)

pytestmark = [pytest.mark.packaging, pytest.mark.slow]


def test_select_packaged_browser_job_browser_enables_edge_when_needed() -> None:
    with (
        mock.patch.object(
            smoke,
            "select_startup_probe_browser",
            side_effect=[
                RuntimeError("No supported managed Chromium probe browser available."),
                {"browserName": "msedge", "browserPath": "C:/Edge/msedge.exe"},
            ],
        ),
        mock.patch.object(
            smoke,
            "preferred_packaged_desktop_browser_env",
            return_value={},
        ),
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
                port=ADMIN_BRIDGE_TEST_PORT,
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


def test_launch_packaged_exe_can_pass_owner_idle_timeout_override() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        exe_path = root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        process = mock.Mock(spec=subprocess.Popen)

        with mock.patch.object(runtime_process.subprocess, "Popen", return_value=process) as popen:
            returned_process, stdout_handle, stderr_handle = runtime_process.launch_packaged_exe(
                exe_path,
                site_port=8080,
                bridge_port=ADMIN_BRIDGE_TEST_PORT,
                data_dir=root / "data",
                stdout_path=root / "stdout.log",
                stderr_path=root / "stderr.log",
                owner_idle_timeout_s=7.5,
            )
            stdout_handle.close()
            stderr_handle.close()

    assert returned_process is process
    command = popen.call_args.args[0]
    assert "--owner-idle-timeout-s" in command
    assert command[command.index("--owner-idle-timeout-s") + 1] == "7.5"


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
        with pytest.raises(RuntimeError) as exc_info:
            smoke._select_browser_shutdown_proof(rows)
    error = str(exc_info.value)
    assert "live attached PID or visible window PID" in error
    assert "attachedPid=333" in error
    assert "attachedAlive=False" in error
    assert "windowPid=0" in error
    assert "desktop_browser_job_attached" in error
