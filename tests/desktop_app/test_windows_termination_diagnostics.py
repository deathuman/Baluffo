import subprocess
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest

from src.ship import desktop_app

from ._helpers import _patch_windows_compat_facade, _patch_windows_desktop_app


@pytest.mark.windows
def test_windows_terminate_process_details_falls_back_to_windows_api() -> None:
    kernel32 = SimpleNamespace(
        OpenProcess=mock.Mock(return_value=55),
        TerminateProcess=mock.Mock(return_value=1),
        WaitForSingleObject=mock.Mock(return_value=0),
        CloseHandle=mock.Mock(),
    )

    with (
        _patch_windows_desktop_app(kernel32),
        mock.patch.object(
            desktop_app.subprocess,
            "run",
            return_value=subprocess.CompletedProcess(["taskkill"], 0, stdout="ok", stderr=""),
        ),
        mock.patch.object(desktop_app, "_wait_for_process_exit_pid", return_value=False),
        mock.patch.object(desktop_app, "is_process_alive", return_value=False),
    ):
        details = desktop_app._windows_terminate_process_tree_details_by_pid(323)

    assert details["taskkillAttempted"] is True
    assert details["taskkillReturnCode"] == 0
    assert details["taskkillExited"] is False
    assert details["fallbackMethod"] == "windows-api"
    assert details["windowsApiTerminateOk"] is True
    assert details["terminated"] is True


@pytest.mark.windows
def test_stale_bridge_reclaim_failure_includes_termination_diagnostics() -> None:
    trace_mock = mock.Mock()
    termination = {
        "pid": 202,
        "terminated": False,
        "taskkillReturnCode": 1,
        "taskkillStderr": "denied",
        "fallbackMethod": "windows-api",
        "windowsApiErrorCode": 5,
    }

    with (
        _patch_windows_compat_facade(),
        mock.patch.object(desktop_app, "_append_startup_trace", trace_mock),
        mock.patch.object(
            desktop_app,
            "_pids_listening_on_tcp_port_windows",
            side_effect=[{202}, {202}],
        ),
        mock.patch.object(desktop_app, "is_process_alive", return_value=True),
        mock.patch.object(
            desktop_app,
            "get_baluffo_bridge_health",
            return_value={
                "service": "baluffo-bridge",
                "desktopMode": True,
                "owner": {"token": "owner-token"},
            },
        ),
        mock.patch.object(desktop_app, "_windows_process_image_matches", return_value=True),
        mock.patch.object(
            desktop_app,
            "_windows_terminate_process_tree_details_by_pid",
            return_value=termination,
        ),
    ):
        result = desktop_app._windows_try_reclaim_stale_bridge_process(
            {
                "bridgePort": 8877,
                "bridgePid": 202,
                "desktopOwnerToken": "owner-token",
                "exePath": "C:/tmp/Baluffo.exe",
            },
            data_dir=Path("C:/tmp/baluffo-ship/data"),
        )

    assert result["status"] == "failed"
    assert result["listenerPidsAfter"] == [202]
    assert result["taskkillStderr"] == "denied"
    assert result["targetAliveBefore"] is True
    trace_fields = trace_mock.call_args.kwargs
    assert trace_fields["reason"] == "bridge_termination_failed"
    assert trace_fields["listenerPidsBefore"] == [202]
    assert trace_fields["listenerPidsAfter"] == [202]
    assert trace_fields["targetAliveBefore"] is True
    assert trace_fields["windowsApiErrorCode"] == 5
