from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest

from src.ship import desktop_app

from ._helpers import desktop_runtime_config


@pytest.mark.windows
def test_launch_desktop_app_does_not_retry_unexpected_runtime_launch_bug() -> None:
    config = desktop_runtime_config(data_dir=Path("C:/tmp/baluffo-ship/data"))
    start_child_process = mock.Mock(return_value=SimpleNamespace(pid=101))

    with (
        mock.patch.object(desktop_app, "get_valid_session_state", return_value={}),
        mock.patch.object(
            desktop_app,
            "acquire_instance_lock",
            return_value=desktop_app.InstanceLock(Path("C:/tmp/desktop.lock"), 1),
        ),
        mock.patch.object(desktop_app, "release_instance_lock"),
        mock.patch.object(desktop_app, "resolve_runtime_ports", return_value=config),
        mock.patch.object(desktop_app, "ensure_runtime_ports"),
        mock.patch.object(desktop_app, "start_child_process", start_child_process),
        mock.patch.object(
            desktop_app,
            "wait_for_url",
            side_effect=AssertionError("unexpected site wait bug"),
        ),
        mock.patch.object(desktop_app, "_windows_create_kill_on_close_job", return_value=11),
        mock.patch.object(desktop_app, "_windows_close_desktop_job"),
        mock.patch.object(desktop_app, "clear_session_state"),
        mock.patch.object(desktop_app, "terminate_process"),
        mock.patch.object(desktop_app, "_append_startup_trace"),
        mock.patch.object(desktop_app, "_write_launch_diagnostics"),
        mock.patch.object(desktop_app, "_should_retry_runtime_launch") as should_retry_mock,
    ):
        with pytest.raises(AssertionError, match="unexpected site wait bug"):
            desktop_app.launch_desktop_app(config)

    should_retry_mock.assert_not_called()
    start_child_process.assert_called_once()


@pytest.mark.windows
def test_launch_desktop_app_records_diagnostics_before_reraising_base_exception() -> None:
    config = desktop_runtime_config(data_dir=Path("C:/tmp/baluffo-ship/data"))
    start_child_process = mock.Mock(return_value=SimpleNamespace(pid=101))

    with (
        mock.patch.object(desktop_app, "get_valid_session_state", return_value={}),
        mock.patch.object(
            desktop_app,
            "acquire_instance_lock",
            return_value=desktop_app.InstanceLock(Path("C:/tmp/desktop.lock"), 1),
        ),
        mock.patch.object(desktop_app, "release_instance_lock"),
        mock.patch.object(desktop_app, "resolve_runtime_ports", return_value=config),
        mock.patch.object(desktop_app, "ensure_runtime_ports"),
        mock.patch.object(desktop_app, "start_child_process", start_child_process),
        mock.patch.object(
            desktop_app,
            "wait_for_url",
            side_effect=KeyboardInterrupt("interrupted startup"),
        ),
        mock.patch.object(desktop_app, "_windows_create_kill_on_close_job", return_value=11),
        mock.patch.object(desktop_app, "_windows_close_desktop_job"),
        mock.patch.object(desktop_app, "clear_session_state"),
        mock.patch.object(desktop_app, "terminate_process"),
        mock.patch.object(desktop_app, "_append_startup_trace") as trace_mock,
        mock.patch.object(desktop_app, "_write_launch_diagnostics") as diagnostics_mock,
    ):
        with pytest.raises(KeyboardInterrupt, match="interrupted startup"):
            desktop_app.launch_desktop_app(config)

    assert any(call.args[1] == "desktop_launch_error" for call in trace_mock.call_args_list)
    diagnostics_mock.assert_called_with(
        config.data_dir,
        "desktop-launch-error.txt",
        mock.ANY,
    )
