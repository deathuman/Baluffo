import subprocess
from pathlib import Path
from unittest import mock

from src.ship import desktop_app
from tests.helpers.ports import ADMIN_BRIDGE_TEST_PORT


def test_watch_browser_session_uses_session_close_request_fast_path(tmp_path: Path) -> None:
    bridge_process = mock.Mock(spec=subprocess.Popen)
    bridge_process.poll.return_value = None
    browser_process = mock.Mock(spec=subprocess.Popen)
    browser_process.poll.return_value = 0
    rows = [
        {
            "event": "desktop_regular_close_shutdown_requested",
            "payload": {"sessionId": "session-1"},
        }
    ]

    with (
        mock.patch.object(desktop_app, "read_startup_metrics", return_value=rows),
        mock.patch.object(desktop_app, "wait_for_startup_handoff_signal") as handoff_mock,
        mock.patch.object(desktop_app.time, "sleep"),
        mock.patch.object(desktop_app, "_append_startup_trace") as trace_mock,
    ):
        result = desktop_app.watch_browser_session(
            tmp_path,
            5.0,
            bridge_port=ADMIN_BRIDGE_TEST_PORT,
            bridge_process=bridge_process,
            browser_process=browser_process,
            browser_pid=9093,
            desktop_session_id="session-1",
        )

    assert result == "shutdown_requested"
    handoff_mock.assert_not_called()
    event_names = [call.args[1] for call in trace_mock.call_args_list]
    assert "desktop_browser_watchdog_shutdown_requested" in event_names
    assert "desktop_browser_watchdog_handoff_candidate" not in event_names


def test_watch_browser_session_cancelled_close_keeps_handoff_path(tmp_path: Path) -> None:
    bridge_process = mock.Mock(spec=subprocess.Popen)
    bridge_process.poll.return_value = None
    browser_process = mock.Mock(spec=subprocess.Popen)
    browser_process.poll.return_value = 0
    rows = [
        {
            "event": "desktop_regular_close_shutdown_requested",
            "payload": {"sessionId": "session-1"},
        },
        {
            "event": "desktop_regular_close_shutdown_cancelled",
            "payload": {"sessionId": "session-1"},
        },
    ]

    with (
        mock.patch.object(desktop_app, "read_startup_metrics", return_value=rows),
        mock.patch.object(
            desktop_app,
            "wait_for_startup_handoff_signal",
            return_value=(None, None),
        ) as handoff_mock,
        mock.patch.object(desktop_app.time, "sleep"),
        mock.patch.object(desktop_app, "_append_startup_trace"),
    ):
        result = desktop_app.watch_browser_session(
            tmp_path,
            5.0,
            bridge_port=ADMIN_BRIDGE_TEST_PORT,
            bridge_process=bridge_process,
            browser_process=browser_process,
            browser_pid=9094,
            desktop_session_id="session-1",
        )

    assert result == "browser_handoff_failed"
    handoff_mock.assert_called_once()
