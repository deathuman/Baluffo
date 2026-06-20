import subprocess
from pathlib import Path
from unittest import mock

from src.ship import desktop_app
from src.ship.desktop_app import session as desktop_session
from tests.helpers.ports import ADMIN_BRIDGE_TEST_PORT


def test_watch_browser_session_uses_heartbeat_when_no_browser_process() -> None:
    with (
        mock.patch.object(desktop_app, "wait_for_browser_heartbeat", return_value=True),
        mock.patch.object(
            desktop_app, "latest_browser_heartbeat_ts", side_effect=[100.0, 100.0, 100.0]
        ),
        mock.patch.object(desktop_app, "bridge_last_activity_ts", return_value=0.0),
        mock.patch.object(desktop_app.time, "time", side_effect=[110.0, 120.0, 140.5]),
        mock.patch.object(desktop_app.time, "sleep"),
        mock.patch.object(desktop_app, "_append_startup_trace"),
        mock.patch.object(desktop_app, "_is_baluffo_browser_window_open", return_value=True),
    ):
        result = desktop_app.watch_browser_session(
            Path("C:/tmp"),
            5.0,
            bridge_port=ADMIN_BRIDGE_TEST_PORT,
            browser_process=None,
            heartbeat_idle_timeout_s=30.0,
        )

    assert result == "heartbeat_timeout"


def test_watch_browser_session_prefers_bridge_exit_when_authoritative_process_is_available():
    bridge_process = mock.Mock(spec=subprocess.Popen)
    bridge_process.poll.side_effect = [None, 0]
    browser_process = mock.Mock(spec=subprocess.Popen)
    browser_process.poll.return_value = None

    with (
        mock.patch.object(desktop_app.time, "sleep"),
        mock.patch.object(desktop_app, "_append_startup_trace"),
    ):
        result = desktop_app.watch_browser_session(
            Path("C:/tmp"),
            5.0,
            bridge_port=ADMIN_BRIDGE_TEST_PORT,
            bridge_process=bridge_process,
            browser_process=browser_process,
        )

    assert result == "bridge_exit"


def test_watch_browser_session_times_out_missing_window_with_bridge_authoritative_detached_browser() -> (
    None
):
    bridge_process = mock.Mock(spec=subprocess.Popen)
    bridge_process.poll.return_value = None

    with (
        mock.patch.object(
            desktop_app, "_is_baluffo_browser_window_open", return_value=False
        ) as window_mock,
        mock.patch.object(desktop_app, "latest_browser_heartbeat_ts", return_value=100.0),
        mock.patch.object(desktop_app, "bridge_last_activity_ts", return_value=0.0),
        mock.patch.object(
            desktop_app.time,
            "time",
            side_effect=[110.0, 100.0 + desktop_app.DETACHED_WINDOW_IDLE_TIMEOUT_S + 1.0],
        ),
        mock.patch.object(desktop_app.time, "sleep"),
        mock.patch.object(desktop_app, "_append_startup_trace") as trace_mock,
    ):
        result = desktop_app.watch_browser_session(
            Path("C:/tmp"),
            5.0,
            bridge_port=ADMIN_BRIDGE_TEST_PORT,
            bridge_process=bridge_process,
            browser_process=None,
            browser_pid=6060,
            heartbeat_idle_timeout_s=desktop_app.HEARTBEAT_IDLE_TIMEOUT_S,
        )

    assert result == "heartbeat_timeout"
    event_names = [call.args[1] for call in trace_mock.call_args_list]
    assert "desktop_browser_window_missing_waiting_for_bridge" in event_names
    assert "desktop_browser_heartbeat_timeout" in event_names
    window_mock.assert_called_with(
        browser_pid=6060,
        allow_title_fallback=True,
    )


def test_watch_browser_session_bridge_authoritative_missing_window_still_prefers_bridge_exit() -> (
    None
):
    bridge_process = mock.Mock(spec=subprocess.Popen)
    bridge_process.poll.side_effect = [None, 0]

    with (
        mock.patch.object(
            desktop_app, "_is_baluffo_browser_window_open", return_value=False
        ) as window_mock,
        mock.patch.object(desktop_app, "latest_browser_heartbeat_ts", return_value=100.0),
        mock.patch.object(desktop_app, "bridge_last_activity_ts", return_value=0.0),
        mock.patch.object(desktop_app.time, "time", return_value=110.0),
        mock.patch.object(desktop_app.time, "sleep"),
        mock.patch.object(desktop_app, "_append_startup_trace") as trace_mock,
    ):
        result = desktop_app.watch_browser_session(
            Path("C:/tmp"),
            5.0,
            bridge_port=ADMIN_BRIDGE_TEST_PORT,
            bridge_process=bridge_process,
            browser_process=None,
            browser_pid=6061,
            heartbeat_idle_timeout_s=desktop_app.HEARTBEAT_IDLE_TIMEOUT_S,
        )

    assert result == "bridge_exit"
    event_names = [call.args[1] for call in trace_mock.call_args_list]
    assert "desktop_browser_window_missing_waiting_for_bridge" in event_names
    assert "desktop_browser_heartbeat_timeout" not in event_names
    window_mock.assert_called_with(
        browser_pid=6061,
        allow_title_fallback=True,
    )


def test_watch_browser_session_detects_window_close_in_detached_mode() -> None:
    with (
        mock.patch.object(
            desktop_app, "_is_baluffo_browser_window_open", return_value=False
        ) as window_mock,
        mock.patch.object(desktop_app, "latest_browser_heartbeat_ts", return_value=0.0),
        mock.patch.object(desktop_app, "bridge_last_activity_ts", return_value=0.0),
        mock.patch.object(desktop_app.time, "sleep"),
        mock.patch.object(desktop_app, "_append_startup_trace"),
    ):
        result = desktop_app.watch_browser_session(
            Path("C:/tmp"),
            5.0,
            bridge_port=ADMIN_BRIDGE_TEST_PORT,
            browser_process=None,
            browser_pid=4040,
        )

    assert result == "window_closed"
    window_mock.assert_called_with(
        browser_pid=4040,
        allow_title_fallback=True,
    )


def test_watch_browser_session_keeps_detached_launcher_alive_when_heartbeat_exists() -> None:
    with (
        mock.patch.object(desktop_app, "_is_baluffo_browser_window_open", return_value=False),
        mock.patch.object(
            desktop_app, "latest_browser_heartbeat_ts", side_effect=[100.0, 100.0, 100.0]
        ),
        mock.patch.object(desktop_app, "bridge_last_activity_ts", return_value=0.0),
        mock.patch.object(desktop_app.time, "time", side_effect=[110.0, 120.0, 140.5]),
        mock.patch.object(desktop_app.time, "sleep"),
        mock.patch.object(desktop_app, "_append_startup_trace"),
    ):
        result = desktop_app.watch_browser_session(
            Path("C:/tmp"),
            5.0,
            bridge_port=ADMIN_BRIDGE_TEST_PORT,
            browser_process=None,
            browser_pid=5050,
            heartbeat_idle_timeout_s=30.0,
        )

    assert result == "heartbeat_timeout"


def test_watch_browser_session_ignores_missing_window_in_no_browser_mode() -> None:
    with (
        mock.patch.object(
            desktop_app, "latest_browser_heartbeat_ts", side_effect=[100.0, 100.0, 100.0]
        ),
        mock.patch.object(desktop_app, "bridge_last_activity_ts", return_value=0.0),
        mock.patch.object(desktop_app.time, "time", side_effect=[110.0, 120.0, 140.5]),
        mock.patch.object(desktop_app.time, "sleep"),
        mock.patch.object(desktop_app, "_append_startup_trace"),
        mock.patch.object(desktop_app, "_is_baluffo_browser_window_open") as window_mock,
    ):
        result = desktop_app.watch_browser_session(
            Path("C:/tmp"),
            5.0,
            bridge_port=ADMIN_BRIDGE_TEST_PORT,
            browser_process=None,
            heartbeat_idle_timeout_s=30.0,
            require_window=False,
        )

    assert result == "heartbeat_timeout"
    window_mock.assert_not_called()


def test_watch_browser_session_background_recovery_waits_for_active_work_to_finish() -> None:
    bridge_process = mock.Mock(spec=subprocess.Popen)
    bridge_process.poll.return_value = None

    with (
        mock.patch.object(
            desktop_app,
            "get_baluffo_bridge_health",
            side_effect=[
                {"service": "baluffo-bridge", "desktopMode": True, "owner": {"token": "live"}},
                {"service": "baluffo-bridge", "desktopMode": True, "owner": {"token": "live"}},
            ],
        ),
        mock.patch.object(desktop_app, "_bridge_health_matches_owner_session", return_value=True),
        mock.patch.object(
            desktop_app,
            "_load_active_critical_desktop_tasks",
            side_effect=[
                [{"taskType": "fetch", "runId": "fetch_live_1", "status": "running"}],
                [],
            ],
        ),
        mock.patch.object(desktop_app.time, "sleep"),
        mock.patch.object(desktop_app, "_append_startup_trace"),
    ):
        result = desktop_app.watch_browser_session(
            Path("C:/tmp"),
            5.0,
            bridge_port=ADMIN_BRIDGE_TEST_PORT,
            bridge_process=bridge_process,
            browser_process=None,
            require_window=False,
            background_active_work_recovery=True,
            recovery_owner_token="live",
        )

    assert result == "active_work_completed"


def test_load_active_critical_desktop_tasks_uses_summary_route() -> None:
    called_urls: list[str] = []

    def fake_fetch_json(url: str, timeout_s: float = 10.0) -> dict[str, object]:  # noqa: ANN001
        called_urls.append(url)
        return {
            "summary": True,
            "tasks": [
                {
                    "taskType": "fetch",
                    "runId": "fetch_live_1",
                    "status": "running",
                    "active": True,
                }
            ],
        }

    with mock.patch.object(desktop_session, "_fetch_json", side_effect=fake_fetch_json):
        active_tasks = desktop_app._load_active_critical_desktop_tasks(
            Path("C:/tmp"),
            bridge_port=ADMIN_BRIDGE_TEST_PORT,
            allow_disk_fallback=False,
        )

    assert called_urls == ["http://127.0.0.1:8877/ops/task-state?view=summary"]
    assert active_tasks == [{"taskType": "fetch", "runId": "fetch_live_1", "status": "running"}]


def test_watch_browser_session_background_recovery_exits_when_bridge_is_unavailable() -> None:
    bridge_process = mock.Mock(spec=subprocess.Popen)
    bridge_process.poll.return_value = None

    with (
        mock.patch.object(desktop_app, "get_baluffo_bridge_health", return_value={}),
        mock.patch.object(desktop_app, "_bridge_health_matches_owner_session", return_value=False),
        mock.patch.object(desktop_app, "_load_active_critical_desktop_tasks", return_value=[]),
        mock.patch.object(desktop_app.time, "sleep"),
        mock.patch.object(desktop_app, "_append_startup_trace"),
    ):
        result = desktop_app.watch_browser_session(
            Path("C:/tmp"),
            5.0,
            bridge_port=ADMIN_BRIDGE_TEST_PORT,
            bridge_process=bridge_process,
            browser_process=None,
            require_window=False,
            background_active_work_recovery=True,
            recovery_owner_token="live",
        )

    assert result == "bridge_exit"


def test_watch_browser_session_confirms_handoff_after_accepted_process_exit_when_signal_arrives() -> (
    None
):
    bridge_process = mock.Mock(spec=subprocess.Popen)
    bridge_process.poll.return_value = None
    browser_process = mock.Mock(spec=subprocess.Popen)
    browser_process.poll.return_value = 0

    with (
        mock.patch.object(
            desktop_app,
            "wait_for_startup_handoff_signal",
            return_value=("startup_metric", 950),
        ) as handoff_mock,
        mock.patch.object(
            desktop_app,
            "latest_browser_heartbeat_ts",
            side_effect=[0.0, 100.0, 100.0],
        ),
        mock.patch.object(
            desktop_app, "_is_baluffo_browser_window_open", return_value=True
        ) as window_mock,
        mock.patch.object(desktop_app, "wait_for_browser_heartbeat", return_value=True),
        mock.patch.object(desktop_app, "bridge_last_activity_ts", return_value=0.0),
        mock.patch.object(desktop_app.time, "time", return_value=102.5),
        mock.patch.object(desktop_app.time, "sleep"),
        mock.patch.object(desktop_app, "_append_startup_trace") as trace_mock,
    ):
        result = desktop_app.watch_browser_session(
            Path("C:/tmp"),
            5.0,
            bridge_port=ADMIN_BRIDGE_TEST_PORT,
            bridge_process=bridge_process,
            browser_process=browser_process,
            browser_pid=9090,
            launch_accepted_elapsed_ms=700,
            heartbeat_idle_timeout_s=1.0,
        )

    assert result == "heartbeat_timeout"
    handoff_mock.assert_called_once_with(
        Path("C:/tmp"),
        browser_pid=9090,
        min_elapsed_ms=700,
        timeout_s=desktop_app.STARTUP_HANDOFF_GRACE_TIMEOUT_S,
    )
    event_names = [call.args[1] for call in trace_mock.call_args_list]
    assert "desktop_browser_watchdog_handoff_candidate" in event_names
    assert "desktop_browser_watchdog_handoff_confirmed" in event_names
    assert "desktop_browser_watchdog_handoff" in event_names
    assert "desktop_browser_watchdog_handoff_failed" not in event_names
    window_mock.assert_called_with(
        browser_pid=9090,
        allow_title_fallback=False,
    )


def test_watch_browser_session_returns_handoff_failed_when_signal_never_arrives() -> None:
    bridge_process = mock.Mock(spec=subprocess.Popen)
    bridge_process.poll.return_value = None
    browser_process = mock.Mock(spec=subprocess.Popen)
    browser_process.poll.return_value = 0

    with (
        mock.patch.object(
            desktop_app,
            "wait_for_startup_handoff_signal",
            return_value=(None, None),
        ) as handoff_mock,
        mock.patch.object(desktop_app.time, "sleep"),
        mock.patch.object(desktop_app, "_append_startup_trace") as trace_mock,
    ):
        result = desktop_app.watch_browser_session(
            Path("C:/tmp"),
            5.0,
            bridge_port=ADMIN_BRIDGE_TEST_PORT,
            bridge_process=bridge_process,
            browser_process=browser_process,
            browser_pid=9091,
            launch_accepted_elapsed_ms=720,
        )

    assert result == "browser_handoff_failed"
    handoff_mock.assert_called_once_with(
        Path("C:/tmp"),
        browser_pid=9091,
        min_elapsed_ms=720,
        timeout_s=desktop_app.STARTUP_HANDOFF_GRACE_TIMEOUT_S,
    )
    event_names = [call.args[1] for call in trace_mock.call_args_list]
    assert "desktop_browser_watchdog_handoff_candidate" in event_names
    assert "desktop_browser_watchdog_handoff_failed" in event_names
    assert "desktop_browser_watchdog_handoff_confirmed" not in event_names


def test_watch_browser_session_times_out_missing_handoff_window_stale_activity() -> None:
    bridge_process = mock.Mock(spec=subprocess.Popen)
    bridge_process.poll.side_effect = [None, 0]
    browser_process = mock.Mock(spec=subprocess.Popen)
    browser_process.poll.return_value = 0

    with (
        mock.patch.object(
            desktop_app,
            "wait_for_startup_handoff_signal",
            return_value=("visible_window", 980),
        ) as handoff_mock,
        mock.patch.object(desktop_app, "updater_install_requested", return_value=False),
        mock.patch.object(desktop_app, "latest_browser_session_activity_ts", return_value=100.0),
        mock.patch.object(desktop_app.time, "time", return_value=131.0),
        mock.patch.object(
            desktop_app,
            "_is_baluffo_browser_window_open",
            side_effect=lambda *, browser_pid, allow_title_fallback: bool(allow_title_fallback),
        ) as window_mock,
        mock.patch.object(desktop_app.time, "sleep"),
        mock.patch.object(desktop_app, "_append_startup_trace") as trace_mock,
    ):
        result = desktop_app.watch_browser_session(
            Path("C:/tmp"),
            5.0,
            bridge_port=ADMIN_BRIDGE_TEST_PORT,
            bridge_process=bridge_process,
            browser_process=browser_process,
            browser_pid=9092,
            launch_accepted_elapsed_ms=740,
            heartbeat_idle_timeout_s=30.0,
        )

    assert result == "heartbeat_timeout"
    handoff_mock.assert_called_once_with(
        Path("C:/tmp"),
        browser_pid=9092,
        min_elapsed_ms=740,
        timeout_s=desktop_app.STARTUP_HANDOFF_GRACE_TIMEOUT_S,
    )
    window_mock.assert_called_with(
        browser_pid=9092,
        allow_title_fallback=False,
    )
    event_names = [call.args[1] for call in trace_mock.call_args_list]
    assert "desktop_browser_window_missing_waiting_for_bridge" in event_names
