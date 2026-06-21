import subprocess
from unittest import mock

import pytest

from src.ship import desktop_app


def test_launch_browser_for_url_switches_to_default_browser_when_chromium_exits_with_error() -> (
    None
):
    fake_process = mock.Mock(spec=subprocess.Popen)
    fake_process.pid = 123
    fake_process.poll.return_value = 1
    with (
        mock.patch.object(
            desktop_app,
            "resolve_chromium_browser_candidates",
            return_value=[{"name": "chrome", "path": "C:/Chrome/chrome.exe"}],
        ),
        mock.patch.object(desktop_app, "launch_chromium_app", return_value=fake_process),
        mock.patch.object(desktop_app.webbrowser, "open", return_value=True) as open_mock,
        mock.patch.object(desktop_app, "terminate_process") as terminate_mock,
    ):
        result = desktop_app.launch_browser_for_url("http://127.0.0.1:8080/jobs.html")

    assert result["mode"] == "default-browser"
    assert isinstance(result["windowShownAtMonotonic"], float)
    terminate_mock.assert_called_once_with(fake_process)
    open_mock.assert_called_once()


def test_launch_browser_for_url_keeps_chromium_mode_when_launcher_exits_cleanly() -> None:
    fake_process = mock.Mock(spec=subprocess.Popen)
    fake_process.pid = 456
    fake_process.poll.return_value = 0
    with (
        mock.patch.object(
            desktop_app,
            "resolve_chromium_browser_candidates",
            return_value=[{"name": "brave", "path": "C:/Brave/brave.exe"}],
        ),
        mock.patch.object(desktop_app, "launch_chromium_app", return_value=fake_process),
        mock.patch.object(
            desktop_app,
            "_wait_for_browser_reveal",
            return_value={
                "pid": 999,
                "title": "Baluffo",
                "observedAtMonotonic": 91.0,
                "event": "desktop_shell_window_shown",
                "observed": True,
                "handoffEvidence": "startup_metric",
            },
        ) as reveal_mock,
        mock.patch.object(desktop_app.webbrowser, "open", return_value=True) as open_mock,
        mock.patch.object(desktop_app, "terminate_process") as terminate_mock,
    ):
        result = desktop_app.launch_browser_for_url("http://127.0.0.1:8080/jobs.html")

    assert result["mode"] == "chromium-app"
    assert result["browserName"] == "brave"
    assert result["process"] is None
    assert result["windowShownAtMonotonic"] == 91.0
    assert result["windowShownObserved"] is True
    reveal_mock.assert_called_once_with(
        browser_pid=456,
        data_dir=None,
        launch_accepted_elapsed_ms=0,
        allow_title_fallback=True,
    )
    terminate_mock.assert_not_called()
    open_mock.assert_not_called()


def test_launch_browser_for_url_marks_reveal_inferred_when_visible_window_not_observed() -> None:
    fake_process = mock.Mock(spec=subprocess.Popen)
    fake_process.pid = 654
    with (
        mock.patch.object(
            desktop_app,
            "resolve_chromium_browser_candidates",
            return_value=[{"name": "chrome", "path": "C:/Chrome/chrome.exe"}],
        ),
        mock.patch.object(desktop_app, "launch_chromium_app", return_value=fake_process),
        mock.patch.object(desktop_app, "wait_for_browser_process_ready", return_value=True),
        mock.patch.object(
            desktop_app,
            "_wait_for_browser_reveal",
            return_value={
                "observedAtMonotonic": 88.0,
                "event": "desktop_shell_window_shown_inferred",
                "observed": False,
                "inferredElapsedMsCap": 0,
                "handoffEvidence": "",
            },
        ) as reveal_mock,
    ):
        result = desktop_app.launch_browser_for_url("http://127.0.0.1:8080/jobs.html")

    assert result["mode"] == "chromium-app"
    assert result["windowShownObserved"] is False
    assert result["shellWindowEventEmitted"] is False
    reveal_mock.assert_called_once_with(
        browser_pid=654,
        data_dir=None,
        launch_accepted_elapsed_ms=0,
    )


@pytest.mark.windows
def test_launch_browser_for_url_treats_clean_exit_after_reveal_as_detached() -> None:
    fake_process = mock.Mock(spec=subprocess.Popen)
    fake_process.pid = 654
    fake_process.poll.return_value = 0
    with (
        mock.patch.object(
            desktop_app,
            "resolve_chromium_browser_candidates",
            return_value=[{"name": "chrome", "path": "C:/Chrome/chrome.exe"}],
        ),
        mock.patch.object(desktop_app, "launch_chromium_app", return_value=fake_process),
        mock.patch.object(desktop_app, "wait_for_browser_process_ready", return_value=True),
        mock.patch.object(
            desktop_app,
            "_wait_for_browser_reveal",
            return_value={
                "observedAtMonotonic": 88.0,
                "event": "desktop_shell_window_shown_inferred",
                "observed": False,
                "inferredElapsedMsCap": 900,
                "handoffEvidence": "startup_metric",
            },
        ),
        mock.patch.object(desktop_app, "_windows_try_assign_pid_to_job") as assign_mock,
    ):
        result = desktop_app.launch_browser_for_url(
            "http://127.0.0.1:8080/jobs.html",
            job_handle=11,
        )

    assert result["mode"] == "chromium-app"
    assert result["process"] is None
    assert result["browserPid"] == 654
    assert result["windowShownObserved"] is False
    assert result["revealHandoffEvidence"] == "startup_metric"
    assign_mock.assert_called_once_with(11, 654)


def test_launch_browser_for_url_emits_trace_events_at_spawn_accept_and_reveal() -> None:
    fake_process = mock.Mock(spec=subprocess.Popen)
    fake_process.pid = 700
    trace_events: list[tuple[str, float, dict[str, object]]] = []

    def _trace(event: str, _mono: float, fields: dict[str, object]) -> None:
        trace_events.append((event, _mono, fields))

    with (
        mock.patch.object(
            desktop_app,
            "resolve_chromium_browser_candidates",
            return_value=[{"name": "chrome", "path": "C:/Chrome/chrome.exe"}],
        ),
        mock.patch.object(desktop_app, "launch_chromium_app", return_value=fake_process),
        mock.patch.object(desktop_app, "wait_for_browser_process_ready", return_value=True),
        mock.patch.object(
            desktop_app,
            "_wait_for_browser_reveal",
            return_value={
                "pid": 700,
                "title": "Baluffo",
                "observedAtMonotonic": 45.0,
                "event": "desktop_shell_window_shown",
                "observed": True,
                "handoffEvidence": "",
            },
        ),
    ):
        result = desktop_app.launch_browser_for_url(
            "http://127.0.0.1:8080/jobs.html",
            trace_hook=_trace,
        )

    assert result["launchTraceEventsEmitted"] is True
    assert result["shellWindowEventEmitted"] is True
    assert [event for event, _mono, _fields in trace_events] == [
        "desktop_browser_process_spawn_started",
        "desktop_window_created",
        "desktop_browser_launch_accepted",
        "desktop_browser_launch_selected",
        "desktop_shell_window_shown",
    ]
    selected_event = next(
        item for item in trace_events if item[0] == "desktop_browser_launch_selected"
    )
    shell_event = next(item for item in trace_events if item[0] == "desktop_shell_window_shown")
    assert selected_event[1] == shell_event[1]


def test_launch_browser_for_url_attaches_browser_job_before_readiness_polling() -> None:
    fake_process = mock.Mock(spec=subprocess.Popen)
    fake_process.pid = 701
    call_order: list[str] = []
    trace_events: list[str] = []

    def _trace(event: str, _mono: float, _fields: dict[str, object]) -> None:
        trace_events.append(event)

    def _attach(_job_handle: int, _pid: int) -> None:
        call_order.append("attach")

    def _wait(*args: object, **kwargs: object) -> bool:
        call_order.append("wait")
        return True

    def _reveal(**kwargs: object) -> dict[str, object]:
        call_order.append("reveal")
        return {
            "pid": 701,
            "title": "Baluffo",
            "observedAtMonotonic": 46.0,
            "event": "desktop_shell_window_shown",
            "observed": True,
            "handoffEvidence": "",
        }

    with (
        mock.patch.object(
            desktop_app,
            "resolve_chromium_browser_candidates",
            return_value=[{"name": "chrome", "path": "C:/Chrome/chrome.exe"}],
        ),
        mock.patch.object(desktop_app, "launch_chromium_app", return_value=fake_process),
        mock.patch.object(
            desktop_app,
            "_windows_try_assign_pid_to_job",
            side_effect=_attach,
        ) as assign_mock,
        mock.patch.object(
            desktop_app,
            "wait_for_browser_process_ready",
            side_effect=_wait,
        ) as wait_mock,
        mock.patch.object(
            desktop_app,
            "_wait_for_browser_reveal",
            side_effect=_reveal,
        ) as reveal_mock,
    ):
        result = desktop_app.launch_browser_for_url(
            "http://127.0.0.1:8080/jobs.html",
            job_handle=11,
            trace_hook=_trace,
        )

    assert result["mode"] == "chromium-app"
    assign_mock.assert_called_once_with(11, 701)
    wait_mock.assert_called_once()
    reveal_mock.assert_called_once()
    assert call_order == ["attach", "wait", "reveal"]
    assert trace_events[:2] == [
        "desktop_browser_process_spawn_started",
        "desktop_browser_job_attached",
    ]


def test_launch_browser_for_url_stops_before_readiness_when_job_attach_fails() -> None:
    fake_process = mock.Mock(spec=subprocess.Popen)
    fake_process.pid = 702
    trace_events: list[str] = []

    def _trace(event: str, _mono: float, _fields: dict[str, object]) -> None:
        trace_events.append(event)

    with (
        mock.patch.object(
            desktop_app,
            "resolve_chromium_browser_candidates",
            return_value=[{"name": "chrome", "path": "C:/Chrome/chrome.exe"}],
        ),
        mock.patch.object(desktop_app, "launch_chromium_app", return_value=fake_process),
        mock.patch.object(
            desktop_app,
            "_windows_try_assign_pid_to_job",
            side_effect=OSError("browser attach failed"),
        ) as assign_mock,
        mock.patch.object(desktop_app, "wait_for_browser_process_ready") as wait_mock,
        mock.patch.object(desktop_app, "_wait_for_browser_reveal") as reveal_mock,
        mock.patch.object(desktop_app, "terminate_process") as terminate_mock,
    ):
        with pytest.raises(OSError, match="browser attach failed"):
            desktop_app.launch_browser_for_url(
                "http://127.0.0.1:8080/jobs.html",
                job_handle=11,
                trace_hook=_trace,
            )

    assign_mock.assert_called_once_with(11, 702)
    terminate_mock.assert_called_once_with(fake_process)
    wait_mock.assert_not_called()
    reveal_mock.assert_not_called()
    assert trace_events == [
        "desktop_browser_process_spawn_started",
        "desktop_browser_job_attach_failed",
    ]
