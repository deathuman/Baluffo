from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest

from src.ship import desktop_app

from ._helpers import _patch_windows_compat_facade, _patch_windows_desktop_app


@pytest.mark.windows
def test_windows_try_assign_pid_to_job_raises_when_open_process_fails() -> None:
    kernel32 = SimpleNamespace(
        GetLastError=mock.Mock(return_value=5),
        OpenProcess=mock.Mock(return_value=0),
    )

    with _patch_windows_desktop_app(kernel32):
        with pytest.raises(
            OSError,
            match="OpenProcess failed while attaching pid=123 to desktop job: Access is denied.",
        ):
            desktop_app._windows_try_assign_pid_to_job(11, 123)


@pytest.mark.windows
def test_windows_try_assign_pid_to_job_raises_when_assign_process_fails() -> None:
    kernel32 = SimpleNamespace(
        GetLastError=mock.Mock(return_value=5),
        OpenProcess=mock.Mock(return_value=99),
        AssignProcessToJobObject=mock.Mock(return_value=0),
        CloseHandle=mock.Mock(),
    )

    with _patch_windows_desktop_app(kernel32):
        with pytest.raises(
            OSError,
            match=(
                "AssignProcessToJobObject failed while attaching pid=123 to desktop job: "
                "Access is denied."
            ),
        ):
            desktop_app._windows_try_assign_pid_to_job(11, 123)

    kernel32.CloseHandle.assert_called_once_with(99)


@pytest.mark.windows
def test_start_child_process_terminates_child_when_job_attach_fails() -> None:
    fake_process = SimpleNamespace(pid=321)

    with (
        mock.patch.object(desktop_app.os, "name", "nt"),
        mock.patch.object(desktop_app.subprocess, "Popen", return_value=fake_process) as popen_mock,
        mock.patch.object(
            desktop_app,
            "_windows_try_assign_pid_to_job",
            side_effect=OSError("attach failed"),
        ),
        mock.patch.object(desktop_app, "terminate_process") as terminate_mock,
    ):
        with pytest.raises(OSError, match="attach failed"):
            desktop_app.start_child_process(["python", "-V"], job_handle=11)

    terminate_mock.assert_called_once_with(fake_process)
    assert popen_mock.call_args.kwargs["close_fds"] is True


@pytest.mark.windows
def test_windows_create_kill_on_close_job_marks_handle_non_inheritable() -> None:
    kernel32 = mock.Mock()
    kernel32.CreateJobObjectW.return_value = 77
    kernel32.SetHandleInformation.return_value = 1
    kernel32.SetInformationJobObject.return_value = 1

    with _patch_windows_desktop_app(kernel32):
        handle = desktop_app._windows_create_kill_on_close_job()

    assert handle == 77
    kernel32.SetHandleInformation.assert_called_once_with(77, 0x00000001, 0)
    kernel32.SetInformationJobObject.assert_called_once()


@pytest.mark.windows
def test_is_process_alive_returns_false_for_signaled_windows_process_handle() -> None:
    kernel32 = SimpleNamespace(
        OpenProcess=mock.Mock(return_value=55),
        WaitForSingleObject=mock.Mock(return_value=0),
        GetExitCodeProcess=mock.Mock(return_value=1),
        CloseHandle=mock.Mock(),
    )

    with _patch_windows_desktop_app(kernel32):
        assert desktop_app.is_process_alive(123) is False

    kernel32.GetExitCodeProcess.assert_not_called()
    kernel32.CloseHandle.assert_called_once_with(55)


@pytest.mark.windows
def test_is_process_alive_returns_true_for_running_windows_process_handle() -> None:
    def _get_exit_code(_handle: int, exit_code_ptr: object) -> int:
        exit_code_ptr._obj.value = 259
        return 1

    kernel32 = SimpleNamespace(
        OpenProcess=mock.Mock(return_value=55),
        WaitForSingleObject=mock.Mock(return_value=0x00000102),
        GetExitCodeProcess=mock.Mock(side_effect=_get_exit_code),
        CloseHandle=mock.Mock(),
    )

    with _patch_windows_desktop_app(kernel32):
        assert desktop_app.is_process_alive(123) is True

    kernel32.GetExitCodeProcess.assert_called_once()
    kernel32.CloseHandle.assert_called_once_with(55)


@pytest.mark.windows
def test_find_baluffo_visible_window_accepts_same_pid_chromium_window_without_baluffo_title() -> (
    None
):
    with (
        _patch_windows_compat_facade(),
        mock.patch.object(
            desktop_app,
            "_enumerate_visible_desktop_windows",
            return_value=[
                {
                    "hwnd": 100,
                    "pid": 777,
                    "title": "Jobs",
                    "className": "Chrome_WidgetWin_1",
                    "matchesTitle": False,
                    "isChromiumClass": True,
                }
            ],
        ),
    ):
        result = desktop_app._find_baluffo_visible_window(
            browser_pid=777,
            allow_title_fallback=False,
        )

    assert result is not None
    assert result["pid"] == 777
    assert result["className"] == "Chrome_WidgetWin_1"


def test_wait_for_browser_reveal_accepts_handoff_window_after_startup_evidence() -> None:
    with (
        mock.patch.object(desktop_app, "_enumerate_visible_desktop_windows", return_value=[]),
        mock.patch.object(desktop_app, "_find_baluffo_visible_window", return_value=None),
        mock.patch.object(
            desktop_app,
            "earliest_startup_handoff_signal",
            return_value=("startup_metric", 1300),
        ),
        mock.patch.object(
            desktop_app,
            "_find_reveal_handoff_window",
            return_value={
                "hwnd": 55,
                "pid": 9001,
                "title": "",
                "className": "Chrome_WidgetWin_1",
                "matchesTitle": False,
                "isChromiumClass": True,
            },
        ),
        mock.patch.object(desktop_app.time, "monotonic", side_effect=[0.0, 0.0]),
    ):
        result = desktop_app._wait_for_browser_reveal(
            browser_pid=321,
            data_dir=Path("C:/tmp"),
            launch_accepted_elapsed_ms=1200,
        )

    assert result["observed"] is True
    assert result["event"] == "desktop_shell_window_shown"
    assert result["handoffEvidence"] == "startup_metric"
    assert result["pid"] == 9001


def test_wait_for_browser_reveal_caps_inferred_fallback_at_earliest_browser_evidence() -> None:
    with (
        mock.patch.object(desktop_app, "_enumerate_visible_desktop_windows", return_value=[]),
        mock.patch.object(desktop_app, "_find_baluffo_visible_window", return_value=None),
        mock.patch.object(
            desktop_app,
            "earliest_startup_handoff_signal",
            return_value=("startup_metric", 1900),
        ),
        mock.patch.object(desktop_app, "_find_reveal_handoff_window", return_value=None),
        mock.patch.object(desktop_app.time, "monotonic", side_effect=[0.0, 0.0, 2.0]),
        mock.patch.object(desktop_app.time, "sleep"),
    ):
        result = desktop_app._wait_for_browser_reveal(
            browser_pid=321,
            data_dir=Path("C:/tmp"),
            launch_accepted_elapsed_ms=1200,
        )

    assert result["observed"] is False
    assert result["event"] == "desktop_shell_window_shown_inferred"
    assert result["inferredElapsedMsCap"] == 1900
