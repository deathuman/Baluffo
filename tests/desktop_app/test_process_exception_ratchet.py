import subprocess
from types import SimpleNamespace
from unittest import mock

import pytest

from src.ship import desktop_app


def _running_process() -> mock.Mock:
    process = mock.Mock(spec=subprocess.Popen)
    process.pid = 4321
    process.poll.return_value = None
    return process


@pytest.mark.windows
def test_terminate_process_falls_back_after_expected_taskkill_failures() -> None:
    process = _running_process()

    with (
        mock.patch.object(desktop_app, "os", SimpleNamespace(name="nt")),
        mock.patch.object(
            desktop_app.subprocess,
            "run",
            side_effect=PermissionError("taskkill denied"),
        ) as run_mock,
    ):
        desktop_app.terminate_process(process)

    run_mock.assert_called_once()
    process.terminate.assert_called_once()
    process.wait.assert_called_once_with(timeout=5)


@pytest.mark.windows
def test_terminate_process_does_not_swallow_unexpected_taskkill_failures() -> None:
    process = _running_process()

    with (
        mock.patch.object(desktop_app, "os", SimpleNamespace(name="nt")),
        mock.patch.object(
            desktop_app.subprocess,
            "run",
            side_effect=AssertionError("unexpected taskkill bug"),
        ),
        pytest.raises(AssertionError, match="unexpected taskkill bug"),
    ):
        desktop_app.terminate_process(process)


def test_terminate_process_suppresses_expected_posix_terminate_failures() -> None:
    process = _running_process()
    process.terminate.side_effect = ProcessLookupError("already exited")

    with mock.patch.object(desktop_app, "os", SimpleNamespace(name="posix")):
        desktop_app.terminate_process(process)

    process.wait.assert_not_called()


def test_terminate_process_does_not_swallow_unexpected_posix_terminate_failures() -> None:
    process = _running_process()
    process.terminate.side_effect = AssertionError("unexpected terminate bug")

    with (
        mock.patch.object(desktop_app, "os", SimpleNamespace(name="posix")),
        pytest.raises(AssertionError, match="unexpected terminate bug"),
    ):
        desktop_app.terminate_process(process)
