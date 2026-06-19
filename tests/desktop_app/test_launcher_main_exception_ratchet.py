from __future__ import annotations

from unittest import mock

import pytest

from src.ship import desktop_app
from src.ship.desktop_app import launcher as desktop_launcher


def test_main_surfaces_user_facing_launch_error() -> None:
    with (
        mock.patch.object(desktop_app, "create_runtime_config", return_value=object()),
        mock.patch.object(
            desktop_launcher,
            "launch_desktop_app",
            side_effect=RuntimeError("Baluffo could not launch a browser window."),
        ),
        mock.patch.object(desktop_app, "show_native_message", return_value=False) as show_mock,
    ):
        exit_code = desktop_app.main([])

    assert exit_code == 1
    show_mock.assert_called_once()


def test_main_does_not_swallow_unexpected_launch_bug() -> None:
    with (
        mock.patch.object(desktop_app, "create_runtime_config", return_value=object()),
        mock.patch.object(
            desktop_launcher,
            "launch_desktop_app",
            side_effect=AssertionError("unexpected launch bug"),
        ),
        mock.patch.object(desktop_app, "show_native_message") as show_mock,
    ):
        with pytest.raises(AssertionError, match="unexpected launch bug"):
            desktop_app.main([])

    show_mock.assert_not_called()
