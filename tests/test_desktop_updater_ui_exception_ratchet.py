import builtins
from unittest import mock

import pytest

from src.ship import desktop_updater_ui as updater_ui


def test_helper_progress_window_waits_when_tkinter_is_unavailable(monkeypatch) -> None:
    progress = updater_ui.HelperProgressWindow()
    progress._closed = mock.Mock(wait=mock.Mock(return_value=True), set=mock.Mock())
    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):  # noqa: ANN001
        if name == "tkinter" or name.startswith("tkinter."):
            raise ImportError("tk unavailable")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(updater_ui.os, "name", "nt")
    with mock.patch.object(builtins, "__import__", side_effect=fake_import):
        progress.run("Preparing update")

    progress._closed.wait.assert_called_once_with()


def test_helper_progress_window_does_not_suppress_unexpected_import_failures(
    monkeypatch,
) -> None:
    progress = updater_ui.HelperProgressWindow()
    progress._closed = mock.Mock(wait=mock.Mock(return_value=True), set=mock.Mock())
    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):  # noqa: ANN001
        if name == "tkinter" or name.startswith("tkinter."):
            raise RuntimeError("unexpected tk import bug")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(updater_ui.os, "name", "nt")
    with (
        mock.patch.object(builtins, "__import__", side_effect=fake_import),
        pytest.raises(RuntimeError, match="unexpected tk import bug"),
    ):
        progress.run("Preparing update")

    progress._closed.wait.assert_not_called()
