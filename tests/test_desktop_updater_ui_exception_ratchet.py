import builtins
from types import SimpleNamespace
from unittest import mock

import pytest

from src.ship import desktop_updater_ui as updater_ui


class _FakeTclError(Exception):
    pass


class _FakeWidget:
    def __init__(self, *args, **kwargs) -> None:  # noqa: ANN002,ANN003
        return None

    def pack(self, *args, **kwargs) -> None:  # noqa: ANN002,ANN003
        return None


class _FakeStringVar:
    def __init__(self, *, value: str = "") -> None:
        self.value = value

    def set(self, value: str) -> None:
        self.value = value


class _FakeRoot(_FakeWidget):
    mainloop_error: Exception | None = None

    def title(self, _value: str) -> None:
        return None

    def resizable(self, _width: bool, _height: bool) -> None:
        return None

    def attributes(self, *_args) -> None:  # noqa: ANN002
        return None

    def protocol(self, *_args) -> None:  # noqa: ANN002
        return None

    def configure(self, **_kwargs) -> None:  # noqa: ANN003
        return None

    def update_idletasks(self) -> None:
        return None

    def winfo_width(self) -> int:
        return 0

    def winfo_height(self) -> int:
        return 0

    def winfo_screenwidth(self) -> int:
        return 1024

    def winfo_screenheight(self) -> int:
        return 768

    def geometry(self, _value: str) -> None:
        return None

    def after(self, _delay: int, callback) -> None:  # noqa: ANN001
        callback()

    def mainloop(self) -> None:
        if self.mainloop_error is not None:
            raise self.mainloop_error
        return None

    def destroy(self) -> None:
        return None


class _FakeBar(_FakeWidget):
    stop_error: Exception | None = None

    def start(self, _delay: int) -> None:
        return None

    def stop(self) -> None:
        if self.stop_error is not None:
            raise self.stop_error
        return None


def _fake_tk_modules(
    *,
    theme_error: Exception | None = None,
    bar_stop_error: Exception | None = None,
    mainloop_error: Exception | None = None,
) -> tuple[object, object]:
    class FakeStyle:
        def __init__(self, _root) -> None:  # noqa: ANN001
            return None

        def theme_use(self, _theme: str) -> None:
            if theme_error is not None:
                raise theme_error

        def configure(self, *_args, **_kwargs) -> None:  # noqa: ANN002,ANN003
            return None

    class FakeBar(_FakeBar):
        stop_error = bar_stop_error

    class FakeRoot(_FakeRoot):
        pass

    FakeRoot.mainloop_error = mainloop_error

    fake_ttk = SimpleNamespace(Style=FakeStyle, Progressbar=FakeBar)
    fake_tk = SimpleNamespace(
        TclError=_FakeTclError,
        Tk=FakeRoot,
        Frame=_FakeWidget,
        Label=_FakeWidget,
        StringVar=_FakeStringVar,
        ttk=fake_ttk,
    )
    return fake_tk, fake_ttk


def _patch_tk_import(fake_tk: object, fake_ttk: object):
    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):  # noqa: ANN001
        if name == "tkinter":
            return fake_tk
        if name == "tkinter.ttk":
            return fake_ttk
        return real_import(name, *args, **kwargs)

    return mock.patch.object(builtins, "__import__", side_effect=fake_import)


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


def test_helper_progress_window_ignores_expected_theme_failures(monkeypatch) -> None:
    progress = updater_ui.HelperProgressWindow()
    progress._queue.put(("close", ""))
    progress._closed = mock.Mock(wait=mock.Mock(return_value=True), set=mock.Mock())
    fake_tk, fake_ttk = _fake_tk_modules(theme_error=_FakeTclError("theme unavailable"))

    monkeypatch.setattr(updater_ui, "root", None)
    monkeypatch.setattr(updater_ui.os, "name", "nt")
    with _patch_tk_import(fake_tk, fake_ttk):
        progress.run("Preparing update")

    assert progress._closed.set.call_count >= 1


def test_helper_progress_window_does_not_suppress_unexpected_theme_failures(
    monkeypatch,
) -> None:
    progress = updater_ui.HelperProgressWindow()
    progress._queue.put(("close", ""))
    progress._closed = mock.Mock(wait=mock.Mock(return_value=True), set=mock.Mock())
    fake_tk, fake_ttk = _fake_tk_modules(theme_error=RuntimeError("unexpected theme bug"))

    monkeypatch.setattr(updater_ui, "root", None)
    monkeypatch.setattr(updater_ui.os, "name", "nt")
    with (
        _patch_tk_import(fake_tk, fake_ttk),
        pytest.raises(RuntimeError, match="unexpected theme bug"),
    ):
        progress.run("Preparing update")


def test_helper_progress_window_ignores_expected_progress_stop_failures(
    monkeypatch,
) -> None:
    progress = updater_ui.HelperProgressWindow()
    progress._queue.put(("close", ""))
    progress._closed = mock.Mock(wait=mock.Mock(return_value=True), set=mock.Mock())
    fake_tk, fake_ttk = _fake_tk_modules(bar_stop_error=_FakeTclError("bar gone"))

    monkeypatch.setattr(updater_ui, "root", None)
    monkeypatch.setattr(updater_ui.os, "name", "nt")
    with _patch_tk_import(fake_tk, fake_ttk):
        progress.run("Preparing update")

    assert progress._closed.set.call_count >= 1


def test_helper_progress_window_does_not_suppress_unexpected_progress_stop_failures(
    monkeypatch,
) -> None:
    progress = updater_ui.HelperProgressWindow()
    progress._queue.put(("close", ""))
    progress._closed = mock.Mock(wait=mock.Mock(return_value=True), set=mock.Mock())
    fake_tk, fake_ttk = _fake_tk_modules(bar_stop_error=RuntimeError("unexpected bar bug"))

    monkeypatch.setattr(updater_ui, "root", None)
    monkeypatch.setattr(updater_ui.os, "name", "nt")
    with (
        _patch_tk_import(fake_tk, fake_ttk),
        pytest.raises(RuntimeError, match="unexpected bar bug"),
    ):
        progress.run("Preparing update")


def test_helper_progress_window_ignores_expected_mainloop_failures(monkeypatch) -> None:
    progress = updater_ui.HelperProgressWindow()
    progress._queue.put(("close", ""))
    progress._closed = mock.Mock(wait=mock.Mock(return_value=True), set=mock.Mock())
    fake_tk, fake_ttk = _fake_tk_modules(mainloop_error=_FakeTclError("window closed"))

    monkeypatch.setattr(updater_ui, "root", None)
    monkeypatch.setattr(updater_ui.os, "name", "nt")
    with _patch_tk_import(fake_tk, fake_ttk):
        progress.run("Preparing update")

    assert progress._closed.set.call_count >= 1


def test_helper_progress_window_does_not_suppress_unexpected_mainloop_failures(
    monkeypatch,
) -> None:
    progress = updater_ui.HelperProgressWindow()
    progress._queue.put(("close", ""))
    progress._closed = mock.Mock(wait=mock.Mock(return_value=True), set=mock.Mock())
    fake_tk, fake_ttk = _fake_tk_modules(mainloop_error=RuntimeError("unexpected loop bug"))

    monkeypatch.setattr(updater_ui, "root", None)
    monkeypatch.setattr(updater_ui.os, "name", "nt")
    with (
        _patch_tk_import(fake_tk, fake_ttk),
        pytest.raises(RuntimeError, match="unexpected loop bug"),
    ):
        progress.run("Preparing update")
