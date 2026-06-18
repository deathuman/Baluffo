from types import SimpleNamespace
from unittest import mock

import pytest

from src.ship import desktop_app

from ._helpers import _patch_windows_compat_facade


def _fake_ctypes_for_window_probe(
    *,
    dwm_exc: Exception,
    class_exc: Exception,
    enum_exc: Exception,
) -> SimpleNamespace:
    def _passthrough_winfuntype(*_args):
        return lambda callback: callback

    return SimpleNamespace(
        windll=SimpleNamespace(
            dwmapi=SimpleNamespace(DwmGetWindowAttribute=mock.Mock(side_effect=dwm_exc)),
            user32=SimpleNamespace(
                GetClassNameW=mock.Mock(side_effect=class_exc),
                EnumWindows=mock.Mock(side_effect=enum_exc),
            ),
        ),
        wintypes=SimpleNamespace(
            DWORD=lambda value=0: SimpleNamespace(value=int(value)),
            HWND=lambda value=0: int(value),
        ),
        byref=lambda obj: SimpleNamespace(_obj=obj),
        sizeof=lambda _obj: 1,
        create_unicode_buffer=lambda _size: SimpleNamespace(value=""),
        WINFUNCTYPE=_passthrough_winfuntype,
        c_bool=bool,
        c_void_p=int,
    )


@pytest.mark.windows
def test_windows_window_probe_helpers_return_defaults_for_expected_api_failures() -> None:
    fake_ctypes = _fake_ctypes_for_window_probe(
        dwm_exc=OSError("dwm"),
        class_exc=OSError("class"),
        enum_exc=OSError("enum"),
    )

    with (
        _patch_windows_compat_facade(),
        mock.patch.object(desktop_app, "ctypes", fake_ctypes, create=True),
    ):
        assert desktop_app._windows_window_is_cloaked(100) is False
        assert desktop_app._windows_window_class_name(100) == ""
        assert desktop_app._enumerate_visible_desktop_windows() == []


@pytest.mark.windows
def test_windows_window_probe_helpers_do_not_swallow_unexpected_api_failures() -> None:
    fake_ctypes = _fake_ctypes_for_window_probe(
        dwm_exc=RuntimeError("unexpected dwm bug"),
        class_exc=RuntimeError("unexpected class bug"),
        enum_exc=RuntimeError("unexpected enum bug"),
    )

    with (
        _patch_windows_compat_facade(),
        mock.patch.object(desktop_app, "ctypes", fake_ctypes, create=True),
    ):
        with pytest.raises(RuntimeError, match="unexpected dwm bug"):
            desktop_app._windows_window_is_cloaked(100)
        with pytest.raises(RuntimeError, match="unexpected class bug"):
            desktop_app._windows_window_class_name(100)
        with pytest.raises(RuntimeError, match="unexpected enum bug"):
            desktop_app._enumerate_visible_desktop_windows()
