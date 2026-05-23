import os

import pytest


@pytest.mark.skipif(os.name != "nt", reason="Windows dispatch contract")
def test_windows_functions_resolve_to_windows_module_on_windows() -> None:
    """Every _windows_* facade function must resolve to _windows.py's implementation."""
    from src.ship import desktop_app
    from src.ship.desktop_app import _windows as win_mod

    for name in sorted(dir(win_mod)):
        if not name.startswith("_windows_"):
            continue
        expected = getattr(win_mod, name)
        if not callable(expected):
            continue

        desktop_app.__dict__.pop(name, None)
        resolved = getattr(desktop_app, name)

        assert resolved is expected, (
            f"{name} resolved to {type(resolved).__module__}, expected _windows"
        )
