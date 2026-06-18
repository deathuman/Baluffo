import builtins
import importlib

import pytest


def _desktop_update_module():
    return importlib.import_module("src.ship.desktop_update")


def test_desktop_update_facade_optional_psutil_import_failure_falls_back(monkeypatch) -> None:
    du = _desktop_update_module()
    real_import = builtins.__import__

    def fake_import(name, globals_=None, locals_=None, fromlist=(), level=0):  # noqa: ANN001
        if name == "psutil":
            raise ModuleNotFoundError("No module named 'psutil'")
        return real_import(name, globals_, locals_, fromlist, level)

    with monkeypatch.context() as context:
        context.setattr(builtins, "__import__", fake_import)
        reloaded = importlib.reload(du)

    try:
        assert reloaded.psutil is None
    finally:
        importlib.reload(du)


def test_desktop_update_facade_optional_psutil_import_does_not_hide_unexpected_failures(
    monkeypatch,
) -> None:
    du = _desktop_update_module()
    real_import = builtins.__import__

    def fake_import(name, globals_=None, locals_=None, fromlist=(), level=0):  # noqa: ANN001
        if name == "psutil":
            raise RuntimeError("unexpected psutil import bug")
        return real_import(name, globals_, locals_, fromlist, level)

    try:
        with monkeypatch.context() as context:
            context.setattr(builtins, "__import__", fake_import)
            with pytest.raises(RuntimeError, match="unexpected psutil import bug"):
                importlib.reload(du)
    finally:
        importlib.reload(du)
