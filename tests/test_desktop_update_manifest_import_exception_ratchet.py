import builtins
import importlib

import pytest


def _manifest_module():
    return importlib.import_module("src.ship.desktop_update_manifest")


def test_optional_cryptography_import_failure_disables_manifest_signing(monkeypatch) -> None:
    manifest = _manifest_module()
    real_import = builtins.__import__

    def fake_import(name, globals_=None, locals_=None, fromlist=(), level=0):  # noqa: ANN001
        if name.startswith("cryptography"):
            raise ModuleNotFoundError("No module named 'cryptography'")
        return real_import(name, globals_, locals_, fromlist, level)

    with monkeypatch.context() as context:
        context.setattr(builtins, "__import__", fake_import)
        reloaded = importlib.reload(manifest)

    try:
        assert reloaded.Ed25519SigningClass is None
        assert reloaded.Ed25519VerifierClass is None
    finally:
        importlib.reload(manifest)


def test_optional_cryptography_import_does_not_hide_unexpected_failures(
    monkeypatch,
) -> None:
    manifest = _manifest_module()
    real_import = builtins.__import__

    def fake_import(name, globals_=None, locals_=None, fromlist=(), level=0):  # noqa: ANN001
        if name.startswith("cryptography"):
            raise RuntimeError("unexpected cryptography import bug")
        return real_import(name, globals_, locals_, fromlist, level)

    try:
        with monkeypatch.context() as context:
            context.setattr(builtins, "__import__", fake_import)
            with pytest.raises(RuntimeError, match="unexpected cryptography import bug"):
                importlib.reload(manifest)
    finally:
        importlib.reload(manifest)
