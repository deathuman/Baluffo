import sys
from types import SimpleNamespace

import pytest

import src.source_sync_runtime as runtime


def test_fernet_key_falls_back_when_keyring_backend_fails(tmp_path, monkeypatch) -> None:
    class KeyringError(Exception):
        pass

    def get_password(_service: str, _username: str) -> str:
        raise KeyringError("backend unavailable")

    monkeypatch.setitem(
        sys.modules,
        "keyring",
        SimpleNamespace(
            errors=SimpleNamespace(KeyringError=KeyringError), get_password=get_password
        ),
    )
    monkeypatch.setattr(runtime, "_resolve_xdg_config_root", lambda: tmp_path / "config")

    key = runtime._get_fernet_key()  # noqa: SLF001

    assert key
    assert (tmp_path / "config" / "sync.key").exists()


def test_fernet_key_falls_back_when_keyring_value_is_invalid(tmp_path, monkeypatch) -> None:
    class KeyringError(Exception):
        pass

    monkeypatch.setitem(
        sys.modules,
        "keyring",
        SimpleNamespace(
            errors=SimpleNamespace(KeyringError=KeyringError),
            get_password=lambda _service, _username: "\udcff",
        ),
    )
    monkeypatch.setattr(runtime, "_resolve_xdg_config_root", lambda: tmp_path / "config")

    key = runtime._get_fernet_key()  # noqa: SLF001

    assert key
    assert (tmp_path / "config" / "sync.key").exists()


def test_fernet_key_unexpected_keyring_failure_propagates(tmp_path, monkeypatch) -> None:
    class KeyringError(Exception):
        pass

    def get_password(_service: str, _username: str) -> str:
        raise AssertionError("keyring bug")

    monkeypatch.setitem(
        sys.modules,
        "keyring",
        SimpleNamespace(
            errors=SimpleNamespace(KeyringError=KeyringError), get_password=get_password
        ),
    )
    monkeypatch.setattr(runtime, "_resolve_xdg_config_root", lambda: tmp_path / "config")

    with pytest.raises(AssertionError, match="keyring bug"):
        runtime._get_fernet_key()  # noqa: SLF001
