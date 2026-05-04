from __future__ import annotations

import json

import pytest

from src import source_sync as sync


@pytest.fixture()
def source_sync_test_root(make_test_root, monkeypatch):
    """Temp root for source_sync tests: clears runtime state, provides config_path and env."""
    root = make_test_root("source-sync")
    config_path = root / "github-app-sync-config.json"
    home = root / "home"
    appdata = root / "appdata"
    local_appdata = root / "localappdata"
    env = {
        sync.PACKAGED_SYNC_CONFIG_ENV: str(config_path),
        "APPDATA": str(appdata),
        "HOME": str(home),
        "LOCALAPPDATA": str(local_appdata),
        "USERPROFILE": str(home),
    }
    monkeypatch.setattr(
        sync,
        "DEFAULT_PACKAGED_SYNC_CONFIG_PATH",
        root / "missing-default-github-app-sync-config.json",
    )
    sync._clear_runtime_state()  # noqa: SLF001
    sync._source_sync_runtime.clear_sync_counters(sync)  # noqa: SLF001
    with sync._AUTH_MANAGER_LOCK:  # noqa: SLF001
        sync._AUTH_MANAGER.clear()  # noqa: SLF001
    with sync._RATE_LIMIT_LOCK:  # noqa: SLF001
        sync._RATE_LIMIT_STATE["calls"] = []  # noqa: SLF001
        sync._RATE_LIMIT_STATE["strike"] = 0  # noqa: SLF001
        sync._RATE_LIMIT_STATE["until"] = None  # noqa: SLF001

    def write_packaged_config(payload: dict | None = None) -> None:
        base = {
            "schemaVersion": 1,
            "appId": "123456",
            "installationId": "999999",
            "repo": "owner/repo",
            "branch": "main",
            "path": "baluffo/source-sync.json",
            "privateKeyPem": "-----BEGIN RSA PRIVATE KEY-----\nTEST\n-----END RSA PRIVATE KEY-----",
        }
        if payload:
            base.update(payload)
        config_path.write_text(json.dumps(base), encoding="utf-8")

    class _Root:
        pass

    out = _Root()
    out.root = root
    out.config_path = config_path
    out.env = env
    out.write_packaged_config = write_packaged_config
    yield out

    sync._clear_runtime_state()  # noqa: SLF001
    sync._source_sync_runtime.clear_sync_counters(sync)  # noqa: SLF001
    with sync._AUTH_MANAGER_LOCK:  # noqa: SLF001
        sync._AUTH_MANAGER.clear()  # noqa: SLF001
