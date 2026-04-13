"""Pytest root conftest. Clear host desktop env before importing Baluffo modules.

``get_storage_defaults()`` and derived constants (e.g. ``DEFAULT_SOCIAL_CONFIG_PATH``) are
computed at import time. A leaked ``BALUFFO_DATA_DIR`` from a local EXE/smoke session would
poison the entire test process.
"""

from __future__ import annotations

import os
import shutil
import uuid
from collections.abc import Callable

_BALUFFO_RUNTIME_ISOLATION_KEYS = (
    "BALUFFO_DATA_DIR",
    "BALUFFO_DISCOVERY_REPORT_PATH",
    "BALUFFO_DISCOVERY_LOG_PATH",
    "BALUFFO_DISCOVERY_RUN_ID",
    "BALUFFO_DISCOVERY_STARTED_AT",
    "BALUFFO_DESKTOP_MODE",
    "BALUFFO_STARTUP_PROBE",
)

for _key in _BALUFFO_RUNTIME_ISOLATION_KEYS:
    os.environ.pop(_key, None)

import json
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
CODEX_TMP_ROOT = REPO_ROOT / ".tmp" / "pytest"
CODEX_TMP_ROOT.mkdir(parents=True, exist_ok=True)

for _stale_root in (
    REPO_ROOT / ".codex-test-tmp",
    REPO_ROOT / ".codex-tmp-tests",
    CODEX_TMP_ROOT,
):
    if not _stale_root.exists():
        continue
    for _child in _stale_root.iterdir():
        if _child.is_dir() and _child.name.startswith("pytest-"):
            shutil.rmtree(_child, ignore_errors=True)


@pytest.fixture(autouse=True)
def _clear_baluffo_runtime_env_each_test() -> None:
    """Prevent one test (or host) from leaving desktop spawn env around for the next test."""
    for key in _BALUFFO_RUNTIME_ISOLATION_KEYS:
        os.environ.pop(key, None)
    yield
    for key in _BALUFFO_RUNTIME_ISOLATION_KEYS:
        os.environ.pop(key, None)


@pytest.fixture(scope="session")
def repo_root() -> Path:
    return REPO_ROOT


@pytest.fixture()
def codex_tmp_root() -> Path:
    CODEX_TMP_ROOT.mkdir(parents=True, exist_ok=True)
    return CODEX_TMP_ROOT


@pytest.fixture()
def tmp_path(make_test_root: Callable[[str], Path]) -> Path:
    """Repo-local replacement for PyTest's tmp_path fixture.

    Windows ACLs in this environment can make PyTest's built-in tmpdir cleanup
    unreliable, so we use the existing disposable workspace temp helper instead.
    """

    return make_test_root("pytest-tmp")


@pytest.fixture()
def make_test_root(codex_tmp_root: Path):
    created: list[Path] = []

    def _make(prefix: str) -> Path:
        root = codex_tmp_root / f"{prefix}-{uuid.uuid4().hex}"
        root.mkdir(parents=True, exist_ok=True)
        created.append(root)
        return root

    yield _make

    for root in created:
        shutil.rmtree(root, ignore_errors=True)


@pytest.fixture()
def source_sync_test_root(make_test_root):
    """Temp root for source_sync tests: clears runtime state, provides config_path and env."""
    from src import source_sync as sync

    root = make_test_root("source-sync")
    config_path = root / "github-app-sync-config.json"
    env = {sync.PACKAGED_SYNC_CONFIG_ENV: str(config_path)}
    sync._clear_runtime_state()  # noqa: SLF001
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
