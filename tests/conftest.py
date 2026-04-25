"""Pytest root conftest. Clear host desktop env before importing Baluffo modules.

``get_storage_defaults()`` and derived constants (e.g. ``DEFAULT_SOCIAL_CONFIG_PATH``) are
computed at import time. A leaked ``BALUFFO_DATA_DIR`` from a local EXE/smoke session would
poison the entire test process.
"""

from __future__ import annotations

import os
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

from pathlib import Path

import pytest

from tests.helpers.temp_paths import (
    TEST_TMP_ROOT,
    cleanup_stale_workspace_tmpdirs,
    make_workspace_tmpdir,
    remove_workspace_tmpdir,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
CODEX_TMP_ROOT = TEST_TMP_ROOT
CODEX_TMP_ROOT.mkdir(parents=True, exist_ok=True)

cleanup_stale_workspace_tmpdirs(
    REPO_ROOT / ".codex-test-tmp",
    REPO_ROOT / ".codex-tmp-tests",
    CODEX_TMP_ROOT,
)


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
        root = make_workspace_tmpdir(prefix, root=codex_tmp_root)
        created.append(root)
        return root

    yield _make

    for root in created:
        remove_workspace_tmpdir(root)
