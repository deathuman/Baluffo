"""Pytest root conftest. Clear host desktop env before importing Baluffo modules.

``get_storage_defaults()`` and derived constants (e.g. ``DEFAULT_SOCIAL_CONFIG_PATH``) are
computed at import time. A leaked ``BALUFFO_DATA_DIR`` from a local EXE/smoke session would
poison the entire test process.
"""

from __future__ import annotations

import atexit
import os
import time
from collections.abc import Callable, Generator

_BALUFFO_RUNTIME_ISOLATION_KEYS = (
    "BALUFFO_DATA_DIR",
    "BALUFFO_SHIP_ROOT",
    "BALUFFO_INSTALL_ROOT",
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

from tests.helpers.discovery_artifact_hygiene import (
    changed_discovery_audit_artifacts,
    format_discovery_audit_change,
    snapshot_discovery_audit_artifacts,
)
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
def _clear_baluffo_runtime_env_each_test() -> Generator[None]:
    """Prevent one test (or host) from leaving desktop spawn env around for the next test."""
    for key in _BALUFFO_RUNTIME_ISOLATION_KEYS:
        os.environ.pop(key, None)
    yield
    for key in _BALUFFO_RUNTIME_ISOLATION_KEYS:
        os.environ.pop(key, None)


@pytest.fixture(scope="session")
def repo_root() -> Path:
    return REPO_ROOT


@pytest.fixture(scope="session", autouse=True)
def _assert_tests_do_not_mutate_repo_discovery_audits() -> Generator[None]:
    before = snapshot_discovery_audit_artifacts(REPO_ROOT)
    yield
    changed = changed_discovery_audit_artifacts(REPO_ROOT, before)
    if changed:
        pytest.fail(format_discovery_audit_change(REPO_ROOT, changed))


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


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers", "windows: Test requires Windows-specific APIs; skipped on Linux."
    )
    _claim_single_pytest_session(config)


_SESSION_LOCK_STALE_S = 6 * 60 * 60


def _claim_single_pytest_session(config: pytest.Config) -> None:
    """Refuse to start a second pytest session against the same checkout.

    Why this exists, and why the obvious fixes do not work
    --------------------------------------------------------
    pytest 9.1.1 puts every ``tmp_path`` under ``.tmp/pytest/pytest-tmp-<uuid>``
    and **deletes sibling ``pytest-tmp-*`` directories when a session starts**
    (verified: a planted ``pytest-tmp-SENTINEL123`` does not survive a run). Two
    sessions in one checkout therefore race on a shared root, and one can remove
    the directory the other is still writing into. That is the proven cause of
    ``test_bridge_profile_summary_records_external_sample_failure`` failing with
    a ``FileNotFoundError`` from a plain ``Path.write_text``.

    Three candidate fixes were measured and all were no-ops on pytest 9.1.1,
    because ``tmp_path`` no longer derives from any of them:

    - ``--basetemp=.tmp/pytest/basetemp`` (what the npm scripts pin) - ignored;
      the observed root is the *parent* of the value, so a per-process suffix
      still resolves to the same shared ``.tmp/pytest``.
    - a deeper ``--basetemp`` (``.../run-<pid>/inner``) - still ignored.
    - ``PYTEST_DEBUG_TEMPROOT`` - ignored.

    ``tmp_path`` also already lands in a per-session UUID directory, so the
    recorded "shared basetemp" explanation was never the mechanism. What is left
    is to stop the sessions colliding at all, so a second run fails with an
    explanation instead of tearing out a live run's working directory.

    A lock is used rather than a PID-liveness probe because checking another
    process portably needs platform-specific calls; a run that has not finished
    in six hours was not a run.
    """
    lock_dir = Path(__file__).resolve().parents[1] / ".tmp" / "pytest"
    lock_path = lock_dir / "session.lock"
    try:
        lock_dir.mkdir(parents=True, exist_ok=True)
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        holder = _describe_lock_holder(lock_path)
        if holder is None:
            # Stale lock from a run that never released it; take it over.
            try:
                lock_path.unlink()
                fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            except OSError:
                fd = None
        else:
            pytest.exit(
                "Another pytest session is already running against this checkout.\n"
                f"  holder: {holder}\n"
                "  Wait for it to finish, or delete "
                f"{lock_path} if you are certain no run is in flight.\n"
                "Two concurrent pytest sessions share .tmp/pytest and will delete "
                "each other's tmp_path directories.",
                returncode=4,
            )
    except OSError:
        return  # Never block a test run because the lock could not be created.

    if fd is None:
        return
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(f"pid={os.getpid()}\nstarted={time.time():.0f}\n")
    # pytest 9 has no config.add_cleanup_handler, and atexit covers the normal
    # exit path. A hard kill leaves the lock behind, which the staleness check
    # above takes over.
    atexit.register(lambda: lock_path.unlink(missing_ok=True))


def _describe_lock_holder(lock_path: Path) -> str | None:
    """Return a human description of the lock's holder, or None if it is stale."""
    try:
        age = time.time() - lock_path.stat().st_mtime
        if age > _SESSION_LOCK_STALE_S:
            return None
        return lock_path.read_text(encoding="utf-8").strip().replace("\n", " ")
    except OSError:
        return None
