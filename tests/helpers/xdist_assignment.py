"""Record which files each pytest worker ran, so a shared-worker interleaving can be replayed.

``--dist=loadfile`` hands whole files to whichever worker is free, and nothing reports the result:
the assignment is invisible in `-q` output. That makes a cross-file leak -- a patch one file
installs and never restores -- reproducible only by running the lane again and hoping the same pair
lands together. The urllib opener leak looked exactly like that: roughly one ``-n 4`` run in four,
reporting itself in a file that imports nothing the corpus code touches.

Each worker writes one JSON record at session end, named for its run and worker. The record also
carries the failing node ids, so a sharded session that fails prints, after its summary, which
worker ran each failure and the single-process command that replays that worker's files in order --
the condition a leak needs: one process, those files, that order.

Recording is always on. A flake happens in an ordinary run, so the record has to exist afterwards;
the cost is one dict update per test and one small write per worker at session end. Records go under
``.tmp/pytest/xdist-assignments`` (gitignored and disposable), or wherever
``BALUFFO_XDIST_ASSIGNMENTS_DIR`` points -- the override the helper's own test uses.

Sessions a test started are recorded too, but under ``nested/`` and left out of the default
listing: the suite spawns child pytest processes all over the place (the seam guards, the packaged
rehearsals), and each would otherwise appear as a one-test "run" indistinguishable from a real one.
A process is nested when it inherited ``PYTEST_CURRENT_TEST`` from the parent that spawned it, or
inherited xdist's run id without being an xdist worker itself. Its record names the test that
spawned it.

:mod:`tests.helpers.xdist_assignment_report` reads what this module writes; the public names are
re-exported here so the command stays ``python -m tests.helpers.xdist_assignment``::

    python -m tests.helpers.xdist_assignment            # newest run, one replay command per worker
    python -m tests.helpers.xdist_assignment --list     # recorded runs, newest first
    python -m tests.helpers.xdist_assignment --all      # the nested child sessions too
    python -m tests.helpers.xdist_assignment --worker gw2 --execute   # replay that worker's files
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import time
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest

from tests.helpers.xdist_assignment_report import (
    NESTED_DIR_NAME,
    RUN_DIR_ENV,
    assignments_root,
    failure_replay_report,
    load_run,
    load_runs,
    main,
    replay_command,
)

# A week of records is ~a few hundred small files; anything older is a previous investigation.
MAX_RECORD_AGE_S = 7 * 24 * 60 * 60.0

# Per-process accumulation, filled by the logstart/logreport hooks and written at session end. The
# list preserves execution order because that is what a replay has to reproduce.
_files: list[str] = []
_tests_per_file: dict[str, int] = {}
_failures: list[str] = []
_started_at = 0.0
# What spawned this session, or ``None`` when a person started it.
_nested_marker: str | None = None
# Where this session's records go, chosen at session start by the process that owns it.
_run_path: Path | None = None


def run_dir(root: Path | None = None) -> Path:
    """The directory this session records into, as chosen at session start."""

    if _run_path is not None:
        return _run_path
    return _fallback_run_dir(root or assignments_root())


def _fallback_run_dir(base: Path) -> Path:
    """A directory for a worker whose owner exported nothing -- the uid-based choice.

    ``PYTEST_XDIST_TESTRUNUID`` is exported to every xdist worker and is identical across them, so
    they still meet in one directory.
    """

    uid = os.environ.get("PYTEST_XDIST_TESTRUNUID") or f"local-{os.getpid()}"
    if _nested_marker is not None:
        return base / NESTED_DIR_NAME / f"nested-{uid}"
    return base / f"run-{uid}"


def _nesting_marker(config: pytest.Config) -> str | None:
    """What spawned this session, or ``None`` for a session a person started.

    ``PYTEST_CURRENT_TEST`` is set by the parent pytest while it runs a test, so any child process
    it spawns inherits it -- the suite's child pytest sessions are all found this way. The second
    branch covers a child that cleared its environment of that variable but still inherited xdist's
    run id: xdist exports the id to workers only, so a process holding it without xdist's worker
    plumbing is a child of one.
    """

    parent_test = os.environ.get("PYTEST_CURRENT_TEST")
    if parent_test:
        return parent_test
    if os.environ.get("PYTEST_XDIST_TESTRUNUID") and not getattr(config, "workerinput", None):
        return "spawned by an xdist worker"
    return None


def _session_run_dir(config: pytest.Config) -> Path:
    """Pick this session's record directory, and export it from the process that owns the session.

    The owner -- a serial session or the xdist controller -- chooses the directory and puts it in
    the environment, which xdist's workers inherit when they are spawned. That is what lets the
    owner read the records back at the end and say which worker ran a failing test: ``xdist`` never
    exports the run id it generates to the controller, so the controller has to be the one naming
    the directory.

    A nested session ignores an inherited directory: that one belongs to the session that spawned
    it, and writing there would put a child's files in the parent's record. It names a fresh
    directory instead, and exports it for its own workers.
    """

    base = assignments_root()
    inherited = os.environ.get(RUN_DIR_ENV)
    if getattr(config, "workerinput", None):
        if inherited:
            return Path(inherited)
        return _fallback_run_dir(base)

    directory = (
        base / NESTED_DIR_NAME / f"nested-{uuid.uuid4().hex}"
        if _nested_marker is not None
        else base / f"run-{uuid.uuid4().hex}"
    )
    os.environ[RUN_DIR_ENV] = str(directory)
    return directory


def _is_xdist_controller(config: pytest.Config) -> bool:
    """True in the process that only schedules workers.

    That process receives every worker's ``logstart`` event, so its accumulation is the whole
    suite, not one worker's share. Writing it would produce a record claiming one worker ran
    everything, which is worse than no record at all.
    """

    if getattr(config, "workerinput", None):
        return False
    return bool(getattr(config.option, "numprocesses", None))


def _worker_name(config: pytest.Config) -> str:
    workerinput = getattr(config, "workerinput", None)
    if isinstance(workerinput, Mapping):
        return str(workerinput.get("workerid") or "worker")
    if _nested_marker is not None:
        # Nested serial children of one run share a directory, so the pid is what keeps their
        # records apart; a real serial run is alone in its own and needs no suffix.
        return f"solo-{os.getpid()}"
    return "solo"


def _prune_old_runs(root: Path) -> None:
    """Drop records from earlier investigations; a live run only writes at session end."""

    cutoff = time.time() - MAX_RECORD_AGE_S
    try:
        children = list(root.iterdir())
    except OSError:
        return
    for child in children:
        try:
            if child.is_dir() and child.stat().st_mtime < cutoff:
                shutil.rmtree(child, ignore_errors=True)
        except OSError:
            continue


def record_session_start(config: pytest.Config) -> None:
    """Reset this process's accumulation; the session owner names the run and drops stale records."""

    global _nested_marker, _run_path, _started_at

    _files.clear()
    _tests_per_file.clear()
    _failures.clear()
    _nested_marker = _nesting_marker(config)
    _started_at = time.time()
    _run_path = _session_run_dir(config)
    if not getattr(config, "workerinput", None) and _nested_marker is None:
        # Only the process that owns the session prunes; a worker would race the run it belongs to,
        # and a nested child only ever writes its own directory.
        _prune_old_runs(assignments_root())


def record_test_start(location: tuple[str, int | None, str]) -> None:
    """Record the file a test came from, in the order this process runs them."""

    path = location[0]
    if path not in _tests_per_file:
        _files.append(path)
        _tests_per_file[path] = 0
    _tests_per_file[path] += 1


def record_test_outcome(report: pytest.TestReport) -> None:
    """Remember a failing node id, once, in the order this process saw it.

    The worker that ran the test is the only process whose record needs it, and it is also the only
    one that can name it precisely: the controller sees the same report but not which worker ran it.
    """

    if not report.failed or report.nodeid in _failures:
        return
    _failures.append(report.nodeid)


def record_session_end(session: pytest.Session) -> None:
    """Write this process's record. A write failure must never fail a test run."""

    config = session.config
    if _is_xdist_controller(config) or not _files:
        return
    record: dict[str, Any] = {
        "run": run_dir().name.removeprefix("run-").removeprefix("nested-"),
        "worker": _worker_name(config),
        "nested": _nested_marker is not None,
        "spawned_by": _nested_marker,
        "rootdir": str(config.rootpath),
        "started_at": round(_started_at, 3),
        "finished_at": round(time.time(), 3),
        "tests": sum(_tests_per_file.values()),
        "files": [{"path": path, "tests": _tests_per_file[path]} for path in _files],
        "failures": list(_failures),
    }
    try:
        directory = run_dir()
        directory.mkdir(parents=True, exist_ok=True)
        target = directory / f"worker-{record['worker']}.json"
        staged = target.with_suffix(".json.tmp")
        staged.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
        staged.replace(target)
    except OSError:
        return


def report_failing_workers(config: pytest.Config) -> None:
    """Print the replay commands at the end of a sharded session that had failures.

    Called from ``pytest_unconfigure`` so it lands after the session's own summary: the workers
    have all written their records by then, which is what makes the failing worker knowable here.
    Nothing is printed for a passing session, a serial one, or a worker process.
    """

    if _run_path is None or getattr(config, "workerinput", None):
        return
    if not getattr(config.option, "numprocesses", None):
        return
    lines = failure_replay_report(_run_path)
    if lines:
        print("\n".join(lines), flush=True)


# The hooks below exist so a child pytest can load this module as a plugin
# (``-p tests.helpers.xdist_assignment``). The suite's own conftest calls the ``record_*`` functions
# from its hooks instead of registering this module, because ``pytest_plugins`` is only permitted in
# a top-level conftest and ``tests/conftest.py`` is not one (the rootdir is the repository root).
def pytest_configure(config: pytest.Config) -> None:
    record_session_start(config)


def pytest_runtest_logstart(nodeid: str, location: tuple[str, int | None, str]) -> None:
    record_test_start(location)


def pytest_runtest_logreport(report: pytest.TestReport) -> None:
    record_test_outcome(report)


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    record_session_end(session)


def pytest_unconfigure(config: pytest.Config) -> None:
    report_failing_workers(config)


__all__ = [
    "assignments_root",
    "failure_replay_report",
    "load_run",
    "load_runs",
    "main",
    "record_session_end",
    "record_session_start",
    "record_test_outcome",
    "record_test_start",
    "replay_command",
    "report_failing_workers",
    "run_dir",
]

if __name__ == "__main__":
    sys.exit(main())
