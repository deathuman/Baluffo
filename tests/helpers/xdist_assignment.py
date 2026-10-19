"""Record which files each pytest worker ran, so a shared-worker interleaving can be replayed.

``--dist=loadfile`` hands whole files to whichever worker is free, and nothing reports the result:
the assignment is invisible in `-q` output. That makes a cross-file leak -- a patch one file
installs and never restores -- reproducible only by running the lane again and hoping the same pair
lands together. The urllib opener leak looked exactly like that: roughly one ``-n 4`` run in four,
reporting itself in a file that imports nothing the corpus code touches.

Each worker writes one JSON record at session end, named for its run and worker.
:func:`replay_command` turns a record into a single-process pytest invocation over the same files in
the same order, which is the condition a leak needs: one process, those files, that order.

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

Read a run with::

    python -m tests.helpers.xdist_assignment            # newest run, one replay command per worker
    python -m tests.helpers.xdist_assignment --list     # recorded runs, newest first
    python -m tests.helpers.xdist_assignment --all      # the nested child sessions too
    python -m tests.helpers.xdist_assignment --worker gw2 --execute   # replay that worker's files
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import shutil
import subprocess
import sys
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import pytest

ASSIGNMENTS_DIR_ENV = "BALUFFO_XDIST_ASSIGNMENTS_DIR"
DEFAULT_ROOT = Path(__file__).resolve().parents[2] / ".tmp" / "pytest" / "xdist-assignments"
NESTED_DIR_NAME = "nested"

# A week of records is ~a few hundred small files; anything older is a previous investigation.
MAX_RECORD_AGE_S = 7 * 24 * 60 * 60.0

# Per-process accumulation, filled by ``pytest_runtest_logstart`` and written at session end. The
# list preserves execution order because that is what a replay has to reproduce.
_files: list[str] = []
_tests_per_file: dict[str, int] = {}
_started_at = 0.0
# What spawned this session, or ``None`` when a person started it.
_nested_marker: str | None = None


def assignments_root() -> Path:
    """The records directory: the environment override, else the repo-local disposable root."""

    override = os.environ.get(ASSIGNMENTS_DIR_ENV)
    return Path(override) if override else DEFAULT_ROOT


def run_dir(root: Path | None = None) -> Path:
    """The directory for this process's run.

    ``PYTEST_XDIST_TESTRUNUID`` is exported to every xdist worker and is identical across them, so
    they meet in one directory. The controller has no public copy of that id and does not write a
    record, and a serial session is a single process, so both fall back to their own pid -- unique
    enough for one run and never shared with another.
    """

    base = root or assignments_root()
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
    """Reset this process's accumulation and, in the session owner, drop stale records."""

    global _nested_marker, _started_at

    _files.clear()
    _tests_per_file.clear()
    _nested_marker = _nesting_marker(config)
    _started_at = time.time()
    if _is_xdist_controller(config):
        # Only the process that owns the session prunes, and an xdist controller is still that
        # owner; a worker pruning would race the run it belongs to.
        _prune_old_runs(assignments_root())


def record_test_start(location: tuple[str, int | None, str]) -> None:
    """Record the file a test came from, in the order this process runs them."""

    path = location[0]
    if path not in _tests_per_file:
        _files.append(path)
        _tests_per_file[path] = 0
    _tests_per_file[path] += 1


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


# The hooks below exist so a child pytest can load this module as a plugin
# (``-p tests.helpers.xdist_assignment``). The suite's own conftest calls the ``record_*`` functions
# from its hooks instead of registering this module, because ``pytest_plugins`` is only permitted in
# a top-level conftest and ``tests/conftest.py`` is not one (the rootdir is the repository root).
def pytest_configure(config: pytest.Config) -> None:
    record_session_start(config)


def pytest_runtest_logstart(nodeid: str, location: tuple[str, int | None, str]) -> None:
    record_test_start(location)


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    record_session_end(session)


def _worker_order(worker: Mapping[str, Any]) -> tuple[int, int, str]:
    """gw2 before gw10: a plain string sort puts "gw10" first, which reads as the lead worker."""

    name = str(worker.get("worker", ""))
    digits = name.removeprefix("gw")
    if digits.isdigit():
        return (0, int(digits), "")
    return (1, 0, name)


def load_run(directory: Path) -> dict[str, Any] | None:
    """Read one run directory, newest worker record first. Unreadable records are skipped."""

    workers: list[dict[str, Any]] = []
    for path in sorted(directory.glob("worker-*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        # A half-written or foreign record is not evidence of an assignment, so it is dropped
        # rather than guessed at: a wrong replay target wastes more time than a missing one.
        if not isinstance(payload, dict) or not isinstance(payload.get("files"), list):
            continue
        workers.append(payload)
    if not workers:
        return None
    workers.sort(key=_worker_order)
    return {
        "run": directory.name.removeprefix("run-").removeprefix("nested-"),
        "dir": directory,
        "workers": workers,
    }


def load_runs(root: Path | None = None, *, include_nested: bool = False) -> list[dict[str, Any]]:
    """Every recorded run under ``root``, newest first.

    Nested sessions are excluded unless asked for: one child pytest process is not a run anyone
    wants to replay, and listing them made the real runs harder to find than the records help.
    """

    base = root or assignments_root()
    try:
        directories = [child for child in base.iterdir() if child.is_dir()]
    except OSError:
        return []
    directories = [child for child in directories if child.name != NESTED_DIR_NAME]
    if include_nested:
        try:
            directories += [child for child in (base / NESTED_DIR_NAME).iterdir() if child.is_dir()]
        except OSError:
            pass
    directories.sort(key=lambda child: child.stat().st_mtime if child.exists() else 0.0)
    runs = [load_run(directory) for directory in reversed(directories)]
    return [run for run in runs if run is not None]


def replay_command(worker: Mapping[str, Any], *, rootdir: str | None = None) -> str:
    """The single-process pytest command that replays exactly the files one worker ran.

    Order is preserved from the record: pytest runs explicitly named files in the order given, so
    this reproduces both the file set and the sequence a leak would have seen.
    """

    # POSIX separators in the printed command: the record holds the platform's own (backslashes on
    # Windows), and a copied command in a POSIX shell reads a backslash as an escape.
    paths = [
        Path(str(entry["path"])).as_posix()
        for entry in worker.get("files", [])
        if isinstance(entry, Mapping) and entry.get("path")
    ]
    directory = rootdir or str(worker.get("rootdir") or ".")
    return " ".join(["python -m pytest", *paths, "-q", f"--rootdir={Path(directory).as_posix()}"])


def _format_counts(value: Any) -> str:
    return f"{int(value):,}" if isinstance(value, int) else "?"


def _print_run(run: Mapping[str, Any], *, worker: str | None, execute: bool) -> int:
    print(f"run {run['run']}  ({run['dir']})", flush=True)
    spawned_by = sorted(
        {str(worker.get("spawned_by")) for worker in run["workers"] if worker.get("nested")}
    )
    if spawned_by:
        print("  nested child session, spawned by: " + ", ".join(spawned_by), flush=True)
    selected = [
        record for record in run["workers"] if worker is None or record.get("worker") == worker
    ]
    if not selected:
        print(
            f"  no worker record named {worker!r}; recorded: "
            + ", ".join(str(record.get("worker")) for record in run["workers"])
        )
        return 1
    status = 0
    for record in selected:
        print(
            f"  {record.get('worker', '?')}: {_format_counts(record.get('tests'))} tests"
            f" in {_format_counts(len(record.get('files', [])))} files",
            # Flushed before a replay runs: otherwise the child's output arrives first and the
            # command that produced it looks like it was printed afterwards.
            flush=True,
        )
        command = replay_command(record)
        print(f"    {command}", flush=True)
        if execute:
            # No shell: the command is a path list this module built, and a shell would let a
            # checkout path containing a space split a file name in two. Run it from the
            # session's own rootdir, because a relative path argument makes pytest collect only
            # the directories it walked below that root -- an absolute one makes it walk the
            # file's ancestors, which for a temp root means listing directories other processes
            # create and delete mid-run.
            rootdir = record.get("rootdir")
            completed = subprocess.run(
                shlex.split(command), cwd=str(rootdir) if rootdir else None, check=False
            )
            status = status or completed.returncode
    return status


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m tests.helpers.xdist_assignment",
        description="Show which files each pytest worker ran, and how to replay one worker's set.",
    )
    parser.add_argument("--root", type=Path, default=None, help="records directory to read")
    parser.add_argument("--list", action="store_true", dest="list_runs", help="list runs only")
    parser.add_argument("--run", default=None, help="run id (see --list); default newest")
    parser.add_argument(
        "--all",
        action="store_true",
        dest="include_nested",
        help="include nested child sessions, which are hidden by default",
    )
    parser.add_argument("--worker", default=None, help="a worker id such as gw2")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="run the replay command instead of printing it",
    )
    args = parser.parse_args(argv)

    runs = load_runs(args.root, include_nested=args.include_nested)
    if not runs:
        print(f"no recorded runs under {args.root or assignments_root()}")
        return 1
    if args.list_runs:
        for run in runs:
            workers = ", ".join(str(record.get("worker")) for record in run["workers"])
            suffix = "  [nested]" if any(w.get("nested") for w in run["workers"]) else ""
            print(f"{run['run']}  workers: {workers}{suffix}")
        return 0

    run = runs[0]
    if args.run is not None:
        matched = [candidate for candidate in runs if candidate["run"] == args.run]
        if not matched:
            print(f"no recorded run {args.run!r}; see --list, or --all for nested sessions")
            return 1
        run = matched[0]
    return _print_run(run, worker=args.worker, execute=args.execute)


if __name__ == "__main__":
    sys.exit(main())
