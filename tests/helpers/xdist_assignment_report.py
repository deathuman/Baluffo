"""Reading half of the worker-assignment records: load them, replay one worker, print the CLI.

The recorder that writes these lives in :mod:`tests.helpers.xdist_assignment`; this module only reads
what it wrote, so it has no pytest dependency and can be used from a shell after the run.
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

ASSIGNMENTS_DIR_ENV = "BALUFFO_XDIST_ASSIGNMENTS_DIR"
# Exported by the session owner so every worker writes into the directory the owner chose, and the
# owner can read the records back when the session ends.
RUN_DIR_ENV = "BALUFFO_XDIST_RUN_DIR"
DEFAULT_ROOT = Path(__file__).resolve().parents[2] / ".tmp" / "pytest" / "xdist-assignments"
NESTED_DIR_NAME = "nested"


def assignments_root() -> Path:
    """The records directory: the environment override, else the repo-local disposable root."""

    override = os.environ.get(ASSIGNMENTS_DIR_ENV)
    return Path(override) if override else DEFAULT_ROOT


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


def failure_replay_report(directory: Path, *, limit: int = 5) -> list[str]:
    """Lines naming each failing test's worker and the command that replays that worker.

    One process over that worker's files in that order is the condition a cross-file leak needs;
    re-running the sharded lane would only re-roll the assignment that produced the failure.
    """

    run = load_run(directory)
    if run is None:
        return []
    lines: list[str] = []
    for worker in run["workers"]:
        failures = [str(nodeid) for nodeid in worker.get("failures") or []]
        if not failures:
            continue
        name = str(worker.get("worker") or "?")
        lines.append(f"  {name}: {len(failures)} failed")
        lines.extend(f"    {nodeid}" for nodeid in failures[:limit])
        if len(failures) > limit:
            lines.append(f"    ... and {len(failures) - limit} more")
        lines.append(f"    replay {name}: {replay_command(worker)}")
    if not lines:
        return []
    return ["sharded failures, by the worker that ran them:", *lines]


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
        failed = len(record.get("failures") or [])
        outcome = f", {_format_counts(failed)} failed" if failed else ""
        print(
            f"  {record.get('worker', '?')}: {_format_counts(record.get('tests'))} tests"
            f" in {_format_counts(len(record.get('files', [])))} files{outcome}",
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


__all__ = [
    "ASSIGNMENTS_DIR_ENV",
    "DEFAULT_ROOT",
    "NESTED_DIR_NAME",
    "RUN_DIR_ENV",
    "assignments_root",
    "failure_replay_report",
    "load_run",
    "load_runs",
    "main",
    "replay_command",
]
