#!/usr/bin/env python3
"""In-container per-thread CPU sampler for the fetch child (dev-only payload).

Copied into a bench container by ``tools/measurements/pipeline/thread_cpu.py``
and run as one long-lived ``docker exec`` whose stdout is the driver's input --
the same pattern ``full_pipeline_memory.py`` uses for its memory sampler.

It reads *another* process's ``/proc`` entries, so nothing is injected into the
pipeline and the measurement cannot perturb what it measures. Absolute counters
are emitted, never deltas: a truncated run stays usable, and the arithmetic lives
on the host where it can be tested.

Usage (inside the container):
    python /tmp/baluffo-thread-cpu.py --match jobs_fetcher --interval 1.0 [--ticks N]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

DEFAULT_PROC_ROOT = "/proc"
DEFAULT_MATCH = "jobs_fetcher"
DEFAULT_INTERVAL_S = 1.0


def parse_stat_cpu_seconds(raw: bytes | str) -> float | None:
    """``utime + stime`` from one ``/proc/<pid>/[task/<tid>/]stat`` line.

    ``comm`` may contain spaces and parentheses, so fields are counted from
    after the *last* ``)``; splitting the whole line shifts every index. A shape
    we do not recognise returns ``None`` rather than a guess.

    Deliberately duplicated from ``src.shared.profile_utils._parse_task_stat_cpu_seconds``:
    this file is copied into a container whose ``src/`` tree is not on
    ``sys.path`` for a ``/tmp`` script, and reaching for a private helper across
    that boundary would be worse than the duplication. ``tests/tools/test_thread_cpu_sampler.py``
    pins the two against the same fixture so they cannot drift.
    """
    text = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else str(raw)
    close = text.rfind(")")
    if close < 0:
        return None
    fields = text[close + 1 :].split()
    if len(fields) < 13:
        return None
    try:
        utime_ticks = int(fields[11])
        stime_ticks = int(fields[12])
    except ValueError:
        return None
    return (utime_ticks + stime_ticks) / _clock_ticks_per_second()


def _clock_ticks_per_second() -> int:
    try:
        return int(os.sysconf("SC_CLK_TCK"))
    except (AttributeError, OSError, ValueError):
        return 100


def parse_stat_state(raw: bytes | str) -> str:
    """Single-letter process state (``R``, ``S``, ``D`` ...) from a stat line."""
    text = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else str(raw)
    close = text.rfind(")")
    if close < 0:
        return ""
    fields = text[close + 1 :].split()
    return fields[0] if fields else ""


def read_cmdline(proc_root: Path, pid: int) -> str:
    """NUL-separated argv as a space-joined string; empty when unreadable."""
    try:
        raw = (proc_root / str(pid) / "cmdline").read_bytes()
    except OSError:
        return ""
    return " ".join(part for part in raw.decode("utf-8", errors="replace").split("\x00") if part)


def find_target_pids(proc_root: Path, match: str, *, exclude: set[int]) -> list[int]:
    """PIDs whose command line contains ``match``.

    Matches on the command line, never on a pid we were told about: the fetch
    child is spawned by the container's own service, so its pid is only knowable
    by looking.
    """
    found: list[int] = []
    try:
        entries = list(proc_root.iterdir())
    except OSError:
        return found
    for entry in entries:
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        if pid in exclude:
            continue
        cmdline = read_cmdline(proc_root, pid)
        if cmdline and match in cmdline:
            found.append(pid)
    return sorted(found)


def sample_process(proc_root: Path, pid: int) -> dict[str, Any]:
    """One tick of absolute counters for a process and each of its threads."""
    try:
        process_stat = (proc_root / str(pid) / "stat").read_bytes()
    except OSError:
        return {"pid": pid, "gone": True}
    threads: list[dict[str, Any]] = []
    task_dir = proc_root / str(pid) / "task"
    try:
        task_entries = sorted(entry for entry in task_dir.iterdir() if entry.name.isdigit())
    except OSError:
        task_entries = []
    for tid_dir in task_entries:
        try:
            tid_stat = (tid_dir / "stat").read_bytes()
        except OSError:
            continue
        cpu_seconds = parse_stat_cpu_seconds(tid_stat)
        if cpu_seconds is None:
            continue
        try:
            comm = (tid_dir / "comm").read_text(encoding="utf-8", errors="replace").strip()
        except OSError:
            comm = ""
        threads.append(
            {
                "tid": int(tid_dir.name),
                "comm": comm,
                "state": parse_stat_state(tid_stat),
                "cpuSeconds": round(cpu_seconds, 6),
            }
        )
    return {
        "pid": pid,
        "gone": False,
        "cmdline": read_cmdline(proc_root, pid),
        "state": parse_stat_state(process_stat),
        "processCpuSeconds": round(parse_stat_cpu_seconds(process_stat) or 0.0, 6),
        "threads": threads,
    }


def _emit(payload: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(payload, ensure_ascii=False) + "\n")
    sys.stdout.flush()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--match", default=DEFAULT_MATCH, help="substring of the target command line"
    )
    parser.add_argument(
        "--interval", type=float, default=DEFAULT_INTERVAL_S, help="seconds per tick"
    )
    parser.add_argument(
        "--ticks", type=int, default=0, help="stop after N ticks (0 = until stopped)"
    )
    parser.add_argument("--proc-root", default=DEFAULT_PROC_ROOT)
    args = parser.parse_args(argv)

    proc_root = Path(args.proc_root)
    if not (proc_root / "self").exists():
        print(
            f"thread_cpu_sampler: {proc_root} has no /proc -- this payload is Linux-only "
            "(the fetch child's per-thread CPU is not otherwise observable)",
            file=sys.stderr,
        )
        return 2

    interval_s = max(0.05, float(args.interval))
    self_pid = os.getpid()
    exclude = {self_pid, os.getppid()}
    tick = 0
    while True:
        tick += 1
        started = time.monotonic()
        for pid in find_target_pids(proc_root, args.match, exclude=exclude):
            payload = sample_process(proc_root, pid)
            payload["tick"] = tick
            payload["t"] = round(started, 6)
            payload["iso"] = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())
            _emit(payload)
        if args.ticks and tick >= int(args.ticks):
            return 0
        elapsed = time.monotonic() - started
        time.sleep(max(0.0, interval_s - elapsed))


if __name__ == "__main__":
    raise SystemExit(main())
