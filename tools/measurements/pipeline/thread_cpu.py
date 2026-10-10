#!/usr/bin/env python3
"""Sample the bench container's fetch child per thread, from the host.

S1 of the fetch-pipeline performance plan needs to know *which thread* burns the
core before it can ask which functions do. That question is not answerable from
``BALUFFO_PROFILE``: the per-source capture in ``profile_utils.run_profiled``
wraps worker threads (``pipeline_source_loop.py``) while every HTTP byte is
processed on the single asyncio loop thread created in ``transport.py``, and
cProfile only sees the thread it wraps. So this samples from outside instead:

* ``thread_cpu_sampler.py`` is copied into the container and run as one
  long-lived ``docker exec`` -- reading another process's ``/proc`` needs no hook
  inside the pipeline and cannot perturb what it measures.
* This host side streams that payload's JSON lines to
  ``<run-dir>/thread-cpu.ndjson`` and writes ``thread-cpu-summary.json``.

Usage:
    python tools/measurements/pipeline/thread_cpu.py \
        --container baluffo-perf-s1 --run-dir _out/perf-runs/s1-<token>

Run it *after* the container is up and *before* the pipeline POST: the fetch
child does not exist until the run starts, and the sampler retries looking for it
on every tick.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

REMOTE_PAYLOAD = "/tmp/baluffo-thread-cpu.py"
DEFAULT_MATCH = "jobs_fetcher"
DEFAULT_INTERVAL_S = 1.0
DEFAULT_PROGRESS_EVERY = 30


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--container", required=True, help="bench container name, already running")
    parser.add_argument(
        "--run-dir", required=True, help="artifact directory, e.g. _out/perf-runs/<token>"
    )
    parser.add_argument(
        "--match", default=DEFAULT_MATCH, help="substring of the target command line"
    )
    parser.add_argument(
        "--interval", type=float, default=DEFAULT_INTERVAL_S, help="seconds per tick"
    )
    parser.add_argument(
        "--ticks", type=int, default=0, help="stop after N ticks (0 = until the run ends)"
    )
    parser.add_argument(
        "--payload", default="", help="path to thread_cpu_sampler.py (default: sibling file)"
    )
    parser.add_argument("--progress-every", type=int, default=DEFAULT_PROGRESS_EVERY)
    return parser.parse_args(argv)


def _docker(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", *args],  # noqa: S607 - docker on PATH is the documented tool dependency
        check=check,
        capture_output=True,
        text=True,
    )


def container_running(container: str) -> bool:
    probe = _docker("inspect", "--format", "{{.State.Running}}", container, check=False)
    return probe.returncode == 0 and (probe.stdout or "").strip().lower() == "true"


def parse_ndjson(text: str) -> tuple[list[dict[str, Any]], int]:
    """Rows plus the count of lines that were not JSON objects.

    A malformed line is counted, never silently dropped: on a long capture the
    difference between "no threads to report" and "we could not read our own
    output" is the difference between a verdict and a fabrication.
    """
    rows: list[dict[str, Any]] = []
    malformed = 0
    for line in text.splitlines():
        candidate = line.strip()
        if not candidate:
            continue
        try:
            value = json.loads(candidate)
        except json.JSONDecodeError:
            malformed += 1
            continue
        if isinstance(value, dict):
            rows.append(value)
        else:
            malformed += 1
    return rows, malformed


def _monotonic_delta(values: list[float]) -> float:
    """Sum of forward steps of a monotonic counter series.

    A backwards step is a restarted process or a reused pid, and contributes
    nothing rather than a negative. The counter's rise from zero to its first
    reading after such a restart is therefore lost -- an undercount, which is the
    safe direction for a cost figure.
    """
    total = 0.0
    previous: float | None = None
    for value in values:
        if previous is not None and value >= previous:
            total += value - previous
        previous = value
    return total


def summarise(rows: list[dict[str, Any]], *, malformed: int = 0) -> dict[str, Any]:
    """Turn absolute per-tick counters into per-process and per-thread CPU totals."""
    ticks: dict[int, set[int]] = {}
    process_series: dict[int, list[tuple[float, float]]] = {}
    process_meta: dict[int, dict[str, Any]] = {}
    thread_series: dict[tuple[int, int], list[tuple[float, float]]] = {}
    thread_meta: dict[tuple[int, int], dict[str, Any]] = {}
    tick_times: dict[int, float] = {}

    for row in rows:
        tick = int(row.get("tick") or 0)
        at = float(row.get("t") or 0.0)
        tick_times[tick] = at
        pid = row.get("pid")
        if not isinstance(pid, int) or row.get("gone"):
            continue
        ticks.setdefault(pid, set()).add(tick)
        meta = process_meta.setdefault(
            pid, {"pid": pid, "cmdline": str(row.get("cmdline") or "")[:300], "ticks": 0}
        )
        meta["ticks"] = len(ticks[pid])
        process_series.setdefault(pid, []).append((at, float(row.get("processCpuSeconds") or 0.0)))
        for thread in row.get("threads") or []:
            if not isinstance(thread, dict) or not isinstance(thread.get("tid"), int):
                continue
            key = (pid, int(thread["tid"]))
            thread_series.setdefault(key, []).append((at, float(thread.get("cpuSeconds") or 0.0)))
            entry = thread_meta.setdefault(
                key,
                {"pid": pid, "tid": key[1], "comm": "", "ticks": 0, "states": {}},
            )
            entry["ticks"] += 1
            if thread.get("comm"):
                entry["comm"] = str(thread["comm"])
            state = str(thread.get("state") or "")
            if state:
                entry["states"][state] = int(entry["states"].get(state, 0)) + 1

    processes = []
    for pid, series in process_series.items():
        series.sort(key=lambda item: item[0])
        row = dict(process_meta[pid])
        row["cpuSeconds"] = round(_monotonic_delta([value for _, value in series]), 6)
        row["firstSeenSeconds"] = series[0][0]
        row["lastSeenSeconds"] = series[-1][0]
        processes.append(row)
    processes.sort(key=lambda item: -float(item["cpuSeconds"]))

    threads = []
    for key, series in thread_series.items():
        series.sort(key=lambda item: item[0])
        row = dict(thread_meta[key])
        row["cpuSeconds"] = round(_monotonic_delta([value for _, value in series]), 6)
        threads.append(row)
    threads.sort(key=lambda item: -float(item["cpuSeconds"]))

    ordered_ticks = sorted(tick_times)
    gaps = [
        tick_times[right] - tick_times[left]
        for left, right in zip(ordered_ticks, ordered_ticks[1:], strict=False)
    ]
    span = (
        (tick_times[ordered_ticks[-1]] - tick_times[ordered_ticks[0]])
        if len(ordered_ticks) > 1
        else 0.0
    )
    process_total = round(sum(float(item["cpuSeconds"]) for item in processes), 6)
    thread_total = round(sum(float(item["cpuSeconds"]) for item in threads), 6)
    return {
        "malformedLines": malformed,
        "tickCount": len(ordered_ticks),
        "spanSeconds": round(span, 3),
        "maxGapSeconds": round(max(gaps), 3) if gaps else 0.0,
        "tickRateHz": round(len(ordered_ticks) / span, 3) if span > 0 else 0.0,
        "processes": processes,
        "threads": threads,
        "reconciliation": {
            "processCpuSeconds": process_total,
            "summedThreadCpuSeconds": thread_total,
            "deltaSeconds": round(thread_total - process_total, 6),
            "deltaPct": round((thread_total - process_total) / process_total * 100.0, 3)
            if process_total
            else 0.0,
        },
    }


def collect(
    container: str,
    payload: Path,
    *,
    match: str,
    interval: float,
    ticks: int,
    out_path: Path,
    progress_every: int,
) -> tuple[list[dict[str, Any]], int]:
    """Stream the payload's JSON lines into ``out_path``, writing as they arrive."""
    _docker("cp", str(payload), f"{container}:{REMOTE_PAYLOAD}")
    command = [
        "docker",
        "exec",
        container,
        "python",
        REMOTE_PAYLOAD,
        "--match",
        match,
        "--interval",
        str(interval),
    ]
    if ticks:
        command += ["--ticks", str(ticks)]
    # ponytail: keep the stream on disk as it arrives -- a capture that only
    # lands at exit is exactly what a killed tool destroys.
    process = subprocess.Popen(command, stdout=subprocess.PIPE, text=True, bufsize=1)  # noqa: S603
    rows: list[dict[str, Any]] = []
    malformed = 0
    out_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with out_path.open("w", encoding="utf-8") as handle:
            stream = process.stdout
            if stream is not None:
                for line in stream:
                    candidate = line.strip()
                    if not candidate:
                        continue
                    handle.write(candidate + "\n")
                    handle.flush()
                    try:
                        value = json.loads(candidate)
                    except json.JSONDecodeError:
                        malformed += 1
                        continue
                    if not isinstance(value, dict):
                        malformed += 1
                        continue
                    rows.append(value)
                    if progress_every and len(rows) % progress_every == 0:
                        live = {int(row["pid"]) for row in rows[-progress_every:] if row.get("pid")}
                        cpu = max(
                            (
                                float(row.get("processCpuSeconds") or 0.0)
                                for row in rows[-progress_every:]
                            ),
                            default=0.0,
                        )
                        print(
                            f"[thread-cpu] ticks={len(rows)} pids={sorted(live)} cpuSeconds={cpu:.1f}",
                            flush=True,
                        )
    except KeyboardInterrupt:
        print("[thread-cpu] interrupted; writing what we have", flush=True)
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
    return rows, malformed


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    run_dir = Path(str(args.run_dir)).expanduser().resolve()
    run_dir.mkdir(parents=True, exist_ok=True)
    container = str(args.container)
    if not container_running(container):
        print(f"[thread-cpu] container {container} is not running", file=sys.stderr)
        return 2
    payload = (
        Path(args.payload).expanduser()
        if str(args.payload).strip()
        else Path(__file__).with_name("thread_cpu_sampler.py")
    )
    if not payload.is_file():
        print(f"[thread-cpu] payload not found: {payload}", file=sys.stderr)
        return 2

    out_path = run_dir / "thread-cpu.ndjson"
    rows, malformed = collect(
        container,
        payload,
        match=str(args.match),
        interval=float(args.interval),
        ticks=int(args.ticks),
        out_path=out_path,
        progress_every=int(args.progress_every),
    )
    summary = summarise(rows, malformed=malformed)
    summary_path = run_dir / "thread-cpu-summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    print(
        f"[thread-cpu] rows={len(summary['processes'])} processes, {len(summary['threads'])} threads"
    )
    print(
        f"[thread-cpu] ticks={summary['tickCount']} span={summary['spanSeconds']}s "
        f"maxGap={summary['maxGapSeconds']}s rate={summary['tickRateHz']}Hz"
    )
    print(
        f"[thread-cpu] child CPU={summary['reconciliation']['processCpuSeconds']}s "
        f"vs summed threads={summary['reconciliation']['summedThreadCpuSeconds']}s "
        f"({summary['reconciliation']['deltaPct']}%)"
    )
    for thread in summary["threads"][:8]:
        print(f"    tid={thread['tid']:<8} comm={thread['comm']:<10} cpu={thread['cpuSeconds']}s")
    print(f"[thread-cpu] wrote {out_path} and {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
