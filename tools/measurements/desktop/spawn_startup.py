#!/usr/bin/env python3
"""Measure packaged desktop startup from process spawn, not from the first journal row.

``src/packaged_desktop_smoke.py --startup-probe`` reports ``Launch -> First Usable
UI`` from ``desktop_launch_start``, the first row the launcher writes to
``desktop-startup-metrics.jsonl``. That row lands only after the PyInstaller
bootloader, the interpreter start and the launcher's import preamble, so the
probe's total is not the wait a double-click produces.

This tool measures the missing part. It starts the portable EXE itself, takes the
spawn timestamp immediately before ``Popen``, and tails the same journal:

* launcher rows are placed at ``ts - spawn``;
* page rows are placed at their own page clock (``browserTsMs`` / ``pageTsMs``),
  because a page row's ``ts`` is the bridge *arrival* time and a queued metric
  POST can delay it by hundreds of milliseconds. The arrival-based value is kept
  beside it, so that delay is visible instead of silently charged to the page.

Two consequences are worth stating rather than smoothing over. The page clock is
a cross-process clock: at a few tens of milliseconds it can disagree with the
launcher's timestamps, and a page can report interactive before the launcher has
recorded the shell as shown (it detects the window after the browser is up). Read
the two together; neither is corrected to match the other.

Cold runs delete the runtime data directory first, warm runs reuse it. Every run
gets its own copy of the arguments the packaged smoke harness uses, so nothing is
written to ``data/``. The EXE is killed with its whole process tree, and any
process still listening on the two runtime ports is killed as a backstop.

Usage::

    python tools/measurements/desktop/spawn_startup.py
    python tools/measurements/desktop/spawn_startup.py --runs 3 --modes cold,warm
    python tools/measurements/desktop/spawn_startup.py --page admin --probe --json
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import statistics
import subprocess
import sys
import time
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.ship.startup_spawn_clock import PAGE_INTERACTIVE_EVENTS, build_spawn_clock  # noqa: E402

DEFAULT_EXE = REPO_ROOT / "dist" / "baluffo-portable" / "Baluffo.exe"
DEFAULT_OUTPUT_DIR = REPO_ROOT / ".tmp" / "spawn-startup"
JOURNAL_NAME = "desktop-startup-metrics.jsonl"
MODE_CHOICES = ("cold", "warm")
PAGE_CHOICES = ("jobs", "admin")

OPEN_PATHS = {"jobs": "jobs.html", "admin": "admin.html"}
# A launcher may inherit these from the caller's shell; each would point the
# child at a different data directory or startup mode than the one being measured.
_CHILD_ENV_DROP = (
    "BALUFFO_DATA_DIR",
    "BALUFFO_SHIP_ROOT",
    "BALUFFO_DESKTOP_MODE",
    "BALUFFO_STARTUP_PROBE",
)


def _now_epoch_ms() -> float:
    return datetime.now(UTC).timestamp() * 1000.0


def read_new_rows(path: Path, offset: int) -> tuple[list[dict[str, Any]], int]:
    """Rows appended past ``offset``; a torn or non-JSON line is skipped."""
    if not path.is_file():
        return [], offset
    with path.open("rb") as handle:
        handle.seek(offset)
        chunk = handle.read()
        new_offset = handle.tell()
    rows: list[dict[str, Any]] = []
    for line in chunk.decode("utf-8", "replace").splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            rows.append(row)
    return rows, new_offset


def summarize_runs(runs: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Median/min/max for the three marks the tool exists to report."""

    def stats(key: str) -> dict[str, Any]:
        values = [int(run[key]) for run in runs if isinstance(run.get(key), int)]
        return {
            "values": values,
            "median": int(round(statistics.median(values))) if values else None,
            "min": min(values) if values else None,
            "max": max(values) if values else None,
        }

    return {
        "runs": len(runs),
        "bootMs": stats("bootMs"),
        "windowShownMs": stats("windowShownMs"),
        "firstInteractiveMs": stats("firstInteractiveMs"),
    }


def render_markdown(report: Mapping[str, Any]) -> str:
    """Per-run table plus the medians, as the doc quotes them."""

    def cell(run: Mapping[str, Any], key: str, suffix: str = "") -> str:
        value = run.get(key)
        return f"{int(value)}{suffix}" if isinstance(value, int) else "n/a"

    lines = [
        "| run | mode | boot | window | first interactive | clock |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for run in report["runs"]:
        lines.append(
            "| {label} | {mode} | {boot} | {window} | {interactive} | {clock} |".format(
                label=run.get("label", ""),
                mode=run.get("mode", ""),
                boot=cell(run, "bootMs", " ms"),
                window=cell(run, "windowShownMs", " ms"),
                interactive=cell(run, "firstInteractiveMs", " ms"),
                clock=run.get("firstInteractiveClock") or "n/a",
            )
        )
    summary = report.get("summary") or {}
    medians = [
        (summary.get("bootMs") or {}).get("median"),
        (summary.get("windowShownMs") or {}).get("median"),
        (summary.get("firstInteractiveMs") or {}).get("median"),
    ]
    lines.append(
        "| **median** | {runs} runs | {boot} | {window} | {interactive} | |".format(
            runs=summary.get("runs", 0),
            boot=f"{medians[0]} ms" if medians[0] is not None else "n/a",
            window=f"{medians[1]} ms" if medians[1] is not None else "n/a",
            interactive=f"{medians[2]} ms" if medians[2] is not None else "n/a",
        )
    )
    return "\n".join(lines)


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _free_ports() -> tuple[int, int]:
    site = _free_port()
    bridge = _free_port()
    while bridge == site:
        bridge = _free_port()
    return site, bridge


def _listening_pids_windows(port: int) -> set[int]:
    try:
        completed = subprocess.run(
            ["netstat", "-ano", "-p", "tcp"],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except OSError:
        return set()
    pids: set[int] = set()
    for line in str(completed.stdout or "").splitlines():
        parts = line.split()
        if len(parts) < 5 or str(parts[0]).upper() != "TCP":
            continue
        if str(parts[3]).upper() != "LISTENING" or not str(parts[1]).endswith(f":{port}"):
            continue
        try:
            pid = int(parts[-1])
        except ValueError:
            continue
        if pid > 0:
            pids.add(pid)
    return pids


def _kill_port_listeners(*ports: int) -> None:
    """Backstop for a child that outlived its tree (Windows only)."""
    if os.name != "nt":
        return
    own = os.getpid()
    for port in ports:
        for pid in _listening_pids_windows(int(port)):
            if pid == own:
                continue
            subprocess.run(
                ["taskkill", "/PID", str(pid), "/T", "/F"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )


def _terminate_tree(process: subprocess.Popen[Any] | None) -> None:
    if process is None or process.poll() is not None:
        return
    if os.name == "nt":
        subprocess.run(
            ["taskkill", "/PID", str(process.pid), "/T", "/F"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
    else:
        process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def _expected_events(page: str) -> tuple[str, ...]:
    return ("desktop_shell_window_shown", *PAGE_INTERACTIVE_EVENTS[page])


def _run_once(
    *,
    exe: Path,
    page: str,
    mode: str,
    probe: bool,
    data_dir: Path,
    logs_dir: Path,
    label: str,
    timeout_s: float,
) -> dict[str, Any]:
    fresh = mode == "cold" or not data_dir.is_dir()
    if fresh:
        shutil.rmtree(data_dir, ignore_errors=True)
    data_dir.mkdir(parents=True, exist_ok=True)
    journal = data_dir / JOURNAL_NAME
    offset = journal.stat().st_size if journal.is_file() else 0
    site_port, bridge_port = _free_ports()
    command = [
        str(exe),
        "--site-port",
        str(site_port),
        "--bridge-port",
        str(bridge_port),
        "--data-dir",
        str(data_dir),
        "--open-path",
        OPEN_PATHS[page],
    ]
    if probe:
        command.append("--startup-probe")
    env = {key: value for key, value in os.environ.items() if key not in _CHILD_ENV_DROP}
    logs_dir.mkdir(parents=True, exist_ok=True)
    stdout_path = logs_dir / f"{label}.out.log"
    stderr_path = logs_dir / f"{label}.err.log"

    rows: list[dict[str, Any]] = []
    spawn_ms = _now_epoch_ms()
    with stdout_path.open("wb") as stdout_handle, stderr_path.open("wb") as stderr_handle:
        process = subprocess.Popen(
            command,
            cwd=exe.parent,
            stdout=stdout_handle,
            stderr=stderr_handle,
            env=env,
        )
        try:
            deadline = time.monotonic() + max(5.0, float(timeout_s))
            wanted = _expected_events(page)
            while time.monotonic() < deadline:
                new_rows, offset = read_new_rows(journal, offset)
                rows.extend(new_rows)
                seen = {str(row.get("event") or "") for row in rows}
                if all(event in seen for event in wanted):
                    break
                if process.poll() is not None:
                    # Drain what the children flushed before they exited.
                    time.sleep(0.3)
                    new_rows, offset = read_new_rows(journal, offset)
                    rows.extend(new_rows)
                    break
                time.sleep(0.03)
        finally:
            _terminate_tree(process)
            _kill_port_listeners(site_port, bridge_port)

    marks = build_spawn_clock(rows, spawn_epoch_ms=spawn_ms, page=page)
    record: dict[str, Any] = {
        "label": label,
        "mode": mode,
        "fresh": fresh,
        "probe": probe,
        "sitePort": site_port,
        "bridgePort": bridge_port,
        "journal": str(journal),
        "stdout": str(stdout_path),
        "stderr": str(stderr_path),
        **marks,
    }
    missing = [
        key for key in ("bootMs", "windowShownMs", "firstInteractiveMs") if record[key] is None
    ]
    record["ok"] = not missing
    record["missing"] = missing
    return record


def _report_paths(output_dir: Path) -> tuple[Path, Path]:
    return output_dir / "spawn-startup.json", output_dir / "spawn-startup.md"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--exe", default=str(DEFAULT_EXE), help="Portable Baluffo.exe path.")
    parser.add_argument("--page", choices=PAGE_CHOICES, default="jobs")
    parser.add_argument(
        "--modes",
        default="cold,warm",
        help="Comma-separated subset of cold,warm. Asked in this order.",
    )
    parser.add_argument("--runs", type=int, default=2, help="Launches per mode.")
    parser.add_argument(
        "--probe",
        action="store_true",
        help="Pass --startup-probe to the EXE (the smoke harness's page mode).",
    )
    parser.add_argument("--timeout-s", type=float, default=60.0)
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Report, logs and runtime data live here; never data/.",
    )
    parser.add_argument("--data-dir", default="", help="Override the runtime data directory.")
    parser.add_argument("--json", action="store_true", help="Print the report JSON, not the table.")
    args = parser.parse_args(argv)

    exe = Path(args.exe).resolve()
    if not exe.is_file():
        print(
            f"spawn startup: no portable executable at {exe}; run npm run build:portable-exe",
            file=sys.stderr,
        )
        return 2
    modes = [mode.strip() for mode in str(args.modes).split(",") if mode.strip()]
    invalid = [mode for mode in modes if mode not in MODE_CHOICES]
    if not modes or invalid:
        print(f"spawn startup: --modes must be a subset of {MODE_CHOICES}", file=sys.stderr)
        return 2
    output_dir = Path(args.output_dir).resolve()
    data_dir = Path(args.data_dir).resolve() if args.data_dir else output_dir / "runtime-data"
    runs_per_mode = max(1, int(args.runs))

    runs: list[dict[str, Any]] = []
    for mode in modes:
        for index in range(1, runs_per_mode + 1):
            label = f"{mode}-{index}" + ("-probe" if args.probe else "")
            print(f"spawn startup: running {label} ({args.page})...", file=sys.stderr)
            runs.append(
                _run_once(
                    exe=exe,
                    page=args.page,
                    mode=mode,
                    probe=bool(args.probe),
                    data_dir=data_dir,
                    logs_dir=output_dir / "logs",
                    label=label,
                    timeout_s=float(args.timeout_s),
                )
            )

    report = {
        "schemaVersion": 1,
        "generatedAt": datetime.now(UTC).isoformat(),
        "exe": str(exe),
        "page": args.page,
        "probe": bool(args.probe),
        "dataDir": str(data_dir),
        "runs": runs,
        "summary": summarize_runs(runs),
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path, markdown_path = _report_paths(output_dir)
    json_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8", newline="\n")
    markdown = render_markdown(report)
    markdown_path.write_text(markdown + "\n", encoding="utf-8", newline="\n")

    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print(markdown)
        print(f"\nreport: {json_path}\nmarkdown: {markdown_path}")
    failures = [run["label"] for run in runs if not run["ok"]]
    if failures:
        print(f"spawn startup: incomplete runs: {', '.join(failures)}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
