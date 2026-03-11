#!/usr/bin/env python3
"""Probe rapid double-launch behavior for packaged Baluffo desktop executable."""

from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_EXE_PATH = ROOT / "dist" / "baluffo-portable" / "Baluffo.exe"
DEFAULT_ARTIFACT_DIR = ROOT / ".codex-tmp" / "double-launch-probe"
DEFAULT_TIMEOUT_S = 20.0


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def choose_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as handle:
        handle.bind(("127.0.0.1", 0))
        return int(handle.getsockname()[1])


def fetch_json(url: str, timeout_s: float = 2.0) -> dict[str, Any]:
    with urllib.request.urlopen(url, timeout=timeout_s) as response:  # noqa: S310
        payload = json.loads(response.read().decode("utf-8", errors="replace") or "{}")
    return payload if isinstance(payload, dict) else {}


def wait_for_bridge_ready(bridge_port: int, *, timeout_s: float = DEFAULT_TIMEOUT_S) -> dict[str, Any]:
    deadline = time.monotonic() + max(1.0, float(timeout_s))
    last_error = ""
    while time.monotonic() < deadline:
        try:
            payload = fetch_json(f"http://127.0.0.1:{int(bridge_port)}/ops/health", timeout_s=1.5)
            if str(payload.get("service") or "") == "baluffo-bridge":
                return payload
        except (OSError, ValueError, urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError) as exc:
            last_error = str(exc)
        time.sleep(0.25)
    raise TimeoutError(f"Bridge did not become ready on port {bridge_port}. {last_error}".strip())


def terminate_process_tree(process: subprocess.Popen[Any] | None) -> None:
    if process is None:
        return
    if process.poll() is not None:
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
        process.wait(timeout=8)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=3)


def launch_packaged_exe(
    exe_path: Path,
    *,
    site_port: int,
    bridge_port: int,
    data_dir: Path,
    local_app_data_root: Path,
    stdout_path: Path,
    stderr_path: Path,
) -> tuple[subprocess.Popen[Any], Any, Any]:
    command = [
        str(exe_path),
        "--site-port",
        str(int(site_port)),
        "--bridge-port",
        str(int(bridge_port)),
        "--data-dir",
        str(data_dir),
        "--open-path",
        "jobs.html",
    ]
    env = os.environ.copy()
    env["LOCALAPPDATA"] = str(local_app_data_root)
    stdout_handle = stdout_path.open("wb")
    stderr_handle = stderr_path.open("wb")
    process = subprocess.Popen(command, cwd=exe_path.parent, stdout=stdout_handle, stderr=stderr_handle, env=env)
    return process, stdout_handle, stderr_handle


def read_metrics(data_dir: Path) -> list[dict[str, Any]]:
    path = data_dir / "desktop-startup-metrics.jsonl"
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            rows.append(parsed)
    return rows


def count_event(rows: list[dict[str, Any]], name: str) -> int:
    return sum(1 for row in rows if str(row.get("event") or "") == name)


def run_probe(args: argparse.Namespace) -> int:
    exe_path = Path(args.exe_path).expanduser().resolve()
    if not exe_path.exists():
        raise RuntimeError(f"Packaged executable not found: {exe_path}")
    run_dir = Path(args.artifacts_dir).expanduser().resolve() / datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    runtime_data_dir = run_dir / "runtime-data"
    local_app_data_root = run_dir / "local-user-data"
    run_dir.mkdir(parents=True, exist_ok=True)
    runtime_data_dir.mkdir(parents=True, exist_ok=True)
    local_app_data_root.mkdir(parents=True, exist_ok=True)

    site_port = int(args.site_port or choose_free_port())
    bridge_port = int(args.bridge_port or choose_free_port())
    launch_gap_ms = max(10, int(args.gap_ms))

    first = second = None
    first_out = first_err = second_out = second_err = None
    report: dict[str, Any] = {
        "ok": False,
        "startedAt": utc_now_iso(),
        "finishedAt": "",
        "exePath": str(exe_path),
        "artifactsDir": str(run_dir),
        "runtimeDataDir": str(runtime_data_dir),
        "localAppDataRoot": str(local_app_data_root),
        "sitePort": site_port,
        "bridgePort": bridge_port,
        "launchGapMs": launch_gap_ms,
        "assertions": {},
        "failure": "",
    }

    try:
        first, first_out, first_err = launch_packaged_exe(
            exe_path,
            site_port=site_port,
            bridge_port=bridge_port,
            data_dir=runtime_data_dir,
            local_app_data_root=local_app_data_root,
            stdout_path=run_dir / "first.stdout.log",
            stderr_path=run_dir / "first.stderr.log",
        )
        time.sleep(float(launch_gap_ms) / 1000.0)
        second, second_out, second_err = launch_packaged_exe(
            exe_path,
            site_port=site_port,
            bridge_port=bridge_port,
            data_dir=runtime_data_dir,
            local_app_data_root=local_app_data_root,
            stdout_path=run_dir / "second.stdout.log",
            stderr_path=run_dir / "second.stderr.log",
        )

        wait_for_bridge_ready(bridge_port, timeout_s=float(args.timeout_s))
        second_exit = second.wait(timeout=max(3.0, float(args.timeout_s)))
        rows = read_metrics(runtime_data_dir)

        site_spawns = count_event(rows, "desktop_site_spawned")
        bridge_spawns = count_event(rows, "desktop_bridge_spawned")
        reused_events = count_event(rows, "desktop_session_reused")
        launch_errors = count_event(rows, "desktop_launch_error")

        report["assertions"] = {
            "siteSpawnedCount": site_spawns,
            "bridgeSpawnedCount": bridge_spawns,
            "sessionReusedCount": reused_events,
            "launchErrorCount": launch_errors,
            "firstStillAliveAfterSecondExit": bool(first.poll() is None),
            "secondExitCode": int(second_exit),
        }
        report["ok"] = (
            site_spawns == 1
            and bridge_spawns == 1
            and reused_events >= 1
            and launch_errors == 0
            and bool(first.poll() is None)
            and int(second_exit) == 0
        )
        if not report["ok"]:
            report["failure"] = "Double-launch hardening probe assertions failed."
    except Exception as exc:  # noqa: BLE001
        report["failure"] = str(exc)
    finally:
        terminate_process_tree(second)
        terminate_process_tree(first)
        if first_out is not None:
            first_out.close()
        if first_err is not None:
            first_err.close()
        if second_out is not None:
            second_out.close()
        if second_err is not None:
            second_err.close()
        report["finishedAt"] = utc_now_iso()
        (run_dir / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report.get("ok") else 1


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Probe rapid double-launch hardening of packaged Baluffo desktop executable.")
    parser.add_argument("--exe-path", default=str(DEFAULT_EXE_PATH))
    parser.add_argument("--artifacts-dir", default=str(DEFAULT_ARTIFACT_DIR))
    parser.add_argument("--site-port", type=int, default=0)
    parser.add_argument("--bridge-port", type=int, default=0)
    parser.add_argument("--gap-ms", type=int, default=100)
    parser.add_argument("--timeout-s", type=float, default=DEFAULT_TIMEOUT_S)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    return run_probe(parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
