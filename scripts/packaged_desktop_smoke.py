#!/usr/bin/env python3
"""Release-gating smoke runner for the packaged Baluffo desktop executable."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_EXE_PATH = ROOT / "dist" / "baluffo-portable" / "Baluffo.exe"
DEFAULT_REPORT_PATH = ROOT / "data" / "packaged-desktop-smoke-report.json"
DEFAULT_ARTIFACT_ROOT = ROOT / ".codex-tmp" / "packaged-desktop-smoke"
DEFAULT_RUNTIME_TIMEOUT_S = 35.0
DEFAULT_PLAYWRIGHT_TIMEOUT_S = 180.0
STARTUP_REQUIRED_EVENTS = (
    "desktop_launch_start",
    "desktop_site_ready",
    "desktop_window_created",
    "desktop_window_load_url",
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def slugify_token(value: str) -> str:
    lowered = "".join(ch.lower() if ch.isalnum() else "-" for ch in str(value or ""))
    compact = "-".join(part for part in lowered.split("-") if part)
    return compact or "scenario"


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def fetch_json(url: str, timeout_s: float = 2.5) -> Dict[str, Any]:
    with urllib.request.urlopen(url, timeout=timeout_s) as response:
        body = response.read().decode("utf-8", errors="replace")
    parsed = json.loads(body or "{}")
    return parsed if isinstance(parsed, dict) else {}


def fetch_text(url: str, timeout_s: float = 2.5) -> str:
    with urllib.request.urlopen(url, timeout=timeout_s) as response:
        return response.read().decode("utf-8", errors="replace")


def choose_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as handle:
        handle.bind(("127.0.0.1", 0))
        return int(handle.getsockname()[1])


def run_portable_build() -> None:
    if shutil.which("py"):
        command = ["py", "-3.13", str(ROOT / "scripts" / "build_portable_exe.py")]
    else:
        command = [sys.executable, str(ROOT / "scripts" / "build_portable_exe.py")]
    subprocess.run(command, cwd=ROOT, check=True)


def resolve_playwright_command() -> List[str]:
    local_cmd = ROOT / "node_modules" / ".bin" / ("playwright.cmd" if os.name == "nt" else "playwright")
    if local_cmd.exists():
        return [str(local_cmd)]
    fallback = shutil.which("npx.cmd") or shutil.which("npx") or "npx"
    return [fallback, "playwright"]


def ensure_portable_exe(exe_path: Path, rebuild: bool = False) -> Path:
    exe = Path(exe_path).expanduser().resolve()
    if rebuild or not exe.exists():
        run_portable_build()
    if not exe.exists():
        raise RuntimeError(f"Packaged desktop executable not found: {exe}")
    return exe


def launch_packaged_exe(
    exe_path: Path,
    *,
    site_port: int,
    bridge_port: int,
    data_dir: Path,
    stdout_path: Path,
    stderr_path: Path,
) -> tuple[subprocess.Popen[Any], Any, Any]:
    stdout_path.parent.mkdir(parents=True, exist_ok=True)
    stderr_path.parent.mkdir(parents=True, exist_ok=True)
    stdout_handle = stdout_path.open("wb")
    stderr_handle = stderr_path.open("wb")
    command = [
        str(exe_path),
        "--site-port",
        str(int(site_port)),
        "--bridge-port",
        str(int(bridge_port)),
        "--data-dir",
        str(data_dir),
    ]
    process = subprocess.Popen(
        command,
        cwd=exe_path.parent,
        stdout=stdout_handle,
        stderr=stderr_handle,
    )
    return process, stdout_handle, stderr_handle


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
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def wait_for_packaged_runtime(
    process: subprocess.Popen[Any],
    *,
    site_base_url: str,
    bridge_base_url: str,
    timeout_s: float,
) -> Dict[str, Any]:
    deadline = time.monotonic() + max(1.0, float(timeout_s))
    last_error = ""
    while time.monotonic() < deadline:
        exit_code = process.poll()
        if exit_code is not None:
            raise RuntimeError(f"Packaged desktop executable exited before smoke runtime became ready (exit {exit_code}).")
        try:
            health = fetch_json(f"{bridge_base_url}/ops/health")
            session = fetch_json(f"{bridge_base_url}/desktop-local-data/session")
            metrics_payload = fetch_json(f"{bridge_base_url}/desktop-local-data/startup-metrics?limit=200")
            metrics_rows = metrics_payload.get("rows") if isinstance(metrics_payload.get("rows"), list) else []
            events = {str(row.get("event") or "") for row in metrics_rows if isinstance(row, dict)}
            jobs_html = fetch_text(f"{site_base_url}/jobs.html?desktop=1", timeout_s=2.5)
            if all(event in events for event in STARTUP_REQUIRED_EVENTS) and "jobs-list" in jobs_html:
                return {
                    "health": health,
                    "session": session,
                    "startupMetrics": metrics_rows,
                }
        except (TimeoutError, urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError, ValueError) as exc:
            last_error = str(exc)
        time.sleep(0.35)
    raise TimeoutError(
        f"Packaged desktop runtime did not become ready within {timeout_s:.1f}s."
        + (f" Last error: {last_error}" if last_error else "")
    )


def capture_runtime_snapshot(bridge_base_url: str, artifacts_dir: Path) -> Dict[str, str]:
    snapshots = {
        "opsHealthSnapshot": artifacts_dir / "ops-health.json",
        "sessionSnapshot": artifacts_dir / "session.json",
        "startupMetricsSnapshot": artifacts_dir / "startup-metrics.json",
    }
    write_json(snapshots["opsHealthSnapshot"], fetch_json(f"{bridge_base_url}/ops/health"))
    write_json(snapshots["sessionSnapshot"], fetch_json(f"{bridge_base_url}/desktop-local-data/session"))
    metrics_payload = fetch_json(f"{bridge_base_url}/desktop-local-data/startup-metrics?limit=200")
    write_json(snapshots["startupMetricsSnapshot"], metrics_payload)
    return {key: str(path) for key, path in snapshots.items()}


def parse_playwright_report(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8") or "{}")
    rows: List[Dict[str, Any]] = []

    def append_spec(spec: Dict[str, Any]) -> None:
        tests = spec.get("tests") if isinstance(spec.get("tests"), list) else []
        result_statuses: List[str] = []
        durations: List[int] = []
        errors: List[str] = []
        for item in tests:
            if not isinstance(item, dict):
                continue
            results = item.get("results") if isinstance(item.get("results"), list) else []
            if results:
                for result in results:
                    if not isinstance(result, dict):
                        continue
                    status = str(result.get("status") or "").strip() or str(item.get("status") or "").strip()
                    if status:
                        result_statuses.append(status)
                    durations.append(int(result.get("duration") or 0))
                    error_obj = result.get("error")
                    if isinstance(error_obj, dict) and error_obj.get("message"):
                        errors.append(str(error_obj.get("message")))
            else:
                status = str(item.get("status") or "").strip()
                if status:
                    result_statuses.append(status)
        status = result_statuses[-1] if result_statuses else "unknown"
        title = str(spec.get("title") or "Playwright Scenario").strip() or "Playwright Scenario"
        rows.append(
            {
                "name": title,
                "slug": slugify_token(title),
                "status": status,
                "durationMs": sum(durations),
                "error": errors[0] if errors else "",
            }
        )

    def walk_suite(suite: Dict[str, Any]) -> None:
        for spec in suite.get("specs", []) if isinstance(suite.get("specs"), list) else []:
            if isinstance(spec, dict):
                append_spec(spec)
        for child in suite.get("suites", []) if isinstance(suite.get("suites"), list) else []:
            if isinstance(child, dict):
                walk_suite(child)

    for suite in payload.get("suites", []) if isinstance(payload.get("suites"), list) else []:
        if isinstance(suite, dict):
            walk_suite(suite)
    return rows


def run_playwright_packaged_smoke(
    *,
    site_base_url: str,
    bridge_base_url: str,
    artifacts_dir: Path,
    headed: bool,
    pause_on_failure: bool,
    timeout_s: float,
) -> Dict[str, Any]:
    output_dir = artifacts_dir / "playwright-output"
    report_path = artifacts_dir / "playwright-report.json"
    command = [
        *resolve_playwright_command(),
        "test",
        "tests/frontend/packaged-desktop.spec.js",
        "-c",
        "playwright.packaged.config.js",
    ]
    env = os.environ.copy()
    env["PACKAGED_DESKTOP_BASE_URL"] = site_base_url
    env["PACKAGED_DESKTOP_BRIDGE_BASE"] = bridge_base_url
    env["PACKAGED_SMOKE_ARTIFACTS_DIR"] = str(output_dir)
    env["PACKAGED_SMOKE_PLAYWRIGHT_REPORT"] = str(report_path)
    env["PACKAGED_SMOKE_HEADED"] = "1" if headed else "0"
    env["PACKAGED_SMOKE_PAUSE_ON_FAILURE"] = "1" if pause_on_failure else "0"
    completed = subprocess.run(
        command,
        cwd=ROOT,
        env=env,
        timeout=max(30.0, float(timeout_s)),
        check=False,
    )
    scenarios = parse_playwright_report(report_path)
    return {
        "exitCode": int(completed.returncode),
        "reportPath": str(report_path),
        "outputDir": str(output_dir),
        "scenarios": scenarios,
    }


def build_failure_payload(step: str, error: Exception | str) -> Dict[str, Any]:
    return {
        "step": str(step or "unknown"),
        "message": str(error),
    }


def run_packaged_smoke(args: argparse.Namespace) -> Dict[str, Any]:
    started_at = utc_now_iso()
    run_token = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    artifacts_dir = Path(args.artifacts_dir or (DEFAULT_ARTIFACT_ROOT / run_token)).expanduser().resolve()
    runtime_data_dir = artifacts_dir / "runtime-data"
    stdout_path = artifacts_dir / "desktop-exe.stdout.log"
    stderr_path = artifacts_dir / "desktop-exe.stderr.log"
    latest_report_path = Path(args.report_path or DEFAULT_REPORT_PATH).expanduser().resolve()
    report_path = artifacts_dir / "report.json"
    site_port = int(args.site_port or choose_free_port())
    bridge_port = int(args.bridge_port or choose_free_port())
    site_base_url = f"http://127.0.0.1:{site_port}"
    bridge_base_url = f"http://127.0.0.1:{bridge_port}"
    exe_path = ensure_portable_exe(Path(args.exe_path or DEFAULT_EXE_PATH), rebuild=bool(args.rebuild))

    artifacts_dir.mkdir(parents=True, exist_ok=True)
    runtime_data_dir.mkdir(parents=True, exist_ok=True)

    report: Dict[str, Any] = {
        "ok": False,
        "startedAt": started_at,
        "finishedAt": "",
        "exePath": str(exe_path),
        "dataDir": str(runtime_data_dir),
        "siteBaseUrl": site_base_url,
        "bridgeBaseUrl": bridge_base_url,
        "startupMetrics": [],
        "bridgeReady": False,
        "scenarios": [],
        "artifacts": {
            "artifactsDir": str(artifacts_dir),
            "reportPath": str(report_path),
            "exeStdout": str(stdout_path),
            "exeStderr": str(stderr_path),
        },
        "failure": None,
    }

    process: subprocess.Popen[Any] | None = None
    stdout_handle = None
    stderr_handle = None
    try:
        process, stdout_handle, stderr_handle = launch_packaged_exe(
            exe_path,
            site_port=site_port,
            bridge_port=bridge_port,
            data_dir=runtime_data_dir,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
        )
        runtime_state = wait_for_packaged_runtime(
            process,
            site_base_url=site_base_url,
            bridge_base_url=bridge_base_url,
            timeout_s=float(args.runtime_timeout or DEFAULT_RUNTIME_TIMEOUT_S),
        )
        report["bridgeReady"] = True
        report["startupMetrics"] = runtime_state.get("startupMetrics") or []
        report["artifacts"].update(capture_runtime_snapshot(bridge_base_url, artifacts_dir))

        playwright_result = run_playwright_packaged_smoke(
            site_base_url=site_base_url,
            bridge_base_url=bridge_base_url,
            artifacts_dir=artifacts_dir,
            headed=bool(args.headed),
            pause_on_failure=bool(args.pause_on_failure),
            timeout_s=float(args.playwright_timeout or DEFAULT_PLAYWRIGHT_TIMEOUT_S),
        )
        report["artifacts"]["playwrightReport"] = str(playwright_result["reportPath"])
        report["artifacts"]["playwrightOutputDir"] = str(playwright_result["outputDir"])
        report["scenarios"] = list(playwright_result.get("scenarios") or [])
        if int(playwright_result.get("exitCode", 1)) != 0:
            failed = next((row for row in report["scenarios"] if str(row.get("status")) != "passed"), None)
            report["failure"] = build_failure_payload(
                "playwright",
                failed.get("error") if isinstance(failed, dict) and failed.get("error") else "Packaged Playwright smoke failed.",
            )
        else:
            report["ok"] = True
        report["artifacts"].update(capture_runtime_snapshot(bridge_base_url, artifacts_dir))
    except Exception as exc:  # noqa: BLE001
        if not report["failure"]:
            report["failure"] = build_failure_payload("runner", exc)
    finally:
        terminate_process_tree(process)
        if stdout_handle is not None:
            stdout_handle.close()
        if stderr_handle is not None:
            stderr_handle.close()
        report["finishedAt"] = utc_now_iso()
        write_json(report_path, report)
        write_json(latest_report_path, report)
    return report


def parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run packaged desktop smoke validation against Baluffo.exe.")
    parser.add_argument("--exe-path", default=str(DEFAULT_EXE_PATH))
    parser.add_argument("--report-path", default=str(DEFAULT_REPORT_PATH))
    parser.add_argument("--artifacts-dir", default="")
    parser.add_argument("--site-port", type=int, default=0)
    parser.add_argument("--bridge-port", type=int, default=0)
    parser.add_argument("--runtime-timeout", type=float, default=DEFAULT_RUNTIME_TIMEOUT_S)
    parser.add_argument("--playwright-timeout", type=float, default=DEFAULT_PLAYWRIGHT_TIMEOUT_S)
    parser.add_argument("--rebuild", action="store_true")
    parser.add_argument("--headed", action="store_true")
    parser.add_argument("--pause-on-failure", action="store_true")
    return parser.parse_args(argv)


def main(argv: List[str] | None = None) -> int:
    args = parse_args(argv)
    report = run_packaged_smoke(args)
    return 0 if report.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
