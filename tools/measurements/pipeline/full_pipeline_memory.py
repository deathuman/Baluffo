#!/usr/bin/env python3
"""Run a real pipeline while sampling cgroup and process memory.

The target container must already be running with ``run_dir`` mounted at the
container output directory. The sampler is copied into the container as a
development-only file; no application image or runtime dependency changes.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

SAMPLER_NAME = "baluffo-memory-profile.py"


def _http_json(
    base_url: str,
    method: str,
    path: str,
    *,
    body: dict[str, Any] | None = None,
    timeout_s: float = 15.0,
) -> dict[str, Any]:
    payload = None if body is None else json.dumps(body).encode("utf-8")
    request = Request(
        f"{base_url.rstrip('/')}{path}",
        data=payload,
        method=method,
        headers={"Content-Type": "application/json"} if payload is not None else {},
    )
    try:
        with urlopen(request, timeout=timeout_s) as response:  # noqa: S310 - local benchmark URL
            value = json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
        return {"ok": False, "error": repr(exc)}
    return value if isinstance(value, dict) else {"ok": False, "error": "non-object response"}


def _trigger_accepted(payload: dict[str, Any]) -> bool:
    return bool(payload.get("started") or payload.get("ok"))


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _docker(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", *args],
        check=check,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )


def _container_state(container: str) -> dict[str, Any]:
    result = _docker("inspect", container, check=False)
    if result.returncode != 0:
        return {"available": False, "error": result.stderr.strip()}
    try:
        values = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        return {"available": False, "error": repr(exc)}
    if not isinstance(values, list) or not values:
        return {"available": False, "error": "empty inspect response"}
    state = values[0].get("State") if isinstance(values[0], dict) else None
    return state if isinstance(state, dict) else {"available": False}


def _start_sampler(
    *,
    container: str,
    module_path: Path,
    output_dir: str,
    interval_s: float,
    stderr_path: Path,
) -> subprocess.Popen[str]:
    remote_module = f"/tmp/{SAMPLER_NAME}"
    _docker("cp", str(module_path), f"{container}:{remote_module}")
    command = [
        "docker",
        "exec",
        container,
        "python",
        remote_module,
        "--output",
        f"{output_dir}/cgroup-process.ndjson",
        "--cgroup-root",
        "/sys/fs/cgroup",
        "--proc-root",
        "/proc",
        "--interval",
        str(interval_s),
        "--phase-file",
        f"{output_dir}/phase.json",
        "--stop-file",
        f"{output_dir}/sampler.stop",
    ]
    stderr_path.parent.mkdir(parents=True, exist_ok=True)
    stderr_handle = stderr_path.open("a", encoding="utf-8")
    try:
        return subprocess.Popen(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=stderr_handle,
            text=True,
        )
    finally:
        stderr_handle.close()


def _stop_sampler(
    process: subprocess.Popen[str],
    *,
    run_dir: Path,
) -> None:
    stop_file = run_dir / "sampler.stop"
    stop_file.write_text("stop\n", encoding="utf-8")
    try:
        process.wait(timeout=15)
    except subprocess.TimeoutExpired:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()


def _progress_payload(status: dict[str, Any]) -> dict[str, Any]:
    progress_value = status.get("progress")
    progress: dict[str, Any] = progress_value if isinstance(progress_value, dict) else {}
    child = status.get("activeChildTaskType") or ""
    return {
        "sampledAt": datetime.now(UTC).isoformat(),
        "active": bool(status.get("active")),
        "runId": str(status.get("runId") or ""),
        "stage": str(status.get("stage") or ""),
        "phaseKey": str(progress.get("phaseKey") or ""),
        "phaseLabel": str(progress.get("phaseLabel") or ""),
        "activeChildTaskType": str(child),
        "finalOutputCount": int(status.get("finalOutputCount") or 0),
    }


def _sampler_summary(run_dir: Path) -> dict[str, Any]:
    path = run_dir / "cgroup-process.ndjson"
    if not path.is_file():
        return {"sampleCount": 0, "available": False}
    rows: list[dict[str, Any]] = []
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            value = json.loads(line)
            if isinstance(value, dict):
                rows.append(value)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return {"sampleCount": 0, "available": False}
    if not rows:
        return {"sampleCount": 0, "available": True}
    cgroup_rows: list[dict[str, Any]] = []
    for row in rows:
        cgroup_value = row.get("cgroup")
        if isinstance(cgroup_value, dict):
            cgroup_rows.append(cgroup_value)
    peaks = [
        int(row.get("memoryPeakBytes") or 0)
        for row in cgroup_rows
        if row.get("memoryPeakBytes") is not None
    ]
    last = rows[-1]
    return {
        "sampleCount": len(rows),
        "available": True,
        "peakMemoryBytes": max(peaks) if peaks else None,
        "lastSampleAt": str(last.get("sampledAt") or ""),
        "terminalCgroup": last.get("cgroup") if isinstance(last.get("cgroup"), dict) else {},
        "terminalProcesses": list(last.get("processes") or [])[:10],
    }


def run(args: argparse.Namespace) -> int:
    run_dir = Path(args.run_dir).expanduser().resolve()
    run_dir.mkdir(parents=True, exist_ok=True)
    module_path = Path(__file__).with_name("memory_profile.py").resolve()
    state_before = _container_state(args.container)
    if not state_before.get("Running"):
        raise RuntimeError(f"container is not running: {args.container}")
    _write_json(run_dir / "phase.json", {})
    started_at = datetime.now(UTC)
    started = time.monotonic()
    sampler_stderr_path = run_dir / "sampler.stderr.log"
    sampler = _start_sampler(
        container=args.container,
        module_path=module_path,
        output_dir=args.container_output_dir,
        interval_s=args.interval_s,
        stderr_path=sampler_stderr_path,
    )
    sampler_restarted = False
    run_id = ""
    status: dict[str, Any] = {}
    error = ""
    progress_path = run_dir / "progress.ndjson"
    try:
        with progress_path.open("a", encoding="utf-8") as progress_handle:
            trigger = _http_json(
                args.base_url,
                "POST",
                "/tasks/run-jobs-pipeline",
                body={"jobsPageLoadedCount": 0},
            )
            if not _trigger_accepted(trigger):
                raise RuntimeError(f"pipeline trigger failed: {trigger}")
            run_id = str(trigger.get("runId") or "")
            while True:
                status = _http_json(
                    args.base_url,
                    "GET",
                    "/tasks/run-jobs-pipeline-status",
                )
                progress = _progress_payload(status)
                progress["runId"] = run_id or progress["runId"]
                _write_json(run_dir / "phase.json", progress)
                progress_handle.write(json.dumps(progress, sort_keys=True) + "\n")
                progress_handle.flush()
                if not progress["active"] and progress["runId"] == run_id:
                    break
                if sampler.poll() is not None:
                    if sampler_restarted:
                        raise RuntimeError("memory sampler exited twice before pipeline terminal")
                    sampler_restarted = True
                    sampler = _start_sampler(
                        container=args.container,
                        module_path=module_path,
                        output_dir=args.container_output_dir,
                        interval_s=args.interval_s,
                        stderr_path=sampler_stderr_path,
                    )
                if args.timeout_s > 0 and time.monotonic() - started >= args.timeout_s:
                    raise TimeoutError("pipeline memory profile timeout")
                if not _container_state(args.container).get("Running"):
                    raise RuntimeError("container stopped before pipeline terminal")
                time.sleep(max(1.0, float(args.interval_s)))
    except (HTTPError, URLError, RuntimeError, TimeoutError, OSError) as exc:
        error = repr(exc)
    finally:
        _stop_sampler(sampler, run_dir=run_dir)
    summary = {
        "runId": run_id,
        "baseUrl": args.base_url,
        "container": args.container,
        "containerStateBefore": state_before,
        "startedAt": started_at.isoformat(),
        "elapsedSeconds": round(time.monotonic() - started, 3),
        "terminalStatus": status,
        "error": error,
        "samplerModule": str(module_path),
        "containerOutputDir": args.container_output_dir,
        "sampler": _sampler_summary(run_dir),
        "samplerRestarted": sampler_restarted,
        "samplerStderr": str(sampler_stderr_path),
    }
    _write_json(run_dir / "meta.json", summary)
    if error:
        print(json.dumps(summary, indent=2, sort_keys=True))
        return 1
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--container", required=True)
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--container-output-dir", default="/out")
    parser.add_argument("--interval-s", type=float, default=1.0)
    parser.add_argument("--timeout-s", type=float, default=0.0)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    return run(parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
