"""Shared packaged smoke helpers behind the root facade."""

from __future__ import annotations

import json
import shutil
import sys
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def console_safe(value: Any) -> str:
    encoding = getattr(sys.stdout, "encoding", None) or "utf-8"
    return str(value).encode(encoding, errors="replace").decode(encoding, errors="replace")


def print_console(value: Any) -> None:
    print(console_safe(value))


def print_failure_summary(report: dict[str, Any]) -> None:
    """Print a summary of the failure to stdout for CI visibility."""
    failure = report.get("failure")
    if not failure:
        return
    step = failure.get("step", "unknown")
    message = failure.get("message", "No error message available")
    category = failure.get("category", "")
    print_console(f"\n[SMOKE FAILURE] Step: {step}")
    print_console(f"[SMOKE FAILURE] Error: {message}")
    if category:
        print_console(f"[SMOKE FAILURE] Category: {category}")
    artifacts = report.get("artifacts", {})
    exe_stdout = artifacts.get("exeStdout")
    exe_stderr = artifacts.get("exeStderr")
    report_path = artifacts.get("reportPath")
    if exe_stdout:
        print_console(f"[SMOKE FAILURE] Exe stdout log: {exe_stdout}")
    if exe_stderr:
        print_console(f"[SMOKE FAILURE] Exe stderr log: {exe_stderr}")
    if report_path:
        print_console(f"[SMOKE FAILURE] Full report: {report_path}")
    scenarios = report.get("scenarios", [])
    if scenarios:
        print_console("[SMOKE FAILURE] Scenarios summary:")
        for scenario in scenarios:
            name = scenario.get("name", "unknown")
            status = scenario.get("status", "unknown")
            status_char = "." if status == "passed" else "X"
            print_console(f"  [{status_char}] {name}: {status}")
            if status != "passed" and scenario.get("error"):
                print_console(f"      Error: {scenario['error']}")


def slugify_token(value: str) -> str:
    lowered = "".join(ch.lower() if ch.isalnum() else "-" for ch in str(value or ""))
    compact = "-".join(part for part in lowered.split("-") if part)
    return compact or "scenario"


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def remove_tree_or_file(path: Path) -> bool:
    candidate = Path(path).expanduser().resolve()
    if not candidate.exists():
        return False
    try:
        if candidate.is_dir():
            shutil.rmtree(candidate, ignore_errors=True)
        else:
            candidate.unlink()
    except OSError:
        return False
    return not candidate.exists()


def generate_packaged_smoke_run_token(
    *, now: datetime | None = None, entropy_ns: int | None = None
) -> str:
    resolved_now = now if isinstance(now, datetime) else datetime.now(UTC)
    resolved_entropy = int(entropy_ns if entropy_ns is not None else time.time_ns())
    return f"{resolved_now.strftime('%Y%m%d-%H%M%S-%f')}-{resolved_entropy % 1_000_000_000:09d}"


def fetch_json(url: str, timeout_s: float = 2.5) -> dict[str, Any]:
    with urllib.request.urlopen(url, timeout=timeout_s) as response:
        body = response.read().decode("utf-8", errors="replace")
    parsed = json.loads(body or "{}")
    return parsed if isinstance(parsed, dict) else {}


def fetch_text(url: str, timeout_s: float = 2.5) -> str:
    with urllib.request.urlopen(url, timeout=timeout_s) as response:
        return str(response.read().decode("utf-8", errors="replace"))


def request_json(
    url: str,
    *,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
    timeout_s: float = 10.0,
) -> tuple[int, dict[str, Any]]:
    body = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(
        str(url),
        data=body,
        headers=headers,
        method=str(method or "GET").upper(),
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_s) as response:
            raw = response.read().decode("utf-8", errors="replace")
            parsed = json.loads(raw or "{}")
            return int(getattr(response, "status", 200) or 200), (
                parsed if isinstance(parsed, dict) else {}
            )
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        try:
            parsed = json.loads(raw or "{}")
        except json.JSONDecodeError:
            parsed = {"error": raw or str(exc)}
        return int(getattr(exc, "code", 500) or 500), (parsed if isinstance(parsed, dict) else {})


def post_json(
    url: str,
    payload: dict[str, Any] | None = None,
    *,
    timeout_s: float = 10.0,
) -> tuple[int, dict[str, Any]]:
    return request_json(url, method="POST", payload=payload or {}, timeout_s=timeout_s)
