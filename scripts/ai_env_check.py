#!/usr/bin/env python3
"""Compact environment preflight for AI coders working on Baluffo."""

from __future__ import annotations

import argparse
import json
import platform
import re
import shutil
import subprocess
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REQUIRED_NODE = "25.8.0"
REQUIRED_PYTHON = (3, 13)
SERENA_PACKAGE = "serena-agent"


@dataclass(frozen=True)
class Check:
    name: str
    status: str
    detail: str


def _capture(*args: str, timeout: int = 15) -> subprocess.CompletedProcess[str] | None:
    try:
        return subprocess.run(
            args,
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None


def _first_line(completed: subprocess.CompletedProcess[str] | None) -> str:
    if completed is None:
        return "command did not start or timed out"
    text = (completed.stdout or completed.stderr).strip()
    return text.splitlines()[0] if text else f"exit {completed.returncode}"


def _check_python() -> Check:
    completed = _capture(sys.executable, "scripts/check_python_version.py")
    status = "ok" if completed is not None and completed.returncode == 0 else "fail"
    return Check("python", status, _first_line(completed))


def _check_node() -> Check:
    node = shutil.which("node")
    if node is None:
        return Check("node", "fail", "node not found")
    completed = _capture(node, "--version")
    version = _first_line(completed).lstrip("v")
    status = "ok" if version == REQUIRED_NODE else "warn"
    return Check("node", status, f"{version} (expected {REQUIRED_NODE})")


def _check_npm() -> Check:
    npm = shutil.which("npm.cmd") or shutil.which("npm")
    if npm is None:
        return Check("npm", "fail", "npm not found")
    completed = _capture(npm, "--version")
    status = "ok" if completed is not None and completed.returncode == 0 else "fail"
    return Check("npm", status, _first_line(completed))


def _version_tuple(version: str) -> tuple[int, ...]:
    return tuple(int(part) for part in re.findall(r"\d+", version)[:4])


def _extract_version(text: str) -> str | None:
    match = re.search(r"\d+(?:\.\d+)+", text)
    return match.group(0) if match else None


def _latest_pypi_version(package: str) -> str | None:
    url = f"https://pypi.org/pypi/{package}/json"
    try:
        with urllib.request.urlopen(url, timeout=8) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (OSError, TimeoutError, urllib.error.URLError, json.JSONDecodeError):
        return None
    version = payload.get("info", {}).get("version")
    return version if isinstance(version, str) and version else None


def _check_serena(check_updates: bool) -> Check:
    serena = shutil.which("serena.exe") or shutil.which("serena")
    if serena is None:
        return Check(
            "serena",
            "fail",
            "serena not found; install with uv tool install -p 3.13 serena-agent@latest --prerelease=allow",
        )
    completed = _capture(serena, "--version")
    if completed is None or completed.returncode != 0:
        return Check("serena", "fail", _first_line(completed))

    first_line = _first_line(completed)
    current = _extract_version(first_line)
    if not current:
        return Check("serena", "ok", first_line)
    if not check_updates:
        return Check("serena", "ok", f"{current}; use --check-updates to compare PyPI")

    latest = _latest_pypi_version(SERENA_PACKAGE)
    if latest is None:
        return Check("serena", "warn", f"{current}; PyPI update check unavailable")
    if _version_tuple(current) < _version_tuple(latest):
        return Check(
            "serena",
            "warn",
            f"{current} installed; latest is {latest}; run uv tool upgrade serena-agent --prerelease=allow",
        )
    return Check("serena", "ok", f"{current} current (latest {latest})")


def _check_lockfiles() -> Check:
    missing = [
        path
        for path in ("package-lock.json", "requirements-lock.txt", "requirements.txt")
        if not (ROOT / path).exists()
    ]
    if missing:
        return Check("lockfiles", "fail", f"missing: {', '.join(missing)}")
    return Check("lockfiles", "ok", "package-lock.json and Python requirement locks present")


def _check_node_modules() -> Check:
    if (ROOT / "node_modules").is_dir():
        return Check("node_modules", "ok", "node_modules present")
    return Check("node_modules", "warn", "node_modules missing; run npm ci")


def _check_python_env() -> Check:
    expected = ROOT / ".venv"
    if expected.exists():
        return Check("python_env", "ok", ".venv present")
    return Check("python_env", "warn", ".venv missing; create/sync local Python env")


def _check_git_hooks() -> Check:
    pre_commit = ROOT / ".git" / "hooks" / "pre-commit"
    pre_push = ROOT / ".git" / "hooks" / "pre-push"
    missing = [path.name for path in (pre_commit, pre_push) if not path.exists()]
    if missing:
        return Check("git_hooks", "warn", f"missing hooks: {', '.join(missing)}")
    return Check("git_hooks", "ok", "pre-commit and pre-push hooks present")


def _check_toolbelt(smoke: bool) -> Check:
    args = [sys.executable, "scripts/toolbelt_check.py", "--json"]
    if smoke:
        args.append("--smoke")
    completed = _capture(*args)
    if completed is None or completed.returncode != 0:
        return Check("toolbelt", "fail", _first_line(completed))
    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError:
        return Check("toolbelt", "fail", "toolbelt status was not valid JSON")

    missing = payload.get("missing") or []
    smoke_rows = payload.get("smoke") or []
    smoke_failures = [row.get("tool") for row in smoke_rows if row.get("status") != "ok"]
    if missing:
        return Check("toolbelt", "warn", f"missing: {', '.join(missing)}")
    if smoke_failures:
        return Check("toolbelt", "warn", f"smoke failures: {', '.join(smoke_failures)}")
    detail = "all default tools available"
    if smoke:
        detail += " and smoke-tested"
    return Check("toolbelt", "ok", detail)


def _check_playwright() -> Check:
    npx = shutil.which("npx.cmd") or shutil.which("npx")
    if npx is None:
        return Check("playwright", "warn", "npx not found")
    completed = _capture(npx, "playwright", "--version")
    status = "ok" if completed is not None and completed.returncode == 0 else "warn"
    return Check("playwright", status, _first_line(completed))


def _check_path_location() -> Check:
    system = platform.system()
    root = str(ROOT)
    if system == "Linux" and root.startswith("/mnt/"):
        return Check("repo_path", "warn", "repo is under /mnt; WSL native filesystem is faster")
    return Check("repo_path", "ok", root)


def _checks(*, smoke: bool, check_updates: bool) -> list[Check]:
    return [
        _check_python(),
        _check_node(),
        _check_npm(),
        _check_serena(check_updates),
        _check_lockfiles(),
        _check_node_modules(),
        _check_python_env(),
        _check_git_hooks(),
        _check_toolbelt(smoke),
        _check_playwright(),
        _check_path_location(),
    ]


def _print(checks: list[Check]) -> None:
    width = max(len(check.name) for check in checks)
    print("AI Environment Status")
    print("-" * 72)
    for check in checks:
        marker = "OK" if check.status == "ok" else check.status.upper()
        print(f"{marker:<5} {check.name:<{width}}  {check.detail}")
    print("-" * 72)
    if any(check.status == "fail" for check in checks):
        print("Failures need attention before broad validation.")
    elif any(check.status == "warn" for check in checks):
        print("Warnings are usually local setup gaps; narrow repo work may still proceed.")
    else:
        print("Environment looks ready for AI coding.")


def main() -> int:
    parser = argparse.ArgumentParser(description="Check local AI-coder environment readiness.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable results.")
    parser.add_argument("--smoke", action="store_true", help="Run toolbelt smoke checks too.")
    parser.add_argument(
        "--check-updates", action="store_true", help="Check PyPI for current AI MCP tool versions."
    )
    args = parser.parse_args()

    checks = _checks(smoke=args.smoke, check_updates=args.check_updates)
    if args.json:
        print(json.dumps([check.__dict__ for check in checks], indent=2))
    else:
        _print(checks)
    return 1 if any(check.status == "fail" for check in checks) else 0


if __name__ == "__main__":
    raise SystemExit(main())
