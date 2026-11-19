#!/usr/bin/env python3
"""Compact environment preflight for AI coders working on Baluffo."""

from __future__ import annotations

import argparse
import json
import os
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
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.python_version_guard import MIN_VERSION, is_required_python

REQUIRED_NODE = "25.8.0"
SERENA_PACKAGE = "serena-agent"
SERENA_CLIENT_CONFIG = "opencode.json"
BASIC_MEMORY_PACKAGE = "basic-memory"
BASIC_MEMORY_REFRESH = f"uv tool install -p 3.13 {BASIC_MEMORY_PACKAGE}@latest --force"
# uvx options that consume the following argument, so the probe parser can tell a
# flag's value from the tool name uvx is about to run.
UVX_VALUE_FLAGS = frozenset({"-p", "--python", "--from", "--with", "--index", "--python-version"})
MEMORY_VAULT_DIR_NAME = "BaluffoMemory"
MEMORY_VAULT_ENV_VAR = "BALUFFO_MEMORY_VAULT"


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


def _uvx_tool_prefix(command: list[str]) -> list[str] | None:
    """Trim ``uvx [flags] <tool> <subcommand...>`` down to the prefix that runs ``<tool>``.

    Returns None when the command is not uvx-shaped or carries no bare tool name.
    """
    skip_value = False
    for index, token in enumerate(command[1:], start=1):
        if skip_value:
            skip_value = False
            continue
        if token in UVX_VALUE_FLAGS:
            skip_value = True
            continue
        if not token.startswith("-"):
            return command[: index + 1]
    return None


def _serena_uvx_probe() -> list[str] | None:
    """Build a ``serena --version`` probe from the client registration in opencode.json.

    Both first-class clients register Serena through ``uvx`` (tools/mcp/SERENA.md), which
    resolves the tool per launch and never puts a ``serena`` executable on PATH. A
    PATH-only lookup therefore reports a Serena whose MCP is connected and serving tool
    calls as a hard failure, which trains the reader to ignore the check.

    Both ``mcp.servers.<name>`` and the flat ``mcp.<name>`` layouts are accepted, because
    the committed config and locally normalised copies of it differ.
    """
    try:
        mcp = json.loads((ROOT / SERENA_CLIENT_CONFIG).read_text(encoding="utf-8"))["mcp"]
    except (OSError, KeyError, TypeError, json.JSONDecodeError):
        return None
    if not isinstance(mcp, dict):
        return None
    servers = mcp.get("servers") if isinstance(mcp.get("servers"), dict) else mcp
    entry = servers.get("serena")
    command = entry.get("command") if isinstance(entry, dict) else None
    if not isinstance(command, list) or not all(isinstance(part, str) for part in command):
        return None
    prefix = _uvx_tool_prefix(command)
    if prefix is None:
        return None
    launcher = shutil.which(prefix[0])
    if launcher is None:
        return None
    return [launcher, *prefix[1:], "--version"]


def _serena_version_check(current: str, *, check_updates: bool, via: str, stale_hint: str) -> Check:
    """Report a resolved Serena version, optionally against the current PyPI release."""
    if not check_updates:
        return Check("serena", "ok", f"{current} {via}; use --check-updates to compare PyPI")

    latest = _latest_pypi_version(SERENA_PACKAGE)
    if latest is None:
        return Check("serena", "warn", f"{current} {via}; PyPI update check unavailable")
    if _version_tuple(current) < _version_tuple(latest):
        return Check("serena", "warn", f"{current} {via}; latest is {latest}; {stale_hint}")
    return Check("serena", "ok", f"{current} {via}; current (latest {latest})")


def _check_serena(check_updates: bool) -> Check:
    """Resolve Serena the way this repo's clients launch it.

    ``opencode.json``, ``.agents/mcp.json`` and the Codex profile all launch Serena through
    ``uvx``, which resolves the tool per launch and never puts a ``serena`` executable on
    PATH. That launch shape is the documented baseline (tools/mcp/SERENA.md), so a
    uvx-resolvable Serena is a healthy environment rather than a warning. A bare PATH
    install still answers ``serena --version``, but no first-class client would launch it,
    so it is reported as a warning naming the shape the clients expect.
    """
    probe = _serena_uvx_probe()
    if probe is not None:
        # uvx resolves the tool on demand, so this probe can be slow on a cold cache.
        resolved = _capture(*probe, timeout=90)
        if resolved is None or resolved.returncode != 0:
            return Check("serena", "fail", _first_line(resolved))
        version = _extract_version(_first_line(resolved)) or "unknown"
        return _serena_version_check(
            version,
            check_updates=check_updates,
            via=f"via uvx ({SERENA_CLIENT_CONFIG})",
            stale_hint="restart the client session so uvx re-resolves the latest release",
        )

    serena = shutil.which("serena.exe") or shutil.which("serena")
    if serena is None:
        return Check(
            "serena",
            "fail",
            "serena not resolvable: no uvx launch in "
            f"{SERENA_CLIENT_CONFIG} and no serena on PATH; install uv so uvx is on PATH "
            f'(`uvx -p 3.13 --from "{SERENA_PACKAGE}@latest" serena --version`)',
        )
    completed = _capture(serena, "--version")
    if completed is None or completed.returncode != 0:
        return Check("serena", "fail", _first_line(completed))

    first_line = _first_line(completed)
    current = _extract_version(first_line) or "unknown"
    check = _serena_version_check(
        current,
        check_updates=check_updates,
        via="on PATH only, not via uvx",
        stale_hint=f"run uv tool upgrade {SERENA_PACKAGE} --prerelease=allow",
    )
    if shutil.which("uvx") is None:
        note = f"`uvx` is not on PATH and {SERENA_CLIENT_CONFIG} launches Serena through it"
    else:
        note = f"{SERENA_CLIENT_CONFIG} has no uvx launch for serena"
    return Check("serena", "warn", f"{check.detail}; {note}")


def _resolved_version(executable: str, *args: str, timeout: int = 15) -> str | None:
    """Run ``executable`` and return the version it prints, or None when it cannot."""
    completed = _capture(executable, *args, timeout=timeout)
    if completed is None or completed.returncode != 0:
        return None
    return _extract_version(_first_line(completed))


def _same_version(left: str, right: str) -> bool:
    """Compare dotted versions without treating a trailing ``.0`` as a difference."""
    left_parts, right_parts = _version_tuple(left), _version_tuple(right)
    width = max(len(left_parts), len(right_parts))
    return left_parts + (0,) * (width - len(left_parts)) == right_parts + (0,) * (
        width - len(right_parts)
    )


def _check_basic_memory() -> Check:
    """Compare the two Basic Memory launch shapes against each other.

    tools/mcp/BASIC_MEMORY.md requires both shapes to stay on one version: every
    first-class client launches ``uvx basic-memory@latest``, while the optional
    bare-command examples (Cursor, Cline) launch whatever ``uv tool install`` last put on
    PATH. Both read one ``%USERPROFILE%\\.basic-memory\\memory.db``, so the channel that
    starts a newer release first migrates the database forward, and a lagging install then
    dies with ``Can't locate revision``. Either version looks healthy on its own, which is
    why the guide's rule needs a check rather than a reader.
    """
    uvx = shutil.which("uvx")
    # uvx resolves the package on demand, so a cold cache makes this probe slow.
    uvx_version = (
        _resolved_version(uvx, f"{BASIC_MEMORY_PACKAGE}@latest", "--version", timeout=90)
        if uvx is not None
        else None
    )

    on_path = shutil.which("basic-memory.exe") or shutil.which("basic-memory")
    path_version = _resolved_version(on_path, "--version") if on_path is not None else None

    if uvx_version and path_version:
        if _same_version(uvx_version, path_version):
            return Check(
                "basic_memory",
                "ok",
                f"{uvx_version} via uvx and PATH; both launch shapes agree",
            )
        return Check(
            "basic_memory",
            "warn",
            f"uvx {uvx_version} vs PATH {path_version}; both read one memory.db, so the "
            f"newer one migrates it out from under the older; refresh with "
            f"`{BASIC_MEMORY_REFRESH}`",
        )

    if uvx_version:
        return Check(
            "basic_memory",
            "warn",
            f"{uvx_version} via uvx only; no PATH install, so the bare-command examples "
            f"(Cursor, Cline) cannot launch it; add one with `{BASIC_MEMORY_REFRESH}`",
        )

    if path_version:
        uvx_note = (
            "`uvx` is not on PATH, and every first-class client launches it through uvx"
            if uvx is None
            else f"`uvx {BASIC_MEMORY_PACKAGE}@latest --version` did not report a version"
        )
        return Check(
            "basic_memory",
            "warn",
            f"PATH {path_version} only; {uvx_note}; refresh with `{BASIC_MEMORY_REFRESH}`",
        )

    return Check(
        "basic_memory",
        "warn",
        f"neither uvx nor PATH resolves {BASIC_MEMORY_PACKAGE}; install uv for the uvx "
        f"clients, then `{BASIC_MEMORY_REFRESH}`",
    )


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
    """Verify the installed tree, not just the directory.

    ``node_modules`` being a directory reported a false green: a half-finished or
    corrupted install (interrupted ``npm ci``, a stale lockfile, a removed dependency)
    leaves the folder in place while imports fail at runtime. Run ``npm ls`` so the
    check reflects whether the tree actually satisfies ``package.json``.
    """
    if not (ROOT / "node_modules").is_dir():
        return Check("node_modules", "warn", "node_modules missing; run npm ci")

    npm = shutil.which("npm.cmd") or shutil.which("npm")
    if npm is None:
        return Check("node_modules", "warn", "npm not found; cannot verify node_modules")

    completed = _capture(npm, "ls", "--depth=0", "--offline", timeout=30)
    if completed is None or completed.returncode != 0:
        return Check(
            "node_modules", "warn", f"node_modules present but unusable: {_first_line(completed)}"
        )
    return Check("node_modules", "ok", "node_modules present and satisfies package.json")


PYTHON_VERSION_PROBE = "import sys; print('%d.%d.%d' % sys.version_info[:3])"


def _check_python_env() -> Check:
    """Verify the venv runs Python at the repo's pinned version.

    Directory existence alone reported a false green: this clone's ``.venv`` is a Linux/WSL
    venv whose ``bin/python`` points at a ``/usr/bin/python3`` that does not exist on Windows,
    so every ``python -m ...`` inside it failed while the preflight called the environment
    ready. Presence is read with ``lstat`` because ``exists()`` raises ``OSError`` on those
    broken links instead of answering; a present interpreter is then run, and the version it
    prints is compared with the pin in ``src/python_version_guard.py``.
    """
    venv = ROOT / ".venv"
    if not venv.is_dir():
        return Check("python_env", "warn", ".venv missing; create/sync local Python env")

    pinned = f"{MIN_VERSION[0]}.{MIN_VERSION[1]}+"
    problems: list[str] = []
    for relative in ("Scripts/python.exe", "bin/python"):  # Windows and POSIX layouts
        interpreter = venv / relative
        try:
            interpreter.lstat()
        except OSError:
            continue
        version = _resolved_version(str(interpreter), "-c", PYTHON_VERSION_PROBE)
        if version is None:
            problems.append(f"{relative} is not runnable here")
        elif not is_required_python(_version_tuple(version)):
            problems.append(f"{relative} reports Python {version}, needs {pinned}")
        else:
            return Check(
                "python_env", "ok", f".venv {version} runnable ({relative}), meets {pinned}"
            )

    if not problems:
        return Check(
            "python_env",
            "warn",
            f".venv has no Scripts/python.exe or bin/python; recreate it for this OS "
            f"(needs Python {pinned})",
        )
    return Check(
        "python_env",
        "warn",
        f".venv unusable: {'; '.join(problems)}; recreate it for this OS (needs Python {pinned})",
    )


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


def _memory_vault_path() -> Path | None:
    """Locate the BaluffoMemory continuity vault, or None when it is not on this machine."""
    override = os.environ.get(MEMORY_VAULT_ENV_VAR)
    if override:
        candidate = Path(override).expanduser()
        return candidate if candidate.is_dir() else None
    candidate = ROOT.parent / MEMORY_VAULT_DIR_NAME
    return candidate if candidate.is_dir() else None


def _git_lines(vault: Path, *args: str) -> list[str] | None:
    try:
        completed = subprocess.run(
            ["git", "-C", str(vault), *args],
            capture_output=True,
            text=True,
            check=False,
            timeout=15,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if completed.returncode != 0:
        return None
    return [line for line in completed.stdout.splitlines() if line.strip()]


def _check_memory_vault() -> Check | None:
    """Warn when continuity notes are written but never committed.

    Notes are only durable once pushed: an uncommitted note is invisible to every
    other machine and is destroyed by a routine ``git clean``. The closeout
    checklist in tools/mcp/BASIC_MEMORY.md documents the commit step, but nothing
    detected a session that skipped it, so notes accumulated uncommitted twice.
    """
    vault = _memory_vault_path()
    if vault is None:
        return None
    if _git_lines(vault, "rev-parse", "--is-inside-work-tree") is None:
        return Check("memory_vault", "warn", f"{vault.name} present but not a git repo")

    status = _git_lines(vault, "status", "--porcelain")
    if status is None:
        return Check("memory_vault", "warn", f"{vault.name} git status unavailable")

    untracked = sum(1 for line in status if line.startswith("??"))
    uncommitted = len(status) - untracked

    unpushed = _git_lines(vault, "rev-list", "--count", "origin/main..HEAD")
    unpushed_count = int(unpushed[0]) if unpushed and unpushed[0].isdigit() else 0

    if not status and not unpushed_count:
        return Check("memory_vault", "ok", f"{vault.name} clean and pushed")

    parts = []
    if uncommitted:
        parts.append(f"{uncommitted} uncommitted")
    if untracked:
        parts.append(f"{untracked} untracked")
    if unpushed_count:
        parts.append(f"{unpushed_count} unpushed commit(s)")
    detail = ", ".join(parts) + f" in {vault.name}; commit and push before closeout"
    return Check("memory_vault", "warn", detail)


def _checks(*, smoke: bool, check_updates: bool) -> list[Check]:
    checks = [
        _check_python(),
        _check_node(),
        _check_npm(),
        _check_serena(check_updates),
        _check_basic_memory(),
        _check_lockfiles(),
        _check_node_modules(),
        _check_python_env(),
        _check_git_hooks(),
        _check_toolbelt(smoke),
        _check_playwright(),
        _check_path_location(),
    ]
    memory_check = _check_memory_vault()
    if memory_check is not None:
        checks.append(memory_check)
    return checks


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
