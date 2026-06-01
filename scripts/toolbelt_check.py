#!/usr/bin/env python3
"""Check and optionally install the AI-toolbelt CLI tools.

These are contributor-local CLI helpers for AI-assisted repo work.
They are not Baluffo runtime, packaging, release, or CI dependencies.
"""

from __future__ import annotations

import argparse
import json
import os
import platform as _platform
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LOCAL_BIN = Path.home() / ".local" / "bin"

_COLOR_OK = "\033[92m"
_COLOR_WARN = "\033[93m"
_COLOR_BAD = "\033[91m"
_COLOR_BOLD = "\033[1m"
_COLOR_RESET = "\033[0m"


@dataclass(frozen=True)
class Tool:
    binary: str
    desc: str
    alt_binaries: tuple[str, ...] = ()
    apt_package: str | None = None
    apt_post_install: tuple[str, ...] = ()
    npm_package: str | None = None
    winget_id: str | None = None


TOOLS = (
    Tool(
        "rg",
        "Fast text search",
        apt_package="ripgrep",
        winget_id="BurntSushi.ripgrep.MSVC",
    ),
    Tool(
        "fd",
        "Fast file discovery",
        alt_binaries=("fdfind",),
        apt_package="fd-find",
        apt_post_install=(
            f"mkdir -p {LOCAL_BIN}",
            f"ln -sf $(which fdfind) {LOCAL_BIN}/fd",
        ),
        winget_id="sharkdp.fd",
    ),
    Tool(
        "bat",
        "Focused file previews",
        alt_binaries=("batcat",),
        apt_package="bat",
        apt_post_install=(
            f"mkdir -p {LOCAL_BIN}",
            f"ln -sf $(which batcat) {LOCAL_BIN}/bat",
        ),
        winget_id="sharkdp.bat",
    ),
    Tool("jq", "JSON querying", apt_package="jq", winget_id="jqlang.jq"),
    Tool("yq", "YAML/TOML/XML querying", apt_package="yq", winget_id="MikeFarah.yq"),
    Tool(
        "ast-grep",
        "Syntax-aware code search",
        npm_package="@ast-grep/cli",
        winget_id="ast-grep.ast-grep",
    ),
    Tool(
        "tokei",
        "Codebase composition overview",
        apt_package="tokei",
        winget_id="XAMPPRocky.Tokei",
    ),
)


def _system() -> str:
    return _platform.system()


def _can_encode(text: str) -> bool:
    encoding = sys.stdout.encoding or "utf-8"
    try:
        text.encode(encoding)
    except UnicodeEncodeError:
        return False
    return True


def _color_enabled() -> bool:
    return sys.stdout.isatty() and os.environ.get("NO_COLOR") is None


def _style(text: str, color: str) -> str:
    if not _color_enabled():
        return text
    return f"{color}{text}{_COLOR_RESET}"


def _ok(text: str) -> str:
    return _style(text, _COLOR_OK)


def _warn(text: str) -> str:
    return _style(text, _COLOR_WARN)


def _bad(text: str) -> str:
    return _style(text, _COLOR_BAD)


def _glyphs() -> tuple[str, str, str]:
    if _can_encode("✓✗─"):
        return "✓", "✗", "─"
    return "OK", "MISS", "-"


def _command_path(*names: str) -> str | None:
    for name in names:
        found = shutil.which(name)
        if found:
            return found
    return None


def _npm_command() -> str | None:
    if _system() == "Windows":
        return _command_path("npm.cmd", "npm")
    return _command_path("npm")


def _winget_command() -> str | None:
    if _system() == "Windows":
        return _command_path("winget.exe", "winget")
    return None


def _npm_global_bin() -> str | None:
    npm = _npm_command()
    if npm is None:
        return None
    try:
        completed = subprocess.run(
            [npm, "config", "get", "prefix"],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        return None
    if completed.returncode != 0:
        return None
    prefix = completed.stdout.strip()
    if not prefix:
        return None
    bin_dir = Path(prefix) if _system() == "Windows" else Path(prefix) / "bin"
    return str(bin_dir) if bin_dir.is_dir() else None


def _extra_path_dirs() -> list[str]:
    dirs: list[str] = [str(LOCAL_BIN)]
    npm_bin = _npm_global_bin()
    if npm_bin:
        dirs.append(npm_bin)
    return dirs


def _winget_tool_dirs(tool: Tool) -> list[str]:
    if _system() != "Windows" or not tool.winget_id:
        return []
    local_appdata = os.environ.get("LOCALAPPDATA")
    if not local_appdata:
        return []
    base = Path(local_appdata) / "Microsoft" / "WinGet" / "Packages"
    if not base.is_dir():
        return []

    dirs: list[str] = []
    for package_dir in base.glob(f"{tool.winget_id}_*"):
        if not package_dir.is_dir():
            continue
        dirs.append(str(package_dir))
        try:
            dirs.extend(str(child) for child in package_dir.iterdir() if child.is_dir())
        except OSError:
            continue
    return dirs


def _which(tool: Tool) -> str | None:
    """Return the available binary path, searching PATH and known extra dirs."""
    names = (tool.binary, *tool.alt_binaries)
    for name in names:
        found = shutil.which(name)
        if found:
            return found
    for extra in [*_extra_path_dirs(), *_winget_tool_dirs(tool)]:
        for name in names:
            found = shutil.which(name, path=extra)
            if found:
                return found
    return None


def _check_sudo_nopasswd() -> bool:
    sudo = _command_path("sudo")
    if sudo is None:
        return False
    try:
        completed = subprocess.run(
            [sudo, "-n", "true"],
            capture_output=True,
            check=False,
        )
    except OSError:
        return False
    return completed.returncode == 0


def _run(*args: str) -> bool:
    print(f"  $ {' '.join(args)}")
    try:
        completed = subprocess.run(args, check=False)
    except OSError as exc:
        print(f"  {_bad('failed to start')}: {exc}")
        return False
    return completed.returncode == 0


def _capture(
    *args: str, cwd: Path = ROOT, timeout: int = 10
) -> subprocess.CompletedProcess[str] | None:
    try:
        return subprocess.run(
            args,
            cwd=cwd,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None


def _format_available_note(tool: Tool, path: str | None) -> str:
    if not path:
        return ""
    executable = Path(path).name
    if Path(executable).stem.lower() != tool.binary.lower():
        return f" (via {executable})"
    return ""


def _print_check_results(available: list[Tool], missing: list[Tool]) -> None:
    ok_marker, missing_marker, line_char = _glyphs()

    print(f"\n{_style('AI Toolbelt Status', _COLOR_BOLD)}")
    print(line_char * 56)
    for tool in TOOLS:
        if tool in available:
            note = _format_available_note(tool, _which(tool))
            print(f"  {_ok(ok_marker):<7} {tool.binary:<12} {tool.desc}{note}")
        else:
            print(f"  {_bad(missing_marker):<7} {tool.binary:<12} {tool.desc}")
    print(line_char * 56)

    total = len(TOOLS)
    ok_count = len(available)
    if ok_count == total:
        print(_ok(f"All {total} default tools available."))
        return

    missing_names = ", ".join(tool.binary for tool in missing)
    print(f"Available: {ok_count}/{total}  Missing: {total - ok_count}")
    print(_warn(f"Missing: {missing_names}"))
    print(_warn("Run with --install to install missing default tools."))


def _tool_command(tool: Tool) -> list[str] | None:
    path = _which(tool)
    if path is None:
        return None
    return [path]


def _smoke_command(tool: Tool) -> list[str] | None:
    base = _tool_command(tool)
    if base is None:
        return None

    if tool.binary == "rg":
        return [*base, "-n", "Repo Guardrails", "AGENTS.md"]
    if tool.binary == "fd":
        return [*base, "AI_ASSISTANT_GUIDE.md", "docs"]
    if tool.binary == "bat":
        return [*base, "--style=plain", "--line-range", "92:98", "docs/AI_ASSISTANT_GUIDE.md"]
    if tool.binary == "jq":
        return [*base, "-r", ".scripts.verify", "package.json"]
    if tool.binary == "yq":
        return [*base, "-r", ".repos[0].repo", ".pre-commit-config.yaml"]
    if tool.binary == "ast-grep":
        return [
            *base,
            "run",
            "--lang",
            "py",
            "--pattern",
            "shutil.which($ARG)",
            "scripts/toolbelt_check.py",
        ]
    if tool.binary == "tokei":
        return [*base, "scripts/toolbelt_check.py"]
    return None


def _smoke_results() -> list[dict[str, object]]:
    results: list[dict[str, object]] = []
    for tool in TOOLS:
        path = _which(tool)
        direct_path = shutil.which(tool.binary)
        command = _smoke_command(tool)
        if path is None or command is None:
            results.append(
                {
                    "tool": tool.binary,
                    "status": "missing",
                    "path": path,
                    "direct_path": direct_path,
                    "works_in_current_shell": False,
                }
            )
            continue

        completed = _capture(*command)
        ok = completed is not None and completed.returncode == 0
        results.append(
            {
                "tool": tool.binary,
                "status": "ok" if ok else "failed",
                "path": path,
                "direct_path": direct_path,
                "works_in_current_shell": direct_path is not None,
                "smoke_returncode": completed.returncode if completed else None,
            }
        )
    return results


def _print_smoke_results(results: list[dict[str, object]]) -> None:
    ok_marker, missing_marker, line_char = _glyphs()
    print(f"\n{_style('AI Toolbelt Smoke', _COLOR_BOLD)}")
    print(line_char * 72)
    for result in results:
        status = str(result["status"])
        marker = ok_marker if status == "ok" else missing_marker
        tool = str(result["tool"])
        shell = "direct" if result["works_in_current_shell"] else "fallback"
        path = str(result.get("path") or "")
        print(
            f"  {(_ok(marker) if status == 'ok' else _bad(marker)):<7} {tool:<12} {status:<7} {shell:<8} {path}"
        )
    print(line_char * 72)
    failed = [str(r["tool"]) for r in results if r["status"] != "ok"]
    if failed:
        print(_warn(f"Smoke failures: {', '.join(failed)}"))
    else:
        print(_ok("All default tools passed smoke checks."))


def _ensure_local_bin_in_path() -> None:
    if str(LOCAL_BIN) not in os.environ.get("PATH", ""):
        print(_warn(f"Note: {LOCAL_BIN} is not in your PATH."))
        print(f'  Add it with:  export PATH="{LOCAL_BIN}:$PATH"')
        print("  Or add it to your shell profile (~/.bashrc or ~/.zshrc).")
        print()


def _print_apt_manual_commands(apt_tools: list[Tool]) -> None:
    for tool in apt_tools:
        print(f"  sudo apt install -y {tool.apt_package}")
        for cmd in tool.apt_post_install:
            print(f"  {cmd}")


def _install_apt(
    missing: list[Tool],
    *,
    has_sudo: bool,
) -> tuple[list[Tool], list[Tool]]:
    apt_tools = [t for t in missing if t.apt_package is not None]
    other_tools = [t for t in missing if t.apt_package is None]

    if not apt_tools:
        return [], other_tools

    sudo = _command_path("sudo")
    apt = _command_path("apt")
    if not has_sudo or sudo is None or apt is None:
        print(_warn("\nsudo/apt is unavailable for non-interactive installs."))
        print("Run the following commands manually and re-run this script after:\n")
        _print_apt_manual_commands(apt_tools)
        print()
        return [], missing

    installed: list[Tool] = []
    still_missing: list[Tool] = []

    for tool in apt_tools:
        print(f"\n  Installing {tool.binary}...")
        ok = _run(sudo, "-n", apt, "install", "-y", str(tool.apt_package))
        if not ok:
            print(f"  {_bad('apt install failed')}")
            still_missing.append(tool)
            continue

        for cmd in tool.apt_post_install:
            try:
                subprocess.run(cmd, shell=True, check=False)
            except OSError as exc:
                print(f"  {_warn(f'post-install command failed to start: {exc}')}")

        if _which(tool):
            print(f"  {_ok('installed')}")
            installed.append(tool)
        else:
            print(_warn(f"  apt succeeded but {tool.binary} was not found in PATH"))
            still_missing.append(tool)

    return installed, [*other_tools, *still_missing]


def _install_npm(missing: list[Tool]) -> tuple[list[Tool], list[Tool]]:
    npm_tools = [t for t in missing if t.npm_package is not None]
    other_tools = [t for t in missing if t.npm_package is None]

    if not npm_tools:
        return [], other_tools

    npm = _npm_command()
    if npm is None:
        print(_warn("\nnpm is unavailable. Run these commands manually after installing npm:\n"))
        for tool in npm_tools:
            print(f"  npm install -g {tool.npm_package}")
        print()
        return [], missing

    installed: list[Tool] = []
    still_missing: list[Tool] = []

    for tool in npm_tools:
        print(f"\n  Installing {tool.binary}...")
        ok = _run(npm, "install", "-g", str(tool.npm_package))
        if not ok:
            print(f"  {_bad('npm install failed')}")
            still_missing.append(tool)
            continue

        if _which(tool):
            print(f"  {_ok('installed')}")
            installed.append(tool)
        else:
            print(_warn(f"  npm succeeded but {tool.binary} was not found in PATH"))
            still_missing.append(tool)

    return installed, [*other_tools, *still_missing]


def _install_winget(missing: list[Tool]) -> tuple[list[Tool], list[Tool]]:
    winget_tools = [t for t in missing if t.winget_id is not None]
    other_tools = [t for t in missing if t.winget_id is None]

    if not winget_tools:
        return [], other_tools

    winget = _winget_command()
    if winget is None:
        print(_warn("\nwinget is unavailable. Run these commands manually if available:\n"))
        for tool in winget_tools:
            print(f"  winget install -e --id {tool.winget_id}")
        print()
        return [], missing

    installed: list[Tool] = []
    still_missing: list[Tool] = []

    for tool in winget_tools:
        print(f"\n  Installing {tool.binary}...")
        ok = _run(
            winget,
            "install",
            "-e",
            "--id",
            str(tool.winget_id),
            "--source",
            "winget",
            "--silent",
            "--accept-package-agreements",
            "--accept-source-agreements",
            "--disable-interactivity",
        )
        if not ok:
            print(f"  {_bad('winget install failed')}")
            still_missing.append(tool)
            continue

        if _which(tool):
            print(f"  {_ok('installed')}")
            installed.append(tool)
        else:
            print(_warn(f"  winget succeeded but {tool.binary} was not found in this shell"))
            still_missing.append(tool)

    return installed, [*other_tools, *still_missing]


def _print_installed(installed: list[Tool]) -> None:
    if installed:
        print(f"\n  {_ok('Installed')}: {', '.join(t.binary for t in installed)}")


def _install_linux(missing: list[Tool]) -> list[Tool]:
    has_sudo = _check_sudo_nopasswd()

    if has_sudo:
        print(f"\n{_ok('Installing missing tools via apt + npm...')}")
    else:
        print(_warn("\nsudo is unavailable or requires a password; apt installs will be skipped."))
        print("Install apt packages manually and re-run this script.\n")

    _ensure_local_bin_in_path()

    remaining: list[Tool] = list(missing)
    apt_installed, remaining = _install_apt(remaining, has_sudo=has_sudo)
    npm_installed, remaining = _install_npm(remaining)
    _print_installed([*apt_installed, *npm_installed])
    return remaining


def _install_windows(missing: list[Tool]) -> list[Tool]:
    print(f"\n{_ok('Installing missing tools via winget...')}")
    installed, remaining = _install_winget(missing)
    _print_installed(installed)
    if remaining:
        print(_warn("Restart the shell if winget installed a tool that is not yet on PATH."))
    return remaining


def _install(missing: list[Tool]) -> list[Tool]:
    system = _system()
    if system == "Linux":
        return _install_linux(missing)
    if system == "Windows":
        return _install_windows(missing)
    if system == "Darwin":
        print(f"\n{_warn('macOS: install missing tools via Homebrew.')}")
        print("See docs/AI_ASSISTANT_GUIDE.md for commands.\n")
        return missing

    print(f"\n{_warn(f'Unknown platform {system!r}. Cannot auto-install.')}")
    return missing


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check and optionally install AI-toolbelt CLI tools.",
    )
    parser.add_argument(
        "--install",
        action="store_true",
        help="Install missing default tools when supported by this platform.",
    )
    parser.add_argument(
        "--smoke",
        action="store_true",
        help="Run tiny repo-local smoke checks for each available default tool.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable status for checks and smoke results.",
    )
    args = parser.parse_args()

    available: list[Tool] = []
    missing: list[Tool] = []

    for tool in TOOLS:
        if _which(tool):
            available.append(tool)
        else:
            missing.append(tool)

    smoke_results = _smoke_results() if args.smoke else []

    if args.json:
        print(
            json.dumps(
                {
                    "available": [tool.binary for tool in available],
                    "missing": [tool.binary for tool in missing],
                    "smoke": smoke_results,
                },
                indent=2,
            )
        )
    else:
        _print_check_results(available, missing)
        if args.smoke:
            _print_smoke_results(smoke_results)

    if missing and args.install:
        remaining = _install(missing)
        if remaining:
            print(
                f"\n{_warn(f'{len(remaining)} tool(s) still missing. ')}"
                f"{_warn('See docs/AI_ASSISTANT_GUIDE.md for manual install.')}"
            )
        else:
            print(f"\n{_ok('All default tools now available.')}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
