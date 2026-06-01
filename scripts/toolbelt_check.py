#!/usr/bin/env python3
"""Check and optionally install the AI-toolbelt CLI tools.

These are contributor-local CLI helpers for AI-assisted repo work.
They are not Baluffo runtime, packaging, release, or CI dependencies.
"""

from __future__ import annotations

import argparse
import os
import platform as _platform
import shutil
import subprocess
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
    custom_install: bool = False


TOOLS = (
    Tool("rg", "Fast text search", apt_package="ripgrep"),
    Tool(
        "fd",
        "Fast file discovery",
        alt_binaries=("fdfind",),
        apt_package="fd-find",
        apt_post_install=(
            f"mkdir -p {LOCAL_BIN}",
            f"ln -sf $(which fdfind) {LOCAL_BIN}/fd",
        ),
    ),
    Tool(
        "bat",
        "File previews with line ranges",
        alt_binaries=("batcat",),
        apt_package="bat",
        apt_post_install=(
            f"mkdir -p {LOCAL_BIN}",
            f"ln -sf $(which batcat) {LOCAL_BIN}/bat",
        ),
    ),
    Tool("jq", "JSON querying", apt_package="jq"),
    Tool("yq", "YAML/TOML/XML querying (jq syntax)", apt_package="yq"),
    Tool("ast-grep", "Syntax-aware code search", npm_package="@ast-grep/cli"),
    Tool("tokei", "Codebase line-count stats", apt_package="tokei"),
    Tool("gron", "Flatten JSON for grep exploration", apt_package="gron"),
    Tool("eza", "Directory tree visualization", apt_package="eza"),
    Tool("mlr", "Unified CSV/JSONL/TSV processing", apt_package="miller"),
    Tool("difft", "Syntax-aware structural git diff", custom_install=True),
)


def _npm_global_bin() -> str | None:
    completed = subprocess.run(
        ["npm", "config", "get", "prefix"],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        return None
    prefix = completed.stdout.strip()
    if not prefix:
        return None
    bin_dir = os.path.join(prefix, "bin")
    return bin_dir if os.path.isdir(bin_dir) else None


def _extra_path_dirs() -> list[str]:
    dirs: list[str] = [str(LOCAL_BIN)]
    npm_bin = _npm_global_bin()
    if npm_bin:
        dirs.append(npm_bin)
    return dirs


def _which(tool: Tool) -> str | None:
    """Return the available binary path for a tool, searching PATH and known extra dirs."""
    found = shutil.which(tool.binary)
    if found:
        return found
    for extra in _extra_path_dirs():
        candidate = os.path.join(extra, tool.binary)
        if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
            return candidate
    for alt in tool.alt_binaries:
        found = shutil.which(alt)
        if found:
            return found
        for extra in _extra_path_dirs():
            candidate = os.path.join(extra, alt)
            if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
                return candidate
    return None


def _check_sudo_nopasswd() -> bool:
    completed = subprocess.run(
        ["sudo", "-n", "true"],
        capture_output=True,
        check=False,
    )
    return completed.returncode == 0


def _run(*args: str, **kwargs: object) -> bool:
    print(f"  $ {' '.join(args)}")
    completed = subprocess.run(args, check=False)
    return completed.returncode == 0


def _print_check_results(available: list[Tool], missing: list[Tool]) -> None:
    print(f"\n{_COLOR_BOLD}AI Toolbelt Status{_COLOR_RESET}")
    print("─" * 56)
    for tool in TOOLS:
        if tool in available:
            path = _which(tool)
            label = tool.binary
            note = ""
            if path and os.path.basename(path) != tool.binary:
                note = f" (via {os.path.basename(path)})"
            print(f"  {_COLOR_OK}\u2713{_COLOR_RESET} {label:<12} {tool.desc}{note}")
        else:
            print(f"  {_COLOR_BAD}\u2717{_COLOR_RESET} {tool.binary:<12} {tool.desc}")
    print("─" * 56)
    total = len(TOOLS)
    ok = len(available)
    if ok == total:
        print(f"{_COLOR_OK}All {total} tools available.{_COLOR_RESET}")
    else:
        print(
            f"Available: {ok}/{total}  Missing: {total - ok}\n"
            f"{_COLOR_WARN}Run with --install to install missing tools.{_COLOR_RESET}"
        )


def _ensure_local_bin_in_path() -> None:
    if str(LOCAL_BIN) not in os.environ.get("PATH", ""):
        print(f"{_COLOR_WARN}Note: {LOCAL_BIN} is not in your PATH.{_COLOR_RESET}")
        print(f'  Add it with:  export PATH="{LOCAL_BIN}:$PATH"')
        print("  Or add it to your shell profile (~/.bashrc or ~/.zshrc).")
        print()


def _install_apt(
    missing: list[Tool],
    *,
    has_sudo: bool,
) -> tuple[list[Tool], list[Tool]]:
    apt_tools = [t for t in missing if t.apt_package is not None]
    other_tools = [t for t in missing if t.apt_package is None]

    if not apt_tools:
        return [], other_tools

    if not has_sudo:
        print(f"\n{_COLOR_WARN}sudo requires a password. Skipping apt installs.{_COLOR_RESET}")
        print("Run the following commands manually and re-run this script after:\n")
        for tool in apt_tools:
            print(f"  sudo apt install -y {tool.apt_package}")
            for cmd in tool.apt_post_install:
                print(f"  {cmd}")
        print()
        return apt_tools, other_tools

    installed: list[Tool] = []
    still_missing: list[Tool] = []

    for tool in apt_tools:
        print(f"\n  Installing {tool.binary}...")
        ok = _run("sudo", "-n", "apt", "install", "-y", tool.apt_package)  # type: ignore[arg-type]
        if not ok:
            print(f"  {_COLOR_BAD}  apt install failed{_COLOR_RESET}")
            still_missing.append(tool)
            continue

        for cmd in tool.apt_post_install:
            subprocess.run(cmd, shell=True, check=False)

        if _which(tool):
            print(f"  {_COLOR_OK}  installed{_COLOR_RESET}")
            installed.append(tool)
        else:
            print(
                f"  {_COLOR_WARN}  apt succeeded but {tool.binary} not found in PATH{_COLOR_RESET}"
            )
            still_missing.append(tool)

    return installed, [*other_tools, *still_missing]


def _install_npm(missing: list[Tool]) -> tuple[list[Tool], list[Tool]]:
    npm_tools = [t for t in missing if t.npm_package is not None]
    other_tools = [t for t in missing if t.npm_package is None]

    if not npm_tools:
        return [], other_tools

    installed: list[Tool] = []
    still_missing: list[Tool] = []

    for tool in npm_tools:
        print(f"\n  Installing {tool.binary}...")
        ok = _run("npm", "install", "-g", tool.npm_package)  # type: ignore[arg-type]
        if not ok:
            print(f"  {_COLOR_BAD}  npm install failed{_COLOR_RESET}")
            still_missing.append(tool)
            continue

        if _which(tool):
            print(f"  {_COLOR_OK}  installed{_COLOR_RESET}")
            installed.append(tool)
        else:
            print(
                f"  {_COLOR_WARN}  npm succeeded but {tool.binary} not found in PATH{_COLOR_RESET}"
            )
            still_missing.append(tool)

    return installed, other_tools


def _install_difftastic() -> bool:
    arch = _platform.machine()
    if arch == "x86_64":
        asset = "difft-x86_64-unknown-linux-gnu"
    elif arch == "aarch64":
        asset = "difft-aarch64-unknown-linux-gnu"
    else:
        print(
            f"  {_COLOR_WARN}Unknown architecture '{arch}' for difftastic, skipping.{_COLOR_RESET}"
        )
        return False

    url = f"https://github.com/Wilfred/difftastic/releases/latest/download/{asset}.tar.gz"
    tmp_archive = Path("/tmp/difftastic.tar.gz")
    tmp_dir = Path("/tmp/difftastic_extract")

    try:
        print("  Downloading difftastic...")
        subprocess.run(
            ["curl", "-fsSL", "-o", str(tmp_archive), url],
            check=True,
        )

        tmp_dir.mkdir(exist_ok=True)
        subprocess.run(
            ["tar", "xzf", str(tmp_archive), "-C", str(tmp_dir)],
            check=True,
        )

        binary_path = tmp_dir / "difft"
        if not binary_path.exists():
            print(f"  {_COLOR_BAD}  difft binary not found in extracted archive{_COLOR_RESET}")
            return False

        LOCAL_BIN.mkdir(parents=True, exist_ok=True)
        _run("cp", str(binary_path), str(LOCAL_BIN / "difft"))
        _run("chmod", "+x", str(LOCAL_BIN / "difft"))

        tmp_archive.unlink(missing_ok=True)
        shutil.rmtree(tmp_dir, ignore_errors=True)

        return True
    except subprocess.CalledProcessError as exc:
        print(f"  {_COLOR_BAD}  Failed: {exc}{_COLOR_RESET}")
        tmp_archive.unlink(missing_ok=True)
        shutil.rmtree(tmp_dir, ignore_errors=True)
        return False


def _install_custom(missing: list[Tool]) -> list[Tool]:
    custom_tools = [t for t in missing if t.custom_install]
    if not custom_tools:
        return missing

    LOCAL_BIN.mkdir(parents=True, exist_ok=True)
    remaining: list[Tool] = []

    for tool in custom_tools:
        print(f"\n  Installing {tool.binary} ({tool.desc})...")
        if tool.binary == "difft":
            ok = _install_difftastic()
        else:
            print(f"  {_COLOR_WARN}  no custom installer for {tool.binary}{_COLOR_RESET}")
            remaining.append(tool)
            continue

        if ok and _which(tool):
            print(f"  {_COLOR_OK}  installed{_COLOR_RESET}")
        else:
            print(f"  {_COLOR_BAD}  install failed{_COLOR_RESET}")
            remaining.append(tool)

    return [t for t in missing if not t.custom_install] + remaining


def _install_linux(missing: list[Tool]) -> list[Tool]:
    has_sudo = _check_sudo_nopasswd()

    if has_sudo:
        print(
            f"\n{_COLOR_OK}Installing missing tools via apt + npm + binary download...{_COLOR_RESET}"
        )
    else:
        print(
            f"\n{_COLOR_WARN}sudo requires a password — apt installs will be skipped.{_COLOR_RESET}\n"
            f"Install the apt packages manually and re-run this script.\n"
        )

    _ensure_local_bin_in_path()

    remaining: list[Tool] = list(missing)

    apt_installed, remaining = _install_apt(remaining, has_sudo=has_sudo)

    npm_installed, remaining = _install_npm(remaining)

    remaining = _install_custom(remaining)

    all_installed = apt_installed + npm_installed

    if all_installed:
        print(
            f"\n  {_COLOR_OK}Installed: {', '.join(t.binary for t in all_installed)}{_COLOR_RESET}"
        )

    return remaining


def _install(missing: list[Tool]) -> list[Tool]:
    system = _platform.system()
    if system == "Linux":
        return _install_linux(missing)
    elif system == "Darwin":
        print(f"\n{_COLOR_WARN}macOS: install via Homebrew.{_COLOR_RESET}")
        print("See docs/AI_ASSISTANT_GUIDE.md for per-tool commands.\n")
        return missing
    elif system == "Windows":
        print(f"\n{_COLOR_WARN}Windows: install via winget.{_COLOR_RESET}")
        print("See docs/AI_ASSISTANT_GUIDE.md for per-tool commands.\n")
        return missing
    else:
        print(f"\n{_COLOR_WARN}Unknown platform '{system}'. Cannot auto-install.{_COLOR_RESET}")
        return missing


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check and optionally install AI-toolbelt CLI tools.",
    )
    parser.add_argument(
        "--install",
        action="store_true",
        help="Install missing tools (requires sudo for apt packages).",
    )
    args = parser.parse_args()

    available: list[Tool] = []
    missing: list[Tool] = []

    for tool in TOOLS:
        if _which(tool):
            available.append(tool)
        else:
            missing.append(tool)

    _print_check_results(available, missing)

    if missing and args.install:
        remaining = _install(missing)
        if remaining:
            print(
                f"\n{_COLOR_WARN}{len(remaining)} tool(s) still missing. "
                f"See docs/AI_ASSISTANT_GUIDE.md for manual install.{_COLOR_RESET}"
            )
        else:
            print(f"\n{_COLOR_OK}All tools now available.{_COLOR_RESET}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
