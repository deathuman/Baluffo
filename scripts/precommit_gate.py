#!/usr/bin/env python3
"""Run pre-commit in either a changed-files or full-repo mode."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PYTHON = sys.executable


def _default_pre_commit_home() -> Path:
    local_app_data = os.environ.get("LOCALAPPDATA")
    if local_app_data:
        return Path(local_app_data) / "pre-commit"
    return Path.home() / "AppData" / "Local" / "pre-commit"


PRE_COMMIT_HOME = _default_pre_commit_home()
EXCLUDED_FILES = {
    "data/jobs-fetch-report.json",
    "data/jobs-fetch-tasks.json",
}
EXCLUDED_ROOT_PREFIXES = (
    ".pre-commit-home",
    ".tmp",
    ".pytest",
    ".mypy_cache",
    ".ruff_cache",
    ".venv",
    "pytest-cache-files-",
    "_out",
    "build",
    "dist",
    "tmp",
)
MAX_PRECOMMIT_FILES_PER_COMMAND = 200


def _git_lines(*args: str) -> list[str]:
    completed = subprocess.run(
        ["git", "-C", str(ROOT), *args],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        stderr = completed.stderr.strip()
        stdout = completed.stdout.strip()
        details = stderr or stdout or "no git output"
        raise RuntimeError(
            f"git {' '.join(args)} failed with exit code {completed.returncode}: {details}"
        )
    return [line.strip() for line in completed.stdout.splitlines() if line.strip()]


def _top_level_component(rel_path: str) -> str:
    parts = Path(rel_path).parts
    return parts[0] if parts else rel_path


def _is_excluded_root(rel_path: str, extra_roots: tuple[str, ...] = ()) -> bool:
    if rel_path in EXCLUDED_FILES:
        return True
    root_name = _top_level_component(rel_path)
    if any(root_name.startswith(prefix) for prefix in EXCLUDED_ROOT_PREFIXES):
        return True
    normalized_roots = tuple(prefix.strip().strip("/\\") for prefix in extra_roots)
    return any(root_name == prefix for prefix in normalized_roots if prefix)


def collect_changed_files() -> list[str]:
    """Return changed, staged, and untracked files relative to the repo root."""
    files: list[str] = []
    seen: set[str] = set()
    git_queries = (
        ("diff", "--cached", "--name-only", "--diff-filter=ACMRTUXB"),
        ("diff", "--name-only", "--diff-filter=ACMRTUXB"),
        ("ls-files", "--others", "--exclude-standard"),
    )
    for query in git_queries:
        for rel_path in _git_lines(*query):
            if rel_path in seen:
                continue
            if _is_excluded_root(rel_path):
                continue
            abs_path = ROOT / rel_path
            if not abs_path.exists() or abs_path.is_dir():
                continue
            seen.add(rel_path)
            files.append(rel_path)
    return files


def collect_repo_files(exclude_roots: tuple[str, ...] = ()) -> list[str]:
    """Return tracked repository files, optionally excluding selected top-level roots."""
    files: list[str] = []
    for rel_path in _git_lines("ls-files"):
        if _is_excluded_root(rel_path, exclude_roots):
            continue
        abs_path = ROOT / rel_path
        if not abs_path.exists() or abs_path.is_dir():
            continue
        files.append(rel_path)
    return files


def _precommit_base_command() -> list[str]:
    return [
        PYTHON,
        "-m",
        "pre_commit",
        "run",
        "--show-diff-on-failure",
        "--color=always",
    ]


def _run_precommit_command(command: list[str]) -> int:
    env = os.environ.copy()
    pre_commit_home = Path(env.setdefault("PRE_COMMIT_HOME", str(PRE_COMMIT_HOME)))
    pre_commit_home.mkdir(parents=True, exist_ok=True)
    completed = subprocess.run(command, cwd=ROOT, env=env)
    return completed.returncode


def build_changed_command(files: list[str]) -> list[str]:
    return [*_precommit_base_command(), "--files", *files]


def build_all_commands(files: list[str] | None = None) -> list[list[str]]:
    commands: list[list[str]] = []
    if files:
        for index in range(0, len(files), MAX_PRECOMMIT_FILES_PER_COMMAND):
            commands.append(
                [
                    *_precommit_base_command(),
                    "--files",
                    *files[index : index + MAX_PRECOMMIT_FILES_PER_COMMAND],
                ]
            )
    else:
        commands.append([*_precommit_base_command(), "--all-files"])
    commands.append(
        [
            *_precommit_base_command(),
            "vulture",
            "--all-files",
            "--hook-stage",
            "pre-push",
        ]
    )
    return commands


def should_run_repo_guardrails(files: list[str]) -> bool:
    watched_roots = ("src/", "frontend/", "tests/", "scripts/", ".github/", "docs/")
    watched_exact = {
        "AGENTS.md",
        "CONTRIBUTING.md",
        "README.md",
        "package.json",
        "package-lock.json",
        ".pre-commit-config.yaml",
        "ruff.toml",
        "mypy.ini",
        "data/defaults/source-registry-active.seed.json",
        "data/defaults/source-registry-pending.seed.json",
    }
    return any(path in watched_exact or path.startswith(watched_roots) for path in files)


def run_repo_guardrails(groups: tuple[str, ...] = ()) -> int:
    command = [PYTHON, "tools/repo_health/repo_guardrails.py"]
    for group in groups:
        command.extend(["--group", group])
    completed = subprocess.run(command, cwd=ROOT, check=False)
    return completed.returncode


def run_complexity_baseline() -> int:
    completed = subprocess.run(
        [PYTHON, "scripts/check_complexity_baseline.py"],
        cwd=ROOT,
        check=False,
    )
    return completed.returncode


def run_changed() -> int:
    files = collect_changed_files()
    if not files:
        print("No changed files found for pre-commit; skipping.")
        return 0
    return_code = _run_precommit_command(build_changed_command(files))
    if return_code != 0:
        return return_code
    if should_run_repo_guardrails(files):
        return run_repo_guardrails()
    return 0


def run_all(exclude_roots: tuple[str, ...] = ()) -> int:
    files = collect_repo_files(exclude_roots) if exclude_roots else None
    if files is not None and not files:
        print("No tracked files found for pre-commit; skipping.")
        return 0
    for command in build_all_commands(files):
        return_code = _run_precommit_command(command)
        if return_code != 0:
            return return_code
    return_code = run_repo_guardrails()
    if return_code != 0:
        return return_code
    return run_complexity_baseline()


def main() -> int:
    parser = argparse.ArgumentParser(description="Baluffo pre-commit gate")
    parser.add_argument(
        "--mode",
        choices=("changed", "all"),
        default="changed",
        help="Run only changed files or the full repository guardrail set.",
    )
    parser.add_argument(
        "--exclude-root",
        action="append",
        default=[],
        help="Exclude tracked files whose top-level path matches the given repo root.",
    )
    args = parser.parse_args()

    if args.mode == "all":
        return run_all(tuple(args.exclude_root))
    return run_changed()


if __name__ == "__main__":
    raise SystemExit(main())
