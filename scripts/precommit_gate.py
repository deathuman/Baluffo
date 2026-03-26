#!/usr/bin/env python3
"""Run pre-commit in either a changed-files or full-repo mode."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PYTHON = sys.executable
EXCLUDED_ROOT_PREFIXES = (
    ".codex-tmp-tests",
    ".pytest",
    ".mypy_cache",
    ".ruff_cache",
    ".venv",
    "_out",
    "build",
    "dist",
)


def _git_lines(*args: str) -> list[str]:
    completed = subprocess.run(
        ["git", *args],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return [line.strip() for line in completed.stdout.splitlines() if line.strip()]


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
            root_name = Path(rel_path).parts[0] if Path(rel_path).parts else rel_path
            if any(root_name.startswith(prefix) for prefix in EXCLUDED_ROOT_PREFIXES):
                continue
            abs_path = ROOT / rel_path
            if not abs_path.exists() or abs_path.is_dir():
                continue
            seen.add(rel_path)
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
    completed = subprocess.run(command, cwd=ROOT)
    return completed.returncode


def build_changed_command(files: list[str]) -> list[str]:
    return [*_precommit_base_command(), "--files", *files]


def build_all_commands() -> list[list[str]]:
    return [
        [*_precommit_base_command(), "--all-files"],
        [*_precommit_base_command(), "--hook-stage", "pre-push", "--all-files"],
    ]


def run_changed() -> int:
    files = collect_changed_files()
    if not files:
        print("No changed files found for pre-commit; skipping.")
        return 0
    return _run_precommit_command(build_changed_command(files))


def run_all() -> int:
    for command in build_all_commands():
        return_code = _run_precommit_command(command)
        if return_code != 0:
            return return_code
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Baluffo pre-commit gate")
    parser.add_argument(
        "--mode",
        choices=("changed", "all"),
        default="changed",
        help="Run only changed files or the full repository guardrail set.",
    )
    args = parser.parse_args()

    if args.mode == "all":
        return run_all()
    return run_changed()


if __name__ == "__main__":
    raise SystemExit(main())
