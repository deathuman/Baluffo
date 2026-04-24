from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
HOOKS_PATH = ".githooks"


def _is_ci_environment() -> bool:
    return os.getenv("CI", "").strip().lower() == "true"


def _verify_mypy_available() -> int:
    if _is_ci_environment():
        return 0

    completed = subprocess.run(
        [sys.executable, "-m", "mypy", "--version"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode == 0:
        return 0

    details = completed.stderr.strip() or completed.stdout.strip() or "no output"
    print("ERROR: python -m mypy --version failed in the active interpreter.", file=sys.stderr)
    print(f"Active interpreter: {sys.executable}", file=sys.stderr)
    print(f"Details: {details}", file=sys.stderr)
    print("Install mypy in this interpreter before setting up hooks.", file=sys.stderr)
    print(
        "After switching Python interpreters, run `npm run lint:precommit:changed` before starting refactor work.",
        file=sys.stderr,
    )
    return 1


def main() -> int:
    mypy_status = _verify_mypy_available()
    if mypy_status != 0:
        return mypy_status
    completed = subprocess.run(
        ["git", "config", "--local", "core.hooksPath", HOOKS_PATH],
        cwd=ROOT,
        check=False,
    )
    if completed.returncode != 0:
        return completed.returncode
    print(f"Configured git core.hooksPath to {HOOKS_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
