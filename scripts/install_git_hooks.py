from __future__ import annotations

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
HOOKS_PATH = ".githooks"


def main() -> int:
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
