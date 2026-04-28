#!/usr/bin/env python3
"""Run gitleaks against the file list supplied by pre-commit."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / ".gitleaks.toml"


def _scan_file(path: Path) -> int:
    rel_path = path.relative_to(ROOT).as_posix()
    command = [
        "gitleaks",
        "dir",
        "--config",
        str(CONFIG),
        "--redact",
        "--no-banner",
        "--verbose",
        rel_path,
    ]
    completed = subprocess.run(command, cwd=ROOT, check=False)
    return completed.returncode


def main(argv: list[str] | None = None) -> int:
    args = argv if argv is not None else sys.argv[1:]
    files = [ROOT / arg for arg in args if (ROOT / arg).is_file()]
    if not files:
        return 0

    failed = False
    for path in files:
        return_code = _scan_file(path)
        if return_code != 0:
            failed = True
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
