"""Shared helpers for the ``tools/measurements/pipeline`` report tools.

``dedup_pressure_report`` and ``latest_run_report`` both resolve the report
roots they search and load a JSON report with the same error contract (missing
file -> ``FileNotFoundError``, malformed JSON -> ``ValueError``). Those two
helpers were byte-identical copies before this module existed.

Import style: callers import the fully qualified
``tools.measurements.pipeline._common`` path, which both already make
importable by putting the repo root on ``sys.path``.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def read_report_json(path: Path) -> Any:
    """Load a JSON report, naming the file in the failure it reports."""
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise FileNotFoundError(f"Missing report file: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in report file: {path}") from exc


def candidate_roots(repo_root: Path) -> list[Path]:
    """Report directories to search, newest run directories last."""
    roots: list[Path] = []
    for candidate in (repo_root / "data", repo_root / "_out" / "latest"):
        if candidate.is_dir():
            roots.append(candidate)
    runs_root = repo_root / "_out" / "runs"
    if runs_root.is_dir():
        roots.extend(path for path in runs_root.iterdir() if path.is_dir())
    return roots
