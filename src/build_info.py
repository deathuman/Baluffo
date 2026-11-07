"""Build identity for the running app: the commit and time the artifact was built.

The packaged bundle writes ``build-info.json`` beside ``current.txt`` and in the
version directory. Container builds supply the same values as environment
variables because the image has no git history. A development checkout answers
with empty strings rather than guessing, so a missing stamp is visible.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

BUILD_COMMIT_ENV = "BALUFFO_BUILD_COMMIT"
BUILD_BUILT_AT_ENV = "BALUFFO_BUILT_AT"


def _candidate_paths() -> tuple[Path, ...]:
    here = Path(__file__).resolve()
    # Source checkout: <root>/src/build_info.py -> <root>/build-info.json is not
    # written; packaged: <version>/src/build_info.py -> <version>/build-info.json.
    return (here.parents[1] / "build-info.json", here.parent / "build-info.json")


def read_build_info() -> dict[str, str]:
    commit = str(os.environ.get(BUILD_COMMIT_ENV) or "").strip()
    built_at = str(os.environ.get(BUILD_BUILT_AT_ENV) or "").strip()
    if commit and built_at:
        return {"commit": commit, "builtAt": built_at}
    for path in _candidate_paths():
        try:
            payload: Any = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if not isinstance(payload, dict):
            continue
        commit = commit or str(payload.get("commit") or "").strip()
        built_at = built_at or str(payload.get("builtAt") or "").strip()
        break
    return {"commit": commit, "builtAt": built_at}
