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
# The container image used to default these variables to the literal "unknown"
# when no build arg was supplied. Treated as absent here so an unstamped build
# reports empty strings (the honest answer) instead of a placeholder that release
# verification could mistake for a real commit.
_UNSET_VALUES = frozenset({"unknown"})


def _clean_stamp(value: Any) -> str:
    text = str(value or "").strip()
    return "" if text.lower() in _UNSET_VALUES else text


def _candidate_paths() -> tuple[Path, ...]:
    here = Path(__file__).resolve()
    # Source checkout: <root>/src/build_info.py -> <root>/build-info.json is not
    # written; packaged: <version>/src/build_info.py -> <version>/build-info.json.
    return (here.parents[1] / "build-info.json", here.parent / "build-info.json")


def read_build_info() -> dict[str, str]:
    commit = _clean_stamp(os.environ.get(BUILD_COMMIT_ENV))
    built_at = _clean_stamp(os.environ.get(BUILD_BUILT_AT_ENV))
    if commit and built_at:
        return {"commit": commit, "builtAt": built_at}
    for path in _candidate_paths():
        try:
            payload: Any = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if not isinstance(payload, dict):
            continue
        commit = commit or _clean_stamp(payload.get("commit"))
        built_at = built_at or _clean_stamp(payload.get("builtAt"))
        break
    return {"commit": commit, "builtAt": built_at}
