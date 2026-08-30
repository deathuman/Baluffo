"""Shared leaf: reviewed-collision allowlist data reads.

Single authoritative loader for the ``source-registry-known-url-collisions.json``
baseline, consumed by both the runtime twin rule (``source_registry_policy``) and
the commit-time guardrail (``tools/repo_health/source_registry_duplicate_url_policy``)
so the allowlist can never be parsed or normalized differently by the two sides.

Two entrypoints with deliberately different failure modes:

* :func:`load_known_collision_urls` -- the guardrail view: missing, unreadable, or
  shape-mismatched files yield an **empty set** (no allowlist means every twin is
  uncovered and the gate fails loudly).
* :func:`known_twin_career_urls` -- the runtime view: missing, unreadable, or
  shape-mismatched files yield ``None``, and callers must **skip URL-twin
  automation entirely** rather than risk auto-demoting a reviewed collision.

AI boundary owns: known-collision baseline file location and parsing shared by the
runtime twin rule and the repo-health guardrail.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.source_registry_io import DEFAULTS_DIR

KNOWN_TWIN_URLS_FILENAME = "source-registry-known-url-collisions.json"
KNOWN_TWIN_URLS_DEFAULT_RELATIVE_PATH = Path("data/defaults") / KNOWN_TWIN_URLS_FILENAME


def _known_collision_set(payload: Any) -> set[str]:
    """Normalize a reviewed-collision payload into its canonical URL set.

    Both supported payload shapes are accepted: a mapping keyed by canonical URL
    (``{"<canonical>": <review-notes>}``) and a plain list (``["<canonical>", ...]``).
    Anything else yields an empty set.
    """
    if isinstance(payload, dict):
        return {str(key) for key in payload}
    if isinstance(payload, list):
        return {str(key) for key in payload}
    return set()


def load_known_collision_urls(path: Path) -> set[str]:
    """Parse the reviewed-collision allowlist file into a set of canonical URLs.

    Missing, unreadable, or shape-mismatched files yield an empty set -- the
    conservative guardrail view: with no allowlist every twin is uncovered and the
    gate reports it.
    """
    if not path.exists():
        return set()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return set()
    return _known_collision_set(payload)


def known_twin_career_urls() -> set[str] | None:
    """Runtime loader: the reviewed-collision allowlist, or ``None`` when unavailable.

    ``None`` means the baseline could not be loaded (missing, unreadable, or not a
    mapping/list payload), and callers must skip URL-twin automation entirely rather
    than risk auto-demoting a reviewed collision. A valid (even empty) mapping/list
    payload returns its URL set.

    The path resolves through the storage layer at call time (``DEFAULTS_DIR`` is
    rebound at runtime by ``source_registry._sync_io_paths``), so this works in dev
    and in the bundled app where the baseline ships under ``data/``.
    """
    path = DEFAULTS_DIR / KNOWN_TWIN_URLS_FILENAME
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, (dict, list)):
        return None
    return _known_collision_set(payload)
