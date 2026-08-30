"""Source-registry duplicate-careers-URL guardrail.

Repo guardrail that fails when two *active* registry rows resolve to the same
canonicalized careers URL, so www/apex, scheme, trailing-slash and fragment
twins that would double-emit jobs are caught in the tracked seeds before they
reach the published registry.

The canonicalization rule lives in ``src.source_registry_identity`` (the
single authoritative implementation shared with the runtime conflict
automations); this module re-exports it so the commit-time and runtime sides
can never drift apart. Currently-known and reviewed collisions are
grandfathered in an explicit baseline file
(``data/defaults/source-registry-known-url-collisions.json``). Removing an
entry from that file requires the canonical URL to have at most one active
row, so the baseline shrinks as the twins are reconciled and any *new* twin
fails immediately.

Two invariants are enforced together, so pruning and reconciliation stay in
lockstep:

* **No uncovered collision** -- every canonical URL registered by 2+ active
  rows must be in the baseline (`list_active_url_collisions`).
* **No stale baseline entry** -- every baseline entry must still be backed by
  at least two active rows (`list_stale_known_collisions`). When reconciliation
  drops a URL back to a single row the entry must be pruned in the same change;
  keeping it would keep the guardrail permanently green for a normalized
  (now-single) URL and let future drift go unnoticed.

Deliberate scope: the rule keys purely on URL and only looks at **active**
rows. Legitimate same-board, different-studio rows (e.g. two studios on a
shared parent board) collide by design; they belong in the baseline and are
reviewed there. A demoted row lives in pending, not active, so active+pending
overlap is intentionally not treated as a twin.
"""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from src.source_registry_identity import canonicalize_careers_url  # noqa: F401

KNOWN_COLLISIONS_RELATIVE_PATH = Path("data/defaults/source-registry-known-url-collisions.json")


def _row_careers_url(row: dict[str, Any]) -> str:
    return row.get("board_url") or row.get("listing_url") or row.get("url") or ""


def list_active_url_collisions(
    active_rows: Iterable[dict[str, Any]],
    known_urls: Iterable[str] = (),
) -> list[str]:
    """Return failure messages for canonical URLs registered by 2+ active rows.

    Canonical URLs present in ``known_urls`` are treated as reviewed
    collisions and are excluded from the failures.
    """
    known = set(known_urls)
    by_url: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in active_rows:
        canonical = canonicalize_careers_url(_row_careers_url(row))
        if not canonical:
            continue
        by_url[canonical].append(row)

    failures: list[str] = []
    for canonical, rows in sorted(by_url.items()):
        if len(rows) < 2 or canonical in known:
            continue
        ids = ", ".join(row.get("id") or "?" for row in rows)
        failures.append(
            f"{canonical} is registered by {len(rows)} active rows ({ids}). "
            "Keep exactly one active registration; demote the duplicates or add this URL "
            "to data/defaults/source-registry-known-url-collisions.json."
        )
    return failures


def list_stale_known_collisions(
    known_urls: Iterable[str],
    active_rows: Iterable[dict[str, Any]],
) -> list[str]:
    """Return failure messages for baseline entries that are no longer plural.

    A baseline (``known_urls``) entry exists to grandfather a reviewed
    collision between two or more active rows. Once reconciliation drops that
    canonical URL to a single active row (or zero), the entry is stale and must
    be pruned from ``data/defaults/source-registry-known-url-collisions.json``
    in the same change -- otherwise the allowlist would mask a normalized URL
    forever and re-introduced twins would silently pass.

    Only canonical URLs present in ``known_urls`` are considered; a URL that
    was never baselined is not stale.
    """
    if not known_urls:
        return []
    counts: dict[str, int] = defaultdict(int)
    for row in active_rows:
        canonical = canonicalize_careers_url(_row_careers_url(row))
        if canonical and canonical in set(known_urls):
            counts[canonical] += 1
    failures: list[str] = []
    for canonical in sorted(set(known_urls)):
        n = counts.get(canonical, 0)
        if n >= 2:
            continue
        verb = "none" if n == 0 else "only one"
        failures.append(
            f"baseline entry {canonical} is stale: backed by {verb} active seed row (need 2+). "
            "Prune it from data/defaults/source-registry-known-url-collisions.json "
            "in the same change that reconciled the twins."
        )
    return failures


def _load_known_collisions(repo_root: Path) -> set[str]:
    path = repo_root / KNOWN_COLLISIONS_RELATIVE_PATH
    if not path.exists():
        return set()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return set()
    if isinstance(payload, dict):
        return {str(key) for key in payload}
    if isinstance(payload, list):
        return {str(key) for key in payload}
    return set()


def _active_seed_path(repo_root: Path) -> Path:
    return repo_root / "data" / "defaults" / "source-registry-active.seed.json"


def check_active_seed_twin_career_urls(repo_root: Path) -> list[str]:
    """Guardrail entrypoint: fail when two active seed rows share a careers URL."""
    seed_path = _active_seed_path(repo_root)
    rows = _load_active_seed(seed_path)
    if isinstance(rows, list):
        known = _load_known_collisions(repo_root)
        return list_active_url_collisions(rows, known_urls=known)
    return rows


def check_active_seed_stale_baseline(repo_root: Path) -> list[str]:
    """Guardrail entrypoint: fail when any baseline entry is backed by < 2 rows."""
    seed_path = _active_seed_path(repo_root)
    rows = _load_active_seed(seed_path)
    if not isinstance(rows, list):
        return rows
    known = _load_known_collisions(repo_root)
    return list_stale_known_collisions(known, active_rows=rows)


def _load_active_seed(seed_path: Path) -> list[dict[str, Any]] | list[str]:
    """Return the active seed rows, or a list of failure messages on error."""
    if not seed_path.exists():
        return [f"active seed not found: {seed_path}"]
    try:
        rows = json.loads(seed_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return [f"could not read active seed {seed_path}: {exc}"]
    if not isinstance(rows, list):
        return [f"active seed {seed_path} is not a JSON array"]
    return rows
