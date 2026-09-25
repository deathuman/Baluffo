"""Source-registry structural guardrails.

Repo guardrails that fail when the tracked registry seeds contain a structural
defect which cannot be a legitimate configuration. Five checks, split by
whether a reviewed exception is meaningful:

* **No uncovered duplicate careers URL** -- two *active* rows resolving to the
  same canonicalized careers URL, so www/apex, scheme, trailing-slash and
  fragment twins that would double-emit jobs are caught before they reach the
  published registry (`list_active_url_collisions`).
* **No stale baseline entry** -- every reviewed-collision entry must still be
  backed by at least two active rows (`list_stale_known_collisions`).
* **No definition-less static row** -- a static row with neither ``listing_url``
  nor a non-empty ``pages`` list fetches nothing while reporting ok
  (`list_definitionless_static_rows`).
* **No inline asset URLs** -- a ``data:`` URI inside a page list is page
  content, not a fetchable page (`list_rows_with_inline_asset_urls`).
* **No duplicate registry ids** -- one id claimed by several rows
  (`list_duplicate_source_ids`).
* **No malformed page references** -- any page entry that is not an absolute
  http(s) URL with a host (`list_rows_with_malformed_page_refs`).

The canonicalization rule lives in ``src.source_registry_identity`` (the
single authoritative implementation shared with the runtime conflict
automations); this module re-exports it so the commit-time and runtime sides
can never drift apart. Currently-known and reviewed collisions are
grandfathered in an explicit baseline file
(``data/defaults/source-registry-known-url-collisions.json``). Removing an
entry from that file requires the canonical URL to have at most one active
row, so the baseline shrinks as the twins are reconciled and any *new* twin
fails immediately.

Two invariants are enforced together for that baseline, so pruning and
reconciliation stay in lockstep:

* **No uncovered collision** -- every canonical URL registered by 2+ active
  rows must be in the baseline (`list_active_url_collisions`).
* **No stale baseline entry** -- every baseline entry must still be backed by
  at least two active rows (`list_stale_known_collisions`). When reconciliation
  drops a URL back to a single row the entry must be pruned in the same change;
  keeping it would keep the guardrail permanently green for a normalized
  (now-single) URL and let future drift go unnoticed.

Deliberate scope: the URL rule keys purely on URL and only looks at **active**
rows. Legitimate same-board, different-studio rows (e.g. two studios on a
shared parent board) collide by design; they belong in the baseline and are
reviewed there. A demoted row lives in pending, not active, so active+pending
overlap is intentionally not treated as a twin. The other checks have no
allowlist by design: a duplicate id or an unfetchable page is a keying or
fetchability defect, never a reviewed-and-accepted configuration.

Scope limit worth stating plainly: these checks read the **committed seeds**
(``data/defaults/*.seed.json``). The live registry that the pipeline actually
fetches can drift from them, so a green run here is not proof the live
registry is clean. The advisory ``registryHygieneAudit`` runtime block exists
to cover that live surface.
"""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from src.source_registry_data import (
    KNOWN_TWIN_URLS_DEFAULT_RELATIVE_PATH,
    load_known_collision_urls,
)
from src.source_registry_identity import canonicalize_careers_url  # noqa: F401

KNOWN_COLLISIONS_RELATIVE_PATH = KNOWN_TWIN_URLS_DEFAULT_RELATIVE_PATH


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
    """Reviewed-collision allowlist for the repo seeds (empty when unavailable).

    Delegates to the shared leaf loader so the commit-time guardrail and the
    runtime twin rule can never parse the baseline differently.
    """
    return load_known_collision_urls(repo_root / KNOWN_COLLISIONS_RELATIVE_PATH)


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


def list_definitionless_static_rows(rows: Iterable[dict[str, Any]]) -> list[str]:
    """Return static seed rows that would fetch nothing.

    A static registry row needs a definition: at least one of ``listing_url``
    or a non-empty ``pages`` list (``careersUrl`` alone is advisory metadata —
    the pipeline's page set comes from ``pages``/``listing_url``). A row whose
    id was renamed without its definition (the batch-3 lean-registry trap:
    direct ``.json.gz`` writes bypassing ``save_json_atomic``) loads with zero
    pages and the pipeline silently fetches nothing while reporting status ok.
    """
    failures: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        row_id = str(row.get("id") or "").strip()
        if not row_id.startswith("static:"):
            continue
        listing_url = str(row.get("listing_url") or "").strip()
        pages = row.get("pages")
        pages_ok = isinstance(pages, list) and any(str(p or "").strip() for p in pages)
        if listing_url or pages_ok:
            continue
        failures.append(
            f"{row_id} has no listing_url and no pages — definition-less static row "
            f"(fetches nothing; likely an id rename that bypassed save_json_atomic)"
        )
    return failures


def check_active_seed_definitions(repo_root: Path) -> list[str]:
    """Guardrail entrypoint: fail when an active seed static row is definition-less."""
    seed_path = _active_seed_path(repo_root)
    rows = _load_active_seed(seed_path)
    if isinstance(rows, list):
        return list_definitionless_static_rows(rows)
    return rows


def list_rows_with_inline_asset_urls(rows: Iterable[dict[str, Any]]) -> list[str]:
    """Return active seed rows whose page lists carry non-http(s) URIs.

    Discovery probes must only ever harvest http(s) detail links. A
    ``data:image/...`` href inside ``pages``/``detailPagesSample`` (the CarX
    Technologies 2026-09-07 contamination: a 6.5 MB base64 logo whose base64
    path noise matched the detail-path regex) is fetched as a listing page
    every pipeline pass and fails with ``URL too long``, failing the whole
    source. Inline-asset URIs are page content, not pages.
    """
    failures: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        row_id = str(row.get("id") or "").strip()
        offending: list[str] = []
        for field in ("pages", "detailPagesSample"):
            values = row.get(field)
            if not isinstance(values, list):
                continue
            for value in values:
                text = str(value or "").strip()
                if text.lower().startswith("data:"):
                    offending.append(f"{field}[{text[:24]}...]")
        if offending:
            failures.append(
                f"{row_id} carries inline-asset URI(s) in its page lists "
                f"({'; '.join(offending[:3])}) — data:/blob: URIs are page content, "
                "not fetchable pages; purge them from the row"
            )
    return failures


def check_active_seed_no_inline_asset_urls(repo_root: Path) -> list[str]:
    """Guardrail entrypoint: fail when an active seed row embeds data: URIs."""
    seed_path = _active_seed_path(repo_root)
    rows = _load_active_seed(seed_path)
    if isinstance(rows, list):
        return list_rows_with_inline_asset_urls(rows)
    return rows


def check_active_seed_stale_baseline(repo_root: Path) -> list[str]:
    """Guardrail entrypoint: fail when any baseline entry is backed by < 2 rows."""
    seed_path = _active_seed_path(repo_root)
    rows = _load_active_seed(seed_path)
    if not isinstance(rows, list):
        return rows
    known = _load_known_collisions(repo_root)
    return list_stale_known_collisions(known, active_rows=rows)


def list_duplicate_source_ids(rows: Iterable[dict[str, Any]]) -> list[str]:
    """Return failure messages for registry ids claimed by more than one row.

    The id is the registry's primary key: it keys source state, tombstones,
    saved suppression evidence, and every operator-facing row reference. Two
    rows sharing an id make all of those lookups ambiguous, so one row silently
    overwrites the other's state and a demote or approve applies to whichever
    copy happened to be read. This is a pure keying defect with no legitimate
    configuration -- unlike a shared careers URL, it can never be baselined
    away -- so it is a hard failure with no allowlist.
    """
    counts: dict[str, int] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        row_id = str(row.get("id") or "").strip()
        if not row_id:
            continue
        counts[row_id] = counts.get(row_id, 0) + 1
    return [
        f"registry id {row_id!r} is claimed by {count} active rows "
        f"(ids must be unique: they key source state, tombstones, and suppression evidence)"
        for row_id, count in sorted(counts.items())
        if count > 1
    ]


def check_active_seed_duplicate_ids(repo_root: Path) -> list[str]:
    """Guardrail entrypoint: fail when two active seed rows share a registry id."""
    seed_path = _active_seed_path(repo_root)
    rows = _load_active_seed(seed_path)
    if isinstance(rows, list):
        return list_duplicate_source_ids(rows)
    return rows


def list_rows_with_malformed_page_refs(rows: Iterable[dict[str, Any]]) -> list[str]:
    """Return rows whose page lists carry unfetchable references.

    A configured page must be an absolute ``http(s)`` URL with a host.
    Anything else -- an empty entry, a relative path, a ``data:``/``blob:``/
    ``javascript:`` URI, or a scheme-relative or hostless ``https:///...`` URL
    -- cannot be fetched and makes the row silently contribute nothing on every
    pass. This generalizes the inline-asset rule (which only matches ``data:``)
    to the whole class of malformed page references, and unlike the inline-asset
    check it is not limited to any single field.
    """
    failures: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        row_id = str(row.get("id") or "").strip()
        offending: list[str] = []
        for field in ("pages", "detailPagesSample"):
            values = row.get(field)
            if not isinstance(values, list):
                continue
            for value in values:
                text = str(value or "").strip()
                if not text:
                    offending.append(f"{field}[<empty>]")
                    continue
                if not text.lower().startswith(("http://", "https://")):
                    offending.append(f"{field}[{text[:32]}]")
                    continue
                try:
                    if not urlparse(text).hostname:
                        offending.append(f"{field}[{text[:32]}]")
                except ValueError:
                    offending.append(f"{field}[{text[:32]}]")
        if offending:
            failures.append(
                f"{row_id} carries malformed page reference(s) "
                f"({'; '.join(offending[:3])}) — every configured page must be an "
                "absolute http(s) URL with a host; relative, inline, and hostless "
                "entries fetch nothing"
            )
    return failures


def check_active_seed_no_malformed_page_refs(repo_root: Path) -> list[str]:
    """Guardrail entrypoint: fail when an active seed row has unfetchable pages."""
    seed_path = _active_seed_path(repo_root)
    rows = _load_active_seed(seed_path)
    if isinstance(rows, list):
        return list_rows_with_malformed_page_refs(rows)
    return rows


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
