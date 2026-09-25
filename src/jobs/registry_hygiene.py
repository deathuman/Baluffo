"""Advisory registry-hygiene audit for configured source rows.

The monitor is deliberately read-only. It reports bounded evidence for the
registry defects that previously required one-off hunts: asset pages in a
configured window, canonical-URL duplicate candidates, unreachable evidence
from the previous source state, and host drift between a row's identity URL and
its configured fetch URLs. It never demotes, deletes, or rewrites a row.
"""

from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Iterable
from typing import Any
from urllib.parse import urlparse

from src.jobs.page_gating import looks_like_asset_url
from src.jobs.text_utils import clean_text
from src.source_registry_identity import canonicalize_careers_url

REGISTRY_HYGIENE_SOURCE_LIMIT = 20
REGISTRY_HYGIENE_SAMPLE_LIMIT = 3
REGISTRY_HYGIENE_FLAGS = (
    "asset_pages",
    "duplicate_candidate",
    "host_drift_candidate",
    "unreachable_page",
)
_STATIC_LISTING_URL_RE = re.compile(
    r"^(?:static_source::)?static:listing_url:(?P<url>https?://\S+)$", re.IGNORECASE
)
_PERMANENT_ERROR_PATTERNS = (
    ("http_404", re.compile(r"(?:http|status[^\d])\s*404|\b404\b", re.IGNORECASE)),
    ("not_found", re.compile(r"\bnot found\b|\bno such host\b", re.IGNORECASE)),
    (
        "dns_or_tls",
        re.compile(r"getaddrinfo|name or service not known|certificate|ssl|tls", re.IGNORECASE),
    ),
    (
        "connection",
        re.compile(r"connection refused|network is unreachable|host not found", re.IGNORECASE),
    ),
)


def _rows(value: Any) -> list[dict[str, Any]]:
    try:
        values = list(value)
    except TypeError:
        return []
    return [row for row in values if isinstance(row, dict)]


def _dedupe(values: Iterable[str]) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = clean_text(value)
        if text and text not in seen:
            seen.add(text)
            output.append(text)
    return output


def _host(value: Any) -> str:
    try:
        return (urlparse(clean_text(value)).hostname or "").lower()
    except ValueError:
        return ""


def _configured_urls(row: dict[str, Any]) -> list[str]:
    values: list[str] = []
    for field in ("pages", "listing_url", "listingUrl", "careersUrl", "currentUrl"):
        raw = row.get(field)
        if isinstance(raw, list):
            values.extend(str(item or "") for item in raw)
        elif isinstance(raw, str):
            values.append(raw)
    return _dedupe(
        value for value in values if clean_text(value).startswith(("http://", "https://"))
    )


def _identity_url(row: dict[str, Any]) -> str:
    source_id = clean_text(row.get("id"))
    match = _STATIC_LISTING_URL_RE.match(source_id)
    if match:
        return match.group("url")
    for field in ("listing_url", "listingUrl", "careersUrl", "currentUrl", "board_url", "url"):
        value = clean_text(row.get(field))
        if value.startswith(("http://", "https://")):
            return value
    return ""


def _canonical_url(row: dict[str, Any]) -> str:
    for field in ("board_url", "listing_url", "listingUrl", "url"):
        value = clean_text(row.get(field))
        if not value:
            continue
        try:
            return canonicalize_careers_url(value)
        except (TypeError, ValueError):
            continue
    return ""


def _state_for_row(row: dict[str, Any], state_rows: Any) -> dict[str, Any]:
    """Resolve a registry row to its source-state entry.

    Static rows are keyed by id (optionally with the ``static_source::``
    loader prefix), while provider rows are keyed by their human-readable
    registry ``name`` (for example ``workable:account:hutch`` →
    ``Hutch (Workable)``). Both forms are tried so reachability evidence is
    attributed to the row that actually failed.
    """
    states = state_rows if isinstance(state_rows, dict) else {}
    source_id = clean_text(row.get("id"))
    for key in (
        source_id,
        f"static_source::{source_id}",
        source_id.removeprefix("static_source::"),
        clean_text(row.get("name")),
    ):
        if not key:
            continue
        value = states.get(key)
        if isinstance(value, dict):
            return value
    return {}


def _unreachable_evidence(state: dict[str, Any]) -> str:
    if clean_text(state.get("lastStatus")).lower() != "error":
        return ""
    try:
        failures = int(state.get("consecutiveFailures") or 0)
    except (TypeError, ValueError):
        failures = 0
    if failures < 2:
        return ""
    error = clean_text(state.get("lastError") or state.get("error"))
    try:
        http_status = int(state.get("lastHttpStatus") or 0)
    except (TypeError, ValueError):
        http_status = 0
    if http_status in {404, 410}:
        return f"http_{http_status}"
    for name, pattern in _PERMANENT_ERROR_PATTERNS:
        if pattern.search(error):
            return name
    return ""


def _sample(values: list[str]) -> list[str]:
    return values[:REGISTRY_HYGIENE_SAMPLE_LIMIT]


def registry_hygiene_audit(
    rows: Any,
    *,
    source_state_rows: Any = None,
    known_collision_urls: Any = None,
) -> dict[str, Any]:
    """Return a bounded advisory hygiene report for registry rows.

    Counts remain exact even when the flagged-source and sample lists are
    capped. All findings are candidates for review; this function performs no
    registry mutation and treats host drift as advisory because provider and
    redirect relationships can legitimately cross hosts.

    ``known_collision_urls`` is the reviewed-collision allowlist from
    ``source-registry-known-url-collisions.json``. When supplied, duplicate
    groups are split by ``knownCollisionGroupCount`` (already reviewed and
    grandfathered) and ``uncoveredDuplicateGroupCount`` (new drift). When it is
    ``None`` or empty, every duplicate is reported as uncovered so drift is never
    silently hidden by a baseline that failed to load.
    """
    known: set[str] = set()
    if isinstance(known_collision_urls, (set, frozenset, list, tuple)):
        known = {url for url in (clean_text(item) for item in known_collision_urls) if url}
    source_rows = _rows(rows)
    duplicate_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    findings: dict[str, dict[str, Any]] = {}
    for row in source_rows:
        source_id = clean_text(row.get("id"))
        if not source_id:
            continue
        urls = _configured_urls(row)
        entry = findings.setdefault(
            source_id,
            {
                "sourceId": source_id,
                "registryState": clean_text(row.get("registryState")),
                "flags": set(),
                "assetPageCount": 0,
                "sampleAssetPages": [],
                "duplicateGroupCount": 0,
                "uncoveredDuplicateGroupCount": 0,
                "sampleDuplicateUrls": [],
                "unreachableEvidence": "",
                "sampleUnreachablePages": [],
                "hostDriftCount": 0,
                "sampleHostDriftUrls": [],
            },
        )
        assets = [url for url in urls if looks_like_asset_url(url)]
        if assets:
            entry["flags"].add("asset_pages")
            entry["assetPageCount"] = len(assets)
            entry["sampleAssetPages"] = _sample(assets)
        canonical = _canonical_url(row)
        if canonical:
            duplicate_groups[canonical].append(row)
        identity_host = _host(_identity_url(row))
        if identity_host:
            foreign = [url for url in urls if _host(url) and _host(url) != identity_host]
            if foreign:
                entry["flags"].add("host_drift_candidate")
                entry["hostDriftCount"] = len(foreign)
                entry["sampleHostDriftUrls"] = _sample(foreign)
        evidence = _unreachable_evidence(_state_for_row(row, source_state_rows))
        if evidence:
            entry["flags"].add("unreachable_page")
            entry["unreachableEvidence"] = evidence
            # Provider rows often carry no careers page, so fall back to the API
            # endpoint that actually failed rather than reporting an empty sample.
            entry["sampleUnreachablePages"] = _sample(
                _dedupe([*urls, clean_text(row.get("api_url")), _identity_url(row)])
            )

    duplicate_row_count = 0
    duplicate_group_count = 0
    known_group_count = 0
    uncovered_group_count = 0
    uncovered_row_count = 0
    for canonical, group in duplicate_groups.items():
        if len(group) < 2:
            continue
        duplicate_group_count += 1
        duplicate_row_count += len(group)
        reviewed = canonical in known
        if reviewed:
            known_group_count += 1
        else:
            uncovered_group_count += 1
            uncovered_row_count += len(group)
        for row in group:
            source_id = clean_text(row.get("id"))
            candidate = findings.get(source_id)
            if candidate is None:
                continue
            candidate["flags"].add("duplicate_candidate")
            candidate["duplicateGroupCount"] += 1
            if not reviewed:
                candidate["uncoveredDuplicateGroupCount"] += 1
            candidate["sampleDuplicateUrls"] = _sample(
                [*candidate["sampleDuplicateUrls"], canonical]
            )

    ordered = []
    for source_id in sorted(findings):
        entry = findings[source_id]
        flags = [flag for flag in REGISTRY_HYGIENE_FLAGS if flag in entry["flags"]]
        if not flags:
            continue
        ordered.append(
            {
                "sourceId": entry["sourceId"],
                "registryState": entry["registryState"],
                "flags": flags,
                "assetPageCount": entry["assetPageCount"],
                "sampleAssetPages": entry["sampleAssetPages"],
                "duplicateGroupCount": entry["duplicateGroupCount"],
                "uncoveredDuplicateGroupCount": entry["uncoveredDuplicateGroupCount"],
                "sampleDuplicateUrls": entry["sampleDuplicateUrls"],
                "unreachableEvidence": entry["unreachableEvidence"],
                "sampleUnreachablePages": entry["sampleUnreachablePages"],
                "hostDriftCount": entry["hostDriftCount"],
                "sampleHostDriftUrls": entry["sampleHostDriftUrls"],
            }
        )
    return {
        "sourceCount": len(ordered),
        "assetPageCount": sum(item["assetPageCount"] for item in ordered),
        "duplicateGroupCount": duplicate_group_count,
        "duplicateRowCount": duplicate_row_count,
        "knownCollisionGroupCount": known_group_count,
        "uncoveredDuplicateGroupCount": uncovered_group_count,
        "uncoveredDuplicateRowCount": uncovered_row_count,
        "unreachablePageCount": sum("unreachable_page" in item["flags"] for item in ordered),
        "hostDriftCount": sum("host_drift_candidate" in item["flags"] for item in ordered),
        "sources": ordered[:REGISTRY_HYGIENE_SOURCE_LIMIT],
    }


__all__ = [
    "REGISTRY_HYGIENE_FLAGS",
    "REGISTRY_HYGIENE_SAMPLE_LIMIT",
    "REGISTRY_HYGIENE_SOURCE_LIMIT",
    "registry_hygiene_audit",
]
