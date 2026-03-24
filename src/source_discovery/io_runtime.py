from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from src.source_registry import (
    DISCOVERY_CANDIDATES_PATH,
    DISCOVERY_REPORT_PATH,
    load_json_array,
    save_json_atomic,
    source_identity,
    unique_sources,
)

from .scoring import careers_keyword_count, clean_token, studio_domain_match


def endpoint_url(candidate: dict[str, Any]) -> str:
    for key in ("api_url", "feed_url", "board_url", "listing_url"):
        raw = str(candidate.get(key) or "").strip()
        if raw:
            return raw
    return ""


def candidate_variant_key(candidate: dict[str, Any]) -> str:
    adapter = str(candidate.get("adapter") or "").strip().lower()
    studio = clean_token(str(candidate.get("studio") or candidate.get("name") or ""))
    careers_url = str(candidate.get("careersUrl") or "").strip().lower()
    if not adapter:
        return ""
    return f"{adapter}:{studio}:{careers_url}"


def collapse_competing_candidates(candidates: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    preferred: dict[str, dict[str, Any]] = {}
    passthrough: list[dict[str, Any]] = []
    for raw in candidates:
        if not isinstance(raw, dict):
            continue
        row = dict(raw)
        if str(row.get("discoveryMethod") or "") != "seed_careers_page":
            passthrough.append(row)
            continue
        key = candidate_variant_key(row)
        if not key:
            passthrough.append(row)
            continue
        current = preferred.get(key)
        if current is None:
            preferred[key] = row
            continue
        current_score = (
            int(current.get("evidenceScore") or 0),
            careers_keyword_count(endpoint_url(current)),
            int(bool(studio_domain_match(str(current.get("studio") or ""), endpoint_url(current)))),
            len(endpoint_url(current)),
        )
        row_score = (
            int(row.get("evidenceScore") or 0),
            careers_keyword_count(endpoint_url(row)),
            int(bool(studio_domain_match(str(row.get("studio") or ""), endpoint_url(row)))),
            len(endpoint_url(row)),
        )
        if row_score > current_score:
            preferred[key] = row
    return unique_sources([*passthrough, *preferred.values()])


def collapse_competing_candidates_by_identity(
    rows: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Merge candidates by source_identity (e.g. for sheet_directory provider list)."""
    seen: dict[str, dict[str, Any]] = {}
    for row in rows:
        identity = source_identity(row)
        if not identity:
            continue
        previous = seen.get(identity)
        if not previous:
            seen[identity] = dict(row)
            continue
        if int(row.get("evidenceScore") or 0) > int(previous.get("evidenceScore") or 0):
            seen[identity] = dict(row)
    return list(seen.values())


def load_existing_candidates() -> list[dict[str, Any]]:
    return load_json_array(DISCOVERY_CANDIDATES_PATH)


def write_discovery_outputs(report: dict[str, Any]) -> None:
    save_json_atomic(DISCOVERY_REPORT_PATH, report)
