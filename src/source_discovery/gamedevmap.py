from __future__ import annotations

"""GameDevMap directory parsing and candidate extraction.

Responsibilities:
- Fetch and parse the public GameDevMap CSV feed into normalized rows
- Collapse repeated exact URLs into representative rows with provenance
- Emit direct provider candidates when the feed URL already points at a supported ATS
- Spend a bounded homepage-fetch budget on unresolved rows for provider/static inference
"""

import csv
import io
from typing import Any
from urllib.parse import urlencode

from src.source_registry import normalize_source_url

from .config import DEFAULT_DISCOVERY_CONFIG
from .reporting import emit_log
from .scoring import unique_string_list
from .web_search import (
    fetch_text,
    is_blocked_generic_static_url,
)

GAMEDEVMAP_CSV_URL = "https://www.gamedevmap.com/cmsdata/gamedevmapdata.csv"
GAMEDEVMAP_INDEX_URL = "https://www.gamedevmap.com/index.php"
CORE_EMPLOYER_CATEGORIES = ("Developer", "Developer and Publisher", "Publisher")


def _gamedevmap_config_value(config: dict[str, Any] | None, key: str, default: Any) -> Any:
    source = config if isinstance(config, dict) else {}
    gamedevmap_cfg = source.get("gamedevmap")
    if isinstance(gamedevmap_cfg, dict):
        if key == "gamedevmap":
            return gamedevmap_cfg
        return gamedevmap_cfg.get(key, default)
    return default


def _gamedevmap_enabled(config: dict[str, Any] | None) -> bool:
    return bool(_gamedevmap_config_value(config, "enabled", False))


def _gamedevmap_cache_signature(cfg: dict[str, Any]) -> dict[str, Any]:
    return {
        "csvUrl": str(cfg.get("csvUrl") or "").strip(),
        "indexUrl": str(cfg.get("indexUrl") or "").strip(),
        "promoteValidatedStatic": bool(cfg.get("promoteValidatedStatic", True)),
        "activeAuditBatchSize": max(1, int(cfg.get("activeAuditBatchSize") or 1000)),
        "activeAuditHomepageFetchConcurrency": max(
            0, int(cfg.get("activeAuditHomepageFetchConcurrency") or 0)
        ),
        "activeAuditRecoveryFetchConcurrency": max(
            0, int(cfg.get("activeAuditRecoveryFetchConcurrency") or 0)
        ),
        "activeAuditRecoveryPerHostConcurrency": max(
            0, int(cfg.get("activeAuditRecoveryPerHostConcurrency") or 0)
        ),
        "activeAuditRecoveryTimeoutSeconds": max(
            0, int(cfg.get("activeAuditRecoveryTimeoutSeconds") or 0)
        ),
        "maxRows": max(0, int(cfg.get("maxRows") or 0)),
        "maxHomepageFetches": max(0, int(cfg.get("maxHomepageFetches") or 0)),
        "allowedCategories": list(cfg.get("allowedCategories") or []),
        "blockedCategories": list(cfg.get("blockedCategories") or []),
        "requireAiReviewed": bool(cfg.get("requireAiReviewed", False)),
    }


def _clean_csv_value(value: Any) -> str:
    return str(value or "").strip()


def _gamedevmap_ai_reviewed(value: Any) -> bool:
    return bool(_clean_csv_value(value))


def _gamedevmap_category_priority(category: str) -> int:
    token = _clean_csv_value(category)
    if token in CORE_EMPLOYER_CATEGORIES:
        return 0
    if token:
        return 1
    return 2


def _compose_location(*parts: str) -> str:
    ordered: list[str] = []
    seen: set[str] = set()
    for raw in parts:
        text = _clean_csv_value(raw)
        if not text:
            continue
        lowered = text.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        ordered.append(text)
    return ", ".join(ordered)


def _representative_sort_key(row: dict[str, Any]) -> tuple[int, int, int, str]:
    return (
        0 if bool(row.get("aiReviewed")) else 1,
        _gamedevmap_category_priority(str(row.get("category") or "")),
        max(0, int(row.get("duplicateCount") or 0)),
        str(row.get("studio") or "").strip().lower(),
    )


def parse_gamedevmap_csv(csv_text: str) -> list[dict[str, str]]:
    text = str(csv_text or "").lstrip("\ufeff")
    if not text.strip():
        return []
    reader = csv.DictReader(io.StringIO(text))
    rows: list[dict[str, str]] = []
    for raw_row in reader:
        if not isinstance(raw_row, dict):
            continue
        studio = _clean_csv_value(raw_row.get("Organization"))
        url = _clean_csv_value(raw_row.get("URL"))
        if not studio or not url:
            continue
        rows.append(
            {
                "studio": studio,
                "url": url,
                "city": _clean_csv_value(raw_row.get("City")),
                "state": _clean_csv_value(raw_row.get("State/Province")),
                "country": _clean_csv_value(raw_row.get("Country/Region")),
                "category": _clean_csv_value(raw_row.get("Category")),
                "comments": _clean_csv_value(raw_row.get("Comments")),
                "aiResponse": _clean_csv_value(raw_row.get("AI Response")),
            }
        )
    return rows


def build_gamedevmap_search_url(index_url: str, row: dict[str, Any]) -> str:
    base_url = str(index_url or GAMEDEVMAP_INDEX_URL).strip() or GAMEDEVMAP_INDEX_URL
    params: list[tuple[str, str]] = [
        ("query", str(row.get("studio") or "").strip()),
        ("exact", "1"),
    ]
    for field_name, query_key in (
        ("category", "type"),
        ("country", "country"),
        ("state", "state"),
        ("city", "city"),
    ):
        value = str(row.get(field_name) or "").strip()
        if value:
            params.append((query_key, value))
    encoded = urlencode(params)
    separator = "&" if "?" in base_url else "?"
    return f"{base_url}{separator}{encoded}"


def select_gamedevmap_representative_rows(
    rows: list[dict[str, Any]],
    *,
    allowed_categories: list[str],
    blocked_categories: list[str],
    require_ai_reviewed: bool = False,
    index_url: str = GAMEDEVMAP_INDEX_URL,
) -> list[dict[str, Any]]:
    allowed = {str(item).strip() for item in allowed_categories if str(item).strip()}
    blocked = {str(item).strip() for item in blocked_categories if str(item).strip()}
    grouped: dict[str, list[dict[str, Any]]] = {}

    for raw in rows:
        if not isinstance(raw, dict):
            continue
        studio = str(raw.get("studio") or "").strip()
        category = str(raw.get("category") or "").strip()
        ai_response = str(raw.get("aiResponse") or "").strip()
        normalized_url = normalize_source_url(str(raw.get("url") or ""))
        if not studio or not normalized_url:
            continue
        if blocked and category in blocked:
            continue
        if allowed and category not in allowed:
            continue
        if require_ai_reviewed and not ai_response:
            continue
        if is_blocked_generic_static_url(normalized_url):
            continue
        row = dict(raw)
        row["url"] = normalized_url
        grouped.setdefault(normalized_url, []).append(row)

    representatives: list[dict[str, Any]] = []
    for normalized_url, group_rows in grouped.items():
        ranked_group = sorted(
            group_rows,
            key=lambda row: (
                0 if _gamedevmap_ai_reviewed(row.get("aiResponse")) else 1,
                _gamedevmap_category_priority(str(row.get("category") or "")),
                str(row.get("studio") or "").strip().lower(),
                str(row.get("country") or "").strip().lower(),
                str(row.get("city") or "").strip().lower(),
            ),
        )
        representative = dict(ranked_group[0])
        representative["url"] = normalized_url
        representative["categories"] = unique_string_list(
            [
                str(item.get("category") or "").strip()
                for item in group_rows
                if str(item.get("category") or "").strip()
            ]
        )
        representative["duplicateCount"] = len(group_rows)
        representative["aiReviewed"] = _gamedevmap_ai_reviewed(representative.get("aiResponse"))
        representative["location"] = _compose_location(
            str(representative.get("city") or ""),
            str(representative.get("state") or ""),
            str(representative.get("country") or ""),
        )
        representative["sourceDirectoryEntryUrl"] = build_gamedevmap_search_url(
            index_url, representative
        )
        representatives.append(representative)

    return sorted(representatives, key=_representative_sort_key)


def _apply_gamedevmap_provenance(
    candidate: dict[str, Any],
    row: dict[str, Any],
    *,
    index_url: str,
    include_homepage_fetch: bool = False,
    include_direct_url: bool = False,
) -> dict[str, Any]:
    enriched = dict(candidate)
    evidence_types = list(enriched.get("evidenceTypes") or [])
    evidence_types.append("gamedevmap_directory")
    if row.get("categories"):
        evidence_types.append("gamedevmap_category")
    if bool(row.get("aiReviewed")):
        evidence_types.append("gamedevmap_ai_reviewed")
    if include_homepage_fetch:
        evidence_types.append("gamedevmap_homepage_fetch")
    if include_direct_url:
        evidence_types.append("gamedevmap_direct_url")
    enriched["evidenceTypes"] = unique_string_list(evidence_types)
    enriched["evidenceSource"] = "gamedevmap"
    enriched["discoveryMethod"] = "gamedevmap"
    enriched["sourceDirectory"] = "gamedevmap"
    enriched["sourceDirectoryUrl"] = index_url
    enriched["sourceDirectoryEntryUrl"] = str(row.get("sourceDirectoryEntryUrl") or "").strip()
    enriched["sourceDirectoryCategories"] = unique_string_list(row.get("categories") or [])
    enriched["sourceDirectoryLocation"] = str(row.get("location") or "").strip()
    enriched["sourceDirectoryDuplicateCount"] = max(0, int(row.get("duplicateCount") or 0))
    enriched["sourceDirectoryCity"] = str(row.get("city") or "").strip()
    enriched["sourceDirectoryState"] = str(row.get("state") or "").strip()
    enriched["sourceDirectoryCountry"] = str(row.get("country") or "").strip()
    enriched["sourceDirectoryAiResponse"] = str(row.get("aiResponse") or "").strip()
    enriched["sourceDirectoryComments"] = str(row.get("comments") or "").strip()
    if include_direct_url or str(enriched.get("adapter") or "") != "static":
        enriched["evidenceScore"] = max(int(enriched.get("evidenceScore") or 0), 44)
    if str(enriched.get("adapter") or "") == "static":
        enriched["name"] = f"{str(row.get('studio') or '').strip()} (GameDevMap)"
    return enriched


def discover_gamedevmap_candidates(
    timeout_s: int,
    *,
    config: dict[str, Any] | None = None,
    fetcher=fetch_text,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    cfg = dict(
        _gamedevmap_config_value(config, "gamedevmap", DEFAULT_DISCOVERY_CONFIG["gamedevmap"])
    )
    if not bool(cfg.get("enabled")):
        emit_log("GameDevMap directory disabled, skipping.")
        return [], [], []

    from .gamedevmap_active_dry_run import discover_gamedevmap_audit_candidates

    return discover_gamedevmap_audit_candidates(
        timeout_s,
        config=config,
        fetcher=fetcher,
    )
