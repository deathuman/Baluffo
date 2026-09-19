"""Web-search audit input signature for cache/artifact reuse decisions.

AI boundary owns: the seed-catalog and full audit input signature used to detect stale audit artifacts.
AI boundary implement in: this file for signature computation; cache reads stay in the audit runners.
AI boundary search before contracts: audit artifact schema version and web search audit tests.
AI boundary verify: `python -m pytest tests/source_discovery/test_web_search_directory_audit.py -q`.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from .directory_page_recovery import RECOVERY_LOGIC_VERSION as RECOVERY_LOGIC_VERSION
from .web_search_audit_contracts import (
    WEB_SEARCH_AUDIT_SCHEMA_VERSION as WEB_SEARCH_AUDIT_SCHEMA_VERSION,
)


def _seed_catalog_signature(studio_seeds: list[dict[str, Any]]) -> dict[str, Any]:
    normalized = [
        {
            "studio": str(seed.get("studio") or "").strip(),
            "careersUrl": str(seed.get("careersUrl") or "").strip(),
            "nlPriority": bool(seed.get("nlPriority")),
        }
        for seed in studio_seeds
    ]
    payload = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
    return {
        "count": len(normalized),
        "sha256": hashlib.sha256(payload.encode("utf-8")).hexdigest(),
    }


# pure — cache validation signature builder


def _web_search_audit_signature(
    *,
    studio_seeds: list[dict[str, Any]],
    include_seed_careers: bool,
    include_web_search: bool,
    max_queries: int,
    max_links_per_query: int,
    recovery_enabled: bool,
    recovery_url_limit: int,
) -> dict[str, Any]:
    return {
        "parserVersion": WEB_SEARCH_AUDIT_SCHEMA_VERSION,
        "includeSeedCareers": bool(include_seed_careers),
        "includeWebSearch": bool(include_web_search),
        "maxQueries": max(0, int(max_queries)),
        "maxLinksPerQuery": max(0, int(max_links_per_query)),
        "activeAuditRecoveryEnabled": bool(recovery_enabled),
        "activeAuditRecoveryUrlLimit": int(recovery_url_limit),
        "recoveryLogicVersion": RECOVERY_LOGIC_VERSION,
        "seedCatalog": _seed_catalog_signature(studio_seeds),
    }


# orchestration — network + mutation
