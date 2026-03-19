"""Registry selection/filtering helpers for jobs sources."""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from src.jobs.common.config import SCRAPY_BROWSER_QUEUE_PATH
from src.jobs.common.numbers import _clamped_int
from src.jobs.text_utils import clean_text


def _static_source_primary_host(row: Dict[str, Any]) -> str:
    pages = row.get("pages") if isinstance(row.get("pages"), list) else []
    url = (pages[0] if pages else None) or clean_text(row.get("listing_url")) or ""
    if not url:
        return ""
    try:
        parsed = urlparse(url)
        host = (parsed.netloc or "").strip().lower()
        if host.startswith("www."):
            host = host[4:]
        return host
    except Exception:  # noqa: BLE001
        return ""


def _scrapy_static_registry_from_browser_queue(*, enabled_only: bool = True) -> List[Dict[str, Any]]:
    queue_path = SCRAPY_BROWSER_QUEUE_PATH
    common_module = sys.modules.get("src.jobs.common")
    if common_module is not None:
        candidate = getattr(common_module, "SCRAPY_BROWSER_QUEUE_PATH", queue_path)
        if isinstance(candidate, Path):
            queue_path = candidate
    try:
        if not queue_path.exists():
            return []
        payload = json.loads(queue_path.read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            return []
    except (OSError, json.JSONDecodeError):
        return []

    by_source: Dict[str, List[Dict[str, Any]]] = {}
    for row in payload:
        if not isinstance(row, dict):
            continue
        if clean_text(row.get("adapter")) != "scrapy_static":
            continue
        page = clean_text(row.get("page"))
        if not page:
            continue
        source_id = clean_text(row.get("sourceId")) or f"scrapy_static:{hashlib.sha1(page.encode('utf-8')).hexdigest()[:12]}"
        by_source.setdefault(source_id, []).append(row)

    rows: List[Dict[str, Any]] = []
    for source_id, group in by_source.items():
        def path_len(r: Dict[str, Any]) -> int:
            return len((urlparse(clean_text(r.get("page")) or "").path))

        best = min(group, key=path_len)
        page = clean_text(best.get("page")) or ""
        if not page:
            continue
        name = clean_text(best.get("name")) or clean_text(best.get("studio")) or "scrapy_static_source"
        studio = clean_text(best.get("studio")) or name
        rows.append(
            {
                "name": name,
                "studio": studio,
                "adapter": "scrapy_static",
                "pages": [page],
                "id": source_id,
                "enabledByDefault": True,
                "fetchStrategy": "http",
                "cadenceMinutes": 0,
            }
        )
    return rows


def _provider_keys_present_in_registry(
    studio_source_registry: List[Dict[str, Any]],
    redundant_static_rules: List[Dict[str, Any]],
    *,
    enabled_only: bool = True,
) -> set[tuple[str, str]]:
    out: set[tuple[str, str]] = set()
    for rule in redundant_static_rules:
        ad = clean_text(rule.get("adapter"))
        field = clean_text(rule.get("provider_id_field"))
        val = clean_text(rule.get("provider_id_value"))
        if not ad or not field or not val:
            continue
        for row in studio_source_registry:
            if clean_text(row.get("adapter")) != ad:
                continue
            if enabled_only and not bool(row.get("enabledByDefault", True)):
                continue
            if clean_text(row.get(field)) == val:
                out.add((ad, val))
                break
    return out


def registry_entries(
    adapter: str,
    *,
    enabled_only: bool = True,
    studio_source_registry: List[Dict[str, Any]],
    redundant_static_rules: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    if adapter == "scrapy_static":
        return _scrapy_static_registry_from_browser_queue(enabled_only=enabled_only)

    rows: List[Dict[str, Any]] = []
    for row in studio_source_registry:
        if clean_text(row.get("adapter")) != adapter:
            continue
        if enabled_only and not bool(row.get("enabledByDefault", True)):
            continue
        normalized = dict(row)
        normalized["fetchStrategy"] = clean_text(row.get("fetchStrategy")) or "auto"
        normalized["cadenceMinutes"] = _clamped_int(row.get("cadenceMinutes"), 0, 0)
        rows.append(normalized)

    rules = redundant_static_rules if isinstance(redundant_static_rules, list) else []
    if adapter == "static" and rules:
        provider_keys = _provider_keys_present_in_registry(studio_source_registry, rules, enabled_only=enabled_only)
        filtered: List[Dict[str, Any]] = []
        for r in rows:
            host = _static_source_primary_host(r)
            if not host:
                filtered.append(r)
                continue
            skip = False
            for rule in rules:
                hosts = rule.get("hosts")
                if not isinstance(hosts, list):
                    continue
                if host not in [str(h).strip().lower() for h in hosts]:
                    continue
                ad = clean_text(rule.get("adapter"))
                val = clean_text(rule.get("provider_id_value"))
                if (ad, val) in provider_keys:
                    skip = True
                    break
            if not skip:
                filtered.append(r)
        rows = filtered
    return rows

