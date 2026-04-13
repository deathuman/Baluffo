"""Registry selection/filtering helpers for jobs sources."""

from __future__ import annotations

import hashlib
import json
from fnmatch import fnmatch
from typing import Any
from urllib.parse import urlparse

from src.jobs.common.config import SCRAPY_BROWSER_QUEUE_PATH
from src.jobs.common.numbers import _clamped_int
from src.jobs.text_utils import clean_text
from src.source_registry import source_identity, source_url_fingerprint


def _static_source_primary_host(row: dict[str, Any]) -> str:
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


def _host_matches_pattern(host: str, pattern: str) -> bool:
    clean_host = clean_text(host).lower()
    clean_pattern = clean_text(pattern).lower()
    if not clean_host or not clean_pattern:
        return False
    if "*" in clean_pattern:
        return fnmatch(clean_host, clean_pattern)
    return clean_host == clean_pattern


def _migration_adapter_for_host(host: str) -> str:
    clean_host = clean_text(host).lower()
    if not clean_host:
        return ""
    if clean_host.endswith(".bamboohr.com") or clean_host == "bamboohr.com":
        return "bamboohr"
    if clean_host.endswith(".myworkdayjobs.com") or clean_host == "myworkdayjobs.com":
        return "workday"
    if clean_host.endswith(".workday.com") or clean_host == "workday.com":
        return "workday"
    return ""


def _scrapy_static_registry_from_browser_queue(
    *, enabled_only: bool = True
) -> list[dict[str, Any]]:
    queue_path = SCRAPY_BROWSER_QUEUE_PATH
    try:
        if not queue_path.exists():
            return []
        payload = json.loads(queue_path.read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            return []
    except (OSError, json.JSONDecodeError):
        return []

    by_source: dict[str, list[dict[str, Any]]] = {}
    for row in payload:
        if not isinstance(row, dict):
            continue
        if clean_text(row.get("adapter")) != "scrapy_static":
            continue
        page = clean_text(row.get("page"))
        if not page:
            continue
        source_id = (
            clean_text(row.get("sourceId"))
            or f"scrapy_static:{hashlib.sha1(page.encode('utf-8')).hexdigest()[:12]}"
        )
        by_source.setdefault(source_id, []).append(row)

    rows: list[dict[str, Any]] = []
    for source_id, group in by_source.items():

        def path_len(r: dict[str, Any]) -> int:
            return len(urlparse(clean_text(r.get("page")) or "").path)

        best = min(group, key=path_len)
        page = clean_text(best.get("page")) or ""
        if not page:
            continue
        name = (
            clean_text(best.get("name")) or clean_text(best.get("studio")) or "scrapy_static_source"
        )
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
    studio_source_registry: list[dict[str, Any]],
    redundant_static_rules: list[dict[str, Any]],
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
    studio_source_registry: list[dict[str, Any]],
    redundant_static_rules: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    if adapter == "scrapy_static":
        return _scrapy_static_registry_from_browser_queue(enabled_only=enabled_only)

    rows: list[dict[str, Any]] = []
    seen_identities: set[str] = set()
    for row in studio_source_registry:
        if enabled_only and not bool(row.get("enabledByDefault", True)):
            continue
        row_adapter = clean_text(row.get("adapter"))
        normalized = dict(row)
        normalized["fetchStrategy"] = clean_text(row.get("fetchStrategy")) or "auto"
        normalized["cadenceMinutes"] = _clamped_int(row.get("cadenceMinutes"), 0, 0)
        identity = source_url_fingerprint(normalized) or source_identity(normalized)
        if row_adapter == adapter:
            if identity in seen_identities:
                continue
            seen_identities.add(identity)
            rows.append(normalized)
            continue
        if adapter in {"bamboohr", "workday"} and row_adapter == "static":
            host = _static_source_primary_host(row)
            target_adapter = _migration_adapter_for_host(host)
            if target_adapter != adapter:
                continue
            derived = dict(normalized)
            derived["adapter"] = adapter
            derived["fetchStrategy"] = "http"
            derived["migrationSourceAdapter"] = "static"
            derived["migrationSourceHost"] = host
            identity = source_url_fingerprint(derived) or source_identity(derived)
            if identity in seen_identities:
                continue
            seen_identities.add(identity)
            rows.append(derived)

    rules = redundant_static_rules if isinstance(redundant_static_rules, list) else []
    if adapter == "static" and rules:
        provider_keys = _provider_keys_present_in_registry(
            studio_source_registry, rules, enabled_only=enabled_only
        )
        filtered: list[dict[str, Any]] = []
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
                if not any(_host_matches_pattern(host, str(h)) for h in hosts):
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
