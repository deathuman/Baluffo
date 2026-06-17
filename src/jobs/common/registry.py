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
    except ValueError:
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
    del enabled_only
    payload = _read_scrapy_browser_queue()
    by_source: dict[str, list[dict[str, Any]]] = {}
    for row in payload:
        source_id = _scrapy_queue_source_id(row)
        if source_id:
            by_source.setdefault(source_id, []).append(row)
    return [
        source_row
        for source_id, group in by_source.items()
        if (source_row := _scrapy_queue_source_row(source_id, group))
    ]


def _read_scrapy_browser_queue() -> list[dict[str, Any]]:
    try:
        if not SCRAPY_BROWSER_QUEUE_PATH.exists():
            return []
        payload = json.loads(SCRAPY_BROWSER_QUEUE_PATH.read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            return []
    except (OSError, json.JSONDecodeError):
        return []
    return [row for row in payload if isinstance(row, dict)]


def _scrapy_queue_source_id(row: dict[str, Any]) -> str:
    if clean_text(row.get("adapter")) != "scrapy_static":
        return ""
    page = clean_text(row.get("page"))
    if not page:
        return ""
    return (
        clean_text(row.get("sourceId"))
        or f"scrapy_static:{hashlib.sha1(page.encode('utf-8')).hexdigest()[:12]}"
    )


def _scrapy_queue_source_row(source_id: str, group: list[dict[str, Any]]) -> dict[str, Any]:
    best = min(group, key=lambda row: len(urlparse(clean_text(row.get("page"))).path))
    page = clean_text(best.get("page"))
    name = clean_text(best.get("name")) or clean_text(best.get("studio")) or "scrapy_static_source"
    studio = clean_text(best.get("studio")) or name
    return {
        "name": name,
        "studio": studio,
        "adapter": "scrapy_static",
        "pages": [page],
        "id": source_id,
        "enabledByDefault": True,
        "fetchStrategy": "http",
        "cadenceMinutes": 0,
    }


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
        if row_adapter == adapter:
            _append_seen(rows, seen_identities, normalized)
            continue
        if adapter in {"bamboohr", "workday"} and row_adapter == "static":
            derived = _provider_migration_entry(normalized, adapter)
            if not derived:
                continue
            _append_seen(rows, seen_identities, derived)

    rules = redundant_static_rules if isinstance(redundant_static_rules, list) else []
    if adapter == "static" and rules:
        rows = _filter_redundant_static_rows(
            rows,
            rules,
            _provider_keys_present_in_registry(
                studio_source_registry, rules, enabled_only=enabled_only
            ),
        )
    return rows


def _append_seen(
    rows: list[dict[str, Any]], seen_identities: set[str], row: dict[str, Any]
) -> None:
    identity = source_url_fingerprint(row) or source_identity(row)
    if identity not in seen_identities:
        seen_identities.add(identity)
        rows.append(row)


def _provider_migration_entry(row: dict[str, Any], adapter: str) -> dict[str, Any]:
    host = _static_source_primary_host(row)
    if _migration_adapter_for_host(host) != adapter:
        return {}
    derived = dict(row)
    derived["adapter"] = adapter
    derived["fetchStrategy"] = "http"
    derived["migrationSourceAdapter"] = "static"
    derived["migrationSourceHost"] = host
    return derived


def _matches_redundant_static_rule(
    row: dict[str, Any], rules: list[dict[str, Any]], provider_keys: set[tuple[str, str]]
) -> bool:
    host = _static_source_primary_host(row)
    if not host:
        return False
    for rule in rules:
        hosts = rule.get("hosts")
        if not isinstance(hosts, list):
            continue
        if (
            any(_host_matches_pattern(host, str(h)) for h in hosts)
            and (
                clean_text(rule.get("adapter")),
                clean_text(rule.get("provider_id_value")),
            )
            in provider_keys
        ):
            return True
    return False


def _filter_redundant_static_rows(
    rows: list[dict[str, Any]],
    rules: list[dict[str, Any]],
    provider_keys: set[tuple[str, str]],
) -> list[dict[str, Any]]:
    return [row for row in rows if not _matches_redundant_static_rule(row, rules, provider_keys)]
