"""Registry conflict row helpers — URL, alias, and job-identity keys.

AI boundary owns: normalized url/alias extraction, job identity keys, overlap scoring, and provider endpoint shapes.
AI boundary implement in: this registry_conflicts_row_identity.py leaf.
AI boundary search before contracts: registry conflict routes, registry_conflicts coordinator, and frontend registry conflict callers.
AI boundary verify: `npm run lint:repo-guardrails` plus focused registry conflict row tests."""

from __future__ import annotations

import json
import re
from typing import Any
from urllib.parse import urlparse

from src.bridge.registry_conflicts_row_core import (
    _as_list,
    _clean_text,
    _row_adapter,
    _row_identity,
    _row_urls,
)
from src.source_registry import static_listing_url_aliases


def _normalized_url_for_comparison(url: str) -> str:
    try:
        parsed = urlparse(_clean_text(url))
    except ValueError:
        return ""
    if not parsed.scheme or not parsed.netloc:
        return ""
    path = parsed.path.rstrip("/") or "/"
    return f"{parsed.scheme.lower()}://{parsed.netloc.lower()}{path}"


def _provider_slug(row: dict[str, Any]) -> str:
    slug = _clean_text(row.get("slug")).lower()
    if slug:
        return slug
    row_id = _row_identity(row).lower()
    prefix = f"{_row_adapter(row)}:slug:"
    if row_id.startswith(prefix):
        return row_id.removeprefix(prefix).split(":", 1)[0]
    return ""


def _provider_source_aliases(row: dict[str, Any]) -> set[str]:
    adapter = _row_adapter(row)
    aliases = {
        _clean_text(row.get("id")).lower(),
        _clean_text(row.get("sourceId")).lower(),
        _clean_text(row.get("sourceIdentity")).lower(),
        _row_identity(row).lower(),
    }
    slug = _provider_slug(row)
    if adapter and slug:
        aliases.update({slug, f"{adapter}:{slug}", f"{adapter}:slug:{slug}"})
    return {alias for alias in aliases if alias}


def _source_item_aliases(item: dict[str, Any]) -> set[str]:
    aliases = {
        _clean_text(item.get("source")).lower(),
        _clean_text(item.get("sourceId")).lower(),
        _clean_text(item.get("sourceIdentity")).lower(),
    }
    source_job_id = _clean_text(item.get("sourceJobId")).lower()
    parts = [part for part in source_job_id.split(":") if part]
    if len(parts) >= 2:
        aliases.update({f"{parts[0]}:{parts[1]}", f"{parts[0]}:slug:{parts[1]}"})
    return {alias for alias in aliases if alias}


def _job_identity_keys(item: dict[str, Any]) -> set[str]:
    keys: set[str] = set()
    source_job_id = _clean_text(item.get("sourceJobId"))
    job_link = _clean_text(item.get("jobLink") or item.get("url"))
    if source_job_id:
        keys.add(f"id:{source_job_id.lower()}")
        for token in re.findall(r"\d{5,}", source_job_id):
            keys.add(f"token:{token}")
    if job_link:
        normalized = _normalized_url_for_comparison(job_link)
        if normalized:
            keys.add(f"url:{normalized}")
        for token in re.findall(r"\d{5,}", job_link):
            keys.add(f"token:{token}")
    return keys


def _source_job_identity_index(job_rows: Any) -> dict[str, set[str]]:
    index: dict[str, set[str]] = {}
    for row in _as_list(job_rows):
        if not isinstance(row, dict):
            continue
        items = [item for item in _as_list(row.get("sourceBundle")) if isinstance(item, dict)]
        if not items:
            items = [row]
        for item in items:
            keys = _job_identity_keys(item)
            if not keys:
                continue
            for alias in _source_item_aliases(item):
                index.setdefault(alias, set()).update(keys)
    return index


def _row_direct_job_identity_keys(row: dict[str, Any]) -> set[str]:
    keys: set[str] = set()
    for value in _as_list(row.get("jobIdentityKeys")):
        text = _clean_text(value)
        if text:
            keys.add(text.lower())
    for value in _as_list(row.get("sourceJobIds")):
        keys.update(_job_identity_keys({"sourceJobId": value}))
    for value in _as_list(row.get("jobLinks")):
        keys.update(_job_identity_keys({"jobLink": value}))
    return keys


def _row_job_identity_keys(row: dict[str, Any], job_index: dict[str, set[str]]) -> set[str]:
    keys = _row_direct_job_identity_keys(row)
    for alias in _provider_source_aliases(row):
        keys.update(job_index.get(alias, set()))
    return keys


def _identity_overlap_ratio(left: set[str], right: set[str]) -> float:
    shared = left & right
    if any(value.startswith("token:") for value in shared):
        return 1.0
    denominator = min(len(left), len(right))
    if not denominator:
        return 0.0
    return len(shared) / denominator


def _row_primary_url(row: dict[str, Any]) -> str:
    return next(iter(_row_urls(row)), "")


def _row_live_final_url(row: dict[str, Any]) -> str:
    return _clean_text(row.get("liveProbeFinalUrl") or row.get("finalUrl"))


def _static_url_has_job_fragment(row: dict[str, Any]) -> bool:
    exact_job_fragments = {
        "jobs",
        "job",
        "positions",
        "position",
        "openings",
        "opening",
        "vacancies",
        "join",
        "join-us",
        "job-openings",
        "open-positions",
        "current-openings",
    }
    job_fragment_tokens = {
        "job",
        "jobs",
        "position",
        "positions",
        "opening",
        "openings",
        "vacancy",
        "vacancies",
        "role",
        "roles",
        "opportunity",
        "opportunities",
        "join",
    }
    for url in _row_urls(row):
        try:
            fragment = urlparse(url).fragment.strip().lower().strip("/")
        except ValueError:
            continue
        if fragment in exact_job_fragments:
            return True
        fragment_tokens = {token for token in re.split(r"[^a-z0-9]+", fragment) if token}
        if fragment_tokens & job_fragment_tokens:
            return True
    return False


def _provider_endpoint_shape(row: dict[str, Any]) -> str:
    for url in _row_urls(row):
        parsed = urlparse(url)
        path = parsed.path.strip().lower().rstrip("/")
        if path:
            return path
    return ""


def _normalized_static_url_aliases(row: dict[str, Any]) -> set[str]:
    return static_listing_url_aliases(row)


def _json_value(value: Any) -> str:
    if isinstance(value, (dict, list)):
        try:
            return json.dumps(value, sort_keys=True, ensure_ascii=False)
        except TypeError:
            return str(value)
    return str(value or "").strip()
