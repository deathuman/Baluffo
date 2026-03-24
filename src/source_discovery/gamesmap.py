from __future__ import annotations

"""Gamesmap directory parsing and candidate extraction.

Responsibilities:
- Parse Gamesmap index HTML/JS into detail entries
- Parse Gamesmap detail pages into normalized studio/category payloads
- Apply category filters and build provider/static candidates
"""

import json
import re
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

from src.source_registry import unique_sources

from .config import CAREERS_URL_HINTS, DEFAULT_DISCOVERY_CONFIG
from .scoring import unique_string_list
from .web_search import fetch_text, infer_web_candidate


def _strip_html_tags(html: str) -> str:
    text = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", str(html or ""))
    text = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", text)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _gamesmap_config_value(config: dict[str, Any] | None, key: str, default: Any) -> Any:
    source = config if isinstance(config, dict) else {}
    return source.get(key, default)


def _gamesmap_cache_path(config: dict[str, Any] | None) -> Path | None:
    source = config if isinstance(config, dict) else {}
    if isinstance(source.get("gamesmap"), dict):
        source = source.get("gamesmap") or {}
    raw = str(source.get("cachePath") or "").strip()
    if not raw:
        return Path(__file__).resolve().parents[2] / "data" / "gamesmap-discovery-cache.json"
    return Path(raw)


def _gamesmap_cache_ttl_minutes(config: dict[str, Any] | None) -> int:
    source = config if isinstance(config, dict) else {}
    if isinstance(source.get("gamesmap"), dict):
        source = source.get("gamesmap") or {}
    raw = source.get("cacheTtlMinutes", "")
    if raw in {"", None}:
        raw = 360
    try:
        return max(0, int(raw))
    except (TypeError, ValueError):
        return 360


def _gamesmap_cache_signature(cfg: dict[str, Any]) -> dict[str, Any]:
    return {
        "baseUrl": str(cfg.get("baseUrl") or "").strip(),
        "indexUrls": [str(item).strip() for item in (cfg.get("indexUrls") or []) if str(item).strip()],
        "preferEnglish": bool(cfg.get("preferEnglish", True)),
        "websiteOnlyFallback": bool(cfg.get("websiteOnlyFallback", True)),
        "websiteOnlyManualOnly": bool(cfg.get("websiteOnlyManualOnly", False)),
        "maxDetailPages": max(0, int(cfg.get("maxDetailPages") or 0)),
        "allowedCategoryTokens": list(cfg.get("allowedCategoryTokens") or []),
        "blockedCategoryTokens": list(cfg.get("blockedCategoryTokens") or []),
    }


def _load_gamesmap_cache(config: dict[str, Any] | None, cfg: dict[str, Any], *, fetcher: Any) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]] | None:
    cache_path = _gamesmap_cache_path(config)
    ttl_minutes = _gamesmap_cache_ttl_minutes(config)
    if ttl_minutes <= 0 or cache_path is None:
        return None
    # Keep fixture-driven/unit test fetchers deterministic unless they explicitly opt into cache via config.
    source = config if isinstance(config, dict) else {}
    if isinstance(source.get("gamesmap"), dict):
        source = source.get("gamesmap") or {}
    if fetcher is not fetch_text and not str(source.get("cachePath") or "").strip():
        return None
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    updated_at_raw = str(payload.get("updatedAt") or "").strip()
    if not updated_at_raw:
        return None
    try:
        updated_at = datetime.fromisoformat(updated_at_raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if datetime.now(UTC) - updated_at > timedelta(minutes=ttl_minutes):
        return None
    if payload.get("configSignature") != _gamesmap_cache_signature(cfg):
        return None
    provider_rows = payload.get("providerCandidates")
    static_rows = payload.get("staticCandidates")
    failures = payload.get("failures")
    if not isinstance(provider_rows, list) or not isinstance(static_rows, list) or not isinstance(failures, list):
        return None
    return unique_sources(provider_rows), unique_sources(static_rows), failures


def _write_gamesmap_cache(
    config: dict[str, Any] | None,
    cfg: dict[str, Any],
    *,
    provider_candidates: list[dict[str, Any]],
    static_candidates: list[dict[str, Any]],
    failures: list[dict[str, Any]],
) -> None:
    cache_path = _gamesmap_cache_path(config)
    if cache_path is None:
        return
    payload = {
        "updatedAt": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "configSignature": _gamesmap_cache_signature(cfg),
        "providerCandidates": unique_sources(provider_candidates),
        "staticCandidates": unique_sources(static_candidates),
        "failures": failures,
    }
    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    except OSError:
        return


def normalize_gamesmap_category_token(value: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9+]+", " ", str(value or "").lower())).strip()


def _extract_gamesmap_js_data_container(markup: str) -> list[Any] | None:
    html = str(markup or "")
    token = "window.jsDataContainer"
    start = html.find(token)
    if start < 0:
        return None
    array_start = html.find("[", start)
    if array_start < 0:
        return None
    depth = 0
    for idx in range(array_start, len(html)):
        char = html[idx]
        if char == "[":
            depth += 1
        elif char == "]":
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(html[array_start : idx + 1])
                except json.JSONDecodeError:
                    return None
    return None


def parse_gamesmap_index_links(html: str, base_url: str) -> list[str]:
    links = re.findall(r'(?is)href=["\']([^"\']+)["\']', str(html or ""))
    out: list[str] = []
    seen = set()
    for raw in links:
        absolute = urljoin(base_url, raw)
        try:
            parsed = urlparse(absolute)
        except ValueError:
            continue
        path = (parsed.path or "").lower()
        if "/detail/industry/" not in path:
            continue
        normalized = absolute.split("#", 1)[0]
        if normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out


def parse_gamesmap_index_entries(html: str, base_url: str, *, prefer_english: bool = True) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    seen = set()
    payload = _extract_gamesmap_js_data_container(html)
    if isinstance(payload, list):
        for item in payload:
            if not (
                isinstance(item, list)
                and len(item) >= 2
                and item[0] == "map.coordinates"
                and isinstance(item[1], dict)
            ):
                continue
            points = item[1].get("points")
            if not isinstance(points, dict):
                continue
            for point in points.get("industry") or []:
                if not isinstance(point, dict):
                    continue
                slug = str(point.get("slug") or "").strip().strip("/")
                studio = str(point.get("name") or "").strip()
                if not slug or not studio:
                    continue
                path = f"/en/detail/industry/{slug}" if prefer_english else f"/detail/industry/{slug}"
                detail_url = urljoin(base_url.rstrip("/") + "/", path.lstrip("/"))
                province = point.get("province") if isinstance(point.get("province"), dict) else {}
                location = str(
                    (
                        province.get("nameEn") if prefer_english else province.get("name")
                    )
                    or province.get("nameEn")
                    or province.get("name")
                    or ""
                ).strip()
                if detail_url in seen:
                    continue
                seen.add(detail_url)
                out.append(
                    {
                        "detailUrl": detail_url,
                        "studio": studio,
                        "location": location,
                    }
                )
            break
    for detail_url in parse_gamesmap_index_links(html, base_url):
        if detail_url in seen:
            continue
        seen.add(detail_url)
        out.append({"detailUrl": detail_url, "studio": "", "location": ""})
    return out


def parse_gamesmap_detail_page(page_url: str, html: str) -> dict[str, Any] | None:
    markup = str(html or "")
    name_match = re.search(r"(?is)<h1[^>]*>(.*?)</h1>", markup)
    if not name_match:
        title_match = re.search(r"(?is)<title[^>]*>(.*?)</title>", markup)
        name = _strip_html_tags(title_match.group(1)) if title_match else ""
    else:
        name = _strip_html_tags(name_match.group(1))
    if not name:
        return None

    categories: list[str] = []
    for match in re.finditer(
        r'(?is)<[^>]+class=["\'][^"\']*(?:tag|badge|category|chip)[^"\']*["\'][^>]*>(.*?)</[^>]+>',
        markup,
    ):
        token = _strip_html_tags(match.group(1))
        if token:
            categories.append(token)
    for match in re.finditer(
        r'(?is)(?:Category|Categories|Branche|Branchen)\s*</[^>]+>\s*<[^>]+>(.*?)</[^>]+>',
        markup,
    ):
        chunk = _strip_html_tags(match.group(1))
        for part in re.split(r"[|,/]| • |\s{2,}", chunk):
            token = part.strip()
            if token:
                categories.append(token)
    categories = unique_string_list(categories)

    location = ""
    for match in re.finditer(r"(?is)(?:Location|Standort|City)\s*</[^>]+>\s*<[^>]+>(.*?)</[^>]+>", markup):
        token = _strip_html_tags(match.group(1))
        if token:
            location = token
            break

    website_url = ""
    careers_url = ""
    ignored_hosts = (
        "game.de",
        "facebook.com",
        "instagram.com",
        "twitter.com",
        "x.com",
        "twitch.tv",
        "linkedin.com",
        "youtube.com",
        "youtu.be",
        "discord.gg",
        "discord.com",
        "list-manage.com",
    )
    for match in re.finditer(r'(?is)<a\b[^>]*href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', markup):
        href = str(match.group(1) or "").strip()
        if not href or href.startswith(("mailto:", "javascript:", "#")):
            continue
        absolute = urljoin(page_url, href).split("#", 1)[0]
        try:
            parsed = urlparse(absolute)
            page_host = (urlparse(page_url).netloc or "").lower()
        except ValueError:
            continue
        host = (parsed.netloc or "").lower()
        if not host or host.endswith("gamesmap.de") or host == page_host:
            continue
        if any(host == blocked or host.endswith(f".{blocked}") for blocked in ignored_hosts):
            continue
        label = _strip_html_tags(match.group(2))
        context_start = max(0, match.start() - 140)
        context = _strip_html_tags(markup[context_start : match.end()])
        hint_blob = f"{label} {absolute} {context}".lower()
        if any(token in hint_blob for token in CAREERS_URL_HINTS + ("job page", "job pages", "stellen", "karriere")):
            if not careers_url:
                careers_url = absolute
                continue
        if not website_url:
            website_url = absolute
    if not website_url and careers_url:
        try:
            parsed = urlparse(careers_url)
            website_url = f"{parsed.scheme}://{parsed.netloc}"
        except ValueError:
            website_url = ""

    return {
        "studio": name,
        "detailUrl": page_url,
        "websiteUrl": website_url,
        "careersUrl": careers_url,
        "categories": categories,
        "location": location,
    }


def gamesmap_matches_category(categories: Iterable[str], allowed: Iterable[str], blocked: Iterable[str]) -> bool:
    normalized_categories = [
        normalize_gamesmap_category_token(item) for item in categories if str(item or "").strip()
    ]
    if not normalized_categories:
        return False
    blocked_tokens = [
        normalize_gamesmap_category_token(item) for item in blocked if str(item or "").strip()
    ]
    if any(any(token and token in category for token in blocked_tokens) for category in normalized_categories):
        return False
    allowed_tokens = [
        normalize_gamesmap_category_token(item) for item in allowed if str(item or "").strip()
    ]
    if not allowed_tokens:
        return True
    return any(any(token and token in category for token in allowed_tokens) for category in normalized_categories)


def build_gamesmap_static_candidate(
    *,
    studio: str,
    target_url: str,
    nl_priority: bool,
    website_only: bool,
    detail_url: str,
    categories: list[str],
    location: str,
    manual_only: bool = False,
) -> dict[str, Any]:
    evidence_types = ["gamesmap_directory", "gamesmap_category_match"]
    evidence_score = 24
    if website_only:
        evidence_types.append("gamesmap_website")
        evidence_types.append("gamesmap_website_only")
        if manual_only:
            evidence_types.append("gamesmap_manual_website_only")
    else:
        evidence_types.append("gamesmap_careers_url")
        evidence_score = 40
    if location:
        evidence_types.append("gamesmap_location")
    return {
        "name": f"{studio} (Gamesmap)",
        "studio": studio,
        "company": studio,
        "adapter": "static",
        "pages": [target_url],
        "listing_url": target_url,
        "nlPriority": nl_priority,
        "enabledByDefault": False,
        "discoveryMethod": "gamesmap",
        "discoveryStage": "generic_static",
        "careersUrl": "" if website_only else target_url,
        "evidenceSource": "gamesmap",
        "evidenceTypes": evidence_types,
        "evidenceScore": evidence_score,
        "weakSignal": bool(website_only),
        "sourceDirectory": "gamesmap",
        "sourceDirectoryUrl": "https://www.gamesmap.de/",
        "sourceDirectoryEntryUrl": detail_url,
        "sourceDirectoryCategories": unique_string_list(categories),
        "sourceDirectoryLocation": str(location or "").strip(),
        "manualOnly": bool(manual_only),
    }


def discover_gamesmap_candidates(
    timeout_s: int,
    *,
    config: dict[str, Any] | None = None,
    fetcher=fetch_text,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    cfg = dict(_gamesmap_config_value(config, "gamesmap", DEFAULT_DISCOVERY_CONFIG["gamesmap"]))
    if not bool(cfg.get("enabled")):
        return [], [], []
    cached = _load_gamesmap_cache(config, cfg, fetcher=fetcher)
    if cached is not None:
        return cached
    base_url = str(cfg.get("baseUrl") or "https://www.gamesmap.de").strip()
    index_urls = [str(item).strip() for item in (cfg.get("indexUrls") or []) if str(item).strip()]
    prefer_english = bool(cfg.get("preferEnglish", True))
    allowed_tokens = list(cfg.get("allowedCategoryTokens") or [])
    blocked_tokens = list(cfg.get("blockedCategoryTokens") or [])
    website_only_fallback = bool(cfg.get("websiteOnlyFallback", True))
    website_only_manual_only = bool(cfg.get("websiteOnlyManualOnly", False))
    max_detail_pages = max(0, int(cfg.get("maxDetailPages") or 0))

    provider_candidates: list[dict[str, Any]] = []
    static_candidates: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    detail_entries: list[dict[str, str]] = []
    seen_details = set()

    for index_url in index_urls:
        try:
            index_html = fetcher(index_url, timeout_s)
        except Exception as exc:  # noqa: BLE001
            failures.append(
                {
                    "name": index_url,
                    "adapter": "gamesmap",
                    "error": str(exc),
                    "stage": "directory_index_fetch",
                }
            )
            continue
        for entry in parse_gamesmap_index_entries(index_html, base_url, prefer_english=prefer_english):
            detail_url = str(entry.get("detailUrl") or "").strip()
            if not detail_url:
                continue
            if detail_url in seen_details:
                continue
            seen_details.add(detail_url)
            detail_entries.append(entry)
            if max_detail_pages and len(detail_entries) >= max_detail_pages:
                break
        if max_detail_pages and len(detail_entries) >= max_detail_pages:
            break

    for entry in detail_entries:
        detail_url = str(entry.get("detailUrl") or "").strip()
        try:
            detail_html = fetcher(detail_url, timeout_s)
        except Exception as exc:  # noqa: BLE001
            failures.append(
                {
                    "name": detail_url,
                    "adapter": "gamesmap",
                    "error": str(exc),
                    "stage": "directory_detail_fetch",
                }
            )
            continue
        parsed = parse_gamesmap_detail_page(detail_url, detail_html)
        if not parsed:
            failures.append(
                {
                    "name": detail_url,
                    "adapter": "gamesmap",
                    "error": "detail parse failed",
                    "stage": "directory_detail_parse",
                }
            )
            continue
        studio = str(parsed.get("studio") or "").strip()
        categories = list(parsed.get("categories") or [])
        if not studio or not gamesmap_matches_category(categories, allowed_tokens, blocked_tokens):
            continue
        location = str(parsed.get("location") or entry.get("location") or "").strip()
        careers_url = str(parsed.get("careersUrl") or "").strip()
        website_url = str(parsed.get("websiteUrl") or "").strip()
        nl_priority = False
        if careers_url:
            inferred = infer_web_candidate(careers_url, studio, nl_priority=nl_priority, discovery_method="gamesmap")
            if inferred:
                inferred["evidenceSource"] = "gamesmap"
                inferred["evidenceTypes"] = unique_string_list(
                    [*(inferred.get("evidenceTypes") or []), "gamesmap_directory", "gamesmap_careers_url", "gamesmap_category_match"]
                )
                inferred["evidenceScore"] = max(int(inferred.get("evidenceScore") or 0), 44)
                inferred["careersUrl"] = careers_url
                inferred["sourceDirectory"] = "gamesmap"
                inferred["sourceDirectoryUrl"] = "https://www.gamesmap.de/"
                inferred["sourceDirectoryEntryUrl"] = detail_url
                inferred["sourceDirectoryCategories"] = unique_string_list(categories)
                inferred["sourceDirectoryLocation"] = location
                provider_candidates.append(inferred)
            else:
                static_candidates.append(
                    build_gamesmap_static_candidate(
                        studio=studio,
                        target_url=careers_url,
                        nl_priority=nl_priority,
                        website_only=False,
                        detail_url=detail_url,
                        categories=categories,
                        location=location,
                        manual_only=False,
                    )
                )
            continue

        if website_only_fallback and website_url:
            static_candidates.append(
                build_gamesmap_static_candidate(
                    studio=studio,
                    target_url=website_url,
                    nl_priority=nl_priority,
                    website_only=True,
                    detail_url=detail_url,
                    categories=categories,
                    location=location,
                    manual_only=website_only_manual_only,
                )
            )

    provider_candidates = unique_sources(provider_candidates)
    static_candidates = unique_sources(static_candidates)
    _write_gamesmap_cache(
        config,
        cfg,
        provider_candidates=provider_candidates,
        static_candidates=static_candidates,
        failures=failures,
    )
    return provider_candidates, static_candidates, failures

