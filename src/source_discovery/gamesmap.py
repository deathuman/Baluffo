from __future__ import annotations

"""Gamesmap directory parsing and candidate extraction.

Responsibilities:
- Parse Gamesmap index HTML/JS into normalized company entries
- Support the legacy detail-page parser for older fixtures/site shapes
- Apply category filters and build provider/static candidates from company websites
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
from .directory_fetch import fetch_directory_pages, resolve_directory_fetch_limits
from .page_analysis import analyze_fetched_page
from .scoring import unique_string_list
from .static_candidates import build_known_careers_url_candidate
from .web_search import fetch_text, infer_web_candidate

GAMESMAP_IGNORED_WEBSITE_HOSTS = (
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
    "threads.com",
)
GAMESMAP_PARSER_CACHE_VERSION = 2
_GAMESMAP_CATEGORY_REFERENCE_RE = re.compile(
    r"^\$[^:]*:props:children:props:children:props:children:props:companies:(\d+):categories:(\d+)$"
)


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
        "parserVersion": GAMESMAP_PARSER_CACHE_VERSION,
        "baseUrl": str(cfg.get("baseUrl") or "").strip(),
        "indexUrls": [
            str(item).strip() for item in (cfg.get("indexUrls") or []) if str(item).strip()
        ],
        "preferEnglish": bool(cfg.get("preferEnglish", True)),
        "websiteOnlyFallback": bool(cfg.get("websiteOnlyFallback", True)),
        "websiteOnlyManualOnly": bool(cfg.get("websiteOnlyManualOnly", False)),
        "maxDetailPages": max(0, int(cfg.get("maxDetailPages") or 0)),
        "allowedCategoryTokens": list(cfg.get("allowedCategoryTokens") or []),
        "blockedCategoryTokens": list(cfg.get("blockedCategoryTokens") or []),
    }


def _load_gamesmap_cache(
    config: dict[str, Any] | None, cfg: dict[str, Any], *, fetcher: Any
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]] | None:
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
    if (
        not isinstance(provider_rows, list)
        or not isinstance(static_rows, list)
        or not isinstance(failures, list)
    ):
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


def _extract_json_array(markup: str, array_start: int) -> list[Any] | None:
    depth = 0
    in_string = False
    escape = False
    for idx in range(array_start, len(markup)):
        char = markup[idx]
        if in_string:
            if escape:
                escape = False
                continue
            if char == "\\":
                escape = True
                continue
            if char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
            continue
        if char == "[":
            depth += 1
            continue
        if char == "]":
            depth -= 1
            if depth == 0:
                try:
                    payload = json.loads(markup[array_start : idx + 1])
                except json.JSONDecodeError:
                    return None
                return payload if isinstance(payload, list) else None
    return None


def _extract_gamesmap_next_companies(markup: str) -> list[dict[str, Any]] | None:
    html = str(markup or "")
    raw_token = '\\"companies\\":'
    start = html.find(raw_token)
    if start < 0:
        return None
    end = len(html)
    escape = False
    for idx in range(start, len(html)):
        char = html[idx]
        if escape:
            escape = False
            continue
        if char == "\\":
            escape = True
            continue
        if char == '"':
            end = idx
            break
    try:
        decoded = json.loads(f'"{html[start:end]}"')
    except json.JSONDecodeError:
        return None
    token = '"companies":'
    token_start = decoded.find(token)
    if token_start < 0:
        return None
    array_start = decoded.find("[", token_start + len(token))
    if array_start < 0:
        return None
    payload = _extract_json_array(decoded, array_start)
    if not isinstance(payload, list):
        return None
    return [item for item in payload if isinstance(item, dict)]


def _gamesmap_company_detail_url(slug: str, base_url: str, *, prefer_english: bool = True) -> str:
    path = f"/en/company/{slug}" if prefer_english else f"/company/{slug}"
    return urljoin(base_url.rstrip("/") + "/", path.lstrip("/"))


def _gamesmap_location_from_company(company: dict[str, Any]) -> str:
    address = company.get("address") if isinstance(company.get("address"), dict) else {}
    parts: list[str] = []
    for raw in (address.get("city"), address.get("state")):
        token = str(raw or "").strip()
        if token and token not in parts:
            parts.append(token)
    if not parts:
        country = str(address.get("country") or "").strip()
        if country:
            parts.append(country)
    return ", ".join(parts)


def _gamesmap_category_reference_indices(value: str) -> tuple[int, int] | None:
    match = _GAMESMAP_CATEGORY_REFERENCE_RE.match(str(value or "").strip())
    if not match:
        return None
    return int(match.group(1)), int(match.group(2))


def _gamesmap_increment_unresolved_reference(diagnostics: dict[str, int] | None) -> None:
    if not isinstance(diagnostics, dict):
        return
    diagnostics["unresolvedReferenceCount"] = (
        max(0, int(diagnostics.get("unresolvedReferenceCount") or 0)) + 1
    )


def _resolve_gamesmap_category_item(
    item: Any,
    companies: list[dict[str, Any]],
    *,
    diagnostics: dict[str, int] | None = None,
    seen_refs: set[tuple[int, int]] | None = None,
) -> list[str]:
    if isinstance(item, dict):
        token = str(item.get("name") or "").strip()
        if not token:
            return []
        return _resolve_gamesmap_category_item(
            token,
            companies,
            diagnostics=diagnostics,
            seen_refs=seen_refs,
        )
    token = str(item or "").strip()
    if not token:
        return []
    ref = _gamesmap_category_reference_indices(token)
    if ref is None:
        return [token]
    seen = set(seen_refs or set())
    if ref in seen:
        _gamesmap_increment_unresolved_reference(diagnostics)
        return []
    company_idx, category_idx = ref
    if company_idx < 0 or company_idx >= len(companies):
        _gamesmap_increment_unresolved_reference(diagnostics)
        return []
    source_company = companies[company_idx]
    categories_raw = (
        source_company.get("categories")
        if isinstance(source_company.get("categories"), list)
        else []
    )
    if category_idx < 0 or category_idx >= len(categories_raw):
        _gamesmap_increment_unresolved_reference(diagnostics)
        return []
    return _resolve_gamesmap_category_item(
        categories_raw[category_idx],
        companies,
        diagnostics=diagnostics,
        seen_refs=seen | {ref},
    )


def _gamesmap_categories_from_company(
    company: dict[str, Any],
    companies: list[dict[str, Any]],
    *,
    diagnostics: dict[str, int] | None = None,
) -> list[str]:
    categories_raw = (
        company.get("categories") if isinstance(company.get("categories"), list) else []
    )
    out: list[str] = []
    for item in categories_raw:
        out.extend(
            _resolve_gamesmap_category_item(
                item,
                companies,
                diagnostics=diagnostics,
            )
        )
    return unique_string_list(out)


def _gamesmap_valid_website_url(value: str) -> str:
    candidate = str(value or "").strip()
    if not candidate:
        return ""
    try:
        parsed = urlparse(candidate)
    except ValueError:
        return ""
    host = (parsed.netloc or "").strip().lower()
    if not host or host.endswith("gamesmap.de"):
        return ""
    if any(
        host == blocked or host.endswith(f".{blocked}")
        for blocked in GAMESMAP_IGNORED_WEBSITE_HOSTS
    ):
        return ""
    if parsed.scheme not in {"http", "https"}:
        return ""
    return candidate.split("#", 1)[0]


def _gamesmap_website_from_company(company: dict[str, Any]) -> str:
    websites = company.get("websites") if isinstance(company.get("websites"), list) else []
    for item in websites:
        website = _gamesmap_valid_website_url(str(item or "").strip())
        if website:
            return website
    return ""


def _normalize_gamesmap_company_entry(
    company: dict[str, Any],
    companies: list[dict[str, Any]],
    base_url: str,
    *,
    prefer_english: bool = True,
    diagnostics: dict[str, int] | None = None,
) -> dict[str, Any] | None:
    studio = str(company.get("name") or "").strip()
    slug = str(company.get("slug") or "").strip().strip("/")
    if not studio or not slug:
        return None
    return {
        "detailUrl": _gamesmap_company_detail_url(slug, base_url, prefer_english=prefer_english),
        "studio": studio,
        "location": _gamesmap_location_from_company(company),
        "categories": _gamesmap_categories_from_company(
            company,
            companies,
            diagnostics=diagnostics,
        ),
        "websiteUrl": _gamesmap_website_from_company(company),
        "slug": slug,
    }


def _normalize_gamesmap_company_entries(
    companies: list[dict[str, Any]],
    base_url: str,
    *,
    prefer_english: bool = True,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    diagnostics = {"unresolvedReferenceCount": 0}
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for company in companies:
        row = _normalize_gamesmap_company_entry(
            company,
            companies,
            base_url,
            prefer_english=prefer_english,
            diagnostics=diagnostics,
        )
        if not isinstance(row, dict):
            continue
        detail_url = str(row.get("detailUrl") or "").strip()
        if not detail_url or detail_url in seen:
            continue
        seen.add(detail_url)
        out.append(row)
    return out, diagnostics


def _parse_gamesmap_index_entries_with_diagnostics(
    html: str, base_url: str, *, prefer_english: bool = True
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    out: list[dict[str, Any]] = []
    diagnostics = {"unresolvedReferenceCount": 0}
    seen = set()
    companies = _extract_gamesmap_next_companies(html)
    if isinstance(companies, list):
        normalized_rows, diagnostics = _normalize_gamesmap_company_entries(
            companies,
            base_url,
            prefer_english=prefer_english,
        )
        if normalized_rows:
            return normalized_rows, diagnostics
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
                detail_url = (
                    f"/en/detail/industry/{slug}" if prefer_english else f"/detail/industry/{slug}"
                )
                detail_url = urljoin(base_url.rstrip("/") + "/", detail_url.lstrip("/"))
                province = point.get("province") if isinstance(point.get("province"), dict) else {}
                location = str(
                    (province.get("nameEn") if prefer_english else province.get("name"))
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
                        "categories": [],
                        "websiteUrl": "",
                        "slug": slug,
                    }
                )
            break
    for detail_url in parse_gamesmap_index_links(html, base_url):
        if detail_url in seen:
            continue
        seen.add(detail_url)
        out.append(
            {
                "detailUrl": detail_url,
                "studio": "",
                "location": "",
                "categories": [],
                "websiteUrl": "",
                "slug": "",
            }
        )
    return out, diagnostics


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
        if "/detail/industry/" not in path and "/company/" not in path:
            continue
        normalized = absolute.split("#", 1)[0]
        if normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out


def parse_gamesmap_index_entries(
    html: str, base_url: str, *, prefer_english: bool = True
) -> list[dict[str, Any]]:
    rows, _diagnostics = _parse_gamesmap_index_entries_with_diagnostics(
        html,
        base_url,
        prefer_english=prefer_english,
    )
    return rows


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
        r"(?is)(?:Category|Categories|Branche|Branchen)\s*</[^>]+>\s*<[^>]+>(.*?)</[^>]+>",
        markup,
    ):
        chunk = _strip_html_tags(match.group(1))
        for part in re.split(r"[|,/]| • |\s{2,}", chunk):
            token = part.strip()
            if token:
                categories.append(token)
    categories = unique_string_list(categories)

    location = ""
    for match in re.finditer(
        r"(?is)(?:Location|Standort|City)\s*</[^>]+>\s*<[^>]+>(.*?)</[^>]+>", markup
    ):
        token = _strip_html_tags(match.group(1))
        if token:
            location = token
            break

    website_url = ""
    careers_url = ""
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
        if any(
            host == blocked or host.endswith(f".{blocked}")
            for blocked in GAMESMAP_IGNORED_WEBSITE_HOSTS
        ):
            continue
        label = _strip_html_tags(match.group(2))
        context_start = max(0, match.start() - 140)
        context = _strip_html_tags(markup[context_start : match.end()])
        hint_blob = f"{label} {absolute} {context}".lower()
        if any(
            token in hint_blob
            for token in CAREERS_URL_HINTS + ("job page", "job pages", "stellen", "karriere")
        ):
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


def _gamesmap_singularize_word(word: str) -> str:
    token = str(word or "").strip().lower()
    if len(token) <= 3:
        return token
    if token.endswith("ies") and len(token) > 4:
        return f"{token[:-3]}y"
    if token.endswith("s") and not token.endswith(("ss", "us", "is")):
        return token[:-1]
    return token


def _gamesmap_category_words(value: str) -> list[str]:
    normalized = normalize_gamesmap_category_token(value)
    if not normalized:
        return []
    return [
        _gamesmap_singularize_word(part)
        for part in normalized.split()
        if _gamesmap_singularize_word(part)
    ]


def _gamesmap_phrase_matches(category_words: list[str], target_words: list[str]) -> bool:
    if not category_words or not target_words or len(target_words) > len(category_words):
        return False
    window_size = len(target_words)
    return any(
        category_words[idx : idx + window_size] == target_words
        for idx in range(0, len(category_words) - window_size + 1)
    )


def gamesmap_matches_category(
    categories: Iterable[str], allowed: Iterable[str], blocked: Iterable[str]
) -> bool:
    category_words = [
        _gamesmap_category_words(str(item)) for item in categories if str(item or "").strip()
    ]
    category_words = [words for words in category_words if words]
    if not category_words:
        return False
    blocked_phrases = [
        _gamesmap_category_words(str(item)) for item in blocked if str(item or "").strip()
    ]
    blocked_phrases = [phrase for phrase in blocked_phrases if phrase]
    if any(
        _gamesmap_phrase_matches(words, blocked_phrase)
        for words in category_words
        for blocked_phrase in blocked_phrases
    ):
        return False
    allowed_tokens = [
        normalize_gamesmap_category_token(item) for item in allowed if str(item or "").strip()
    ]
    if not allowed_tokens:
        return True
    aggregate_word_set = {word for words in category_words for word in words}
    for words in category_words:
        word_set = set(words)
        for token in allowed_tokens:
            if not token:
                continue
            if token == "developer and publisher":
                if {"developer", "publisher"} <= aggregate_word_set:
                    return True
                continue
            if token in {"developer", "publisher", "mobile", "browser", "online", "vr", "ar"}:
                if token in word_set:
                    return True
                continue
            if token == "pc":
                if "pc" in word_set or {"console", "pc"} <= word_set:
                    return True
                continue
            if token == "console":
                if "console" in word_set or {"console", "pc"} <= word_set:
                    return True
                continue
            target_words = _gamesmap_category_words(token)
            if target_words and _gamesmap_phrase_matches(words, target_words):
                return True
    return False


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
    if website_only:
        evidence_score = 24
        evidence_types.append("gamesmap_website")
        evidence_types.append("gamesmap_website_only")
        if manual_only:
            evidence_types.append("gamesmap_manual_website_only")
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
            "careersUrl": "",
            "evidenceSource": "gamesmap",
            "evidenceTypes": evidence_types,
            "evidenceScore": evidence_score,
            "weakSignal": True,
            "sourceDirectory": "gamesmap",
            "sourceDirectoryUrl": "https://www.gamesmap.de/",
            "sourceDirectoryEntryUrl": detail_url,
            "sourceDirectoryCategories": unique_string_list(categories),
            "sourceDirectoryLocation": str(location or "").strip(),
            "manualOnly": bool(manual_only),
        }
    evidence_types.append("gamesmap_careers_url")
    if location:
        evidence_types.append("gamesmap_location")
    return build_known_careers_url_candidate(
        target_url,
        studio=studio,
        name_suffix="Gamesmap",
        nl_priority=nl_priority,
        discovery_method="gamesmap",
        evidence_source="gamesmap",
        evidence_types=evidence_types,
        evidence_score=40,
        enabled_by_default=False,
        extra_fields={
            "sourceDirectory": "gamesmap",
            "sourceDirectoryUrl": "https://www.gamesmap.de/",
            "sourceDirectoryEntryUrl": detail_url,
            "sourceDirectoryCategories": unique_string_list(categories),
            "sourceDirectoryLocation": str(location or "").strip(),
            "manualOnly": bool(manual_only),
        },
    )


def _apply_gamesmap_provider_provenance(
    candidate: dict[str, Any],
    *,
    detail_url: str,
    website_url: str,
    categories: list[str],
    location: str,
    fetched_website: bool = False,
) -> dict[str, Any]:
    enriched = dict(candidate)
    evidence_types = [
        *(enriched.get("evidenceTypes") or []),
        "gamesmap_directory",
        "gamesmap_category_match",
        "gamesmap_website",
    ]
    if fetched_website:
        evidence_types.append("gamesmap_website_fetch")
    if location:
        evidence_types.append("gamesmap_location")
    enriched["evidenceSource"] = "gamesmap"
    enriched["evidenceTypes"] = unique_string_list(evidence_types)
    enriched["evidenceScore"] = max(int(enriched.get("evidenceScore") or 0), 44)
    enriched["careersUrl"] = str(enriched.get("careersUrl") or website_url).strip() or website_url
    enriched["sourceDirectory"] = "gamesmap"
    enriched["sourceDirectoryUrl"] = "https://www.gamesmap.de/"
    enriched["sourceDirectoryEntryUrl"] = detail_url
    enriched["sourceDirectoryCategories"] = unique_string_list(categories)
    enriched["sourceDirectoryLocation"] = str(location or "").strip()
    return enriched


def _apply_gamesmap_static_provenance(
    candidate: dict[str, Any],
    *,
    detail_url: str,
    website_url: str,
    categories: list[str],
    location: str,
    fetched_website: bool = False,
) -> dict[str, Any]:
    enriched = dict(candidate)
    evidence_types = [
        *(enriched.get("evidenceTypes") or []),
        "gamesmap_directory",
        "gamesmap_category_match",
        "gamesmap_website",
    ]
    if fetched_website:
        evidence_types.append("gamesmap_website_fetch")
    if location:
        evidence_types.append("gamesmap_location")
    enriched["name"] = f"{str(enriched.get('studio') or '').strip()} (Gamesmap)"
    enriched["evidenceSource"] = "gamesmap"
    enriched["evidenceTypes"] = unique_string_list(evidence_types)
    enriched["sourceDirectory"] = "gamesmap"
    enriched["sourceDirectoryUrl"] = "https://www.gamesmap.de/"
    enriched["sourceDirectoryEntryUrl"] = detail_url
    enriched["sourceDirectoryCategories"] = unique_string_list(categories)
    enriched["sourceDirectoryLocation"] = str(location or "").strip()
    enriched["careersUrl"] = str(enriched.get("careersUrl") or website_url).strip() or website_url
    return enriched


def discover_gamesmap_candidates(
    timeout_s: int,
    *,
    config: dict[str, Any] | None = None,
    fetcher=fetch_text,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    from .reporting import emit_log

    cfg = dict(_gamesmap_config_value(config, "gamesmap", DEFAULT_DISCOVERY_CONFIG["gamesmap"]))
    if not bool(cfg.get("enabled")):
        emit_log("Gamesmap directory disabled, skipping.")
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
    fetch_concurrency, per_host_concurrency = resolve_directory_fetch_limits(cfg)

    provider_candidates: list[dict[str, Any]] = []
    static_candidates: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    detail_entries: list[dict[str, Any]] = []
    seen_details = set()
    unresolved_reference_count = 0

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
        parsed_entries, diagnostics = _parse_gamesmap_index_entries_with_diagnostics(
            index_html,
            base_url,
            prefer_english=prefer_english,
        )
        unresolved_reference_count += int(diagnostics.get("unresolvedReferenceCount") or 0)
        if not parsed_entries:
            failures.append(
                {
                    "name": index_url,
                    "adapter": "gamesmap",
                    "error": "no entries parsed from index",
                    "stage": "directory_index_parse",
                }
            )
            continue
        for entry in parsed_entries:
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

    rows_with_website = sum(
        1 for entry in detail_entries if str(entry.get("websiteUrl") or "").strip()
    )
    if not detail_entries:
        emit_log(
            "Gamesmap parsed entries: "
            f"rows=0, withWebsite=0, eligibleAfterFilter=0, unresolvedCategoryRefs={unresolved_reference_count}."
        )
        _write_gamesmap_cache(
            config,
            cfg,
            provider_candidates=provider_candidates,
            static_candidates=static_candidates,
            failures=failures,
        )
        return [], [], failures

    homepage_entries: list[dict[str, Any]] = []
    eligible_entries = 0
    for entry in detail_entries:
        detail_url = str(entry.get("detailUrl") or "").strip()
        studio = str(entry.get("studio") or "").strip()
        categories = list(entry.get("categories") or [])
        if not studio or not gamesmap_matches_category(categories, allowed_tokens, blocked_tokens):
            continue
        location = str(entry.get("location") or "").strip()
        website_url = str(entry.get("websiteUrl") or "").strip()
        if not website_url:
            continue
        eligible_entries += 1
        nl_priority = False
        inferred = infer_web_candidate(
            website_url, studio, nl_priority=nl_priority, discovery_method="gamesmap"
        )
        if inferred:
            provider_candidates.append(
                _apply_gamesmap_provider_provenance(
                    inferred,
                    detail_url=detail_url,
                    website_url=website_url,
                    categories=categories,
                    location=location,
                    fetched_website=False,
                )
            )
            continue
        homepage_entries.append(entry)

    emit_log(
        "Gamesmap parsed entries: "
        f"rows={len(detail_entries)}, withWebsite={rows_with_website}, "
        f"eligibleAfterFilter={eligible_entries}, unresolvedCategoryRefs={unresolved_reference_count}."
    )
    emit_log(f"Gamesmap homepage fetch jobs: {len(homepage_entries)}")
    homepage_fetch_results = fetch_directory_pages(
        timeout_s,
        [
            {
                "url": str(entry.get("websiteUrl") or "").strip(),
                "payload": entry,
                "name": str(entry.get("websiteUrl") or "").strip(),
                "adapter": "gamesmap",
                "failureStage": "website_fetch",
            }
            for entry in homepage_entries
            if str(entry.get("websiteUrl") or "").strip()
        ],
        fetcher=fetcher,
        total_concurrency=fetch_concurrency,
        per_host_concurrency=per_host_concurrency,
        progress_label="Gamesmap website fetch",
    )

    for result in homepage_fetch_results:
        entry = dict(result.get("payload") or {})
        detail_url = str(entry.get("detailUrl") or "").strip()
        studio = str(entry.get("studio") or "").strip()
        categories = list(entry.get("categories") or [])
        location = str(entry.get("location") or "").strip()
        website_url = str(result.get("url") or entry.get("websiteUrl") or "").strip()
        if not bool(result.get("ok")):
            failure = result.get("failure")
            if isinstance(failure, dict):
                failures.append(failure)
            continue
        website_html = str(result.get("text") or "")
        analyzed = analyze_fetched_page(
            page_url=website_url,
            html=website_html,
            studio=studio,
            nl_priority=False,
            discovery_method="gamesmap",
        )
        providers = list(analyzed.get("provider_candidates") or [])
        if providers:
            for inferred in providers:
                provider_candidates.append(
                    _apply_gamesmap_provider_provenance(
                        inferred,
                        detail_url=detail_url,
                        website_url=website_url,
                        categories=categories,
                        location=location,
                        fetched_website=True,
                    )
                )
            continue
        explicit_careers_url = str(analyzed.get("explicit_careers_url") or "").strip()
        if explicit_careers_url:
            static_candidate = build_gamesmap_static_candidate(
                studio=studio,
                target_url=explicit_careers_url,
                nl_priority=False,
                website_only=False,
                detail_url=detail_url,
                categories=categories,
                location=location,
            )
            static_candidate["evidenceTypes"] = unique_string_list(
                [*(static_candidate.get("evidenceTypes") or []), "gamesmap_website_fetch"]
            )
            static_candidates.append(static_candidate)
            continue
        generic_static_candidate = analyzed.get("generic_static_candidate")
        if generic_static_candidate:
            static_candidates.append(
                _apply_gamesmap_static_provenance(
                    generic_static_candidate,
                    detail_url=detail_url,
                    website_url=website_url,
                    categories=categories,
                    location=location,
                    fetched_website=True,
                )
            )
            continue
        if website_only_fallback:
            static_candidate = build_gamesmap_static_candidate(
                studio=studio,
                target_url=website_url,
                nl_priority=False,
                website_only=True,
                detail_url=detail_url,
                categories=categories,
                location=location,
                manual_only=website_only_manual_only,
            )
            static_candidate["evidenceTypes"] = unique_string_list(
                [*(static_candidate.get("evidenceTypes") or []), "gamesmap_website_fetch"]
            )
            static_candidates.append(static_candidate)

    provider_candidates = unique_sources(provider_candidates)
    static_candidates = unique_sources(static_candidates)
    emit_log(
        "Gamesmap candidates: "
        f"provider={len(provider_candidates)}, static={len(static_candidates)}, failures={len(failures)}."
    )
    _write_gamesmap_cache(
        config,
        cfg,
        provider_candidates=provider_candidates,
        static_candidates=static_candidates,
        failures=failures,
    )
    return provider_candidates, static_candidates, failures
