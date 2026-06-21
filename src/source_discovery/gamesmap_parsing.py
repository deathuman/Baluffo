"""GamesMap parsing helpers.

AI boundary owns: GamesMap HTML/text parsing and studio/source extraction primitives.
AI boundary implement in: this file for GamesMap parsing primitives; candidate scoring and orchestration stay in sibling leaves.
AI boundary search before contracts: GamesMap candidate helpers, gamesmap tests, and discovery generation tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused GamesMap parsing tests.
"""

from __future__ import annotations

import json
import re
from typing import Any
from urllib.parse import urljoin, urlparse

from .config import CAREERS_URL_HINTS
from .scoring import unique_string_list

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
_GAMESMAP_CATEGORY_REFERENCE_RE = re.compile(
    r"^\$[^:]*:props:children:props:children:props:children:props:companies:(\d+):categories:(\d+)$"
)


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _strip_html_tags(html: str) -> str:
    text = re.sub(r"(?is)<script\b[^>]*>.*?</script\b[^>]*>", " ", str(html or ""))
    text = re.sub(r"(?is)<style\b[^>]*>.*?</style\b[^>]*>", " ", text)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


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
                    payload = json.loads(html[array_start : idx + 1])
                except json.JSONDecodeError:
                    return None
                return payload if isinstance(payload, list) else None
    return None


def _advance_json_string_state(
    char: str, *, in_string: bool, escape: bool
) -> tuple[bool, bool, bool]:
    if not in_string:
        return char == '"', False, False
    if escape:
        return True, False, True
    if char == "\\":
        return True, True, True
    if char == '"':
        return False, False, True
    return True, False, True


def _json_array_end(markup: str, array_start: int) -> int | None:
    depth = 0
    in_string = False
    escape = False
    for idx in range(array_start, len(markup)):
        char = markup[idx]
        in_string, escape, consumed = _advance_json_string_state(
            char,
            in_string=in_string,
            escape=escape,
        )
        if consumed:
            continue
        if char == "[":
            depth += 1
        elif char == "]":
            depth -= 1
            if depth == 0:
                return idx
    return None


def _decode_json_array(markup: str, array_start: int, array_end: int) -> list[Any] | None:
    try:
        payload = json.loads(markup[array_start : array_end + 1])
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, list) else None


def _extract_json_array(markup: str, array_start: int) -> list[Any] | None:
    array_end = _json_array_end(markup, array_start)
    if array_end is None:
        return None
    return _decode_json_array(markup, array_start, array_end)


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
    address = _as_dict(company.get("address"))
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
    categories_raw = _as_list(source_company.get("categories"))
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
    categories_raw = _as_list(company.get("categories"))
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
    websites = _as_list(company.get("websites"))
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


def _gamesmap_js_industry_points(payload: list[Any]) -> list[dict[str, Any]]:
    for item in payload:
        if not (
            isinstance(item, list)
            and len(item) >= 2
            and item[0] == "map.coordinates"
            and isinstance(item[1], dict)
        ):
            continue
        item_payload = _as_dict(item[1])
        points = _as_dict(item_payload.get("points"))
        return [point for point in _as_list(points.get("industry")) if isinstance(point, dict)]
    return []


def _gamesmap_industry_point_detail_url(
    point: dict[str, Any], base_url: str, *, prefer_english: bool
) -> str:
    slug = str(point.get("slug") or "").strip().strip("/")
    if not slug:
        return ""
    detail_url = f"/en/detail/industry/{slug}" if prefer_english else f"/detail/industry/{slug}"
    return urljoin(base_url.rstrip("/") + "/", detail_url.lstrip("/"))


def _gamesmap_industry_point_location(point: dict[str, Any], *, prefer_english: bool) -> str:
    province = _as_dict(point.get("province"))
    return str(
        (province.get("nameEn") if prefer_english else province.get("name"))
        or province.get("nameEn")
        or province.get("name")
        or ""
    ).strip()


def _normalize_gamesmap_industry_point(
    point: dict[str, Any], base_url: str, *, prefer_english: bool
) -> dict[str, Any] | None:
    studio = str(point.get("name") or "").strip()
    slug = str(point.get("slug") or "").strip().strip("/")
    detail_url = _gamesmap_industry_point_detail_url(
        point,
        base_url,
        prefer_english=prefer_english,
    )
    if not detail_url or not studio:
        return None
    return {
        "detailUrl": detail_url,
        "studio": studio,
        "location": _gamesmap_industry_point_location(point, prefer_english=prefer_english),
        "categories": [],
        "websiteUrl": "",
        "slug": slug,
    }


def _append_unique_gamesmap_entry(
    out: list[dict[str, Any]], seen: set[str], row: dict[str, Any] | None
) -> None:
    if not isinstance(row, dict):
        return
    detail_url = str(row.get("detailUrl") or "").strip()
    if not detail_url or detail_url in seen:
        return
    seen.add(detail_url)
    out.append(row)


def _parse_gamesmap_js_data_entries(
    html: str,
    base_url: str,
    *,
    prefer_english: bool,
    seen: set[str],
) -> list[dict[str, Any]]:
    payload = _extract_gamesmap_js_data_container(html)
    if not isinstance(payload, list):
        return []
    out: list[dict[str, Any]] = []
    for point in _gamesmap_js_industry_points(payload):
        _append_unique_gamesmap_entry(
            out,
            seen,
            _normalize_gamesmap_industry_point(
                point,
                base_url,
                prefer_english=prefer_english,
            ),
        )
    return out


def _gamesmap_link_entry(detail_url: str) -> dict[str, Any]:
    return {
        "detailUrl": detail_url,
        "studio": "",
        "location": "",
        "categories": [],
        "websiteUrl": "",
        "slug": "",
    }


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
    out.extend(
        _parse_gamesmap_js_data_entries(
            html,
            base_url,
            prefer_english=prefer_english,
            seen=seen,
        )
    )
    for detail_url in parse_gamesmap_index_links(html, base_url):
        _append_unique_gamesmap_entry(out, seen, _gamesmap_link_entry(detail_url))
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


def _gamesmap_detail_name(markup: str) -> str:
    name_match = re.search(r"(?is)<h1[^>]*>(.*?)</h1>", markup)
    if name_match:
        return _strip_html_tags(name_match.group(1))
    title_match = re.search(r"(?is)<title[^>]*>(.*?)</title>", markup)
    return _strip_html_tags(title_match.group(1)) if title_match else ""


def _gamesmap_detail_tag_categories(markup: str) -> list[str]:
    return [
        token
        for token in (
            _strip_html_tags(match.group(1))
            for match in re.finditer(
                r'(?is)<[^>]+class=["\'][^"\']*(?:tag|badge|category|chip)[^"\']*["\'][^>]*>(.*?)</[^>]+>',
                markup,
            )
        )
        if token
    ]


def _gamesmap_detail_label_categories(markup: str) -> list[str]:
    out: list[str] = []
    for match in re.finditer(
        r"(?is)(?:Category|Categories|Branche|Branchen)\s*</[^>]+>\s*<[^>]+>(.*?)</[^>]+>",
        markup,
    ):
        chunk = _strip_html_tags(match.group(1))
        out.extend(part.strip() for part in re.split(r"[|,/]| â€¢ |\s{2,}", chunk) if part.strip())
    return out


def _gamesmap_detail_categories(markup: str) -> list[str]:
    return unique_string_list(
        [*_gamesmap_detail_tag_categories(markup), *_gamesmap_detail_label_categories(markup)]
    )


def _gamesmap_detail_location(markup: str) -> str:
    for match in re.finditer(
        r"(?is)(?:Location|Standort|City)\s*</[^>]+>\s*<[^>]+>(.*?)</[^>]+>", markup
    ):
        token = _strip_html_tags(match.group(1))
        if token:
            return token
    return ""


def _gamesmap_external_link(page_url: str, href: str) -> str:
    raw_href = str(href or "").strip()
    if not raw_href or raw_href.startswith(("mailto:", "javascript:", "#")):
        return ""
    absolute = urljoin(page_url, raw_href).split("#", 1)[0]
    try:
        parsed = urlparse(absolute)
        page_host = (urlparse(page_url).netloc or "").lower()
    except ValueError:
        return ""
    host = (parsed.netloc or "").lower()
    if not host or host.endswith("gamesmap.de") or host == page_host:
        return ""
    if any(
        host == blocked or host.endswith(f".{blocked}")
        for blocked in GAMESMAP_IGNORED_WEBSITE_HOSTS
    ):
        return ""
    return absolute


def _gamesmap_link_hint_blob(markup: str, match: re.Match[str], absolute: str) -> str:
    label = _strip_html_tags(match.group(2))
    context_start = max(0, match.start() - 140)
    context = _strip_html_tags(markup[context_start : match.end()])
    return f"{label} {absolute} {context}".lower()


def _gamesmap_link_is_careers(markup: str, match: re.Match[str], absolute: str) -> bool:
    hint_blob = _gamesmap_link_hint_blob(markup, match, absolute)
    return any(
        token in hint_blob
        for token in CAREERS_URL_HINTS + ("job page", "job pages", "stellen", "karriere")
    )


def _gamesmap_detail_external_links(page_url: str, markup: str) -> tuple[str, str]:
    website_url = ""
    careers_url = ""
    for match in re.finditer(r'(?is)<a\b[^>]*href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', markup):
        absolute = _gamesmap_external_link(page_url, str(match.group(1) or ""))
        if not absolute:
            continue
        if _gamesmap_link_is_careers(markup, match, absolute) and not careers_url:
            careers_url = absolute
            continue
        if not website_url:
            website_url = absolute
    return website_url, careers_url


def _gamesmap_website_from_careers(careers_url: str) -> str:
    if not careers_url:
        return ""
    try:
        parsed = urlparse(careers_url)
    except ValueError:
        return ""
    return f"{parsed.scheme}://{parsed.netloc}"


def parse_gamesmap_detail_page(page_url: str, html: str) -> dict[str, Any] | None:
    markup = str(html or "")
    name = _gamesmap_detail_name(markup)
    if not name:
        return None

    categories = _gamesmap_detail_categories(markup)
    location = _gamesmap_detail_location(markup)
    website_url, careers_url = _gamesmap_detail_external_links(page_url, markup)
    if not website_url and careers_url:
        website_url = _gamesmap_website_from_careers(careers_url)

    return {
        "studio": name,
        "detailUrl": page_url,
        "websiteUrl": website_url,
        "careersUrl": careers_url,
        "categories": categories,
        "location": location,
    }
