from __future__ import annotations

import re
from urllib.parse import parse_qs, unquote, urljoin, urlparse

from .config import CAREERS_URL_HINTS, GENERIC_STATIC_BLOCKED_DOMAINS

# Compiled once: the phase runs it over every fetched body, and `re`'s module cache only hides the
# lookup, not the argument hashing.
_HREF_PATTERN = re.compile(r'(?is)href=["\']([^"\']+)["\']')
_JOBISH_TOKENS = CAREERS_URL_HINTS + ("job", "position", "opening", "vacancy")


def is_blocked_generic_static_url(url: str) -> bool:
    try:
        host = (urlparse(str(url or "")).netloc or "").lower()
    except ValueError:
        return False
    host = host.lstrip(".")
    return any(
        host == domain or host.endswith(f".{domain}") for domain in GENERIC_STATIC_BLOCKED_DOMAINS
    )


def href_values(html: str) -> list[str]:
    """Every quoted `href` value in the body, in document order."""
    return _HREF_PATTERN.findall(html)


def links_from_hrefs(hrefs: list[str] | tuple[str, ...]) -> list[str]:
    """`extract_links_from_html` for href values a caller already scanned."""
    out: list[str] = []
    for raw in hrefs:
        if not raw:
            continue
        if "uddg=" in raw:
            query = parse_qs(urlparse(raw).query)
            target = query.get("uddg", [""])[0]
            if target:
                out.append(unquote(target))
        elif raw.startswith("http://") or raw.startswith("https://"):
            out.append(raw)
    return out


def jobish_links_from_hrefs(
    hrefs: list[str] | tuple[str, ...],
    base_url: str,
) -> list[str]:
    """`extract_jobish_links` for href values a caller already scanned."""
    out: list[str] = []
    seen = set()
    for raw in hrefs:
        if (
            not raw
            or raw.startswith("#")
            or raw.startswith("mailto:")
            or raw.startswith("javascript:")
        ):
            continue
        absolute = urljoin(base_url, raw) if base_url else raw
        parsed = urlparse(absolute)
        text = f"{parsed.path} {absolute}".lower()
        if not any(token in text for token in _JOBISH_TOKENS):
            continue
        normalized = absolute.split("#", 1)[0]
        if normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out


def extract_jobish_links(html: str, base_url: str) -> list[str]:
    return jobish_links_from_hrefs(href_values(str(html or "")), base_url)


def extract_links_from_html(html: str) -> list[str]:
    return links_from_hrefs(href_values(html))
