from __future__ import annotations

import re
from urllib.parse import parse_qs, unquote, urljoin, urlparse

from .config import CAREERS_URL_HINTS, GENERIC_STATIC_BLOCKED_DOMAINS


def is_blocked_generic_static_url(url: str) -> bool:
    try:
        host = (urlparse(str(url or "")).netloc or "").lower()
    except ValueError:
        return False
    host = host.lstrip(".")
    return any(
        host == domain or host.endswith(f".{domain}") for domain in GENERIC_STATIC_BLOCKED_DOMAINS
    )


def extract_jobish_links(html: str, base_url: str) -> list[str]:
    matches = re.findall(r'(?is)href=["\']([^"\']+)["\']', str(html or ""))
    out: list[str] = []
    seen = set()
    for raw in matches:
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
        if not any(
            token in text for token in CAREERS_URL_HINTS + ("job", "position", "opening", "vacancy")
        ):
            continue
        normalized = absolute.split("#", 1)[0]
        if normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out


def extract_links_from_html(html: str) -> list[str]:
    links = re.findall(r'(?is)href=["\']([^"\']+)["\']', html)
    out: list[str] = []
    for raw in links:
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
