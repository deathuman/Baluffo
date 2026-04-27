from __future__ import annotations

"""Shared recovery URL planning helpers for source-discovery adapters."""

import html as html_lib
from collections.abc import Callable
from re import Pattern
from urllib.parse import unquote, urljoin, urlparse

from src.shared.regex import find_urls_in_text

from .web_search import extract_jobish_links, extract_links_from_html


def host(url: str) -> str:
    try:
        return (urlparse(str(url or "")).netloc or "").lower().lstrip(".")
    except ValueError:
        return ""


def registrable_host(host_value: str) -> str:
    parts = [part for part in str(host_value or "").lower().split(".") if part]
    if len(parts) <= 2:
        return ".".join(parts)
    if len(parts[-2]) <= 3 and len(parts[-1]) == 2 and len(parts) >= 3:
        return ".".join(parts[-3:])
    return ".".join(parts[-2:])


def same_party_url(source_url: str, candidate_url: str) -> bool:
    source_host = host(source_url)
    candidate_host = host(candidate_url)
    if not source_host or not candidate_host:
        return False
    if source_host == candidate_host:
        return True
    return registrable_host(source_host) == registrable_host(candidate_host)


def host_in(host_value: str, blocked_hosts: set[str]) -> bool:
    normalized = str(host_value or "").lower().lstrip(".")
    return any(normalized == item or normalized.endswith(f".{item}") for item in blocked_hosts)


def html_url_candidates(
    html: str,
    *,
    provider_url_pattern: Pattern[str] | None = None,
) -> list[str]:
    text = html_lib.unescape(str(html or "")).replace("\\/", "/")
    candidates = [*extract_links_from_html(text), *find_urls_in_text(text)]
    if provider_url_pattern is not None:
        for raw in provider_url_pattern.findall(text):
            candidates.append(unquote(raw))
    out: list[str] = []
    seen = set()
    for raw in candidates:
        url = str(raw or "").strip().strip("\"'.,;)")
        if not url or url in seen:
            continue
        seen.add(url)
        out.append(url)
    return out


def common_recovery_urls(
    page_url: str,
    paths: tuple[str, ...],
    *,
    blocked_hosts: set[str] | None = None,
) -> list[str]:
    parsed = urlparse(page_url)
    if not parsed.scheme or not parsed.netloc:
        return []
    if blocked_hosts and host_in(parsed.netloc, blocked_hosts):
        return []
    origin = f"{parsed.scheme}://{parsed.netloc}"
    return [urljoin(origin, path) for path in paths]


def same_party_jobish_urls(
    page_url: str,
    html: str,
    *,
    html_url_candidate_fn: Callable[[str], list[str]] | None = None,
) -> list[str]:
    candidates = [*extract_jobish_links(html, page_url)]
    candidate_fn = html_url_candidate_fn or html_url_candidates
    for raw_url in candidate_fn(html):
        if not same_party_url(page_url, raw_url):
            continue
        parsed = urlparse(raw_url)
        text = f"{parsed.path} {raw_url}".lower()
        if any(token in text for token in ("career", "jobs", "join", "opening", "vacancy")):
            candidates.append(raw_url)
    out: list[str] = []
    seen = set()
    for raw_url in candidates:
        url = str(raw_url or "").split("#", 1)[0].strip()
        if not url or url in seen or not same_party_url(page_url, url):
            continue
        seen.add(url)
        out.append(url)
    return out


def recovery_urls(
    page_url: str,
    html: str,
    *,
    paths: tuple[str, ...],
    limit: int,
    blocked_hosts: set[str] | None = None,
    include_jobish_links: bool = True,
    html_url_candidate_fn: Callable[[str], list[str]] | None = None,
) -> list[str]:
    page_host = host(page_url)
    if blocked_hosts and host_in(page_host, blocked_hosts):
        return []
    out: list[str] = []
    seen = set()
    jobish_urls = (
        same_party_jobish_urls(
            page_url,
            html,
            html_url_candidate_fn=html_url_candidate_fn,
        )
        if include_jobish_links
        else []
    )
    for candidate_url in [
        *jobish_urls,
        *common_recovery_urls(page_url, paths, blocked_hosts=blocked_hosts),
    ]:
        normalized = str(candidate_url or "").split("#", 1)[0].strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
        if len(out) >= limit:
            break
    return out
