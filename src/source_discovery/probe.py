"""Probe discovery candidates (validate, fetch, parse job count)."""

from __future__ import annotations

import asyncio
import json
import re
import sys
from collections.abc import Callable
from html import unescape
from typing import Any
from urllib.parse import urljoin, urlparse
from xml.etree import ElementTree as ET

from src.jobs.parsers import parse_jobpostings_from_html

from .io_runtime import endpoint_url
from .web_search import (
    async_fetch_text_with_retry,
    fetch_text,
    fetch_text_with_retry,
)

# Optional Playwright fallback: (url, timeout_s) -> (html, error). Used only for static adapter.
TryPlaywrightFn = Callable[[str, int], tuple[str, str]]


_STATIC_DETAIL_PATH_RE = re.compile(r"(?i)/(?:jobs?|positions?|openings?|vacancies?)/[^/?#]+")

_GENERIC_APPLICATION_TOKENS = (
    "can't find",
    "cannot find",
    "general application",
    "open application",
    "submit your application",
    "spontaneous application",
    "speculative application",
    "talent community",
    "unsolicited application",
)


def _anchor_links(html: str) -> list[tuple[str, str]]:
    anchors: list[tuple[str, str]] = []
    for match in re.finditer(r"(?is)<a\b(?P<attrs>[^>]*)>(?P<label>.*?)</a>", str(html or "")):
        attrs = match.group("attrs") or ""
        href_match = re.search(r'(?is)href=["\']([^"\']+)["\']', attrs)
        if not href_match:
            continue
        label = re.sub(r"(?is)<[^>]+>", " ", match.group("label") or "")
        label = " ".join(unescape(label).split()).strip().lower()
        anchors.append((href_match.group(1), label))
    return anchors


def _is_generic_application_link(label: str, parsed_path: str) -> bool:
    text = f"{label} {parsed_path}".lower()
    return any(token in text for token in _GENERIC_APPLICATION_TOKENS)


def _static_probe_count(text: str, base_url: str) -> int:
    jobs = parse_jobpostings_from_html(
        text,
        base_url=base_url,
        fallback_source_id_prefix="static-probe",
    )
    if jobs:
        return len(jobs)

    links = _anchor_links(text)
    base = urlparse(base_url or "")
    base_page = (base.scheme, base.netloc, base.path.rstrip("/") or "/")
    seen: set[str] = set()
    for raw, label in links:
        value = str(raw or "").strip()
        if (
            not value
            or value.startswith("#")
            or value.startswith("mailto:")
            or value.startswith("javascript:")
        ):
            continue
        absolute = urljoin(base_url, value) if base_url else value
        parsed = urlparse(absolute)
        page = (parsed.scheme, parsed.netloc, parsed.path.rstrip("/") or "/")
        if page == base_page:
            continue
        if _is_generic_application_link(label, parsed.path):
            continue
        if not _STATIC_DETAIL_PATH_RE.search(parsed.path):
            continue
        normalized = absolute.split("#", 1)[0]
        if normalized in seen:
            continue
        seen.add(normalized)
    return len(seen)


def _is_playwright_fallback_error(error: str) -> bool:
    """True if the probe failure is worth retrying with a browser (403, timeout, challenge)."""
    if not error:
        return False
    lower = str(error).lower()
    if "403" in lower or "http error 403" in lower:
        return True
    if "timed out" in lower or "timeout" in lower:
        return True
    challenge_tokens = (
        "challenge",
        "cloudflare",
        "just a moment",
        "enable javascript",
        "captcha",
    )
    return any(tok in lower for tok in challenge_tokens)


def _is_valid_identity_token(token: str) -> bool:
    value = str(token or "").strip()
    return bool(
        len(value) >= 3 and re.match(r"^[A-Za-z0-9][A-Za-z0-9_-]*$", value) and not value.isdigit()
    )


def validate_candidate_for_probe(candidate: dict[str, Any]) -> tuple[bool, str]:
    adapter = str(candidate.get("adapter") or "").strip().lower()
    if adapter in {"lever", "workable"}:
        token = str(candidate.get("account") or "").strip()
        return (
            _is_valid_identity_token(token),
            "" if _is_valid_identity_token(token) else "invalid account token",
        )
    if adapter == "greenhouse":
        slug = str(candidate.get("slug") or "").strip()
        return (
            _is_valid_identity_token(slug),
            "" if _is_valid_identity_token(slug) else "invalid board slug",
        )
    if adapter == "smartrecruiters":
        company_id = str(candidate.get("company_id") or "").strip()
        valid = len(company_id) >= 3 and bool(re.search(r"[A-Za-z]", company_id))
        return (valid, "" if valid else "invalid company identifier")
    if adapter == "personio":
        host = (urlparse(str(candidate.get("feed_url") or "")).netloc or "").lower()
        return (
            ".jobs.personio.de" in host,
            "" if ".jobs.personio.de" in host else "invalid personio host",
        )
    if adapter == "teamtailor":
        parsed = urlparse(str(candidate.get("listing_url") or "").strip())
        host = (parsed.netloc or "").lower()
        path = (parsed.path or "").lower()
        valid = ".teamtailor.com" in host or path.startswith("/jobs")
        return (valid, "" if valid else "invalid teamtailor host")
    if adapter == "ashby":
        host = (urlparse(str(candidate.get("board_url") or "").strip()).netloc or "").lower()
        return ("ashbyhq.com" in host, "" if "ashbyhq.com" in host else "invalid ashby host")
    if adapter == "recruitee":
        host = (urlparse(str(candidate.get("api_url") or "").strip()).netloc or "").lower()
        return (
            ".recruitee.com" in host,
            "" if ".recruitee.com" in host else "invalid recruitee host",
        )
    if adapter == "pinpoint":
        host = (urlparse(str(candidate.get("api_url") or "").strip()).netloc or "").lower()
        return (
            ".pinpointhq.com" in host,
            "" if ".pinpointhq.com" in host else "invalid pinpoint host",
        )
    if adapter == "static":
        listing = str(candidate.get("listing_url") or "").strip()
        pages = candidate.get("pages")
        valid = bool(listing) or bool(
            isinstance(pages, list) and any(str(item or "").strip() for item in pages)
        )
        return (valid, "" if valid else "invalid static source")
    return True, ""


def fallback_probe_urls(candidate: dict[str, Any]) -> list[str]:
    adapter = str(candidate.get("adapter") or "").strip().lower()
    url = _fallback_probe_url(candidate, adapter)
    return [url] if url else []


def _fallback_probe_url(candidate: dict[str, Any], adapter: str) -> str:
    if adapter in {"greenhouse", "lever", "smartrecruiters", "workable"}:
        return _provider_homepage_probe_url(candidate, adapter)
    if adapter in {"recruitee", "pinpoint"}:
        return _host_probe_url(str(candidate.get("api_url") or ""))
    if adapter == "personio":
        return _host_probe_url(str(candidate.get("feed_url") or ""))
    return ""


def _provider_homepage_probe_url(candidate: dict[str, Any], adapter: str) -> str:
    if adapter == "greenhouse":
        slug = str(candidate.get("slug") or "").strip()
        return f"https://boards.greenhouse.io/{slug}" if slug else ""
    if adapter == "smartrecruiters":
        company_id = str(candidate.get("company_id") or "").strip()
        return f"https://jobs.smartrecruiters.com/{company_id}" if company_id else ""
    account = str(candidate.get("account") or "").strip()
    if not account:
        return ""
    if adapter == "lever":
        return f"https://jobs.lever.co/{account}"
    if adapter == "workable":
        return f"https://apply.workable.com/{account}"
    return ""


def _host_probe_url(raw_url: str) -> str:
    host = (urlparse(str(raw_url or "")).netloc or "").strip()
    return f"https://{host}/" if host else ""


def _json_count(text: str, key: str | None = None) -> int:
    payload = json.loads(text)
    if key is None:
        return len(payload) if isinstance(payload, list) else 0
    return len(payload.get(key, [])) if isinstance(payload, dict) else 0


def _html_link_count(text: str, pattern: str) -> int:
    return len(set(re.findall(pattern, text)))


def _json_or_html_count(text: str, *, json_key: str | None, html_pattern: str) -> int:
    if text.strip().startswith("{"):
        return _json_count(text, json_key)
    return _html_link_count(text, html_pattern)


def _parse_provider_probe_count(adapter: str, text: str) -> int | None:
    provider_specs = {
        "lever": (None, r'(?is)href=["\'][^"\']+/jobs/[^"\']+["\']'),
        "greenhouse": ("jobs", r'(?is)href=["\'][^"\']+/jobs/\d+[^"\']*["\']'),
        "smartrecruiters": ("content", r'(?is)href=["\'][^"\']+/job/[^"\']+["\']'),
        "workable": ("jobs", r'(?is)href=["\'][^"\']+/j/[^"\']+["\']'),
        "recruitee": ("offers", r'(?is)href=["\'][^"\']+/o/[^"\']+["\']'),
        "pinpoint": ("data", r'(?is)href=["\'][^"\']+/postings/[^"\']+["\']'),
    }
    spec = provider_specs.get(adapter)
    if spec is None:
        return None
    json_key, html_pattern = spec
    if adapter == "lever":
        return _json_count(text, "data") if text.strip().startswith("{") else _json_count(text)
    return _json_or_html_count(text, json_key=json_key, html_pattern=html_pattern)


def parse_probe_count(adapter: str, text: str, *, base_url: str = "") -> int:
    provider_count = _parse_provider_probe_count(adapter, text)
    if provider_count is not None:
        return provider_count
    if adapter == "personio":
        if text.lstrip().startswith("<"):
            return len(ET.fromstring(text).findall(".//position"))
        return 0
    if adapter == "ashby":
        return len(set(re.findall(r'(?is)<a[^>]+href=["\']([^"\']+/job/[^"\']+)["\']', text)))
    if adapter == "teamtailor":
        return len(set(re.findall(r'(?is)<a[^>]+href=["\']([^"\']+/jobs/[^"\']+)["\']', text)))
    if adapter == "static":
        return _static_probe_count(text, base_url)
    raise ValueError("unsupported adapter")


def _probe_urls(candidate: dict[str, Any]) -> list[str]:
    return [endpoint_url(candidate), *fallback_probe_urls(candidate)]


def _probe_fetch_urls(
    probe_urls: list[str],
    *,
    adapter: str,
    timeout_s: int,
    fetcher: Callable[[str, int], str],
) -> tuple[bool, int, str]:
    seen_urls = set()
    last_error = "probe failed"
    for probe_url in probe_urls:
        if not probe_url or probe_url in seen_urls:
            continue
        seen_urls.add(probe_url)
        try:
            text = fetch_text_with_retry(probe_url, timeout_s, adapter=adapter, fetcher=fetcher)
            return True, max(0, int(parse_probe_count(adapter, text, base_url=probe_url))), ""
        except Exception as exc:  # noqa: BLE001
            last_error = f"{probe_url}: {exc}"
    return False, 0, last_error


def _probe_with_playwright(
    probe_urls: list[str], *, timeout_s: int, try_playwright: TryPlaywrightFn | None
) -> tuple[bool, int, str] | None:
    if try_playwright is None:
        return None
    for probe_url in probe_urls[:3]:
        if not probe_url:
            continue
        try:
            html, pw_err = try_playwright(probe_url, timeout_s)
        except (OSError, RuntimeError):
            continue
        if html and not pw_err:
            try:
                count = parse_probe_count("static", html, base_url=probe_url)
            except (TypeError, ValueError, json.JSONDecodeError, ET.ParseError):
                continue
            print(
                f"[discovery] probe_playwright_fallback url={probe_url!r} success=True count={count}",
                file=sys.stderr,
                flush=True,
            )
            return True, max(0, int(count)), ""
    return None


def probe_candidate(
    candidate: dict[str, Any],
    timeout_s: int,
    *,
    fetcher=fetch_text,
    try_playwright: TryPlaywrightFn | None = None,
) -> tuple[bool, int, str]:
    adapter = str(candidate.get("adapter") or "").strip().lower()
    url = endpoint_url(candidate)
    if not adapter or not url:
        return False, 0, "missing adapter or URL"
    valid, reason = validate_candidate_for_probe(candidate)
    if not valid:
        return False, 0, reason
    probe_urls = _probe_urls(candidate)
    ok, count, last_error = _probe_fetch_urls(
        probe_urls,
        adapter=adapter,
        timeout_s=timeout_s,
        fetcher=fetcher,
    )
    if ok:
        return ok, count, last_error
    if adapter == "static" and try_playwright and _is_playwright_fallback_error(last_error):
        playwright_result = _probe_with_playwright(
            probe_urls,
            timeout_s=timeout_s,
            try_playwright=try_playwright,
        )
        if playwright_result is not None:
            return playwright_result
    return False, 0, last_error


async def _async_probe_fetch_urls(
    probe_urls: list[str],
    *,
    adapter: str,
    timeout_s: int,
    fetcher: Callable[[str, int], str],
) -> tuple[bool, int, str]:
    seen_urls = set()
    last_error = "probe failed"
    for probe_url in probe_urls:
        if not probe_url or probe_url in seen_urls:
            continue
        seen_urls.add(probe_url)
        try:
            text = await async_fetch_text_with_retry(
                probe_url, timeout_s, adapter=adapter, fetcher=fetcher
            )
            return True, max(0, int(parse_probe_count(adapter, text, base_url=probe_url))), ""
        except Exception as exc:  # noqa: BLE001
            last_error = f"{probe_url}: {exc}"
    return False, 0, last_error


async def _async_playwright_fetch(
    probe_url: str,
    *,
    timeout_s: int,
    try_playwright: TryPlaywrightFn,
    playwright_semaphore: asyncio.Semaphore | None,
) -> tuple[str, str]:
    if playwright_semaphore is not None:
        await playwright_semaphore.acquire()
    try:
        try:
            return await asyncio.to_thread(try_playwright, probe_url, timeout_s)
        except (OSError, RuntimeError) as exc:
            return "", str(exc)
    finally:
        if playwright_semaphore is not None:
            playwright_semaphore.release()


async def _async_probe_with_playwright(
    probe_urls: list[str],
    *,
    timeout_s: int,
    try_playwright: TryPlaywrightFn | None,
    playwright_semaphore: asyncio.Semaphore | None,
) -> tuple[bool, int, str] | None:
    if try_playwright is None:
        return None
    for probe_url in probe_urls[:3]:
        if not probe_url:
            continue
        html, pw_err = await _async_playwright_fetch(
            probe_url,
            timeout_s=timeout_s,
            try_playwright=try_playwright,
            playwright_semaphore=playwright_semaphore,
        )
        if html and not pw_err:
            try:
                count = parse_probe_count("static", html, base_url=probe_url)
            except (TypeError, ValueError, json.JSONDecodeError, ET.ParseError):
                continue
            print(
                f"[discovery] probe_playwright_fallback url={probe_url!r} success=True count={count}",
                file=sys.stderr,
                flush=True,
            )
            return True, max(0, int(count)), ""
    return None


async def async_probe_candidate(
    candidate: dict[str, Any],
    timeout_s: int,
    *,
    fetcher,
    try_playwright: TryPlaywrightFn | None = None,
    playwright_semaphore: asyncio.Semaphore | None = None,
) -> tuple[bool, int, str]:
    adapter = str(candidate.get("adapter") or "").strip().lower()
    url = endpoint_url(candidate)
    if not adapter or not url:
        return False, 0, "missing adapter or URL"
    valid, reason = validate_candidate_for_probe(candidate)
    if not valid:
        return False, 0, reason
    probe_urls = _probe_urls(candidate)
    ok, count, last_error = await _async_probe_fetch_urls(
        probe_urls,
        adapter=adapter,
        timeout_s=timeout_s,
        fetcher=fetcher,
    )
    if ok:
        return ok, count, last_error
    if adapter == "static" and try_playwright and _is_playwright_fallback_error(last_error):
        playwright_result = await _async_probe_with_playwright(
            probe_urls,
            timeout_s=timeout_s,
            try_playwright=try_playwright,
            playwright_semaphore=playwright_semaphore,
        )
        if playwright_result is not None:
            return playwright_result
    return False, 0, last_error
