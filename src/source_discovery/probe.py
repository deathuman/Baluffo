"""Probe discovery candidates (validate, fetch, parse job count)."""
from __future__ import annotations

import asyncio
import json
import re
from typing import Any, Dict, List, Tuple
from urllib.parse import urlparse
from xml.etree import ElementTree as ET

from .io_runtime import endpoint_url
from .web_search import (
    extract_jobish_links,
    fetch_text,
    fetch_text_with_retry,
    async_fetch_text_with_retry,
)


def _is_valid_identity_token(token: str) -> bool:
    value = str(token or "").strip()
    return bool(
        len(value) >= 3
        and re.match(r"^[A-Za-z0-9][A-Za-z0-9_-]*$", value)
        and not value.isdigit()
    )


def validate_candidate_for_probe(candidate: Dict[str, Any]) -> Tuple[bool, str]:
    adapter = str(candidate.get("adapter") or "").strip().lower()
    if adapter in {"lever", "workable"}:
        token = str(candidate.get("account") or "").strip()
        return (
            (_is_valid_identity_token(token), "" if _is_valid_identity_token(token) else "invalid account token")
        )
    if adapter == "greenhouse":
        slug = str(candidate.get("slug") or "").strip()
        return (_is_valid_identity_token(slug), "" if _is_valid_identity_token(slug) else "invalid board slug")
    if adapter == "smartrecruiters":
        company_id = str(candidate.get("company_id") or "").strip()
        valid = len(company_id) >= 3 and bool(re.search(r"[A-Za-z]", company_id))
        return (valid, "" if valid else "invalid company identifier")
    if adapter == "personio":
        host = (urlparse(str(candidate.get("feed_url") or "")).netloc or "").lower()
        return (".jobs.personio.de" in host, "" if ".jobs.personio.de" in host else "invalid personio host")
    if adapter == "teamtailor":
        parsed = urlparse(str(candidate.get("listing_url") or "").strip())
        host = (parsed.netloc or "").lower()
        path = (parsed.path or "").lower()
        valid = ".teamtailor.com" in host or path.startswith("/jobs")
        return (valid, "" if valid else "invalid teamtailor host")
    if adapter == "ashby":
        host = (urlparse(str(candidate.get("board_url") or "").strip()).netloc or "").lower()
        return ("ashbyhq.com" in host, "" if "ashbyhq.com" in host else "invalid ashby host")
    if adapter == "static":
        listing = str(candidate.get("listing_url") or "").strip()
        pages = candidate.get("pages")
        valid = bool(listing) or bool(
            isinstance(pages, list) and any(str(item or "").strip() for item in pages)
        )
        return (valid, "" if valid else "invalid static source")
    return True, ""


def fallback_probe_urls(candidate: Dict[str, Any]) -> List[str]:
    adapter = str(candidate.get("adapter") or "").strip().lower()
    urls: List[str] = []
    if adapter == "greenhouse":
        slug = str(candidate.get("slug") or "").strip()
        if slug:
            urls.append(f"https://boards.greenhouse.io/{slug}")
    elif adapter == "lever":
        account = str(candidate.get("account") or "").strip()
        if account:
            urls.append(f"https://jobs.lever.co/{account}")
    elif adapter == "smartrecruiters":
        company_id = str(candidate.get("company_id") or "").strip()
        if company_id:
            urls.append(f"https://jobs.smartrecruiters.com/{company_id}")
    elif adapter == "workable":
        account = str(candidate.get("account") or "").strip()
        if account:
            urls.append(f"https://apply.workable.com/{account}")
    elif adapter == "personio":
        host = (urlparse(str(candidate.get("feed_url") or "")).netloc or "").strip()
        if host:
            urls.append(f"https://{host}/")
    return urls


def parse_probe_count(adapter: str, text: str) -> int:
    if adapter == "lever":
        if text.strip().startswith("{"):
            payload = json.loads(text)
            postings = payload.get("data", []) if isinstance(payload, dict) else []
            return len(postings) if isinstance(postings, list) else 0
        payload = json.loads(text)
        return len(payload) if isinstance(payload, list) else 0
    if adapter == "greenhouse":
        if text.strip().startswith("{"):
            payload = json.loads(text)
            return len(payload.get("jobs", [])) if isinstance(payload, dict) else 0
        return len(set(re.findall(r'(?is)href=["\'][^"\']+/jobs/\d+[^"\']*["\']', text)))
    if adapter == "smartrecruiters":
        if text.strip().startswith("{"):
            payload = json.loads(text)
            return len(payload.get("content", [])) if isinstance(payload, dict) else 0
        return len(set(re.findall(r'(?is)href=["\'][^"\']+/job/[^"\']+["\']', text)))
    if adapter == "workable":
        if text.strip().startswith("{"):
            payload = json.loads(text)
            return len(payload.get("jobs", [])) if isinstance(payload, dict) else 0
        return len(set(re.findall(r'(?is)href=["\'][^"\']+/j/[^"\']+["\']', text)))
    if adapter == "personio":
        if text.lstrip().startswith("<"):
            return len(ET.fromstring(text).findall(".//position"))
        return 0
    if adapter == "ashby":
        return len(set(re.findall(r'(?is)<a[^>]+href=["\']([^"\']+/job/[^"\']+)["\']', text)))
    if adapter == "teamtailor":
        return len(set(re.findall(r'(?is)<a[^>]+href=["\']([^"\']+/jobs/[^"\']+)["\']', text)))
    if adapter == "static":
        return len(extract_jobish_links(text, ""))
    raise ValueError("unsupported adapter")


def probe_candidate(
    candidate: Dict[str, Any],
    timeout_s: int,
    *,
    fetcher=fetch_text,
) -> Tuple[bool, int, str]:
    adapter = str(candidate.get("adapter") or "").strip().lower()
    url = endpoint_url(candidate)
    if not adapter or not url:
        return False, 0, "missing adapter or URL"
    valid, reason = validate_candidate_for_probe(candidate)
    if not valid:
        return False, 0, reason
    probe_urls = [url, *fallback_probe_urls(candidate)]
    seen_urls = set()
    last_error = "probe failed"
    for probe_url in probe_urls:
        if not probe_url or probe_url in seen_urls:
            continue
        seen_urls.add(probe_url)
        try:
            text = fetch_text_with_retry(probe_url, timeout_s, adapter=adapter, fetcher=fetcher)
            return True, max(0, int(parse_probe_count(adapter, text))), ""
        except Exception as exc:  # noqa: BLE001
            last_error = f"{probe_url}: {exc}"
    return False, 0, last_error


async def async_probe_candidate(
    candidate: Dict[str, Any],
    timeout_s: int,
    *,
    fetcher,
) -> Tuple[bool, int, str]:
    adapter = str(candidate.get("adapter") or "").strip().lower()
    url = endpoint_url(candidate)
    if not adapter or not url:
        return False, 0, "missing adapter or URL"
    valid, reason = validate_candidate_for_probe(candidate)
    if not valid:
        return False, 0, reason
    probe_urls = [url, *fallback_probe_urls(candidate)]
    seen_urls = set()
    last_error = "probe failed"
    for probe_url in probe_urls:
        if not probe_url or probe_url in seen_urls:
            continue
        seen_urls.add(probe_url)
        try:
            text = await async_fetch_text_with_retry(probe_url, timeout_s, adapter=adapter, fetcher=fetcher)
            return True, max(0, int(parse_probe_count(adapter, text))), ""
        except Exception as exc:  # noqa: BLE001
            last_error = f"{probe_url}: {exc}"
    return False, 0, last_error
