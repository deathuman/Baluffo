"""HTTP and URL helpers for static source checking.

Fetch fallbacks (Playwright), error normalization, alternate career URL discovery,
and browser-challenge detection. Used by admin_bridge when building callables
for bridge/source_checker.check_static_source.
"""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

from src.source_registry import normalize_source_url


def try_fetch_with_playwright(url: str, timeout_s: int) -> tuple[str, str]:
    """Best-effort browser fallback for anti-bot pages; returns (html, error)."""
    try:
        from playwright.sync_api import Error as PlaywrightError
        from playwright.sync_api import sync_playwright
    except ImportError:
        return "", "browser fallback unavailable (playwright is not installed)"
    try:
        with sync_playwright() as p:
            browser = None
            page = None
            cleanup_error = ""
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=max(1, int(timeout_s)) * 1000)
            html = page.content() or ""
            for closer in (getattr(page, "close", None), getattr(browser, "close", None)):
                if not callable(closer):
                    continue
                try:
                    closer()
                except (OSError, PlaywrightError) as exc:
                    cleanup_error = cleanup_error or str(exc)
            if not html:
                return "", (
                    normalize_browser_fallback_error(cleanup_error)
                    if cleanup_error
                    else "browser fallback returned empty content"
                )
            return html, ""
    except (OSError, PlaywrightError) as exc:
        return "", normalize_browser_fallback_error(str(exc))


def normalize_browser_fallback_error(error_text: str) -> str:
    text = str(error_text or "").strip()
    lowered = text.lower()
    if any(
        token in lowered
        for token in (
            "write epipe",
            "broken pipe",
            "pipetransport",
            "target closed",
            "transport closed",
            "connection closed",
            "browser has been closed",
        )
    ):
        return "browser fallback unavailable (playwright transport closed)"
    return text


def is_browser_fallback_environment_error(error_text: str) -> bool:
    text = str(error_text or "").lower()
    if not text:
        return False
    tokens = (
        "browser fallback unavailable",
        "playwright is not installed",
        "spawn eperm",
        "permission denied",
        "access is denied",
        "operation not permitted",
        "failed to launch browser",
        "cannot launch browser",
        "could not find browser",
        "browser_type.launch",
        "executable doesn't exist",
        "executable does not exist",
        "worker spawn blocked",
        "write epipe",
        "broken pipe",
        "pipetransport",
        "target closed",
        "transport closed",
        "connection closed",
        "browser has been closed",
    )
    return any(token in text for token in tokens)


def is_http_forbidden_error(exc: Exception) -> bool:
    return bool(re.search(r"\bHTTP Error 403\b", str(exc), flags=re.I))


def normalize_error_code(error_text: str) -> str:
    text = str(error_text or "").lower()
    if is_browser_fallback_environment_error(text):
        return "browser_fallback_unavailable"
    if "http error 404" in text:
        return "not_found"
    if "http error 403" in text:
        return "forbidden"
    if "certificate verify failed" in text or "hostname mismatch" in text or "[ssl:" in text:
        return "ssl_error"
    if (
        "getaddrinfo failed" in text
        or "name or service not known" in text
        or "nodename nor servname provided" in text
    ):
        return "dns_error"
    if "timed out" in text:
        return "timeout"
    if "no job postings found" in text:
        return "no_jobs"
    return "probe_failed"


def suggest_alternate_career_urls(url: str) -> list[str]:
    parsed = urlparse(str(url or "").strip())
    host = (parsed.netloc or "").strip().lower()
    if not host:
        return []
    if ":" in host:
        host = host.split(":", 1)[0]
    labels = [part for part in host.split(".") if part]
    base_host = ".".join(labels[1:]) if labels[:1] == ["www"] and len(labels) > 2 else host
    path = parsed.path or ""
    if path.endswith("/") and path != "/":
        path = path[:-1]
    path = path or "/"
    source_norm = normalize_source_url(url)

    candidates_raw = [
        f"https://careers.{base_host}/",
        f"https://jobs.{base_host}/",
        f"https://{base_host}/careers",
        f"https://{base_host}/jobs",
        f"https://{base_host}/vacancies",
    ]
    if host != base_host:
        candidates_raw.append(f"https://{base_host}{path}")
    else:
        candidates_raw.append(f"https://www.{base_host}{path}")

    out: list[str] = []
    seen = set()
    for raw in candidates_raw:
        normalized = normalize_source_url(raw)
        if not normalized or normalized == source_norm or normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out[:5]


def discover_redirect_career_candidates(source_url: str, timeout_s: int) -> list[str]:
    parsed = urlparse(str(source_url or "").strip())
    host = (parsed.netloc or "").strip().lower()
    if not host:
        return []
    if ":" in host:
        host = host.split(":", 1)[0]
    labels = [part for part in host.split(".") if part]
    base_host = ".".join(labels[1:]) if labels[:1] == ["www"] and len(labels) > 2 else host
    roots = [f"https://{base_host}/"]
    if not base_host.startswith("www."):
        roots.append(f"https://www.{base_host}/")

    out: list[str] = []
    seen = set()
    for root in roots:
        body = ""
        try:
            req = Request(root, headers={"User-Agent": "Mozilla/5.0 Baluffo/1.0"})
            with urlopen(req, timeout=max(4, int(timeout_s))) as resp:
                final_url = normalize_source_url(resp.geturl() or "")
                charset = resp.headers.get_content_charset() or "utf-8"
                body = resp.read().decode(charset, errors="replace")
        except (LookupError, OSError, UnicodeError, ValueError):
            continue
        if final_url and final_url not in seen:
            low = final_url.lower()
            parsed_final = urlparse(final_url)
            path = (parsed_final.path or "").lower()
            if any(
                token in low
                for token in ("jobs.", "careers.", "/jobs", "/career", "/careers", "/vacancies")
            ) or path in {"/jobs", "/career", "/careers", "/vacancies"}:
                seen.add(final_url)
                out.append(final_url)
        for href in re.findall(r'(?is)<a[^>]+href=["\']([^"\']+)["\']', str(body or "")):
            candidate = normalize_source_url(urljoin(root, str(href or "").strip()))
            if not candidate or candidate in seen:
                continue
            low_candidate = candidate.lower()
            if not any(
                token in low_candidate
                for token in (
                    "jobs.",
                    "careers.",
                    "apply.workable.com/",
                    "jobs.lever.co/",
                    "boards.greenhouse.io/",
                    "jobs.ashbyhq.com/",
                    "jobs.smartrecruiters.com/",
                    ".jobs.personio.de/",
                    "intervieweb.it/",
                    "/jobs",
                    "/career",
                    "/careers",
                    "/vacancies",
                    "/vacancy",
                )
            ):
                continue
            seen.add(candidate)
            out.append(candidate)
    return out[:6]


def is_not_found_error_text(error_text: str) -> bool:
    return "http error 404" in str(error_text or "").lower()


def looks_like_browser_challenge_page(html: str) -> bool:
    low = str(html or "").lower()
    if not low:
        return False
    challenge_tokens = (
        "challenge-platform",
        "/cdn-cgi/challenge-platform/",
        "cf-chl-",
        "cloudflare",
        "just a moment...",
        "enable javascript and cookies to continue",
    )
    return any(token in low for token in challenge_tokens)


def build_check_failure_details(
    error_text: str, source_url: str, *, browser_fallback_attempted: bool = False
) -> dict[str, Any]:
    code = normalize_error_code(error_text)
    details: dict[str, Any] = {
        "errorCode": code,
        "browserFallbackAttempted": bool(browser_fallback_attempted),
    }
    if code == "not_found":
        details["suggestedUrls"] = suggest_alternate_career_urls(source_url)
    else:
        details["suggestedUrls"] = []
    return details
