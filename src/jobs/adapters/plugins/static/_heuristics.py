from __future__ import annotations

import re
from urllib.parse import urljoin

from src.jobs.adapters.html_parsers import strip_html_text
from src.jobs.text_utils import clean_text, normalize_url

# Canonical classification values for static/scrapy_static diagnostics and browser queue.
# Use these when setting classification in plugins and adapter code so reporting stays consistent.
CLASSIFICATION_OK_WITH_JOBS = "ok_with_jobs"
CLASSIFICATION_OK_NO_JOBS = "ok_no_jobs"
CLASSIFICATION_EMPTY_CONFIRMED = "empty_confirmed"
CLASSIFICATION_FETCH_OK_EXTRACT_ZERO = "fetch_ok_extract_zero"
CLASSIFICATION_BLOCKED_OR_CHALLENGE = "blocked_or_challenge"
CLASSIFICATION_TIMEOUT = "timeout"
CLASSIFICATION_BROWSER_TIMEOUT = "browser_timeout"
CLASSIFICATION_BROWSER_RETRY_NOT_RECOMMENDED = "browser_retry_not_recommended"
CLASSIFICATION_RATE_LIMITED = "rate_limited"
CLASSIFICATION_PARSER_STALE = "parser_stale"
CLASSIFICATION_DEAD_LISTING_PAGE = "dead_listing_page"
CLASSIFICATION_PARSE_ERROR = "parse_error"
CLASSIFICATION_ERROR = "error"

# Classifications that cause a source to be added to the browser fallback queue.
CLASSIFICATIONS_FOR_BROWSER_QUEUE = frozenset(
    {
        CLASSIFICATION_BLOCKED_OR_CHALLENGE,
        CLASSIFICATION_TIMEOUT,
    }
)


def normalize_html(html: str) -> str:
    return str(html or "")


def visible_text_len(html: str) -> int:
    text = strip_html_text(re.sub(r"(?is)<[^>]+>", " ", normalize_html(html)))
    return len(clean_text(text))


def detect_js_shell(html: str) -> bool:
    """Best-effort detection for JS-rendered app shells.

    This is intentionally conservative: it aims to detect pages that almost
    certainly require a browser/JS to render listings.
    """
    s = normalize_html(html)
    lower = s.lower()
    if visible_text_len(s) < 180:
        # Very little visible text; if also looks like an SPA shell, flag it.
        if any(
            tok in lower
            for tok in (
                '<div id="root"',
                '<div id="app"',
                "data-reactroot",
                "ng-version",
                "__next_data__",
            )
        ):
            return True
        if any(tok in lower for tok in ("window.__", "webpackjsonp", "react", "next.js")):
            return True
    return False


def detect_no_openings(html: str) -> bool:
    """Detect explicit 'no openings' markers to allow a proven-empty result."""
    s = normalize_html(html).lower()
    markers = [
        "no open positions",
        "no open roles",
        "no openings",
        "no jobs available",
        "no jobs found",
        "0 results",
        "0 job",
        "we're not hiring",
        "we are not hiring",
    ]
    return any(m in s for m in markers)


def detect_outbound_ats_links(html: str, *, base_url: str) -> list[str]:
    """Find outbound links to known ATS/job platforms."""
    s = normalize_html(html)
    links: list[str] = []
    for m in re.finditer(r'(?is)<a[^>]+href=["\']([^"\']+)["\']', s):
        href = clean_text(m.group(1))
        if not href:
            continue
        absolute = normalize_url(urljoin(base_url, href)) or ""
        if not absolute:
            continue
        lower = absolute.lower()
        if any(
            host in lower
            for host in (
                "greenhouse.io",
                "lever.co",
                "bamboohr.com",
                "myworkdayjobs.com",
                "workday.com",
                "smartrecruiters.com",
                "ashbyhq.com",
                "teamtailor.com",
                "personio.de",
                "jobvite.com",
            )
        ):
            links.append(absolute)
    # Dedup but keep stable order.
    out: list[str] = []
    seen = set()
    for url in links:
        if url in seen:
            continue
        seen.add(url)
        out.append(url)
    return out


def classify_fetch_exception(exc: Exception) -> tuple[str, bool]:
    msg = str(exc or "")
    if "HTTP 403" in msg:
        return CLASSIFICATION_BLOCKED_OR_CHALLENGE, True
    if "HTTP 429" in msg:
        return CLASSIFICATION_RATE_LIMITED, False
    if "Network error" in msg or "timed out" in msg or "Timeout" in msg:
        return CLASSIFICATION_TIMEOUT, True
    return CLASSIFICATION_ERROR, False
