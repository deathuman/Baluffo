"""Shared static plugin for the Phenom "phApp" careers platform (sitemap recovery).

WP14/WP15/WP17 (jobs-coverage plan): a family of large studios — Activision,
Blizzard, King, Treyarch, Raven, Sledgehammer, Beenox, High Moon, Infinity Ward,
Warner Bros. Games, TT Games, Scopely, and more — all run the proprietary Phenom
People "CareerConnect" platform ("phApp" / `phw-unified` shells with a
client-side widget API). The widget-API JSON is behind a tenant + CSRF gate, but
every one of these sites publishes a **public per-locale sitemap**
(`<scheme>://<host>/<locale>/sitemap.xml`) that lists every job page as a
`/job/{jobCode}/{slug-title}` URL, and each job detail page is **server-rendered**
with a stable `<title>` of the form:

    `{Job Title} job in {City, State, Country} | {Discipline} jobs at {Company}`

That gives a fully open, crawlable recovery path with no auth: parse the sitemap
for job URLs, fetch each detail page, and derive title + location + company from
the `<title>` (with a URL-slug fallback). This one adapter covers all the platform
rows in the registry (they share the same sitemap contract).

AI boundary owns: shared Phenom careers-page recovery, sitemap job-URL collection,
and job-detail title/location extraction for phApp rows.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from html import unescape
from typing import Any
from urllib.parse import urljoin, urlparse

from src.jobs.adapters.html_parsers import strip_html_text
from src.jobs.adapters.parsers.location import parse_generic_location_fields
from src.jobs.adapters.plugins.static._runner import (
    _EXPECTED_STATIC_PLUGIN_FETCH_EXCEPTIONS,
    first_static_page,
    is_static_fetch_fallback_exception,
    stamp_static_plugin_rows,
    static_plugin_context_values,
)
from src.jobs.models import RawJob
from src.jobs.text_utils import clean_text

# Known Phenom careers hosts in the active registry (WP15 scan). Sorted.
PHA_HOSTS = frozenset(
    {
        "careers.activision.com",
        "careers.blizzard.com",
        "careers.highmoonstudios.com",
        "careers.infinityward.com",
        "careers.king.com",
        "careers.ravensoftware.com",
        "careers.sledgehammergames.com",
        "careers.treyarch.com",
        "careers.wbd.com",
        "careers.beenox.com",
        "scopely.com",
        "www.scopely.com",
    }
)

_KNOWN_LOCALES = ("global", "us", "en", "uk", "gb")
# Two family variants appear on the platform:
#   Blizzard/Activision:  "{Title} | {InnerLoc} job in {Loc}, {Country} | {Disc} jobs at {Co}"
#   King:                "{Title} in {Loc}, {Country} | {Disc} at {Co}"
# The Blizzard form skips its "| {InnerLoc}" disambiguator so titles stay clean,
# and it is tried first because it is the more specific shape. The King form uses
# a plain " in " anchor; titles that genuinely contain " in " are rare on these
# boards and only become ambiguous when a second " in " follows (thus swallowed).
_JOB_IN_TITLE_RE = re.compile(
    r"(?is)^(?P<title>.+?)\s*\|\s*.+?\s+job in\s+(?P<location>.+?)\s*\|\s*"
    r"(?P<discipline>.+?)\s+jobs? at\s+(?P<company>.+)$"
)
_PLAIN_IN_TITLE_RE = re.compile(
    r"(?is)^(?P<title>.+?)\s+(?:job in|in)\s+(?P<location>.+?)\s*\|\s*(?P<discipline>.+?)\s+at\s+(?P<company>.+)$"
)
_LOC_TAG_RE = re.compile(r"(?is)<title\b[^>]*>(?:<!\[CDATA\[)?(.+?)(?:\]\]>)?</title>")
_SLUG_TITLE_RE = re.compile(
    r"(?is)<loc>\s*(?:<!\[CDATA\[)?(https?://[^<\]]*/job/[^/\s<\]]+/([A-Za-z0-9-]+?))\s*(?:\]\]>)?</loc>"
)
_SITEMAP_CHILD_RE = re.compile(
    r"(?is)<loc>\s*(?:<!\[CDATA\[)?(https?://[^<\]]+?sitemap[^<\]]*\.xml)\s*(?:\]\]>)?</loc>"
)
_MAX_SITEMAPS = 8
_MAX_JOBS_PER_SOURCE = 80


def _locale_path(page_url: str) -> str:
    """Best-effort Phenom locale prefix (e.g. ``/global/en``, ``/us/en``) from the page URL."""
    try:
        parsed = urlparse(page_url)
    except ValueError:
        return ""
    parts = [clean_text(p) for p in (parsed.path or "").split("/") if clean_text(p)]
    if (
        len(parts) >= 2
        and clean_text(parts[0]).lower() in _KNOWN_LOCALES
        and clean_text(parts[1]).lower() in ("en", "us", "en_us")
    ):
        return "/" + "/".join(parts[:2])
    return ""


def sitemap_candidates(page_url: str) -> list[str]:
    """Sitemap URLs to probe for a phApp host, most canonical first."""
    try:
        parsed = urlparse(page_url)
    except ValueError:
        return []
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return []
    origin = f"{parsed.scheme}://{parsed.netloc}"
    loc = _locale_path(page_url)
    candidates = [f"{origin}{loc}/sitemap.xml"] if loc else []
    candidates.append(f"{origin}/global/en/sitemap.xml")
    candidates.append(f"{origin}/sitemap.xml")
    return list(dict.fromkeys(candidates))


def collect_job_urls(sitemap_xml: str, base_url: str) -> list[str]:
    """Job detail URLs from a Phenom sitemap (including nested sitemap indexes)."""
    # Child sitemaps (sitemapindex) reference more .xml docs; their <loc> are sitemaps too.
    urls: list[str] = []
    stack = [sitemap_xml]
    seen_urls: set[str] = set()
    while stack:
        fragment = stack.pop()
        children = _SITEMAP_CHILD_RE.findall(fragment)
        if children:
            # It's a sitemap index; we cannot recurse further without fetching — collected
            # here are the job URLs present in this document regardless.
            pass
        for match in _SLUG_TITLE_RE.finditer(fragment):
            absolute = match.group(1)
            if absolute in seen_urls:
                continue
            seen_urls.add(absolute)
            urls.append(absolute)
    # base join fallback for relative job URLs
    resolved = []
    for u in urls:
        r = urljoin(base_url, u)
        resolved.append(r) if r not in resolved else None
    return resolved


def _extract_title(html: str) -> str:
    # Prefer the <title> tag: phApp job pages carry the canonical
    # "{Title} job in {Location} | {Discipline} jobs at {Company}" there, while
    # og:twitter meta omit the "job in" fragment.
    best = ""
    for m in _LOC_TAG_RE.finditer(html):
        candidate = clean_text(strip_html_text(m.group(1)))
        if not candidate:
            continue
        # The job-detail <title> carries the "… job in … | … jobs at …" shape;
        # prefer it over a generic page <title>.
        if " job in " in candidate:
            return candidate
        if not best:
            best = candidate
    return best


def extract_phapp_job_meta(
    html: str,
    *,
    job_url: str,
    fallback_company: str,
) -> dict[str, Any] | None:
    """Title / location / company for a phApp job detail page.

    Prefers the ``<title>``/``og:title`` ``{Title} job in {Location} | {Discipline} at
    {Company}`` shape; falls back to a title derived from the URL slug. Returns None
    only when nothing usable is found.
    """
    title_text = _extract_title(html)
    title = ""
    location_text = ""
    company = fallback_company
    if title_text:
        match = _JOB_IN_TITLE_RE.search(unescape(title_text)) or _PLAIN_IN_TITLE_RE.search(
            unescape(title_text)
        )
        if match:
            title = clean_text(match.group("title").strip(" ,-"))
            location_text = clean_text(match.group("location"))
            company = clean_text(match.group("company")) or fallback_company
        else:
            # No canonical shape; take the text before a trailing " | " chunk.
            title = re.split(r"\s*\|\s*", title_text, maxsplit=1)[0]
            title = clean_text(title)
    if not title:
        parsed = urlparse(job_url)
        slug = (parsed.path.rstrip("/").split("/")[-1] or "") if parsed.path else ""
        if slug:
            title = clean_text(slug.replace("-", " "))
    if not title:
        return None
    city, country, _locations = (
        parse_generic_location_fields(location_text) if location_text else ("", "Unknown", "")
    )
    job_code = ""
    code_match = re.search(r"/job/([^/]+)/", job_url)
    if code_match:
        job_code = code_match.group(1)
    return {
        "title": title,
        "city": city,
        "country": country or "Unknown",
        "jobLink": job_url,
        "sourceJobId": f"phapp:{job_code}" if job_code else f"phapp:{job_url}",
        "company": company,
    }


def can_handle(ctx: Any) -> bool:
    identity = (ctx.source_identity or "").strip().lower()
    if identity in PHA_HOSTS:
        return True
    host = identity.split("://")[-1].split("/")[0] if "://" in identity else identity
    return host in PHA_HOSTS


def run(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    pages: list[str],
    source_row: dict[str, Any],
    **kwargs: Any,
) -> list[RawJob]:
    _ = (retries, backoff_s, kwargs)
    page_url = first_static_page(pages)
    if not page_url:
        return []
    company, source_id, source_name = static_plugin_context_values(
        source_row=source_row,
        default_company="",
        default_source_id="phapp",
        default_source_name="phapp",
    )

    sitemap_xml = ""
    for candidate in sitemap_candidates(page_url)[:_MAX_SITEMAPS]:
        try:
            body = fetch_text(candidate, timeout_s)
        except _EXPECTED_STATIC_PLUGIN_FETCH_EXCEPTIONS as exc:
            if not is_static_fetch_fallback_exception(exc):
                raise
            body = ""
        if "<urlset" in (body or "") or "<sitemapindex" in (body or ""):
            sitemap_xml = body
            break

    job_urls = collect_job_urls(sitemap_xml, page_url)[:_MAX_JOBS_PER_SOURCE]
    if not job_urls:
        return []

    rows: list[RawJob] = []
    for job_url in job_urls:
        try:
            html = fetch_text(job_url, timeout_s)
        except _EXPECTED_STATIC_PLUGIN_FETCH_EXCEPTIONS as exc:
            if not is_static_fetch_fallback_exception(exc):
                raise
            html = ""
        meta = extract_phapp_job_meta(html or "", job_url=job_url, fallback_company=company)
        if not meta:
            continue
        rows.append(
            {
                "title": meta["title"],
                "company": meta["company"] or company,
                "jobLink": meta["jobLink"],
                "sourceJobId": meta["sourceJobId"],
                "city": meta["city"],
                "country": meta["country"],
                "workType": "",
                "contractType": "",
                "sector": "Game",
                "postedAt": "",
            }
        )
    if not rows:
        return []
    return stamp_static_plugin_rows(rows=rows, company=company, source_name=source_name)
