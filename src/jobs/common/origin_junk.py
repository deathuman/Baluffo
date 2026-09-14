"""Origin-aware junk-provenance classification for static-source rows.

Hold-tail systemic fix (Fusebox adjudication 2026-09-13, plan option 3): static
listing sources whose registry origin is NOT LinkedIn keep harvesting LinkedIn
guest-view URLs — ``linkedin.com/jobs/{slug}?trk=…`` search-result rows and the
nav-anchor/self-page shape that leaked Konami's "Community" row. These rows can
never re-verify for a logged-out fetcher (LinkedIn answers bots with HTTP 999),
carry no detail surface, and strand as ``verification_overdue`` floor rows
run-over-run.

Scope guards baked into every predicate:

- **Origin-aware.** A source whose registry identity is itself a
  ``linkedin.com`` listing is sanctioned (its per-job ``/jobs/view/{id}`` rows
  are the product); the junk class only applies to rows harvested from other
  origins. ``static_source_identity_url`` resolves the source's own listing URL
  so both directions share one definition.
- **Fail-open.** Unparseable URLs, ambiguous shapes, and unknown sources are
  never classified as junk; the availability lane still re-verifies those rows
  the ordinary way. Junk classification only ever *skips* work, never asserts
  a page's content.
"""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import urlparse

from src.jobs.common import config as common_config
from src.jobs.text_utils import clean_text
from src.url_hosts import host_matches_domain

# Two id spellings carry a listing URL: the registry/extraction form
# (``static:listing_url:…``, what ctx.source.id holds in the rows flow) and the
# lifecycle/state form (``static_source::static:listing_url:…``). The 2026-09-14
# Fusebox revival proved the prefix-sensitive spelling silently no-oped every
# extraction-lane guard (empty identity host → fail-open), so both are accepted.
_SOURCE_LISTING_URL_RE = re.compile(
    r"(?i)^(?:static_source::)?static:listing_url:(?P<url>https?://\S+)$"
)

# Bare numeric view URL: the only LinkedIn row shape that carries a real
# per-job surface a logged-out fetcher can still usefully re-verify.
_LINKEDIN_VIEW_NUMERIC_PATH_RE = re.compile(r"(?i)^/jobs/view/\d+/?$")

# Queryless guest search-results shape: /jobs/{anything}-jobs (Fusebox's 41).
_LINKEDIN_SLUG_SEARCH_PATH_RE = re.compile(r"(?i)^/jobs/[a-z0-9][a-z0-9-]*-jobs/?$")

# Company-page shape (the rendered widget's "LinkedIn" link and its browse_jobs
# relatives — any path under /company/): a company/talent surface, never a
# per-job detail page (jobs live at /jobs/view/{id}). Sanctioned LinkedIn-origin
# sources stay exempt via the origin check, not this shape.
_LINKEDIN_COMPANY_PATH_PREFIX = "/company/"

# Nav-anchor / generic self-page shape that leaked as a "job" row (Konami's
# "Community" → /pages/sns_account). Exact titles only — substring or prefix
# matching measurably hits legitimate rows ("Product", "Product Manager").
_NAV_TITLE_EXACT = frozenset(
    {
        "about",
        "about us",
        "blog",
        "careers",
        "career",
        "community",
        "contact",
        "contact us",
        "events",
        "faq",
        "games",
        "help",
        "home",
        "jobs",
        "login",
        "media kit",
        "news",
        "press",
        "press kit",
        "privacy",
        "privacy policy",
        "shop",
        "sign in",
        "store",
        "support",
        "terms",
        "terms of use",
    }
)

# A URL with one of these path tokens or query keys is a real detail surface
# regardless of its anchor text, so it is never nav-junk.
_DETAIL_PATH_TOKENS = (
    "/job/",
    "/jobs/",
    "/jobdetail/",
    "/career/",
    "/careers/",
    "/position/",
    "/positions/",
    "/vacancy/",
    "/vacancies/",
    "/openings/",
)
_DETAIL_QUERY_KEYS = ("job_id", "gh_jid", "jid", "jobid")


def is_linkedin_host(host: str) -> bool:
    """True for linkedin.com and any subdomain (www., uk., es., …)."""
    return host_matches_domain((host or "").strip().lower(), "linkedin.com")


def source_identity_url(source: dict[str, Any] | None) -> str:
    """The source row's own listing URL, when the registry identity carries one."""
    if not isinstance(source, dict):
        return ""
    source_id = clean_text(source.get("id")) or clean_text(source.get("sourceId"))
    if not source_id:
        return ""
    match = _SOURCE_LISTING_URL_RE.match(source_id)
    if not match:
        return ""
    return clean_text(match.group("url"))


def source_identity_host(source: dict[str, Any] | None) -> str:
    """Lowercase hostname of the source's registry listing URL ('' when none)."""
    url = source_identity_url(source)
    if not url:
        return ""
    try:
        parsed = urlparse(url)
    except ValueError:
        return ""
    return (parsed.hostname or "").strip().lower()


def source_origin_is_linkedin(source: dict[str, Any] | None) -> bool:
    """True when the registry row's own listing URL is a LinkedIn page.

    Sanctioned LinkedIn-origin sources legitimately emit ``/jobs/view/{id}``
    rows; the guest-junk class must never apply to their output.
    """
    return is_linkedin_host(source_identity_host(source))


def guest_junk_guard_enabled() -> bool:
    """Kill switch, read dynamically so a mid-session rollback takes effect."""
    return bool(common_config.GUEST_JUNK_GUARD_ENABLED)


def _linkedin_junk_url_shape(url: str) -> bool:
    try:
        parsed = urlparse(url)
    except ValueError:
        return False
    host = (parsed.hostname or "").strip().lower()
    if not host or not is_linkedin_host(host):
        return False
    path = (parsed.path or "").strip().lower()
    if parsed.query or parsed.fragment:
        # Tracked / decorated rows: junk unless a bare numeric view URL.
        return not _LINKEDIN_VIEW_NUMERIC_PATH_RE.match(path)
    if path.startswith("/jobs/search"):
        return True
    if path.startswith(_LINKEDIN_COMPANY_PATH_PREFIX):
        return True
    return bool(_LINKEDIN_SLUG_SEARCH_PATH_RE.match(path))


def _has_job_detail_surface(url: str) -> bool:
    try:
        parsed = urlparse(url)
    except ValueError:
        return False
    path = (parsed.path or "").lower()
    if any(token in path for token in _DETAIL_PATH_TOKENS):
        return True
    query = (parsed.query or "").lower()
    return any(f"{key}=" in query for key in _DETAIL_QUERY_KEYS)


def is_linkedin_guest_junk_url(url: str) -> bool:
    """Guest-view LinkedIn search/slug URL: unverifiable for the logged-out fetcher."""
    url = clean_text(url)
    if not url:
        return False
    return _linkedin_junk_url_shape(url)


def is_nav_anchor_junk_row(title: str, url: str, *, source_host: str) -> bool:
    """Nav-anchor title pointing at the source's own generic page.

    The Konami "Community" shape: the listing page's navigation was harvested
    as a job row. Exact-title match plus same-host requirement plus no job
    detail surface (path tokens or job query keys) keeps real rows safe.
    """
    title_key = clean_text(title).strip().lower()
    if not title_key or title_key not in _NAV_TITLE_EXACT:
        return False
    host_key = (source_host or "").strip().lower()
    if not host_key:
        return False
    url = clean_text(url)
    if not url:
        return False
    try:
        parsed = urlparse(url)
    except ValueError:
        return False
    host = (parsed.hostname or "").strip().lower()
    if not host or not host_matches_domain(host, host_key):
        return False
    return not _has_job_detail_surface(url)


def is_junk_provenance_row(row: dict[str, Any], *, source: dict[str, Any] | None) -> bool:
    """Whole-row predicate for emitted static rows (origin-aware, fail-open).

    True when the row's jobLink is a LinkedIn guest-view search/slug URL, or a
    nav-anchor self-page row (Konami shape), for a source whose registry
    origin is NOT LinkedIn. Sources without a resolvable static listing URL
    identity are always out of scope (fail-open). This is the guard's single
    gating choke point: the kill switch is enforced here, so every consumer
    (rows flow, detail candidates, lifecycle drain) flips together.
    """
    if not guest_junk_guard_enabled():
        return False
    if source_origin_is_linkedin(source):
        return False
    source_host = source_identity_host(source)
    if not source_host:
        return False
    url = clean_text(row.get("jobLink") if isinstance(row, dict) else None)
    if not url:
        return False
    if is_linkedin_guest_junk_url(url):
        return True
    return is_nav_anchor_junk_row(
        clean_text(row.get("title")) if isinstance(row, dict) else "",
        url,
        source_host=source_host,
    )
