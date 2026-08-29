from __future__ import annotations

import re
from typing import Any
from urllib.parse import urljoin

from src.jobs.adapters.html_parsers import strip_html_text
from src.jobs.adapters.static_runtime_support import classify_static_fetch_exception
from src.jobs.common.no_openings import contains_no_openings_marker
from src.jobs.text_utils import clean_text, normalize_url

# Canonical classification values for static/scrapy_static diagnostics and browser queue.
# Use these when setting classification in plugins and adapter code so reporting stays consistent.
CLASSIFICATION_OK_WITH_JOBS = "ok_with_jobs"
CLASSIFICATION_EMPTY_CONFIRMED = "empty_confirmed"
CLASSIFICATION_NEEDS_REVIEW = "needs_review"
CLASSIFICATION_FETCH_OK_EXTRACT_ZERO = CLASSIFICATION_NEEDS_REVIEW
CLASSIFICATION_JS_REQUIRED = "js_required"
CLASSIFICATION_SITE_CHANGED = "site_changed"
CLASSIFICATION_BLOCKED_OR_CHALLENGE = "blocked_or_challenge"
CLASSIFICATION_PARSER_STALE = "parser_stale"
CLASSIFICATION_DEAD_LISTING_PAGE = "dead_listing_page"


def normalize_html(html: str) -> str:
    return str(html or "")


def visible_text_len(html: str) -> int:
    text = strip_html_text(re.sub(r"(?is)<[^>]+>", " ", normalize_html(html)))
    return len(clean_text(text))


# Legacy (jQuery-era) JS shells are missed by the SPA-token detector below:
# these are real app shells (Ember/AngularJS/Backbone/jQuery SPA) whose listings
# are JS-rendered but which emit no React/Next/Angular-2 boot tokens. Detect them
# only when the hydration evidence is *corroborated* (a template/hydration marker
# plus a careers/job context, or a client-rendered href placeholder, or an
# AngularJS/legacy-SPA boot), so plain server-rendered pages that merely ship
# jQuery/handlebars are NOT misclassified.
_LegacyHydrationTemplateTokens = (
    "x-handlebars",
    "text/x-handlebars",
    "ember-application",
    "{{#each",
    "{{#if",
    "data-bind",  # knockout
)
# Standalone placeholder href emitted by client-hydrated loops (jobs/careers cards).
_HYDRATED_HREF_PLACEHOLDERS = ("data-href=", "data-url=", "data-joburl=")

_LegacySpaBootTokens = (
    "ng-app",
    "ng-controller",
    "ng-view",
    "ng-repeat",
    "backbone",
    "requirejs",
)

_LegacyJobListingHintTokens = (
    "job-listing",
    "job-card",
    "job-list",
    "open-positions",
    "career-list",
    "career-card",
    "jobopenings",
    "current-opening",
    "position-card",
    "join-our-team",
    "vacancy",
    "opening",
)

_LegacyCareerContextTokens = (
    "career",
    "careers",
    "job",
    "jobs",
    "position",
    "positions",
    "opening",
    "openings",
    "vacanc",
    "roles",
)


def detect_js_shell(html: str) -> bool:
    """Best-effort detection for JS-rendered app shells.

    SPA framework tokens alone are sufficient evidence — nav menus,
    cookie banners, and footers can produce >180 chars of visible text
    while the actual job listings are still JS-rendered.
    Very short text with SPA-related tokens is also evidence.

    Also detects jQuery-era / legacy-hydration shells (Ember, AngularJS,
    Backbone, jQuery SPA) that emit no modern SPA boot tokens, but only
    when the hydration evidence is corroborated by a job/career context
    (template markers, client-hydrated href placeholders, or a legacy-SPA
    boot) — so ordinary server-rendered pages that merely bundle
    jQuery/handlebars stay negative.
    """
    s = normalize_html(html)
    lower = s.lower()

    _spa_div_tokens = (
        '<div id="root"',
        '<div id="app"',
        "data-reactroot",
        "ng-version",
        "__next_data__",
    )
    _spa_framework_tokens = ("window.__", "webpackjsonp", "react", "next.js")

    has_spa_div = any(tok in lower for tok in _spa_div_tokens)
    has_spa_framework = any(tok in lower for tok in _spa_framework_tokens)

    if has_spa_div or has_spa_framework:
        return True

    career_context = any(tok in lower for tok in _LegacyCareerContextTokens)
    listing_hint = any(tok in lower for tok in _LegacyJobListingHintTokens)
    legacy_template = sum(1 for tok in _LegacyHydrationTemplateTokens if tok in lower)
    spa_boot = sum(1 for tok in _LegacySpaBootTokens if tok in lower)
    href_placeholder = any(tok in lower for tok in _HYDRATED_HREF_PLACEHOLDERS)
    jquery_present = lower.count("jquery") >= 2

    # Two AngularJS / legacy-SPA boot directives are unambiguous app shells.
    if spa_boot >= 2:
        return True
    # A handlebars/ember/knockout template marker corroborated by a careers
    # or job-listing context indicates a client-rendered listing.
    if legacy_template >= 1 and (listing_hint or career_context):
        return True
    # A client-hydrated href placeholder (loop-rendered job cards) inside a
    # careers/job context is a strong shell signal.
    if href_placeholder and (listing_hint or career_context):
        return True
    # jQuery rehydrating an explicit job-listing container is the classic
    # jQuery-era shell pattern.
    if jquery_present and listing_hint:
        return True
    return False


def detect_no_openings(html: str) -> bool:
    """Detect explicit 'no openings' markers to allow a proven-empty result."""
    return contains_no_openings_marker(normalize_html(html))


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
                "recruiting.ultipro.com",
                "paycomonline.net",
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
    return classify_static_fetch_exception(exc)


def build_static_plugin_meta(
    classification: str,
    *,
    browser_fallback_recommended: bool | None = None,
    extractor_hint: str | None = None,
    ats_links: list[str] | None = None,
    empty_confirmed: bool | None = None,
    detail_fetch_required: bool | None = None,
    detail_traversal_mode: str | None = None,
    error: str | None = None,
    **extra: Any,
) -> dict[str, Any]:
    """Build a consistent _staticPluginMeta payload for static plugins."""
    meta: dict[str, Any] = {"classification": classification}
    if browser_fallback_recommended is not None:
        meta["browserFallbackRecommended"] = browser_fallback_recommended
    if extractor_hint:
        meta["extractorHint"] = extractor_hint
    if ats_links is not None:
        meta["atsLinks"] = list(ats_links[:5])
    if empty_confirmed is not None:
        meta["emptyConfirmed"] = empty_confirmed
    if detail_fetch_required is not None:
        meta["detailFetchRequired"] = detail_fetch_required
    if detail_traversal_mode:
        meta["detailTraversalMode"] = detail_traversal_mode
    if error:
        meta["error"] = error
    for key, value in extra.items():
        if value is not None:
            meta[key] = value
    return meta
