"""Rows from the JSON a page's own app fetched while rendering.

A single-page app's openings are not in its markup. They arrive in the XHR the page makes
after load -- Feishu's board answers `POST /api/v1/search/job_post/count`, and the rendered
markup holds no job links at all. The browser already fetches that response; this reads it.

The rows come out of :func:`extract_embedded_json_rows`'s own posting-shape guard, so the
junk a page's payloads carry (CMS assets, nav entries, ``name``/``url`` pairs, non-``JobPosting``
schema.org nodes) is rejected by the same rule that governs the static embedded-JSON lane --
one definition of "this object is a posting", not two.

Host-agnostic by construction: there is no host list here, so a board that moves platform
keeps working, and a new platform needs no code at all.

AI boundary owns: turning captured render payloads into rows.
AI boundary implement in: this file for payload-to-row extraction; the capture itself lives in
``browser_fallback_pool``, the browser gate in ``browser_fallback``, and the lane that emits
these rows in ``static_listing_rows.py``.
AI boundary search before contracts: the embedded-JSON lane, the rendered-card lane, the
browser fallback pool, and captured-payload tests.
AI boundary verify: `npm run lint:repo-guardrails` plus tests/jobs/adapters/plugins/static/.
"""

from __future__ import annotations

from src.jobs.adapters.plugins.static.embedded_json import rows_from_json_payload
from src.jobs.models import RawJob
from src.jobs.text_utils import clean_text, normalize_url

# Payloads a page fetches for itself: telemetry, feature flags, translations, session pings.
# Walking them costs budget and produces nothing; a payload whose URL says what it is for can
# be skipped without losing a board's openings, which arrive under a jobs/positions/postings
# path or from the page's own domain.
_SKIP_URL_TOKENS = (
    "/analytics",
    "/telemetry",
    "/gtm",
    "/gtag",
    "/tracking",
    "/beacon",
    "/log.",
    "/logs",
    "/pixel",
    "/collect",
    "/metric",
    "/event",
    "/events",
    "/feature",
    "/flags",
    "/config.json",
    "/i18n",
    "/locale",
    "/locales",
    "/translations",
    "/sentry",
    "/fonts",
    "/favicon",
)


def payload_is_worth_walking(payload_url: str) -> bool:
    """Whether a captured payload could plausibly carry postings.

    A host-agnostic *filter*, not a whitelist: anything on a known telemetry path is skipped
    and everything else is walked, because a board that serves its openings from an
    unremarkable URL must not be dropped by a guess.
    """
    url = str(payload_url or "").strip().lower()
    if not url:
        return False
    return not any(token in url for token in _SKIP_URL_TOKENS)


def extract_rendered_json_rows(
    payloads: list[tuple[str, str]],
    *,
    board_url: str,
    fallback_company: str = "",
) -> list[RawJob]:
    """Rows from the JSON a render captured, deduplicated by link.

    Payloads are walked in capture order and the first row to claim a link keeps it, so a
    board's own listing payload wins over a later duplicate from a footer widget. Payloads
    that are not JSON, or that carry no posting-shaped object, contribute nothing and are not
    an error: most pages fetch several responses and only one of them is the listings.
    """
    rows: list[RawJob] = []
    seen: set[str] = set()
    for payload_url, body in payloads or []:
        if not payload_is_worth_walking(payload_url):
            continue
        text = str(body or "").strip()
        if not text:
            continue
        for row in rows_from_json_payload(
            text, board_url=board_url, fallback_company=fallback_company
        ):
            link = normalize_url(clean_text(row.get("jobLink")))
            if not link or link in seen:
                continue
            seen.add(link)
            row["jobLink"] = link
            row["renderedJsonPayloadUrl"] = clean_text(payload_url)
            rows.append(row)
    return rows


def describe_capture(payloads: list[tuple[str, str]]) -> str:
    """One-line summary of a capture, for the entry report.

    The count of captured payloads is what distinguishes "this board has no openings" from
    "this board's data we never saw", which is the distinction the whole lane exists for.
    """
    total = len(payloads or [])
    if not total:
        return ""
    walked = [url for url, _body in payloads if payload_is_worth_walking(url)]
    return f"{total} captured payloads, {len(walked)} walked"


__all__ = [
    "describe_capture",
    "extract_rendered_json_rows",
    "payload_is_worth_walking",
]
