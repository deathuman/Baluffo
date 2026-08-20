"""Static listing shared constants, fetch helpers, and the detail candidate dataclass.

AI boundary owns: fanout caps, ATS signature hints, careers-landing detection, timeout/fallback
classification, and the `StaticDetailCandidate` record shared by all listing leaves.
AI boundary implement in: this base leaf for shared primitives; state lives in
``static_listing_state.py``, listing flow in ``static_listing_flow.py``, and the fetch runner in
``static_listing_runner.py``.
"""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlparse

from src.jobs.adapters.static_runtime_support import (
    effective_timeout_for_remaining_budget,
    is_static_fetch_fallback_exception,
)
from src.jobs.common.http import HttpStatusError
from src.jobs.text_utils import clean_text

from .static_runtime import StaticSourceContext

_EXTERNAL_DETAIL_FANOUT_HOST_THRESHOLD = 2
_EXTERNAL_DETAIL_FANOUT_LINK_CAP = 8
_PLUGIN_STATIC_ARTIFACT_NESTED_DETAIL_LIMIT = 12
_EXPECTED_STATIC_LISTING_FETCH_FALLBACK_EXCEPTIONS = (HttpStatusError, OSError, RuntimeError)

# (adapter_name, html_substring) pairs for diagnostic warnings
# when a static source's page HTML contains an ATS signature.
_ATS_SIGNATURE_HINTS: list[tuple[str, str]] = [
    ("teamtailor", "teamtailor"),
    ("greenhouse", "greenhouse.io"),
    ("bamboohr", "bamboohr"),
    ("workday", "myworkdayjobs"),
    ("smartrecruiters", "smartrecruiters"),
    ("lever", "lever.co"),
    ("workable", "workable"),
]

_CAREERS_LANDING_TOKENS = (
    "careers",
    "career",
    "jobs",
    "job",
    "join-us",
    "open-positions",
    "vacancies",
    "work-with-us",
    "openings",
    "vacancy",
    "positions",
    "recruitment",
    "karriere",
    "stellenanzeigen",
    "emploi",
    "recrutement",
    "vacantes",
    "lavora",
    "offerte",
    "vagas",
)


def _careers_landing_url(url: str) -> bool:
    """Check if URL host+path suggests a career listing page."""
    try:
        parsed = urlparse(str(url or ""))
    except ValueError:
        return False
    text = f"{(parsed.hostname or '').lower()}{(parsed.path or '').lower()}"
    return any(token in text for token in _CAREERS_LANDING_TOKENS)


def _is_expected_static_listing_fetch_fallback(exc: Exception) -> bool:
    return is_static_fetch_fallback_exception(exc)


# pure — budget arithmetic + TimeoutError gate
def _effective_timeout_or_raise(
    *,
    timeout_s: int,
    remaining_budget_s: float,
    source_budget_s: int,
) -> int:
    effective_timeout_s = effective_timeout_for_remaining_budget(
        timeout_s=timeout_s,
        remaining_budget_s=remaining_budget_s,
    )
    if effective_timeout_s <= 0:
        raise TimeoutError(f"time budget exceeded ({source_budget_s}s)")
    return effective_timeout_s


# pure — URL host normalization
def _normalized_host(url: str) -> str:
    host = (urlparse(clean_text(url) or "").hostname or "").lower()
    return host[4:] if host.startswith("www.") else host


# mutation — modifies in-place state
def _cap_external_detail_fanout(
    ctx: StaticSourceContext,
    *,
    page_url: str,
    detail_links: list[StaticDetailCandidate],
    cap: int = _EXTERNAL_DETAIL_FANOUT_LINK_CAP,
) -> list[StaticDetailCandidate]:
    if len(detail_links) <= cap:
        return detail_links
    page_host = _normalized_host(page_url)
    if not page_host:
        return detail_links
    external_hosts = {
        host
        for candidate in detail_links
        if (host := _normalized_host(candidate.url)) and host != page_host
    }
    if len(external_hosts) <= _EXTERNAL_DETAIL_FANOUT_HOST_THRESHOLD:
        return detail_links
    capped: list[StaticDetailCandidate] = []
    external_kept = 0
    for candidate in detail_links:
        host = _normalized_host(candidate.url)
        if not host or host == page_host:
            capped.append(candidate)
            continue
        if external_kept >= cap:
            continue
        capped.append(candidate)
        external_kept += 1
    pruned = max(0, len(detail_links) - len(capped))
    if pruned:
        ctx.stats["external_detail_links_capped"] = (
            int(ctx.stats.get("external_detail_links_capped") or 0) + pruned
        )
        ctx.link_rejections["non_job_url"] += pruned
    return capped


@dataclass(frozen=True)
class StaticDetailCandidate:
    url: str
    title: str = ""
    depth: int = 0
    parent_url: str = ""
