from __future__ import annotations

"""Static-site candidate heuristics (HTML-only discovery)."""

import re
from typing import Any

from .scoring import careers_keyword_count, studio_domain_match
from .web_search import extract_jobish_links, is_blocked_generic_static_url


def build_static_candidate_from_page(
    page_url: str,
    html: str,
    *,
    studio: str,
    nl_priority: bool,
    discovery_method: str,
) -> dict[str, Any] | None:
    if is_blocked_generic_static_url(page_url):
        return None
    if not careers_keyword_count(page_url) and careers_keyword_count(html) == 0:
        return None
    detail_links = extract_jobish_links(html, page_url)
    jsonld_hits = re.findall(r'"@type"\s*:\s*"JobPosting"', str(html or ""), flags=re.I)
    if not detail_links and not jsonld_hits:
        return None
    evidence_types = ["careers_keyword"]
    evidence_score = 18
    if detail_links:
        evidence_types.append("structured_job_links")
        evidence_score += min(24, len(detail_links) * 6)
    if jsonld_hits:
        evidence_types.append("jobposting_jsonld")
        evidence_score += 18
    if studio_domain_match(studio, page_url):
        evidence_types.append("studio_domain_match")
        evidence_score += 10
    detail_sample = detail_links[:6]
    return {
        "name": f"{studio} (Manual Website)",
        "studio": studio,
        "company": studio,
        "adapter": "static",
        "pages": [page_url, *detail_sample],
        "listing_url": page_url,
        "nlPriority": nl_priority,
        "enabledByDefault": False,
        "discoveryMethod": discovery_method,
        "discoveryStage": "generic_static",
        "careersUrl": page_url,
        "evidenceSource": "careers_page",
        "evidenceTypes": evidence_types,
        "evidenceScore": evidence_score,
        "weakSignal": len(detail_sample) < 2 and not jsonld_hits,
        "detailPageCount": len(detail_links),
        "detailPagesSample": detail_sample,
    }

