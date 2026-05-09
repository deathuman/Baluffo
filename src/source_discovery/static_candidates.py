from __future__ import annotations

"""Static-site candidate heuristics (HTML-only discovery)."""

import re
from typing import Any

from .scoring import careers_keyword_count, studio_domain_match, unique_string_list
from .web_search import is_blocked_generic_static_url


def build_known_careers_url_candidate(
    target_url: str,
    *,
    studio: str,
    name_suffix: str,
    nl_priority: bool,
    discovery_method: str,
    evidence_source: str,
    evidence_types: list[str],
    evidence_score: int,
    discovery_stage: str = "generic_static",
    enabled_by_default: bool | None = False,
    weak_signal: bool = False,
    extra_fields: dict[str, Any] | None = None,
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "name": f"{studio} ({name_suffix})" if name_suffix else studio,
        "studio": studio,
        "company": studio,
        "adapter": "static",
        "pages": [target_url],
        "listing_url": target_url,
        "nlPriority": nl_priority,
        "discoveryMethod": discovery_method,
        "discoveryStage": discovery_stage,
        "careersUrl": target_url,
        "evidenceSource": evidence_source,
        "evidenceTypes": unique_string_list(evidence_types),
        "evidenceScore": int(evidence_score),
        "weakSignal": bool(weak_signal),
    }
    if enabled_by_default is not None:
        row["enabledByDefault"] = bool(enabled_by_default)
    if isinstance(extra_fields, dict):
        row.update(extra_fields)
    return row


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
    from .probe import static_probe_evidence

    probe_evidence = static_probe_evidence(html, page_url)
    detail_links = list(probe_evidence.sample_urls)
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
        "detailPageCount": max(len(detail_links), int(probe_evidence.count or 0)),
        "detailPagesSample": detail_sample,
    }
