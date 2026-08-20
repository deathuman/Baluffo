"""Registry conflict adjudication — source probe and job parsing.

AI boundary owns: per-source probing, job payload/html parsing, overlap scoring, and best-probe selection for conflict families.
AI boundary implement in: this registry_conflict_adjudication_probe.py leaf.
AI boundary search before contracts: conflict adjudication routes, progress payloads, and adjudication tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused registry adjudication tests."""

from __future__ import annotations

import json
from typing import Any
from urllib.parse import urlparse

from src.bridge.registry_conflict_adjudication_core import (
    _adapter_token,
    _as_dict,
    _as_list,
    _clean,
    _endpoint_url,
    _row_adapter,
    _row_id,
)
from src.bridge.source_check_http import try_fetch_with_playwright
from src.bridge.source_probe_evidence import probe_source_evidence
from src.jobs.adapters.html_parsers import parse_jobpostings_from_html
from src.jobs.adapters.parsers.json_payloads import (
    parse_greenhouse_jobs_payload,
    parse_lever_jobs_payload,
    parse_pinpoint_jobs_payload,
    parse_recruitee_jobs_payload,
    parse_smartrecruiters_jobs_payload,
    parse_workable_jobs_payload,
)
from src.jobs.adapters.parsers.provider_html import parse_jazzhr_jobs_html
from src.jobs.text_utils import clean_text, norm_text, normalize_url
from src.source_discovery.probe import static_probe_evidence

_PROVIDER_PAYLOAD_PARSERS = {
    "greenhouse": parse_greenhouse_jobs_payload,
    "lever": parse_lever_jobs_payload,
    "smartrecruiters": parse_smartrecruiters_jobs_payload,
    "workable": parse_workable_jobs_payload,
    "recruitee": parse_recruitee_jobs_payload,
    "pinpoint": parse_pinpoint_jobs_payload,
}


def _json_payload(text: str) -> Any:
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def _parse_jobs(
    row: dict[str, Any], text: str, final_url: str
) -> tuple[bool, list[dict[str, Any]]]:
    adapter = _row_adapter(row)
    if adapter == "static":
        evidence = static_probe_evidence(text, final_url or _endpoint_url(row))
        jobs = [
            {
                "sourceJobId": f"static:{_row_id(row)}:{idx}",
                "company": _clean(row.get("company") or row.get("studio") or row.get("name")),
                "jobLink": url,
            }
            for idx, url in enumerate(evidence.sample_urls, start=1)
        ]
        while len(jobs) < evidence.count:
            idx = len(jobs) + 1
            jobs.append(
                {
                    "sourceJobId": f"static:{_row_id(row)}:{idx}",
                    "company": _clean(row.get("company") or row.get("studio") or row.get("name")),
                }
            )
        return bool(text), jobs
    payload = _json_payload(text)
    fallback_company = _clean(row.get("company") or row.get("studio") or row.get("name"))
    token = _adapter_token(row, "slug", "account", "company_id")
    parser = _PROVIDER_PAYLOAD_PARSERS.get(adapter)
    if parser:
        jobs = parser(payload, token, fallback_company)
    elif adapter == "jazzhr":
        jobs = parse_jazzhr_jobs_html(text, final_url or _endpoint_url(row), fallback_company)
    elif adapter == "ubisoft_algolia":
        hits = _as_list(_as_dict(payload).get("hits"))
        jobs = [
            {
                "sourceJobId": (
                    "ubisoft_algolia:"
                    f"{clean_text(hit.get('objectID') or hit.get('refNumber') or hit.get('slug'))}"
                ),
                "title": clean_text(hit.get("title")),
                "company": fallback_company or "Ubisoft",
                "city": clean_text(hit.get("city")),
                "country": clean_text(hit.get("countryCode")).upper(),
                "jobLink": normalize_url(hit.get("link") or hit.get("referralUrl")),
                "postedAt": clean_text(hit.get("createdAt")),
            }
            for hit in hits
            if isinstance(hit, dict) and clean_text(hit.get("title"))
        ]
    else:
        jobs = parse_jobpostings_from_html(
            text,
            base_url=final_url or _endpoint_url(row),
            fallback_company=fallback_company,
            fallback_source_id_prefix=_row_id(row),
        )
    valid_payload = payload is not None if adapter != "static" else bool(text)
    return bool(valid_payload or jobs), [dict(job) for job in jobs if isinstance(job, dict)]


def _job_location(job: dict[str, Any]) -> str:
    summary = clean_text(job.get("locationSummary"))
    if summary:
        return summary
    return ", ".join(
        part for part in (clean_text(job.get("city")), clean_text(job.get("country"))) if part
    )


def _job_key(job: dict[str, Any]) -> str:
    return "|".join(
        (
            norm_text(job.get("title")),
            norm_text(job.get("company")),
            norm_text(_job_location(job)),
        )
    )


def _job_sample(job: dict[str, Any]) -> dict[str, str]:
    return {
        "sourceJobId": clean_text(job.get("sourceJobId")),
        "title": clean_text(job.get("title")),
        "company": clean_text(job.get("company")),
        "location": _job_location(job),
        "jobLink": normalize_url(job.get("jobLink")),
        "postedAt": clean_text(job.get("postedAt")),
    }


def _newest_job_date(jobs: list[dict[str, Any]]) -> str:
    values = sorted(
        clean_text(job.get("postedAt")) for job in jobs if clean_text(job.get("postedAt"))
    )
    return values[-1] if values else ""


def _probe_row(row: dict[str, Any], timeout_s: int) -> dict[str, Any]:
    source_id = _row_id(row)
    evidence = probe_source_evidence(
        row,
        timeout_s,
        try_playwright=try_fetch_with_playwright,
    )
    endpoint = evidence.endpoint_url or _endpoint_url(row)
    final_url = evidence.final_url or endpoint
    text = evidence.response_text
    jobs: list[dict[str, Any]] = []
    valid_payload = False
    parse_error = ""
    if text:
        try:
            parse_row = row
            if evidence.payload_adapter:
                parse_row = {
                    **row,
                    **(evidence.payload_fields or {}),
                    "adapter": evidence.payload_adapter,
                }
            valid_payload, jobs = _parse_jobs(parse_row, text, final_url)
        except (TypeError, ValueError, KeyError) as exc:
            parse_error = str(exc)
    provider_jobs_found = int(evidence.jobs_found or 0)
    jobs_found = len(jobs) if valid_payload or jobs else provider_jobs_found
    if evidence.adapter != "static" or evidence.payload_adapter:
        jobs_found = max(jobs_found, provider_jobs_found)
    ok = bool(evidence.ok and not parse_error and (valid_payload or jobs or jobs_found == 0))
    return {
        "sourceId": source_id,
        "name": _clean(row.get("name")),
        "adapter": evidence.adapter or _row_adapter(row),
        "endpointUrl": endpoint,
        "finalUrl": final_url,
        "httpStatus": int(evidence.http_status or 0),
        "ok": ok,
        "error": evidence.error or parse_error,
        "validPayload": bool(valid_payload),
        "jobsFound": jobs_found,
        "countConfidence": evidence.count_confidence,
        "countReason": evidence.count_reason,
        "sampleUrls": list(evidence.sample_urls),
        "browserFallbackRecommended": bool(evidence.browser_fallback_recommended),
        "browserFallbackUsed": bool(evidence.browser_fallback_used),
        "newestJobDate": _newest_job_date(jobs),
        "sampleJobs": [_job_sample(job) for job in jobs[:5]],
        "_jobIds": sorted(
            {
                clean_text(job.get("sourceJobId")).lower()
                for job in jobs
                if clean_text(job.get("sourceJobId"))
            }
        ),
        "_jobLinks": sorted(
            {
                normalize_url(job.get("jobLink")).lower()
                for job in jobs
                if normalize_url(job.get("jobLink"))
            }
        ),
        "_jobKeys": sorted({_job_key(job) for job in jobs if _job_key(job)}),
    }


def _public_probe(probe: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in probe.items() if not str(key).startswith("_")}


def _overlap(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    counts: dict[str, int] = {}
    ratios: list[float] = []
    for key, label in (
        ("_jobIds", "sourceJobIds"),
        ("_jobLinks", "jobLinks"),
        ("_jobKeys", "jobFingerprints"),
    ):
        left_values = set(_as_list(left.get(key)))
        right_values = set(_as_list(right.get(key)))
        shared = left_values & right_values
        counts[label] = len(shared)
        denominator = min(len(left_values), len(right_values))
        if denominator:
            ratios.append(len(shared) / denominator)
    ratio = max(ratios) if ratios else 0.0
    return {
        "ratio": round(ratio, 3),
        "sharedSourceJobIds": counts.get("sourceJobIds", 0),
        "sharedJobLinks": counts.get("jobLinks", 0),
        "sharedJobFingerprints": counts.get("jobFingerprints", 0),
    }


def _probe_score(probe: dict[str, Any]) -> tuple[int, int]:
    return (
        1 if bool(probe.get("ok")) else 0,
        int(probe.get("jobsFound") or 0),
    )


def _canonical_host_score(probe: dict[str, Any]) -> int:
    final = urlparse(_clean(probe.get("finalUrl") or probe.get("endpointUrl")))
    endpoint = urlparse(_clean(probe.get("endpointUrl")))
    host = final.netloc.lower()
    score = 0
    if host and not any(token in host for token in ("homeinteractive", "old", "legacy")):
        score += 1
    if (
        final.scheme
        and endpoint.scheme
        and final.netloc.lower() == endpoint.netloc.lower()
        and final.path.rstrip("/") == endpoint.path.rstrip("/")
    ):
        score += 2
    if "focusentertainment" in host:
        score += 2
    return score


def _best_probe(probes: list[dict[str, Any]]) -> dict[str, Any]:
    return max(
        probes,
        key=lambda probe: (
            *_probe_score(probe),
            _canonical_host_score(probe),
            _clean(probe.get("newestJobDate")),
        ),
    )
