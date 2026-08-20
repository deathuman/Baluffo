"""Registry conflict row helpers — core row accessors and evidence scoring.

AI boundary owns: row shape accessors, provider/static classification, and live-evidence counts shared by every conflict family.
AI boundary implement in: this registry_conflicts_row_core.py leaf.
AI boundary search before contracts: registry conflict routes, registry_conflicts coordinator, and frontend registry conflict callers.
AI boundary verify: `npm run lint:repo-guardrails` plus focused registry conflict row tests."""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import urlparse

from src.source_registry import source_identity

PROVIDER_ADAPTERS = {
    "ashby",
    "bamboohr",
    "breezy",
    "greenhouse",
    "jazzhr",
    "lever",
    "personio",
    "pinpoint",
    "recruitee",
    "smartrecruiters",
    "teamtailor",
    "workable",
    "workday",
}

PROVIDER_HOST_SUFFIX_ADAPTERS = {
    ".bamboohr.com": "bamboohr",
    ".jobs.ashbyhq.com": "ashby",
    ".jobs.personio.de": "personio",
    ".myworkdayjobs.com": "workday",
    ".pinpointhq.com": "pinpoint",
    ".recruitee.com": "recruitee",
    ".teamtailor.com": "teamtailor",
    ".workable.com": "workable",
    ".workday.com": "workday",
}

PROVIDER_HOST_EXACT_ADAPTERS = {
    "apply.workable.com": "workable",
    "bamboohr.com": "bamboohr",
    "boards.greenhouse.io": "greenhouse",
    "jobs.ashbyhq.com": "ashby",
    "jobs.greenhouse.io": "greenhouse",
    "jobs.smartrecruiters.com": "smartrecruiters",
}


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _row_identity(row: dict[str, Any]) -> str:
    return _clean_text(row.get("id") or row.get("sourceId") or source_identity(row))


def _row_state(row: dict[str, Any]) -> str:
    return _clean_text(row.get("registryState") or row.get("candidateState")).lower()


def _row_adapter(row: dict[str, Any]) -> str:
    adapter = _clean_text(row.get("adapter") or row.get("sourceType")).lower()
    row_id = _clean_text(row.get("id") or row.get("sourceId") or source_identity(row)).lower()
    if not adapter and ":" in row_id:
        adapter = row_id.split(":", 1)[0]
    return adapter or "unknown"


def _is_static_row(row: dict[str, Any]) -> bool:
    return _row_adapter(row) == "static"


def _is_provider_row(row: dict[str, Any]) -> bool:
    return _row_adapter(row) in PROVIDER_ADAPTERS


def _provider_adapter_from_urls(row: dict[str, Any]) -> str:
    for url in _row_urls(row):
        try:
            parsed = urlparse(url)
        except ValueError:
            continue
        host = _clean_text(parsed.netloc).lower()
        if host.startswith("www."):
            host = host[4:]
        if not host:
            continue
        exact = PROVIDER_HOST_EXACT_ADAPTERS.get(host)
        if exact:
            return exact
        for suffix, adapter in PROVIDER_HOST_SUFFIX_ADAPTERS.items():
            if host.endswith(suffix):
                return adapter
    return ""


def _effective_provider_adapter(row: dict[str, Any]) -> str:
    adapter = _row_adapter(row)
    if adapter in PROVIDER_ADAPTERS:
        return adapter
    if _is_static_row(row):
        return _provider_adapter_from_urls(row)
    return ""


def _is_provider_like_row(row: dict[str, Any]) -> bool:
    return bool(_effective_provider_adapter(row))


def _row_has_weak_job_signal(row: dict[str, Any]) -> bool:
    confidence = _clean_text(row.get("lastProbeCountConfidence")).lower()
    return bool(
        any(bool(row.get(key)) for key in ("weakSignal", "lastProbeWeakSignal"))
        or (confidence and confidence != "high")
    )


def _int_value(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _row_jobs_evidence(row: dict[str, Any]) -> int:
    live_jobs = _count_from_key(row, "liveJobsFound")
    if live_jobs is not None:
        return live_jobs
    fresh_jobs = _fresh_jobs_found_count(row)
    if fresh_jobs is not None:
        return fresh_jobs
    if _is_static_row(row) and _row_has_weak_job_signal(row):
        reliable_jobs = _count_from_key(row, "lastReliableJobsFound")
        if reliable_jobs is not None:
            return reliable_jobs
        return 0
    for key in (
        "jobsFound",
        "jobs_found",
        "lastKeptCount",
        "lastJobsKept",
        "keptCount",
        "kept_count",
    ):
        value = _int_value(row.get(key))
        if value > 0:
            return value
    return 0


def _count_from_key(row: dict[str, Any], key: str) -> int | None:
    if key not in row:
        return None
    return max(0, _int_value(row.get(key)))


def _latest_fetch_failed(row: dict[str, Any]) -> bool:
    status = _clean_text(row.get("lastStatus")).lower()
    health = _clean_text(row.get("health")).lower()
    return status in {"error", "failed", "failure"} or health == "broken"


def _fresh_jobs_found_count(row: dict[str, Any]) -> int | None:
    if _latest_fetch_failed(row):
        return None
    for key in ("lastJobsFound", "lastJobsKept", "lastKeptCount"):
        value = _count_from_key(row, key)
        if value is not None:
            return value
    return None


def _positive_evidence_score(row: dict[str, Any]) -> int:
    return _row_jobs_evidence(row) + sum(
        max(0, _int_value(row.get(key)))
        for key in ("rankScore", "score", "lastJobsKept", "lastKeptCount")
    )


def _jobs_found_count(row: dict[str, Any]) -> int | None:
    live_jobs = _count_from_key(row, "liveJobsFound")
    if live_jobs is not None:
        return live_jobs
    fresh_jobs = _fresh_jobs_found_count(row)
    if fresh_jobs is not None:
        return fresh_jobs
    if _is_static_row(row) and _row_has_weak_job_signal(row):
        reliable_jobs = _count_from_key(row, "lastReliableJobsFound")
        return reliable_jobs if reliable_jobs is not None else 0
    for key in ("jobsFound", "sampleCount"):
        if key in row:
            return max(0, _int_value(row.get(key)))
    return None


def _has_fresh_or_healthy_signal(row: dict[str, Any]) -> bool:
    health = _clean_text(row.get("health") or row.get("lastStatus")).lower()
    return health in {"healthy", "ok", "success"}


def _row_urls(row: dict[str, Any]) -> list[str]:
    values = [
        row.get(key)
        for key in (
            "id",
            "sourceId",
            "api_url",
            "feed_url",
            "board_url",
            "listing_url",
            "careersUrl",
            "url",
        )
    ]
    urls: list[str] = []
    for value in values:
        for match in re.findall(r"https?://[^\s]+", _clean_text(value)):
            urls.append(match.rstrip("),.;'\""))
    return urls


def _static_row_current_jobs(row: dict[str, Any]) -> int:
    for key in ("liveJobsFound", "lastJobsKept", "lastKeptCount", "lastReliableJobsFound"):
        value = _count_from_key(row, key)
        if value is not None:
            return value
    return _row_jobs_evidence(row)
