from __future__ import annotations

"""Shared probe mechanics for source-discovery audit and recovery paths."""

import asyncio
import json
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any
from xml.etree import ElementTree as ET

import httpx

from src.shared.utils import now_iso
from src.source_registry import source_identity

from . import audit_ledger
from .core import (
    compute_candidate_score,
    normalize_candidate,
    probe_bucket_for,
    probe_concurrency_defaults,
)
from .io_runtime import endpoint_url
from .web_search_fetch import async_fetch_text_httpx, fetch_text

ProbeResult = tuple[dict[str, Any], bool, int, str, int]
AsyncProbe = Callable[..., Awaitable[tuple[bool, int, str]]]
ProbeFailedRejection = Callable[[dict[str, Any], str], dict[str, Any]]
ZeroJobsRejection = Callable[[dict[str, Any], int], dict[str, Any]]
ProbeNormalizer = Callable[[dict[str, Any], int], dict[str, Any]]


@dataclass
class ProbeClassification:
    positive_candidates: list[dict[str, Any]]
    zero_job_candidates: list[dict[str, Any]]
    rejected_rows: list[dict[str, Any]]


def candidate_id(candidate: dict[str, Any]) -> str:
    return str(source_identity(candidate) or "").strip()


def candidate_with_probe_evidence(
    candidate: dict[str, Any],
    jobs_found: int,
    *,
    prevalidated_discovery: bool = False,
) -> dict[str, Any]:
    score, reasons = compute_candidate_score(candidate, jobs_found)
    normalized = normalize_candidate(candidate, score, reasons, jobs_found, probed_at=now_iso())
    normalized["deferred"] = False
    normalized.pop("deferReason", None)
    normalized["probeStatus"] = "ok"
    normalized["candidateState"] = "validated"
    if prevalidated_discovery:
        normalized["prevalidatedDiscovery"] = True
    identity = candidate_id(normalized)
    if identity:
        normalized["id"] = identity
    return normalized


def classify_probe_results(
    probe_results: list[ProbeResult],
    *,
    probe_failed_rejection: ProbeFailedRejection,
    zero_jobs_rejection: ZeroJobsRejection,
    normalize_candidate: ProbeNormalizer = candidate_with_probe_evidence,
) -> ProbeClassification:
    positive_candidates: list[dict[str, Any]] = []
    zero_job_candidates: list[dict[str, Any]] = []
    rejected_rows: list[dict[str, Any]] = []
    for candidate, ok, jobs_found, error, duration_ms in probe_results:
        if not ok:
            rejected_rows.append(probe_failed_rejection(candidate, error))
            continue
        normalized = normalize_candidate(candidate, jobs_found)
        normalized["probeDurationMs"] = int(duration_ms)
        if jobs_found > 0:
            positive_candidates.append(normalized)
        else:
            zero_job_candidates.append(normalized)
            rejected_rows.append(zero_jobs_rejection(normalized, jobs_found))
    return ProbeClassification(
        positive_candidates=positive_candidates,
        zero_job_candidates=zero_job_candidates,
        rejected_rows=rejected_rows,
    )


def rendered_static_probe_result(
    candidate: dict[str, Any],
    *,
    rendered_url: str,
    rendered_html: str,
) -> ProbeResult | None:
    from .probe import parse_probe_count

    if str(candidate.get("adapter") or "").strip().lower() != "static":
        return None
    candidate_url = str(endpoint_url(candidate) or candidate.get("careersUrl") or "").strip()
    if candidate_url.rstrip("/") != str(rendered_url or "").strip().rstrip("/"):
        return None
    try:
        jobs_found = parse_probe_count("static", rendered_html)
    except (TypeError, ValueError, json.JSONDecodeError, ET.ParseError):
        return None
    if jobs_found <= 0:
        return None
    return candidate, True, int(jobs_found), "", 0


def probe_candidates_after_rendered_results(
    all_candidates: list[dict[str, Any]],
    rendered_probe_results: list[ProbeResult],
) -> list[dict[str, Any]]:
    rendered_ids = {candidate_id(result[0]) for result in rendered_probe_results}
    return [
        row for row in all_candidates if candidate_id(row) and candidate_id(row) not in rendered_ids
    ]


async def run_bounded_probe_batch_async(
    candidates: list[dict[str, Any]],
    *,
    timeout_s: int,
    fetcher,
    async_probe: AsyncProbe,
    default_fetcher=fetch_text,
    probe_kwargs: dict[str, Any] | None = None,
) -> list[ProbeResult]:
    probe_options = dict(probe_kwargs or {})
    limits = probe_concurrency_defaults()
    total_sem = asyncio.Semaphore(int(limits["total"]))
    bucket_sems = {
        "static": asyncio.Semaphore(int(limits["static"])),
        "provider": asyncio.Semaphore(int(limits["provider"])),
        "teamtailor": asyncio.Semaphore(int(limits["teamtailor"])),
    }

    async def _call_fetch(url: str, call_timeout_s: int) -> str:
        if fetcher is default_fetcher:
            return await async_fetch_text_httpx(client, url, call_timeout_s)
        return await asyncio.to_thread(fetcher, url, call_timeout_s)

    async def _probe_one(row: dict[str, Any]) -> ProbeResult:
        bucket = probe_bucket_for(row)
        bucket_sem = bucket_sems.get(bucket, bucket_sems["provider"])
        async with total_sem:
            async with bucket_sem:
                started = time.perf_counter()
                ok, jobs_found, error = await async_probe(
                    row,
                    timeout_s,
                    fetcher=_call_fetch,
                    **probe_options,
                )
                return row, ok, jobs_found, error, audit_ledger.duration_ms(started)

    async with httpx.AsyncClient(timeout=httpx.Timeout(timeout_s)) as client:
        tasks = [asyncio.create_task(_probe_one(row)) for row in candidates]
        results: list[ProbeResult] = []
        for fut in asyncio.as_completed(tasks):
            results.append(await fut)
        return results


async def probe_candidates_async(
    candidates: list[dict[str, Any]],
    *,
    timeout_s: int,
    fetcher,
) -> list[ProbeResult]:
    from .probe import async_probe_candidate

    return await run_bounded_probe_batch_async(
        candidates,
        timeout_s=timeout_s,
        fetcher=fetcher,
        async_probe=async_probe_candidate,
    )
