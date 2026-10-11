"""Source discovery probe runtime helpers.

Shared probe mechanics for source-discovery audit and recovery paths.

AI boundary owns: probe runtime batching, result envelopes, and probe execution support.
AI boundary implement in: this file for probe runtime mechanics; candidate validation/parsing stays in probe.py.
AI boundary search before contracts: orchestrator probe, runtime metrics, and probe runtime tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused probe runtime tests.
"""

from __future__ import annotations

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

# ponytail: mirrors probe.py's _EXPECTED_PROBE_FETCH_EXCEPTIONS. A probe failure
# must degrade that one candidate, never the batch, so the batch loop catches the
# same family. TimeoutError matters most: it is what a hung fetcher raises once
# _call_fetch is bounded, and letting it escape used to abort every other
# candidate's result.
_EXPECTED_PROBE_BATCH_EXCEPTIONS: tuple[type[BaseException], ...] = (
    OSError,
    TimeoutError,
    RuntimeError,
    KeyError,
    ValueError,
    json.JSONDecodeError,
    ET.ParseError,
    httpx.HTTPError,
)


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
    from .probe import static_probe_evidence

    if str(candidate.get("adapter") or "").strip().lower() != "static":
        return None
    candidate_url = str(endpoint_url(candidate) or candidate.get("careersUrl") or "").strip()
    if candidate_url.rstrip("/") != str(rendered_url or "").strip().rstrip("/"):
        return None
    try:
        evidence = static_probe_evidence(rendered_html, rendered_url)
    except (TypeError, ValueError, json.JSONDecodeError, ET.ParseError):
        return None
    jobs_found = int(evidence.count)
    if jobs_found <= 0:
        return None
    candidate["lastProbeCountReason"] = evidence.reason
    candidate["lastProbeCountConfidence"] = evidence.confidence
    candidate["lastReliableJobsFound"] = jobs_found if evidence.confidence == "high" else 0
    if evidence.sample_urls:
        candidate["lastProbeSampleUrls"] = list(evidence.sample_urls)
    if evidence.confidence != "high":
        candidate["lastProbeWeakSignal"] = True
        candidate["weakSignal"] = True
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
        # ponytail: `httpx.Timeout` is a PER-OPERATION budget, not a total one -- the read timeout
        # re-arms on every chunk received -- so a slow-drip server holds this call open past its
        # own timeout and returns "successfully" minutes later (measured 40.3 s against a 2 s
        # timeout on the loopback drip server, and 40.7 s against Timeout(2.0) in the shared
        # batcher, where the same loophole parked a GameDevMap batch for 900 s). The sync branch
        # below has always been wrapped this way; the default httpx branch was not.
        budget = max(1.0, float(call_timeout_s))
        try:
            if fetcher is default_fetcher:
                return await asyncio.wait_for(
                    async_fetch_text_httpx(client, url, call_timeout_s),
                    timeout=budget,
                )
            # ponytail: the sync branch runs in a worker thread, which cannot be
            # cancelled. The module has no wait_for anywhere, so a fetcher whose
            # socket read never returns parks `_probe_one` forever: its semaphores
            # stay held, and the whole probe batch stops with no error. Give the
            # await its own deadline so a hung fetch becomes an ordinary
            # per-candidate TimeoutError (already handled by the probe
            # retry/except path) instead of an unbounded stall. The thread itself
            # still runs to completion -- unavoidable for sync callables -- but the
            # batch is no longer blocked by it.
            return await asyncio.wait_for(
                asyncio.to_thread(fetcher, url, call_timeout_s),
                timeout=budget,
            )
        except TimeoutError as exc:
            # say why: asyncio's own TimeoutError stringifies to "", and an empty error is a
            # failure row an operator cannot classify. The wording keeps the "timed out" token
            # the retry predicate and the failure-stage classifier already recognise.
            raise TimeoutError(f"fetch timed out after {budget:.0f}s") from exc

    async def _probe_one(index: int, row: dict[str, Any]) -> tuple[int, ProbeResult]:
        bucket = probe_bucket_for(row)
        bucket_sem = bucket_sems.get(bucket, bucket_sems["provider"])
        async with total_sem:
            async with bucket_sem:
                started = time.perf_counter()
                # ponytail: a probe must never escape as an exception. Without
                # this guard a failure raised out of async_probe (including the
                # TimeoutError produced when the fetcher thread hangs) propagates
                # through as_completed, aborts the whole batch, and returns no
                # results at all. Converting it to an ordinary per-candidate
                # failure keeps the one-result-per-candidate contract and lets
                # the caller's normal rejection handling record it.
                try:
                    ok, jobs_found, error = await async_probe(
                        row,
                        timeout_s,
                        fetcher=_call_fetch,
                        **probe_options,
                    )
                except _EXPECTED_PROBE_BATCH_EXCEPTIONS as exc:
                    ok, jobs_found, error = False, 0, str(exc)
                return index, (row, ok, jobs_found, error, audit_ledger.duration_ms(started))

    tasks: list[asyncio.Task[tuple[int, ProbeResult]]] = []
    by_index: dict[int, ProbeResult] = {}
    finished: list[int] = []

    async def _run(client: Any) -> list[ProbeResult]:
        tasks.extend(
            asyncio.create_task(_probe_one(index, row)) for index, row in enumerate(candidates)
        )
        for fut in asyncio.as_completed(tasks):
            index, result = await fut
            by_index[index] = result
            finished.append(index)
        return [by_index[index] for index in finished]

    # ponytail: the same defect class as src/shared/http_batch.py, and the same shape of bound. A
    # probe batch funnels every candidate in the wave, so one candidate whose requests never
    # return parks the whole stage with no error and no timeout -- the pathological case is one
    # board that trickles bytes, not one that is slow. Bounding it at `timeout_s` would be wrong:
    # that is a per-request budget and a probe may make several requests (fallbacks, structured
    # APIs) plus retries. Let every candidate spend its own timeout, divided by the concurrency
    # actually available, plus a grace for scheduling -- measured at 4 s for 3 candidates at a 2 s
    # timeout, against 40.3 s unbounded on the loopback drip server.
    per_row_s = max(1.0, float(timeout_s))
    effective_concurrency = max(1, min(int(limits["total"]), max(1, len(candidates))))
    total_deadline_s = per_row_s * (len(candidates) / effective_concurrency) + per_row_s
    deadline_error = f"total probe deadline exceeded ({total_deadline_s:.0f}s)"
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(timeout_s)) as client:
            return await asyncio.wait_for(_run(client), timeout=total_deadline_s)
    except TimeoutError:
        _cancel_pending(tasks)
        return _deadline_results(
            candidates,
            by_index,
            error=deadline_error,
            duration_ms=int(total_deadline_s * 1000),
        )


def _cancel_pending(tasks: list[asyncio.Task[Any]]) -> None:
    """Cancel every wave task still running, so a deadline leaves no orphan work behind."""
    for task in tasks:
        if not task.done():
            task.cancel()


def _deadline_results(
    candidates: list[dict[str, Any]],
    completed: dict[int, ProbeResult],
    *,
    error: str,
    duration_ms: int,
) -> list[ProbeResult]:
    """One result per candidate for a wave that ran out of time: what finished, plus failures."""
    return [
        completed[index] if index in completed else (row, False, 0, error, duration_ms)
        for index, row in enumerate(candidates)
    ]


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
