from __future__ import annotations

"""Resumable GameDevMap active-source dry-run reporting."""

import asyncio
import time
from collections import Counter
from contextlib import suppress
from pathlib import Path
from typing import Any

from src import source_registry as source_registry_module
from src.shared.utils import now_iso
from src.source_registry import unique_sources

from . import audit_ledger, audit_report_summary, recovery_url_planner
from . import browser_recovery as browser_recovery_helpers
from .config import DEFAULT_DISCOVERY_CONFIG
from .directory_fetch import fetch_directory_pages, resolve_directory_fetch_limits
from .gamedevmap import (
    GAMEDEVMAP_CSV_URL,
    GAMEDEVMAP_INDEX_URL,
    _apply_gamedevmap_provenance,
    _gamedevmap_cache_signature,
    _gamedevmap_config_value,
    parse_gamedevmap_csv,
    select_gamedevmap_representative_rows,
)
from .io_runtime import endpoint_url
from .page_analysis import analyze_fetched_page
from .page_diagnostics import (
    looks_like_js_shell as shared_looks_like_js_shell,
)
from .page_diagnostics import (
    no_candidate_reason_detail as shared_no_candidate_reason_detail,
)
from .page_outcomes import (
    FetchedPageContext,
    PageOutcome,
    classify_fetched_page,
    static_page_outcome_builders,
)
from .prevalidated_queue_policy import apply_prevalidated_queue_overrides
from .probe_runtime import (
    candidate_id as probe_candidate_id,
)
from .probe_runtime import (
    candidate_with_probe_evidence as probe_candidate_with_probe_evidence,
)
from .probe_runtime import (
    probe_candidates_async as shared_probe_candidates_async,
)
from .probe_runtime import (
    rendered_static_probe_result,
)
from .provider_inference_filters import bad_provider_inference_detail
from .reporting import emit_log
from .web_search import (
    extract_jobish_links,
    fetch_text,
    infer_provider_candidates_from_html,
    infer_web_candidate,
)

DRY_RUN_SCHEMA_VERSION = 3
LAST_GAMEDEVMAP_AUDIT_REPORT_SUMMARY: dict[str, Any] = {}
GAMEDEVMAP_RERUN_REASONS = {
    "homepage_fetch_failed",
    "no_careers_evidence",
    "probe_failed",
    "zero_jobs",
}
NO_CAREERS_RECOVERY_PATHS = (
    "/careers",
    "/jobs",
    "/join-us",
    "/work-with-us",
    "/company/careers",
    "/about/careers",
)
PRIMARY_RECOVERY_PATHS = ("/careers", "/jobs")
SECONDARY_RECOVERY_PATHS = (
    "/join-us",
    "/work-with-us",
    "/company/careers",
    "/about/careers",
)
FAILURE_SAMPLE_LIMIT = 200
SOCIAL_PROFILE_HOSTS = {
    "facebook.com",
    "instagram.com",
    "linkedin.com",
    "tiktok.com",
    "twitter.com",
    "x.com",
    "youtube.com",
}
THIRD_PARTY_PROFILE_HOSTS = {
    "impress.games",
    "itch.io",
    "linktr.ee",
    "sites.google.com",
}
TECHNICAL_REJECTION_REASONS = {"bad_provider_inference", "homepage_fetch_failed", "probe_failed"}


def gamedevmap_active_dry_run_path() -> Path:
    return source_registry_module.ACTIVE_PATH.parent / "gamedevmap-active-source-dry-run.json"


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _row_url(row: dict[str, Any]) -> str:
    return str(row.get("url") or "").strip()


def _candidate_id(candidate: dict[str, Any]) -> str:
    return probe_candidate_id(candidate)


def _candidate_url_key(candidate: dict[str, Any]) -> str:
    raw = str(
        candidate.get("listing_url")
        or candidate.get("careersUrl")
        or candidate.get("api_url")
        or candidate.get("url")
        or endpoint_url(candidate)
        or ""
    ).strip()
    return f"url:{raw}" if raw else ""


def _host(url: str) -> str:
    return recovery_url_planner.host(url)


def _host_in(host: str, blocked_hosts: set[str]) -> bool:
    return recovery_url_planner.host_in(host, blocked_hosts)


def _normalize_failure_bucket(reason: str, detail: str = "") -> str:
    reason_key = str(reason or "").strip()
    detail_key = str(detail or "").strip()
    if reason_key in TECHNICAL_REJECTION_REASONS or detail_key == "recovery_fetch_failed":
        return "technical_failure"
    if reason_key in {"no_careers_evidence", "zero_jobs"}:
        return "coverage_miss"
    return "other"


def _error_text(result: dict[str, Any]) -> str:
    error = str(result.get("error") or "").strip()
    if error:
        return error
    failure = result.get("failure")
    if isinstance(failure, dict):
        error = str(failure.get("error") or "").strip()
        if error:
            return error
    return ""


def _rejection(
    *,
    reason: str,
    row: dict[str, Any] | None = None,
    candidate: dict[str, Any] | None = None,
    error: str = "",
    jobs_found: int = 0,
    reason_detail: str = "",
    failure_bucket: str = "",
) -> dict[str, Any]:
    detail = str(reason_detail or "").strip()
    payload: dict[str, Any] = {
        "reason": str(reason),
        "reasonDetail": detail,
        "failureBucket": str(failure_bucket or _normalize_failure_bucket(reason, detail)),
        "error": str(error or ""),
        "jobsFound": max(0, int(jobs_found or 0)),
    }
    if isinstance(row, dict):
        payload["studio"] = str(row.get("studio") or "")
        payload["url"] = _row_url(row)
        payload["sourceDirectoryEntryUrl"] = str(row.get("sourceDirectoryEntryUrl") or "")
    if isinstance(candidate, dict):
        payload["candidate"] = dict(candidate)
        payload["sourceId"] = _candidate_id(candidate)
        payload["adapter"] = str(candidate.get("adapter") or "")
        payload["name"] = str(candidate.get("name") or "")
    return payload


def _candidate_with_probe_evidence(candidate: dict[str, Any], jobs_found: int) -> dict[str, Any]:
    return probe_candidate_with_probe_evidence(candidate, jobs_found)


def _initial_artifact(
    *,
    run_id: str,
    started_at: str,
    timeout_s: int,
    csv_url: str,
    index_url: str,
    cfg: dict[str, Any],
    batch_size: int,
    fetch_concurrency: int,
    per_host_concurrency: int,
    homepage_fetch_concurrency: int,
    recovery_fetch_concurrency: int,
    recovery_per_host_concurrency: int,
    recovery_timeout_s: int,
) -> dict[str, Any]:
    return {
        "schemaVersion": DRY_RUN_SCHEMA_VERSION,
        "runId": str(run_id or ""),
        "startedAt": str(started_at or now_iso()),
        "updatedAt": "",
        "finishedAt": "",
        "mode": "gamedevmap_active_source_dry_run",
        "summary": {},
        "progress": {
            "complete": False,
            "cursorPosition": 0,
            "batchSize": int(batch_size),
            "batchesCompleted": 0,
            "completedUrlsCount": 0,
        },
        "runtime": {
            "timeoutSeconds": int(timeout_s),
            "fetchConcurrency": int(fetch_concurrency),
            "perHostConcurrency": int(per_host_concurrency),
            "homepageFetchConcurrency": int(homepage_fetch_concurrency),
            "recoveryFetchConcurrency": int(recovery_fetch_concurrency),
            "recoveryPerHostConcurrency": int(recovery_per_host_concurrency),
            "recoveryTimeoutSeconds": int(recovery_timeout_s),
            "csvUrl": csv_url,
            "indexUrl": index_url,
            "configSignature": _gamedevmap_cache_signature(cfg),
        },
        "timings": {"batches": [], "totalsMs": {}},
        "failureCounts": {},
        "failureErrorCounts": {},
        "failureSamples": [],
        "completedUrls": [],
        "activeCandidates": [],
        "zeroJobCandidates": [],
        "rejectedForActivation": [],
        "browserRecoveryCandidates": [],
        "failures": [],
        "allCandidates": [],
    }


def _load_or_initialize_artifact(
    output_path: Path,
    *,
    reset: bool,
    run_id: str,
    started_at: str,
    timeout_s: int,
    csv_url: str,
    index_url: str,
    cfg: dict[str, Any],
    batch_size: int,
    fetch_concurrency: int,
    per_host_concurrency: int,
    homepage_fetch_concurrency: int,
    recovery_fetch_concurrency: int,
    recovery_per_host_concurrency: int,
    recovery_timeout_s: int,
) -> dict[str, Any]:
    if reset:
        with suppress(FileNotFoundError, PermissionError, OSError):
            output_path.unlink()
    existing = source_registry_module.load_json_object(output_path, {})
    if int(existing.get("schemaVersion") or 0) == DRY_RUN_SCHEMA_VERSION:
        artifact = dict(existing)
        artifact["runtime"] = {
            **_as_dict(artifact.get("runtime")),
            "timeoutSeconds": int(timeout_s),
            "fetchConcurrency": int(fetch_concurrency),
            "perHostConcurrency": int(per_host_concurrency),
            "homepageFetchConcurrency": int(homepage_fetch_concurrency),
            "recoveryFetchConcurrency": int(recovery_fetch_concurrency),
            "recoveryPerHostConcurrency": int(recovery_per_host_concurrency),
            "recoveryTimeoutSeconds": int(recovery_timeout_s),
            "csvUrl": csv_url,
            "indexUrl": index_url,
            "configSignature": _gamedevmap_cache_signature(cfg),
        }
        artifact["progress"] = {
            **_as_dict(artifact.get("progress")),
            "batchSize": int(batch_size),
        }
        artifact.setdefault("browserRecoveryCandidates", [])
        artifact.setdefault("timings", {"batches": [], "totalsMs": {}})
        artifact.setdefault("failureCounts", {})
        artifact.setdefault("failureErrorCounts", {})
        artifact.setdefault(
            "failureSamples", _as_list(artifact.get("failures"))[:FAILURE_SAMPLE_LIMIT]
        )
        return artifact
    return _initial_artifact(
        run_id=run_id,
        started_at=started_at,
        timeout_s=timeout_s,
        csv_url=csv_url,
        index_url=index_url,
        cfg=cfg,
        batch_size=batch_size,
        fetch_concurrency=fetch_concurrency,
        per_host_concurrency=per_host_concurrency,
        homepage_fetch_concurrency=homepage_fetch_concurrency,
        recovery_fetch_concurrency=recovery_fetch_concurrency,
        recovery_per_host_concurrency=recovery_per_host_concurrency,
        recovery_timeout_s=recovery_timeout_s,
    )


def _next_batch(
    representative_rows: list[dict[str, Any]],
    completed_urls: set[str],
    batch_size: int,
) -> tuple[list[dict[str, Any]], int]:
    batch: list[dict[str, Any]] = []
    cursor = len(representative_rows)
    for index, row in enumerate(representative_rows):
        url = _row_url(row)
        if url and url in completed_urls:
            continue
        if not batch:
            cursor = index
        batch.append(row)
        if len(batch) >= batch_size:
            break
    return batch, cursor


def _merge_unique_rows(existing: Any, incoming: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return unique_sources(
        [*(_as_list(existing)), *[dict(row) for row in incoming if isinstance(row, dict)]]
    )


def _merge_by_source_id(existing: Any, incoming: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    passthrough: list[dict[str, Any]] = []
    for row in [*(_as_list(existing)), *incoming]:
        if not isinstance(row, dict):
            continue
        row_id = _candidate_id(row)
        if row_id:
            rows[row_id] = dict(row)
        else:
            passthrough.append(dict(row))
    return [*passthrough, *rows.values()]


def _duration_ms(started: float) -> int:
    return audit_ledger.duration_ms(started)


def _append_batch_timing(artifact: dict[str, Any], timing: dict[str, Any]) -> None:
    audit_ledger.append_batch_timing(artifact, timing)


def _record_failures(artifact: dict[str, Any], failures: list[dict[str, Any]]) -> None:
    audit_ledger.record_failures(artifact, failures, sample_limit=FAILURE_SAMPLE_LIMIT)


def _failure_count(artifact: dict[str, Any]) -> int:
    return audit_ledger.failure_count(artifact)


def _summarize_artifact(
    artifact: dict[str, Any],
    *,
    parsed_rows: list[dict[str, str]],
    representative_rows: list[dict[str, Any]],
    completed_urls: set[str],
) -> None:
    prior_summary = _as_dict(artifact.get("summary"))
    rejected = _as_list(artifact.get("rejectedForActivation"))
    reason_counts = Counter(
        str(row.get("reason") or "unknown") for row in rejected if isinstance(row, dict)
    )
    detail_counts = Counter(
        str(row.get("reasonDetail") or "unknown") for row in rejected if isinstance(row, dict)
    )
    active = [row for row in _as_list(artifact.get("activeCandidates")) if isinstance(row, dict)]
    all_candidates = [
        row for row in _as_list(artifact.get("allCandidates")) if isinstance(row, dict)
    ]
    recovered_candidates = [row for row in all_candidates if bool(row.get("gamedevmapRecovery"))]
    recovered_active = [row for row in active if bool(row.get("gamedevmapRecovery"))]
    technical_failures = [
        row
        for row in rejected
        if isinstance(row, dict)
        and str(row.get("failureBucket") or _normalize_failure_bucket(row.get("reason", "")))
        == "technical_failure"
    ]
    coverage_misses = [
        row
        for row in rejected
        if isinstance(row, dict)
        and str(row.get("failureBucket") or _normalize_failure_bucket(row.get("reason", "")))
        == "coverage_miss"
    ]
    adapter_counts = Counter(str(row.get("adapter") or "unknown") for row in active)
    browser_recovery = _as_dict(artifact.get("browserRecovery"))
    lost_recovery = _as_dict(artifact.get("lostRecoveryAudit"))
    csv_rows = len(parsed_rows) if parsed_rows else _safe_int(prior_summary.get("csvRows"))
    eligible_rows = (
        len(representative_rows)
        if representative_rows
        else _safe_int(prior_summary.get("eligibleRows"))
    )
    completed_count = (
        len(completed_urls) if completed_urls else _safe_int(prior_summary.get("completedUrls"))
    )
    artifact["summary"] = {
        "csvRows": csv_rows,
        "eligibleRows": eligible_rows,
        "completedUrls": completed_count,
        "remainingUrls": max(0, eligible_rows - completed_count),
        "homepageFetchAttempts": _safe_int(
            _as_dict(artifact.get("summary")).get("homepageFetchAttempts")
        ),
        "homepagesFetched": _safe_int(_as_dict(artifact.get("summary")).get("homepagesFetched")),
        "recoveryFetchAttempts": _safe_int(
            _as_dict(artifact.get("summary")).get("recoveryFetchAttempts")
        ),
        "recoveryUniqueFetchAttempts": _safe_int(
            _as_dict(artifact.get("summary")).get("recoveryUniqueFetchAttempts")
        ),
        "recoveryNetworkFetchAttempts": _safe_int(
            _as_dict(artifact.get("summary")).get("recoveryNetworkFetchAttempts")
        ),
        "recoveryPagesFetched": _safe_int(
            _as_dict(artifact.get("summary")).get("recoveryPagesFetched")
        ),
        "providerCandidates": len(
            [row for row in all_candidates if str(row.get("adapter") or "") != "static"]
        ),
        "staticCandidates": len(
            [row for row in all_candidates if str(row.get("adapter") or "") == "static"]
        ),
        "recoveredCandidates": len(recovered_candidates),
        "recoveredActiveCandidates": len(recovered_active),
        "probedCandidates": len(all_candidates),
        "activeCandidates": len(active),
        "zeroJobCandidates": len(_as_list(artifact.get("zeroJobCandidates"))),
        "probeFailures": int(reason_counts.get("probe_failed") or 0),
        "technicalFailures": len(technical_failures),
        "coverageMisses": len(coverage_misses),
        "failures": _failure_count(artifact),
        "failureSampleCount": len(_as_list(artifact.get("failureSamples"))),
        "artifactSizeBytes": _safe_int(_as_dict(artifact.get("runtime")).get("artifactSizeBytes")),
        "rejectedForActivation": len(rejected),
        "rejectedReasonCounts": dict(reason_counts),
        "rejectedReasonDetailCounts": dict(detail_counts),
        "activeAdapterCounts": dict(adapter_counts),
        "browserRecoveryCandidates": len(_as_list(artifact.get("browserRecoveryCandidates"))),
        "browserRecoveryProcessed": _safe_int(browser_recovery.get("processedCount")),
        "browserRecoveredActiveCandidates": _safe_int(browser_recovery.get("activeCandidates")),
        "lostRecoveredActiveCandidates": _safe_int(lost_recovery.get("lostCount")),
    }


def _write_artifact(
    artifact: dict[str, Any],
    output_path: Path,
    *,
    parsed_rows: list[dict[str, str]],
    representative_rows: list[dict[str, Any]],
    completed_urls: set[str],
    complete: bool,
) -> None:
    progress = _as_dict(artifact.get("progress"))
    progress["complete"] = bool(complete)
    progress["completedUrlsCount"] = len(completed_urls)
    if complete:
        progress["cursorPosition"] = len(representative_rows)
        artifact["finishedAt"] = now_iso()
    artifact["progress"] = progress
    artifact["completedUrls"] = sorted(completed_urls)
    artifact["updatedAt"] = now_iso()
    _summarize_artifact(
        artifact,
        parsed_rows=parsed_rows,
        representative_rows=representative_rows,
        completed_urls=completed_urls,
    )
    audit_ledger.save_artifact_atomic(artifact, output_path)


def _recovered_active_by_id(artifact: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for row in _as_list(artifact.get("activeCandidates")):
        if not isinstance(row, dict) or not bool(row.get("gamedevmapRecovery")):
            continue
        row_id = _candidate_id(row)
        if row_id:
            rows[row_id] = dict(row)
    return rows


def _index_current_rejections(artifact: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    indexed: dict[str, list[dict[str, Any]]] = {}
    for rejection in _as_list(artifact.get("rejectedForActivation")):
        if not isinstance(rejection, dict):
            continue
        candidate = _as_dict(rejection.get("candidate"))
        keys = {
            str(rejection.get("sourceId") or "").strip(),
            _candidate_id(candidate),
            _candidate_url_key(candidate),
            f"url:{str(rejection.get('url') or '').strip()}",
            f"entry:{str(rejection.get('sourceDirectoryEntryUrl') or '').strip()}",
            f"entry:{str(candidate.get('sourceDirectoryEntryUrl') or '').strip()}",
        }
        for key in keys:
            if key and key not in {"url:", "entry:"}:
                indexed.setdefault(key, []).append(dict(rejection))
    return indexed


def _classify_lost_recovery(
    previous_candidate: dict[str, Any],
    current_rejections: dict[str, list[dict[str, Any]]],
) -> tuple[str, dict[str, Any]]:
    keys = [
        _candidate_id(previous_candidate),
        _candidate_url_key(previous_candidate),
        f"entry:{str(previous_candidate.get('sourceDirectoryEntryUrl') or '').strip()}",
    ]
    matched = next(
        (
            rejection
            for key in keys
            for rejection in current_rejections.get(key, [])
            if key and key != "entry:"
        ),
        {},
    )
    reason = str(matched.get("reason") or "").strip()
    detail = str(matched.get("reasonDetail") or "").strip()
    error = str(matched.get("error") or "").strip().lower()
    if reason == "homepage_fetch_failed":
        return "homepage_fetch_failure", matched
    if reason == "probe_failed":
        return "probe_failure", matched
    if reason == "zero_jobs":
        return "zero_jobs", matched
    if detail in {"social_profile_host", "third_party_profile_host"}:
        return "skipped_profile_host", matched
    if detail == "recovery_fetch_failed" or "timeout" in error or "timed out" in error:
        return "recovery_timeout_or_fetch_failed", matched
    if str(previous_candidate.get("gamedevmapRecoverySource") or "") == "same_party_recovery_url":
        return "skipped_wave_two", matched
    return "unknown", matched


def compare_gamedevmap_recovered_sources(
    *,
    current_artifact: dict[str, Any],
    previous_artifact: dict[str, Any],
) -> dict[str, Any]:
    previous = _recovered_active_by_id(previous_artifact)
    current = _recovered_active_by_id(current_artifact)
    current_rejections = _index_current_rejections(current_artifact)
    lost_rows: list[dict[str, Any]] = []
    cause_counts: Counter[str] = Counter()
    for row_id, previous_candidate in previous.items():
        if row_id in current:
            continue
        cause, matched_rejection = _classify_lost_recovery(
            previous_candidate,
            current_rejections,
        )
        cause_counts[cause] += 1
        lost_rows.append(
            {
                "sourceId": row_id,
                "cause": cause,
                "name": str(previous_candidate.get("name") or ""),
                "adapter": str(previous_candidate.get("adapter") or ""),
                "jobsFound": _safe_int(previous_candidate.get("jobsFound")),
                "recoverySource": str(previous_candidate.get("gamedevmapRecoverySource") or ""),
                "careersUrl": str(
                    previous_candidate.get("careersUrl")
                    or previous_candidate.get("listing_url")
                    or ""
                ),
                "matchedCurrentRejection": matched_rejection,
            }
        )
    return {
        "previousRecoveredActiveCount": len(previous),
        "currentRecoveredActiveCount": len(current),
        "lostCount": len(lost_rows),
        "lossCauseCounts": dict(cause_counts),
        "lostCandidates": sorted(lost_rows, key=lambda row: str(row.get("sourceId") or "")),
    }


def apply_gamedevmap_lost_recovery_audit(
    artifact: dict[str, Any],
    *,
    compare_artifact_path: Path | str | None,
) -> None:
    if compare_artifact_path is None:
        return
    previous = source_registry_module.load_json_object(Path(compare_artifact_path), {})
    if not isinstance(previous, dict) or not previous:
        artifact["lostRecoveryAudit"] = {
            "error": f"compare artifact not found or invalid: {compare_artifact_path}",
            "lostCount": 0,
            "lossCauseCounts": {},
            "lostCandidates": [],
        }
        return
    artifact["lostRecoveryAudit"] = compare_gamedevmap_recovered_sources(
        current_artifact=artifact,
        previous_artifact=previous,
    )


def gamedevmap_audit_report_summary(
    artifact: dict[str, Any],
    *,
    cache_hit: bool = False,
    output_path: Path | str | None = None,
) -> dict[str, Any]:
    summary = audit_report_summary.as_dict(artifact.get("summary"))
    runtime = audit_report_summary.as_dict(artifact.get("runtime"))
    timings = audit_report_summary.as_dict(artifact.get("timings"))
    totals_ms = audit_report_summary.as_dict(timings.get("totalsMs"))
    active_split = audit_report_summary.active_candidate_split(summary)
    return {
        "cacheHit": bool(cache_hit),
        "complete": bool(audit_report_summary.as_dict(artifact.get("progress")).get("complete")),
        "auditDurationMs": audit_report_summary.safe_int(totals_ms.get("totalMs")),
        "activeCandidates": active_split["activeCandidates"],
        "activeProviderCandidates": active_split["activeProviderCandidates"],
        "activeStaticCandidates": active_split["activeStaticCandidates"],
        "recoveredActiveCandidates": audit_report_summary.safe_int(
            summary.get("recoveredActiveCandidates")
        ),
        "browserRecoveryCandidates": audit_report_summary.safe_int(
            summary.get("browserRecoveryCandidates")
        ),
        "browserRecoveredActiveCandidates": audit_report_summary.safe_int(
            summary.get("browserRecoveredActiveCandidates")
        ),
        "artifactSizeBytes": audit_report_summary.artifact_size_bytes(
            summary=summary, runtime=runtime
        ),
        "timingTotalsMs": dict(totals_ms),
        "topFailureBuckets": audit_report_summary.top_failure_buckets(
            rejected_reason_detail_counts=summary.get("rejectedReasonDetailCounts"),
            failure_counts=artifact.get("failureCounts"),
        ),
        "lostRecoveredActiveCandidates": audit_report_summary.safe_int(
            summary.get("lostRecoveredActiveCandidates")
        ),
        "outputPath": str(output_path or gamedevmap_active_dry_run_path()),
    }


def latest_gamedevmap_audit_report_summary() -> dict[str, Any]:
    return dict(LAST_GAMEDEVMAP_AUDIT_REPORT_SUMMARY)


def _save_updated_artifact(artifact: dict[str, Any], output_path: Path) -> None:
    completed_urls = {
        str(item).strip() for item in _as_list(artifact.get("completedUrls")) if str(item).strip()
    }
    _summarize_artifact(
        artifact,
        parsed_rows=[],
        representative_rows=[],
        completed_urls=completed_urls,
    )
    artifact["updatedAt"] = now_iso()
    audit_ledger.save_artifact_atomic(artifact, output_path)


def _default_browser_fetcher():
    return browser_recovery_helpers.default_browser_fetcher()


def _browser_recovery_processed_key(row: dict[str, Any]) -> str:
    return browser_recovery_helpers.browser_recovery_processed_key(row)


def _browser_static_probe_result_from_rendered_html(
    candidate: dict[str, Any],
    *,
    rendered_url: str,
    rendered_html: str,
) -> tuple[dict[str, Any], bool, int, str, int] | None:
    return rendered_static_probe_result(
        candidate,
        rendered_url=rendered_url,
        rendered_html=rendered_html,
    )


def _load_browser_recovery_artifact(
    *,
    output_path: Path,
    cfg: dict[str, Any],
    timeout_s: int,
    run_id: str,
    started_at: str,
) -> dict[str, Any]:
    artifact = source_registry_module.load_json_object(output_path, {})
    if isinstance(artifact, dict) and artifact:
        return artifact
    return _initial_artifact(
        run_id=run_id,
        started_at=started_at or now_iso(),
        timeout_s=timeout_s,
        csv_url=str(cfg.get("csvUrl") or GAMEDEVMAP_CSV_URL),
        index_url=str(cfg.get("indexUrl") or GAMEDEVMAP_INDEX_URL),
        cfg=cfg,
        batch_size=max(1, int(cfg.get("activeAuditBatchSize") or 1000)),
        fetch_concurrency=max(1, int(cfg.get("fetchConcurrency") or 24)),
        per_host_concurrency=max(1, int(cfg.get("perHostConcurrency") or 3)),
        homepage_fetch_concurrency=max(
            1, int(cfg.get("activeAuditHomepageFetchConcurrency") or 32)
        ),
        recovery_fetch_concurrency=max(
            1, int(cfg.get("activeAuditRecoveryFetchConcurrency") or 72)
        ),
        recovery_per_host_concurrency=max(
            1, int(cfg.get("activeAuditRecoveryPerHostConcurrency") or 4)
        ),
        recovery_timeout_s=max(
            1, min(int(timeout_s), int(cfg.get("activeAuditRecoveryTimeoutSeconds") or 5))
        ),
    )


def _select_browser_recovery_candidates(
    artifact: dict[str, Any],
    cfg: dict[str, Any],
    browser_recovery: dict[str, Any],
) -> tuple[list[dict[str, Any]], set[str]]:
    limit = max(0, int(cfg.get("activeAuditBrowserRecoveryLimit") or 0))
    return browser_recovery_helpers.select_unprocessed_candidates(
        [dict(row) for row in _as_list(artifact.get("browserRecoveryCandidates"))],
        browser_recovery=browser_recovery,
        limit=limit,
    )


def _record_browser_fetch_sample(
    browser_recovery: dict[str, Any],
    *,
    source_url: str,
    duration_ms: int,
    html: str,
) -> None:
    browser_recovery_helpers.append_fetch_sample(
        browser_recovery,
        source_url=source_url,
        duration_ms=duration_ms,
        html=html,
    )


def _analyze_browser_recovery_fetches(
    *,
    fetch_results: list[tuple[dict[str, Any], str, str, int]],
    cfg: dict[str, Any],
    browser_recovery: dict[str, Any],
    processed: set[str],
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[tuple[dict[str, Any], bool, int, str, int]],
]:
    provider_candidates: list[dict[str, Any]] = []
    static_candidates: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    rendered_probe_results: list[tuple[dict[str, Any], bool, int, str, int]] = []
    index_url = str(cfg.get("indexUrl") or GAMEDEVMAP_INDEX_URL)
    for row, html, error, duration_ms in fetch_results:
        key = _browser_recovery_processed_key(row)
        if key:
            processed.add(key)
        source_url = str(row.get("url") or "").strip()
        if error or not html:
            rejected.append(
                _rejection(
                    reason="no_careers_evidence",
                    row={"studio": row.get("studio"), "url": source_url},
                    error=error or "browser fallback returned empty content",
                    reason_detail="browser_recovery_fetch_failed",
                    failure_bucket="technical_failure",
                )
            )
            continue
        row_payload = {
            "studio": row.get("studio"),
            "url": source_url,
            "sourceDirectoryEntryUrl": row.get("sourceDirectoryEntryUrl"),
        }
        provider_candidates.extend(
            _provider_candidates_from_html_text(
                row=row_payload,
                page_url=source_url,
                html=html,
                index_url=index_url,
            )
        )
        static_before = len(static_candidates)
        _append_analyzed_candidates(
            page_url=source_url,
            html=html,
            row=row_payload,
            index_url=index_url,
            recovery_source="browser_rendered_homepage",
            provider_candidates=provider_candidates,
            static_candidates=static_candidates,
        )
        rendered_probe_results.extend(
            result
            for candidate in static_candidates[static_before:]
            for result in [
                _browser_static_probe_result_from_rendered_html(
                    candidate,
                    rendered_url=source_url,
                    rendered_html=html,
                )
            ]
            if result is not None
        )
        _record_browser_fetch_sample(
            browser_recovery,
            source_url=source_url,
            duration_ms=duration_ms,
            html=html,
        )
    all_candidates, bad_provider_rejections = _filter_bad_provider_inferences(
        unique_sources([*provider_candidates, *static_candidates])
    )
    return all_candidates, [*rejected, *bad_provider_rejections], rendered_probe_results


def _analyze_browser_recovery_batch(
    cfg: dict[str, Any],
):
    def _analyze(
        fetch_results: list[tuple[dict[str, Any], str, str, int]],
        browser_recovery: dict[str, Any],
        processed: set[str],
    ) -> browser_recovery_helpers.BrowserRecoveryAnalysis:
        all_candidates, rejected, rendered_probe_results = _analyze_browser_recovery_fetches(
            fetch_results=fetch_results,
            cfg=cfg,
            browser_recovery=browser_recovery,
            processed=processed,
        )
        return browser_recovery_helpers.BrowserRecoveryAnalysis(
            all_candidates=all_candidates,
            rendered_probe_results=rendered_probe_results,
            rejected_rows=rejected,
        )

    return _analyze


def _mark_browser_recovery_probe_results(
    probe_results: list[tuple[dict[str, Any], bool, int, str, int]],
    *,
    rendered_count: int,
) -> None:
    for index, (candidate, ok, jobs_found, _error, _duration_ms) in enumerate(probe_results):
        if ok and jobs_found > 0:
            candidate["gamedevmapBrowserRecovery"] = True
        if index < rendered_count:
            candidate["probeStatus"] = "ok"
            candidate["candidateState"] = "validated"


def _merge_browser_recovery_artifact_updates(
    *,
    artifact: dict[str, Any],
    browser_recovery: dict[str, Any],
    all_candidates: list[dict[str, Any]],
    rejected: list[dict[str, Any]],
    processed: set[str],
    started: float,
    probe_candidates: list[dict[str, Any]],
    rendered_probe_results: list[tuple[dict[str, Any], bool, int, str, int]],
    probe_results: list[tuple[dict[str, Any], bool, int, str, int]],
) -> None:
    combined_probe_results = [*rendered_probe_results, *probe_results]
    _mark_browser_recovery_probe_results(
        combined_probe_results,
        rendered_count=len(rendered_probe_results),
    )
    artifact["allCandidates"] = _merge_unique_rows(artifact.get("allCandidates"), all_candidates)
    artifact["rejectedForActivation"] = [
        *(_as_list(artifact.get("rejectedForActivation"))),
        *rejected,
    ]
    _apply_probe_results(artifact, combined_probe_results)
    active_browser_count = len(
        [
            row
            for row in _as_list(artifact.get("activeCandidates"))
            if isinstance(row, dict) and bool(row.get("gamedevmapBrowserRecovery"))
        ]
    )
    browser_recovery_helpers.update_browser_recovery_state(
        browser_recovery,
        processed=processed,
        started=started,
        candidate_count=len(_as_list(artifact.get("browserRecoveryCandidates"))),
        activeCandidates=active_browser_count,
        probeCandidates=len(probe_candidates),
        renderedStaticValidated=len(rendered_probe_results),
    )
    artifact["browserRecovery"] = browser_recovery


def run_gamedevmap_browser_recovery(
    *,
    timeout_s: int,
    config: dict[str, Any] | None = None,
    fetcher=fetch_text,
    output_path: Path | None = None,
    run_id: str = "",
    started_at: str = "",
    browser_fetcher=None,
) -> dict[str, Any]:
    cfg = dict(
        _gamedevmap_config_value(config, "gamedevmap", DEFAULT_DISCOVERY_CONFIG["gamedevmap"])
    )
    output_path = output_path or gamedevmap_active_dry_run_path()
    artifact = _load_browser_recovery_artifact(
        output_path=output_path,
        cfg=cfg,
        timeout_s=timeout_s,
        run_id=run_id,
        started_at=started_at,
    )
    browser_fetcher = browser_fetcher or _default_browser_fetcher()
    browser_recovery = _as_dict(artifact.get("browserRecovery"))
    candidates, processed = _select_browser_recovery_candidates(artifact, cfg, browser_recovery)
    concurrency = max(1, int(cfg.get("activeAuditBrowserRecoveryConcurrency") or 2))
    browser_timeout_s = max(
        1,
        min(max(1, int(timeout_s)), int(cfg.get("activeAuditBrowserRecoveryTimeoutSeconds") or 15)),
    )
    batch = browser_recovery_helpers.run_browser_recovery_batch(
        selected=candidates,
        processed=processed,
        browser_recovery=browser_recovery,
        timeout_s=browser_timeout_s,
        fetcher=fetcher,
        browser_fetcher=browser_fetcher,
        concurrency=concurrency,
        analyze_fetches=_analyze_browser_recovery_batch(cfg),
        probe_timeout_s=timeout_s,
        emit_log=emit_log,
        log_label="GameDevMap browser recovery",
    )
    _merge_browser_recovery_artifact_updates(
        artifact=artifact,
        browser_recovery=browser_recovery,
        all_candidates=batch.analysis.all_candidates,
        rejected=list(batch.analysis.rejected_rows or []),
        processed=batch.processed,
        started=batch.started,
        probe_candidates=batch.probe_candidates,
        rendered_probe_results=batch.analysis.rendered_probe_results,
        probe_results=batch.probe_results,
    )
    _save_updated_artifact(artifact, output_path)
    return artifact


def _validated_static_audit_candidate(
    row: dict[str, Any],
    *,
    promote_validated_static: bool,
    validated_static_queue_cap: int,
    validated_static_domain_cap: int,
) -> dict[str, Any] | None:
    if not promote_validated_static:
        return None
    return apply_prevalidated_queue_overrides(
        row,
        adapter_cap=validated_static_queue_cap,
        domain_cap=validated_static_domain_cap,
    )


def _html_url_candidates(html: str) -> list[str]:
    return recovery_url_planner.html_url_candidates(html)


def _looks_like_js_shell(html: str) -> bool:
    return shared_looks_like_js_shell(html, include_noscript_script_shell=True)


def _no_careers_reason_detail(page_url: str, html: str) -> str:
    return shared_no_candidate_reason_detail(
        page_url,
        html,
        social_profile_hosts=SOCIAL_PROFILE_HOSTS,
        third_party_profile_hosts=THIRD_PARTY_PROFILE_HOSTS,
        jobish_url_fn=lambda url, body: extract_jobish_links(body, url),
        include_noscript_script_shell=True,
    )


def _recovery_urls(
    page_url: str,
    html: str,
    *,
    limit: int = 6,
    paths: tuple[str, ...] = NO_CAREERS_RECOVERY_PATHS,
    include_jobish_links: bool = True,
) -> list[str]:
    return recovery_url_planner.recovery_urls(
        page_url,
        html,
        paths=paths,
        limit=limit,
        blocked_hosts=SOCIAL_PROFILE_HOSTS | THIRD_PARTY_PROFILE_HOSTS,
        include_jobish_links=include_jobish_links,
        html_url_candidate_fn=_html_url_candidates,
    )


def _provider_candidates_from_html_text(
    *,
    row: dict[str, Any],
    page_url: str,
    html: str,
    index_url: str,
) -> list[dict[str, Any]]:
    studio = str(row.get("studio") or "").strip()
    candidates: list[dict[str, Any]] = []
    for inferred_row in infer_provider_candidates_from_html(
        page_url=page_url,
        html=html,
        studio=studio,
        nl_priority=False,
        discovery_method="gamedevmap",
    ):
        inferred = dict(inferred_row)
        inferred["careersUrl"] = page_url
        inferred["gamedevmapRecovery"] = True
        inferred["gamedevmapRecoverySource"] = "homepage_html_provider_url"
        inferred["evidenceTypes"] = list(
            dict.fromkeys(
                [
                    *(inferred.get("evidenceTypes") or []),
                    "gamedevmap_recovery_provider_url",
                ]
            )
        )
        candidates.append(
            _apply_gamedevmap_provenance(
                inferred,
                row,
                index_url=index_url,
                include_homepage_fetch=True,
            )
        )
    return candidates


def _gamedevmap_page_outcome(
    *,
    page_url: str,
    html: str,
    row: dict[str, Any],
    index_url: str,
    recovery_source: str = "",
) -> PageOutcome:
    studio = str(row.get("studio") or "").strip()
    context = FetchedPageContext(
        page_url=page_url,
        html=html,
        studio=studio,
        nl_priority=False,
        discovery_method="gamedevmap",
    )
    provider_rows, explicit_static, generic_static = static_page_outcome_builders(
        name_suffix="GameDevMap",
        evidence_source="gamedevmap",
        evidence_types=(
            ["gamedevmap_careers_url", "gamedevmap_recovery_page"]
            if recovery_source
            else ["gamedevmap_careers_url"]
        ),
        evidence_score=44 if recovery_source else 40,
        enabled_by_default=False,
    )

    def _mark_candidate(candidate: dict[str, Any]) -> dict[str, Any]:
        marked = dict(candidate)
        if recovery_source:
            marked["gamedevmapRecovery"] = True
            marked["gamedevmapRecoverySource"] = recovery_source
        return _apply_gamedevmap_provenance(
            marked,
            row,
            index_url=index_url,
            include_homepage_fetch=True,
        )

    def _provider_rows(
        rows: list[dict[str, Any]],
        outcome_context: FetchedPageContext,
    ) -> list[dict[str, Any]]:
        return [_mark_candidate(candidate) for candidate in provider_rows(rows, outcome_context)]

    def _explicit_static(
        explicit_careers_url: str,
        outcome_context: FetchedPageContext,
    ) -> dict[str, Any]:
        return _mark_candidate(explicit_static(explicit_careers_url, outcome_context))

    def _generic_static(
        candidate: dict[str, Any],
        outcome_context: FetchedPageContext,
    ) -> dict[str, Any]:
        return _mark_candidate(generic_static(candidate, outcome_context))

    return classify_fetched_page(
        context,
        provider_rows=_provider_rows,
        explicit_static=_explicit_static,
        generic_static=_generic_static,
        analyze_page=analyze_fetched_page,
    )


def _append_analyzed_candidates(
    *,
    page_url: str,
    html: str,
    row: dict[str, Any],
    index_url: str,
    recovery_source: str,
    provider_candidates: list[dict[str, Any]],
    static_candidates: list[dict[str, Any]],
) -> bool:
    outcome = _gamedevmap_page_outcome(
        page_url=page_url,
        html=html,
        row=row,
        index_url=index_url,
        recovery_source=recovery_source,
    )
    provider_candidates.extend(outcome.provider_candidates)
    static_candidates.extend(outcome.static_candidates)
    return outcome.found_candidates


def _recovery_job(
    *,
    row: dict[str, Any],
    homepage_url: str,
    recovery_url: str,
    reason_detail: str,
    recovery_source: str,
    wave: int,
) -> dict[str, Any]:
    return {
        "url": recovery_url,
        "payload": {
            "row": row,
            "homepageUrl": homepage_url,
            "homepageReasonDetail": reason_detail,
            "recoverySource": recovery_source,
            "recoveryWave": int(wave),
        },
        "name": f"{str(row.get('studio') or '').strip()} recovery {recovery_url}",
        "adapter": "gamedevmap",
        "failureStage": "gamedevmap_recovery_fetch",
    }


def _queue_no_careers_recovery(
    *,
    row: dict[str, Any],
    target_url: str,
    html: str,
    index_url: str,
    provider_candidates: list[dict[str, Any]],
    primary_recovery_jobs: list[dict[str, Any]],
    secondary_recovery_jobs: list[dict[str, Any]],
    browser_recovery_candidates: list[dict[str, Any]],
) -> bool:
    studio = str(row.get("studio") or "").strip()
    detail = _no_careers_reason_detail(target_url, html)
    if detail == "js_shell":
        browser_recovery_candidates.append(
            {
                "adapter": "gamedevmap",
                "name": f"{studio} browser recovery",
                "studio": studio,
                "url": target_url,
                "sourceDirectoryEntryUrl": str(row.get("sourceDirectoryEntryUrl") or "").strip(),
                "reasonDetail": detail,
            }
        )
    row_provider_candidates = _provider_candidates_from_html_text(
        row=row,
        page_url=target_url,
        html=html,
        index_url=index_url,
    )
    provider_candidates.extend(row_provider_candidates)
    primary_recovery_urls = _recovery_urls(
        target_url,
        html,
        paths=PRIMARY_RECOVERY_PATHS,
        include_jobish_links=True,
    )
    secondary_recovery_urls = _recovery_urls(
        target_url,
        html,
        paths=SECONDARY_RECOVERY_PATHS,
        include_jobish_links=False,
    )
    for recovery_url in primary_recovery_urls:
        primary_recovery_jobs.append(
            _recovery_job(
                row=row,
                homepage_url=target_url,
                recovery_url=recovery_url,
                reason_detail=detail,
                recovery_source="same_party_recovery_url",
                wave=1,
            )
        )
    for recovery_url in secondary_recovery_urls:
        secondary_recovery_jobs.append(
            _recovery_job(
                row=row,
                homepage_url=target_url,
                recovery_url=recovery_url,
                reason_detail=detail,
                recovery_source="same_party_recovery_url",
                wave=2,
            )
        )
    return bool(primary_recovery_urls or secondary_recovery_urls or row_provider_candidates)


def _extract_candidates_from_homepages(
    *,
    batch_rows: list[dict[str, Any]],
    homepage_fetch_results: list[dict[str, Any]],
    index_url: str,
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    int,
]:
    provider_candidates: list[dict[str, Any]] = []
    static_candidates: list[dict[str, Any]] = []
    rejected_for_activation: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    primary_recovery_jobs: list[dict[str, Any]] = []
    secondary_recovery_jobs: list[dict[str, Any]] = []
    browser_recovery_candidates: list[dict[str, Any]] = []
    homepages_fetched = 0
    fetched_urls = {str(result.get("url") or "").strip() for result in homepage_fetch_results}
    direct_rows = [row for row in batch_rows if _row_url(row) not in fetched_urls]

    for row in direct_rows:
        studio = str(row.get("studio") or "").strip()
        inferred = infer_web_candidate(
            _row_url(row),
            studio,
            nl_priority=False,
            discovery_method="gamedevmap",
        )
        if inferred:
            inferred["careersUrl"] = _row_url(row)
            provider_candidates.append(
                _apply_gamedevmap_provenance(
                    inferred,
                    row,
                    index_url=index_url,
                    include_direct_url=True,
                )
            )

    for result in homepage_fetch_results:
        row = dict(result.get("payload") or {})
        target_url = str(result.get("url") or row.get("url") or "").strip()
        studio = str(row.get("studio") or "").strip()
        if not bool(result.get("ok")):
            failure = result.get("failure")
            if isinstance(failure, dict):
                failures.append(failure)
            rejected_for_activation.append(
                _rejection(
                    reason="homepage_fetch_failed",
                    row=row,
                    error=_error_text(result),
                    reason_detail="homepage_fetch_failed",
                )
            )
            continue
        homepages_fetched += 1
        html = str(result.get("text") or "")
        outcome = _gamedevmap_page_outcome(
            page_url=target_url,
            html=html,
            row=row,
            index_url=index_url,
        )
        if outcome.found_candidates:
            provider_candidates.extend(outcome.provider_candidates)
            static_candidates.extend(outcome.static_candidates)
            continue
        else:
            detail = _no_careers_reason_detail(target_url, html)
            queued = _queue_no_careers_recovery(
                row=row,
                target_url=target_url,
                html=html,
                index_url=index_url,
                provider_candidates=provider_candidates,
                primary_recovery_jobs=primary_recovery_jobs,
                secondary_recovery_jobs=secondary_recovery_jobs,
                browser_recovery_candidates=browser_recovery_candidates,
            )
            if not queued:
                rejected_for_activation.append(
                    _rejection(
                        reason="no_careers_evidence",
                        row=row,
                        reason_detail=detail,
                    )
                )

    return (
        provider_candidates,
        static_candidates,
        rejected_for_activation,
        primary_recovery_jobs,
        secondary_recovery_jobs,
        browser_recovery_candidates,
        homepages_fetched,
    )


def _dedupe_recovery_jobs(jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_url: dict[str, dict[str, Any]] = {}
    for job in jobs:
        if not isinstance(job, dict):
            continue
        url = str(job.get("url") or "").strip()
        if not url:
            continue
        payload = _as_dict(job.get("payload"))
        existing = by_url.get(url)
        if existing is None:
            existing = {
                **dict(job),
                "payload": {
                    "requests": [payload],
                    "dedupeCount": 1,
                },
            }
            by_url[url] = existing
        else:
            existing_payload = _as_dict(existing.get("payload"))
            requests = _as_list(existing_payload.get("requests"))
            requests.append(payload)
            existing_payload["requests"] = requests
            existing_payload["dedupeCount"] = len(requests)
            existing["payload"] = existing_payload
    return list(by_url.values())


def _requests_from_recovery_result(result: dict[str, Any]) -> list[dict[str, Any]]:
    payload = _as_dict(result.get("payload"))
    requests = [item for item in _as_list(payload.get("requests")) if isinstance(item, dict)]
    return requests or [payload]


def _recovery_cache_result(
    cached: dict[str, Any],
    job: dict[str, Any],
) -> dict[str, Any]:
    result = {
        "job": job,
        "payload": job.get("payload"),
        "url": str(job.get("url") or cached.get("url") or ""),
        "ok": bool(cached.get("ok")),
        "text": str(cached.get("text") or ""),
        "error": str(cached.get("error") or ""),
    }
    if bool(result["ok"]):
        result["failure"] = None
    else:
        result["failure"] = {
            "name": str(job.get("name") or result["url"]),
            "adapter": str(job.get("adapter") or ""),
            "error": str(result["error"] or ""),
            "stage": str(job.get("failureStage") or ""),
        }
    return result


def _fetch_recovery_jobs(
    *,
    timeout_s: int,
    jobs: list[dict[str, Any]],
    fetcher,
    total_concurrency: int,
    per_host_concurrency: int,
    progress_label: str,
    recovery_cache: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], int, int]:
    deduped_jobs = _dedupe_recovery_jobs(jobs)
    cached_results: list[dict[str, Any]] = []
    fetch_jobs: list[dict[str, Any]] = []
    for job in deduped_jobs:
        url = str(job.get("url") or "").strip()
        cached = recovery_cache.get(url)
        if cached is not None:
            cached_results.append(_recovery_cache_result(cached, job))
        else:
            fetch_jobs.append(job)
    fetched_results = fetch_directory_pages(
        timeout_s,
        fetch_jobs,
        fetcher=fetcher,
        total_concurrency=total_concurrency,
        per_host_concurrency=per_host_concurrency,
        progress_label=progress_label,
    )
    for result in fetched_results:
        url = str(result.get("url") or "").strip()
        if not url:
            continue
        recovery_cache[url] = {
            "url": url,
            "ok": bool(result.get("ok")),
            "text": str(result.get("text") or ""),
            "error": _error_text(result),
        }
    return [*cached_results, *fetched_results], len(deduped_jobs), len(fetch_jobs)


def _apply_recovery_payload_to_group(
    *,
    payload: dict[str, Any],
    result: dict[str, Any],
    grouped: dict[str, dict[str, Any]],
    index_url: str,
    provider_candidates: list[dict[str, Any]],
    static_candidates: list[dict[str, Any]],
) -> str:
    row = _as_dict(payload.get("row"))
    homepage_url = str(payload.get("homepageUrl") or _row_url(row)).strip()
    group = grouped.setdefault(
        homepage_url,
        {
            "row": row,
            "reasonDetail": str(payload.get("homepageReasonDetail") or ""),
            "attempts": 0,
            "fetched": 0,
            "candidates": 0,
            "failures": 0,
        },
    )
    group["attempts"] = _safe_int(group.get("attempts")) + 1
    if not bool(result.get("ok")):
        group["failures"] = _safe_int(group.get("failures")) + 1
        return ""
    group["fetched"] = _safe_int(group.get("fetched")) + 1
    before = len(provider_candidates) + len(static_candidates)
    found = _append_analyzed_candidates(
        page_url=str(result.get("url") or "").strip(),
        html=str(result.get("text") or ""),
        row=row,
        index_url=index_url,
        recovery_source=str(payload.get("recoverySource") or "same_party_recovery_url"),
        provider_candidates=provider_candidates,
        static_candidates=static_candidates,
    )
    if not found:
        return ""
    group["candidates"] = _safe_int(group.get("candidates")) + (
        len(provider_candidates) + len(static_candidates) - before
    )
    return homepage_url


def _apply_recovery_results(
    *,
    recovery_fetch_results: list[dict[str, Any]],
    index_url: str,
    grouped: dict[str, dict[str, Any]] | None = None,
    finalize: bool = True,
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    int,
    dict[str, dict[str, Any]],
    set[str],
]:
    provider_candidates: list[dict[str, Any]] = []
    static_candidates: list[dict[str, Any]] = []
    rejected_for_activation: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    pages_fetched = 0
    grouped = grouped or {}
    recovered_homepages: set[str] = set()

    for result in recovery_fetch_results:
        requests = _requests_from_recovery_result(result)
        if not bool(result.get("ok")):
            failure = result.get("failure")
            if isinstance(failure, dict):
                failures.append(failure)
        else:
            pages_fetched += 1
        for payload in requests:
            recovered_homepage = _apply_recovery_payload_to_group(
                payload=payload,
                result=result,
                grouped=grouped,
                index_url=index_url,
                provider_candidates=provider_candidates,
                static_candidates=static_candidates,
            )
            if recovered_homepage:
                recovered_homepages.add(recovered_homepage)

    if finalize:
        for group in grouped.values():
            if _safe_int(group.get("candidates")) > 0:
                continue
            detail = "recovery_pages_no_jobs"
            if _safe_int(group.get("fetched")) == 0 and _safe_int(group.get("failures")) > 0:
                detail = "recovery_fetch_failed"
            rejected_for_activation.append(
                _rejection(
                    reason="no_careers_evidence",
                    row=_as_dict(group.get("row")),
                    reason_detail=detail,
                )
            )

    return (
        provider_candidates,
        static_candidates,
        rejected_for_activation,
        failures,
        pages_fetched,
        grouped,
        recovered_homepages,
    )


def _filter_bad_provider_inferences(
    candidates: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    good: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for candidate in candidates:
        detail = bad_provider_inference_detail(candidate)
        if detail:
            rejected.append(
                _rejection(
                    reason="bad_provider_inference",
                    candidate=candidate,
                    reason_detail=detail,
                )
            )
            continue
        good.append(candidate)
    return good, rejected


def _parse_rerun_reasons(value: str | list[str] | tuple[str, ...] | None) -> set[str]:
    if not value:
        return set()
    raw_items: list[str] = []
    if isinstance(value, str):
        raw_items = value.split(",")
    else:
        raw_items = [str(item) for item in value]
    return {
        item.strip()
        for item in raw_items
        if item.strip() and item.strip() in GAMEDEVMAP_RERUN_REASONS
    }


def _rejection_row_key(rejection: dict[str, Any]) -> str:
    url = str(rejection.get("url") or "").strip()
    if url:
        return f"url:{url}"
    candidate = _as_dict(rejection.get("candidate"))
    entry_url = str(candidate.get("sourceDirectoryEntryUrl") or "").strip()
    if entry_url:
        return f"entry:{entry_url}"
    careers_url = str(candidate.get("careersUrl") or candidate.get("listing_url") or "").strip()
    if careers_url:
        return f"url:{careers_url}"
    return ""


def _row_keys(row: dict[str, Any]) -> set[str]:
    keys = set()
    url = _row_url(row)
    if url:
        keys.add(f"url:{url}")
    entry_url = str(row.get("sourceDirectoryEntryUrl") or "").strip()
    if entry_url:
        keys.add(f"entry:{entry_url}")
    return keys


def _select_rerun_rows(
    artifact: dict[str, Any],
    representative_rows: list[dict[str, Any]],
    rerun_reasons: set[str],
) -> tuple[list[dict[str, Any]], set[str]]:
    if not rerun_reasons:
        return representative_rows, set()
    requested_keys = {
        key
        for rejection in _as_list(artifact.get("rejectedForActivation"))
        if isinstance(rejection, dict)
        and str(rejection.get("reason") or "").strip() in rerun_reasons
        for key in [_rejection_row_key(rejection)]
        if key
    }
    if not requested_keys:
        return [], set()
    rows = [row for row in representative_rows if _row_keys(row) & requested_keys]
    return rows, requested_keys


def _prune_rerun_rejections(
    artifact: dict[str, Any],
    *,
    rerun_reasons: set[str],
    rerun_row_keys: set[str],
) -> None:
    if not rerun_reasons or not rerun_row_keys:
        return
    kept: list[dict[str, Any]] = []
    for rejection in _as_list(artifact.get("rejectedForActivation")):
        if not isinstance(rejection, dict):
            continue
        reason = str(rejection.get("reason") or "").strip()
        key = _rejection_row_key(rejection)
        if reason in rerun_reasons and key in rerun_row_keys:
            continue
        kept.append(rejection)
    artifact["rejectedForActivation"] = kept


async def _probe_candidates_async(
    candidates: list[dict[str, Any]],
    *,
    timeout_s: int,
    fetcher,
) -> list[tuple[dict[str, Any], bool, int, str, int]]:
    return await shared_probe_candidates_async(candidates, timeout_s=timeout_s, fetcher=fetcher)


def _apply_probe_results(
    artifact: dict[str, Any],
    probe_results: list[tuple[dict[str, Any], bool, int, str, int]],
) -> None:
    active_candidates: list[dict[str, Any]] = []
    zero_job_candidates: list[dict[str, Any]] = []
    rejected_for_activation: list[dict[str, Any]] = []
    for candidate, ok, jobs_found, error, duration_ms in probe_results:
        if not ok:
            rejected_for_activation.append(
                _rejection(
                    reason="probe_failed",
                    candidate=candidate,
                    error=error,
                    reason_detail="probe_failed",
                )
            )
            continue
        normalized = _candidate_with_probe_evidence(candidate, jobs_found)
        normalized["probeDurationMs"] = int(duration_ms)
        if jobs_found > 0:
            active_candidates.append(normalized)
        else:
            zero_job_candidates.append(normalized)
            rejected_for_activation.append(
                _rejection(
                    reason="zero_jobs",
                    candidate=normalized,
                    jobs_found=jobs_found,
                    reason_detail="zero_jobs",
                )
            )
    artifact["activeCandidates"] = _merge_by_source_id(
        artifact.get("activeCandidates"), active_candidates
    )
    artifact["zeroJobCandidates"] = _merge_by_source_id(
        artifact.get("zeroJobCandidates"), zero_job_candidates
    )
    artifact["rejectedForActivation"] = [
        *(_as_list(artifact.get("rejectedForActivation"))),
        *rejected_for_activation,
    ]


def run_gamedevmap_active_source_dry_run(
    *,
    timeout_s: int,
    config: dict[str, Any] | None = None,
    fetcher=fetch_text,
    output_path: Path | None = None,
    run_id: str = "",
    started_at: str = "",
    batch_size: int = 1000,
    reset: bool = False,
    max_batches: int = 0,
    rerun_reasons: str | list[str] | tuple[str, ...] | None = None,
    compare_artifact_path: Path | str | None = None,
) -> dict[str, Any]:
    started = str(started_at or now_iso())
    cfg = dict(
        _gamedevmap_config_value(config, "gamedevmap", DEFAULT_DISCOVERY_CONFIG["gamedevmap"])
    )
    cfg["enabled"] = True
    csv_url = str(cfg.get("csvUrl") or GAMEDEVMAP_CSV_URL).strip() or GAMEDEVMAP_CSV_URL
    index_url = str(cfg.get("indexUrl") or GAMEDEVMAP_INDEX_URL).strip() or GAMEDEVMAP_INDEX_URL
    batch_size = max(1, int(batch_size or 250))
    max_batches = max(0, int(max_batches or 0))
    parsed_rerun_reasons = set() if reset else _parse_rerun_reasons(rerun_reasons)
    fetch_concurrency, per_host_concurrency = resolve_directory_fetch_limits(cfg)
    homepage_fetch_concurrency = max(
        1, int(cfg.get("activeAuditHomepageFetchConcurrency") or fetch_concurrency)
    )
    recovery_fetch_concurrency = max(
        1, int(cfg.get("activeAuditRecoveryFetchConcurrency") or fetch_concurrency)
    )
    recovery_per_host_concurrency = max(
        1, int(cfg.get("activeAuditRecoveryPerHostConcurrency") or per_host_concurrency)
    )
    recovery_timeout_s = max(
        1,
        min(
            int(timeout_s),
            int(cfg.get("activeAuditRecoveryTimeoutSeconds") or min(int(timeout_s), 5)),
        ),
    )
    output_path = output_path or gamedevmap_active_dry_run_path()

    emit_log("GameDevMap active-source dry run: fetching CSV.")
    csv_text = fetcher(csv_url, timeout_s)
    parsed_rows = parse_gamedevmap_csv(csv_text)
    representative_rows = select_gamedevmap_representative_rows(
        parsed_rows,
        allowed_categories=[
            str(item).strip() for item in (cfg.get("allowedCategories") or []) if str(item).strip()
        ],
        blocked_categories=[
            str(item).strip() for item in (cfg.get("blockedCategories") or []) if str(item).strip()
        ],
        require_ai_reviewed=bool(cfg.get("requireAiReviewed", False)),
        index_url=index_url,
    )
    artifact = _load_or_initialize_artifact(
        output_path,
        reset=reset,
        run_id=run_id,
        started_at=started,
        timeout_s=timeout_s,
        csv_url=csv_url,
        index_url=index_url,
        cfg=cfg,
        batch_size=batch_size,
        fetch_concurrency=fetch_concurrency,
        per_host_concurrency=per_host_concurrency,
        homepage_fetch_concurrency=homepage_fetch_concurrency,
        recovery_fetch_concurrency=recovery_fetch_concurrency,
        recovery_per_host_concurrency=recovery_per_host_concurrency,
        recovery_timeout_s=recovery_timeout_s,
    )
    completed_urls = {
        str(item).strip() for item in _as_list(artifact.get("completedUrls")) if str(item).strip()
    }
    if parsed_rerun_reasons:
        representative_rows, rerun_row_keys = _select_rerun_rows(
            artifact,
            representative_rows,
            parsed_rerun_reasons,
        )
        _prune_rerun_rejections(
            artifact,
            rerun_reasons=parsed_rerun_reasons,
            rerun_row_keys=rerun_row_keys,
        )
        completed_urls = set()
        progress = _as_dict(artifact.get("progress"))
        progress["rerunReasons"] = sorted(parsed_rerun_reasons)
        progress["complete"] = False
        progress["completedUrlsCount"] = 0
        artifact["progress"] = progress

    batches_run = 0
    recovery_cache: dict[str, dict[str, Any]] = {}
    while True:
        batch_rows, cursor = _next_batch(representative_rows, completed_urls, batch_size)
        progress = _as_dict(artifact.get("progress"))
        progress["cursorPosition"] = int(cursor)
        artifact["progress"] = progress
        if not batch_rows:
            apply_gamedevmap_lost_recovery_audit(
                artifact,
                compare_artifact_path=compare_artifact_path,
            )
            _write_artifact(
                artifact,
                output_path,
                parsed_rows=parsed_rows,
                representative_rows=representative_rows,
                completed_urls=completed_urls,
                complete=True,
            )
            break
        if max_batches and batches_run >= max_batches:
            apply_gamedevmap_lost_recovery_audit(
                artifact,
                compare_artifact_path=compare_artifact_path,
            )
            _write_artifact(
                artifact,
                output_path,
                parsed_rows=parsed_rows,
                representative_rows=representative_rows,
                completed_urls=completed_urls,
                complete=False,
            )
            break

        emit_log(
            "GameDevMap active-source dry run: "
            f"batch={batches_run + 1}, rows={len(batch_rows)}, cursor={cursor}."
        )
        batch_started = time.perf_counter()
        batch_timing: dict[str, Any] = {
            "batch": _safe_int(progress.get("batchesCompleted")) + 1,
            "rows": len(batch_rows),
            "cursor": int(cursor),
        }
        direct_provider_rows: list[dict[str, Any]] = []
        homepage_rows: list[dict[str, Any]] = []
        rejected_missing: list[dict[str, Any]] = []
        direct_started = time.perf_counter()
        for row in batch_rows:
            studio = str(row.get("studio") or "").strip()
            target_url = _row_url(row)
            if not studio or not target_url:
                rejected_missing.append(
                    _rejection(
                        reason="missing_studio_or_url",
                        row=row,
                        reason_detail="missing_studio_or_url",
                    )
                )
                continue
            inferred = infer_web_candidate(
                target_url,
                studio,
                nl_priority=False,
                discovery_method="gamedevmap",
            )
            if inferred:
                inferred["careersUrl"] = target_url
                direct_provider_rows.append(
                    _apply_gamedevmap_provenance(
                        inferred,
                        row,
                        index_url=index_url,
                        include_direct_url=True,
                    )
                )
            else:
                homepage_rows.append(row)
        batch_timing["directInferenceMs"] = _duration_ms(direct_started)

        homepage_fetch_started = time.perf_counter()
        homepage_fetch_results = fetch_directory_pages(
            timeout_s,
            [
                {
                    "url": _row_url(row),
                    "payload": row,
                    "name": _row_url(row),
                    "adapter": "gamedevmap",
                    "failureStage": "homepage_fetch",
                }
                for row in homepage_rows
            ],
            fetcher=fetcher,
            total_concurrency=homepage_fetch_concurrency,
            per_host_concurrency=per_host_concurrency,
            progress_label="GameDevMap active dry run homepage fetch",
        )
        batch_timing["homepageFetchMs"] = _duration_ms(homepage_fetch_started)
        homepage_analysis_started = time.perf_counter()
        (
            provider_rows,
            static_rows,
            rejected_rows,
            primary_recovery_jobs,
            secondary_recovery_jobs,
            browser_recovery_rows,
            homepages_fetched,
        ) = _extract_candidates_from_homepages(
            batch_rows=[],
            homepage_fetch_results=homepage_fetch_results,
            index_url=index_url,
        )
        batch_timing["homepageAnalysisMs"] = _duration_ms(homepage_analysis_started)

        recovery_wave1_fetch_started = time.perf_counter()
        recovery_wave1_results, wave1_unique_jobs, wave1_network_jobs = _fetch_recovery_jobs(
            timeout_s=recovery_timeout_s,
            jobs=primary_recovery_jobs,
            fetcher=fetcher,
            total_concurrency=recovery_fetch_concurrency,
            per_host_concurrency=recovery_per_host_concurrency,
            progress_label="GameDevMap active dry run careers recovery fetch wave 1",
            recovery_cache=recovery_cache,
        )
        batch_timing["recoveryWave1FetchMs"] = _duration_ms(recovery_wave1_fetch_started)
        recovery_wave1_analysis_started = time.perf_counter()
        (
            recovery_provider_rows_1,
            recovery_static_rows_1,
            _recovery_rejected_rows_1,
            recovery_failures_1,
            recovery_pages_fetched_1,
            recovery_groups,
            recovered_homepages_1,
        ) = _apply_recovery_results(
            recovery_fetch_results=recovery_wave1_results,
            index_url=index_url,
            finalize=False,
        )
        batch_timing["recoveryWave1AnalysisMs"] = _duration_ms(recovery_wave1_analysis_started)

        secondary_jobs_to_fetch = [
            job
            for job in secondary_recovery_jobs
            if str(_as_dict(job.get("payload")).get("homepageUrl") or "").strip()
            not in recovered_homepages_1
        ]
        recovery_wave2_fetch_started = time.perf_counter()
        recovery_wave2_results, wave2_unique_jobs, wave2_network_jobs = _fetch_recovery_jobs(
            timeout_s=recovery_timeout_s,
            jobs=secondary_jobs_to_fetch,
            fetcher=fetcher,
            total_concurrency=recovery_fetch_concurrency,
            per_host_concurrency=recovery_per_host_concurrency,
            progress_label="GameDevMap active dry run careers recovery fetch wave 2",
            recovery_cache=recovery_cache,
        )
        batch_timing["recoveryWave2FetchMs"] = _duration_ms(recovery_wave2_fetch_started)
        recovery_wave2_analysis_started = time.perf_counter()
        (
            recovery_provider_rows_2,
            recovery_static_rows_2,
            recovery_rejected_rows,
            recovery_failures_2,
            recovery_pages_fetched_2,
            _recovery_groups,
            recovered_homepages_2,
        ) = _apply_recovery_results(
            recovery_fetch_results=recovery_wave2_results,
            index_url=index_url,
            grouped=recovery_groups,
            finalize=True,
        )
        batch_timing["recoveryWave2AnalysisMs"] = _duration_ms(recovery_wave2_analysis_started)
        recovery_provider_rows = [*recovery_provider_rows_1, *recovery_provider_rows_2]
        recovery_static_rows = [*recovery_static_rows_1, *recovery_static_rows_2]
        recovery_failures = [*recovery_failures_1, *recovery_failures_2]
        recovery_pages_fetched = recovery_pages_fetched_1 + recovery_pages_fetched_2
        recovery_jobs = [*primary_recovery_jobs, *secondary_jobs_to_fetch]
        batch_timing["primaryRecoveryJobs"] = len(primary_recovery_jobs)
        batch_timing["secondaryRecoveryJobs"] = len(secondary_jobs_to_fetch)
        batch_timing["recoveryUniqueJobs"] = wave1_unique_jobs + wave2_unique_jobs
        batch_timing["recoveryNetworkJobs"] = wave1_network_jobs + wave2_network_jobs
        batch_timing["recoverySkippedByWave1"] = len(secondary_recovery_jobs) - len(
            secondary_jobs_to_fetch
        )
        batch_timing["recoveryRecoveredHomepages"] = len(
            recovered_homepages_1 | recovered_homepages_2
        )
        merge_started = time.perf_counter()
        all_candidates = unique_sources(
            [
                *direct_provider_rows,
                *provider_rows,
                *static_rows,
                *recovery_provider_rows,
                *recovery_static_rows,
            ]
        )
        all_candidates, bad_provider_rejections = _filter_bad_provider_inferences(all_candidates)
        artifact["allCandidates"] = _merge_unique_rows(
            artifact.get("allCandidates"), all_candidates
        )
        artifact["browserRecoveryCandidates"] = _merge_unique_rows(
            artifact.get("browserRecoveryCandidates"), browser_recovery_rows
        )
        _record_failures(
            artifact,
            [
                dict(result.get("failure"))
                for result in homepage_fetch_results
                if isinstance(result.get("failure"), dict)
            ],
        )
        _record_failures(artifact, recovery_failures)
        artifact["rejectedForActivation"] = [
            *(_as_list(artifact.get("rejectedForActivation"))),
            *rejected_missing,
            *rejected_rows,
            *recovery_rejected_rows,
            *bad_provider_rejections,
        ]
        summary = _as_dict(artifact.get("summary"))
        summary["homepageFetchAttempts"] = _safe_int(summary.get("homepageFetchAttempts")) + len(
            homepage_rows
        )
        summary["homepagesFetched"] = _safe_int(summary.get("homepagesFetched")) + int(
            homepages_fetched
        )
        summary["recoveryFetchAttempts"] = _safe_int(summary.get("recoveryFetchAttempts")) + len(
            recovery_jobs
        )
        summary["recoveryUniqueFetchAttempts"] = _safe_int(
            summary.get("recoveryUniqueFetchAttempts")
        ) + int(wave1_unique_jobs + wave2_unique_jobs)
        summary["recoveryNetworkFetchAttempts"] = _safe_int(
            summary.get("recoveryNetworkFetchAttempts")
        ) + int(wave1_network_jobs + wave2_network_jobs)
        summary["recoveryPagesFetched"] = _safe_int(summary.get("recoveryPagesFetched")) + int(
            recovery_pages_fetched
        )
        artifact["summary"] = summary
        batch_timing["mergeMs"] = _duration_ms(merge_started)

        probe_started = time.perf_counter()
        probe_results = asyncio.run(
            _probe_candidates_async(all_candidates, timeout_s=timeout_s, fetcher=fetcher)
        )
        _apply_probe_results(artifact, probe_results)
        batch_timing["probeMs"] = _duration_ms(probe_started)

        completed_urls.update(_row_url(row) for row in batch_rows if _row_url(row))
        progress = _as_dict(artifact.get("progress"))
        progress["batchesCompleted"] = _safe_int(progress.get("batchesCompleted")) + 1
        artifact["progress"] = progress
        batches_run += 1
        batch_timing["totalMs"] = _duration_ms(batch_started)
        batch_timing["artifactWriteMs"] = 0
        _append_batch_timing(artifact, batch_timing)
        apply_gamedevmap_lost_recovery_audit(
            artifact,
            compare_artifact_path=compare_artifact_path,
        )
        _write_artifact(
            artifact,
            output_path,
            parsed_rows=parsed_rows,
            representative_rows=representative_rows,
            completed_urls=completed_urls,
            complete=len(completed_urls) >= len(representative_rows),
        )
        if max_batches and batches_run >= max_batches:
            break

    emit_log(f"GameDevMap active-source dry run written to {output_path}.")
    return artifact


def _active_audit_ttl_minutes(cfg: dict[str, Any]) -> int:
    raw = cfg.get("activeAuditTtlMinutes", cfg.get("cacheTtlMinutes", 360))
    try:
        return max(0, int(raw))
    except (TypeError, ValueError):
        return 360


def _audit_artifact_is_fresh(artifact: dict[str, Any], cfg: dict[str, Any]) -> bool:
    return audit_ledger.artifact_is_fresh(
        artifact,
        schema_version=DRY_RUN_SCHEMA_VERSION,
        expected_signature=_gamedevmap_cache_signature(cfg),
        ttl_minutes=_active_audit_ttl_minutes(cfg),
    )


def _audit_artifact_signature_matches(artifact: dict[str, Any], cfg: dict[str, Any]) -> bool:
    return audit_ledger.artifact_signature_matches(
        artifact,
        schema_version=DRY_RUN_SCHEMA_VERSION,
        expected_signature=_gamedevmap_cache_signature(cfg),
    )


def run_gamedevmap_source_audit(
    *,
    timeout_s: int,
    config: dict[str, Any] | None = None,
    fetcher=fetch_text,
    output_path: Path | None = None,
    run_id: str = "",
    started_at: str = "",
    reset: bool = False,
    max_batches: int | None = None,
    rerun_reasons: str | list[str] | tuple[str, ...] | None = None,
) -> tuple[dict[str, Any], bool]:
    cfg = dict(
        _gamedevmap_config_value(config, "gamedevmap", DEFAULT_DISCOVERY_CONFIG["gamedevmap"])
    )
    cfg["enabled"] = True
    output_path = output_path or gamedevmap_active_dry_run_path()
    parsed_rerun_reasons = _parse_rerun_reasons(rerun_reasons)
    if not reset and not parsed_rerun_reasons:
        existing = source_registry_module.load_json_object(output_path, {})
        existing_artifact = dict(existing) if isinstance(existing, dict) else {}
        if existing_artifact and not _audit_artifact_signature_matches(existing_artifact, cfg):
            reset = True
        fresh = existing_artifact if _audit_artifact_is_fresh(existing_artifact, cfg) else None
        if fresh is not None:
            emit_log(f"GameDevMap active-source audit cache hit: {output_path}.")
            return fresh, True
    batch_size = max(1, int(cfg.get("activeAuditBatchSize") or 1000))
    configured_max_batches = max(0, int(cfg.get("activeAuditMaxBatchesPerDiscoveryRun") or 0))
    effective_max_batches = configured_max_batches if max_batches is None else max(0, max_batches)
    return (
        run_gamedevmap_active_source_dry_run(
            timeout_s=timeout_s,
            config=config,
            fetcher=fetcher,
            output_path=output_path,
            run_id=run_id,
            started_at=started_at,
            batch_size=batch_size,
            reset=reset,
            max_batches=effective_max_batches,
            rerun_reasons=rerun_reasons,
        ),
        False,
    )


def gamedevmap_validated_candidates_from_artifact(
    artifact: dict[str, Any],
    *,
    promote_validated_static: bool = True,
    validated_static_queue_cap: int = 0,
    validated_static_domain_cap: int = 0,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    provider_candidates: list[dict[str, Any]] = []
    static_candidates: list[dict[str, Any]] = []
    for item in _as_list(artifact.get("activeCandidates")):
        if not isinstance(item, dict):
            continue
        row = dict(item)
        if str(row.get("probeStatus") or "").strip().lower() != "ok":
            continue
        if _safe_int(row.get("jobsFound") or row.get("sampleCount")) <= 0:
            continue
        if not _candidate_id(row):
            continue
        row["prevalidatedDiscovery"] = True
        row["gamedevmapAuditValidated"] = True
        row["sourceDirectory"] = str(row.get("sourceDirectory") or "gamedevmap")
        adapter = str(row.get("adapter") or "").strip().lower()
        if adapter == "static":
            static_row = _validated_static_audit_candidate(
                row,
                promote_validated_static=promote_validated_static,
                validated_static_queue_cap=validated_static_queue_cap,
                validated_static_domain_cap=validated_static_domain_cap,
            )
            if static_row is not None:
                static_candidates.append(static_row)
        elif adapter:
            provider_candidates.append(row)
    return unique_sources(provider_candidates), unique_sources(static_candidates)


def discover_gamedevmap_audit_candidates(
    timeout_s: int,
    *,
    config: dict[str, Any] | None = None,
    fetcher=fetch_text,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    cfg = dict(
        _gamedevmap_config_value(config, "gamedevmap", DEFAULT_DISCOVERY_CONFIG["gamedevmap"])
    )
    if not bool(cfg.get("enabled")):
        emit_log("GameDevMap directory disabled, skipping.")
        return [], [], []
    artifact, cache_hit = run_gamedevmap_source_audit(
        timeout_s=timeout_s,
        config=config,
        fetcher=fetcher,
    )
    provider_candidates, static_candidates = gamedevmap_validated_candidates_from_artifact(
        artifact,
        promote_validated_static=bool(cfg.get("promoteValidatedStatic", True)),
        validated_static_queue_cap=max(0, int(cfg.get("validatedStaticQueueCap") or 0)),
        validated_static_domain_cap=max(0, int(cfg.get("validatedStaticDomainCap") or 0)),
    )
    global LAST_GAMEDEVMAP_AUDIT_REPORT_SUMMARY
    LAST_GAMEDEVMAP_AUDIT_REPORT_SUMMARY = gamedevmap_audit_report_summary(
        artifact,
        cache_hit=cache_hit,
    )
    emit_log(
        "GameDevMap audit candidates: "
        f"provider={len(provider_candidates)}, static={len(static_candidates)}, "
        f"cache={'hit' if cache_hit else 'refresh'}."
    )
    return (
        provider_candidates,
        static_candidates,
        [dict(row) for row in _as_list(artifact.get("failures")) if isinstance(row, dict)],
    )
