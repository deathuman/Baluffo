from __future__ import annotations

"""Resumable GameDevMap active-source dry-run reporting."""

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from src import source_registry as source_registry_module
from src.shared.utils import now_iso
from src.source_registry import unique_sources

from . import active_audit_runtime, audit_ledger, audit_report_summary, recovery_url_planner
from . import browser_recovery as browser_recovery_helpers
from . import directory_page_recovery as directory_recovery_helpers
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
from .gamedevmap_rejection import (
    _error_text,
    _gamedevmap_probe_failed_rejection,
    _gamedevmap_zero_jobs_rejection,
    _normalize_failure_bucket,
    _rejection,
    _row_url,
)
from .gamedevmap_rerun import (
    _parse_rerun_reasons,
    _prune_rerun_rejections,
    _select_rerun_rows,
)
from .io_runtime import endpoint_url
from .page_analysis import analyze_fetched_page
from .page_diagnostics import (
    no_candidate_reason_detail as shared_no_candidate_reason_detail,
)
from .page_outcomes import (
    FetchedPageContext,
    PageOutcome,
    PageOutcomeStrategy,
    classify_fetched_page_with_strategy,
    static_page_outcome_builders,
)
from .prevalidated_queue_policy import apply_prevalidated_queue_overrides
from .probe_runtime import (
    candidate_id as probe_candidate_id,
)
from .probe_runtime import (
    classify_probe_results,
    rendered_static_probe_result,
)
from .probe_runtime import (
    probe_candidates_async as shared_probe_candidates_async,
)
from .provider_inference_filters import split_bad_provider_inferences
from .reporting import emit_log
from .static_candidates import build_known_careers_url_candidate
from .web_search import (
    extract_jobish_links,
    fetch_text,
    infer_provider_candidates_from_html,
    infer_web_candidate,
)

DRY_RUN_SCHEMA_VERSION = 3
LAST_GAMEDEVMAP_AUDIT_REPORT_SUMMARY: dict[str, Any] = {}
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


def gamedevmap_active_dry_run_path() -> Path:
    return source_registry_module.ACTIVE_PATH.parent / "gamedevmap-active-source-dry-run.json"


def _as_list(value: Any) -> list[Any]:
    return list(active_audit_runtime._as_list(value))


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(active_audit_runtime._as_dict(value))


def _safe_int(value: Any, default: int = 0) -> int:
    if int(default) == 0:
        return active_audit_runtime._safe_int(value)
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


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
    return active_audit_runtime.create_active_audit_artifact(
        schema_version=DRY_RUN_SCHEMA_VERSION,
        run_id=run_id,
        started_at=started_at,
        mode="gamedevmap_active_source_dry_run",
        progress={
            "complete": False,
            "cursorPosition": 0,
            "batchSize": int(batch_size),
            "batchesCompleted": 0,
            "completedUrlsCount": 0,
        },
        runtime={
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
        list_keys=[
            "failureSamples",
            "completedUrls",
            "activeCandidates",
            "zeroJobCandidates",
            "rejectedForActivation",
            "browserRecoveryCandidates",
            "failures",
            "allCandidates",
        ],
        dict_keys=["failureCounts", "failureErrorCounts"],
    )


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
    return active_audit_runtime.load_or_initialize_active_audit_artifact(
        output_path,
        reset=reset,
        schema_version=DRY_RUN_SCHEMA_VERSION,
        initial_artifact=_initial_artifact(
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
        ),
        runtime_updates={
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
        progress_updates={
            "batchSize": int(batch_size),
        },
        list_keys=[
            "completedUrls",
            "activeCandidates",
            "zeroJobCandidates",
            "rejectedForActivation",
            "browserRecoveryCandidates",
            "failures",
            "allCandidates",
        ],
        dict_keys=["failureCounts", "failureErrorCounts"],
        failure_sample_limit=FAILURE_SAMPLE_LIMIT,
        load_json_object=source_registry_module.load_json_object,
    )


def _summarize_artifact(
    artifact: dict[str, Any],
    *,
    parsed_rows: list[dict[str, str]],
    representative_rows: list[dict[str, Any]],
    completed_urls: set[str],
) -> None:
    prior_summary = _as_dict(artifact.get("summary"))
    counts = active_audit_runtime.active_audit_artifact_counts(
        artifact,
        all_candidates_key="allCandidates",
        active_candidates_key="activeCandidates",
        zero_candidates_key="zeroJobCandidates",
        rejected_key="rejectedForActivation",
        browser_candidates_key="browserRecoveryCandidates",
        recovered_predicate=lambda row: bool(row.get("gamedevmapRecovery")),
        failure_bucket_fn=lambda row: str(
            row.get("failureBucket") or _normalize_failure_bucket(row.get("reason", ""))
        ),
    )
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
            [row for row in counts.all_candidates if str(row.get("adapter") or "") != "static"]
        ),
        "staticCandidates": len(
            [row for row in counts.all_candidates if str(row.get("adapter") or "") == "static"]
        ),
        "recoveredCandidates": len(counts.recovered_candidates),
        "recoveredActiveCandidates": len(counts.recovered_active),
        "probedCandidates": len(counts.all_candidates),
        "activeCandidates": len(counts.active_rows),
        "zeroJobCandidates": counts.zero_job_count,
        "probeFailures": int(counts.reason_counts.get("probe_failed") or 0),
        "technicalFailures": len(counts.technical_failures),
        "coverageMisses": len(counts.coverage_misses),
        "failures": counts.failure_count,
        "failureSampleCount": counts.failure_sample_count,
        "artifactSizeBytes": _safe_int(_as_dict(artifact.get("runtime")).get("artifactSizeBytes")),
        "rejectedForActivation": len(counts.rejected_rows),
        "rejectedReasonCounts": counts.reason_counts,
        "rejectedReasonDetailCounts": counts.detail_counts,
        "activeAdapterCounts": counts.active_adapter_counts,
        "browserRecoveryCandidates": counts.browser_recovery_candidate_count,
        "browserRecoveryProcessed": counts.browser_recovery_processed_count,
        "browserRecoveredActiveCandidates": counts.browser_recovered_active_count,
        "lostRecoveredActiveCandidates": counts.lost_recovered_active_count,
    }


def _recovered_active_by_id(artifact: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return active_audit_runtime.recovered_active_by_identity(
        artifact,
        active_key="activeCandidates",
        recovered_predicate=lambda row: bool(row.get("gamedevmapRecovery")),
        identity_fn=probe_candidate_id,
    )


def _index_current_rejections(artifact: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    return active_audit_runtime.index_rejections_by_identity(
        artifact,
        rejected_key="rejectedForActivation",
        lookup_keys_fn=lambda rejection: active_audit_runtime.rejection_lookup_keys(
            rejection,
            candidate_identity_fn=probe_candidate_id,
            candidate_url_key_fn=_candidate_url_key,
        ),
    )


def _classify_lost_recovery(
    previous_candidate: dict[str, Any],
    current_rejections: dict[str, list[dict[str, Any]]],
) -> tuple[str, dict[str, Any]]:
    keys = [
        probe_candidate_id(previous_candidate),
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
    return active_audit_runtime.compare_recovered_active_maps(
        previous=previous,
        current=current,
        current_rejections=current_rejections,
        classify_lost=_classify_lost_recovery,
        lost_row_builder=lambda row_id, cause, previous_candidate, matched_rejection: {
            "sourceId": row_id,
            "cause": cause,
            "name": str(previous_candidate.get("name") or ""),
            "adapter": str(previous_candidate.get("adapter") or ""),
            "jobsFound": _safe_int(previous_candidate.get("jobsFound")),
            "recoverySource": str(previous_candidate.get("gamedevmapRecoverySource") or ""),
            "careersUrl": str(
                previous_candidate.get("careersUrl") or previous_candidate.get("listing_url") or ""
            ),
            "matchedCurrentRejection": matched_rejection,
        },
    )


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
    index_url = str(cfg.get("indexUrl") or GAMEDEVMAP_INDEX_URL)

    def _handle_failure(
        row: dict[str, Any],
        source_url: str,
        error: str,
        _browser_recovery: dict[str, Any],
    ) -> list[dict[str, Any]]:
        return [
            _rejection(
                reason="no_careers_evidence",
                row={"studio": row.get("studio"), "url": source_url},
                error=error,
                reason_detail="browser_recovery_fetch_failed",
                failure_bucket="technical_failure",
            )
        ]

    def _analyze_success(
        row: dict[str, Any],
        source_url: str,
        html: str,
    ) -> browser_recovery_helpers.BrowserRecoveryPageAnalysis:
        provider_candidates: list[dict[str, Any]] = []
        static_candidates: list[dict[str, Any]] = []
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
        _append_analyzed_candidates(
            page_url=source_url,
            html=html,
            row=row_payload,
            index_url=index_url,
            recovery_source="browser_rendered_homepage",
            provider_candidates=provider_candidates,
            static_candidates=static_candidates,
        )
        return browser_recovery_helpers.BrowserRecoveryPageAnalysis(
            all_candidates=[*provider_candidates, *static_candidates],
            rendered_static_candidates=static_candidates,
        )

    def _finalize_candidates(
        candidates: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        return _filter_bad_provider_inferences(unique_sources(candidates))

    analysis = browser_recovery_helpers.analyze_browser_recovery_fetch_results(
        fetch_results=fetch_results,
        browser_recovery=browser_recovery,
        processed=processed,
        analyze_success=_analyze_success,
        handle_fetch_failure=_handle_failure,
        rendered_static_probe_result=lambda candidate, rendered_url, rendered_html: (
            rendered_static_probe_result(
                candidate,
                rendered_url=rendered_url,
                rendered_html=rendered_html,
            )
        ),
        finalize_candidates=_finalize_candidates,
    )
    return (
        analysis.all_candidates,
        list(analysis.rejected_rows or []),
        analysis.rendered_probe_results,
    )


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
    batch: browser_recovery_helpers.BrowserRecoveryBatch,
    combined_probe_results: list[tuple[dict[str, Any], bool, int, str, int]],
) -> None:
    artifact["allCandidates"] = active_audit_runtime.merge_unique_candidate_rows(
        artifact.get("allCandidates"),
        batch.analysis.all_candidates,
        unique_rows=unique_sources,
    )
    active_audit_runtime.append_artifact_rows(
        artifact,
        "rejectedForActivation",
        list(batch.analysis.rejected_rows or []),
    )
    active_audit_runtime.apply_active_audit_probe_results(
        artifact,
        combined_probe_results,
        classify_probe_results=classify_probe_results,
        probe_failed_rejection=_gamedevmap_probe_failed_rejection,
        zero_jobs_rejection=_gamedevmap_zero_jobs_rejection,
        active_key="activeCandidates",
        zero_candidates_key="zeroJobCandidates",
        rejected_key="rejectedForActivation",
        identity_fn=probe_candidate_id,
    )


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
    browser_fetcher = browser_fetcher or browser_recovery_helpers.default_browser_fetcher()
    browser_recovery = _as_dict(artifact.get("browserRecovery"))
    candidates = [dict(row) for row in _as_list(artifact.get("browserRecoveryCandidates"))]
    limit = max(0, int(cfg.get("activeAuditBrowserRecoveryLimit") or 0))
    concurrency = max(1, int(cfg.get("activeAuditBrowserRecoveryConcurrency") or 2))
    browser_timeout_s = max(
        1,
        min(max(1, int(timeout_s)), int(cfg.get("activeAuditBrowserRecoveryTimeoutSeconds") or 15)),
    )
    browser_recovery_helpers.run_browser_recovery_assembly(
        rows=candidates,
        browser_recovery=browser_recovery,
        timeout_s=browser_timeout_s,
        fetcher=fetcher,
        browser_fetcher=browser_fetcher,
        concurrency=concurrency,
        analyze_fetches=_analyze_browser_recovery_batch(cfg),
        merge_artifact_updates=lambda batch, combined_probe_results: (
            _merge_browser_recovery_artifact_updates(
                artifact=artifact,
                batch=batch,
                combined_probe_results=combined_probe_results,
            )
        ),
        recovered_rows=lambda: _as_list(artifact.get("activeCandidates")),
        recovered_predicate=lambda row: bool(row.get("gamedevmapBrowserRecovery")),
        limit=limit,
        probe_timeout_s=timeout_s,
        emit_log=emit_log,
        log_label="GameDevMap browser recovery",
        mark_probe_results=lambda results, rendered_count: _mark_browser_recovery_probe_results(
            results,
            rendered_count=rendered_count,
        ),
    )
    artifact["browserRecovery"] = browser_recovery
    completed_urls = {
        str(item).strip() for item in _as_list(artifact.get("completedUrls")) if str(item).strip()
    }
    active_audit_runtime.save_updated_active_audit_artifact(
        artifact,
        output_path,
        completed_identities=completed_urls,
        summarize=lambda current, identities: _summarize_artifact(
            current,
            parsed_rows=[],
            representative_rows=[],
            completed_urls=identities,
        ),
    )
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


def _no_careers_reason_detail(page_url: str, html: str) -> str:
    return shared_no_candidate_reason_detail(
        page_url,
        html,
        social_profile_hosts=SOCIAL_PROFILE_HOSTS,
        third_party_profile_hosts=THIRD_PARTY_PROFILE_HOSTS,
        jobish_url_fn=lambda url, body: extract_jobish_links(body, url),
        include_noscript_script_shell=True,
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

    return classify_fetched_page_with_strategy(
        context,
        PageOutcomeStrategy(
            provider_rows=_provider_rows,
            explicit_static=_explicit_static,
            generic_static=_generic_static,
            analyze_page=analyze_fetched_page,
        ),
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
    if len(html) > 1_000_000:
        html = f"{html[:500_000]}\n{html[-500_000:]}"
    outcome = _gamedevmap_page_outcome(
        page_url=page_url,
        html=html,
        row=row,
        index_url=index_url,
        recovery_source=recovery_source,
    )
    provider_candidates.extend(outcome.provider_candidates)
    static_candidates.extend(outcome.static_candidates)
    found_candidates = outcome.found_candidates
    if (
        recovery_source == "browser_rendered_homepage"
        and not found_candidates
        and _rendered_page_has_static_job_evidence(page_url, html)
    ):
        static_candidates.append(
            _apply_gamedevmap_provenance(
                build_known_careers_url_candidate(
                    page_url,
                    studio=str(row.get("studio") or "").strip(),
                    name_suffix="GameDevMap",
                    nl_priority=False,
                    discovery_method="gamedevmap",
                    evidence_source="gamedevmap",
                    evidence_types=[
                        "gamedevmap_careers_url",
                        "gamedevmap_browser_rendered_job_links",
                    ],
                    evidence_score=44,
                    enabled_by_default=False,
                    weak_signal=False,
                ),
                row,
                index_url=index_url,
                include_homepage_fetch=True,
            )
        )
        found_candidates = True
    return found_candidates


def _rendered_page_has_static_job_evidence(page_url: str, html: str) -> bool:
    try:
        from .probe import static_probe_evidence

        return int(static_probe_evidence(html, page_url).count or 0) > 0
    except (TypeError, ValueError):
        return False


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
            browser_recovery_helpers.browser_recovery_candidate_row(
                adapter="gamedevmap",
                name=f"{studio} browser recovery",
                studio=studio,
                url=target_url,
                source_directory_entry_url=str(row.get("sourceDirectoryEntryUrl") or "").strip(),
                reason_detail=detail,
            )
        )
    row_provider_candidates = _provider_candidates_from_html_text(
        row=row,
        page_url=target_url,
        html=html,
        index_url=index_url,
    )
    provider_candidates.extend(row_provider_candidates)
    primary_jobs, secondary_jobs = directory_recovery_helpers.plan_recovery_fetch_job_waves(
        page_url=target_url,
        html=html,
        primary_paths=PRIMARY_RECOVERY_PATHS,
        secondary_paths=SECONDARY_RECOVERY_PATHS,
        payload_factory=lambda _url, wave: {
            "row": row,
            "homepageUrl": target_url,
            "homepageReasonDetail": detail,
            "recoverySource": "same_party_recovery_url",
            "recoveryWave": int(wave),
        },
        name_factory=lambda recovery_url, _wave: f"{studio} recovery {recovery_url}",
        adapter="gamedevmap",
        failure_stage="gamedevmap_recovery_fetch",
        blocked_hosts=SOCIAL_PROFILE_HOSTS | THIRD_PARTY_PROFILE_HOSTS,
        html_url_candidate_fn=recovery_url_planner.html_url_candidates,
    )
    primary_recovery_jobs.extend(primary_jobs)
    secondary_recovery_jobs.extend(secondary_jobs)
    return bool(primary_jobs or secondary_jobs or row_provider_candidates)


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
    def _infer_direct_provider(row: dict[str, Any]) -> dict[str, Any] | None:
        studio = str(row.get("studio") or "").strip()
        inferred = infer_web_candidate(
            _row_url(row),
            studio,
            nl_priority=False,
            discovery_method="gamedevmap",
        )
        if inferred:
            inferred["careersUrl"] = _row_url(row)
            return _apply_gamedevmap_provenance(
                inferred,
                row,
                index_url=index_url,
                include_direct_url=True,
            )
        return None

    def _fetch_failure_rejection(row: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
        return _rejection(
            reason="homepage_fetch_failed",
            row=row,
            error=_error_text(result),
            reason_detail="homepage_fetch_failed",
        )

    def _analyze_homepage(
        row: dict[str, Any], target_url: str, html: str
    ) -> active_audit_runtime.HomepagePageOutcome:
        outcome = _gamedevmap_page_outcome(
            page_url=target_url,
            html=html,
            row=row,
            index_url=index_url,
        )
        return active_audit_runtime.HomepagePageOutcome(
            provider_candidates=outcome.provider_candidates,
            static_candidates=outcome.static_candidates,
            found_candidates=outcome.found_candidates,
        )

    def _handle_no_candidate(
        row: dict[str, Any], target_url: str, html: str
    ) -> active_audit_runtime.NoCandidateOutcome:
        detail = _no_careers_reason_detail(target_url, html)
        provider_candidates: list[dict[str, Any]] = []
        primary_recovery_jobs: list[dict[str, Any]] = []
        secondary_recovery_jobs: list[dict[str, Any]] = []
        browser_recovery_candidates: list[dict[str, Any]] = []
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
        rejected_rows: list[dict[str, Any]] = []
        if not queued:
            rejected_rows.append(
                _rejection(
                    reason="no_careers_evidence",
                    row=row,
                    reason_detail=detail,
                )
            )
        return active_audit_runtime.NoCandidateOutcome(
            provider_candidates=provider_candidates,
            rejected_rows=rejected_rows,
            primary_recovery_jobs=primary_recovery_jobs,
            secondary_recovery_jobs=secondary_recovery_jobs,
            browser_recovery_candidates=browser_recovery_candidates,
        )

    result = active_audit_runtime.run_active_homepage_batch(
        batch_rows=batch_rows,
        homepage_fetch_results=homepage_fetch_results,
        row_url=_row_url,
        infer_direct_provider=_infer_direct_provider,
        fetch_failure_rejection=_fetch_failure_rejection,
        analyze_homepage=_analyze_homepage,
        handle_no_candidate=_handle_no_candidate,
    )

    return (
        result.provider_candidates,
        result.static_candidates,
        result.rejected_rows,
        result.primary_recovery_jobs,
        result.secondary_recovery_jobs,
        result.browser_recovery_candidates,
        result.homepages_fetched,
    )


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
    progress_label: str = "",
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    int,
    dict[str, dict[str, Any]],
    set[str],
]:
    def apply_payload(
        payload: dict[str, Any],
        result: dict[str, Any],
        grouped_rows: dict[str, dict[str, Any]],
        provider_candidates: list[dict[str, Any]],
        static_candidates: list[dict[str, Any]],
    ) -> str:
        return _apply_recovery_payload_to_group(
            payload=payload,
            result=result,
            grouped=grouped_rows,
            index_url=index_url,
            provider_candidates=provider_candidates,
            static_candidates=static_candidates,
        )

    def finalize_group(group: dict[str, Any]) -> list[dict[str, Any]]:
        if _safe_int(group.get("candidates")) > 0:
            return []
        detail = "recovery_pages_no_jobs"
        if _safe_int(group.get("fetched")) == 0 and _safe_int(group.get("failures")) > 0:
            detail = "recovery_fetch_failed"
        return [
            _rejection(
                reason="no_careers_evidence",
                row=_as_dict(group.get("row")),
                reason_detail=detail,
            )
        ]

    output = active_audit_runtime.apply_active_audit_recovery_fetch_results(
        recovery_fetch_results,
        grouped=grouped,
        finalize=finalize,
        apply_payload=apply_payload,
        finalize_group=finalize_group,
        progress_label=progress_label,
        progress_callback=progress_callback,
    )

    return (
        output.provider_candidates,
        output.static_candidates,
        output.rejected_rows,
        output.failures,
        output.pages_fetched,
        output.grouped_state,
        output.recovered_homepages,
    )


def _filter_bad_provider_inferences(
    candidates: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    good, bad = split_bad_provider_inferences(candidates)
    return (
        good,
        [
            _rejection(
                reason="bad_provider_inference",
                candidate=candidate,
                reason_detail=str(candidate.get("reasonDetail") or ""),
            )
            for candidate in bad
        ],
    )


async def _probe_candidates_async(
    candidates: list[dict[str, Any]],
    *,
    timeout_s: int,
    fetcher,
) -> list[tuple[dict[str, Any], bool, int, str, int]]:
    return await shared_probe_candidates_async(candidates, timeout_s=timeout_s, fetcher=fetcher)


def _prepare_gamedevmap_active_batch_rows(
    rows: list[dict[str, Any]],
    *,
    index_url: str,
) -> active_audit_runtime.ActiveAuditPreparedRows:
    direct_provider_rows: list[dict[str, Any]] = []
    homepage_rows: list[dict[str, Any]] = []
    rejected_missing: list[dict[str, Any]] = []
    for row in rows:
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
    return active_audit_runtime.ActiveAuditPreparedRows(
        direct_provider_candidates=direct_provider_rows,
        homepage_rows=homepage_rows,
        rejected_rows=rejected_missing,
    )


def _fetch_gamedevmap_active_homepages(
    rows: list[dict[str, Any]],
    *,
    timeout_s: int,
    fetcher,
    total_concurrency: int,
    per_host_concurrency: int,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> list[dict[str, Any]]:
    return fetch_directory_pages(
        timeout_s,
        [
            {
                "url": _row_url(row),
                "payload": row,
                "name": _row_url(row),
                "adapter": "gamedevmap",
                "failureStage": "homepage_fetch",
            }
            for row in rows
        ],
        fetcher=fetcher,
        total_concurrency=total_concurrency,
        per_host_concurrency=per_host_concurrency,
        progress_label="GameDevMap active dry run homepage fetch",
        emit_progress_log=False,
        progress_callback=(
            (
                lambda progress: progress_callback(
                    {
                        "phase": "homepage_fetch",
                        "phaseLabel": "Fetching studio homepages",
                        "phaseCompleted": progress.get("completed"),
                        "phaseTotal": progress.get("total"),
                    }
                )
            )
            if progress_callback is not None
            else None
        ),
    )


def _analyze_gamedevmap_active_homepages(
    homepage_fetch_results: list[dict[str, Any]],
    *,
    index_url: str,
) -> active_audit_runtime.ActiveHomepageBatchResult:
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
    return active_audit_runtime.ActiveHomepageBatchResult(
        provider_candidates=provider_rows,
        static_candidates=static_rows,
        rejected_rows=rejected_rows,
        primary_recovery_jobs=primary_recovery_jobs,
        secondary_recovery_jobs=secondary_recovery_jobs,
        browser_recovery_candidates=browser_recovery_rows,
        homepages_fetched=homepages_fetched,
    )


def _fetch_gamedevmap_active_recovery(
    jobs: list[dict[str, Any]],
    progress_label: str,
    *,
    timeout_s: int,
    fetcher,
    total_concurrency: int,
    per_host_concurrency: int,
    recovery_cache: dict[str, dict[str, Any]],
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> active_audit_runtime.ActiveAuditRecoveryFetchResult:
    phase = "recovery_wave2_fetch" if "wave 2" in progress_label.lower() else "recovery_wave1_fetch"
    results, unique_jobs, network_jobs = directory_recovery_helpers.fetch_recovery_jobs(
        timeout_s,
        jobs,
        fetcher=fetcher,
        total_concurrency=total_concurrency,
        per_host_concurrency=per_host_concurrency,
        progress_label=progress_label,
        recovery_cache=recovery_cache,
        fetch_pages=fetch_directory_pages,
        progress_callback=(
            (
                lambda progress: progress_callback(
                    {
                        "phase": phase,
                        "phaseLabel": progress_label,
                        "phaseCompleted": progress.get("completed"),
                        "phaseTotal": progress.get("total"),
                    }
                )
            )
            if progress_callback is not None
            else None
        ),
    )
    return active_audit_runtime.ActiveAuditRecoveryFetchResult(
        results=results,
        unique_jobs=unique_jobs,
        network_jobs=network_jobs,
    )


def _apply_gamedevmap_active_recovery(
    recovery_fetch_results: list[dict[str, Any]],
    grouped: dict[str, Any] | None,
    finalize: bool,
    progress_label: str,
    *,
    index_url: str,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> active_audit_runtime.ActiveAuditRecoveryApplicationResult:
    phase = (
        "recovery_wave2_analysis"
        if "wave 2" in progress_label.lower()
        else "recovery_wave1_analysis"
    )
    (
        recovery_provider_rows,
        recovery_static_rows,
        recovery_rejected_rows,
        recovery_failures,
        recovery_pages_fetched,
        recovery_groups,
        recovered_homepages,
    ) = _apply_recovery_results(
        recovery_fetch_results=recovery_fetch_results,
        index_url=index_url,
        grouped=grouped,
        finalize=finalize,
        progress_label=progress_label,
        progress_callback=(
            (
                lambda progress: progress_callback(
                    {
                        "phase": phase,
                        "phaseLabel": progress_label,
                        "phaseCompleted": progress.get("completed"),
                        "phaseTotal": progress.get("total"),
                        "recoveryPayloads": progress.get("payloads"),
                    }
                )
            )
            if progress_callback is not None
            else None
        ),
    )
    return active_audit_runtime.ActiveAuditRecoveryApplicationResult(
        provider_candidates=recovery_provider_rows,
        static_candidates=recovery_static_rows,
        rejected_rows=recovery_rejected_rows,
        failures=recovery_failures,
        pages_fetched=recovery_pages_fetched,
        grouped_state=recovery_groups,
        recovered_homepages=recovered_homepages,
    )


def _merge_gamedevmap_active_batch_candidates(
    direct_provider_rows: list[dict[str, Any]],
    provider_rows: list[dict[str, Any]],
    static_rows: list[dict[str, Any]],
    recovery_provider_rows: list[dict[str, Any]],
    recovery_static_rows: list[dict[str, Any]],
) -> active_audit_runtime.ActiveAuditCandidateMergeResult:
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
    return active_audit_runtime.ActiveAuditCandidateMergeResult(
        candidates=all_candidates,
        rejected_rows=bad_provider_rejections,
    )


def _build_gamedevmap_active_batch_strategy(
    *,
    artifact: dict[str, Any],
    index_url: str,
    timeout_s: int,
    fetcher,
    homepage_fetch_concurrency: int,
    per_host_concurrency: int,
    recovery_timeout_s: int,
    recovery_fetch_concurrency: int,
    recovery_per_host_concurrency: int,
    recovery_cache: dict[str, dict[str, Any]],
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> active_audit_runtime.ActiveAuditBatchStrategy:
    return active_audit_runtime.build_active_audit_batch_strategy(
        prepare_rows=lambda rows: _prepare_gamedevmap_active_batch_rows(
            rows,
            index_url=index_url,
        ),
        fetch_homepages=lambda rows: _fetch_gamedevmap_active_homepages(
            rows,
            timeout_s=timeout_s,
            fetcher=fetcher,
            total_concurrency=homepage_fetch_concurrency,
            per_host_concurrency=per_host_concurrency,
            progress_callback=progress_callback,
        ),
        analyze_homepages=lambda results: _analyze_gamedevmap_active_homepages(
            results,
            index_url=index_url,
        ),
        fetch_recovery=lambda jobs, label: _fetch_gamedevmap_active_recovery(
            jobs,
            label,
            timeout_s=recovery_timeout_s,
            fetcher=fetcher,
            total_concurrency=recovery_fetch_concurrency,
            per_host_concurrency=recovery_per_host_concurrency,
            recovery_cache=recovery_cache,
            progress_callback=progress_callback,
        ),
        apply_recovery=lambda results, grouped, finalize, label: _apply_gamedevmap_active_recovery(
            results,
            grouped,
            finalize,
            label,
            index_url=index_url,
            progress_callback=progress_callback,
        ),
        recovery_homepage_key=lambda job: str(
            _as_dict(job.get("payload")).get("homepageUrl") or ""
        ).strip(),
        merge_candidates=_merge_gamedevmap_active_batch_candidates,
        merge_artifact_updates=lambda all_candidates, browser_recovery_rows, homepage_failures, recovery_failures, rejected_rows: (
            active_audit_runtime.merge_active_audit_batch_artifact_updates(
                artifact,
                all_candidates=all_candidates,
                browser_recovery_rows=browser_recovery_rows,
                homepage_failures=homepage_failures,
                recovery_failures=recovery_failures,
                rejected_rows=rejected_rows,
                all_candidates_key="allCandidates",
                browser_candidates_key="browserRecoveryCandidates",
                rejected_key="rejectedForActivation",
                unique_rows=unique_sources,
                failure_sample_limit=FAILURE_SAMPLE_LIMIT,
            )
        ),
        update_summary=lambda batch_counts: active_audit_runtime.increment_active_audit_summary(
            artifact,
            batch_counts,
        ),
        probe_candidates=lambda all_candidates: asyncio.run(
            _probe_candidates_async(all_candidates, timeout_s=timeout_s, fetcher=fetcher)
        ),
        apply_probe_results=lambda probe_results: (
            active_audit_runtime.apply_active_audit_probe_results(
                artifact,
                probe_results,
                classify_probe_results=classify_probe_results,
                probe_failed_rejection=_gamedevmap_probe_failed_rejection,
                zero_jobs_rejection=_gamedevmap_zero_jobs_rejection,
                active_key="activeCandidates",
                zero_candidates_key="zeroJobCandidates",
                rejected_key="rejectedForActivation",
                identity_fn=probe_candidate_id,
            )
        ),
        row_identity=_row_url,
        append_timing=lambda batch_timing: active_audit_runtime.append_batch_timing(
            artifact,
            batch_timing,
        ),
        progress_callback=progress_callback,
    )


def _build_gamedevmap_active_loop_strategy(
    *,
    artifact: dict[str, Any],
    output_path: Path,
    parsed_rows: list[dict[str, Any]],
    representative_rows: list[dict[str, Any]],
    completed_urls: set[str],
    compare_artifact_path: Path | str | None,
    batch_strategy: active_audit_runtime.ActiveAuditBatchStrategy,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> active_audit_runtime.ActiveAuditLoopStrategy:
    return active_audit_runtime.build_active_audit_loop_strategy(
        artifact=artifact,
        row_identity=_row_url,
        batch_strategy=batch_strategy,
        completed_identities=completed_urls,
        emit_batch_log=lambda batch_number, row_count, cursor: emit_log(
            "GameDevMap active-source dry run: "
            f"batch={batch_number}, rows={row_count}, cursor={cursor}."
        ),
        before_write=lambda: apply_gamedevmap_lost_recovery_audit(
            artifact,
            compare_artifact_path=compare_artifact_path,
        ),
        write_artifact=lambda complete: active_audit_runtime.finalize_active_audit_artifact(
            artifact,
            output_path,
            completed_identities=completed_urls,
            complete=complete,
            completed_cursor_position=len(representative_rows),
            completed_key="completedUrls",
            summarize=lambda current, identities: _summarize_artifact(
                current,
                parsed_rows=parsed_rows,
                representative_rows=representative_rows,
                completed_urls=identities,
            ),
        ),
        progress_callback=progress_callback,
    )


def _build_gamedevmap_subtask_progress_callback(
    *,
    artifact: dict[str, Any],
    representative_rows: list[dict[str, Any]],
    completed_urls: set[str],
    batch_size: int,
    progress_callback: Callable[[dict[str, Any]], None] | None,
) -> Callable[[dict[str, Any]], None] | None:
    if progress_callback is None:
        return None

    total_urls = len(representative_rows)

    def _callback(event: dict[str, Any]) -> None:
        progress = _as_dict(artifact.get("progress"))
        summary = _as_dict(artifact.get("summary"))
        phase = str(event.get("phase") or "audit").strip()
        phase_label = str(event.get("phaseLabel") or "GameDevMap active audit").strip()
        completed = _safe_int(event.get("completed"), len(completed_urls))
        if completed <= 0:
            completed = _safe_int(progress.get("completedUrlsCount"), len(completed_urls))
        total = _safe_int(event.get("total"), total_urls) or total_urls
        phase_completed = _safe_int(event.get("phaseCompleted"), 0)
        phase_total = _safe_int(event.get("phaseTotal"), 0)
        counts = {
            "subtaskKey": "gamedevmap_active_audit",
            "subtaskLabel": "GameDevMap active audit",
            "activeAuditPhase": phase,
            "activeAuditCompletedUrls": completed,
            "activeAuditTotalUrls": total,
            "activeAuditBatch": _safe_int(event.get("batch"), 0),
            "activeAuditBatchSize": int(batch_size),
            "activeAuditBatchRows": _safe_int(event.get("batchRows"), 0),
            "activeAuditCursor": _safe_int(event.get("cursor"), 0),
            "activeAuditPhaseCompleted": phase_completed,
            "activeAuditPhaseTotal": phase_total,
            "activeAuditHomepageFetched": _safe_int(summary.get("homepagesFetched"), 0),
            "activeAuditRecoveryFetched": _safe_int(summary.get("recoveryNetworkFetchAttempts"), 0),
            "activeAuditRecoveryAnalyzed": _safe_int(summary.get("recoveryPagesFetched"), 0),
            "activeAuditCandidates": len(_as_list(artifact.get("allCandidates"))),
            "activeAuditFailures": len(_as_list(artifact.get("failures"))),
        }
        if "recoveryPayloads" in event:
            counts["activeAuditRecoveryPayloads"] = _safe_int(event.get("recoveryPayloads"), 0)
        progress_callback(
            {
                "phaseKey": "scanning_sources",
                "phaseLabel": "Scanning GameDevMap directory",
                "targetLabel": phase_label,
                "counts": counts,
                "force": bool(event.get("force")),
            }
        )

    return _callback


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
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    started = str(started_at or now_iso())
    cfg = dict(
        _gamedevmap_config_value(config, "gamedevmap", DEFAULT_DISCOVERY_CONFIG["gamedevmap"])
    )
    cfg["enabled"] = True
    csv_url = str(cfg.get("csvUrl") or GAMEDEVMAP_CSV_URL).strip() or GAMEDEVMAP_CSV_URL
    index_url = str(cfg.get("indexUrl") or GAMEDEVMAP_INDEX_URL).strip() or GAMEDEVMAP_INDEX_URL
    batch_size = max(1, int(batch_size or 1000))
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

    recovery_cache: dict[str, dict[str, Any]] = {}
    subtask_progress_callback = _build_gamedevmap_subtask_progress_callback(
        artifact=artifact,
        representative_rows=representative_rows,
        completed_urls=completed_urls,
        batch_size=batch_size,
        progress_callback=progress_callback,
    )
    if subtask_progress_callback is not None:
        subtask_progress_callback(
            {
                "phase": "audit_setup",
                "phaseLabel": "Preparing GameDevMap active audit",
                "completed": len(completed_urls),
                "total": len(representative_rows),
                "force": True,
            }
        )
    batch_strategy = _build_gamedevmap_active_batch_strategy(
        artifact=artifact,
        index_url=index_url,
        timeout_s=timeout_s,
        fetcher=fetcher,
        homepage_fetch_concurrency=homepage_fetch_concurrency,
        per_host_concurrency=per_host_concurrency,
        recovery_timeout_s=recovery_timeout_s,
        recovery_fetch_concurrency=recovery_fetch_concurrency,
        recovery_per_host_concurrency=recovery_per_host_concurrency,
        recovery_cache=recovery_cache,
        progress_callback=subtask_progress_callback,
    )
    loop_strategy = _build_gamedevmap_active_loop_strategy(
        artifact=artifact,
        output_path=output_path,
        parsed_rows=parsed_rows,
        representative_rows=representative_rows,
        completed_urls=completed_urls,
        compare_artifact_path=compare_artifact_path,
        batch_strategy=batch_strategy,
        progress_callback=subtask_progress_callback,
    )

    active_audit_runtime.run_active_audit_loop(
        artifact=artifact,
        source_rows=representative_rows,
        completed_identities=completed_urls,
        batch_size=batch_size,
        max_batches=max_batches,
        strategy=loop_strategy,
    )

    emit_log(f"GameDevMap active-source dry run written to {output_path}.")
    if subtask_progress_callback is not None:
        subtask_progress_callback(
            {
                "phase": "audit_complete",
                "phaseLabel": "Completed GameDevMap active audit",
                "completed": len(completed_urls),
                "total": len(representative_rows),
                "force": True,
            }
        )
    return artifact


def _active_audit_ttl_minutes(cfg: dict[str, Any]) -> int:
    raw = cfg.get("activeAuditTtlMinutes", 360)
    try:
        return max(0, int(raw))
    except (TypeError, ValueError):
        return 360


def _gamedevmap_artifact_signature_matches(
    artifact: dict[str, Any],
    *,
    expected_signature: dict[str, Any],
) -> bool:
    if int(artifact.get("schemaVersion") or 0) != int(DRY_RUN_SCHEMA_VERSION):
        return False
    existing = _as_dict(_as_dict(artifact.get("runtime")).get("configSignature"))
    if existing == expected_signature:
        return True
    existing_without_chunking = dict(existing)
    expected_without_chunking = dict(expected_signature)
    existing_without_chunking.pop("activeAuditBatchSize", None)
    expected_without_chunking.pop("activeAuditBatchSize", None)
    return existing_without_chunking == expected_without_chunking


def _gamedevmap_artifact_is_fresh(
    artifact: dict[str, Any],
    *,
    expected_signature: dict[str, Any],
    ttl_minutes: int,
) -> bool:
    if not bool(_as_dict(artifact.get("progress")).get("complete")):
        return False
    if not _gamedevmap_artifact_signature_matches(
        artifact,
        expected_signature=expected_signature,
    ):
        return False
    if ttl_minutes <= 0:
        return False
    updated_at = audit_ledger.parse_artifact_time(
        artifact.get("updatedAt") or artifact.get("finishedAt")
    )
    return bool(updated_at and datetime.now(UTC) - updated_at <= timedelta(minutes=ttl_minutes))


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
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> tuple[dict[str, Any], bool]:
    cfg = dict(
        _gamedevmap_config_value(config, "gamedevmap", DEFAULT_DISCOVERY_CONFIG["gamedevmap"])
    )
    cfg["enabled"] = True
    output_path = output_path or gamedevmap_active_dry_run_path()
    parsed_rerun_reasons = _parse_rerun_reasons(rerun_reasons)
    batch_size = max(1, int(cfg.get("activeAuditBatchSize") or 1000))
    configured_max_batches = max(0, int(cfg.get("activeAuditMaxBatchesPerDiscoveryRun") or 0))
    effective_max_batches = configured_max_batches if max_batches is None else max(0, max_batches)
    expected_signature = _gamedevmap_cache_signature(cfg)

    def _emit_cache_hit_progress(artifact: dict[str, Any]) -> str:
        if progress_callback is not None:
            summary = _as_dict(artifact.get("summary"))
            completed = _safe_int(summary.get("completedUrls"), 0)
            if completed <= 0:
                completed = len(_as_list(artifact.get("completedUrls")))
            progress_callback(
                {
                    "phaseKey": "scanning_sources",
                    "phaseLabel": "Scanning GameDevMap directory",
                    "targetLabel": "GameDevMap active audit cache hit",
                    "counts": {
                        "subtaskKey": "gamedevmap_active_audit",
                        "subtaskLabel": "GameDevMap active audit",
                        "activeAuditPhase": "cache_hit",
                        "activeAuditCompletedUrls": completed,
                        "activeAuditTotalUrls": completed,
                        "activeAuditBatch": 0,
                        "activeAuditBatchSize": batch_size,
                        "activeAuditCandidates": len(_as_list(artifact.get("allCandidates"))),
                        "activeAuditFailures": len(_as_list(artifact.get("failures"))),
                    },
                    "force": True,
                }
            )
        return f"GameDevMap active-source audit cache hit: {output_path}."

    return active_audit_runtime.run_active_audit_cache(
        reset=reset,
        has_rerun_reasons=bool(parsed_rerun_reasons),
        load_artifact=lambda: source_registry_module.load_json_object(output_path, {}),
        signature_matches=lambda artifact: _gamedevmap_artifact_signature_matches(
            artifact,
            expected_signature=expected_signature,
        ),
        is_fresh=lambda artifact: _gamedevmap_artifact_is_fresh(
            artifact,
            expected_signature=expected_signature,
            ttl_minutes=_active_audit_ttl_minutes(cfg),
        ),
        refresh=lambda effective_reset: run_gamedevmap_active_source_dry_run(
            timeout_s=timeout_s,
            config=config,
            fetcher=fetcher,
            output_path=output_path,
            run_id=run_id,
            started_at=started_at,
            batch_size=batch_size,
            reset=effective_reset,
            max_batches=effective_max_batches,
            rerun_reasons=rerun_reasons,
            progress_callback=progress_callback,
        ),
        cache_hit_log=_emit_cache_hit_progress,
        emit_log_fn=emit_log,
    )


def gamedevmap_validated_candidates_from_artifact(
    artifact: dict[str, Any],
    *,
    promote_validated_static: bool = True,
    validated_static_queue_cap: int = 0,
    validated_static_domain_cap: int = 0,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    return active_audit_runtime.validated_active_candidates_from_artifact(
        artifact,
        active_key="activeCandidates",
        identity_fn=probe_candidate_id,
        validation_metadata={
            "prevalidatedDiscovery": True,
            "gamedevmapAuditValidated": True,
        },
        source_directory="gamedevmap",
        static_transform=lambda row: _validated_static_audit_candidate(
            row,
            promote_validated_static=promote_validated_static,
            validated_static_queue_cap=validated_static_queue_cap,
            validated_static_domain_cap=validated_static_domain_cap,
        ),
        unique_rows=unique_sources,
    )


def discover_gamedevmap_audit_candidates(
    timeout_s: int,
    *,
    config: dict[str, Any] | None = None,
    fetcher=fetch_text,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
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
        progress_callback=progress_callback,
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
