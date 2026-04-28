from __future__ import annotations

import time
from typing import Any, cast

from src.shared.json_shapes import as_json_object

from . import audit_ledger
from .directory_fetch_jobs import build_directory_fetch_jobs
from .directory_page_recovery import (
    DEFAULT_RECOVERY_URL_LIMIT,
    apply_recovery_to_scan_result,
    default_recovery_summary,
    run_directory_page_recovery,
    run_recovery_for_requests,
)
from .scoring import unique_string_list
from .static_candidates import build_known_careers_url_candidate

ProviderInferenceFn = Any


def build_directory_static_candidate(
    *,
    studio: str,
    target_url: str,
    nl_priority: bool,
    website_only: bool,
    name_suffix: str,
    discovery_method: str,
    evidence_source: str,
    evidence_types: list[str],
    source_directory: str,
    source_directory_url: str,
    source_directory_entry_url: str,
    source_directory_location: str = "",
    source_directory_categories: list[str] | None = None,
    manual_only: bool = False,
    weak_signal: bool = False,
    website_evidence_types: list[str] | None = None,
    careers_evidence_type: str = "",
    location_evidence_type: str = "",
    weak_signal_evidence_type: str = "",
    evidence_score: int = 40,
    website_only_evidence_score: int = 24,
) -> dict[str, Any]:
    location = str(source_directory_location or "").strip()
    row_evidence_types = list(evidence_types)
    if website_only:
        row_evidence_types.extend(list(website_evidence_types or []))
        if location and location_evidence_type:
            row_evidence_types.append(location_evidence_type)
        if weak_signal and weak_signal_evidence_type:
            row_evidence_types.append(weak_signal_evidence_type)
        row: dict[str, Any] = {
            "name": f"{studio} ({name_suffix})",
            "studio": studio,
            "company": studio,
            "adapter": "static",
            "pages": [target_url],
            "listing_url": target_url,
            "nlPriority": nl_priority,
            "enabledByDefault": False,
            "discoveryMethod": discovery_method,
            "discoveryStage": "generic_static",
            "careersUrl": "",
            "evidenceSource": evidence_source,
            "evidenceTypes": row_evidence_types,
            "evidenceScore": website_only_evidence_score,
            "weakSignal": True,
            "sourceDirectory": source_directory,
            "sourceDirectoryUrl": source_directory_url,
            "sourceDirectoryEntryUrl": source_directory_entry_url,
            "sourceDirectoryLocation": location,
            "manualOnly": bool(manual_only),
        }
        if source_directory_categories is not None:
            row["sourceDirectoryCategories"] = unique_string_list(source_directory_categories)
        return row

    if careers_evidence_type:
        row_evidence_types.append(careers_evidence_type)
    if location and location_evidence_type:
        row_evidence_types.append(location_evidence_type)
    if weak_signal and weak_signal_evidence_type:
        row_evidence_types.append(weak_signal_evidence_type)
    extra_fields: dict[str, Any] = {
        "sourceDirectory": source_directory,
        "sourceDirectoryUrl": source_directory_url,
        "sourceDirectoryEntryUrl": source_directory_entry_url,
        "sourceDirectoryLocation": location,
        "manualOnly": bool(manual_only),
    }
    if source_directory_categories is not None:
        extra_fields["sourceDirectoryCategories"] = unique_string_list(source_directory_categories)
    return build_known_careers_url_candidate(
        target_url,
        studio=studio,
        name_suffix=name_suffix,
        nl_priority=nl_priority,
        discovery_method=discovery_method,
        evidence_source=evidence_source,
        evidence_types=row_evidence_types,
        evidence_score=evidence_score,
        enabled_by_default=False,
        extra_fields=extra_fields,
    )


def build_known_directory_entry_candidate(
    *,
    target_url: str,
    studio: str,
    nl_priority: bool,
    discovery_method: str,
    discovery_stage: str,
    evidence_source: str,
    evidence_types: list[str],
    evidence_score: int,
    name_suffix: str,
    enabled_by_default: bool | None = False,
    weak_signal: bool = False,
    extra_fields: dict[str, Any] | None = None,
    infer_provider: ProviderInferenceFn | None = None,
) -> dict[str, Any]:
    if infer_provider is not None:
        inferred = infer_provider(
            target_url,
            studio,
            nl_priority=nl_priority,
            discovery_method=discovery_method,
        )
        if inferred:
            row = dict(inferred)
            row["discoveryStage"] = discovery_stage
            row["discoveryMethod"] = discovery_method
            row["evidenceTypes"] = unique_string_list(
                [*(row.get("evidenceTypes") or []), *evidence_types]
            )
            row["evidenceScore"] = max(int(row.get("evidenceScore") or 0), int(evidence_score))
            row["weakSignal"] = bool(row.get("weakSignal")) or bool(weak_signal)
            row["careersUrl"] = target_url
            if isinstance(extra_fields, dict):
                row.update(extra_fields)
            return row

    return build_known_careers_url_candidate(
        target_url,
        studio=studio,
        name_suffix=name_suffix,
        nl_priority=nl_priority,
        discovery_method=discovery_method,
        discovery_stage=discovery_stage,
        evidence_source=evidence_source,
        evidence_types=evidence_types,
        evidence_score=int(evidence_score),
        enabled_by_default=enabled_by_default,
        weak_signal=bool(weak_signal),
        extra_fields=extra_fields,
    )


def apply_directory_provenance(
    candidate: dict[str, Any],
    *,
    evidence_source: str,
    evidence_types: list[str],
    source_directory: str,
    source_directory_url: str,
    source_directory_entry_url: str,
    source_directory_location: str = "",
    source_directory_categories: list[str] | None = None,
    careers_url_fallback: str = "",
    name_suffix: str = "",
    evidence_score_floor: int | None = None,
) -> dict[str, Any]:
    enriched = dict(candidate)
    enriched["evidenceSource"] = evidence_source
    enriched["evidenceTypes"] = unique_string_list(
        [*(enriched.get("evidenceTypes") or []), *evidence_types]
    )
    if evidence_score_floor is not None:
        enriched["evidenceScore"] = max(
            int(enriched.get("evidenceScore") or 0), int(evidence_score_floor)
        )
    if name_suffix:
        enriched["name"] = f"{str(enriched.get('studio') or '').strip()} ({name_suffix})"
    enriched["sourceDirectory"] = source_directory
    enriched["sourceDirectoryUrl"] = source_directory_url
    enriched["sourceDirectoryEntryUrl"] = source_directory_entry_url
    if source_directory_categories is not None:
        enriched["sourceDirectoryCategories"] = unique_string_list(source_directory_categories)
    enriched["sourceDirectoryLocation"] = str(source_directory_location or "").strip()
    if careers_url_fallback:
        enriched["careersUrl"] = (
            str(enriched.get("careersUrl") or careers_url_fallback).strip() or careers_url_fallback
        )
    return enriched


def empty_directory_scan_result(
    *,
    failures: list[dict[str, Any]],
    summary: dict[str, Any],
    batch_timing: dict[str, Any],
    write_cache: bool,
) -> dict[str, Any]:
    return empty_scan_result_payload(
        failures=failures,
        summary=summary,
        batch_timing=batch_timing,
        extra_fields={
            "websiteFetchJobs": [],
            "browserRecoveryCandidates": [],
            "writeCache": bool(write_cache),
        },
    )


def empty_scan_result_payload(
    *,
    failures: list[dict[str, Any]],
    summary: dict[str, Any],
    batch_timing: dict[str, Any],
    progress: dict[str, Any] | None = None,
    extra_fields: dict[str, Any] | None = None,
) -> dict[str, Any]:
    row = {
        "providerCandidates": [],
        "staticCandidates": [],
        "failures": failures,
        "summary": summary,
    }
    if progress is not None:
        row["progress"] = progress
    row["batchTiming"] = batch_timing
    if isinstance(extra_fields, dict):
        row.update(extra_fields)
    return row


def run_directory_website_scan(
    timeout_s: int,
    *,
    entries: list[dict[str, Any]],
    url_field: str,
    adapter: str,
    failure_stage: str,
    fetcher: Any,
    fetch_pages: Any,
    fetch_concurrency: int,
    per_host_concurrency: int,
    progress_label: str,
    analyze_result: Any,
    enable_recovery: bool,
    recovery_analyze_result: Any,
    recovery_progress_label: str,
    unique_sources_fn: Any,
    batch_timing: dict[str, Any],
    summary: dict[str, Any],
    progress_cursor: int,
    required_fields: tuple[str, ...] = (),
    initial_provider_candidates: list[dict[str, Any]] | None = None,
    initial_static_candidates: list[dict[str, Any]] | None = None,
    initial_failures: list[dict[str, Any]] | None = None,
    write_cache: bool = True,
    recovery_url_limit: int = DEFAULT_RECOVERY_URL_LIMIT,
    recovery_runner: Any = run_directory_page_recovery,
) -> dict[str, Any]:
    provider_candidates = list(initial_provider_candidates or [])
    static_candidates = list(initial_static_candidates or [])
    failures = list(initial_failures or [])
    browser_recovery_candidates: list[dict[str, Any]] = []
    recovery_requests: list[Any] = []
    fallback_static_candidates: list[dict[str, Any]] = []
    bad_provider_inferences = 0
    recovery_summary: dict[str, int] = default_recovery_summary()

    website_fetch_jobs = build_directory_fetch_jobs(
        entries,
        url_field=url_field,
        adapter=adapter,
        failure_stage=failure_stage,
        required_fields=required_fields,
    )

    started = time.perf_counter()
    website_fetch_results = fetch_pages(
        timeout_s,
        website_fetch_jobs,
        fetcher=fetcher,
        total_concurrency=fetch_concurrency,
        per_host_concurrency=per_host_concurrency,
        progress_label=progress_label,
    )
    batch_timing["websiteFetchMs"] = audit_ledger.duration_ms(started)

    website_fetch_failures = 0
    started = time.perf_counter()
    for result in website_fetch_results:
        rows = analyze_result(result)
        provider_candidates.extend(list(rows.get("providerCandidates") or []))
        static_candidates.extend(list(rows.get("staticCandidates") or []))
        failures.extend(list(rows.get("failures") or []))
        recovery_requests.extend(list(rows.get("recoveryRequests") or []))
        fallback_static_candidates.extend(list(rows.get("fallbackStaticCandidates") or []))
        bad_provider_inferences += int(rows.get("badProviderInferences") or 0)
        if bool(rows.get("fetchFailed")):
            website_fetch_failures += 1
    batch_timing["candidateAnalysisMs"] = audit_ledger.duration_ms(started)

    recovery = run_recovery_for_requests(
        timeout_s,
        recovery_requests if enable_recovery else [],
        fetcher=fetcher,
        total_concurrency=fetch_concurrency,
        per_host_concurrency=per_host_concurrency,
        analyze_result=recovery_analyze_result,
        progress_label=recovery_progress_label,
        url_limit=recovery_url_limit,
        recovery_runner=recovery_runner,
    )
    recovery_summary = dict(recovery.summary)

    def _fallback_static_key(entry: Any) -> str:
        src = as_json_object(entry)
        return str(src.get("key") or "")

    def _fallback_static_candidate(entry: Any) -> dict[str, Any] | None:
        candidate = as_json_object(as_json_object(entry).get("candidate"))
        return dict(candidate) if candidate else None

    scan_payload = apply_recovery_to_scan_result(
        {
            "providerCandidates": provider_candidates,
            "staticCandidates": static_candidates,
            "browserRecoveryCandidates": browser_recovery_candidates,
            "summary": {},
            "batchTiming": batch_timing,
        },
        recovery,
        provider_dedupe=unique_sources_fn,
        static_dedupe=unique_sources_fn,
        fallback_static_candidates=fallback_static_candidates,
        fallback_key=_fallback_static_key,
        fallback_candidate=_fallback_static_candidate,
    )
    provider_candidates = list(scan_payload.get("providerCandidates") or [])
    static_candidates = list(scan_payload.get("staticCandidates") or [])
    browser_recovery_candidates = list(scan_payload.get("browserRecoveryCandidates") or [])
    merged_batch_timing = dict(scan_payload.get("batchTiming") or batch_timing)
    batch_timing.clear()
    batch_timing.update(merged_batch_timing)
    return {
        "providerCandidates": provider_candidates,
        "staticCandidates": static_candidates,
        "failures": failures,
        "summary": {
            **summary,
            "websiteFetchJobs": len(website_fetch_jobs),
            "websiteFetchFailures": website_fetch_failures,
            **recovery_summary,
            "browserRecoveryCandidates": len(browser_recovery_candidates),
            "badProviderInferences": bad_provider_inferences,
        },
        "websiteFetchJobs": website_fetch_jobs,
        "browserRecoveryCandidates": browser_recovery_candidates,
        "progress": {
            "complete": True,
            "cursor": int(progress_cursor),
            "completedUrlIdentities": [
                str(row.get("url") or "").strip()
                for row in website_fetch_jobs
                if isinstance(row, dict)
            ],
        },
        "batchTiming": batch_timing,
        "writeCache": bool(write_cache),
    }


def run_directory_adapter_website_scan(
    timeout_s: int,
    *,
    cfg: dict[str, Any],
    entries: list[dict[str, Any]],
    url_field: str,
    adapter: str,
    failure_stage: str,
    fetcher: Any,
    fetch_pages: Any,
    resolve_fetch_limits: Any,
    progress_label: str,
    analyze_result: Any,
    enable_recovery: bool,
    recovery_analyze_result: Any,
    recovery_progress_label: str,
    unique_sources_fn: Any,
    batch_timing: dict[str, Any],
    summary: dict[str, Any],
    progress_cursor: int,
    recovery_url_limit: int = DEFAULT_RECOVERY_URL_LIMIT,
    required_fields: tuple[str, ...] = (),
    initial_provider_candidates: list[dict[str, Any]] | None = None,
    initial_static_candidates: list[dict[str, Any]] | None = None,
    initial_failures: list[dict[str, Any]] | None = None,
    write_cache: bool = True,
    recovery_runner: Any = run_directory_page_recovery,
) -> dict[str, Any]:
    fetch_concurrency, per_host_concurrency = resolve_fetch_limits(cfg)
    batch_timing["fetchConcurrency"] = fetch_concurrency
    batch_timing["perHostConcurrency"] = per_host_concurrency
    return run_directory_website_scan(
        timeout_s,
        entries=entries,
        url_field=url_field,
        adapter=adapter,
        failure_stage=failure_stage,
        fetcher=fetcher,
        fetch_pages=fetch_pages,
        fetch_concurrency=fetch_concurrency,
        per_host_concurrency=per_host_concurrency,
        progress_label=progress_label,
        analyze_result=analyze_result,
        enable_recovery=enable_recovery,
        recovery_analyze_result=recovery_analyze_result,
        recovery_progress_label=recovery_progress_label,
        unique_sources_fn=unique_sources_fn,
        batch_timing=batch_timing,
        summary=summary,
        progress_cursor=progress_cursor,
        recovery_url_limit=recovery_url_limit,
        required_fields=required_fields,
        initial_provider_candidates=initial_provider_candidates,
        initial_static_candidates=initial_static_candidates,
        initial_failures=initial_failures,
        write_cache=write_cache,
        recovery_runner=recovery_runner,
    )


def run_directory_entry_selection_scan(
    timeout_s: int,
    *,
    cfg: dict[str, Any],
    parsed_entries: list[dict[str, Any]],
    empty_result: Any,
    select_entries: Any,
    emit_selection_log: Any,
    url_field: str,
    adapter: str,
    failure_stage: str,
    fetcher: Any,
    fetch_pages: Any,
    resolve_fetch_limits: Any,
    progress_label: str,
    analyze_result: Any,
    enable_recovery: bool,
    recovery_analyze_result: Any,
    recovery_progress_label: str,
    unique_sources_fn: Any,
    batch_timing: dict[str, Any],
    summary: dict[str, Any],
    recovery_url_limit: int = DEFAULT_RECOVERY_URL_LIMIT,
    selection_timing_key: str = "candidateSelectionMs",
    required_fields: tuple[str, ...] = (),
    initial_failures: list[dict[str, Any]] | None = None,
    write_cache: bool = True,
) -> dict[str, Any]:
    if not parsed_entries:
        return cast(dict[str, Any], empty_result())

    started = time.perf_counter()
    selected = select_entries(parsed_entries)
    if selection_timing_key:
        batch_timing[selection_timing_key] = audit_ledger.duration_ms(started)
    selected = as_json_object(selected)
    homepage_entries = list(selected.get("homepageEntries") or [])
    eligible_entries = int(selected.get("eligibleEntries") or len(homepage_entries))
    provider_candidates = list(selected.get("providerCandidates") or [])
    static_candidates = list(selected.get("staticCandidates") or [])
    selected_failures = list(selected.get("failures") or [])
    emit_selection_log(selected)
    return run_directory_adapter_website_scan(
        timeout_s,
        cfg=cfg,
        entries=homepage_entries,
        url_field=url_field,
        adapter=adapter,
        failure_stage=failure_stage,
        fetcher=fetcher,
        fetch_pages=fetch_pages,
        resolve_fetch_limits=resolve_fetch_limits,
        progress_label=progress_label,
        analyze_result=analyze_result,
        enable_recovery=enable_recovery,
        recovery_analyze_result=recovery_analyze_result,
        recovery_progress_label=recovery_progress_label,
        unique_sources_fn=unique_sources_fn,
        batch_timing=batch_timing,
        summary={
            **summary,
            **dict(as_json_object(selected.get("summary"))),
            "eligibleRows": eligible_entries,
        },
        progress_cursor=int(selected.get("progressCursor") or eligible_entries),
        recovery_url_limit=recovery_url_limit,
        required_fields=required_fields,
        initial_provider_candidates=provider_candidates,
        initial_static_candidates=static_candidates,
        initial_failures=[*list(initial_failures or []), *selected_failures],
        write_cache=write_cache,
    )
