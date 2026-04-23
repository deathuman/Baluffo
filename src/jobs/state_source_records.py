from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from src.contracts import SCHEMA_VERSION
from src.jobs.common.datetime_utils import parse_datetime
from src.jobs.common.numbers import _clamped_int
from src.jobs.interfaces import SourceLoader
from src.jobs.text_utils import clean_text, norm_text, normalize_url
from src.jobs_fetcher_registry import EXCLUDED_DEFAULT_SOURCES, SOURCE_REPORT_META
from src.pipeline_io import write_text_if_changed
from src.shared.utils import now_iso

from . import state_incremental as _state_incremental
from .state_source_migration import normalized_google_sheets_redirect_cache


def source_rows_fingerprint(rows: Sequence[dict[str, Any]]) -> str:
    keys = []
    for row in rows:
        link = normalize_url(row.get("jobLink"))
        source_job_id = clean_text(row.get("sourceJobId"))
        title = norm_text(row.get("title"))
        keys.append(f"{source_job_id}|{link}|{title}")
    keys.sort()
    return hashlib.sha1("\n".join(keys).encode("utf-8")).hexdigest()


def normalize_source_state_payload(
    payload: dict[str, Any], *, updated_at: str = ""
) -> dict[str, Any]:
    src = payload if isinstance(payload, dict) else {}
    rows = src.get("sources")
    out_rows: dict[str, dict[str, Any]] = {}
    if isinstance(rows, dict):
        for raw_name, raw_entry in rows.items():
            name = clean_text(raw_name)
            if not name or not isinstance(raw_entry, dict):
                continue
            entry = {
                "lastRunAt": clean_text(raw_entry.get("lastRunAt")),
                "lastCheckedAt": clean_text(raw_entry.get("lastCheckedAt")),
                "lastStatus": clean_text(raw_entry.get("lastStatus")),
                "lastDurationMs": _clamped_int(raw_entry.get("lastDurationMs"), 0, 0),
                "lastFetchedCount": _clamped_int(raw_entry.get("lastFetchedCount"), 0, 0),
                "lastKeptCount": _clamped_int(raw_entry.get("lastKeptCount"), 0, 0),
                "lastJobsFound": _clamped_int(raw_entry.get("lastJobsFound"), 0, 0),
                "lastCandidateLinksFound": _clamped_int(
                    raw_entry.get("lastCandidateLinksFound"), 0, 0
                ),
                "lastDetailPagesVisited": _clamped_int(
                    raw_entry.get("lastDetailPagesVisited"), 0, 0
                ),
                "lastDetailYieldPct": _clamped_int(raw_entry.get("lastDetailYieldPct"), 0, 0),
                "lastRedirectCandidates": _clamped_int(
                    raw_entry.get("lastRedirectCandidates"), 0, 0
                ),
                "lastRedirectResolved": _clamped_int(raw_entry.get("lastRedirectResolved"), 0, 0),
                "lastRedirectCacheHits": _clamped_int(raw_entry.get("lastRedirectCacheHits"), 0, 0),
                "googleSheetsRedirectCache": normalized_google_sheets_redirect_cache(
                    raw_entry.get("googleSheetsRedirectCache")
                ),
                "lastAdapter": clean_text(raw_entry.get("lastAdapter")),
                "lastSuccessAt": clean_text(raw_entry.get("lastSuccessAt")),
                "lastNonEmptyAt": clean_text(raw_entry.get("lastNonEmptyAt")),
                "lastFingerprint": clean_text(raw_entry.get("lastFingerprint")),
                "lastListingFingerprint": clean_text(raw_entry.get("lastListingFingerprint")),
                "lastListingCheckedAt": clean_text(raw_entry.get("lastListingCheckedAt")),
                "lastHttpEtag": clean_text(raw_entry.get("lastHttpEtag")),
                "lastHttpLastModified": clean_text(raw_entry.get("lastHttpLastModified")),
                "lastHttpStatus": _clamped_int(raw_entry.get("lastHttpStatus"), 0, 0),
                "nextEligibleCheckAt": clean_text(raw_entry.get("nextEligibleCheckAt")),
                "cacheDecision": clean_text(raw_entry.get("cacheDecision")),
                "cacheDecisionReason": clean_text(raw_entry.get("cacheDecisionReason")),
                "browserEscalationEligible": bool(raw_entry.get("browserEscalationEligible")),
                "browserEscalationEligibleAt": clean_text(
                    raw_entry.get("browserEscalationEligibleAt")
                ),
                "browserEscalationEligibilityReason": clean_text(
                    raw_entry.get("browserEscalationEligibilityReason")
                ),
                "browserEscalationLastAttemptAt": clean_text(
                    raw_entry.get("browserEscalationLastAttemptAt")
                ),
                "browserEscalationLastAttemptFingerprint": clean_text(
                    raw_entry.get("browserEscalationLastAttemptFingerprint")
                ),
                "browserEscalationLastAttemptListingFingerprint": clean_text(
                    raw_entry.get("browserEscalationLastAttemptListingFingerprint")
                ),
                "browserEscalationLastSuccessAt": clean_text(
                    raw_entry.get("browserEscalationLastSuccessAt")
                ),
                "browserEscalationLastFailureAt": clean_text(
                    raw_entry.get("browserEscalationLastFailureAt")
                ),
                "browserEscalationLastError": clean_text(
                    raw_entry.get("browserEscalationLastError")
                ),
                "browserEscalationFailureCount": _clamped_int(
                    raw_entry.get("browserEscalationFailureCount"), 0, 0
                ),
                "browserEscalationQuarantinedUntilAt": clean_text(
                    raw_entry.get("browserEscalationQuarantinedUntilAt")
                ),
                "consecutiveFailures": _clamped_int(raw_entry.get("consecutiveFailures"), 0, 0),
                "consecutiveZeroKept": _clamped_int(raw_entry.get("consecutiveZeroKept"), 0, 0),
                "quarantinedUntilAt": clean_text(raw_entry.get("quarantinedUntilAt")),
                "lastFailureAt": clean_text(raw_entry.get("lastFailureAt")),
                "lastError": clean_text(raw_entry.get("lastError")),
                "healthScore": _clamped_int(raw_entry.get("healthScore"), 0, 100),
                "lastFailureBucket": clean_text(raw_entry.get("lastFailureBucket")),
                "structuredMigrationTargetAdapter": clean_text(
                    raw_entry.get("structuredMigrationTargetAdapter")
                ),
                "structuredMigrationBaselineCapturedAt": clean_text(
                    raw_entry.get("structuredMigrationBaselineCapturedAt")
                ),
                "structuredMigrationBaselineDurationMs": _clamped_int(
                    raw_entry.get("structuredMigrationBaselineDurationMs"), 0, 0
                ),
                "structuredMigrationBaselineStatus": clean_text(
                    raw_entry.get("structuredMigrationBaselineStatus")
                ),
                "structuredMigrationBaselineError": clean_text(
                    raw_entry.get("structuredMigrationBaselineError")
                ),
                "structuredMigrationBaselineFailureBucket": clean_text(
                    raw_entry.get("structuredMigrationBaselineFailureBucket")
                ),
                "structuredMigrationBaselineKeptCount": _clamped_int(
                    raw_entry.get("structuredMigrationBaselineKeptCount"), 0, 0
                ),
                "structuredMigrationShadowRunCount": _clamped_int(
                    raw_entry.get("structuredMigrationShadowRunCount"), 0, 0
                ),
                "structuredMigrationHealthyRunCount": _clamped_int(
                    raw_entry.get("structuredMigrationHealthyRunCount"), 0, 0
                ),
                "structuredMigrationPromotedAt": clean_text(
                    raw_entry.get("structuredMigrationPromotedAt")
                ),
                "structuredMigrationDemotedAt": clean_text(
                    raw_entry.get("structuredMigrationDemotedAt")
                ),
                "structuredMigrationLastDuplicateRate": float(
                    raw_entry.get("structuredMigrationLastDuplicateRate") or 0.0
                ),
                "structuredMigrationLastKeptCount": _clamped_int(
                    raw_entry.get("structuredMigrationLastKeptCount"), 0, 0
                ),
            }
            raw_latencies = raw_entry.get("recentLatencies")
            if isinstance(raw_latencies, list):
                clean_latencies = [
                    _clamped_int(x, 0, 2**31 - 1)
                    for x in raw_latencies
                    if isinstance(x, (int, float))
                ]
                if clean_latencies:
                    entry["recentLatencies"] = clean_latencies
            raw_stage_timings = (
                raw_entry.get("lastStageTimingsMs")
                if isinstance(raw_entry.get("lastStageTimingsMs"), dict)
                else {}
            )
            clean_stage_timings = {
                "listingFetch": _clamped_int(raw_stage_timings.get("listingFetch"), 0, 0),
                "parseCsv": _clamped_int(raw_stage_timings.get("parseCsv"), 0, 0),
                "candidateExtraction": _clamped_int(
                    raw_stage_timings.get("candidateExtraction"), 0, 0
                ),
                "detailFetch": _clamped_int(raw_stage_timings.get("detailFetch"), 0, 0),
                "redirectResolve": _clamped_int(raw_stage_timings.get("redirectResolve"), 0, 0),
                "canonicalization": _clamped_int(raw_stage_timings.get("canonicalization"), 0, 0),
            }
            if any(clean_stage_timings.values()):
                entry["lastStageTimingsMs"] = clean_stage_timings
            out_rows[name] = {
                key: value for key, value in entry.items() if value != "" and value is not None
            }
    return {
        "schemaVersion": SCHEMA_VERSION,
        "updatedAt": clean_text(src.get("updatedAt")) or clean_text(updated_at) or now_iso(),
        "sources": out_rows,
    }


def read_source_state(state_path: Path) -> dict[str, dict[str, Any]]:
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    normalized = normalize_source_state_payload(payload)
    rows = normalized.get("sources")
    return rows if isinstance(rows, dict) else {}


def write_source_state(state_path: Path, rows: dict[str, dict[str, Any]]) -> None:
    payload = normalize_source_state_payload({"sources": rows}, updated_at=now_iso())
    write_text_if_changed(state_path, json.dumps(payload, indent=2, ensure_ascii=False))


def circuit_breaker_until(
    source_name: str, state_rows: dict[str, dict[str, Any]], failure_threshold: int
) -> datetime | None:
    if failure_threshold <= 0:
        return None
    entry = state_rows.get(source_name)
    if not isinstance(entry, dict):
        return None
    if int(entry.get("consecutiveFailures") or 0) < failure_threshold:
        return None
    return parse_datetime(entry.get("quarantinedUntilAt"))


def build_excluded_source_report(source_name: str, reason: str) -> dict[str, Any]:
    return {
        "name": source_name,
        "status": "excluded",
        "adapter": clean_text(SOURCE_REPORT_META.get(source_name, {}).get("adapter")) or "custom",
        "fetchStrategy": clean_text(SOURCE_REPORT_META.get(source_name, {}).get("fetchStrategy"))
        or "auto",
        "studio": clean_text(SOURCE_REPORT_META.get(source_name, {}).get("studio")) or "",
        "fetchedCount": 0,
        "keptCount": 0,
        "error": clean_text(reason),
        "exclusionReason": clean_text(reason),
        "durationMs": 0,
    }


def apply_circuit_breaker_exclusions(
    selected_loaders: list[tuple[str, SourceLoader]],
    *,
    source_state_rows: dict[str, dict[str, Any]],
    circuit_breaker_failures: int,
    circuit_breaker_cooldown_minutes: int,
    ignore_circuit_breaker: bool,
) -> tuple[list[tuple[str, SourceLoader]], list[dict[str, Any]]]:
    if (
        ignore_circuit_breaker
        or circuit_breaker_failures <= 0
        or circuit_breaker_cooldown_minutes <= 0
    ):
        return list(selected_loaders), []
    filtered: list[tuple[str, SourceLoader]] = []
    excluded_rows: list[dict[str, Any]] = []
    now_dt = datetime.now(UTC)
    for name, loader in selected_loaders:
        blocked_until = circuit_breaker_until(name, source_state_rows, circuit_breaker_failures)
        if blocked_until and blocked_until > now_dt:
            excluded_rows.append(
                build_excluded_source_report(
                    name, f"circuit_breaker_active_until:{blocked_until.isoformat()}"
                )
            )
            continue
        filtered.append((name, loader))
    return filtered, excluded_rows


def append_excluded_default_sources(source_reports: list[dict[str, Any]]) -> None:
    for source_name, reason in EXCLUDED_DEFAULT_SOURCES.items():
        source_reports.append(build_excluded_source_report(source_name, reason))


def snapshot_prior_source_state(entry: dict[str, Any]) -> dict[str, Any]:
    return {
        "lastDurationMs": int(entry.get("lastDurationMs") or 0),
        "lastStatus": clean_text(entry.get("lastStatus")),
        "lastError": clean_text(entry.get("lastError")),
        "lastFailureBucket": clean_text(entry.get("lastFailureBucket")),
        "lastKeptCount": int(entry.get("lastKeptCount") or 0),
    }


def apply_static_detail_stats(
    entry: dict[str, Any], report: dict[str, Any]
) -> list[dict[str, Any]]:
    details = report.get("details") if isinstance(report.get("details"), list) else []
    static_detail = details[0] if len(details) == 1 and isinstance(details[0], dict) else {}
    static_stats = (
        static_detail.get("stats")
        if isinstance(static_detail, dict) and isinstance(static_detail.get("stats"), dict)
        else {}
    )
    entry["lastCandidateLinksFound"] = int(static_stats.get("candidate_links_found") or 0)
    entry["lastDetailPagesVisited"] = int(static_stats.get("detail_pages_visited") or 0)
    entry["lastDetailYieldPct"] = int(static_stats.get("detail_yield_percent") or 0)
    entry["lastRedirectCandidates"] = int(static_stats.get("redirect_candidates") or 0)
    entry["lastRedirectResolved"] = int(static_stats.get("redirect_resolved") or 0)
    entry["lastRedirectCacheHits"] = int(static_stats.get("redirect_cache_hits") or 0)
    return details


def apply_stage_timings(entry: dict[str, Any], report: dict[str, Any]) -> None:
    stage_timings = (
        report.get("stageTimingsMs") if isinstance(report.get("stageTimingsMs"), dict) else {}
    )
    clean_stage_timings = {
        "listingFetch": int(stage_timings.get("listingFetch") or 0),
        "parseCsv": int(stage_timings.get("parseCsv") or 0),
        "candidateExtraction": int(stage_timings.get("candidateExtraction") or 0),
        "detailFetch": int(stage_timings.get("detailFetch") or 0),
        "redirectResolve": int(stage_timings.get("redirectResolve") or 0),
        "canonicalization": int(stage_timings.get("canonicalization") or 0),
    }
    if any(clean_stage_timings.values()):
        entry["lastStageTimingsMs"] = clean_stage_timings
    else:
        entry.pop("lastStageTimingsMs", None)


def apply_successful_source_state(
    entry: dict[str, Any],
    *,
    report: dict[str, Any],
    source_name: str,
    canonical_rows: list[dict[str, Any]],
    finished_at: str,
    circuit_breaker_cooldown_minutes: int,
    circuit_breaker_zero_kept: int,
) -> None:
    entry["lastSuccessAt"] = finished_at
    if entry["lastKeptCount"] > 0:
        entry["lastNonEmptyAt"] = finished_at
        entry["consecutiveZeroKept"] = 0
    else:
        zero_kept_count = int(entry.get("consecutiveZeroKept") or 0) + 1
        entry["consecutiveZeroKept"] = zero_kept_count
        if (
            circuit_breaker_zero_kept > 0
            and zero_kept_count >= circuit_breaker_zero_kept
            and circuit_breaker_cooldown_minutes > 0
        ):
            entry["quarantinedUntilAt"] = (
                datetime.now(UTC) + timedelta(minutes=circuit_breaker_cooldown_minutes)
            ).isoformat()
    reported_fingerprint = clean_text(report.get("sourceFingerprint"))
    if not reported_fingerprint and entry["lastKeptCount"] > 0:
        reported_fingerprint = source_rows_fingerprint(
            [row for row in canonical_rows if clean_text(row.get("source")) == source_name]
        )
    previous_fingerprint = clean_text(entry.get("lastFingerprint"))
    if reported_fingerprint:
        entry["lastFingerprint"] = reported_fingerprint
        if reported_fingerprint != previous_fingerprint:
            entry["lastChangedAt"] = finished_at
    entry["consecutiveFailures"] = 0
    for key in ("quarantinedUntilAt", "lastFailureAt", "lastError"):
        entry.pop(key, None)
    failure_bucket = report.get("failureBucket")
    if failure_bucket:
        entry["lastFailureBucket"] = clean_text(failure_bucket)


def apply_errored_source_state(
    entry: dict[str, Any],
    *,
    report: dict[str, Any],
    finished_at: str,
    circuit_breaker_failures: int,
    circuit_breaker_cooldown_minutes: int,
) -> None:
    failure_count = int(entry.get("consecutiveFailures") or 0) + 1
    entry["consecutiveFailures"] = failure_count
    entry["lastFailureAt"] = finished_at
    entry["lastError"] = clean_text(report.get("error"))
    failure_bucket = report.get("failureBucket")
    if failure_bucket:
        entry["lastFailureBucket"] = clean_text(failure_bucket)
    if (
        circuit_breaker_failures > 0
        and failure_count >= circuit_breaker_failures
        and circuit_breaker_cooldown_minutes > 0
    ):
        entry["quarantinedUntilAt"] = (
            datetime.now(UTC) + timedelta(minutes=circuit_breaker_cooldown_minutes)
        ).isoformat()


def apply_excluded_source_state(
    entry: dict[str, Any], *, report: dict[str, Any], finished_at: str
) -> None:
    exclusion_reason = clean_text(report.get("exclusionReason")) or clean_text(
        report.get("cacheDecisionReason")
    )
    if exclusion_reason == "not_modified_304":
        entry["lastSuccessAt"] = finished_at
        entry["consecutiveFailures"] = 0
        for key in ("quarantinedUntilAt", "lastFailureAt", "lastError"):
            entry.pop(key, None)


def refresh_next_eligible_check_at(
    entry: dict[str, Any], *, source_name: str, finished_at: str
) -> None:
    entry["nextEligibleCheckAt"] = _state_incremental.compute_next_eligible_check_at(
        entry,
        adapter=_state_incremental.adapter_for_cache(source_name, entry),
        checked_at=finished_at,
    )


def read_previously_successful_sources(report_path: Path) -> set[str]:
    try:
        payload = json.loads(report_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return set()
    if not isinstance(payload, dict):
        return set()
    rows = payload.get("sources")
    if not isinstance(rows, list):
        return set()
    successful: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        name = clean_text(row.get("name"))
        if name and norm_text(row.get("status")) == "ok" and int(row.get("keptCount") or 0) > 0:
            successful.add(name)
    return successful


def read_success_cache(cache_path: Path) -> set[str]:
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return set()
    if not isinstance(payload, dict):
        return set()
    rows = payload.get("successfulSources")
    if not isinstance(rows, list):
        return set()
    return {clean_text(item) for item in rows if clean_text(item)}


def write_success_cache(cache_path: Path, source_reports: Sequence[dict[str, Any]]) -> None:
    successful = {
        clean_text(row.get("name"))
        for row in source_reports
        if norm_text(row.get("status")) == "ok"
        and int(row.get("keptCount") or 0) > 0
        and clean_text(row.get("name"))
    }
    if not successful:
        return
    previous = read_success_cache(cache_path)
    payload = {"updatedAt": now_iso(), "successfulSources": sorted(previous | successful)}
    write_text_if_changed(cache_path, json.dumps(payload, indent=2, ensure_ascii=False))
