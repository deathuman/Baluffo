"""Source-state helpers for the jobs pipeline."""

from __future__ import annotations

from typing import Any

from src.jobs.text_utils import clean_text

from .state_source_records import (
    append_excluded_default_sources,
    apply_browser_escalation_state,
    apply_circuit_breaker_exclusions,
    apply_errored_source_state,
    apply_excluded_source_state,
    apply_provider_coverage_state,
    apply_stage_timings,
    apply_static_detail_stats,
    apply_structured_migration_state,
    apply_successful_source_state,
    browser_fallback_state_row,
    build_browser_fallback_circuit_breaker,
    circuit_breaker_until,
    normalize_source_state_payload,
    read_previously_successful_sources,
    read_source_state,
    read_success_cache,
    refresh_next_eligible_check_at,
    set_browser_fallback_state,
    should_skip_static_source_for_structured_migration,
    snapshot_prior_source_state,
    source_rows_fingerprint,
    write_source_state,
    write_success_cache,
)


def _apply_report_to_entry(
    source_state_rows: dict[str, dict[str, Any]],
    *,
    source_name: str,
    report: dict[str, Any],
    canonical_rows: list[dict[str, Any]],
    finished_at: str,
    circuit_breaker_failures: int,
    circuit_breaker_cooldown_minutes: int,
    circuit_breaker_zero_kept: int,
) -> None:
    entry = dict(source_state_rows.get(source_name) or {})
    prior_state = snapshot_prior_source_state(entry)
    _copy_report_fields(entry, report, finished_at)
    _apply_browser_state(
        entry,
        report,
        finished_at,
        circuit_breaker_cooldown_minutes,
    )

    details = apply_static_detail_stats(entry, report)
    apply_stage_timings(entry, report)

    _apply_status_state(
        entry,
        report=report,
        source_name=source_name,
        canonical_rows=canonical_rows,
        finished_at=finished_at,
        circuit_breaker_failures=circuit_breaker_failures,
        circuit_breaker_cooldown_minutes=circuit_breaker_cooldown_minutes,
        circuit_breaker_zero_kept=circuit_breaker_zero_kept,
    )

    apply_structured_migration_state(
        entry,
        report=report,
        finished_at=finished_at,
        prior_state=prior_state,
    )
    apply_provider_coverage_state(
        entry,
        report=report,
        source_name=source_name,
        canonical_rows=canonical_rows,
        finished_at=finished_at,
    )
    source_state_rows[source_name] = entry
    _apply_detail_reports(
        source_state_rows,
        details=details,
        canonical_rows=canonical_rows,
        finished_at=finished_at,
        circuit_breaker_failures=circuit_breaker_failures,
        circuit_breaker_cooldown_minutes=circuit_breaker_cooldown_minutes,
        circuit_breaker_zero_kept=circuit_breaker_zero_kept,
    )


def _copy_report_fields(
    entry: dict[str, Any],
    report: dict[str, Any],
    finished_at: str,
) -> None:
    entry["lastRunAt"] = finished_at
    entry["lastCheckedAt"] = finished_at
    entry["lastStatus"] = clean_text(report.get("status"))
    entry["lastAdapter"] = clean_text(report.get("adapter")) or clean_text(entry.get("lastAdapter"))
    entry["lastDurationMs"] = int(report.get("durationMs") or 0)
    entry["lastFetchedCount"] = int(report.get("fetchedCount") or 0)
    entry["lastKeptCount"] = int(report.get("keptCount") or 0)
    entry["lastJobsFound"] = int(report.get("keptCount") or 0)
    entry["cacheDecision"] = clean_text(report.get("cacheDecision")) or clean_text(
        entry.get("cacheDecision")
    )
    entry["cacheDecisionReason"] = clean_text(report.get("cacheDecisionReason")) or clean_text(
        entry.get("cacheDecisionReason")
    )
    if clean_text(report.get("listingFingerprint")):
        entry["lastListingFingerprint"] = clean_text(report.get("listingFingerprint"))
        entry["lastListingCheckedAt"] = clean_text(report.get("listingCheckedAt")) or finished_at
    if clean_text(report.get("httpEtag")):
        entry["lastHttpEtag"] = clean_text(report.get("httpEtag"))
    if clean_text(report.get("httpLastModified")):
        entry["lastHttpLastModified"] = clean_text(report.get("httpLastModified"))
    if int(report.get("httpStatus") or 0) > 0:
        entry["lastHttpStatus"] = int(report.get("httpStatus") or 0)


def _apply_browser_state(
    entry: dict[str, Any],
    report: dict[str, Any],
    finished_at: str,
    circuit_breaker_cooldown_minutes: int,
) -> None:
    apply_browser_escalation_state(
        entry,
        report=report,
        finished_at=finished_at,
        circuit_breaker_cooldown_minutes=circuit_breaker_cooldown_minutes,
    )
    if entry["lastKeptCount"] > 0:
        for key in (
            "browserEscalationEligible",
            "browserEscalationEligibleAt",
            "browserEscalationEligibilityReason",
        ):
            entry.pop(key, None)


def _apply_status_state(
    entry: dict[str, Any],
    *,
    report: dict[str, Any],
    source_name: str,
    canonical_rows: list[dict[str, Any]],
    finished_at: str,
    circuit_breaker_failures: int,
    circuit_breaker_cooldown_minutes: int,
    circuit_breaker_zero_kept: int,
) -> None:
    if entry["lastStatus"] == "ok":
        apply_successful_source_state(
            entry,
            report=report,
            source_name=source_name,
            canonical_rows=canonical_rows,
            finished_at=finished_at,
            circuit_breaker_cooldown_minutes=circuit_breaker_cooldown_minutes,
            circuit_breaker_zero_kept=circuit_breaker_zero_kept,
        )
    elif entry["lastStatus"] == "error":
        apply_errored_source_state(
            entry,
            report=report,
            finished_at=finished_at,
            circuit_breaker_failures=circuit_breaker_failures,
            circuit_breaker_cooldown_minutes=circuit_breaker_cooldown_minutes,
        )
    elif entry["lastStatus"] == "excluded":
        apply_excluded_source_state(entry, report=report, finished_at=finished_at)
    else:
        return
    refresh_next_eligible_check_at(entry, source_name=source_name, finished_at=finished_at)


def _apply_detail_reports(
    source_state_rows: dict[str, dict[str, Any]],
    *,
    details: list[dict[str, Any]],
    canonical_rows: list[dict[str, Any]],
    finished_at: str,
    circuit_breaker_failures: int,
    circuit_breaker_cooldown_minutes: int,
    circuit_breaker_zero_kept: int,
) -> None:
    for item in details:
        detail_name = clean_text(item.get("name")) if isinstance(item, dict) else ""
        if detail_name:
            _apply_report_to_entry(
                source_state_rows,
                source_name=detail_name,
                report=item,
                canonical_rows=canonical_rows,
                finished_at=finished_at,
                circuit_breaker_failures=circuit_breaker_failures,
                circuit_breaker_cooldown_minutes=circuit_breaker_cooldown_minutes,
                circuit_breaker_zero_kept=circuit_breaker_zero_kept,
            )


def update_source_state_rows(
    *,
    source_state_rows: dict[str, dict[str, Any]],
    source_reports: list[dict[str, Any]],
    canonical_rows: list[dict[str, Any]],
    finished_at: str,
    circuit_breaker_failures: int,
    circuit_breaker_cooldown_minutes: int,
    circuit_breaker_zero_kept: int = 3,
) -> dict[str, dict[str, Any]]:
    next_rows = dict(source_state_rows or {})
    for report in source_reports:
        source_name = clean_text(report.get("name")) if isinstance(report, dict) else ""
        if not source_name:
            continue
        _apply_report_to_entry(
            next_rows,
            source_name=source_name,
            report=report,
            canonical_rows=canonical_rows,
            finished_at=finished_at,
            circuit_breaker_failures=circuit_breaker_failures,
            circuit_breaker_cooldown_minutes=circuit_breaker_cooldown_minutes,
            circuit_breaker_zero_kept=circuit_breaker_zero_kept,
        )
    return next_rows


__all__ = [
    "source_rows_fingerprint",
    "normalize_source_state_payload",
    "should_skip_static_source_for_structured_migration",
    "read_source_state",
    "write_source_state",
    "circuit_breaker_until",
    "apply_circuit_breaker_exclusions",
    "append_excluded_default_sources",
    "update_source_state_rows",
    "read_previously_successful_sources",
    "read_success_cache",
    "browser_fallback_state_row",
    "build_browser_fallback_circuit_breaker",
    "set_browser_fallback_state",
    "write_success_cache",
]
