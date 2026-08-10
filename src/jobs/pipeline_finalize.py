"""Jobs pipeline finalization helpers.

AI boundary owns: pipeline output finalization, report assembly, lifecycle closeout, and artifact writes.
AI boundary implement in: this file for terminal pipeline artifacts; source execution and runtime writers stay in sibling leaves.
AI boundary search before contracts: fetcher runtime contracts, report contracts, bridge fetch-report routes, and pipeline finalization tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused pipeline finalization tests.
"""

from __future__ import annotations

import json
import threading
import time
from collections import Counter
from collections.abc import Callable
from contextlib import contextmanager
from functools import wraps
from pathlib import Path
from typing import Any

from src.bridge.fetch_report_summary import write_fetch_report_summary_artifact
from src.contracts import SCHEMA_VERSION
from src.core.contracts import validate_canonical_jobs_payload
from src.jobs.availability_identity import (
    IDENTITY_QUARANTINE_ARTIFACT_NAME,
    AvailabilityIdentityPreflightError,
    prepare_availability_identities,
    read_identity_quarantine,
    reconcile_identity_quarantine,
    validate_published_availability_rows,
    write_identity_quarantine,
)
from src.jobs.availability_schedule import build_availability_sweep_plan
from src.jobs.availability_tombstones import (
    TOMBSTONE_ARTIFACT_NAME,
    read_availability_tombstones,
    reconcile_availability_tombstones,
    write_availability_tombstones,
)
from src.jobs.canonicalize import snapshot_sector_quality_audit
from src.jobs.common.config import STRICT_GAME_ONLY_ENABLED
from src.jobs.common.contracts_dedup_review_state import read_dedup_review_state_artifact
from src.jobs.common.contracts_fetch_report import normalize_fetch_report_payload
from src.jobs.common.contracts_provider_coverage import build_provider_coverage_summary
from src.jobs.common.contracts_provider_static_overlap import (
    build_provider_static_overlap_summary,
)
from src.jobs.common.contracts_redundant_static_proposals import (
    build_redundant_static_proposals_summary,
)
from src.jobs.common.contracts_source_health import normalize_source_health_payload
from src.jobs.common.contracts_source_policy_recommendations import (
    build_source_policy_recommendations_artifact,
    read_source_policy_recommendations_artifact,
)
from src.jobs.common.contracts_source_policy_review_state import (
    read_source_policy_review_state_artifact,
)
from src.jobs.common.contracts_static_suppression_policy import (
    refresh_static_suppression_policy_with_current_evidence,
)
from src.jobs.contamination_audit import build_public_text_quality_report
from src.jobs.dedup import CanonicalDeduplicator
from src.jobs.feed_reconciliation_lock import jobs_feed_reconciliation_lock
from src.jobs.models import CanonicalJob
from src.jobs.pipeline_runtime_summary import (
    append_fetch_runtime_event,
    build_detailed_source_rows,
    snapshot_task_rows,
    update_fetch_runtime_phase,
)
from src.jobs.pipeline_timing import build_runtime_timing_summary, percentile_ms
from src.jobs.registry import STUDIO_SOURCE_REGISTRY
from src.jobs.reporting_dedup_evidence import build_dedup_evidence
from src.jobs.reporting_queues import (
    build_browser_fallback_queue,
    build_parser_regression_queue,
    count_site_changed_diagnosed_sources,
    count_site_changed_missing_old_url_sources,
)
from src.jobs.reporting_social import (
    SOCIAL_EXPERIMENT_REVIEW_FILENAME,
    SOCIAL_EXPERIMENT_SAMPLE_SIZE,
    build_social_experiment_review_payload,
    build_social_experiment_review_sample,
    summarize_social_experiment,
)
from src.jobs.reporting_summary import build_pipeline_summary
from src.jobs.state_lifecycle import (
    apply_job_lifecycle_state,
    build_availability_history_payload,
    build_lifecycle_source_evidence,
    lifecycle_archive_state_path,
    lifecycle_state_fingerprint,
    read_job_lifecycle_state,
    write_job_lifecycle_archive_state,
    write_job_lifecycle_state,
)
from src.jobs.state_source_records import derive_source_health_fields
from src.jobs.state_source_state import (
    update_source_state_rows,
    write_source_state,
    write_success_cache,
)
from src.jobs.text_utils import (
    classify_city_filter_rejection,
    clean_text,
    get_city_filter_option_values,
    norm_text,
    sanitize_location_text,
)
from src.pipeline_io import (
    read_existing_output,
    serialize_rows_for_json,
    write_atomic_if_changed,
    write_hot_text_if_changed,
    write_text_if_changed,
)
from src.shared.json_io import existing_json_candidate
from src.shared.json_shapes import as_json_list, json_object_rows
from src.shared.utils import now_iso

from .common import config as common_config
from .common import health as health_module
from .common import sources as common_sources
from .pipeline_run_setup import canonicalize_existing_output_row

OUTPUT_FIELDS = common_config.OUTPUT_FIELDS
LIGHTWEIGHT_OUTPUT_FIELDS = common_config.LIGHTWEIGHT_OUTPUT_FIELDS

_EXPECTED_SOURCE_POLICY_EXPORT_EXCEPTIONS = (OSError, TypeError, ValueError)
_MISSING_COUNTRY_PLACEHOLDERS = {"", "unknown", "n/a", "na", "none", "null"}


def _is_missing_country_placeholder(value: Any) -> bool:
    return norm_text(value) in _MISSING_COUNTRY_PLACEHOLDERS


def _clean_final_location_entry(
    item: dict[str, Any],
) -> tuple[list[dict[str, str]], dict[str, str]]:
    raw_city = clean_text(item.get("city"))
    raw_country = clean_text(item.get("country"))
    city, city_reason = sanitize_location_text(raw_city, field_name="city")
    country, country_reason = ("", "")
    if raw_country and not _is_missing_country_placeholder(raw_country):
        country, country_reason = sanitize_location_text(raw_country, field_name="country")
    city_options = get_city_filter_option_values(city, country) if city else []
    city_filter_reason = classify_city_filter_rejection(city) if city and not city_options else ""
    cleaned_items = [{"city": option, "country": country} for option in city_options]
    if not cleaned_items and country:
        cleaned_items = [{"city": "", "country": country}]
    reasons: dict[str, str] = {}
    if raw_city and (not city_options or city_options != [city]):
        reasons["city"] = city_reason or city_filter_reason or "split_compound_city"
    if raw_country and raw_country != country and not _is_missing_country_placeholder(raw_country):
        reasons["country"] = country_reason or "cleaned_country"
    return cleaned_items, reasons


def _location_summary_from_clean_entries(entries: list[dict[str, str]]) -> str:
    return " | ".join(
        ", ".join(
            part for part in [clean_text(item.get("city")), clean_text(item.get("country"))] if part
        )
        for item in entries
        if clean_text(item.get("city")) or clean_text(item.get("country"))
    )


def _is_high_confidence_summary_rejection(reason: str) -> bool:
    return reason in {
        "known_non_city",
        "prose_or_navigation",
        "css_fragment",
        "time_fragment",
    }


def _append_location_guardrail_example(
    examples: list[dict[str, Any]],
    row: dict[str, Any],
    *,
    field: str,
    reason: str,
    value: Any,
) -> None:
    if len(examples) >= 20:
        return
    examples.append(
        {
            "company": clean_text(row.get("company")),
            "title": clean_text(row.get("title")),
            "source": clean_text(row.get("source")),
            "jobLink": clean_text(row.get("jobLink")),
            "field": field,
            "reason": reason,
            "value": clean_text(value),
        }
    )


def _apply_final_locations_list_guardrail(
    row: dict[str, Any],
    raw_locations: list[Any],
    *,
    field_counts: Counter[str],
    reason_counts: Counter[str],
    examples: list[dict[str, Any]],
) -> None:
    cleaned_locations: list[dict[str, str]] = []
    seen_locations: set[str] = set()
    for item in raw_locations:
        if not isinstance(item, dict):
            continue
        cleaned_items, item_reasons = _clean_final_location_entry(item)
        for nested_field, reason in item_reasons.items():
            field_name = f"locations.{nested_field}"
            field_counts[field_name] += 1
            reason_counts[reason] += 1
            _append_location_guardrail_example(
                examples,
                row,
                field=field_name,
                reason=reason,
                value=item.get(nested_field),
            )
        for cleaned_item in cleaned_items:
            key = "|".join(
                [norm_text(cleaned_item.get("city")), norm_text(cleaned_item.get("country"))]
            )
            if key in seen_locations:
                continue
            seen_locations.add(key)
            cleaned_locations.append(cleaned_item)
    if cleaned_locations != raw_locations:
        row["locations"] = cleaned_locations
    rebuilt_summary = _location_summary_from_clean_entries(cleaned_locations)
    if clean_text(row.get("locationSummary")) == rebuilt_summary:
        return
    if clean_text(row.get("locationSummary")):
        field_counts["locationSummary"] += 1
        reason_counts["rebuilt_from_clean_locations"] += 1
    row["locationSummary"] = rebuilt_summary


def _apply_final_location_scalar_guardrail(
    row: dict[str, Any],
    *,
    field_counts: Counter[str],
    reason_counts: Counter[str],
    examples: list[dict[str, Any]],
) -> None:
    for field_name in ("city", "country"):
        if field_name == "country" and _is_missing_country_placeholder(row.get(field_name)):
            continue
        value, reason = sanitize_location_text(row.get(field_name), field_name=field_name)
        if field_name == "city" and value:
            filter_reason = classify_city_filter_rejection(value)
            if filter_reason:
                value = ""
                reason = filter_reason
        if not reason:
            continue
        _append_location_guardrail_example(
            examples,
            row,
            field=field_name,
            reason=reason,
            value=row.get(field_name),
        )
        row[field_name] = value
        field_counts[field_name] += 1
        reason_counts[reason] += 1


def _apply_final_location_summary_guardrail(
    row: dict[str, Any],
    *,
    field_counts: Counter[str],
    reason_counts: Counter[str],
    examples: list[dict[str, Any]],
) -> None:
    summary_reason = classify_city_filter_rejection(row.get("locationSummary"))
    if not _is_high_confidence_summary_rejection(summary_reason):
        return
    replacement_summary = ", ".join(
        part
        for part in [clean_text(row.get("city")), clean_text(row.get("country"))]
        if part and not _is_missing_country_placeholder(part)
    )
    if clean_text(row.get("locationSummary")) == replacement_summary:
        return
    _append_location_guardrail_example(
        examples,
        row,
        field="locationSummary",
        reason=summary_reason,
        value=row.get("locationSummary"),
    )
    row["locationSummary"] = replacement_summary
    field_counts["locationSummary"] += 1
    reason_counts[summary_reason] += 1


def _apply_final_location_quality_guardrail(rows: list[dict[str, Any]]) -> dict[str, Any]:
    field_counts: Counter[str] = Counter()
    reason_counts: Counter[str] = Counter()
    examples: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        raw_locations = row.get("locations")
        if isinstance(raw_locations, list):
            _apply_final_locations_list_guardrail(
                row,
                raw_locations,
                field_counts=field_counts,
                reason_counts=reason_counts,
                examples=examples,
            )
        _apply_final_location_scalar_guardrail(
            row,
            field_counts=field_counts,
            reason_counts=reason_counts,
            examples=examples,
        )
        if not isinstance(raw_locations, list):
            _apply_final_location_summary_guardrail(
                row,
                field_counts=field_counts,
                reason_counts=reason_counts,
                examples=examples,
            )
    return {
        "totalRows": len(rows),
        "invalidLocationFieldCount": int(sum(field_counts.values())),
        "fieldCounts": dict(field_counts),
        "reasonCounts": dict(reason_counts),
        "examples": examples,
    }


def _runtime_timing_summary(
    source_reports: list[dict[str, Any]], *, wall_clock_duration_ms: int
) -> dict[str, Any]:
    return build_runtime_timing_summary(
        source_reports,
        wall_clock_duration_ms=wall_clock_duration_ms,
        clean_text_fn=clean_text,
        norm_text_fn=norm_text,
        percentile_ms_fn=percentile_ms,
    )


def _deduplicate_or_preserve_previous(
    *,
    paths,
    canonical_rows: list[CanonicalJob],
    preserve_previous_on_empty: bool,
    started_at: str,
) -> tuple[list[CanonicalJob], dict[str, Any], bool]:
    deduplicator = CanonicalDeduplicator()
    deduped_rows = deduplicator.process(canonical_rows)
    preserved_previous = False
    if preserve_previous_on_empty and not deduped_rows:
        previous_rows = read_existing_output(
            paths.json_path,
            started_at,
            canonicalize_job=canonicalize_existing_output_row,
            clean_text=clean_text,
            canonical_job_cls=CanonicalJob,
        )
        if previous_rows:
            deduped_rows = list(previous_rows)  # already CanonicalJob
            preserved_previous = True
    return deduped_rows, deduplicator.stats, preserved_previous


def _lifecycle_missing_context(
    *,
    source_reports: list[dict[str, Any]],
    selected_loaders: list[tuple[str, Any]],
    using_default_loaders: bool,
    effective_seed_from_existing_output: bool,
) -> dict[str, Any]:
    selected_loader_names = {name for name, _ in selected_loaders}
    # Existing-output seeding is a cache/dedup implementation detail. It must not
    # suppress trustworthy per-source missing evidence.
    may_mark_missing = using_default_loaders
    return build_lifecycle_source_evidence(
        source_reports,
        selected_source_names=selected_loader_names,
        allow_missing=may_mark_missing,
    )


def _apply_lifecycle_state(
    *,
    deduped_rows: list[CanonicalJob],
    observed_rows: list[CanonicalJob],
    lifecycle_rows: dict[str, dict[str, Any]],
    source_reports: list[dict[str, Any]],
    selected_loaders: list[tuple[str, Any]],
    using_default_loaders: bool,
    effective_seed_from_existing_output: bool,
    lifecycle_finished_at: str,
) -> tuple[
    list[CanonicalJob],
    dict[str, dict[str, Any]],
    dict[int, dict[str, dict[str, Any]]],
    dict[str, int],
]:
    source_evidence = _lifecycle_missing_context(
        source_reports=source_reports,
        selected_loaders=selected_loaders,
        using_default_loaders=using_default_loaders,
        effective_seed_from_existing_output=effective_seed_from_existing_output,
    )
    return apply_job_lifecycle_state(
        deduped_rows=deduped_rows,
        observed_rows=observed_rows,
        lifecycle_rows=lifecycle_rows,
        finished_at=lifecycle_finished_at,
        allow_mark_missing=False,
        eligible_missing_sources=source_evidence.get("eligibleMissingSources", set()),
        source_evidence=source_evidence,
    )


def _write_lifecycle_archive_rows(
    *,
    lifecycle_state_path: Path,
    archive_rows_by_year: dict[int, dict[str, dict[str, Any]]],
) -> None:
    for archive_year, rows in archive_rows_by_year.items():
        if not rows:
            continue
        archive_path = lifecycle_archive_state_path(lifecycle_state_path, archive_year)
        write_job_lifecycle_archive_state(archive_path, rows)


def _lifecycle_summary_payload(lifecycle_counts_map: dict[str, int]) -> dict[str, int]:
    return {
        "activeCount": int(lifecycle_counts_map.get("active") or 0),
        "newCount": int(lifecycle_counts_map.get("new") or 0),
        "carriedInitializedCount": int(lifecycle_counts_map.get("carriedInitialized") or 0),
        "reappearedCount": int(lifecycle_counts_map.get("reappeared") or 0),
        "likelyRemovedCount": int(lifecycle_counts_map.get("likelyRemoved") or 0),
        "archivedCount": int(lifecycle_counts_map.get("archived") or 0),
        "preservedBecauseSourceFailedCount": int(
            lifecycle_counts_map.get("preservedBecauseSourceFailed") or 0
        ),
        "preservedBecauseSourceSkippedCount": int(
            lifecycle_counts_map.get("preservedBecauseSourceSkipped") or 0
        ),
        "eligibleMissingSourceCount": int(
            lifecycle_counts_map.get("eligibleMissingSourceCount") or 0
        ),
        "ineligibleMissingSourceCount": int(
            lifecycle_counts_map.get("ineligibleMissingSourceCount") or 0
        ),
        "availabilityAvailableCount": int(lifecycle_counts_map.get("availabilityAvailable") or 0),
        "availabilityOverdueCount": int(lifecycle_counts_map.get("availabilityOverdue") or 0),
        "availabilityUnavailableCount": int(
            lifecycle_counts_map.get("availabilityUnavailable") or 0
        ),
    }


@contextmanager
def _finalization_phase(
    *,
    key: str,
    label: str,
    progress_phase: dict[str, str],
    task_runtime: Any,
    write_progress_report: Any,
    write_task_state: Any,
    timings: dict[str, int],
):
    progress_phase["key"] = key
    progress_phase["label"] = label
    try:
        task_runtime.finalization_phase_key = key
    except (AttributeError, TypeError):
        pass
    if hasattr(task_runtime, "task_lock"):
        update_fetch_runtime_phase(task_runtime, phase_key=key, phase_label=label)
    write_task_state(finished_at="", force=True)
    write_progress_report(force=True)
    stop = threading.Event()

    def heartbeat() -> None:
        while not stop.wait(30.0):
            write_task_state(finished_at="", force=False)
            write_progress_report(force=False)

    thread = threading.Thread(
        target=heartbeat,
        daemon=True,
        name=f"pipeline-finalize-{key}",
    )
    started = time.perf_counter()
    thread.start()
    try:
        yield
    finally:
        stop.set()
        thread.join(timeout=1.0)
        timings[f"{key}Ms"] = max(0, int((time.perf_counter() - started) * 1000))
        try:
            task_runtime.finalization_timings = dict(timings)
        except (AttributeError, TypeError):
            pass


def _merge_source_health_report_payload(
    report_payload: dict[str, Any], source_state_rows: dict[str, Any]
) -> None:
    source_health_rows_by_name = {
        clean_text(name): dict(row)
        for name, row in source_state_rows.items()
        if clean_text(name) and isinstance(row, dict)
    }
    merged_source_rows: list[dict[str, Any]] = []
    for source_row in report_payload.get("sources") or []:
        if not isinstance(source_row, dict):
            continue
        merged_row = dict(source_row)
        source_name = clean_text(source_row.get("name"))
        if source_name and source_name in source_health_rows_by_name:
            merged_row.update(derive_source_health_fields(source_health_rows_by_name[source_name]))
        merged_source_rows.append(merged_row)
    report_payload["sources"] = merged_source_rows
    report_payload["sourceHealth"] = normalize_source_health_payload(
        report_payload.get("sourceHealth"), merged_source_rows
    )


def _record_sector_gate_loss(
    source_reports: list[dict[str, Any]],
    non_game_by_source: Counter[str],
) -> None:
    for report in source_reports:
        if not isinstance(report, dict):
            continue
        loss = report.get("loss")
        if not isinstance(loss, dict):
            continue
        source_name = clean_text(report.get("name"))
        source_dropped = non_game_by_source.get(source_name, 0)
        if source_dropped:
            drop_reasons = loss.setdefault("canonicalDropReasons", {})
            drop_reasons["sector_gate_filtered"] = (
                int(drop_reasons.get("sector_gate_filtered") or 0) + source_dropped
            )


def _apply_sector_gate(
    deduped_payload_rows: list[dict[str, Any]],
    source_reports: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], int]:
    if not STRICT_GAME_ONLY_ENABLED:
        return deduped_payload_rows, 0
    game_rows: list[dict[str, Any]] = []
    non_game_by_source: Counter[str] = Counter()
    for row in deduped_payload_rows:
        if not isinstance(row, dict):
            continue
        if row.get("sector") == "Game":
            game_rows.append(row)
        else:
            source = clean_text(row.get("source"))
            if source:
                non_game_by_source[source] += 1
    dropped = len(deduped_payload_rows) - len(game_rows)
    if dropped:
        _record_sector_gate_loss(source_reports, non_game_by_source)
    return game_rows, dropped


def _quality_reports(deduped_payload_rows: list[dict[str, Any]]) -> tuple[dict[str, Any], ...]:
    location_quality_audit = _apply_final_location_quality_guardrail(deduped_payload_rows)
    sector_quality_audit = snapshot_sector_quality_audit(total_rows=len(deduped_payload_rows))
    contamination_report = build_public_text_quality_report(deduped_payload_rows)
    contamination_rows = int(contamination_report.get("contaminatedRows") or 0)
    if contamination_rows > 0:
        raise ValueError(
            "Public text contamination validation failed: "
            f"{contamination_rows} row(s) still contain HTML-like fragments"
        )
    city_garbage_audit = (
        contamination_report.get("cityGarbageAudit")
        if isinstance(contamination_report.get("cityGarbageAudit"), dict)
        else {}
    )
    return (
        location_quality_audit,
        sector_quality_audit,
        contamination_report,
        city_garbage_audit,
    )


def _apply_final_output_loss_counts(
    source_reports: list[dict[str, Any]],
    deduped_payload_rows: list[dict[str, Any]],
) -> None:
    final_output_by_source: Counter[str] = Counter(
        clean_text(row.get("source"))
        for row in deduped_payload_rows
        if clean_text(row.get("source"))
    )
    for report in source_reports:
        if not isinstance(report, dict):
            continue
        loss = report.get("loss")
        if not isinstance(loss, dict):
            continue
        source_name = clean_text(report.get("name"))
        canonical_kept = int(loss.get("canonicalKept") or report.get("keptCount") or 0)
        final_output = int(final_output_by_source.get(source_name, 0))
        loss["finalOutput"] = max(0, final_output)
        loss["dedupMerged"] = max(0, canonical_kept - final_output)


def _write_output_rows(paths, deduped_payload_rows: list[dict[str, Any]]) -> tuple[bool, bool]:
    if deduped_payload_rows:
        validate_canonical_jobs_payload(deduped_payload_rows)
    wrote_json = write_atomic_if_changed(
        paths.json_path,
        serialize_rows_for_json(deduped_payload_rows, OUTPUT_FIELDS),
    )
    wrote_light_json = write_atomic_if_changed(
        paths.light_json_path,
        serialize_rows_for_json(deduped_payload_rows, LIGHTWEIGHT_OUTPUT_FIELDS),
    )
    if hasattr(paths, "startup_json_path"):
        write_atomic_if_changed(
            paths.startup_json_path,
            serialize_rows_for_json(deduped_payload_rows[:10], LIGHTWEIGHT_OUTPUT_FIELDS),
        )
    return wrote_json, wrote_light_json


def _write_review_queue_artifacts(
    *,
    paths,
    source_reports: list[dict[str, Any]],
    lifecycle_finished_at: str,
    redirect_resolver: Any,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    browser_fallback_queue_rows = build_browser_fallback_queue(
        source_reports, generated_at=lifecycle_finished_at
    )
    write_text_if_changed(
        paths.browser_fallback_queue_path,
        json.dumps(browser_fallback_queue_rows, indent=2, ensure_ascii=False),
    )
    parser_regression_queue_rows = build_parser_regression_queue(
        source_reports,
        generated_at=lifecycle_finished_at,
        resolve_redirect_url=getattr(redirect_resolver, "resolve", None),
    )
    write_text_if_changed(
        paths.parser_regression_queue_path,
        json.dumps(parser_regression_queue_rows, indent=2, ensure_ascii=False),
    )
    return browser_fallback_queue_rows, parser_regression_queue_rows


def _load_social_review_rows(social_review_path) -> list[Any]:
    if not social_review_path.exists():
        return []
    try:
        loaded_review = json.loads(social_review_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    if not isinstance(loaded_review, dict):
        return []
    return as_json_list(loaded_review.get("rows"))


def _merge_social_review_rows(
    deduped_rows: list[CanonicalJob],
    *,
    existing_review_rows: list[Any],
) -> list[dict[str, Any]]:
    review_rows_by_key = {
        clean_text(row.get("dedupKey")): row
        for row in existing_review_rows
        if isinstance(row, dict) and clean_text(row.get("dedupKey"))
    }
    merged_rows: list[dict[str, Any]] = []
    for candidate in build_social_experiment_review_sample(
        deduped_rows,
        sample_size=SOCIAL_EXPERIMENT_SAMPLE_SIZE,
    ):
        key = clean_text(candidate.get("dedupKey"))
        merged_candidate = dict(candidate)
        previous = review_rows_by_key.get(key) or {}
        decision = clean_text(previous.get("reviewDecision"))
        notes = clean_text(previous.get("reviewNotes"))
        if decision:
            merged_candidate["reviewDecision"] = decision
        if notes:
            merged_candidate["reviewNotes"] = notes
        merged_rows.append(merged_candidate)
    return merged_rows


def _write_social_review_artifact(
    *,
    paths,
    deduped_rows: list[CanonicalJob],
    lifecycle_finished_at: str,
    started_at: str,
) -> tuple[dict[str, Any], Any]:
    social_review_path = paths.output_dir / SOCIAL_EXPERIMENT_REVIEW_FILENAME
    social_review_payload = build_social_experiment_review_payload(
        _merge_social_review_rows(
            deduped_rows,
            existing_review_rows=_load_social_review_rows(social_review_path),
        ),
        generated_at=lifecycle_finished_at,
        pilot_window_start_at=started_at,
        pilot_window_end_at=lifecycle_finished_at,
        review_artifact_path=str(social_review_path),
    )
    write_atomic_if_changed(
        social_review_path,
        json.dumps(social_review_payload, indent=2, ensure_ascii=False),
    )
    return social_review_payload, social_review_path


def _update_runtime_timing_payload(
    *,
    runtime_payload: dict[str, Any],
    task_runtime: Any,
    source_reports: list[dict[str, Any]],
    run_started_mono: float,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    detailed_source_rows = build_detailed_source_rows(task_runtime.task_rows, source_reports)
    timing_summary = _runtime_timing_summary(
        detailed_source_rows,
        wall_clock_duration_ms=int((time.perf_counter() - run_started_mono) * 1000),
    )
    runtime_payload["slowestSources"] = list(timing_summary.get("slowestSources") or [])
    runtime_payload["staticDomainGateWaitMs"] = int(
        timing_summary.get("staticDomainGateWaitMs") or 0
    )
    runtime_payload["staticDetailBatchCount"] = int(
        timing_summary.get("staticDetailBatchCount") or 0
    )
    runtime_payload["staticAdaptiveStops"] = int(timing_summary.get("staticAdaptiveStops") or 0)
    runtime_payload["staticListingTimeoutStops"] = int(
        timing_summary.get("staticListingTimeoutStops") or 0
    )
    runtime_payload["staticListingBrowserFallbacks"] = int(
        timing_summary.get("staticListingBrowserFallbacks") or 0
    )
    runtime_payload["timingSummary"] = {
        "totalDurationMs": int(timing_summary.get("totalDurationMs") or 0),
        "wallClockDurationMs": int(timing_summary.get("wallClockDurationMs") or 0),
        "medianSourceDurationMs": int(timing_summary.get("medianSourceDurationMs") or 0),
        "p95SourceDurationMs": int(timing_summary.get("p95SourceDurationMs") or 0),
        "stageTotalsMs": dict(timing_summary.get("stageTotalsMs") or {}),
        "stageTop": list(timing_summary.get("stageTop") or []),
        "adapterTimings": list(timing_summary.get("adapterTimings") or []),
        "slowestAdapters": list(timing_summary.get("slowestAdapters") or []),
        "highCostLowYieldSources": list(timing_summary.get("highCostLowYieldSources") or []),
        "detailHeavySources": list(timing_summary.get("detailHeavySources") or []),
    }
    return detailed_source_rows, timing_summary


def _output_sizes(paths) -> tuple[int, int]:
    return (
        paths.json_path.stat().st_size if paths.json_path.exists() else 0,
        paths.light_json_path.stat().st_size if paths.light_json_path.exists() else 0,
    )


def _is_operational_excluded_row(row: dict[str, Any]) -> bool:
    if norm_text(row.get("status")) != "excluded":
        return False
    reason = clean_text(row.get("exclusionReason"))
    return reason != "only_sources_filter" and not reason.startswith("disabled_by_default:")


def _final_source_rows(
    detailed_source_rows: list[dict[str, Any]],
    source_reports: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in [item for item in detailed_source_rows if isinstance(item, dict)]:
        name = clean_text(row.get("name"))
        if not name:
            continue
        rows.append(row)
        seen.add(name)
    for row in [item for item in source_reports if isinstance(item, dict)]:
        name = clean_text(row.get("name"))
        if not name or name in seen:
            continue
        if not _is_operational_excluded_row(row):
            continue
        rows.append(dict(row))
        seen.add(name)
    return rows


def _completed_task_progress(summary: dict[str, Any]) -> dict[str, Any]:
    source_count = max(0, int(summary.get("sourceCount") or 0))
    failed_sources = max(0, int(summary.get("failedSources") or 0))
    excluded_sources = max(0, int(summary.get("excludedSources") or 0))
    successful_sources = max(0, int(summary.get("successfulSources") or 0))
    resolved_sources = successful_sources + failed_sources + excluded_sources
    output_count = max(0, int(summary.get("outputCount") or 0))
    return {
        "active": False,
        "phaseKey": "completed",
        "phaseLabel": "Completed",
        "mode": "determinate",
        "ratio": 1.0,
        "counts": {
            "sourceCount": source_count,
            "totalTasks": source_count,
            "queuedTasks": 0,
            "runningTasks": 0,
            "completedTasks": resolved_sources,
            "resolvedSources": resolved_sources,
            "outputCount": output_count,
            "failedSources": failed_sources,
            "excludedSources": excluded_sources,
        },
    }


def _export_source_policy_recommendations(
    *,
    report_payload: dict[str, Any],
    source_policy_recommendations_path: Path,
    source_policy_review_state_path: Path,
    finished_at: str,
) -> None:
    source_policy_recommendation_warning = ""
    source_policy_review_state_warning = ""
    updated_recommendation_pair_count = len(
        json_object_rows(report_payload["redundantStaticProposals"].get("proposals"))
    )
    try:
        prior_recommendations, source_policy_recommendation_warning = (
            read_source_policy_recommendations_artifact(source_policy_recommendations_path)
        )
        source_policy_review_state, source_policy_review_state_warning = (
            read_source_policy_review_state_artifact(source_policy_review_state_path)
        )
        source_policy_recommendations = build_source_policy_recommendations_artifact(
            prior_artifact=prior_recommendations,
            redundant_static_proposals=report_payload["redundantStaticProposals"],
            observed_at=finished_at,
            review_state=source_policy_review_state,
        )
        write_atomic_if_changed(
            source_policy_recommendations_path,
            json.dumps(source_policy_recommendations, indent=2, ensure_ascii=False),
        )
        review_summary = source_policy_review_state.get("summary", {})
        report_payload["sourcePolicyRecommendationExport"] = {
            "status": "ok",
            "artifactPath": str(source_policy_recommendations_path),
            "reviewStatePath": str(source_policy_review_state_path),
            "updatedPairCount": updated_recommendation_pair_count,
            "reviewStatePairCount": int(review_summary.get("totalPairs") or 0),
            "manualForcePausedCount": int(review_summary.get("forcePausedCount") or 0),
            **(
                {"warning": source_policy_recommendation_warning}
                if source_policy_recommendation_warning
                else {}
            ),
            **(
                {"reviewStateWarning": source_policy_review_state_warning}
                if source_policy_review_state_warning
                else {}
            ),
        }
    except _EXPECTED_SOURCE_POLICY_EXPORT_EXCEPTIONS as exc:
        report_payload["sourcePolicyRecommendationExport"] = {
            "status": "warning",
            "artifactPath": str(source_policy_recommendations_path),
            "reviewStatePath": str(source_policy_review_state_path),
            "updatedPairCount": 0,
            "reviewStatePairCount": 0,
            "manualForcePausedCount": 0,
            "warning": f"source_policy_recommendation_export_failed:{type(exc).__name__}",
        }


def _write_availability_artifacts(
    *, paths: Any, lifecycle_rows: dict[str, dict[str, Any]], finished_at: str
) -> dict[str, Any]:
    history = build_availability_history_payload(lifecycle_rows, finished_at=finished_at)
    wrote_history = write_atomic_if_changed(
        paths.availability_history_path,
        json.dumps(history, ensure_ascii=False, separators=(",", ":")),
    )
    priority_manifest: dict[str, Any] = {}
    priority_path = paths.output_dir / "jobs-availability-priority.json"
    if priority_path.exists():
        try:
            loaded = json.loads(priority_path.read_text(encoding="utf-8"))
            priority_manifest = loaded if isinstance(loaded, dict) else {}
        except (OSError, json.JSONDecodeError):
            pass
    shadow_payload: dict[str, Any] = {}
    shadow_path = paths.output_dir / "jobs-availability-shadow-results.json"
    if shadow_path.exists():
        try:
            loaded = json.loads(shadow_path.read_text(encoding="utf-8"))
            shadow_payload = loaded if isinstance(loaded, dict) else {}
        except (OSError, json.JSONDecodeError):
            pass
    direct_checkpoints: dict[str, Any] = {}
    checkpoint_path = paths.output_dir / "jobs-availability-direct-checkpoints.json"
    if checkpoint_path.exists():
        try:
            loaded = json.loads(checkpoint_path.read_text(encoding="utf-8"))
            direct_checkpoints = loaded if isinstance(loaded, dict) else {}
        except (OSError, json.JSONDecodeError):
            pass
    sweep = build_availability_sweep_plan(
        lifecycle_rows,
        priority_manifest,
        finished_at=finished_at,
        direct_checkpoints=direct_checkpoints,
    )
    wrote_sweep = write_atomic_if_changed(
        paths.availability_sweep_plan_path,
        json.dumps(sweep, ensure_ascii=False, separators=(",", ":")),
    )
    shadow_rows = [row for row in shadow_payload.get("rows") or [] if isinstance(row, dict)]
    counts = dict(Counter(clean_text(row.get("kind")) or "unknown" for row in shadow_rows))
    lifecycle_by_id = {
        clean_text(entry.get("availabilityId")): entry
        for entry in lifecycle_rows.values()
        if clean_text(entry.get("availabilityId"))
    }
    conflicts = []
    for row in shadow_rows[-200:]:
        availability_id = clean_text(row.get("availabilityId"))
        source_status = clean_text(
            (lifecycle_by_id.get(availability_id) or {}).get("availabilityStatus") or "available"
        )
        direct_kind = clean_text(row.get("kind"))
        if (direct_kind, source_status) in {
            ("direct_live", "unavailable"),
            ("direct_closed", "available"),
        }:
            conflicts.append(
                {
                    "availabilityId": availability_id,
                    "sourceStatus": source_status,
                    "directKind": direct_kind,
                    "checkedAt": clean_text(row.get("checkedAt")),
                }
            )
    return {
        "sweep": sweep,
        "conflicts": conflicts,
        "shadowCounts": counts,
        "wroteHistory": wrote_history,
        "wroteSweep": wrote_sweep,
    }


def _merge_concurrent_direct_live_rows(
    canonical_rows: list[CanonicalJob],
    current_rows: list[Any],
    lifecycle_rows: dict[str, dict[str, Any]],
) -> list[CanonicalJob]:
    merged = list(canonical_rows)
    known_availability_ids = {
        clean_text(row.availabilityId) for row in merged if clean_text(row.availabilityId)
    }
    lifecycle_by_id = {
        clean_text(entry.get("availabilityId")): entry
        for entry in lifecycle_rows.values()
        if isinstance(entry, dict) and clean_text(entry.get("availabilityId"))
    }
    for row in current_rows:
        availability_id = clean_text(
            row.availabilityId if isinstance(row, CanonicalJob) else row.get("availabilityId")
        )
        entry = lifecycle_by_id.get(availability_id) or {}
        evidence = entry.get("availabilityEvidence")
        direct_live = (
            clean_text(entry.get("availabilityStatus")) == "available"
            and isinstance(evidence, dict)
            and clean_text(evidence.get("kind")) == "direct_live"
            and clean_text(evidence.get("confidence")) == "definitive"
        )
        if availability_id and availability_id not in known_availability_ids and direct_live:
            if isinstance(row, CanonicalJob):
                merged.append(row)
            else:
                merged.append(CanonicalJob.from_mapping(row))
            known_availability_ids.add(availability_id)
    return merged


def _direct_live_row_predicate(
    lifecycle_rows: dict[str, dict[str, Any]],
) -> Callable[[dict[str, Any]], bool]:
    """Build a filter that only accepts rows matching the direct-live merge test.

    Lets `_serialize_jobs_feed_reconciliation` skip CanonicalJob conversion for
    the 40k+ pre-existing jobs that aren't direct-live — that conversion alone
    was the single biggest memory allocation during finalize.
    """
    lifecycle_by_id = {
        clean_text(entry.get("availabilityId")): entry
        for entry in lifecycle_rows.values()
        if isinstance(entry, dict) and clean_text(entry.get("availabilityId"))
    }

    def _matches(row: dict[str, Any]) -> bool:
        availability_id = clean_text(row.get("availabilityId"))
        if not availability_id:
            return False
        entry = lifecycle_by_id.get(availability_id) or {}
        evidence = entry.get("availabilityEvidence")
        return (
            clean_text(entry.get("availabilityStatus")) == "available"
            and isinstance(evidence, dict)
            and clean_text(evidence.get("kind")) == "direct_live"
            and clean_text(evidence.get("confidence")) == "definitive"
        )

    return _matches


def _serialize_jobs_feed_reconciliation(func):
    @wraps(func)
    def wrapped(*args, **kwargs):
        paths = kwargs.get("paths")
        if paths is None or not hasattr(paths, "output_dir"):
            raise TypeError("finalize_pipeline_run requires paths with an output_dir")
        with jobs_feed_reconciliation_lock(paths.output_dir):
            if existing_json_candidate(paths.lifecycle_state_path) is not None:
                kwargs = dict(kwargs)
                # ponytail: skip the 300+ MB re-parse of jobs-lifecycle-state.json
                # when the file hasn't changed since setup loaded it. mtime+size
                # is the cheapest change signal that still catches an external
                # writer between setup and finalize.
                fingerprint_at_setup = kwargs.get("lifecycle_state_fingerprint")
                current_fingerprint = lifecycle_state_fingerprint(paths.lifecycle_state_path)
                if fingerprint_at_setup is not None and current_fingerprint == fingerprint_at_setup:
                    latest_lifecycle = kwargs.get("lifecycle_rows") or {}
                else:
                    latest_lifecycle = read_job_lifecycle_state(paths.lifecycle_state_path)
                kwargs["lifecycle_rows"] = latest_lifecycle
                # ponytail: only convert direct-live rows to CanonicalJob; skip
                # the 40k+ pre-existing rows that will be filtered out anyway.
                current_rows = read_existing_output(
                    paths.json_path,
                    clean_text(kwargs.get("started_at")) or now_iso(),
                    canonicalize_job=canonicalize_existing_output_row,
                    clean_text=clean_text,
                    canonical_job_cls=CanonicalJob,
                    row_predicate=_direct_live_row_predicate(latest_lifecycle),
                )
                kwargs["canonical_rows"] = _merge_concurrent_direct_live_rows(
                    list(kwargs.get("canonical_rows") or []), current_rows, latest_lifecycle
                )
            return func(*args, **kwargs)

    return wrapped


def _bounded_identity_failure_summary(exc: BaseException) -> tuple[str, str, dict[str, Any]]:
    if isinstance(exc, AvailabilityIdentityPreflightError):
        allowed_counts = {
            clean_text(key): max(0, int(value or 0))
            for key, value in exc.summary.items()
            if clean_text(key).endswith("Count") and isinstance(value, (int, float))
        }
        reason_counts = {
            clean_text(key): max(0, int(value or 0))
            for key, value in (
                exc.summary.get("rejectionReasonCounts")
                if isinstance(exc.summary.get("rejectionReasonCounts"), dict)
                else {}
            ).items()
            if clean_text(key) and isinstance(value, (int, float))
        }
        if reason_counts:
            allowed_counts["rejectionReasonCounts"] = dict(list(sorted(reason_counts.items()))[:16])
        return exc.error_code, exc.reason, allowed_counts
    return "pipeline_finalization_failed", type(exc).__name__, {}


def write_failed_pipeline_report(
    *,
    paths: Any,
    source_reports: list[dict[str, Any]],
    canonical_rows: list[CanonicalJob],
    runtime_payload: dict[str, Any],
    task_runtime: Any,
    write_task_state: Any,
    started_at: str,
    run_id: str,
    error: BaseException,
) -> dict[str, Any]:
    """Persist a bounded terminal failure without mutating feed authority."""

    finished_at = now_iso()
    error_code, error_reason, identity_summary = _bounded_identity_failure_summary(error)
    finalization_timings = dict(getattr(task_runtime, "finalization_timings", {}) or {})
    source_rows = _final_source_rows(
        build_detailed_source_rows(task_runtime.task_rows, source_reports),
        source_reports,
    )
    successful_sources = sum(
        1 for row in source_rows if clean_text(row.get("status")).lower() == "ok"
    )
    failed_sources = sum(
        1 for row in source_rows if clean_text(row.get("status")).lower() == "error"
    )
    excluded_sources = sum(
        1 for row in source_rows if clean_text(row.get("status")).lower() == "excluded"
    )
    summary = {
        "sourceCount": len(source_rows),
        "successfulSources": successful_sources,
        "failedSources": failed_sources,
        "excludedSources": excluded_sources,
        "candidateCount": len(canonical_rows),
        "outputCount": 0,
        "publishedOutputUnchanged": (
            clean_text(getattr(task_runtime, "finalization_phase_key", "")) != "writing_outputs"
        ),
        "error": error_code,
        "errorCode": error_code,
        "errorReason": error_reason,
    }
    update_fetch_runtime_phase(
        task_runtime,
        phase_key="failed",
        phase_label="Finalization failed",
    )
    append_fetch_runtime_event(
        task_runtime,
        level="error",
        message=f"Finalization failed: {error_code}",
        phase_key="failed",
    )
    write_task_state(
        finished_at=finished_at,
        force=True,
        terminal_error_code=error_code,
        terminal_summary=summary,
    )
    report_payload = normalize_fetch_report_payload(
        {
            "schemaVersion": SCHEMA_VERSION,
            "taskType": "fetch",
            "status": "error",
            "active": False,
            "runId": run_id,
            "startedAt": started_at,
            "finishedAt": finished_at,
            "runtime": {
                **dict(runtime_payload),
                "finalizationTiming": finalization_timings,
                "lifecycle": {
                    "owner": "fetch_report",
                    "heartbeatAt": finished_at,
                },
            },
            "summary": summary,
            "taskProgress": {
                "active": False,
                "phaseKey": "failed",
                "phaseLabel": "Failed",
                "mode": "determinate",
                "ratio": 1.0,
                "counts": {
                    "sourceCount": len(source_rows),
                    "resolvedSources": successful_sources + failed_sources + excluded_sources,
                    "outputCount": 0,
                    "failedSources": failed_sources,
                    "excludedSources": excluded_sources,
                    "errorCode": error_code,
                },
            },
            "workItems": snapshot_task_rows(task_runtime.task_rows),
            "recentEvents": list(task_runtime.recent_events),
            "availabilitySummary": identity_summary,
            "availabilityHealth": {
                "status": "failed",
                "degradedCoverage": True,
                "shadowClassifier": True,
                "identity": identity_summary,
            },
            "sources": source_rows,
            "sourceFamilies": source_reports,
            "outputs": {
                "json": str(paths.json_path),
                "lightJson": str(paths.light_json_path),
                "report": str(paths.report_path),
                "lifecycleState": str(paths.lifecycle_state_path),
                "changed": {"json": False, "lightJson": False},
            },
        }
    )
    write_hot_text_if_changed(
        paths.report_path, json.dumps(report_payload, indent=2, ensure_ascii=False)
    )
    write_fetch_report_summary_artifact(
        paths.report_path,
        report_payload,
        write_text_if_changed=write_hot_text_if_changed,
        include_sources=True,
    )
    return report_payload


@_serialize_jobs_feed_reconciliation
def finalize_pipeline_run(
    *,
    paths,
    source_reports: list[dict[str, Any]],
    canonical_rows: list[CanonicalJob],
    using_default_loaders: bool,
    selected_loaders: list[tuple[str, Any]],
    effective_seed_from_existing_output: bool,
    preserve_previous_on_empty: bool,
    source_state_rows: dict[str, Any],
    lifecycle_rows: dict[str, dict[str, Any]],
    runtime_payload: dict[str, Any],
    redirect_resolver: Any,
    task_runtime: Any,
    progress_phase: dict[str, str],
    write_progress_report,
    write_task_state,
    started_at: str,
    run_started_mono: float,
    run_id: str,
    circuit_breaker_failures: int,
    circuit_breaker_cooldown_minutes: int,
    circuit_breaker_zero_kept: int,
    observed_rows: list[CanonicalJob] | None = None,
    static_suppression_policy: dict[str, Any] | None = None,
    lifecycle_state_fingerprint: tuple[int, int] | None = None,
) -> dict[str, Any]:
    finalization_timings: dict[str, int] = {}
    with _finalization_phase(
        key="deduplicating",
        label="Deduplicating jobs",
        progress_phase=progress_phase,
        task_runtime=task_runtime,
        write_progress_report=write_progress_report,
        write_task_state=write_task_state,
        timings=finalization_timings,
    ):
        deduped_rows, dedup_stats, preserved_previous = _deduplicate_or_preserve_previous(
            paths=paths,
            canonical_rows=canonical_rows,
            preserve_previous_on_empty=preserve_previous_on_empty,
            started_at=started_at,
        )
    observed_for_lifecycle = list(canonical_rows if observed_rows is None else observed_rows)
    identity_detected_at = now_iso()
    with _finalization_phase(
        key="reconciling_identities",
        label="Reconciling availability identities",
        progress_phase=progress_phase,
        task_runtime=task_runtime,
        write_progress_report=write_progress_report,
        write_task_state=write_task_state,
        timings=finalization_timings,
    ):
        identity_preparation = prepare_availability_identities(
            rows=deduped_rows,
            observed_rows=observed_for_lifecycle,
            lifecycle_rows=lifecycle_rows,
            detected_at=identity_detected_at,
        )
        deduped_rows = identity_preparation.rows
        observed_for_lifecycle = identity_preparation.observed_rows
        lifecycle_rows = identity_preparation.lifecycle_rows
    lifecycle_finished_at = now_iso()
    # ponytail: keep the pre-lifecycle CanonicalJob list so tombstone
    # reconciliation can index it without a full 40k+ to_dict() snapshot.
    pre_lifecycle_rows = deduped_rows
    with _finalization_phase(
        key="applying_lifecycle",
        label="Applying job availability lifecycle",
        progress_phase=progress_phase,
        task_runtime=task_runtime,
        write_progress_report=write_progress_report,
        write_task_state=write_task_state,
        timings=finalization_timings,
    ):
        deduped_rows, lifecycle_rows, lifecycle_archive_rows_by_year, lifecycle_counts_map = (
            _apply_lifecycle_state(
                deduped_rows=deduped_rows,
                observed_rows=observed_for_lifecycle,
                lifecycle_rows=lifecycle_rows,
                source_reports=source_reports,
                selected_loaders=selected_loaders,
                using_default_loaders=using_default_loaders,
                effective_seed_from_existing_output=effective_seed_from_existing_output,
                lifecycle_finished_at=lifecycle_finished_at,
            )
        )

    deduped_payload_rows = [row.to_dict() for row in deduped_rows]
    with _finalization_phase(
        key="running_quality_audits",
        label="Running quality audits",
        progress_phase=progress_phase,
        task_runtime=task_runtime,
        write_progress_report=write_progress_report,
        write_task_state=write_task_state,
        timings=finalization_timings,
    ):
        deduped_payload_rows, _sector_gate_dropped = _apply_sector_gate(
            deduped_payload_rows, source_reports
        )
        dedup_stats["outputCount"] = len(deduped_payload_rows)
        if _sector_gate_dropped:
            dedup_stats["sectorGateFiltered"] = _sector_gate_dropped
        (
            location_quality_audit,
            sector_quality_audit,
            contamination_report,
            city_garbage_audit,
        ) = _quality_reports(deduped_payload_rows)
        _apply_final_output_loss_counts(source_reports, deduped_payload_rows)
        validate_published_availability_rows(deduped_payload_rows)

    with _finalization_phase(
        key="writing_outputs",
        label="Writing outputs",
        progress_phase=progress_phase,
        task_runtime=task_runtime,
        write_progress_report=write_progress_report,
        write_task_state=write_task_state,
        timings=finalization_timings,
    ):
        tombstone_path = paths.output_dir / TOMBSTONE_ARTIFACT_NAME
        tombstones = reconcile_availability_tombstones(
            read_availability_tombstones(tombstone_path),
            before_rows=pre_lifecycle_rows,
            after_rows=deduped_payload_rows,
            lifecycle_rows=lifecycle_rows,
        )
        # ponytail: pre_lifecycle_rows is a reference, not a copy — nothing to free.
        write_availability_tombstones(tombstone_path, tombstones, updated_at=lifecycle_finished_at)
        quarantine_path = paths.output_dir / IDENTITY_QUARANTINE_ARTIFACT_NAME
        quarantine_stats: dict[str, int] = {}
        identity_quarantine = reconcile_identity_quarantine(
            read_identity_quarantine(quarantine_path),
            identity_preparation.quarantine_additions,
            stats=quarantine_stats,
        )
        identity_preparation.summary["quarantineCount"] = len(identity_quarantine)
        identity_preparation.summary["quarantineTruncatedCount"] = int(
            quarantine_stats.get("quarantineTruncatedCount") or 0
        )
        write_identity_quarantine(
            quarantine_path,
            identity_quarantine,
            updated_at=lifecycle_finished_at,
            truncated_count=int(quarantine_stats.get("quarantineTruncatedCount") or 0),
        )
        wrote_json, wrote_light_json = _write_output_rows(paths, deduped_payload_rows)
        availability = _write_availability_artifacts(
            paths=paths, lifecycle_rows=lifecycle_rows, finished_at=lifecycle_finished_at
        )
        availability_sweep_plan = availability["sweep"]
        source_direct_conflicts = availability["conflicts"]
        shadow_classifier_counts = availability["shadowCounts"]
        wrote_availability_history = availability["wroteHistory"]
        wrote_availability_sweep_plan = availability["wroteSweep"]
        json_bytes, light_json_bytes = _output_sizes(paths)
        _browser_fallback_queue_rows, parser_regression_queue_rows = _write_review_queue_artifacts(
            paths=paths,
            source_reports=source_reports,
            lifecycle_finished_at=lifecycle_finished_at,
            redirect_resolver=redirect_resolver,
        )
        social_review_payload, social_review_path = _write_social_review_artifact(
            paths=paths,
            deduped_rows=deduped_rows,
            lifecycle_finished_at=lifecycle_finished_at,
            started_at=started_at,
        )
    detailed_source_rows, _timing_summary = _update_runtime_timing_payload(
        runtime_payload=runtime_payload,
        task_runtime=task_runtime,
        source_reports=source_reports,
        run_started_mono=run_started_mono,
    )
    runtime_payload["finalizationTiming"] = dict(finalization_timings)
    final_source_rows = _final_source_rows(detailed_source_rows, source_reports)
    summary_payload = build_pipeline_summary(
        dedup_stats,
        deduped_rows,
        source_reports,
        len(canonical_rows),
        preserved_previous,
        len([row for row in STUDIO_SOURCE_REGISTRY if bool(row.get("enabledByDefault", True))]),
        len(common_sources.load_registry_from_file(paths.pending_registry_path, [])),
        common_sources.read_approved_since_last_run(paths.approval_state_path),
        json_bytes=json_bytes,
        light_json_bytes=light_json_bytes,
        lifecycle_counts_map=lifecycle_counts_map,
        summary_source_rows=final_source_rows,
    )
    dedup_review_state, dedup_review_state_warning = read_dedup_review_state_artifact(
        paths.dedup_review_state_path
    )
    dedup_evidence_payload = build_dedup_evidence(
        dedup_stats,
        deduped_payload_rows,
        seeded_from_existing_output=effective_seed_from_existing_output,
        review_state=dedup_review_state,
    )

    report_payload = normalize_fetch_report_payload(
        {
            "schemaVersion": SCHEMA_VERSION,
            "runId": run_id,
            "startedAt": started_at,
            "finishedAt": lifecycle_finished_at,
            "runtime": {
                **dict(runtime_payload),
                "lifecycle": {
                    "owner": "fetch_report",
                    "heartbeatAt": lifecycle_finished_at,
                },
            },
            "socialSummary": summarize_social_experiment(
                source_reports,
                deduped_rows,
                pilot_window_start_at=started_at,
                pilot_window_end_at=lifecycle_finished_at,
                review_payload=social_review_payload,
                review_artifact_path=str(social_review_path),
            ),
            "taskProgress": _completed_task_progress(summary_payload),
            "workItems": snapshot_task_rows(task_runtime.task_rows),
            "recentEvents": list(task_runtime.recent_events),
            "summary": summary_payload,
            "dedupEvidence": dedup_evidence_payload,
            "lifecycleSummary": _lifecycle_summary_payload(lifecycle_counts_map),
            "availabilitySummary": {
                **_lifecycle_summary_payload(lifecycle_counts_map),
                **identity_preparation.summary,
                "sourceDirectConflictCount": len(source_direct_conflicts),
                "shadowClassifierCounts": shadow_classifier_counts,
            },
            "availabilityHealth": {
                "status": "healthy"
                if bool(availability_sweep_plan.get("healthTargetMet"))
                and not bool(availability_sweep_plan.get("degradedCoverage"))
                and not int(identity_preparation.summary.get("rejectedRowCount") or 0)
                and not int(identity_preparation.summary.get("unresolvedMissingIdentityCount") or 0)
                and not int(
                    identity_preparation.summary.get("unresolvedIdentityConflictCount") or 0
                )
                else "degraded",
                "overdueCount": int(lifecycle_counts_map.get("availabilityOverdue") or 0),
                "verifiedWithinDaysTarget": 7,
                "verifiedCoverageTarget": 0.95,
                "verifiedWithinSevenDaysCoverage": float(
                    availability_sweep_plan.get("verifiedWithinSevenDaysCoverage") or 0
                ),
                "sweepSelectedCount": int(availability_sweep_plan.get("selectedCount") or 0),
                "sweepDeferredCount": int(availability_sweep_plan.get("deferredCount") or 0),
                "degradedCoverage": bool(
                    availability_sweep_plan.get("degradedCoverage")
                    or int(identity_preparation.summary.get("rejectedRowCount") or 0)
                ),
                "shadowClassifier": True,
                "identity": dict(identity_preparation.summary),
            },
            "sourceDirectConflicts": source_direct_conflicts[-100:],
            "sweepCoverage": {
                key: value for key, value in availability_sweep_plan.items() if key != "rows"
            },
            "shadowClassifierCounts": shadow_classifier_counts,
            "sources": final_source_rows,
            "sourceFamilies": source_reports,
            "contaminationAudit": contamination_report,
            "cityGarbageAudit": city_garbage_audit,
            "locationQualityAudit": location_quality_audit,
            "sectorQualityAudit": sector_quality_audit,
            "outputs": {
                "json": str(paths.json_path),
                "lightJson": str(paths.light_json_path),
                "report": str(paths.report_path),
                "lifecycleState": str(paths.lifecycle_state_path),
                "availabilityHistory": str(paths.availability_history_path),
                "availabilitySweepPlan": str(paths.availability_sweep_plan_path),
                "browserFallbackQueue": str(paths.browser_fallback_queue_path),
                "parserRegressionQueue": str(paths.parser_regression_queue_path),
                "sourcePolicyRecommendations": str(paths.source_policy_recommendations_path),
                "sourcePolicyReviewState": str(paths.source_policy_review_state_path),
                "dedupReviewState": str(paths.dedup_review_state_path),
                "changed": {
                    "json": wrote_json,
                    "lightJson": wrote_light_json,
                    "availabilityHistory": wrote_availability_history,
                    "availabilitySweepPlan": wrote_availability_sweep_plan,
                },
            },
        }
    )
    finished_at = clean_text(report_payload.get("finishedAt")) or now_iso()
    prior_source_state_rows = {
        clean_text(name): dict(row)
        for name, row in source_state_rows.items()
        if clean_text(name) and isinstance(row, dict)
    }
    source_state_rows = update_source_state_rows(
        source_state_rows=source_state_rows,
        source_reports=source_reports,
        canonical_rows=deduped_payload_rows,
        finished_at=finished_at,
        circuit_breaker_failures=circuit_breaker_failures,
        circuit_breaker_cooldown_minutes=circuit_breaker_cooldown_minutes,
        circuit_breaker_zero_kept=circuit_breaker_zero_kept,
    )
    _merge_source_health_report_payload(report_payload, source_state_rows)
    snapshot_redirect_cache = getattr(redirect_resolver, "snapshot_cache", None)
    if callable(snapshot_redirect_cache):
        persisted_redirect_cache = {
            clean_text(url): clean_text(resolved)
            for url, resolved in (snapshot_redirect_cache() or {}).items()
            if clean_text(url) and clean_text(resolved)
        }
        if persisted_redirect_cache:
            for source_name, source_row in source_state_rows.items():
                if clean_text(source_name).startswith("google_sheets") and isinstance(
                    source_row, dict
                ):
                    source_row["googleSheetsRedirectCache"] = dict(persisted_redirect_cache)
    report_payload["providerCoverage"] = build_provider_coverage_summary(source_state_rows)
    overlap_source_state_rows = {name: dict(row) for name, row in source_state_rows.items()}
    for row in final_source_rows:
        source_name = clean_text(row.get("name"))
        if (
            clean_text(row.get("exclusionReason")) == "dynamic_redundant_provider"
            and source_name in prior_source_state_rows
        ):
            overlap_source_state_rows[source_name] = dict(prior_source_state_rows[source_name])
    report_payload["providerStaticOverlap"] = build_provider_static_overlap_summary(
        source_rows=final_source_rows,
        source_state_rows=overlap_source_state_rows,
        canonical_rows=deduped_payload_rows,
    )
    report_payload["staticSuppressionPolicy"] = (
        refresh_static_suppression_policy_with_current_evidence(
            static_suppression_policy or {},
            source_state_rows=overlap_source_state_rows,
            canonical_rows=deduped_payload_rows,
            provider_static_overlap=report_payload["providerStaticOverlap"],
        )
    )
    report_payload["redundantStaticProposals"] = build_redundant_static_proposals_summary(
        static_suppression_policy=report_payload["staticSuppressionPolicy"],
        provider_static_overlap=report_payload["providerStaticOverlap"],
        provider_coverage=report_payload["providerCoverage"],
    )
    if dedup_review_state_warning:
        report_payload["dedupReviewStateExport"] = {
            "status": "warning",
            "artifactPath": str(paths.dedup_review_state_path),
            "warning": dedup_review_state_warning,
        }
    else:
        report_payload["dedupReviewStateExport"] = {
            "status": "ok",
            "artifactPath": str(paths.dedup_review_state_path),
            "reviewedPairCount": int(dedup_review_state.get("summary", {}).get("totalPairs") or 0),
        }
    _export_source_policy_recommendations(
        report_payload=report_payload,
        source_policy_recommendations_path=paths.source_policy_recommendations_path,
        source_policy_review_state_path=paths.source_policy_review_state_path,
        finished_at=finished_at,
    )
    report_payload["healthSummary"] = {
        "topFailingDomains": health_module.get_top_failing_sources(source_state_rows, limit=10),
        "topZeroKeptDomains": health_module.get_top_zero_kept_sources(source_state_rows, limit=10),
        "topSlowDomains": health_module.get_top_slow_sources(source_state_rows, limit=10),
        "quarantinedSources": health_module.get_quarantined_sources(source_state_rows),
        "siteChangedDiagnosedCount": count_site_changed_diagnosed_sources(source_reports),
        "siteChangedMissingOldUrlCount": count_site_changed_missing_old_url_sources(source_reports),
        "parserRegressionQueueCount": len(parser_regression_queue_rows),
    }
    write_hot_text_if_changed(
        paths.report_path, json.dumps(report_payload, indent=2, ensure_ascii=False)
    )
    write_task_state(finished_at=finished_at, force=True)
    write_fetch_report_summary_artifact(
        paths.report_path,
        report_payload,
        write_text_if_changed=write_hot_text_if_changed,
        include_sources=True,
    )
    write_success_cache(paths.success_cache_path, source_reports)
    write_source_state(paths.source_state_path, source_state_rows)
    write_job_lifecycle_state(paths.lifecycle_state_path, lifecycle_rows)
    _write_lifecycle_archive_rows(
        lifecycle_state_path=paths.lifecycle_state_path,
        archive_rows_by_year=lifecycle_archive_rows_by_year,
    )
    return report_payload
