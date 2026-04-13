from __future__ import annotations

from collections import Counter
from collections.abc import Iterable
from typing import Any

from src.contracts import SCHEMA_VERSION
from src.shared.utils import now_iso
from src.source_registry import source_identity

from .config import EVIDENCE_TYPES_SET
from .io_runtime import endpoint_url
from .runtime_metrics import build_discovery_runtime_payload
from .scoring import unique_string_list

M5_ALLOWED_M4_FAMILIES = {"bamboohr", "workday", "breezy"}
M5_STRUCTURED_MIGRATION_ROLLBACK_CHECKLIST = (
    "Re-enable the static twin in the registry.",
    "Keep structured shadow mode until 3 consecutive healthy runs complete.",
    "Demote the structured source if kept count drops to zero or duplicate rate regresses.",
)
M5_COVERAGE_LANES = {
    "lane_a_m4_followup",
    "lane_b_custom",
    "lane_c_asia_custom",
    "lane_d_defer",
}
M5_FIRST_RUN_OUTCOMES = {"healthy_keep", "needs_fix", "defer_after_trial"}
M5_HARD_EXCLUSION_REASONS = {
    "existing_id",
    "existing_domain",
    "queue_threshold",
    "blocked_domain",
    "sheet_directory_stage_cap",
}
M5_SUPPRESSION_STAGES = {"suppressed_static"}
M5_ASIA_TOKENS = {
    "asia",
    "apac",
    "japan",
    "tokyo",
    "osaka",
    "singapore",
    "hong kong",
    "hongkong",
    "china",
    "shanghai",
    "beijing",
    "taipei",
    "seoul",
    "korea",
    "india",
    "bangalore",
    "bengaluru",
    "delhi",
    "mumbai",
    "manila",
    "jakarta",
    "bangkok",
    "kuala lumpur",
    "ho chi minh",
    "hanoi",
    "dubai",
    "uae",
}
M5_EUROPE_TOKENS = {
    "europe",
    "emea",
    "united kingdom",
    "uk",
    "england",
    "scotland",
    "wales",
    "ireland",
    "germany",
    "france",
    "spain",
    "italy",
    "netherlands",
    "sweden",
    "norway",
    "finland",
    "denmark",
    "poland",
    "austria",
    "switzerland",
    "belgium",
    "luxembourg",
    "portugal",
    "greece",
    "czech",
    "romania",
    "bulgaria",
    "croatia",
}
M5_NORTH_AMERICA_TOKENS = {
    "north america",
    "united states",
    "usa",
    "u.s.a.",
    "u.s.",
    "canada",
    "california",
    "new york",
    "seattle",
    "san francisco",
    "austin",
    "boston",
}
M5_SOUTH_AMERICA_TOKENS = {"south america", "brazil", "argentina", "chile", "peru", "colombia"}
M5_OCEANIA_TOKENS = {"oceania", "australia", "new zealand", "sydney", "melbourne", "auckland"}
M5_AFRICA_TOKENS = {"africa", "south africa", "nigeria", "kenya", "egypt", "morocco"}


def build_stage_summary(
    current_candidates: list[dict[str, Any]],
    *,
    found_endpoint_count: int,
    generated_count_by_stage: dict[str, int],
    survived_dedupe_count_by_stage: dict[str, int],
    probed_count_by_stage: dict[str, int],
    queued_count_by_stage: dict[str, int],
    probed: int,
    healthy: int,
    failures: list[dict[str, Any]],
    skipped_duplicate_count: int,
    skipped_invalid: int,
    skipped_low_evidence_probe_count: int,
    validation_skipped_count: int,
    probe_failed_count: int,
    queue_filtered_count: int,
    adapter_counter: Counter[str],
    method_counter: Counter[str],
    duplicate_reasons: dict[str, int],
    deferred_counts: dict[str, int] | None = None,
    queued_by_adapter: dict[str, int] | None = None,
    deferred_by_adapter: dict[str, int] | None = None,
    healthy_but_deferred_by_adapter: dict[str, int] | None = None,
    suppressed_static_count: int = 0,
    suppressed_static_by_reason: dict[str, int] | None = None,
    suppressed_static_by_stage: dict[str, int] | None = None,
    thresholds: dict[str, Any],
    phase: str = "",
    phase_label: str = "",
) -> dict[str, Any]:
    """Build the discovery report summary dict. Pure function; all inputs passed explicitly."""
    deferred_reason_rows = deferred_counts or {}
    deferred_by_cap = int(sum(int(value or 0) for value in deferred_reason_rows.values()))
    failed_probe_count_final = len([row for row in failures if str(row.get("stage")) == "probe"])
    probe_miss_count_final = len([row for row in failures if str(row.get("stage")) == "probe_miss"])
    validated_count = len([row for row in current_candidates if isinstance(row, dict)])
    approved_count = len(
        [
            row
            for row in current_candidates
            if isinstance(row, dict)
            and str(row.get("candidateState") or "").strip().lower() in {"approved", "live"}
        ]
    )
    live_count = len(
        [
            row
            for row in current_candidates
            if isinstance(row, dict)
            and str(row.get("candidateState") or "").strip().lower() == "live"
        ]
    )
    quarantined_count = len(
        [
            row
            for row in current_candidates
            if isinstance(row, dict)
            and str(row.get("candidateState") or "").strip().lower() == "quarantined"
        ]
    )
    return {
        "phase": str(phase or ""),
        "phaseKey": str(phase or ""),
        "phaseLabel": str(phase_label or ""),
        "probedCount": probed,
        "healthyCount": healthy,
        "newCandidateCount": len(current_candidates),
        "taEnvCandidateCount": sum(
            1 for row in current_candidates if "target_role_signal" in row.get("reasons", [])
        ),
        "nlCandidateCount": sum(1 for row in current_candidates if bool(row.get("nlPriority"))),
        "failedProbeCount": failed_probe_count_final,
        "probeMissCount": probe_miss_count_final,
        "foundEndpointCount": found_endpoint_count,
        "probedCandidateCount": probed,
        "queuedCandidateCount": len(
            [row for row in current_candidates if not bool(row.get("deferred"))]
        ),
        "validatedCandidateCount": validated_count,
        "approvedCandidateCount": approved_count,
        "liveCandidateCount": live_count,
        "quarantinedCandidateCount": quarantined_count,
        "discoverableButDeferredCount": len(
            [row for row in current_candidates if bool(row.get("deferred"))]
        ),
        "suppressedStaticCount": int(suppressed_static_count),
        "skippedDuplicateCount": skipped_duplicate_count,
        "skippedInvalidCount": skipped_invalid,
        "skippedLowEvidenceProbeCount": skipped_low_evidence_probe_count,
        "adapterCounts": dict(adapter_counter),
        "queuedByAdapter": dict(queued_by_adapter or {}),
        "deferredByAdapter": dict(deferred_by_adapter or {}),
        "healthyButDeferredByAdapter": dict(healthy_but_deferred_by_adapter or {}),
        "suppressedStaticByReason": dict(suppressed_static_by_reason or {}),
        "suppressedStaticByStage": dict(suppressed_static_by_stage or {}),
        "queuedProviderCount": int(
            sum(
                value
                for key, value in dict(queued_by_adapter or {}).items()
                if str(key) != "static"
            )
        ),
        "queuedStaticCount": int(dict(queued_by_adapter or {}).get("static") or 0),
        "methodCounts": dict(method_counter),
        "generatedCountByStage": dict(generated_count_by_stage),
        "survivedDedupeCountByStage": dict(survived_dedupe_count_by_stage),
        "probedCountByStage": dict(probed_count_by_stage),
        "queuedCountByStage": dict(queued_count_by_stage),
        "duplicateReasons": dict(duplicate_reasons),
        "deferredReasons": dict(deferred_reason_rows),
        "thresholds": dict(thresholds),
        "lossAccounting": {
            "generated": int(found_endpoint_count),
            "dedupSkipped": int(skipped_duplicate_count),
            "dedupSkippedReasons": dict(duplicate_reasons),
            "validationSkipped": int(validation_skipped_count),
            "lowEvidenceSkipped": int(skipped_low_evidence_probe_count),
            "probeFailed": int(failed_probe_count_final + probe_miss_count_final),
            "queueFiltered": int(queue_filtered_count),
            "deferredByCap": deferred_by_cap,
            "queued": int(
                len([row for row in current_candidates if not bool(row.get("deferred"))])
            ),
        },
    }


def build_discovery_task_progress(
    *,
    summary: dict[str, Any],
    finished: bool,
) -> dict[str, Any]:
    phase_key = str(summary.get("phaseKey") or summary.get("phase") or "").strip() or (
        "completed" if finished else "starting"
    )
    phase_label = str(summary.get("phaseLabel") or "").strip() or (
        "Discovery completed" if finished else "Initializing scan"
    )
    found_count = int(summary.get("foundEndpointCount") or 0)
    probed_count = int(summary.get("probedCandidateCount") or summary.get("probedCount") or 0)
    queued_count = int(summary.get("queuedCandidateCount") or 0)
    deferred_count = int(summary.get("discoverableButDeferredCount") or 0)
    failed_count = int(summary.get("failedProbeCount") or 0)
    loss = summary.get("lossAccounting") if isinstance(summary.get("lossAccounting"), dict) else {}
    probe_total = max(
        0,
        int(loss.get("generated") or 0)
        - int(loss.get("dedupSkipped") or 0)
        - int(loss.get("validationSkipped") or 0)
        - int(loss.get("lowEvidenceSkipped") or 0)
        - int(summary.get("suppressedStaticCount") or 0),
    ) or max(probed_count, failed_count, queued_count)
    mode = "indeterminate"
    ratio = 0.0
    if finished:
        mode = "determinate"
        ratio = 1.0
        phase_key = "completed"
        phase_label = "Discovery completed"
    elif phase_key == "probing_candidates" and probe_total > 0:
        mode = "determinate"
        ratio = max(0.0, min(1.0, probed_count / max(1, probe_total)))
    return {
        "active": not bool(finished),
        "phaseKey": phase_key,
        "phaseLabel": phase_label,
        "mode": mode,
        "ratio": ratio,
        "counts": {
            "foundEndpoints": found_count,
            "probedCandidates": probed_count,
            "probeTotal": probe_total,
            "queuedCandidates": queued_count,
            "deferredCandidates": deferred_count,
            "failedProbes": failed_count,
        },
    }


def emit_log(message: str) -> None:
    line = f"[{now_iso()}] {str(message or '').strip()}"
    print(line, flush=True)


def update_candidate_review_metadata(
    row: dict[str, Any],
    *,
    prior_candidate: dict[str, Any] | None,
    now_iso: str,
) -> dict[str, Any]:
    updated = dict(row)
    prior = prior_candidate if isinstance(prior_candidate, dict) else {}
    updated["candidateState"] = str(updated.get("candidateState") or "validated")
    updated["rankScore"] = int(updated.get("rankScore") or updated.get("score") or 0)
    updated["rankReasons"] = unique_string_list(
        updated.get("rankReasons") or updated.get("reasons") or []
    )
    updated["promotionLane"] = str(updated.get("promotionLane") or "manual_review")
    updated["approvedAt"] = str(updated.get("approvedAt") or "")
    updated["approvedBy"] = str(updated.get("approvedBy") or "")
    updated["liveAt"] = str(updated.get("liveAt") or "")
    updated["quarantinedAt"] = str(updated.get("quarantinedAt") or "")
    updated["quarantineReason"] = str(updated.get("quarantineReason") or "")
    updated["deferCount"] = max(0, int(updated.get("deferCount") or prior.get("deferCount") or 0))
    updated["firstDeferredAt"] = str(
        updated.get("firstDeferredAt") or prior.get("firstDeferredAt") or ""
    )
    updated["lastDeferredAt"] = str(
        updated.get("lastDeferredAt") or prior.get("lastDeferredAt") or ""
    )

    if bool(updated.get("deferred")):
        updated["candidateState"] = "validated"
        updated["deferCount"] = max(1, int(prior.get("deferCount") or 0) + 1)
        updated["firstDeferredAt"] = str(
            prior.get("firstDeferredAt") or prior.get("lastDeferredAt") or now_iso
        )
        updated["lastDeferredAt"] = str(now_iso)
        if str(updated.get("deferReason") or "").strip().lower() == "domain_cap":
            updated["promotionLane"] = "domain_cap_review"

    return updated


def write_discovery_progress_report(
    *,
    current_candidates: list[dict[str, Any]],
    phase: str,
    phase_label: str,
    total_duration_ms: int,
    stage_timings_ms: dict[str, int],
    adapter_runtime: dict[str, dict[str, int | str]],
    preset_name: str,
    top_cap_bypassed: bool,
    sheet_static_probe_cap_bypassed: bool,
    url_patch_stats: dict[str, int],
    found_endpoint_count: int,
    generated_count_by_stage: dict[str, int],
    survived_dedupe_count_by_stage: dict[str, int],
    probed_count_by_stage: dict[str, int],
    queued_count_by_stage: dict[str, int],
    probed: int,
    healthy: int,
    failures: list[dict[str, Any]],
    skipped_duplicate_count: int,
    skipped_invalid: int,
    skipped_low_evidence_probe_count: int,
    validation_skipped_count: int,
    probe_failed_count: int,
    queue_filtered_count: int,
    adapter_counter: Counter[str],
    method_counter: Counter[str],
    duplicate_reasons: dict[str, int],
    suppressed_static_count: int,
    suppressed_static_by_reason: dict[str, int],
    suppressed_static_by_stage: dict[str, int],
    thresholds: dict[str, Any],
    run_id: str,
    mode: str,
    started_at: str,
    report_write_path,
    outputs: dict[str, str],
    save_json_atomic_fn,
    now_iso_fn=now_iso,
) -> None:
    runtime_payload = build_discovery_runtime_payload(
        total_duration_ms=total_duration_ms,
        stage_timings_ms=stage_timings_ms,
        adapter_runtime=adapter_runtime,
        preset=preset_name,
        top_cap_bypassed=top_cap_bypassed,
        sheet_static_probe_cap_bypassed=sheet_static_probe_cap_bypassed,
    )
    runtime_payload["urlPatchStats"] = dict(url_patch_stats)
    summary = build_stage_summary(
        current_candidates,
        found_endpoint_count=found_endpoint_count,
        generated_count_by_stage=generated_count_by_stage,
        survived_dedupe_count_by_stage=survived_dedupe_count_by_stage,
        probed_count_by_stage=probed_count_by_stage,
        queued_count_by_stage=queued_count_by_stage,
        probed=probed,
        healthy=healthy,
        failures=failures,
        skipped_duplicate_count=skipped_duplicate_count,
        skipped_invalid=skipped_invalid,
        skipped_low_evidence_probe_count=skipped_low_evidence_probe_count,
        validation_skipped_count=validation_skipped_count,
        probe_failed_count=probe_failed_count,
        queue_filtered_count=queue_filtered_count,
        adapter_counter=adapter_counter,
        method_counter=method_counter,
        duplicate_reasons=duplicate_reasons,
        deferred_counts=None,
        queued_by_adapter=None,
        deferred_by_adapter=None,
        healthy_but_deferred_by_adapter=None,
        suppressed_static_count=suppressed_static_count,
        suppressed_static_by_reason=dict(suppressed_static_by_reason),
        suppressed_static_by_stage=dict(suppressed_static_by_stage),
        thresholds=thresholds,
        phase=phase,
        phase_label=phase_label,
    )
    task_progress = build_discovery_task_progress(summary=summary, finished=False)
    save_json_atomic_fn(
        report_write_path,
        {
            "schemaVersion": SCHEMA_VERSION,
            "runId": run_id,
            "mode": mode,
            "startedAt": started_at,
            "finishedAt": "",
            "summary": summary,
            "runtime": {
                **dict(runtime_payload),
                "lifecycle": {
                    "owner": "discovery_report",
                    "heartbeatAt": now_iso_fn(),
                },
            },
            "taskProgress": task_progress,
            "candidates": current_candidates,
            "failures": failures,
            "topFailures": [],
            "outputs": outputs,
        },
    )


def _validate_evidence_types(values: list[str], *, context: str) -> list[str]:
    cleaned = unique_string_list(
        [str(item or "").strip() for item in (values or []) if str(item or "").strip()]
    )
    unknown = [item for item in cleaned if item not in EVIDENCE_TYPES_SET]
    if unknown:
        emit_log(f"Warning: dropping unknown evidenceTypes in {context}: {unknown}")
    return [item for item in cleaned if item in EVIDENCE_TYPES_SET]


def stage_curated_seed_candidates() -> list[dict[str, Any]]:
    from src.source_registry import unique_sources

    from .config import STATIC_DISCOVERY_CANDIDATES

    rows: list[dict[str, Any]] = []
    for raw in STATIC_DISCOVERY_CANDIDATES:
        if not isinstance(raw, dict):
            continue
        row = dict(raw)
        row["discoveryMethod"] = str(row.get("discoveryMethod") or "seed")
        row["discoveryStage"] = "curated_seed"
        row["evidenceScore"] = int(row.get("evidenceScore") or 52)
        row["evidenceTypes"] = _validate_evidence_types(
            list(row.get("evidenceTypes") or ["seed_curated"]),
            context="stage_curated_seed_candidates",
        )
        row["evidenceSource"] = str(row.get("evidenceSource") or "seed")
        row["careersUrl"] = str(row.get("careersUrl") or endpoint_url(row) or "")
        rows.append(row)
    return unique_sources(rows)


def merge_candidate_streams(
    streams: Iterable[tuple[str, list[dict[str, Any]]]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for stage, items in streams:
        for raw in items:
            if not isinstance(raw, dict):
                continue
            row = dict(raw)
            row["discoveryStage"] = str(row.get("discoveryStage") or stage)
            row["discoveryMethod"] = str(
                row.get("discoveryMethod") or ("seed" if stage == "curated_seed" else "pattern")
            )
            row["discoveredAt"] = str(row.get("discoveredAt") or now_iso())
            row["evidenceTypes"] = _validate_evidence_types(
                list(row.get("evidenceTypes") or []),
                context="merge_candidate_streams",
            )
            row["evidenceScore"] = int(row.get("evidenceScore") or 0)
            rows.append(row)
    return rows


def _candidate_join_key(row: dict[str, Any]) -> str:
    source_id = str(row.get("sourceId") or "").strip()
    if source_id:
        return source_id
    return source_identity(row)


def _text_blob(*values: Any) -> str:
    parts = [str(value or "").strip().lower() for value in values if str(value or "").strip()]
    return " ".join(parts).strip()


def _region_category(row: dict[str, Any]) -> str:
    text = _text_blob(
        row.get("hqRegion"),
        row.get("region"),
        row.get("hqCountry"),
        row.get("country"),
        row.get("countryCode"),
        row.get("location"),
        row.get("sourceDirectoryLocation"),
        row.get("officeLocation"),
    )
    if not text:
        return "unknown"
    if any(token in text for token in M5_ASIA_TOKENS):
        return "asia"
    if any(token in text for token in M5_EUROPE_TOKENS):
        return "europe"
    if any(token in text for token in M5_NORTH_AMERICA_TOKENS):
        return "north_america"
    if any(token in text for token in M5_SOUTH_AMERICA_TOKENS):
        return "south_america"
    if any(token in text for token in M5_OCEANIA_TOKENS):
        return "oceania"
    if any(token in text for token in M5_AFRICA_TOKENS):
        return "africa"
    return "unknown"


def _state_lookup_variants(row: dict[str, Any]) -> list[str]:
    variants = [
        str(row.get("name") or "").strip(),
        str(row.get("studio") or "").strip(),
        str(row.get("source") or "").strip(),
        str(row.get("id") or "").strip(),
        str(row.get("sourceId") or "").strip(),
        _candidate_join_key(row),
    ]
    return [variant for variant in unique_string_list(variants) if variant]


def _lookup_state_entry(
    row: dict[str, Any],
    source_state_rows: dict[str, dict[str, Any]] | None,
) -> dict[str, Any]:
    if not isinstance(source_state_rows, dict):
        return {}
    for key in _state_lookup_variants(row):
        entry = source_state_rows.get(key)
        if isinstance(entry, dict):
            return entry
    return {}


def _m5_hard_exclusion_reason(
    row: dict[str, Any],
    failure_row: dict[str, Any] | None,
    *,
    exclusion_reason: str,
) -> str:
    if exclusion_reason in M5_HARD_EXCLUSION_REASONS:
        return exclusion_reason
    if str(failure_row.get("dropStage") or "").strip().lower() in M5_SUPPRESSION_STAGES:
        if exclusion_reason:
            return exclusion_reason
        return str(failure_row.get("dropReason") or failure_row.get("error") or "suppressed_static")
    if str(row.get("dropStage") or "").strip().lower() in M5_SUPPRESSION_STAGES:
        if exclusion_reason:
            return exclusion_reason
        return str(row.get("dropReason") or row.get("deferReason") or "suppressed_static")
    return exclusion_reason


def _m5_rank_reasons(row: dict[str, Any], *, extra: list[str] | None = None) -> list[str]:
    reasons = unique_string_list(row.get("rankReasons") or row.get("reasons") or [])
    if extra:
        reasons = unique_string_list([*reasons, *extra])
    return reasons


def _m5_base_priority(row: dict[str, Any]) -> int:
    return max(0, min(100, int(row.get("rankScore") or row.get("score") or 0)))


def _m5_first_run_outcome(
    row: dict[str, Any], *, state_entry: dict[str, Any], kept_count: int
) -> str:
    outcome = str(row.get("firstRunOutcome") or state_entry.get("firstRunOutcome") or "").strip()
    if outcome in M5_FIRST_RUN_OUTCOMES:
        return outcome
    if not state_entry:
        return ""
    last_status = str(state_entry.get("lastStatus") or "").strip().lower()
    if kept_count > 0:
        return "healthy_keep" if last_status in {"ok", "healthy", ""} else "needs_fix"
    if last_status == "ok":
        return "needs_fix"
    return "defer_after_trial"


def _m5_structured_migration_comparison(
    row: dict[str, Any], *, state_entry: dict[str, Any]
) -> dict[str, Any]:
    adapter = str(row.get("adapter") or "").strip().lower()
    if adapter not in M5_ALLOWED_M4_FAMILIES:
        return {}

    before_duration_ms = int(state_entry.get("structuredMigrationBaselineDurationMs") or 0)
    before_status = str(state_entry.get("structuredMigrationBaselineStatus") or "").strip()
    before_error = str(state_entry.get("structuredMigrationBaselineError") or "").strip()
    before_failure_bucket = str(
        state_entry.get("structuredMigrationBaselineFailureBucket") or ""
    ).strip()
    before_kept_count = int(state_entry.get("structuredMigrationBaselineKeptCount") or 0)

    after_duration_ms = int(state_entry.get("lastDurationMs") or 0)
    after_status = str(state_entry.get("lastStatus") or "").strip()
    after_error = str(state_entry.get("lastError") or "").strip()
    after_failure_bucket = str(state_entry.get("lastFailureBucket") or "").strip()
    after_kept_count = int(state_entry.get("lastKeptCount") or 0)

    comparison = {
        "before": {
            "durationMs": before_duration_ms,
            "status": before_status,
            "error": before_error,
            "failureBucket": before_failure_bucket,
            "keptCount": before_kept_count,
        },
        "after": {
            "durationMs": after_duration_ms,
            "status": after_status,
            "error": after_error,
            "failureBucket": after_failure_bucket,
            "keptCount": after_kept_count,
        },
        "runtimeDeltaMs": after_duration_ms - before_duration_ms,
        "keptCountDelta": after_kept_count - before_kept_count,
        "shadowRunCount": int(state_entry.get("structuredMigrationShadowRunCount") or 0),
        "healthyRunCount": int(state_entry.get("structuredMigrationHealthyRunCount") or 0),
        "promotedAt": str(state_entry.get("structuredMigrationPromotedAt") or "").strip(),
        "demotedAt": str(state_entry.get("structuredMigrationDemotedAt") or "").strip(),
        "rollbackChecklist": list(M5_STRUCTURED_MIGRATION_ROLLBACK_CHECKLIST),
    }
    if not any(
        [
            comparison["before"]["durationMs"],
            comparison["after"]["durationMs"],
            comparison["before"]["status"],
            comparison["after"]["status"],
            comparison["before"]["error"],
            comparison["after"]["error"],
            comparison["before"]["failureBucket"],
            comparison["after"]["failureBucket"],
            comparison["before"]["keptCount"],
            comparison["after"]["keptCount"],
            comparison["shadowRunCount"],
            comparison["healthyRunCount"],
            comparison["promotedAt"],
            comparison["demotedAt"],
        ]
    ):
        return {}
    return comparison


def build_m5_strategic_backlog(
    *,
    report_candidates: list[dict[str, Any]],
    failures: list[dict[str, Any]],
    active_rows: list[dict[str, Any]] | None = None,
    source_state_rows: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Build the derived M5 review snapshot from discovery output."""

    active_rows = [dict(row) for row in active_rows or [] if isinstance(row, dict)]
    source_state_rows = source_state_rows if isinstance(source_state_rows, dict) else {}
    candidate_rows: dict[str, dict[str, Any]] = {}
    failure_rows: dict[str, dict[str, Any]] = {}
    for row in report_candidates or []:
        if isinstance(row, dict):
            candidate_rows[_candidate_join_key(row)] = dict(row)
    for row in failures or []:
        if isinstance(row, dict):
            failure_rows[_candidate_join_key(row)] = dict(row)

    region_live_counts: Counter[str] = Counter()
    region_kept_counts: Counter[str] = Counter()
    for row in active_rows:
        region = _region_category(row)
        if region == "unknown":
            continue
        region_live_counts[region] += 1
        kept_count = int(_lookup_state_entry(row, source_state_rows).get("lastKeptCount") or 0)
        if kept_count > 0:
            region_kept_counts[region] += 1

    backlog_rows: list[dict[str, Any]] = []
    for identity_key in sorted({*candidate_rows.keys(), *failure_rows.keys()}):
        candidate = candidate_rows.get(identity_key) or {}
        failure = failure_rows.get(identity_key) or {}
        row = dict(candidate or failure)
        if not row:
            continue
        adapter = str(row.get("adapter") or failure.get("adapter") or "").strip().lower()
        region = _region_category(row)
        state_entry = _lookup_state_entry(row, source_state_rows)
        kept_count = int(
            row.get("firstRunKeptCount")
            or state_entry.get("lastKeptCount")
            or row.get("lastKeptCount")
            or 0
        )
        exclusion_reason = str(
            failure.get("dropReason")
            or row.get("dropReason")
            or row.get("deferReason")
            or row.get("error")
            or ""
        ).strip()
        exclusion_reason = _m5_hard_exclusion_reason(
            row,
            failure,
            exclusion_reason=exclusion_reason,
        )
        approved_exception_reason = str(
            row.get("coverageExceptionReason") or row.get("exceptionReason") or ""
        ).strip()
        approved_exception_by = str(
            row.get("approvedExceptionBy") or row.get("approvedBy") or ""
        ).strip()
        has_exception = bool(approved_exception_reason and approved_exception_by)
        is_m4_family = adapter in M5_ALLOWED_M4_FAMILIES
        is_deferred = (
            bool(row.get("deferred"))
            or str(row.get("dropStage") or "").strip().lower() == "deferred_by_cap"
        )
        exclusion_status = "included"
        if is_m4_family and not has_exception:
            exclusion_status = "excluded"
            if not exclusion_reason:
                exclusion_reason = "m4_family_followup"
        elif exclusion_reason and not has_exception:
            exclusion_status = "excluded"
        elif is_deferred:
            exclusion_status = "deferred"
            if not exclusion_reason:
                exclusion_reason = str(row.get("deferReason") or "deferred").strip()

        coverage_lane = "lane_b_custom"
        if is_m4_family:
            coverage_lane = "lane_a_m4_followup"
        elif region == "asia":
            coverage_lane = "lane_c_asia_custom"
        elif exclusion_status in {"excluded", "deferred"}:
            coverage_lane = "lane_d_defer"
        if coverage_lane not in M5_COVERAGE_LANES:
            coverage_lane = "lane_d_defer"

        rank_reasons = _m5_rank_reasons(row)
        if exclusion_reason and exclusion_reason not in rank_reasons:
            rank_reasons = unique_string_list([*rank_reasons, exclusion_reason])
        coverage_priority = _m5_base_priority(row)
        if region == "asia":
            coverage_priority += 2
            rank_reasons = unique_string_list([*rank_reasons, "asia_hq"])
        jobs_found = max(0, int(row.get("jobsFound") or row.get("sampleCount") or 0))
        if jobs_found > 0:
            coverage_priority += 2
            rank_reasons = unique_string_list([*rank_reasons, "open_role_evidence"])
        if region != "unknown" and (
            int(region_live_counts.get(region) or 0) < 5
            or int(region_kept_counts.get(region) or 0) < 3
        ):
            coverage_priority += 2
            rank_reasons = unique_string_list([*rank_reasons, "weak_regional_coverage"])
        if is_m4_family:
            coverage_priority -= 3
            rank_reasons = unique_string_list([*rank_reasons, "m4_family_followup"])
        if exclusion_reason in {"existing_id", "existing_domain"}:
            coverage_priority -= 3
            rank_reasons = unique_string_list([*rank_reasons, "existing_coverage_match"])
        coverage_priority = max(0, min(100, int(coverage_priority)))
        coverage_justification_bits = []
        if is_m4_family:
            coverage_justification_bits.append("structured-family follow-up")
        elif region == "asia":
            coverage_justification_bits.append("asia-priority custom target")
        else:
            coverage_justification_bits.append("custom coverage target")
        if jobs_found > 0:
            coverage_justification_bits.append("open-role evidence")
        if region != "unknown" and (
            int(region_live_counts.get(region) or 0) < 5
            or int(region_kept_counts.get(region) or 0) < 3
        ):
            coverage_justification_bits.append("weak regional coverage")
        if exclusion_status == "excluded" and exclusion_reason:
            coverage_justification_bits.append(f"excluded:{exclusion_reason}")
        elif exclusion_status == "deferred" and exclusion_reason:
            coverage_justification_bits.append(f"deferred:{exclusion_reason}")
        if has_exception:
            coverage_justification_bits.append(f"approved exception:{approved_exception_reason}")
        first_run_outcome = _m5_first_run_outcome(
            row,
            state_entry=state_entry,
            kept_count=kept_count,
        )
        migration_comparison = _m5_structured_migration_comparison(row, state_entry=state_entry)
        backlog_rows.append(
            {
                "candidateIdentityKey": identity_key,
                "sourceId": str(row.get("sourceId") or row.get("id") or identity_key),
                "studio": str(row.get("studio") or row.get("name") or row.get("source") or ""),
                "sourceName": str(row.get("name") or row.get("studio") or row.get("source") or ""),
                "adapter": adapter,
                "lane": coverage_lane,
                "coverageLane": coverage_lane,
                "score": coverage_priority,
                "coveragePriority": coverage_priority,
                "rankReasons": rank_reasons,
                "coverageJustification": "; ".join(
                    part for part in coverage_justification_bits if part
                ),
                "justification": "; ".join(part for part in coverage_justification_bits if part),
                "exclusionStatus": exclusion_status,
                "exclusionReason": exclusion_reason,
                "coverageExceptionReason": approved_exception_reason,
                "approvedExceptionBy": approved_exception_by,
                "hqRegion": str(row.get("hqRegion") or row.get("region") or ""),
                "region": region,
                "firstRunOutcome": first_run_outcome,
                "firstRunKeptCount": kept_count,
                "migrationComparison": migration_comparison,
                "ownerMilestone": "M5",
            }
        )

    backlog_rows.sort(
        key=lambda row: (
            0
            if str(row.get("exclusionStatus") or "") == "included"
            else (1 if str(row.get("exclusionStatus") or "") == "deferred" else 2),
            -int(row.get("coveragePriority") or 0),
            str(row.get("candidateIdentityKey") or ""),
        )
    )
    return backlog_rows
