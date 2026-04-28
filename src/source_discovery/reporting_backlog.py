from __future__ import annotations

from collections import Counter
from typing import Any

from src.source_registry import source_identity

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
_M5_REGION_TOKENS = (
    ("asia", M5_ASIA_TOKENS),
    ("europe", M5_EUROPE_TOKENS),
    ("north_america", M5_NORTH_AMERICA_TOKENS),
    ("south_america", M5_SOUTH_AMERICA_TOKENS),
    ("oceania", M5_OCEANIA_TOKENS),
    ("africa", M5_AFRICA_TOKENS),
)


def _as_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


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
    for region, tokens in _M5_REGION_TOKENS:
        if any(token in text for token in tokens):
            return region
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
    failure = failure_row if isinstance(failure_row, dict) else {}
    if exclusion_reason in M5_HARD_EXCLUSION_REASONS:
        return exclusion_reason
    if str(failure.get("dropStage") or "").strip().lower() in M5_SUPPRESSION_STAGES:
        if exclusion_reason:
            return exclusion_reason
        return str(failure.get("dropReason") or failure.get("error") or "suppressed_static")
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
    before: dict[str, Any] = {
        "durationMs": _as_int(state_entry.get("structuredMigrationBaselineDurationMs")),
        "status": str(state_entry.get("structuredMigrationBaselineStatus") or "").strip(),
        "error": str(state_entry.get("structuredMigrationBaselineError") or "").strip(),
        "failureBucket": str(
            state_entry.get("structuredMigrationBaselineFailureBucket") or ""
        ).strip(),
        "keptCount": _as_int(state_entry.get("structuredMigrationBaselineKeptCount")),
    }
    after: dict[str, Any] = {
        "durationMs": _as_int(state_entry.get("lastDurationMs")),
        "status": str(state_entry.get("lastStatus") or "").strip(),
        "error": str(state_entry.get("lastError") or "").strip(),
        "failureBucket": str(state_entry.get("lastFailureBucket") or "").strip(),
        "keptCount": _as_int(state_entry.get("lastKeptCount")),
    }
    comparison: dict[str, Any] = {
        "before": before,
        "after": after,
        "shadowRunCount": int(state_entry.get("structuredMigrationShadowRunCount") or 0),
        "healthyRunCount": int(state_entry.get("structuredMigrationHealthyRunCount") or 0),
        "promotedAt": str(state_entry.get("structuredMigrationPromotedAt") or "").strip(),
        "demotedAt": str(state_entry.get("structuredMigrationDemotedAt") or "").strip(),
        "rollbackChecklist": list(M5_STRUCTURED_MIGRATION_ROLLBACK_CHECKLIST),
    }
    comparison["runtimeDeltaMs"] = _as_int(after["durationMs"]) - _as_int(before["durationMs"])
    comparison["keptCountDelta"] = _as_int(after["keptCount"]) - _as_int(before["keptCount"])
    if not any(
        [
            before["durationMs"],
            after["durationMs"],
            before["status"],
            after["status"],
            before["error"],
            after["error"],
            before["failureBucket"],
            after["failureBucket"],
            before["keptCount"],
            after["keptCount"],
            comparison["shadowRunCount"],
            comparison["healthyRunCount"],
            comparison["promotedAt"],
            comparison["demotedAt"],
        ]
    ):
        return {}
    return comparison


def _m5_index_rows(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {_candidate_join_key(row): dict(row) for row in rows or [] if isinstance(row, dict)}


def _m5_region_counts(
    active_rows: list[dict[str, Any]],
    source_state_rows: dict[str, dict[str, Any]],
) -> tuple[Counter[str], Counter[str]]:
    live_counts: Counter[str] = Counter()
    kept_counts: Counter[str] = Counter()
    for row in active_rows:
        region = _region_category(row)
        if region == "unknown":
            continue
        live_counts[region] += 1
        kept_count = int(_lookup_state_entry(row, source_state_rows).get("lastKeptCount") or 0)
        if kept_count > 0:
            kept_counts[region] += 1
    return live_counts, kept_counts


def _m5_initial_exclusion_reason(row: dict[str, Any], failure: dict[str, Any]) -> str:
    return str(
        failure.get("dropReason")
        or row.get("dropReason")
        or row.get("deferReason")
        or row.get("error")
        or ""
    ).strip()


def _m5_exception_fields(row: dict[str, Any]) -> tuple[str, str, bool]:
    reason = str(row.get("coverageExceptionReason") or row.get("exceptionReason") or "").strip()
    approved_by = str(row.get("approvedExceptionBy") or row.get("approvedBy") or "").strip()
    return reason, approved_by, bool(reason and approved_by)


def _m5_deferred(row: dict[str, Any]) -> bool:
    return bool(row.get("deferred")) or (
        str(row.get("dropStage") or "").strip().lower() == "deferred_by_cap"
    )


def _m5_exclusion_status(
    row: dict[str, Any],
    *,
    is_m4_family: bool,
    has_exception: bool,
    exclusion_reason: str,
) -> tuple[str, str]:
    if is_m4_family and not has_exception:
        return "excluded", exclusion_reason or "m4_family_followup"
    if exclusion_reason and not has_exception:
        return "excluded", exclusion_reason
    if _m5_deferred(row):
        return "deferred", exclusion_reason or str(row.get("deferReason") or "deferred").strip()
    return "included", exclusion_reason


def _m5_coverage_lane(
    *,
    is_m4_family: bool,
    region: str,
    exclusion_status: str,
) -> str:
    if is_m4_family:
        return "lane_a_m4_followup"
    if region == "asia":
        return "lane_c_asia_custom"
    if exclusion_status in {"excluded", "deferred"}:
        return "lane_d_defer"
    return "lane_b_custom"


def _m5_jobs_found(row: dict[str, Any]) -> int:
    return max(0, int(row.get("jobsFound") or row.get("sampleCount") or 0))


def _m5_weak_region_coverage(
    region: str,
    *,
    live_counts: Counter[str],
    kept_counts: Counter[str],
) -> bool:
    return region != "unknown" and (
        int(live_counts.get(region) or 0) < 5 or int(kept_counts.get(region) or 0) < 3
    )


def _m5_priority_and_reasons(
    row: dict[str, Any],
    *,
    region: str,
    jobs_found: int,
    weak_region_coverage: bool,
    is_m4_family: bool,
    exclusion_reason: str,
) -> tuple[int, list[str]]:
    rank_reasons = _m5_rank_reasons(row)
    if exclusion_reason and exclusion_reason not in rank_reasons:
        rank_reasons = unique_string_list([*rank_reasons, exclusion_reason])
    priority = _m5_base_priority(row)
    if region == "asia":
        priority += 2
        rank_reasons = unique_string_list([*rank_reasons, "asia_hq"])
    if jobs_found > 0:
        priority += 2
        rank_reasons = unique_string_list([*rank_reasons, "open_role_evidence"])
    if weak_region_coverage:
        priority += 2
        rank_reasons = unique_string_list([*rank_reasons, "weak_regional_coverage"])
    if is_m4_family:
        priority -= 3
        rank_reasons = unique_string_list([*rank_reasons, "m4_family_followup"])
    if exclusion_reason in {"existing_id", "existing_domain"}:
        priority -= 3
        rank_reasons = unique_string_list([*rank_reasons, "existing_coverage_match"])
    return max(0, min(100, int(priority))), rank_reasons


def _m5_coverage_justification(
    *,
    is_m4_family: bool,
    region: str,
    jobs_found: int,
    weak_region_coverage: bool,
    exclusion_status: str,
    exclusion_reason: str,
    has_exception: bool,
    approved_exception_reason: str,
) -> str:
    bits = []
    if is_m4_family:
        bits.append("structured-family follow-up")
    elif region == "asia":
        bits.append("asia-priority custom target")
    else:
        bits.append("custom coverage target")
    if jobs_found > 0:
        bits.append("open-role evidence")
    if weak_region_coverage:
        bits.append("weak regional coverage")
    if exclusion_status == "excluded" and exclusion_reason:
        bits.append(f"excluded:{exclusion_reason}")
    elif exclusion_status == "deferred" and exclusion_reason:
        bits.append(f"deferred:{exclusion_reason}")
    if has_exception:
        bits.append(f"approved exception:{approved_exception_reason}")
    return "; ".join(part for part in bits if part)


def _m5_backlog_row(
    *,
    identity_key: str,
    row: dict[str, Any],
    failure: dict[str, Any],
    source_state_rows: dict[str, dict[str, Any]],
    live_counts: Counter[str],
    kept_counts: Counter[str],
) -> dict[str, Any]:
    adapter = str(row.get("adapter") or failure.get("adapter") or "").strip().lower()
    region = _region_category(row)
    state_entry = _lookup_state_entry(row, source_state_rows)
    kept_count = int(
        row.get("firstRunKeptCount")
        or state_entry.get("lastKeptCount")
        or row.get("lastKeptCount")
        or 0
    )
    exclusion_reason = _m5_hard_exclusion_reason(
        row,
        failure,
        exclusion_reason=_m5_initial_exclusion_reason(row, failure),
    )
    approved_exception_reason, approved_exception_by, has_exception = _m5_exception_fields(row)
    is_m4_family = adapter in M5_ALLOWED_M4_FAMILIES
    exclusion_status, exclusion_reason = _m5_exclusion_status(
        row,
        is_m4_family=is_m4_family,
        has_exception=has_exception,
        exclusion_reason=exclusion_reason,
    )
    coverage_lane = _m5_coverage_lane(
        is_m4_family=is_m4_family,
        region=region,
        exclusion_status=exclusion_status,
    )
    if coverage_lane not in M5_COVERAGE_LANES:
        coverage_lane = "lane_d_defer"
    jobs_found = _m5_jobs_found(row)
    weak_region_coverage = _m5_weak_region_coverage(
        region,
        live_counts=live_counts,
        kept_counts=kept_counts,
    )
    coverage_priority, rank_reasons = _m5_priority_and_reasons(
        row,
        region=region,
        jobs_found=jobs_found,
        weak_region_coverage=weak_region_coverage,
        is_m4_family=is_m4_family,
        exclusion_reason=exclusion_reason,
    )
    justification = _m5_coverage_justification(
        is_m4_family=is_m4_family,
        region=region,
        jobs_found=jobs_found,
        weak_region_coverage=weak_region_coverage,
        exclusion_status=exclusion_status,
        exclusion_reason=exclusion_reason,
        has_exception=has_exception,
        approved_exception_reason=approved_exception_reason,
    )
    return {
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
        "coverageJustification": justification,
        "justification": justification,
        "exclusionStatus": exclusion_status,
        "exclusionReason": exclusion_reason,
        "coverageExceptionReason": approved_exception_reason,
        "approvedExceptionBy": approved_exception_by,
        "hqRegion": str(row.get("hqRegion") or row.get("region") or ""),
        "region": region,
        "firstRunOutcome": _m5_first_run_outcome(
            row,
            state_entry=state_entry,
            kept_count=kept_count,
        ),
        "firstRunKeptCount": kept_count,
        "migrationComparison": _m5_structured_migration_comparison(
            row,
            state_entry=state_entry,
        ),
        "ownerMilestone": "M5",
    }


def _m5_backlog_sort_key(row: dict[str, Any]) -> tuple[int, int, str]:
    status = str(row.get("exclusionStatus") or "")
    status_rank = 0 if status == "included" else (1 if status == "deferred" else 2)
    return (
        status_rank,
        -int(row.get("coveragePriority") or 0),
        str(row.get("candidateIdentityKey") or ""),
    )


def build_m5_strategic_backlog(
    *,
    report_candidates: list[dict[str, Any]],
    failures: list[dict[str, Any]],
    active_rows: list[dict[str, Any]] | None = None,
    source_state_rows: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    active_rows = [dict(row) for row in active_rows or [] if isinstance(row, dict)]
    source_state_rows = source_state_rows if isinstance(source_state_rows, dict) else {}
    candidate_rows = _m5_index_rows(report_candidates)
    failure_rows = _m5_index_rows(failures)
    region_live_counts, region_kept_counts = _m5_region_counts(active_rows, source_state_rows)

    backlog_rows: list[dict[str, Any]] = []
    for identity_key in sorted({*candidate_rows.keys(), *failure_rows.keys()}):
        candidate = candidate_rows.get(identity_key) or {}
        failure = failure_rows.get(identity_key) or {}
        row = dict(candidate or failure)
        if not row:
            continue
        backlog_rows.append(
            _m5_backlog_row(
                identity_key=identity_key,
                row=row,
                failure=failure,
                source_state_rows=source_state_rows,
                live_counts=region_live_counts,
                kept_counts=region_kept_counts,
            )
        )

    backlog_rows.sort(key=_m5_backlog_sort_key)
    return backlog_rows
