"""Ops health, alerts, schedule, and report summarization for the bridge. Uses injected deps (paths, loaders)."""

from __future__ import annotations

import re
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from src.bridge.registry_sync_summary import derive_registry_sync_summary
from src.shared.json_shapes import as_json_object, json_object_rows

# Constants used by evaluate_alerts and compute_ops_health (mirror admin_bridge defaults)
STALE_FETCH_HOURS = 12
DEGRADED_FAILURE_RATIO = 0.25
OUTPUT_DROP_RATIO = 0.40
SOCIAL_ZERO_MATCH_THRESHOLD = 2
SOCIAL_FAILURE_THRESHOLD = 2
SOCIAL_LOW_CONFIDENCE_SPIKE_THRESHOLD = 120
SOCIAL_UNIQUE_VALUE_THRESHOLD = 0.10
SOCIAL_DUPLICATE_RATE_THRESHOLD = 0.70
SOCIAL_FALSE_POSITIVE_THRESHOLD = 0.05
SOCIAL_FALSE_POSITIVE_SAMPLE_SIZE = 50
NON_DISMISSIBLE_ALERT_IDS = frozenset({"fetch_never_run"})


def _safe_int(value: Any) -> int:
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return 0


def _has_registry_summary_counts(summary: dict[str, Any]) -> bool:
    if str(summary.get("generation") or "").strip():
        return True
    return any(
        key in summary for key in ("activeCount", "pendingCount", "rejectedCount", "tombstoneCount")
    )


def load_alert_state(
    load_json_object: Callable[[Any, dict[str, Any]], dict[str, Any]],
    path: Any,
    schema_version: int,
) -> dict[str, Any]:
    state = load_json_object(path, {})
    acked = state.get("acked")
    if not isinstance(acked, dict):
        acked = {}
    return {
        "schemaVersion": schema_version,
        "acked": {str(k): str(v) for k, v in acked.items()},
    }


def save_alert_state(
    save_json_atomic: Callable[[Any, Any], None],
    path: Any,
    state: dict[str, Any],
    schema_version: int,
    now_iso: Callable[[], str],
) -> None:
    payload = {
        "schemaVersion": schema_version,
        "acked": dict(state.get("acked") or {}),
        "updatedAt": now_iso(),
    }
    save_json_atomic(path, payload)


def detect_task_interval_hours(task: dict[str, Any]) -> float | None:
    text = " ".join(
        [
            str(task.get("label") or ""),
            str(task.get("command") or ""),
            str(task.get("detail") or ""),
        ]
    ).lower()
    match_hours = re.search(r"every\s+(\d+(?:\.\d+)?)\s*(h|hour|hours)\b", text)
    if match_hours:
        return max(0.1, float(match_hours.group(1)))
    match_minutes = re.search(r"every\s+(\d+(?:\.\d+)?)\s*(m|min|minute|minutes)\b", text)
    if match_minutes:
        return max(1.0, float(match_minutes.group(1))) / 60.0
    match_flag = re.search(r"--every-hours\s+(\d+(?:\.\d+)?)", text)
    if match_flag:
        return max(0.1, float(match_flag.group(1)))
    return None


def parse_schedule_metadata(
    read_tasks_config: Callable[[], dict[str, Any]],
) -> dict[str, Any]:
    fallback = {
        "fetcher": {"intervalHours": None, "nextRunAt": "", "note": "unknown"},
        "discovery": {"intervalHours": None, "nextRunAt": "", "note": "unknown"},
    }
    try:
        payload = read_tasks_config()
    except Exception:  # noqa: BLE001
        return fallback
    tasks = payload.get("tasks")
    if not isinstance(tasks, list):
        return fallback

    by_type: dict[str, dict[str, Any]] = {
        "fetcher": dict(fallback["fetcher"]),
        "discovery": dict(fallback["discovery"]),
    }
    for task in tasks:
        if not isinstance(task, dict):
            continue
        command = str(task.get("command") or "").lower()
        label = str(task.get("label") or "").lower()
        interval = detect_task_interval_hours(task)
        if "jobs_fetcher.py" in command or "run jobs fetcher" in label:
            by_type["fetcher"]["intervalHours"] = interval
            by_type["fetcher"]["note"] = "inferred" if interval else "manual_task"
        if "source_discovery.py" in command or "run source discovery" in label:
            by_type["discovery"]["intervalHours"] = interval
            by_type["discovery"]["note"] = "inferred" if interval else "manual_task"
    return by_type


def median(values: list[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return float(ordered[mid])
    return float((ordered[mid - 1] + ordered[mid]) / 2.0)


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def summarize_fetch_report(report: dict[str, Any]) -> dict[str, Any]:
    summary = as_json_object(report.get("summary"))
    output = int(summary.get("outputCount") or summary.get("uniqueOutputCount") or 0)
    failed = int(summary.get("failedSources") or 0)
    source_count = int(summary.get("sourceCount") or 0)
    duration_ms = 0
    duration_ms = sum(
        int(item.get("durationMs") or 0) for item in json_object_rows(report.get("sources"))
    )
    status = "ok"
    if source_count > 0 and failed >= source_count:
        status = "error"
    elif failed > 0:
        status = "warning"
    return {
        "status": status,
        "outputCount": output,
        "failedSources": failed,
        "sourceCount": source_count,
        "durationMs": duration_ms,
        "failedRatio": (failed / source_count) if source_count > 0 else 0.0,
    }


def summarize_dedup_review_state(report: dict[str, Any]) -> dict[str, Any]:
    summary = as_json_object(report.get("dedupReviewStateSummary"))
    export = as_json_object(report.get("dedupReviewStateExport"))
    dedup_evidence = as_json_object(report.get("dedupEvidence"))
    gate_counts = as_json_object(dedup_evidence.get("providerStaticDisagreementGateCounts"))
    artifact_path = str(
        summary.get("artifactPath") or export.get("artifactPath") or "data/dedup-review-state.json"
    )
    read_warning = str(
        summary.get("readWarning") or report.get("dedupReviewStateReadWarning") or ""
    )
    reviewed_safe_count = int(
        summary.get("reviewedSafeCount") or gate_counts.get("reviewedSafeWarning") or 0
    )
    confirmed_blocking_count = int(
        summary.get("confirmedBlockingCount") or gate_counts.get("confirmedBlocking") or 0
    )
    reviewed_pair_count = int(
        summary.get("reviewedPairCount") or (reviewed_safe_count + confirmed_blocking_count)
    )
    unresolved_blocking_count = int(
        summary.get("unresolvedBlockingCount") or gate_counts.get("blocked") or 0
    )
    status = str(summary.get("status") or ("warning" if read_warning else "ok"))
    return {
        "artifactPath": artifact_path,
        "status": status,
        "readWarning": read_warning,
        "reviewedPairCount": reviewed_pair_count,
        "reviewedSafeCount": reviewed_safe_count,
        "confirmedBlockingCount": confirmed_blocking_count,
        "unresolvedBlockingCount": unresolved_blocking_count,
    }


def summarize_discovery_report(
    report: dict[str, Any],
    normalize_discovery_report_contract: Callable[[dict[str, Any]], dict[str, Any]],
    parse_iso: Callable[[Any], datetime | None],
) -> tuple[dict[str, Any], str]:
    normalized = normalize_discovery_report_contract(report)
    summary = as_json_object(normalized.get("summary"))
    queued = int(summary.get("queuedCandidateCount") or 0)
    failed = int(summary.get("failedProbeCount") or 0)
    probed = int(summary.get("probedCandidateCount") or summary.get("probedCount") or 0)
    duration_ms = 0
    started = parse_iso(report.get("startedAt"))
    finished = parse_iso(report.get("finishedAt"))
    if started and finished:
        duration_ms = int(max(0.0, (finished - started).total_seconds() * 1000))
    status = "ok"
    if probed > 0 and failed >= probed:
        status = "error"
    elif failed > 0:
        status = "warning"
    return (
        {
            "queuedCandidateCount": queued,
            "failedProbeCount": failed,
            "probedCandidateCount": probed,
            "durationMs": duration_ms,
        },
        status,
    )


def format_age(
    finished_at: str,
    parse_iso: Callable[[Any], datetime | None],
    now_utc: Callable[[], datetime],
) -> str:
    dt = parse_iso(finished_at)
    if not dt:
        return "unknown"
    delta = now_utc() - dt
    total_minutes = int(max(0.0, delta.total_seconds() // 60))
    if total_minutes < 60:
        return f"{total_minutes}m"
    hours = total_minutes // 60
    if hours < 48:
        return f"{hours}h"
    return f"{hours // 24}d"


def collect_fetch_history_metrics(
    history: list[dict[str, Any]],
    parse_iso: Callable[[Any], datetime | None],
    now_utc: Callable[[], datetime],
) -> dict[str, Any]:
    now = now_utc()
    seven_days_ago = now - timedelta(days=7)
    fetch_rows = [
        row for row in history if str(row.get("type")) == "fetch" and row.get("finishedAt")
    ]
    fetch_7d = [
        row
        for row in fetch_rows
        if (parse_iso(row.get("finishedAt")) or datetime.min.replace(tzinfo=UTC)) >= seven_days_ago
    ]
    success_7d = [row for row in fetch_7d if str(row.get("status")) in {"ok", "warning"}]
    success_rate = (len(success_7d) / len(fetch_7d)) if fetch_7d else 0.0
    avg_duration = (
        int(sum(int(row.get("durationMs") or 0) for row in fetch_7d) / len(fetch_7d))
        if fetch_7d
        else 0
    )
    latest_fetch = fetch_rows[-1] if fetch_rows else None
    last_success = next(
        (row for row in reversed(fetch_rows) if str(row.get("status")) in {"ok", "warning"}), None
    )
    return {
        "fetchRows": fetch_rows,
        "successRate7d": success_rate,
        "avgDurationMs7d": avg_duration,
        "latestFetch": latest_fetch,
        "lastSuccessFetch": last_success,
    }


def populate_schedule_next_run(
    schedule: dict[str, Any],
    history: list[dict[str, Any]],
    parse_iso: Callable[[Any], datetime | None],
) -> dict[str, Any]:
    for run_type, key in (("fetch", "fetcher"), ("discovery", "discovery")):
        interval_hours = schedule[key].get("intervalHours")
        if not interval_hours:
            schedule[key]["nextRunAt"] = ""
            continue
        last_type_row = next(
            (
                row
                for row in reversed(history)
                if str(row.get("type")) == run_type and row.get("finishedAt")
            ),
            None,
        )
        last_finished = parse_iso(last_type_row.get("finishedAt")) if last_type_row else None
        if last_finished:
            schedule[key]["nextRunAt"] = (
                last_finished + timedelta(hours=float(interval_hours))
            ).isoformat()
        else:
            schedule[key]["nextRunAt"] = ""
    return schedule


def derive_ops_severity(alerts: list[dict[str, Any]]) -> str:
    if any(alert.get("severity") == "critical" for alert in alerts):
        return "critical"
    if alerts:
        return "warning"
    return "healthy"


def evaluate_alerts(
    *,
    history: list[dict[str, Any]],
    latest_fetch_report: dict[str, Any],
    pending_count: int,
    load_alert_state_fn: Callable[[], dict[str, Any]],
    save_alert_state_fn: Callable[[dict[str, Any]], None],
    parse_iso: Callable[[Any], datetime | None],
    now_iso: Callable[[], str],
    now_utc: Callable[[], datetime],
) -> dict[str, Any]:
    alert_state = load_alert_state_fn()
    acked = dict(alert_state.get("acked") or {})
    active_conditions: list[dict[str, Any]] = []
    now = now_utc()
    fetch_rows = [
        row for row in history if str(row.get("type")) == "fetch" and row.get("finishedAt")
    ]
    latest_fetch = fetch_rows[-1] if fetch_rows else None
    last_success_fetch = next(
        (row for row in reversed(fetch_rows) if str(row.get("status")) in {"ok", "warning"}),
        None,
    )
    stale_hours = None
    if last_success_fetch:
        finished = parse_iso(last_success_fetch.get("finishedAt"))
        if finished:
            stale_hours = (now - finished).total_seconds() / 3600.0
    if stale_hours is None:
        active_conditions.append(
            {
                "id": "fetch_never_run",
                "severity": "warning",
                "message": "No successful fetch has run yet. Run Jobs Fetcher to update the jobs listing.",
                "value": None,
                "triggeredAt": now_iso(),
            }
        )
    elif stale_hours > STALE_FETCH_HOURS:
        active_conditions.append(
            {
                "id": "stale_fetch",
                "severity": "warning",
                "message": f"Last successful fetch is older than {STALE_FETCH_HOURS}h. A full Jobs Fetcher run is suggested to update the jobs listing.",
                "value": round(stale_hours, 2),
                "triggeredAt": now_iso(),
            }
        )

    fetch_summary = summarize_fetch_report(latest_fetch_report)
    failed_ratio = float(fetch_summary["failedRatio"])
    if failed_ratio > DEGRADED_FAILURE_RATIO:
        active_conditions.append(
            {
                "id": "degraded_reliability",
                "severity": "warning" if failed_ratio < 0.5 else "critical",
                "message": f"Failed source ratio is {failed_ratio:.0%} (threshold {DEGRADED_FAILURE_RATIO:.0%}).",
                "value": round(failed_ratio, 4),
                "triggeredAt": now_iso(),
            }
        )

    outputs: list[int] = []
    for row in fetch_rows:
        summary = as_json_object(row.get("summary"))
        output_count = int(summary.get("outputCount") or 0)
        if output_count > 0:
            outputs.append(output_count)
    if len(outputs) >= 4 and latest_fetch:
        baseline_values = outputs[:-1] if len(outputs) > 1 else outputs
        baseline = median([float(v) for v in baseline_values[-10:]])
        latest_output = float(outputs[-1])
        if baseline > 0 and latest_output < baseline * (1.0 - OUTPUT_DROP_RATIO):
            drop_ratio = 1.0 - (latest_output / baseline)
            active_conditions.append(
                {
                    "id": "output_drop",
                    "severity": "warning" if drop_ratio < 0.6 else "critical",
                    "message": f"Output dropped {drop_ratio:.0%} vs rolling median.",
                    "value": round(drop_ratio, 4),
                    "triggeredAt": now_iso(),
                }
            )

    source_rows = json_object_rows(latest_fetch_report.get("sources"))
    social_rows = [
        row
        for row in source_rows
        if isinstance(row, dict)
        and str(row.get("name") or "").strip().lower().startswith("social_")
    ]
    if social_rows:
        social_failures = [
            row for row in social_rows if str(row.get("status") or "").strip().lower() == "error"
        ]
        if len(social_failures) >= SOCIAL_FAILURE_THRESHOLD:
            active_conditions.append(
                {
                    "id": "social_sources_failing",
                    "severity": "warning" if len(social_failures) < 3 else "critical",
                    "message": f"{len(social_failures)} social sources failed in the latest run.",
                    "value": int(len(social_failures)),
                    "triggeredAt": now_iso(),
                }
            )

        zero_rows = [
            row
            for row in social_rows
            if str(row.get("status") or "").strip().lower() in {"ok", "error"}
            and int(row.get("keptCount") or 0) == 0
        ]
        if len(zero_rows) >= SOCIAL_ZERO_MATCH_THRESHOLD:
            active_conditions.append(
                {
                    "id": "social_zero_matches",
                    "severity": "warning",
                    "message": f"{len(zero_rows)} social sources produced zero matches in the latest run.",
                    "value": int(len(zero_rows)),
                    "triggeredAt": now_iso(),
                }
            )

        low_conf_dropped = sum(int(row.get("lowConfidenceDropped") or 0) for row in social_rows)
        if low_conf_dropped >= SOCIAL_LOW_CONFIDENCE_SPIKE_THRESHOLD:
            active_conditions.append(
                {
                    "id": "social_low_confidence_spike",
                    "severity": "warning",
                    "message": "Social ingestion dropped an unusually high number of low-confidence posts.",
                    "value": int(low_conf_dropped),
                    "triggeredAt": now_iso(),
                }
            )

    social_summary = as_json_object(latest_fetch_report.get("socialSummary"))
    if social_summary:
        kept_count = int(social_summary.get("keptCount") or 0)
        unique_count = int(social_summary.get("uniqueKeptCount") or 0)
        duplicate_rate = _safe_float(social_summary.get("duplicateRate"))
        reviewed_count = int(social_summary.get("reviewedCount") or 0)
        false_positive_rate = _safe_float(social_summary.get("falsePositiveRate"))
        unique_share = (unique_count / kept_count) if kept_count > 0 else 0.0
        if kept_count > 0 and unique_share < SOCIAL_UNIQUE_VALUE_THRESHOLD:
            active_conditions.append(
                {
                    "id": "social_low_unique_value",
                    "severity": "warning",
                    "message": f"Social kept share is {unique_share:.0%} (threshold {SOCIAL_UNIQUE_VALUE_THRESHOLD:.0%}).",
                    "value": round(unique_share, 4),
                    "triggeredAt": now_iso(),
                }
            )
        if kept_count > 0 and duplicate_rate > SOCIAL_DUPLICATE_RATE_THRESHOLD:
            active_conditions.append(
                {
                    "id": "social_duplicate_rate_high",
                    "severity": "warning",
                    "message": f"Social duplicate rate is {duplicate_rate:.0%} (threshold {SOCIAL_DUPLICATE_RATE_THRESHOLD:.0%}).",
                    "value": round(duplicate_rate, 4),
                    "triggeredAt": now_iso(),
                }
            )
        if (
            reviewed_count >= SOCIAL_FALSE_POSITIVE_SAMPLE_SIZE
            and false_positive_rate > SOCIAL_FALSE_POSITIVE_THRESHOLD
        ):
            active_conditions.append(
                {
                    "id": "social_false_positive_spike",
                    "severity": "warning",
                    "message": f"Manual social false-positive rate is {false_positive_rate:.0%} (threshold {SOCIAL_FALSE_POSITIVE_THRESHOLD:.0%}).",
                    "value": round(false_positive_rate, 4),
                    "triggeredAt": now_iso(),
                }
            )

    for row in active_conditions:
        row["dismissible"] = str(row.get("id") or "") not in NON_DISMISSIBLE_ALERT_IDS

    active_ids = {row["id"] for row in active_conditions}
    non_dismissible_ids = {
        row["id"] for row in active_conditions if not bool(row.get("dismissible", True))
    }
    for key in list(acked.keys()):
        if key not in active_ids or key in non_dismissible_ids:
            acked.pop(key, None)

    visible_alerts = [row for row in active_conditions if row["id"] not in acked]
    save_alert_state_fn({"acked": acked})
    return {
        "alerts": visible_alerts,
        "suppressedCount": max(0, len(active_conditions) - len(visible_alerts)),
        "pendingApprovals": int(pending_count),
    }


def compute_ops_health(deps: Any) -> dict[str, Any]:
    """Build ops health payload. deps must have: get_history, get_fetch_report, get_state,
    now_iso, desktop_mode, desktop_last_activity_at, load_alert_state_fn, save_alert_state_fn,
    parse_schedule_metadata_fn, normalize_fetch_report_contract, parse_iso, now_utc.
    """
    history: list[dict[str, Any]] = deps.get_history()
    latest_fetch_report: dict[str, Any] = deps.get_fetch_report()
    registry_summary: dict[str, Any] = {}
    get_registry_summary_payload = getattr(deps, "get_registry_summary_payload", None)
    if callable(get_registry_summary_payload):
        try:
            registry_summary = as_json_object(get_registry_summary_payload())
        except Exception:  # noqa: BLE001
            registry_summary = {}
    if not _has_registry_summary_counts(registry_summary):
        registry_summary = {}
    state: dict[str, Any] = {}
    if not registry_summary:
        state = deps.get_state()
    schedule = populate_schedule_next_run(
        deps.parse_schedule_metadata_fn(), history, deps.parse_iso
    )
    alerts_meta = evaluate_alerts(
        history=history,
        latest_fetch_report=latest_fetch_report,
        pending_count=(
            _safe_int(registry_summary.get("pendingCount"))
            if registry_summary
            else len(state.get("pending") or [])
        ),
        load_alert_state_fn=deps.load_alert_state_fn,
        save_alert_state_fn=deps.save_alert_state_fn,
        parse_iso=deps.parse_iso,
        now_iso=deps.now_iso,
        now_utc=deps.now_utc,
    )

    metrics = collect_fetch_history_metrics(history, deps.parse_iso, deps.now_utc)
    last_success = metrics["lastSuccessFetch"]
    latest_fetch_summary = summarize_fetch_report(latest_fetch_report)
    failed_ratio_latest = latest_fetch_summary["failedRatio"]
    source_health = as_json_object(latest_fetch_report.get("sourceHealth"))
    provider_coverage = as_json_object(latest_fetch_report.get("providerCoverage"))
    provider_static_overlap = as_json_object(latest_fetch_report.get("providerStaticOverlap"))
    static_suppression_policy = as_json_object(latest_fetch_report.get("staticSuppressionPolicy"))
    redundant_static_proposals = as_json_object(latest_fetch_report.get("redundantStaticProposals"))
    source_policy_soak_report = as_json_object(deps.get_source_policy_soak_report())
    conservative_static_cleanup_proposals = as_json_object(
        as_json_object(source_policy_soak_report.get("sections")).get(
            "conservativeStaticCleanupProposals"
        )
    )
    source_policy_recommendation_export = as_json_object(
        latest_fetch_report.get("sourcePolicyRecommendationExport")
    )
    dedup_review_state = summarize_dedup_review_state(latest_fetch_report)
    try:
        tombstones = deps.get_tombstones()
    except Exception:  # noqa: BLE001
        tombstones = {}
    try:
        sync_status = deps.get_sync_status_payload()
    except Exception:  # noqa: BLE001
        sync_status = {}
    registry_sync = derive_registry_sync_summary(
        state=state,
        summary=registry_summary,
        tombstones=tombstones,
        sync_status=sync_status,
        history=history,
    )

    latest_run = history[-1] if history else {}
    severity = derive_ops_severity(alerts_meta["alerts"])
    social_summary = as_json_object(latest_fetch_report.get("socialSummary"))
    social_channels = as_json_object(social_summary.get("channels"))
    owner_state = as_json_object(getattr(deps, "owner_state", {}))
    updater_status = as_json_object(
        deps.get_updater_status_payload()
        if callable(getattr(deps, "get_updater_status_payload", None))
        else {}
    )
    startup_ready = (
        bool(getattr(deps, "startup_ready", True))
        if hasattr(deps, "startup_ready")
        else (True if not bool(deps.desktop_mode) else bool(owner_state.get("startedAt")))
    )
    app_version = str(getattr(deps, "app_version", "") or "")

    return {
        "service": "baluffo-bridge",
        "desktopMode": bool(deps.desktop_mode),
        "appVersion": app_version,
        "startupReady": bool(startup_ready),
        "generatedAt": deps.now_iso(),
        "desktopLastActivityAt": str(deps.desktop_last_activity_at or ""),
        "owner": {
            "mode": str(owner_state.get("ownerMode") or ""),
            "token": str(owner_state.get("ownerToken") or ""),
            "startedBy": str(owner_state.get("startedBy") or ""),
            "startedAt": str(owner_state.get("startedAt") or ""),
            "lastActivityAt": str(owner_state.get("lastActivityAt") or ""),
            "idleTimeoutSeconds": float(owner_state.get("idleTimeoutSeconds") or 0.0),
        },
        "status": severity,
        "kpis": {
            "lastSuccessfulFetchAge": format_age(
                last_success.get("finishedAt") if last_success else "",
                deps.parse_iso,
                deps.now_utc,
            ),
            "sevenDayFetchSuccessRate": round(float(metrics["successRate7d"]), 4),
            "avgFetchDurationMs7d": int(metrics["avgDurationMs7d"]),
            "failedSourceRatioLatest": round(float(failed_ratio_latest), 4),
            "pendingApprovalsCount": (
                _safe_int(registry_summary.get("pendingCount"))
                if registry_summary
                else len(state.get("pending") or [])
            ),
            "sourceHealth": source_health,
            "providerCoverage": provider_coverage,
            "dedupReviewState": dedup_review_state,
            "providerStaticOverlap": provider_static_overlap,
            "staticSuppressionPolicy": static_suppression_policy,
            "redundantStaticProposals": redundant_static_proposals,
            "conservativeStaticCleanupProposals": conservative_static_cleanup_proposals,
            "sourcePolicyRecommendationExport": source_policy_recommendation_export,
            "registrySync": registry_sync,
            "socialExperiment": {
                "pilotWindowStartAt": str(social_summary.get("pilotWindowStartAt") or ""),
                "pilotWindowEndAt": str(social_summary.get("pilotWindowEndAt") or ""),
                "scheduledRunCount": int(social_summary.get("scheduledRunCount") or 0),
                "keptCount": int(social_summary.get("keptCount") or 0),
                "uniqueKeptCount": int(social_summary.get("uniqueKeptCount") or 0),
                "officialBoardOverlapCount": int(
                    social_summary.get("officialBoardOverlapCount") or 0
                ),
                "duplicateCount": int(social_summary.get("duplicateCount") or 0),
                "duplicateRate": round(_safe_float(social_summary.get("duplicateRate")), 4),
                "lowConfidenceDropped": int(social_summary.get("lowConfidenceDropped") or 0),
                "sampleSize": int(social_summary.get("sampleSize") or 0),
                "reviewedCount": int(social_summary.get("reviewedCount") or 0),
                "falsePositiveCount": int(social_summary.get("falsePositiveCount") or 0),
                "falsePositiveRate": round(_safe_float(social_summary.get("falsePositiveRate")), 4),
                "reviewArtifactPath": str(social_summary.get("reviewArtifactPath") or ""),
                "channels": {
                    str(key): {
                        "keptCount": int(as_json_object(value).get("keptCount") or 0),
                        "uniqueKeptCount": int(as_json_object(value).get("uniqueKeptCount") or 0),
                        "officialBoardOverlapCount": int(
                            as_json_object(value).get("officialBoardOverlapCount") or 0
                        ),
                        "duplicateCount": int(as_json_object(value).get("duplicateCount") or 0),
                        "duplicateRate": round(
                            _safe_float(as_json_object(value).get("duplicateRate")), 4
                        ),
                        "lowConfidenceDropped": int(
                            as_json_object(value).get("lowConfidenceDropped") or 0
                        ),
                    }
                    for key, value in social_channels.items()
                    if str(key).strip()
                },
            },
            "lastRunResult": {
                "type": str(latest_run.get("type") or ""),
                "status": str(latest_run.get("status") or "unknown"),
                "finishedAt": str(
                    latest_run.get("finishedAt") or latest_run.get("startedAt") or ""
                ),
            },
        },
        "schedule": schedule,
        "alerts": alerts_meta["alerts"],
        "suppressedAlertsCount": int(alerts_meta["suppressedCount"]),
        "historyCount": len(history),
        "updater": {
            "currentVersion": str(updater_status.get("currentVersion") or app_version),
            "latestVersion": str(updater_status.get("latestVersion") or ""),
            "availability": str(updater_status.get("availability") or "unknown"),
            "downloadState": str(updater_status.get("downloadState") or "idle"),
            "installState": str(updater_status.get("installState") or "idle"),
            "lastCheckedAt": str(updater_status.get("lastCheckedAt") or ""),
            "lastError": str(updater_status.get("lastError") or ""),
        },
    }
