"""Ops health, alerts, schedule, and report summarization for the bridge. Uses injected deps (paths, loaders)."""

from __future__ import annotations

import re
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

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
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    output = int(summary.get("outputCount") or summary.get("uniqueOutputCount") or 0)
    failed = int(summary.get("failedSources") or 0)
    source_count = int(summary.get("sourceCount") or 0)
    duration_ms = 0
    sources = report.get("sources")
    if isinstance(sources, list):
        duration_ms = sum(
            int(item.get("durationMs") or 0) for item in sources if isinstance(item, dict)
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


def summarize_discovery_report(
    report: dict[str, Any],
    normalize_discovery_report_contract: Callable[[dict[str, Any]], dict[str, Any]],
    parse_iso: Callable[[Any], datetime | None],
) -> tuple[dict[str, Any], str]:
    normalized = normalize_discovery_report_contract(report)
    summary = normalized.get("summary") if isinstance(normalized.get("summary"), dict) else {}
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
    if stale_hours is None or stale_hours > STALE_FETCH_HOURS:
        active_conditions.append(
            {
                "id": "stale_fetch",
                "severity": "critical",
                "message": f"No successful fetch in the last {STALE_FETCH_HOURS}h.",
                "value": None if stale_hours is None else round(stale_hours, 2),
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

    outputs = [
        int((row.get("summary") or {}).get("outputCount") or 0)
        for row in fetch_rows
        if int((row.get("summary") or {}).get("outputCount") or 0) > 0
    ]
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

    source_rows = (
        latest_fetch_report.get("sources")
        if isinstance(latest_fetch_report.get("sources"), list)
        else []
    )
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

    social_summary = (
        latest_fetch_report.get("socialSummary")
        if isinstance(latest_fetch_report.get("socialSummary"), dict)
        else {}
    )
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

    active_ids = {row["id"] for row in active_conditions}
    for key in list(acked.keys()):
        if key not in active_ids:
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
    history = deps.get_history()
    latest_fetch_report = deps.get_fetch_report()
    state = deps.get_state()
    schedule = populate_schedule_next_run(
        deps.parse_schedule_metadata_fn(), history, deps.parse_iso
    )
    alerts_meta = evaluate_alerts(
        history=history,
        latest_fetch_report=latest_fetch_report,
        pending_count=len(state.get("pending") or []),
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

    latest_run = history[-1] if history else {}
    severity = derive_ops_severity(alerts_meta["alerts"])
    social_summary = (
        latest_fetch_report.get("socialSummary")
        if isinstance(latest_fetch_report.get("socialSummary"), dict)
        else {}
    )
    owner_state = deps.owner_state if isinstance(getattr(deps, "owner_state", {}), dict) else {}
    updater_status = (
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
            "pendingApprovalsCount": len(state.get("pending") or []),
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
                        "keptCount": int((value or {}).get("keptCount") or 0),
                        "uniqueKeptCount": int((value or {}).get("uniqueKeptCount") or 0),
                        "officialBoardOverlapCount": int(
                            (value or {}).get("officialBoardOverlapCount") or 0
                        ),
                        "duplicateCount": int((value or {}).get("duplicateCount") or 0),
                        "duplicateRate": round(_safe_float((value or {}).get("duplicateRate")), 4),
                        "lowConfidenceDropped": int((value or {}).get("lowConfidenceDropped") or 0),
                    }
                    for key, value in (
                        social_summary.get("channels")
                        if isinstance(social_summary.get("channels"), dict)
                        else {}
                    ).items()
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
