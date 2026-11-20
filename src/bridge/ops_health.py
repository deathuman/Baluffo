"""Ops health, alerts, schedule, and report summarization for the bridge. Uses injected deps (paths, loaders).

AI boundary owns: ops health readiness, alert evaluation, schedule summaries, and report health rollups.
AI boundary implement in: this file for health policy; route response wiring stays in ops route leaves.
AI boundary search before contracts: ops status routes, pipeline service, sync service, and frontend admin health callers.
AI boundary verify: `npm run lint:repo-guardrails` plus focused ops health tests.
"""

from __future__ import annotations

import math
import re
from collections.abc import Callable
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
from typing import Any

from src.bridge.performance_profile import time_operation
from src.bridge.registry_sync_summary import derive_registry_sync_summary
from src.shared.coerce import as_float as _safe_float
from src.shared.json_shapes import as_json_object, json_object_rows
from src.shared.utils import coerce_non_negative_int as _safe_int

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
NON_DISMISSIBLE_ALERT_IDS = frozenset({"fetch_never_run", "pipeline_never_run"})


def _has_registry_summary_counts(summary: dict[str, Any]) -> bool:
    if str(summary.get("generation") or "").strip():
        return True
    return any(
        key in summary for key in ("activeCount", "pendingCount", "rejectedCount", "tombstoneCount")
    )


def _task_type(row: dict[str, Any]) -> str:
    return str(row.get("taskType") or row.get("type") or "").strip().lower()


def _task_status(row: dict[str, Any]) -> str:
    return str(row.get("lifecycleStatus") or row.get("status") or "").strip().lower()


def _coverage_scope(row: dict[str, Any]) -> str:
    summary = as_json_object(row.get("summary"))
    runtime = as_json_object(row.get("runtime"))
    return str(
        summary.get("coverageScope")
        or runtime.get("coverageScope")
        or row.get("coverageScope")
        or ""
    ).strip()


def _is_bootstrap_scope(row: dict[str, Any]) -> bool:
    return _coverage_scope(row) == "bootstrap_sheets"


def _successful_task(row: dict[str, Any]) -> bool:
    return _task_status(row) in {"ok", "warning", "succeeded", "completed"}


def _has_successful_full_pipeline(history: list[dict[str, Any]]) -> bool:
    return any(
        _task_type(row) == "pipeline"
        and bool(str(row.get("finishedAt") or "").strip())
        and _successful_task(row)
        for row in history
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
    except (OSError, TypeError, ValueError):
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
    rescued = int(summary.get("failuresResolvedByBrowser") or 0)
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
            # The rescued split travels with the failure count so a run row shows both
            # without reloading the report file.
            "failuresResolvedByBrowser": rescued,
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


def _measured_durations(fetch_rows: list[dict[str, Any]]) -> list[int]:
    """Wall-clock durations of the runs that finished, sorted ascending.

    A run that finished still spent the operator's time when it failed, so failures
    stay in the sample; the 7-day success rate reports them separately. A row with no
    positive duration is not a sample at all — counting it as 0 s would pull every
    percentile toward zero, which is how an unmeasured run reads as a fast one.
    """
    return sorted(
        duration
        for duration in (_safe_int(row.get("durationMs"), 0) for row in fetch_rows)
        if duration > 0
    )


def _duration_percentile(durations: list[int], percentile: float) -> int:
    """A percentile of the measured durations, interpolated between neighbours.

    Interpolation keeps ``0.5`` equal to the median, which a nearest-rank rule over
    an even count does not. A single sample returns itself at any percentile, so a
    young install reads one real duration instead of a synthesized zero.
    """
    if not durations:
        return 0
    rank = (len(durations) - 1) * percentile
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return durations[int(lower)]
    weight = rank - lower
    span = durations[int(upper)] - durations[int(lower)]
    return int(round(durations[int(lower)] + span * weight))


def collect_fetch_history_metrics(
    history: list[dict[str, Any]],
    parse_iso: Callable[[Any], datetime | None],
    now_utc: Callable[[], datetime],
) -> dict[str, Any]:
    now = now_utc()
    seven_days_ago = now - timedelta(days=7)
    fetch_rows = [row for row in history if _task_type(row) == "fetch" and row.get("finishedAt")]
    fetch_7d = [
        row
        for row in fetch_rows
        if (parse_iso(row.get("finishedAt")) or datetime.min.replace(tzinfo=UTC)) >= seven_days_ago
    ]
    success_7d = [row for row in fetch_7d if _successful_task(row)]
    success_rate = (len(success_7d) / len(fetch_7d)) if fetch_7d else 0.0
    avg_duration = (
        int(sum(int(row.get("durationMs") or 0) for row in fetch_7d) / len(fetch_7d))
        if fetch_7d
        else 0
    )
    durations = _measured_durations(fetch_7d)
    latest_fetch = fetch_rows[-1] if fetch_rows else None
    last_success = next((row for row in reversed(fetch_rows) if _successful_task(row)), None)
    return {
        "fetchRows": fetch_rows,
        "successRate7d": success_rate,
        # The denominator the rate was taken over. "Every run failed" and "no run
        # happened" both produce 0.0, and only the count separates the two readings.
        "fetchRunCount7d": len(fetch_7d),
        # The mean stays for payload compatibility, but nothing should decide from it:
        # one slow run moves it, which is how the live container read ~50 min beside
        # 2.5 h runs. The Admin card reports the median and its p90 instead.
        "avgDurationMs7d": avg_duration,
        "medianDurationMs7d": _duration_percentile(durations, 0.5),
        "p90DurationMs7d": _duration_percentile(durations, 0.9),
        "durationSampleCount7d": len(durations),
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
                if _task_type(row) == run_type and row.get("finishedAt")
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


# The Action Center labels storage failures by reason code; the summary alert uses the
# same names so one condition does not read as two different findings.
_STORAGE_REASON_LABELS = {
    "write_lock_timeout": "Storage write lock timed out",
    "busy_exhausted_write": "SQLite stayed busy during a write",
    "busy_exhausted_read": "SQLite stayed busy during a read",
    "busy_exhausted_vacuum": "SQLite stayed busy during a database rebuild",
    "write_error": "SQLite write failed",
    "vacuum_error": "Database rebuild failed",
    "wal_checkpoint_error": "WAL checkpoint failed",
    "wal_checkpoint_busy": "WAL checkpoint could not finish",
    "integrity_check": "SQLite integrity check failed",
    "startup_probe": "SQLite startup probe failed",
    "pragma": "SQLite could not enable a required setting",
    "reopen_failed": "The store could not reopen after a database swap",
}


def _human_bytes(value: Any) -> str:
    size = float(_safe_int(value, 0))
    if size >= 1024**3:
        return f"{size / 1024**3:.1f} GB"
    if size >= 1024**2:
        return f"{size / 1024**2:.1f} MB"
    return f"{int(size)} B"


def _optional_size(value: Any) -> int | None:
    """A size the payload measured, or None when it carries no usable one.

    `coerce_non_negative_int` maps null, `""` and `"n/a"` to 0, and 0 is also the
    size of an empty file. Comparing those two would read "the WAL is empty" from a
    field that never measured anything, which is exactly how the gate goes silent.
    """
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(number) or number < 0:
        return None
    return int(number)


def _live_storage_sizes(storage: dict[str, Any]) -> tuple[int | None, int | None]:
    """The files on disk, or (None, None) for a payload that does not carry them."""
    return _optional_size(storage.get("walBytes")), _optional_size(storage.get("databaseBytes"))


def _storage_sizes_copy(wal_bytes: int | None, database_bytes: int | None) -> str:
    parts: list[str] = []
    if wal_bytes is not None and wal_bytes > 0:
        parts.append(f"{_human_bytes(wal_bytes)} WAL")
    if database_bytes is not None and database_bytes > 0:
        parts.append(f"{_human_bytes(database_bytes)} database")
    return " on ".join(parts)


def _storage_size_copy(storage: dict[str, Any], wal: dict[str, Any]) -> str:
    """Live sizes first, with the attempt's snapshot only when it differs.

    The record stores the sizes observed *before* its attempt, so quoting them as
    current made a resolved checkpoint read as a 1 GB WAL beside a 520 MB database
    long after both files had fallen back. The live files are the actionable
    numbers; the attempt's are history, and only worth naming when they disagree.
    """
    live_wal, live_database = _live_storage_sizes(storage)
    attempt_wal = _optional_size(wal.get("walBytes"))
    attempt_database = _optional_size(wal.get("databaseBytes"))
    if live_wal is None and live_database is None:
        # Older than 0.3.064: the record is the only size the payload carries.
        return _storage_sizes_copy(attempt_wal, attempt_database)
    current = _storage_sizes_copy(live_wal, live_database)
    if (attempt_wal, attempt_database) == (live_wal, live_database):
        return current
    attempt = _storage_sizes_copy(attempt_wal, attempt_database)
    if not attempt:
        return current
    return f"{current}; the attempt saw {attempt}" if current else f"the attempt saw {attempt}"


def _wal_still_outstanding(storage: dict[str, Any], wal: dict[str, Any]) -> bool:
    """Whether a busy/failed record still describes the live files.

    W1: the record is a condition and only a live measurement can end it. An
    unknown size or threshold is not evidence the WAL shrank, so it keeps the
    warning; payloads older than 0.3.064 have neither, and the record still
    decides. A stalled health read carries fresh sizes but no `walMaintenance`
    (`storage_health._stalled_storage_payload`), so it never reaches this test and
    cannot silence a live condition.
    """
    live_wal, _ = _live_storage_sizes(storage)
    threshold = _optional_size(wal.get("checkpointThresholdBytes"))
    if live_wal is None or threshold is None or threshold <= 0:
        return True
    return live_wal >= threshold


def _human_age(seconds: Any) -> str:
    total = max(0, _safe_int(seconds, 0))
    if total < 60:
        return f"{total} s"
    if total < 3600:
        return f"{total // 60} min"
    if total < 86400:
        return f"{total // 3600} h"
    return f"{total // 86400} d"


def storage_health_alert(payload: dict[str, Any]) -> dict[str, Any] | None:
    """The storage verdict both the summary and the Action Center report.

    S1.4: `/ops/fetch-kpis?view=summary` used to answer `status: "healthy"`,
    `alerts: []` while the Action Center showed a critical storage signal. This
    derives the same severity from the storage-health payload: a real integrity
    failure is critical; a busy or errored checkpoint is a warning; and a store
    that S1.2 left healthy while its WAL could not be checkpointed is still a
    warning, because the disk risk is the point.

    W1: that warning follows the live WAL, not the record.     `walMaintenance` is
    rewritten only by a later attempt, so a WAL emptied by a VACUUM, a rebuild or a
    bulk-delete checkpoint left the record reading `busy` forever and the panel
    reported a 1 GB WAL that no longer existed. A busy record is a condition, and
    the live file is what decides whether it is still true. An unmeasured size is
    not evidence the condition ended: a null, blank, unparsable or negative size
    keeps the warning, so the gate can only ever fail toward speaking.
    """
    storage = as_json_object(payload.get("storage"))
    if not storage:
        return None
    unhealthy = storage.get("healthy") is False
    wal = as_json_object(storage.get("walMaintenance"))
    wal_status = str(wal.get("status") or "").strip()
    wal_outstanding = wal_status in {"busy", "failed"} and _wal_still_outstanding(storage, wal)
    if not unhealthy and not wal_outstanding:
        return None

    reason = as_json_object(storage.get("unhealthyReason"))
    code = str(reason.get("code") or "").strip()
    reason_message = str(reason.get("message") or "").strip().replace("\n", " ")[:240]
    at = str(reason.get("at") or wal.get("attemptedAt") or "").strip()
    deferred = storage.get("quickCheckDeferred") in (True, 1)
    quick_check = str(storage.get("quickCheck") or "").strip().lower()
    quick_check_failed = not deferred and quick_check not in {
        "",
        "ok",
        "deferred-large-database",
    }
    wal_reason = code in {"wal_checkpoint_busy", "wal_checkpoint_error"} or (
        not code and wal_status == "failed"
    )

    if quick_check_failed:
        severity = "critical"
        detail = str(storage.get("quickCheck") or "").strip().replace("\n", " ")[:160]
        message = (
            f"SQLite integrity check failed: {detail}"
            if detail
            else "SQLite integrity check failed"
        )
    elif wal_reason or wal_outstanding:
        severity = "warning"
        headline = (
            "WAL checkpoint failed" if wal_status == "failed" else "WAL checkpoint could not finish"
        )
        sizes = _storage_size_copy(storage, wal)
        message = f"{headline} — {sizes}" if sizes else headline
        if reason_message:
            message += f": {reason_message}"
    else:
        severity = "critical"
        label = _STORAGE_REASON_LABELS.get(code, "SQLite storage reported a problem")
        message = f"{label}: {reason_message}" if reason_message else label
    attempt_age = _safe_int(wal.get("attemptAgeS"), 0)
    if at:
        when = f"{_human_age(attempt_age)} ago at {at}" if attempt_age > 0 else at
        message += f"; attempt {when}"
    return {
        "id": "storage_unhealthy",
        "severity": severity,
        "message": message,
        "value": code or wal_status or "unhealthy",
        "triggeredAt": at,
        # Appended after alert acknowledgement is resolved, so there is no ack path for it.
        "dismissible": False,
    }


# A `new_context`/`new_page` failure is reported as `context`; the operator-facing
# sentence spells it out rather than saying "blocked at context". Every other stage
# reads as itself.
_BROWSER_STAGE_LABELS = {"context": "context creation"}


def browser_fallback_health_alert(
    value: Any,
    *,
    environment_blocked: bool = False,
    environment_blocked_sources: int = 0,
    now_iso: str = "",
) -> dict[str, Any] | None:
    """Turn a failed browser probe into the warning both summary surfaces show.

    A failed probe is not a healthy fetch: the browser lane may have produced no
    rendered HTML, and a preflight-blocked run skipped it for every source. The
    fetch KPI used to answer `status: "healthy"` because only history and storage
    alerts contributed severity. This alert makes the failure visible without
    pretending the board data itself failed.
    """
    health = as_json_object(value)
    probe_failed = value is False or health.get("ok") is False
    blocked_sources = max(0, _safe_int(environment_blocked_sources, 0))
    # A positive count is itself proof of an environment block. Callers project
    # the flag and the count independently, so neither alone may hide the warning.
    environment_blocked = environment_blocked or blocked_sources > 0
    if not probe_failed and not environment_blocked:
        return None
    stage = str(health.get("stage") or "").strip()
    if not stage:
        stage = "preflight" if environment_blocked else "unknown"
    stage_label = _BROWSER_STAGE_LABELS.get(stage, stage)
    error_type = str(health.get("errorType") or "").strip()
    error = " ".join(
        str(health.get(key) or "") for key in ("error", "lastLaunchError", "lastNavigationError")
    ).strip()
    error = " ".join(error.split())[:240]

    if environment_blocked:
        message = f"Browser fallback environment blocked at {stage_label}"
        if blocked_sources:
            message += f" for {blocked_sources} source{'s' if blocked_sources != 1 else ''}"
    else:
        message = f"Browser fallback probe failed at {stage_label}"
    if error_type:
        message += f" ({error_type})"
    if error:
        message += f": {error}"

    return {
        "id": "browser_fallback_health",
        "severity": "warning",
        "message": message,
        "value": blocked_sources if environment_blocked else stage,
        "triggeredAt": str(health.get("lastFailureAt") or now_iso),
        # Appended after acknowledgement resolution, like the storage alert: a probe
        # failure is current evidence, not a dismissible historical warning.
        "dismissible": False,
    }


def _finalize_alerts(
    active_conditions: list[dict[str, Any]],
    *,
    load_alert_state_fn: Callable[[], dict[str, Any]],
    save_alert_state_fn: Callable[[dict[str, Any]], None],
    state_lock: Any = None,
) -> dict[str, Any]:
    for row in active_conditions:
        row["dismissible"] = str(row.get("id") or "") not in NON_DISMISSIBLE_ALERT_IDS

    # ponytail: the read-modify-write below races with /ops/alerts/ack under
    # ThreadingHTTPServer; callers pass the shared ops lock to serialize it.
    with state_lock if state_lock is not None else nullcontext():
        alert_state = load_alert_state_fn()
        acked = dict(alert_state.get("acked") or {})
        active_ids = {str(row.get("id") or "") for row in active_conditions}
        non_dismissible_ids = {
            str(row.get("id") or "")
            for row in active_conditions
            if not bool(row.get("dismissible", True))
        }
        for key in list(acked.keys()):
            if key not in active_ids or key in non_dismissible_ids:
                acked.pop(key, None)

        visible_alerts = [row for row in active_conditions if str(row.get("id") or "") not in acked]
        # ponytail: this runs on every summary poll (dashboard-health, fetch-kpis);
        # rewriting admin-alert-state.json when nothing changed put a constant disk
        # write on the poll path. Only save when the state actually moved.
        new_alert_state = {"acked": acked}
        if dict(alert_state.get("acked") or {}) != acked:
            save_alert_state_fn(new_alert_state)
    return {
        "alerts": visible_alerts,
        "suppressedCount": max(0, len(active_conditions) - len(visible_alerts)),
    }


def evaluate_alerts_summary(
    *,
    history: list[dict[str, Any]],
    pending_count: int,
    load_alert_state_fn: Callable[[], dict[str, Any]],
    save_alert_state_fn: Callable[[dict[str, Any]], None],
    parse_iso: Callable[[Any], datetime | None],
    now_iso: Callable[[], str],
    now_utc: Callable[[], datetime],
    state_lock: Any = None,
) -> dict[str, Any]:
    """Evaluate cheap, history-backed alerts for summary health routes.

    This intentionally skips diagnostics that need full fetch reports, source rows,
    social evidence, or deep dedup artifacts. It is authoritative for normal
    user-facing stale/never fetch and pipeline-never-run warnings.
    """

    active_conditions: list[dict[str, Any]] = []
    now = now_utc()
    fetch_rows = [row for row in history if _task_type(row) == "fetch" and row.get("finishedAt")]
    last_success_fetch = next(
        (row for row in reversed(fetch_rows) if _successful_task(row)),
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

    if not _has_successful_full_pipeline(history):
        active_conditions.append(
            {
                "id": "pipeline_never_run",
                "severity": "warning",
                "message": "No successful full Jobs pipeline has run yet. Coverage is limited until the full pipeline completes.",
                "value": None,
                "triggeredAt": now_iso(),
            }
        )

    finalized = _finalize_alerts(
        active_conditions,
        load_alert_state_fn=load_alert_state_fn,
        save_alert_state_fn=save_alert_state_fn,
        state_lock=state_lock,
    )
    return {
        **finalized,
        "pendingApprovals": int(pending_count),
    }


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
    state_lock: Any = None,
) -> dict[str, Any]:
    active_conditions: list[dict[str, Any]] = []
    now = now_utc()
    fetch_rows = [row for row in history if _task_type(row) == "fetch" and row.get("finishedAt")]
    latest_fetch = fetch_rows[-1] if fetch_rows else None
    last_success_fetch = next(
        (row for row in reversed(fetch_rows) if _successful_task(row)),
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

    if not _has_successful_full_pipeline(history):
        active_conditions.append(
            {
                "id": "pipeline_never_run",
                "severity": "warning",
                "message": "No successful full Jobs pipeline has run yet. Coverage is limited until the full pipeline completes.",
                "value": None,
                "triggeredAt": now_iso(),
            }
        )

    fetch_summary = summarize_fetch_report(latest_fetch_report)
    failed_ratio = float(fetch_summary["failedRatio"])
    latest_report_is_bootstrap = _is_bootstrap_scope(latest_fetch_report)
    if not latest_report_is_bootstrap and failed_ratio > DEGRADED_FAILURE_RATIO:
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
        if _is_bootstrap_scope(row):
            continue
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

    finalized = _finalize_alerts(
        active_conditions,
        load_alert_state_fn=load_alert_state_fn,
        save_alert_state_fn=save_alert_state_fn,
        state_lock=state_lock,
    )
    return {
        **finalized,
        "pendingApprovals": int(pending_count),
    }


def compute_ops_health(deps: Any) -> dict[str, Any]:
    """Build ops health payload. deps must have: get_history, get_fetch_report, get_state,
    now_iso, desktop_mode, desktop_last_activity_at, load_alert_state_fn, save_alert_state_fn,
    parse_schedule_metadata_fn, normalize_fetch_report_contract, parse_iso, now_utc.
    """
    with time_operation("ops.dashboard.history"):
        history: list[dict[str, Any]] = deps.get_history()
    with time_operation("ops.dashboard.fetch_report"):
        latest_fetch_report: dict[str, Any] = deps.get_fetch_report()
    registry_summary: dict[str, Any] = {}
    get_registry_summary_payload = getattr(deps, "get_registry_summary_payload", None)
    if callable(get_registry_summary_payload):
        with time_operation("ops.dashboard.registry_summary"):
            try:
                registry_summary = as_json_object(get_registry_summary_payload())
            except (OSError, TypeError, ValueError):
                registry_summary = {}
    if not _has_registry_summary_counts(registry_summary):
        registry_summary = {}
    state: dict[str, Any] = {}
    if not registry_summary:
        with time_operation("ops.dashboard.registry_state_fallback"):
            state = deps.get_state()
    with time_operation("ops.dashboard.schedule"):
        schedule = populate_schedule_next_run(
            deps.parse_schedule_metadata_fn(), history, deps.parse_iso
        )
    get_pipeline_schedule = getattr(deps, "get_jobs_pipeline_schedule_ops_entry", None)
    if callable(get_pipeline_schedule):
        with time_operation("ops.dashboard.pipeline_schedule"):
            try:
                pipeline_schedule = get_pipeline_schedule()
            except (RuntimeError, OSError, TypeError, ValueError):
                pipeline_schedule = {}
        if isinstance(pipeline_schedule, dict):
            schedule["pipeline"] = dict(pipeline_schedule)
    with time_operation("ops.dashboard.alerts"):
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
            state_lock=getattr(deps, "alert_state_lock", None),
        )

    with time_operation("ops.dashboard.fetch_history_metrics"):
        metrics = collect_fetch_history_metrics(history, deps.parse_iso, deps.now_utc)
    last_success = metrics["lastSuccessFetch"]
    with time_operation("ops.dashboard.fetch_report_summary"):
        latest_fetch_summary = summarize_fetch_report(latest_fetch_report)
    failed_ratio_latest = latest_fetch_summary["failedRatio"]
    source_health = as_json_object(latest_fetch_report.get("sourceHealth"))
    provider_coverage = as_json_object(latest_fetch_report.get("providerCoverage"))
    provider_static_overlap = as_json_object(latest_fetch_report.get("providerStaticOverlap"))
    static_suppression_policy = as_json_object(latest_fetch_report.get("staticSuppressionPolicy"))
    redundant_static_proposals = as_json_object(latest_fetch_report.get("redundantStaticProposals"))
    with time_operation("ops.dashboard.source_policy_soak"):
        source_policy_soak_report = as_json_object(deps.get_source_policy_soak_report())
    conservative_static_cleanup_proposals = as_json_object(
        as_json_object(source_policy_soak_report.get("sections")).get(
            "conservativeStaticCleanupProposals"
        )
    )
    source_policy_recommendation_export = as_json_object(
        latest_fetch_report.get("sourcePolicyRecommendationExport")
    )
    with time_operation("ops.dashboard.dedup_review_state"):
        dedup_review_state = summarize_dedup_review_state(latest_fetch_report)
    with time_operation("ops.dashboard.tombstones"):
        try:
            tombstones = deps.get_tombstones()
        except (OSError, TypeError, ValueError):
            tombstones = {}
    with time_operation("ops.dashboard.sync_status"):
        try:
            sync_status = deps.get_sync_status_payload()
        except (OSError, TypeError, ValueError):
            sync_status = {}
    with time_operation("ops.dashboard.registry_sync_summary"):
        registry_sync = derive_registry_sync_summary(
            state=state,
            summary=registry_summary,
            tombstones=tombstones,
            sync_status=sync_status,
            history=history,
        )
    get_probe_lifecycle = getattr(deps, "get_probe_lifecycle_payload", None)
    probe_lifecycle: dict[str, Any] = {}
    if callable(get_probe_lifecycle):
        with time_operation("ops.dashboard.probe_lifecycle"):
            try:
                probe_lifecycle = as_json_object(get_probe_lifecycle())
            except (OSError, TypeError, ValueError):
                probe_lifecycle = {}

    # The page-fetch receipt rides alongside the probe receipt, never under it: they answer
    # different questions (probe-wave abandonment vs directory/web-search page fetches) and a
    # reader must be able to see one without the other.
    get_page_fetch_lifecycle = getattr(deps, "get_page_fetch_lifecycle_payload", None)
    page_fetch_lifecycle: dict[str, Any] = {}
    if callable(get_page_fetch_lifecycle):
        with time_operation("ops.dashboard.page_fetch_lifecycle"):
            try:
                page_fetch_lifecycle = as_json_object(get_page_fetch_lifecycle())
            except (OSError, TypeError, ValueError):
                page_fetch_lifecycle = {}

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
    from src.build_info import read_build_info

    return {
        "service": "baluffo-bridge",
        "desktopMode": bool(deps.desktop_mode),
        "appVersion": app_version,
        "build": read_build_info(),
        "startupReady": bool(startup_ready),
        "generatedAt": deps.now_iso(),
        "desktopLastActivityAt": str(deps.desktop_last_activity_at or ""),
        "owner": {
            "mode": str(owner_state.get("ownerMode") or ""),
            "token": str(owner_state.get("ownerToken") or ""),
            "sessionId": str(owner_state.get("sessionId") or ""),
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
            # The rate stays numeric when the window is empty for payload
            # compatibility; the run count says whether 0.0 was measured or unobserved.
            "sevenDayFetchSuccessRate": round(float(metrics["successRate7d"]), 4),
            "fetchRunCount7d": int(metrics["fetchRunCount7d"]),
            "avgFetchDurationMs7d": int(metrics["avgDurationMs7d"]),
            "medianFetchDurationMs7d": int(metrics["medianDurationMs7d"]),
            "p90FetchDurationMs7d": int(metrics["p90DurationMs7d"]),
            "fetchDurationSampleCount7d": int(metrics["durationSampleCount7d"]),
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
            "probeLifecycle": probe_lifecycle,
            "pageFetchLifecycle": page_fetch_lifecycle,
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
