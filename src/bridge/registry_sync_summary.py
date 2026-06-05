"""Derived registry/sync confidence summary for Admin/Ops."""

from __future__ import annotations

from typing import Any

from src.shared.json_shapes import as_json_object, json_object_rows


def _safe_int(value: Any) -> int:
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return 0


def _text(value: Any) -> str:
    return str(value or "").strip()


def _lower(value: Any) -> str:
    return _text(value).lower()


def _bucket_rows(payload: Any) -> tuple[list[dict[str, Any]], int]:
    if not isinstance(payload, list):
        return [], 0
    rows = [row for row in payload if isinstance(row, dict)]
    return rows, max(0, len(payload) - len(rows))


def _pending_is_hidden(row: dict[str, Any]) -> bool:
    return bool(row.get("hiddenFromDefault")) or _lower(row.get("candidateState")) == "hidden"


def _pending_is_deferred(row: dict[str, Any]) -> bool:
    return (
        bool(row.get("deferred"))
        or bool(_text(row.get("deferReason")))
        or bool(_text(row.get("firstDeferredAt")))
        or bool(_text(row.get("lastDeferredAt")))
        or _safe_int(row.get("deferCount")) > 0
    )


def _pending_is_duplicate(row: dict[str, Any]) -> bool:
    reason = _lower(row.get("pendingReason"))
    return bool(_text(row.get("duplicateOfSourceId"))) or "duplicate" in reason


def _latest_sync_history(history: Any) -> dict[str, Any]:
    rows = [
        row
        for row in json_object_rows(history)
        if _lower(row.get("type")) == "sync" and _text(row.get("finishedAt"))
    ]
    if not rows:
        return {}
    return sorted(rows, key=lambda row: _text(row.get("finishedAt")))[-1]


def _latest_sync_timestamp(runtime: dict[str, Any], latest_sync: dict[str, Any]) -> str:
    candidates = [
        _text(runtime.get("lastPullAt")),
        _text(runtime.get("lastPushAt")),
        _text(latest_sync.get("finishedAt")),
    ]
    return sorted([value for value in candidates if value])[-1] if any(candidates) else ""


def _last_sync_status(
    *,
    sync_payload: dict[str, Any],
    runtime: dict[str, Any],
    latest_sync: dict[str, Any],
) -> str:
    config_state = _lower(as_json_object(sync_payload.get("config")).get("state"))
    if config_state in {"remote_conflict", "rate_limited"}:
        return config_state
    if _text(runtime.get("lastError")):
        return "error"
    if _text(latest_sync.get("status")):
        return _lower(latest_sync.get("status"))
    if _text(runtime.get("lastResult")):
        return _lower(runtime.get("lastResult"))
    if _text(runtime.get("lastAction")):
        return "unknown"
    return "never"


def _count_sync_action(latest_sync: dict[str, Any], action: str) -> int:
    summary = as_json_object(latest_sync.get("summary"))
    for key in (f"{action}Count", action):
        if key in summary:
            return _safe_int(summary.get(key))
    if _lower(summary.get("action")) == action and _lower(latest_sync.get("status")) != "error":
        return 1
    return 0


def _conflict_count(
    *,
    status: str,
    runtime: dict[str, Any],
    latest_sync: dict[str, Any],
) -> int:
    haystack = " ".join(
        [
            status,
            _text(runtime.get("lastError")),
            _text(as_json_object(latest_sync.get("summary")).get("error")),
        ]
    ).lower()
    return 1 if "conflict" in haystack else 0


def derive_registry_sync_summary(
    *,
    state: Any,
    summary: Any = None,
    tombstones: Any = None,
    sync_status: Any = None,
    history: Any = None,
) -> dict[str, Any]:
    registry = as_json_object(state)
    summary_payload = as_json_object(summary)
    use_summary = bool(summary_payload)
    if use_summary:
        active_rows, active_invalid = [], 0
        pending_rows, pending_invalid = [], 0
        rejected_rows, rejected_invalid = [], 0
    else:
        active_rows, active_invalid = _bucket_rows(registry.get("active"))
        pending_rows, pending_invalid = _bucket_rows(registry.get("pending"))
        rejected_rows, rejected_invalid = _bucket_rows(registry.get("rejected"))
    tombstone_rows = as_json_object(tombstones)
    sync_payload = as_json_object(sync_status)
    runtime = as_json_object(sync_payload.get("runtime"))
    latest_sync = _latest_sync_history(history)
    latest_summary = as_json_object(latest_sync.get("summary"))
    status = _last_sync_status(
        sync_payload=sync_payload,
        runtime=runtime,
        latest_sync=latest_sync,
    )
    tombstone_count = (
        _safe_int(summary_payload.get("tombstoneCount")) if use_summary else len(tombstone_rows)
    )
    rejected_count = (
        _safe_int(summary_payload.get("rejectedCount")) if use_summary else len(rejected_rows)
    )

    return {
        "summaryExact": (bool(summary_payload.get("summaryExact", True)) if use_summary else True),
        "countBasis": (
            _text(summary_payload.get("countBasis") or "") if use_summary else "normalized"
        ),
        "summaryStatus": (
            _text(summary_payload.get("summaryStatus") or "ready") if use_summary else "ready"
        ),
        "activeCount": (
            _safe_int(summary_payload.get("activeCount")) if use_summary else len(active_rows)
        ),
        "pendingCount": (
            _safe_int(summary_payload.get("pendingCount")) if use_summary else len(pending_rows)
        ),
        "rejectedCount": rejected_count,
        "tombstoneCount": tombstone_count,
        "hiddenPendingCount": (
            _safe_int(summary_payload.get("hiddenPendingCount"))
            if use_summary
            else sum(1 for row in pending_rows if _pending_is_hidden(row))
        ),
        "deferredPendingCount": (
            _safe_int(summary_payload.get("deferredPendingCount"))
            if use_summary
            else sum(1 for row in pending_rows if _pending_is_deferred(row))
        ),
        "duplicatePendingCount": (
            _safe_int(summary_payload.get("duplicatePendingCount"))
            if use_summary
            else sum(1 for row in pending_rows if _pending_is_duplicate(row))
        ),
        "lastSyncAt": _latest_sync_timestamp(runtime, latest_sync),
        "lastSyncStatus": status,
        "remoteActiveCount": _safe_int(latest_summary.get("activeCount")),
        "remotePendingCount": _safe_int(latest_summary.get("pendingCount")),
        "pulledCount": _count_sync_action(latest_sync, "pull"),
        "pushedCount": _count_sync_action(latest_sync, "push"),
        "ignoredRejectedCount": rejected_count,
        "ignoredTombstonedCount": tombstone_count,
        "conflictCount": _conflict_count(status=status, runtime=runtime, latest_sync=latest_sync),
        "localOnlyCount": rejected_count + tombstone_count,
        "remoteOnlyCount": _safe_int(latest_summary.get("remoteOnlyCount")),
        "invalidRowsCount": (
            _safe_int(summary_payload.get("invalidRowsCount"))
            if use_summary
            else active_invalid + pending_invalid + rejected_invalid
        ),
    }


__all__ = ["derive_registry_sync_summary"]
