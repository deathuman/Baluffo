"""Stall detection for pipeline active-child heartbeat.

AI boundary owns: pipeline stall threshold definitions and the pure payload
  computation. The helper is stateless and operates on the status payload.
AI boundary implement in: this file only; pipeline_service calls the helper.
AI boundary search before contracts: get_status_payload() and consumers in
  frontend/jobs/app/runtime/pipeline-controller.js.
AI boundary verify: npm run lint:repo-guardrails plus unit tests in
  tests/bridge/test_pipeline_stall_detection.py and this module's self-check.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

# ponytail: hardcoded thresholds (seconds of silence before we report "stalled").
# Allow fetch the longest window: fetch crawls 32 concurrent homepage requests
# and slow sources can sit on a single page fetch for minutes. Discovery probing
# can also sit inside a single phase (scanning / generating) for minutes without
# the counters advancing, so give it a wide window too. Sync push is a single
# HTTP transaction; allow a much smaller window.
_STAGE_STALL_THRESHOLD_SEC: dict[str, float] = {
    "fetch": 180.0,
    "discovery": 300.0,
    "sync": 60.0,
}

# ponytail: after this much time with no *counter* movement the child is probably
# silently grinding on a long step (e.g. one slow probe/page). Advisory only; the
# UI shows a reassuring "still working" cue, never an alarm.
_CHILD_QUIET_CUE_THRESHOLD_SEC = 60.0


def compute_pipeline_stall_info(
    payload: dict[str, Any],
    *,
    parse_iso: Callable[[Any], Any],
    now_utc: Callable[[], datetime] | None = None,
) -> dict[str, Any] | None:
    """Return stall metadata when the active child appears silent.

    Returns None unless (a) the pipeline is active, (b) the active child is
    fetch or sync, (c) the latest heartbeat is string-parseable, and
    (d) the silence exceeds the stage threshold.

    The result is advisory only -- callers surface it in the UI; they must
    never act on it to kill or restart a task.
    """
    if not bool(payload.get("active")):
        return None
    child_type = str(payload.get("activeChildTaskType") or "").strip().lower()
    threshold = _STAGE_STALL_THRESHOLD_SEC.get(child_type)
    if threshold is None:
        return None
    heartbeat_at = str(payload.get("heartbeatAt") or "").strip()
    if not heartbeat_at:
        return None
    parsed = parse_iso(heartbeat_at)
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    if now_utc is None:

        def _default_now() -> datetime:
            return datetime.now(UTC)

        now_utc = _default_now
    try:
        now = now_utc()
    except Exception:
        return None
    silent = max(0.0, (now - parsed.astimezone(UTC)).total_seconds())
    if silent < threshold:
        return None
    return {
        "stalled": True,
        "silentSeconds": round(silent, 1),
        "thresholdSeconds": threshold,
        "inChild": child_type,
    }


def _extract_quiet_child(payload: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]] | None:
    """Return the active child and its taskProgress, or None when not present."""
    if not bool(payload.get("active")):
        return None
    children = payload.get("activeChildren")
    if not isinstance(children, list) or not children:
        return None
    child = children[0] if isinstance(children[0], dict) else None
    if child is None:
        return None
    progress = child.get("taskProgress")
    if not isinstance(progress, dict):
        return None
    return child, progress


def _monotonic_now_factory() -> Callable[[], datetime]:
    def _default_now() -> datetime:
        return datetime.now(UTC)

    return _default_now


def compute_pipeline_child_quiet_info(
    payload: dict[str, Any],
    *,
    parse_iso: Callable[[Any], Any],
    now_utc: Callable[[], datetime] | None = None,
) -> dict[str, Any] | None:
    """Return "counts unchanged" metadata for a quiet-but-alive active child.

    Complements ``compute_pipeline_stall_info``: a stage that is still heartbeating
    (so not reported as stalled) but whose shown counters have not moved for a
    while is likely grinding on a single long step. Returns the seconds since the
    last counter change so the UI can reassure the user it is still working.

    Advisory only -- never acted on by the pipeline to kill or restart a task.
    """
    extracted = _extract_quiet_child(payload)
    if extracted is None:
        return None
    child, progress = extracted
    counts_updated_at = str(progress.get("countsUpdatedAt") or "").strip()
    if not counts_updated_at:
        return None
    parsed = parse_iso(counts_updated_at)
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    if now_utc is None:
        now_utc = _monotonic_now_factory()
    try:
        now = now_utc()
    except Exception:
        return None
    quiet = max(0.0, (now - parsed.astimezone(UTC)).total_seconds())
    if quiet < _CHILD_QUIET_CUE_THRESHOLD_SEC:
        return None
    return {
        "quiet": True,
        "quietSeconds": round(quiet, 1),
        "thresholdSeconds": _CHILD_QUIET_CUE_THRESHOLD_SEC,
        "inChild": str(child.get("taskType") or child.get("type") or "").strip().lower(),
        "phaseKey": str(progress.get("phaseKey") or "").strip(),
    }
