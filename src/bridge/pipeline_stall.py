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
# and slow sources can sit on a single page fetch for minutes. Sync push is a
# single HTTP transaction; allow a much smaller window.
_STAGE_STALL_THRESHOLD_SEC: dict[str, float] = {"fetch": 180.0, "sync": 60.0}


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
