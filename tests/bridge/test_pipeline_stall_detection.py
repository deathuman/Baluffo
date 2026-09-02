"""Pytest tests for pipeline stall detection (R1).

These tests target the pure helper `compute_pipeline_stall_info` in
`src/bridge/pipeline_stall.py`, avoiding the circular-import chain that
plagues tests importing `src.bridge.pipeline_service` directly.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from src.bridge.pipeline_stall import (
    compute_pipeline_child_quiet_info,
    compute_pipeline_stall_info,
)


def _parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def _payload(
    *,
    now: datetime,
    active: bool = True,
    active_child_task_type: str = "fetch",
    heartbeat_at_seconds_ago: float,
) -> dict[str, Any]:
    heartbeat_at = (now - timedelta(seconds=heartbeat_at_seconds_ago)).isoformat()
    return {
        "active": active,
        "activeChildTaskType": active_child_task_type,
        "heartbeatAt": heartbeat_at,
    }


def test_stall_returns_none_when_inactive() -> None:
    now = datetime.now(UTC)
    assert (
        compute_pipeline_stall_info(
            _payload(now=now, active=False, heartbeat_at_seconds_ago=400),
            parse_iso=_parse_iso,
            now_utc=lambda: now,
        )
        is None
    )


def test_stall_returns_none_for_unknown_child() -> None:
    now = datetime.now(UTC)
    assert (
        compute_pipeline_stall_info(
            _payload(now=now, active_child_task_type="adjudication", heartbeat_at_seconds_ago=400),
            parse_iso=_parse_iso,
            now_utc=lambda: now,
        )
        is None
    )


def test_stall_returns_none_when_fresh() -> None:
    now = datetime.now(UTC)
    assert (
        compute_pipeline_stall_info(
            _payload(now=now, active_child_task_type="fetch", heartbeat_at_seconds_ago=10),
            parse_iso=_parse_iso,
            now_utc=lambda: now,
        )
        is None
    )


def test_stall_emits_for_fetch_over_threshold() -> None:
    now = datetime.now(UTC)
    result = compute_pipeline_stall_info(
        _payload(now=now, active_child_task_type="fetch", heartbeat_at_seconds_ago=200),
        parse_iso=_parse_iso,
        now_utc=lambda: now,
    )
    assert result is not None, "expected stallInfo for stale fetch heartbeat"
    assert result["stalled"] is True
    assert result["inChild"] == "fetch"
    assert result["thresholdSeconds"] == 180.0
    assert result["silentSeconds"] >= 200.0


def test_stall_emits_for_sync_over_threshold() -> None:
    now = datetime.now(UTC)
    result = compute_pipeline_stall_info(
        _payload(now=now, active_child_task_type="sync", heartbeat_at_seconds_ago=70),
        parse_iso=_parse_iso,
        now_utc=lambda: now,
    )
    assert result is not None, "expected stallInfo for stale sync heartbeat"
    assert result["stalled"] is True
    assert result["inChild"] == "sync"
    assert result["thresholdSeconds"] == 60.0


def test_stall_uses_default_now_utc_when_omitted() -> None:
    now = datetime.now(UTC)
    result = compute_pipeline_stall_info(
        _payload(now=now, active_child_task_type="fetch", heartbeat_at_seconds_ago=200),
        parse_iso=_parse_iso,
    )
    assert result is not None
    assert result["stalled"] is True


def test_stall_emits_for_discovery_over_threshold() -> None:
    now = datetime.now(UTC)
    result = compute_pipeline_stall_info(
        _payload(now=now, active_child_task_type="discovery", heartbeat_at_seconds_ago=400),
        parse_iso=_parse_iso,
        now_utc=lambda: now,
    )
    assert result is not None, "expected stallInfo for stale discovery heartbeat"
    assert result["stalled"] is True
    assert result["inChild"] == "discovery"
    assert result["thresholdSeconds"] == 300.0


def test_stall_returns_none_for_fresh_discovery_heartbeat() -> None:
    now = datetime.now(UTC)
    assert (
        compute_pipeline_stall_info(
            _payload(now=now, active_child_task_type="discovery", heartbeat_at_seconds_ago=10),
            parse_iso=_parse_iso,
            now_utc=lambda: now,
        )
        is None
    )


def _quiet_payload(
    *,
    now: datetime,
    counts_updated_at_seconds_ago: float,
    active: bool = True,
    task_type: str = "discovery",
    phase_key: str = "probing_candidates",
) -> dict[str, Any]:
    counts_updated_at = (now - timedelta(seconds=counts_updated_at_seconds_ago)).isoformat()
    return {
        "active": active,
        "activeChildren": [
            {
                "taskType": task_type,
                "taskProgress": {
                    "active": True,
                    "phaseKey": phase_key,
                    "countsUpdatedAt": counts_updated_at,
                },
            }
        ],
    }


def test_quiet_returns_none_when_inactive() -> None:
    now = datetime.now(UTC)
    assert (
        compute_pipeline_child_quiet_info(
            _quiet_payload(now=now, active=False, counts_updated_at_seconds_ago=300),
            parse_iso=_parse_iso,
            now_utc=lambda: now,
        )
        is None
    )


def test_quiet_returns_none_when_counts_fresh() -> None:
    now = datetime.now(UTC)
    assert (
        compute_pipeline_child_quiet_info(
            _quiet_payload(now=now, counts_updated_at_seconds_ago=5),
            parse_iso=_parse_iso,
            now_utc=lambda: now,
        )
        is None
    )


def test_quiet_emits_when_counts_stale_but_heartbeat_alive() -> None:
    now = datetime.now(UTC)
    result = compute_pipeline_child_quiet_info(
        _quiet_payload(
            now=now,
            counts_updated_at_seconds_ago=180,
            task_type="discovery",
            phase_key="probing_candidates",
        ),
        parse_iso=_parse_iso,
        now_utc=lambda: now,
    )
    assert result is not None, "expected quiet info for stale counts"
    assert result["quiet"] is True
    assert result["inChild"] == "discovery"
    assert result["phaseKey"] == "probing_candidates"
    assert result["quietSeconds"] >= 180.0
