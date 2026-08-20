"""Registry conflict adjudication — progress plumbing and payload builders.

AI boundary owns: running/failed/task progress payloads, progress events, and the throttled progress writer used by adjudication runs.
AI boundary implement in: this registry_conflict_adjudication_progress.py leaf.
AI boundary search before contracts: conflict adjudication routes, progress payloads, and adjudication tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused registry adjudication tests."""

from __future__ import annotations

import time
from typing import Any

from src.bridge.registry_conflict_adjudication_core import (
    _artifact_path,
    _as_dict,
    _clean,
    _endpoint_url,
    _now_iso,
    _row_adapter,
    _row_id,
)

_RECENT_PROGRESS_EVENT_LIMIT = 20

_DEFAULT_PROGRESS_THROTTLE_SECONDS = 1.0


def _summary_payload() -> dict[str, int]:
    return {
        "autoDemoteApplied": 0,
        "recommendedDemotion": 0,
        "keepBoth": 0,
        "needsReview": 0,
        "probeFailed": 0,
    }


def _task_progress_payload(
    *,
    active: bool,
    phase_key: str,
    phase_label: str,
    ratio: float,
    counts: dict[str, int],
    updated_at: str,
    target_label: str = "",
    target_url: str = "",
) -> dict[str, Any]:
    mode = "determinate" if counts.get("totalSources", 0) > 0 else "indeterminate"
    return {
        "active": active,
        "phaseKey": phase_key,
        "phaseLabel": phase_label,
        "mode": mode,
        "ratio": max(0.0, min(1.0, ratio)),
        "counts": counts,
        "targetLabel": target_label,
        "targetUrl": target_url,
        "updatedAt": updated_at,
    }


def _base_progress_payload(now: str) -> dict[str, Any]:
    return {
        "totalFamilyCount": 0,
        "checkedFamilyCount": 0,
        "totalSourceCount": 0,
        "checkedSourceCount": 0,
        "currentFamilyKey": "",
        "currentFamilyIndex": 0,
        "currentSourceId": "",
        "currentSourceName": "",
        "currentAdapter": "",
        "currentEndpointUrl": "",
        "lastProgressAt": now,
        "recentEvents": [],
    }


def _progress_counts(progress: dict[str, Any]) -> dict[str, int]:
    return {
        "checkedFamilies": int(progress.get("checkedFamilyCount") or 0),
        "totalFamilies": int(progress.get("totalFamilyCount") or 0),
        "checkedSources": int(progress.get("checkedSourceCount") or 0),
        "totalSources": int(progress.get("totalSourceCount") or 0),
    }


def _progress_ratio(progress: dict[str, Any]) -> float:
    total = int(progress.get("totalSourceCount") or 0)
    if total <= 0:
        return 0.0
    checked = int(progress.get("checkedSourceCount") or 0)
    return checked / total


def _progress_throttle_seconds(payload: dict[str, Any]) -> float:
    try:
        return max(
            0.0,
            float(payload.get("progressThrottleSeconds") or _DEFAULT_PROGRESS_THROTTLE_SECONDS),
        )
    except (TypeError, ValueError):
        return _DEFAULT_PROGRESS_THROTTLE_SECONDS


def _progress_event(
    event: str,
    *,
    timestamp: str,
    family_key: str = "",
    source_id: str = "",
    source_name: str = "",
    adapter: str = "",
    jobs_found: int | None = None,
    ok: bool | None = None,
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "event": event,
        "timestamp": timestamp,
    }
    if family_key:
        row["familyKey"] = family_key
    if source_id:
        row["sourceId"] = source_id
    if source_name:
        row["sourceName"] = source_name
    if adapter:
        row["adapter"] = adapter
    if jobs_found is not None:
        row["jobsFound"] = jobs_found
    if ok is not None:
        row["ok"] = ok
    return row


def _running_adjudication_payload(
    payload: dict[str, Any], *, run_id: str, started_at: str
) -> dict[str, Any]:
    now = _now_iso()
    progress = _base_progress_payload(now)
    return {
        "ok": True,
        "status": "running",
        "runId": run_id,
        "startedAt": started_at,
        "heartbeatAt": now,
        "applyAutopilot": bool(payload.get("applyAutopilot")),
        "trigger": _clean(payload.get("trigger")),
        "checkedFamilyCount": 0,
        "checkedSourceCount": 0,
        "demoted": 0,
        "appliedIds": [],
        "families": [],
        "taskProgress": _task_progress_payload(
            active=True,
            phase_key="building_queue",
            phase_label="Building conflict queue",
            ratio=0.0,
            counts=_progress_counts(progress),
            updated_at=now,
        ),
        "progress": progress,
        "summary": _summary_payload(),
    }


def _failed_adjudication_payload(
    payload: dict[str, Any],
    *,
    run_id: str,
    started_at: str,
    error: str,
    current: dict[str, Any] | None = None,
) -> dict[str, Any]:
    now = _now_iso()
    current_payload = _as_dict(current)
    progress = _as_dict(current_payload.get("progress")) or _base_progress_payload(now)
    progress["lastProgressAt"] = now
    counts = _progress_counts(progress)
    return {
        **_running_adjudication_payload(payload, run_id=run_id, started_at=started_at),
        "status": "failed",
        "finishedAt": now,
        "heartbeatAt": now,
        "checkedFamilyCount": int(progress.get("checkedFamilyCount") or 0),
        "checkedSourceCount": int(progress.get("checkedSourceCount") or 0),
        "taskProgress": _task_progress_payload(
            active=False,
            phase_key="failed",
            phase_label="Conflict source check failed",
            ratio=_progress_ratio(progress),
            counts=counts,
            updated_at=now,
            target_label=_clean(progress.get("currentSourceName")),
            target_url=_clean(progress.get("currentEndpointUrl")),
        ),
        "progress": progress,
        "error": error,
    }


class _AdjudicationProgress:
    def __init__(
        self,
        api: Any,
        payload: dict[str, Any],
        *,
        run_id: str,
        started_at: str,
        throttle_s: float,
    ) -> None:
        self._api = api
        self._payload = dict(payload)
        self._run_id = run_id
        self._started_at = started_at
        self._throttle_s = max(0.0, throttle_s)
        self._last_write_monotonic = 0.0
        self._phase_key = "building_queue"
        self._phase_label = "Building conflict queue"
        self._progress = _base_progress_payload(_now_iso())
        self._recent_events: list[dict[str, Any]] = []
        self._completed_source_ids: set[str] = set()

    def _append_event(self, event: dict[str, Any]) -> None:
        self._recent_events.append(event)
        self._recent_events = self._recent_events[-_RECENT_PROGRESS_EVENT_LIMIT:]
        self._progress["recentEvents"] = list(self._recent_events)

    def write(self, *, force: bool = False) -> None:
        monotonic_now = time.monotonic()
        if (
            not force
            and self._last_write_monotonic
            and monotonic_now - self._last_write_monotonic < self._throttle_s
        ):
            return
        self._last_write_monotonic = monotonic_now
        now = _now_iso()
        self._progress["lastProgressAt"] = now
        counts = _progress_counts(self._progress)
        payload = {
            **_running_adjudication_payload(
                self._payload, run_id=self._run_id, started_at=self._started_at
            ),
            "heartbeatAt": now,
            "checkedFamilyCount": counts["checkedFamilies"],
            "checkedSourceCount": counts["checkedSources"],
            "taskProgress": _task_progress_payload(
                active=True,
                phase_key=self._phase_key,
                phase_label=self._phase_label,
                ratio=_progress_ratio(self._progress),
                counts=counts,
                updated_at=now,
                target_label=_clean(self._progress.get("currentSourceName")),
                target_url=_clean(self._progress.get("currentEndpointUrl")),
            ),
            "progress": dict(self._progress),
        }
        self._api.save_json_atomic(_artifact_path(self._api), payload)

    def phase(self, phase_key: str, phase_label: str) -> None:
        self._phase_key = phase_key
        self._phase_label = phase_label
        self.write(force=True)

    def set_totals(self, *, total_family_count: int, total_source_count: int) -> None:
        self._progress["totalFamilyCount"] = max(0, int(total_family_count))
        self._progress["totalSourceCount"] = max(0, int(total_source_count))
        self.write(force=True)

    def source_started(self, row: dict[str, Any], *, family_key: str, family_index: int) -> None:
        now = _now_iso()
        self._progress.update(
            {
                "currentFamilyKey": family_key,
                "currentFamilyIndex": family_index,
                "currentSourceId": _row_id(row),
                "currentSourceName": _clean(row.get("name")),
                "currentAdapter": _row_adapter(row),
                "currentEndpointUrl": _endpoint_url(row),
            }
        )
        self._append_event(
            _progress_event(
                "source_started",
                timestamp=now,
                family_key=family_key,
                source_id=_row_id(row),
                source_name=_clean(row.get("name")),
                adapter=_row_adapter(row),
            )
        )
        self.write(force=True)

    def source_finished(self, row: dict[str, Any], probe: dict[str, Any]) -> None:
        now = _now_iso()
        source_id = _row_id(row)
        if source_id:
            self._completed_source_ids.add(source_id)
        self._progress["checkedSourceCount"] = len(self._completed_source_ids)
        self._append_event(
            _progress_event(
                "source_finished",
                timestamp=now,
                family_key=_clean(self._progress.get("currentFamilyKey")),
                source_id=source_id,
                source_name=_clean(row.get("name")),
                adapter=_row_adapter(row),
                jobs_found=int(probe.get("jobsFound") or 0),
                ok=bool(probe.get("ok")),
            )
        )
        self.write(force=True)

    def family_finished(self, family_key: str, family_index: int) -> None:
        self._progress["checkedFamilyCount"] = max(
            int(self._progress.get("checkedFamilyCount") or 0),
            family_index,
        )
        self._append_event(
            _progress_event(
                "family_finished",
                timestamp=_now_iso(),
                family_key=family_key,
            )
        )
        self.write(force=True)
