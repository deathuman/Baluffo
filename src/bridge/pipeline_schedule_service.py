"""Bridge-owned recurring scheduler for the Jobs pipeline.

AI boundary owns: jobs pipeline schedule persistence, evaluation, and background trigger coordination.
AI boundary implement in: this file for scheduling policy; manual task launch stays in task_launch_api and pipeline_service.
AI boundary search before contracts: pipeline task routes, pipeline service, and frontend schedule callers.
AI boundary verify: `npm run lint:repo-guardrails` plus focused pipeline schedule tests.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1
DEFAULT_INTERVAL_HOURS = 24
MIN_INTERVAL_HOURS = 1
MAX_INTERVAL_HOURS = 168
POLL_INTERVAL_SECONDS = 60.0


def _is_truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() not in {"", "0", "false", "no", "off"}


def _parse_interval_hours(value: Any) -> int:
    if isinstance(value, bool):
        raise ValueError("intervalHours must be a whole number of hours")
    if isinstance(value, int):
        interval = value
    else:
        text = str(value or "").strip()
        if not text or not text.isdecimal():
            raise ValueError("intervalHours must be a whole number of hours")
        interval = int(text)
    if interval < MIN_INTERVAL_HOURS or interval > MAX_INTERVAL_HOURS:
        raise ValueError("intervalHours must be between 1 and 168")
    return interval


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


class PipelineScheduleService:
    def __init__(
        self,
        *,
        config_path: Path,
        load_json_object: Callable[[Path, Any], Any],
        save_json_atomic: Callable[[Path, Any], None],
        now_iso: Callable[[], str],
        parse_iso: Callable[[Any], datetime | None],
        bridge_log: Callable[..., None],
        get_lifecycle_current_runs: Callable[[], list[dict[str, Any]]],
        get_lifecycle_recent_runs: Callable[[], list[dict[str, Any]]],
        get_jobs_pipeline_status_payload: Callable[[], dict[str, Any]],
        start_jobs_pipeline_task: Callable[[dict[str, Any] | None], dict[str, Any]],
        timer_factory: Callable[[float, Callable[[], None]], Any] | None = None,
    ) -> None:
        self._config_path = Path(config_path)
        self._load_json_object = load_json_object
        self._save_json_atomic = save_json_atomic
        self._now_iso = now_iso
        self._parse_iso = parse_iso
        self._bridge_log = bridge_log
        self._get_lifecycle_current_runs = get_lifecycle_current_runs
        self._get_lifecycle_recent_runs = get_lifecycle_recent_runs
        self._get_jobs_pipeline_status_payload = get_jobs_pipeline_status_payload
        self._start_jobs_pipeline_task = start_jobs_pipeline_task
        self._timer_factory = timer_factory or self._default_timer_factory
        self._lock = threading.RLock()
        self._timer: Any | None = None
        self._started = False
        self._pending = False
        self._last_trigger_run_id = ""
        self._last_trigger_error = ""

    @staticmethod
    def default_config() -> dict[str, Any]:
        return {
            "schemaVersion": SCHEMA_VERSION,
            "enabled": False,
            "intervalHours": DEFAULT_INTERVAL_HOURS,
        }

    @staticmethod
    def normalize_config(payload: dict[str, Any] | None = None) -> dict[str, Any]:
        data = payload if isinstance(payload, dict) else {}
        default = PipelineScheduleService.default_config()
        interval_source = data.get("intervalHours", default["intervalHours"])
        normalized: dict[str, Any] = {
            "schemaVersion": SCHEMA_VERSION,
            "enabled": _is_truthy(data.get("enabled", default["enabled"])),
            "intervalHours": _parse_interval_hours(interval_source),
        }
        configured_at = _clean_text(data.get("configuredAt"))
        if normalized["enabled"] and configured_at:
            normalized["configuredAt"] = configured_at
        return normalized

    def load_config(self) -> dict[str, Any]:
        raw = self._load_json_object(self._config_path, {})
        if not isinstance(raw, dict):
            return self.default_config()
        try:
            return self.normalize_config(raw)
        except ValueError:
            return self.default_config()

    def update_config(self, payload: dict[str, Any] | None) -> dict[str, Any]:
        normalized = self.normalize_config(payload)
        if normalized["enabled"]:
            normalized["configuredAt"] = self._now_iso()
        else:
            normalized.pop("configuredAt", None)
        with self._lock:
            if not normalized["enabled"]:
                self._pending = False
            self._save_json_atomic(self._config_path, normalized)
        self.evaluate_due(reason="config_update")
        return self.get_payload()

    def get_payload(self) -> dict[str, Any]:
        config = self.load_config()
        return {
            "ok": True,
            "savedConfig": dict(config),
            "status": self.get_status(config=config),
        }

    def get_status(self, config: dict[str, Any] | None = None) -> dict[str, Any]:
        cfg = config if isinstance(config, dict) else self.load_config()
        schedule = self._schedule_state(cfg)
        active_pipeline = self._active_pipeline_context()
        with self._lock:
            if not bool(schedule["due"]):
                self._pending = False
            pending = bool(self._pending)
            last_trigger_run_id = self._last_trigger_run_id
            last_trigger_error = self._last_trigger_error
        active_run_id = _clean_text(active_pipeline.get("runId"))
        active_schedule_due = bool(schedule["due"] and active_pipeline.get("active"))
        if active_schedule_due:
            pending = False
        return {
            "enabled": bool(cfg.get("enabled")),
            "pending": pending,
            "due": False if active_schedule_due else bool(schedule["due"]),
            "nextRunAt": str(schedule["nextRunAt"]),
            "lastPipelineFinishedAt": str(schedule["lastPipelineFinishedAt"]),
            "lastTriggerRunId": last_trigger_run_id,
            "lastTriggerError": last_trigger_error,
            **(
                {
                    "activeScheduledRun": bool(
                        active_run_id and active_run_id == _clean_text(last_trigger_run_id)
                    ),
                    "blockedByActiveRun": True,
                    "nextAfterCurrentCompletes": True,
                    "activeRunId": active_run_id,
                }
                if active_schedule_due
                else {}
            ),
        }

    def get_ops_schedule_entry(self) -> dict[str, Any]:
        payload = self.get_payload()
        saved = dict(payload.get("savedConfig") or {})
        status = dict(payload.get("status") or {})
        enabled = bool(saved.get("enabled"))
        return {
            **status,
            "intervalHours": int(saved.get("intervalHours") or DEFAULT_INTERVAL_HOURS),
            "note": "configured" if enabled else "disabled",
        }

    def start_background_polling(self) -> dict[str, Any]:
        with self._lock:
            self._started = True
        result = self.evaluate_due(reason="startup")
        self._schedule_next_timer()
        return result

    def evaluate_due(self, *, reason: str = "poll") -> dict[str, Any]:
        config = self.load_config()
        if not bool(config.get("enabled")):
            with self._lock:
                self._pending = False
            return {"started": False, "enabled": False, "pending": False, "reason": "disabled"}

        schedule = self._schedule_state(config)
        if not bool(schedule["due"]):
            with self._lock:
                self._pending = False
            return {
                "started": False,
                "enabled": True,
                "pending": False,
                "due": False,
                "nextRunAt": str(schedule["nextRunAt"]),
            }

        if not self._is_idle():
            active_pipeline = self._active_pipeline_context()
            with self._lock:
                self._pending = not bool(active_pipeline.get("active"))
                self._last_trigger_error = ""
            self._bridge_log(
                "info",
                "jobs_pipeline_schedule_deferred",
                reason=reason,
                nextRunAt=str(schedule["nextRunAt"]),
                activeRunId=_clean_text(active_pipeline.get("runId")),
            )
            return {
                "started": False,
                "enabled": True,
                "pending": not bool(active_pipeline.get("active")),
                "due": False if active_pipeline.get("active") else True,
                "nextRunAt": str(schedule["nextRunAt"]),
                **(
                    {
                        "blockedByActiveRun": True,
                        "nextAfterCurrentCompletes": True,
                        "activeRunId": _clean_text(active_pipeline.get("runId")),
                    }
                    if active_pipeline.get("active")
                    else {}
                ),
            }

        return self._start_scheduled_pipeline(reason=reason)

    def _schedule_state(self, config: dict[str, Any]) -> dict[str, Any]:
        last_row = self._latest_terminal_pipeline_row()
        last_finished_at = _clean_text((last_row or {}).get("finishedAt"))
        next_run_at = ""
        due = False
        if bool(config.get("enabled")):
            if not last_finished_at:
                anchor = self._schedule_anchor_datetime(config)
                if anchor is not None:
                    next_dt = anchor + timedelta(
                        hours=int(config.get("intervalHours") or DEFAULT_INTERVAL_HOURS)
                    )
                    next_run_at = next_dt.isoformat()
                    now = self._parse_iso(self._now_iso())
                    if now is not None:
                        due = now >= next_dt
            else:
                last_finished = self._parse_iso(last_finished_at)
                now = self._parse_iso(self._now_iso())
                if last_finished is not None:
                    next_dt = last_finished + timedelta(
                        hours=int(config.get("intervalHours") or DEFAULT_INTERVAL_HOURS)
                    )
                    next_run_at = next_dt.isoformat()
                    if now is not None:
                        due = now >= next_dt
        return {
            "due": due,
            "nextRunAt": next_run_at,
            "lastPipelineFinishedAt": last_finished_at,
        }

    def _schedule_anchor_datetime(self, config: dict[str, Any]) -> datetime | None:
        configured_at = _clean_text(config.get("configuredAt"))
        parsed = self._parse_iso(configured_at)
        if parsed is not None:
            return parsed
        try:
            mtime = self._config_path.stat().st_mtime
            return datetime.fromtimestamp(mtime, tz=UTC)
        except OSError:
            pass
        return self._parse_iso(self._now_iso())

    def _active_pipeline_context(self) -> dict[str, Any]:
        payload = self._get_jobs_pipeline_status_payload() or {}
        if not isinstance(payload, dict):
            return {"active": False, "runId": ""}
        active = bool(
            payload.get("active")
            or payload.get("running")
            or payload.get("activeChildRunId")
            or payload.get("activeChildren")
        )
        return {
            "active": active,
            "runId": _clean_text(payload.get("runId")),
            "activeChildRunId": _clean_text(payload.get("activeChildRunId")),
        }

    def _latest_terminal_pipeline_row(self) -> dict[str, Any] | None:
        candidates: list[tuple[datetime, int, dict[str, Any]]] = []
        for index, row in enumerate(self._get_lifecycle_recent_runs() or []):
            if not isinstance(row, dict):
                continue
            task_type = _clean_text(row.get("taskType") or row.get("type")).lower()
            if task_type != "pipeline":
                continue
            finished_at = _clean_text(row.get("finishedAt"))
            if not finished_at:
                continue
            parsed = self._parse_iso(finished_at)
            if parsed is None:
                continue
            candidates.append((parsed, index, dict(row)))
        if not candidates:
            return None
        return max(candidates, key=lambda item: (item[0], item[1]))[2]

    def _is_idle(self) -> bool:
        pipeline_status = self._get_jobs_pipeline_status_payload() or {}
        if bool(pipeline_status.get("active")) or bool(pipeline_status.get("running")):
            return False
        for row in self._get_lifecycle_current_runs() or []:
            if not isinstance(row, dict):
                continue
            lifecycle_status = _clean_text(row.get("lifecycleStatus") or row.get("status")).lower()
            if bool(row.get("active")) or lifecycle_status in {"", "queued", "running", "started"}:
                return False
        return True

    def _start_scheduled_pipeline(self, *, reason: str) -> dict[str, Any]:
        result = self._start_jobs_pipeline_task(
            {
                "trigger": "schedule",
                "automatic": True,
                "jobsPageLoadedCount": 0,
            }
        )
        started = bool((result or {}).get("started"))
        run_id = _clean_text((result or {}).get("runId"))
        error = (
            ""
            if started
            else _clean_text((result or {}).get("error") or (result or {}).get("reason"))
        )
        with self._lock:
            self._pending = False if started else True
            self._last_trigger_run_id = run_id if started else self._last_trigger_run_id
            self._last_trigger_error = error
        self._bridge_log(
            "info" if started else "warn",
            "jobs_pipeline_schedule_triggered" if started else "jobs_pipeline_schedule_blocked",
            reason=reason,
            runId=run_id,
            error=error,
        )
        return {
            **dict(result or {}),
            "scheduled": True,
            "pending": not started,
            "trigger": "schedule",
        }

    def _schedule_next_timer(self) -> None:
        with self._lock:
            if not self._started:
                return
            previous_timer = self._timer
            timer = self._timer_factory(POLL_INTERVAL_SECONDS, self._on_timer)
            try:
                timer.daemon = True
            except (AttributeError, TypeError):
                pass
            self._timer = timer
            if previous_timer is not None and previous_timer is not timer:
                try:
                    previous_timer.cancel()
                except (AttributeError, RuntimeError, TypeError):
                    pass
        try:
            timer.start()
        except (AttributeError, RuntimeError, OSError, TypeError) as exc:
            self._bridge_log("warn", "jobs_pipeline_schedule_timer_failed", error=str(exc))

    def _on_timer(self) -> None:
        try:
            self.evaluate_due(reason="timer")
        finally:
            self._schedule_next_timer()

    @staticmethod
    def _default_timer_factory(delay_s: float, callback: Callable[[], None]) -> threading.Timer:
        return threading.Timer(delay_s, callback)


__all__ = ["PipelineScheduleService"]
