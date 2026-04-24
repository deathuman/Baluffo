"""Discovery service for source discovery operations.

This module provides DiscoveryService for managing source discovery
tasks and auto-sync watch functionality.
"""

from __future__ import annotations

import threading
import uuid
from collections.abc import Callable
from contextlib import nullcontext
from dataclasses import dataclass
from typing import Any

from src.bridge.task_admission import build_duplicate_start_payload, get_active_task_metadata
from src.shared.json_shapes import as_json_object
from src.source_registry import (
    _pending_row_is_auto_approvable as registry_pending_row_is_auto_approvable,
)
from src.source_registry import (
    apply_discovery_auto_approval,
)

BridgeLogFunc = Callable[..., None]


@dataclass(frozen=True)
class DiscoveryPaths:
    report: Any
    candidates: Any
    pending: Any
    log: Any
    settings: Any
    approval_state: Any
    task_state: Any | None = None


@dataclass(frozen=True)
class DiscoveryDeps:
    schema_version: int
    now_iso: Callable[[], str]
    now_utc: Callable[[], Any]
    parse_iso: Callable[[Any], Any]
    pid_is_running: Callable[[int], bool]
    bridge_log: BridgeLogFunc
    load_json_object: Callable[[Any, Any], Any]
    save_json_atomic: Callable[[Any, Any], None]
    run_background_script: Callable[..., int]
    append_run_history: Callable[[dict[str, Any]], dict[str, Any]]
    upsert_run_history: Callable[..., dict[str, Any]]
    prune_started_rows_for_type: Callable[..., None]
    clear_task_state: Callable[[str], None]
    normalize_discovery_report_contract: Callable[[dict[str, Any]], dict[str, Any]]
    load_state: Callable[[], dict[str, list[dict[str, Any]]]]
    persist_state_and_auto_sync: Callable[..., dict[str, list[dict[str, Any]]]]
    load_sync_runtime_state: Callable[[], dict[str, Any]]
    maybe_trigger_auto_sync_push: Callable[[str], bool]
    mark_discovery_sync_finished: Callable[[str], None]
    task_state_lock: Any | None = None


class DiscoveryService:
    def __init__(self, *, paths: DiscoveryPaths, deps: DiscoveryDeps) -> None:
        self._paths = paths
        self._deps = deps

    def _schema_version_int(self) -> int:
        """Return a safe integer schema version, tolerating string values like '1.0'."""
        raw = self._deps.schema_version
        try:
            return int(raw)
        except (TypeError, ValueError):
            try:
                return int(float(str(raw)))
            except (TypeError, ValueError):
                return 1

    def _finalize_discovery_run(
        self,
        *,
        run_id: str,
        started_at: str,
        finished_at: str,
        summary: dict[str, Any],
    ) -> None:
        started_dt = self._deps.parse_iso(started_at)
        finished_dt = self._deps.parse_iso(finished_at)
        duration_ms = (
            int(max(0.0, (finished_dt - started_dt).total_seconds() * 1000))
            if started_dt and finished_dt
            else 0
        )
        failed_probe_count = int(summary.get("failedProbeCount") or 0)
        probe_miss_count = int(summary.get("probeMissCount") or 0)
        status = "warning" if (failed_probe_count > 0 or probe_miss_count > 0) else "ok"
        self._deps.clear_task_state("discovery")
        self._deps.prune_started_rows_for_type("discovery", finished_at=finished_at)
        self._deps.upsert_run_history(
            {
                "id": run_id,
                "runId": run_id,
                "type": "discovery",
                "status": status,
                "startedAt": started_at,
                "finishedAt": finished_at,
                "durationMs": duration_ms,
                "summary": dict(summary or {}),
            },
            dedupe_fields=("type", "runId"),
        )

    @staticmethod
    def _normalize_discovery_settings(payload: dict[str, Any] | None = None) -> dict[str, Any]:
        data = payload if isinstance(payload, dict) else {}
        raw = data.get("autoApproveHealthyPendingOnComplete", True)
        if isinstance(raw, bool):
            enabled = raw
        else:
            enabled = str(raw or "").strip().lower() not in {"", "0", "false", "no", "off"}
        return {"autoApproveHealthyPendingOnComplete": bool(enabled)}

    def load_saved_discovery_settings(self) -> dict[str, Any]:
        raw = self._deps.load_json_object(self._paths.settings, {})
        if isinstance(raw, dict) and "autoApproveHealthyPendingOnComplete" in raw:
            return self._normalize_discovery_settings(raw)
        return {}

    def get_saved_discovery_config_payload(self) -> dict[str, Any]:
        settings = self.load_saved_discovery_settings()
        if "autoApproveHealthyPendingOnComplete" in settings:
            return {
                "autoApproveHealthyPendingOnComplete": bool(
                    settings.get("autoApproveHealthyPendingOnComplete")
                )
            }
        return self._normalize_discovery_settings({})

    def get_discovery_config_payload(self) -> dict[str, Any]:
        return {
            "ok": True,
            "savedConfig": self.get_saved_discovery_config_payload(),
        }

    def update_saved_discovery_settings(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = self._normalize_discovery_settings(payload)
        self._deps.save_json_atomic(self._paths.settings, normalized)
        return normalized

    @classmethod
    def _pending_row_is_auto_approvable(cls, row: dict[str, Any]) -> bool:
        return registry_pending_row_is_auto_approvable(row)

    def watch_discovery_run_for_auto_sync(self, run_id: str, pid: int, started_at: str) -> None:
        started_dt = self._deps.parse_iso(started_at) or self._deps.now_utc()
        report: dict[str, Any] = {}
        while True:
            report = self._deps.normalize_discovery_report_contract(
                self._deps.load_json_object(self._paths.report, {})
            )
            finished_at = str(report.get("finishedAt") or "")
            finished_dt = self._deps.parse_iso(finished_at)
            if finished_dt and finished_dt >= started_dt:
                break
            if not self._deps.pid_is_running(pid):
                # Worker exited without a terminal report (crash/kill); clear stale task state
                # so ops/UI do not show discovery as running forever.
                self._deps.clear_task_state("discovery")
                return
            threading.Event().wait(0.8)
        try:
            finished_at = str(report.get("finishedAt") or "")
            finished_dt = self._deps.parse_iso(finished_at)
            if not finished_dt or finished_dt < started_dt:
                return
            summary = as_json_object(report.get("summary"))
            self._finalize_discovery_run(
                run_id=run_id,
                started_at=started_at,
                finished_at=finished_at,
                summary=summary,
            )
            queued = int(
                summary.get("queuedCandidateCount") or summary.get("newCandidateCount") or 0
            )
            saved_config = self.get_saved_discovery_config_payload()
            auto_approve_enabled = bool(saved_config.get("autoApproveHealthyPendingOnComplete"))
            state, auto_approved = apply_discovery_auto_approval(
                self._deps.load_state(),
                report,
                auto_approve_enabled=auto_approve_enabled,
                approval_state_path=self._paths.approval_state,
                now_iso_fn=self._deps.now_iso,
            )
            if auto_approved > 0:
                self._deps.persist_state_and_auto_sync(state, reason="discovery_auto_approve")
            self._deps.save_json_atomic(self._paths.report, report)
            self._deps.bridge_log(
                "info",
                "discovery_auto_approval_completed",
                runId=run_id,
                enabled=auto_approve_enabled,
                approved=int(auto_approved),
            )
            if queued <= 0 and auto_approved <= 0:
                self._deps.mark_discovery_sync_finished(finished_at)
                return
            runtime_state = self._deps.load_sync_runtime_state()
            if str(runtime_state.get("lastDiscoverySyncFinishedAt") or "") == finished_at:
                return
            if self._deps.maybe_trigger_auto_sync_push("discovery_completed"):
                self._deps.mark_discovery_sync_finished(finished_at)
                self._deps.bridge_log(
                    "info",
                    "sync_auto_push_started",
                    runId=run_id,
                    reason="discovery_completed",
                    queued=queued,
                    autoApproved=int(auto_approved),
                )
        except Exception as exc:  # noqa: BLE001
            self._deps.bridge_log(
                "warn",
                "sync_auto_push_skipped",
                runId=run_id,
                reason="discovery_completed",
                error=str(exc),
            )

    def trigger_discovery_task(
        self,
        *,
        route_name: str,
        payload: dict[str, Any] | None = None,
        enable_auto_sync_watch: bool = True,
    ) -> tuple[int, dict[str, Any]]:
        data = payload if isinstance(payload, dict) else {}
        preset = str(data.get("preset") or "default").strip().lower()
        lock_context = (
            self._deps.task_state_lock if self._deps.task_state_lock is not None else nullcontext()
        )
        with lock_context:
            active_metadata = get_active_task_metadata(
                "discovery",
                load_json_object=self._deps.load_json_object,
                task_state_path=self._paths.task_state,
                pid_is_running=self._deps.pid_is_running,
            )
            if active_metadata:
                response = build_duplicate_start_payload(
                    "source_discovery", "discovery", active_metadata
                )
                self._deps.bridge_log(
                    "info",
                    "task_start_attached_existing",
                    task="source_discovery",
                    taskType="discovery",
                    route=route_name,
                    runId=str(response.get("runId") or ""),
                    pid=int(response.get("pid") or 0),
                )
                return 409, response

            run_id = f"discovery_{uuid.uuid4().hex[:10]}"
            started_at = self._deps.now_iso()
            self._deps.bridge_log(
                "info",
                "discovery_launch_started",
                runId=run_id,
                route=route_name,
                preset=preset or "default",
            )
            self._deps.save_json_atomic(
                self._paths.report,
                {
                    "schemaVersion": self._schema_version_int(),
                    "runId": run_id,
                    "mode": "dynamic",
                    "startedAt": started_at,
                    "finishedAt": "",
                    "summary": {
                        "foundEndpointCount": 0,
                        "probedCandidateCount": 0,
                        "queuedCandidateCount": 0,
                        "failedProbeCount": 0,
                        "skippedDuplicateCount": 0,
                        "skippedLowEvidenceProbeCount": 0,
                        "phase": "starting",
                        "phaseKey": "starting",
                        "phaseLabel": "Spawning discovery worker",
                    },
                    "taskProgress": {
                        "active": True,
                        "phaseKey": "starting",
                        "phaseLabel": "Spawning discovery worker",
                        "mode": "indeterminate",
                        "ratio": 0.0,
                        "counts": {
                            "foundEndpoints": 0,
                            "probedCandidates": 0,
                            "probeTotal": 0,
                            "queuedCandidates": 0,
                            "deferredCandidates": 0,
                            "failedProbes": 0,
                        },
                    },
                    "candidates": [],
                    "failures": [],
                    "topFailures": [],
                    "outputs": {
                        "report": str(self._paths.report),
                        "candidates": str(self._paths.candidates),
                        "pending": str(self._paths.pending),
                    },
                    "runtime": {
                        "lifecycle": {
                            "owner": "discovery_report",
                            "heartbeatAt": started_at,
                        },
                        "autoApproval": {
                            "enabled": bool(
                                self.get_saved_discovery_config_payload().get(
                                    "autoApproveHealthyPendingOnComplete"
                                )
                            ),
                            "approvedCount": 0,
                        },
                    },
                },
            )
            self._deps.prune_started_rows_for_type("discovery", keep_started_at=started_at)
            try:
                self._paths.log.parent.mkdir(parents=True, exist_ok=True)
                self._paths.log.write_text(
                    f"[{started_at}] Launching source discovery task...\n", encoding="utf-8"
                )
            except OSError:
                pass
            self._deps.append_run_history(
                {
                    "id": run_id,
                    "runId": run_id,
                    "type": "discovery",
                    "status": "started",
                    "startedAt": started_at,
                    "finishedAt": "",
                    "durationMs": 0,
                    "summary": {},
                }
            )
            spawn_args = ["--mode", "dynamic"]
            if preset == "uncapped":
                spawn_args.extend(["--top", "0", "--preset", "uncapped"])
            else:
                preset = "default"
                spawn_args.extend(["--preset", "default"])
            try:
                pid = self._deps.run_background_script(
                    "source_discovery.py",
                    spawn_args,
                    extra_env={
                        "BALUFFO_DISCOVERY_RUN_ID": run_id,
                        "BALUFFO_DISCOVERY_STARTED_AT": started_at,
                    },
                )
            except Exception as exc:  # noqa: BLE001
                self._deps.save_json_atomic(
                    self._paths.report,
                    {
                        "schemaVersion": self._schema_version_int(),
                        "runId": run_id,
                        "mode": "dynamic",
                        "startedAt": started_at,
                        "finishedAt": self._deps.now_iso(),
                        "summary": {
                            "foundEndpointCount": 0,
                            "probedCandidateCount": 0,
                            "queuedCandidateCount": 0,
                            "failedProbeCount": 1,
                        },
                        "candidates": [],
                        "failures": [
                            {
                                "name": "source_discovery.py",
                                "adapter": "bridge",
                                "error": str(exc),
                                "stage": "launch",
                            }
                        ],
                        "topFailures": [{"key": "bridge:launch", "count": 1}],
                        "outputs": {
                            "report": str(self._paths.report),
                            "candidates": str(self._paths.candidates),
                            "pending": str(self._paths.pending),
                        },
                        "runtime": {
                            "lifecycle": {
                                "owner": "discovery_report",
                                "heartbeatAt": self._deps.now_iso(),
                            },
                            "autoApproval": {
                                "enabled": bool(
                                    self.get_saved_discovery_config_payload().get(
                                        "autoApproveHealthyPendingOnComplete"
                                    )
                                ),
                                "approvedCount": 0,
                            },
                        },
                    },
                )
                try:
                    with self._paths.log.open("a", encoding="utf-8") as handle:
                        handle.write(f"[{self._deps.now_iso()}] Launch failed: {str(exc)}\n")
                except OSError:
                    pass
                self._deps.bridge_log(
                    "error",
                    "task_start_failed",
                    runId=run_id,
                    task="source_discovery",
                    mode="dynamic",
                    route=route_name,
                    error=str(exc),
                )
                return 500, {
                    "started": False,
                    "task": "source_discovery",
                    "mode": "dynamic",
                    "preset": preset,
                    "route": route_name,
                    "error": str(exc),
                }
            if enable_auto_sync_watch:
                watcher = threading.Thread(
                    target=self.watch_discovery_run_for_auto_sync,
                    args=(run_id, pid, started_at),
                    name=f"discovery-sync-watch-{run_id}",
                    daemon=True,
                )
                watcher.start()
            self._deps.bridge_log(
                "info",
                "task_started",
                runId=run_id,
                task="source_discovery",
                mode="dynamic",
                preset=preset,
                route=route_name,
                pid=int(pid),
            )
            return 200, {
                "started": True,
                "runId": run_id,
                "task": "source_discovery",
                "mode": "dynamic",
                "preset": preset,
                "args": spawn_args,
                "route": route_name,
                "startedAt": started_at,
                "pid": int(pid),
            }


__all__ = ["DiscoveryDeps", "DiscoveryPaths", "DiscoveryService"]
