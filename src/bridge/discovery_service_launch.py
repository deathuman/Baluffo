"""Discovery service discovery service launch.

AI boundary owns: bridge-owned discovery task launch, config persistence, and auto-sync watch behavior.
AI boundary implement in: this discovery_service_launch.py leaf.
AI boundary search before contracts: discovery routes, task launch API, source discovery config, and admin discovery frontend callers.
AI boundary verify: `npm run lint:repo-guardrails` plus focused discovery service tests.
"""

from __future__ import annotations

import threading
import uuid
from collections.abc import Callable
from contextlib import nullcontext
from typing import Any

from src.bridge.discovery_service_core import DiscoveryServiceState
from src.bridge.task_admission import (
    build_duplicate_start_payload,
    get_active_lifecycle_task_metadata,
)

_DISCOVERY_LAUNCH_ERRORS = (RuntimeError, OSError, ValueError)


class DiscoveryServiceLaunchMixin(DiscoveryServiceState):
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

    @staticmethod
    def _run_background_script_with_identity(
        run_background_script: Callable[..., int],
        script_name: str,
        args: list[str],
        *,
        extra_env: dict[str, str],
        run_id: str,
        task_type: str,
        metadata: dict[str, Any],
    ) -> int:
        try:
            return run_background_script(
                script_name,
                args,
                extra_env=extra_env,
                run_id=run_id,
                task_type=task_type,
                metadata=metadata,
            )
        except TypeError as exc:
            message = str(exc)
            if "unexpected keyword argument" not in message or "run_id" not in message:
                raise
            return run_background_script(script_name, args, extra_env=extra_env)

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
            self._reconcile_terminal_discovery_report_from_state()
            active_metadata = get_active_lifecycle_task_metadata(
                "discovery",
                lifecycle_rows=list(self._deps.get_lifecycle_current_runs() or []),
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
                        "targetLabel": "Spawning discovery worker",
                        "updatedAt": started_at,
                        "counts": {
                            "foundEndpoints": 0,
                            "generatedCandidates": 0,
                            "survivedDedupeCandidates": 0,
                            "probedCandidates": 0,
                            "probeTotal": 0,
                            "queuedCandidates": 0,
                            "deferredCandidates": 0,
                            "failedProbes": 0,
                            "stageIndex": 0,
                            "stageTotal": 0,
                            "completedStages": 0,
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
            try:
                self._paths.log.parent.mkdir(parents=True, exist_ok=True)
                self._paths.log.write_text(
                    f"[{started_at}] Launching source discovery task...\n", encoding="utf-8"
                )
            except OSError:
                pass
            spawn_args = ["--mode", "dynamic"]
            if preset == "uncapped":
                spawn_args.extend(["--top", "0", "--preset", "uncapped"])
            else:
                preset = "default"
                spawn_args.extend(["--preset", "default"])
            try:
                pid = self._run_background_script_with_identity(
                    self._deps.run_background_script,
                    "source_discovery.py",
                    spawn_args,
                    extra_env={
                        "BALUFFO_DISCOVERY_RUN_ID": run_id,
                        "BALUFFO_DISCOVERY_STARTED_AT": started_at,
                    },
                    run_id=run_id,
                    task_type="discovery",
                    metadata={"task": "source_discovery", "preset": preset},
                )
            except _DISCOVERY_LAUNCH_ERRORS as exc:
                failed_at = self._deps.now_iso()
                self._deps.fail_lifecycle_run(
                    run_id,
                    "discovery",
                    finished_at=failed_at,
                    terminal_reason="launch_failed",
                    summary={"error": str(exc), "failedProbeCount": 1},
                )
                self._deps.save_json_atomic(
                    self._paths.report,
                    {
                        "schemaVersion": self._schema_version_int(),
                        "runId": run_id,
                        "mode": "dynamic",
                        "startedAt": started_at,
                        "finishedAt": failed_at,
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
                                "heartbeatAt": failed_at,
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
            self._deps.start_lifecycle_run(
                run_id=run_id,
                task_type="discovery",
                started_at=started_at,
                stage="starting",
                owner_kind="process",
                owner_pid=int(pid),
                progress={
                    "active": True,
                    "phaseKey": "starting",
                    "phaseLabel": "Spawning discovery worker",
                    "mode": "indeterminate",
                    "ratio": 0.0,
                    "counts": {},
                    "updatedAt": started_at,
                },
                summary={},
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
