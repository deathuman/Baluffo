from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Tuple


BridgeLogFunc = Callable[[str, str], None]


@dataclass(frozen=True)
class DiscoveryPaths:
    report: Any
    candidates: Any
    pending: Any
    log: Any


@dataclass(frozen=True)
class DiscoveryDeps:
    schema_version: int
    now_iso: Callable[[], str]
    now_utc: Callable[[], Any]
    parse_iso: Callable[[Any], Any]
    pid_is_running: Callable[[int], bool]
    bridge_log: Callable[[str, str], None] | Callable[..., None]
    load_json_object: Callable[[Any, Any], Any]
    save_json_atomic: Callable[[Any, Any], None]
    run_background_script: Callable[[str, List[str] | None], int]
    append_run_history: Callable[[Dict[str, Any]], Dict[str, Any]]
    normalize_discovery_report_contract: Callable[[Dict[str, Any]], Dict[str, Any]]
    load_sync_runtime_state: Callable[[], Dict[str, Any]]
    maybe_trigger_auto_sync_push: Callable[[str], bool]
    mark_discovery_sync_finished: Callable[[str], None]


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

    def watch_discovery_run_for_auto_sync(self, run_id: str, pid: int, started_at: str) -> None:
        started_dt = self._deps.parse_iso(started_at) or self._deps.now_utc()
        while self._deps.pid_is_running(pid):
            threading.Event().wait(0.8)
        try:
            report = self._deps.normalize_discovery_report_contract(
                self._deps.load_json_object(self._paths.report, {})
            )
            finished_at = str(report.get("finishedAt") or "")
            finished_dt = self._deps.parse_iso(finished_at)
            if not finished_dt or finished_dt < started_dt:
                return
            summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
            queued = int(summary.get("queuedCandidateCount") or summary.get("newCandidateCount") or 0)
            if queued <= 0:
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
        payload: Dict[str, Any] | None = None,
        enable_auto_sync_watch: bool = True,
    ) -> Tuple[int, Dict[str, Any]]:
        data = payload if isinstance(payload, dict) else {}
        preset = str(data.get("preset") or "default").strip().lower()
        run_id = f"discovery_{uuid.uuid4().hex[:10]}"
        started_at = self._deps.now_iso()
        self._deps.save_json_atomic(
            self._paths.report,
            {
                "schemaVersion": self._schema_version_int(),
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
                },
                "candidates": [],
                "failures": [],
                "topFailures": [],
                "outputs": {
                    "report": str(self._paths.report),
                    "candidates": str(self._paths.candidates),
                    "pending": str(self._paths.pending),
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
        self._deps.append_run_history(
            {
                "id": run_id,
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
            spawn_args.extend(["--top", "0"])
        else:
            preset = "default"
        try:
            pid = self._deps.run_background_script("source_discovery.py", spawn_args)
        except Exception as exc:  # noqa: BLE001
            self._deps.save_json_atomic(
                self._paths.report,
                {
                    "schemaVersion": self._schema_version_int(),
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

