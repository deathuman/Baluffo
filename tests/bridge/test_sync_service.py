from __future__ import annotations

import threading
from typing import Any

from src.bridge.sync_service import SyncService
from src.bridge.sync_state import ACTIVE_SYNC_RUNS, ACTIVE_SYNC_THREADS, SYNC_STATE_LOCK
from tests.helpers.temp_paths import workspace_tmpdir


class _FakeSourceSync:
    def __init__(self) -> None:
        self.pull_calls: int = 0
        self.push_calls: int = 0
        self.remote_reads: int = 0
        self._enabled = True
        self._ready = True
        self.rate_limit = {
            "remaining": 42,
            "limit": 100,
            "remainingPercent": 42.0,
            "resetAt": "2026-04-25T12:30:00+00:00",
            "until": "",
            "strike": 0,
            "low": False,
        }

    def config_status(self, config: Any) -> dict[str, Any]:
        return {"enabled": bool(self._enabled), "ready": bool(self._ready)}

    def resolve_sync_config(
        self, settings: dict[str, Any] | None = None, env: dict[str, str] | None = None
    ) -> Any:
        # Return a simple token object; SyncService treats it as opaque.
        return {"settings": dict(settings or {}), "env": bool(env)}

    def read_remote_snapshot(self, config: Any) -> dict[str, Any]:
        self.remote_reads += 1
        return {"exists": True, "sha": "abc"}

    def pull_and_merge_sources(self, config: Any, local_state: dict[str, Any]) -> dict[str, Any]:
        self.pull_calls += 1
        return {
            "changed": True,
            "remoteFound": True,
            "remoteSha": "abc",
            "mergedState": {"active": [], "pending": [], "rejected": []},
        }

    def push_sources_snapshot(self, config: Any, state: dict[str, Any]) -> dict[str, Any]:
        self.push_calls += 1
        return {"remoteSha": "abc", "remotePreviouslyExisted": True, "snapshot": state}

    def rate_limit_payload(self) -> dict[str, Any]:
        return dict(self.rate_limit)


class _RunHistory:
    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []

    def append(self, row: dict[str, Any]) -> dict[str, Any]:
        self.rows.append(dict(row))
        return self.rows[-1]

    def upsert(self, entry: dict[str, Any], *, dedupe_fields: tuple[str, ...]) -> dict[str, Any]:
        self.rows.append(dict(entry))
        return self.rows[-1]

    def load(self) -> list[dict[str, Any]]:
        return list(self.rows)

    def prune_started_rows_for_type(self, entry_type: str, finished_at: str) -> None:
        return


def test_sync_service_status_exposes_rate_limit_payload() -> None:
    with workspace_tmpdir("sync-service") as data_dir:
        source_sync = _FakeSourceSync()

        svc = SyncService(
            data_dir=data_dir,
            source_sync=source_sync,
            bridge_log=lambda _level, _message, **_fields: None,
            load_state=lambda: {"active": [], "pending": [], "rejected": []},
            persist_state=lambda state: state,
            summarize_state=lambda _state: {
                "activeCount": 0,
                "pendingCount": 0,
                "rejectedCount": 0,
            },
            run_history=_RunHistory(),
            ops_state_lock=threading.RLock(),
            get_security_defaults=lambda: {"github_app_enabled_default": True},
            get_registry_auto_heal_report=lambda: {
                "autoHealed": True,
                "duplicateSourceIdCount": 1,
                "duplicates": [{"sourceId": "greenhouse:slug:guerrilla-games"}],
            },
        )

        payload = svc.get_sync_status_payload()
        runtime = payload["runtime"]
        assert runtime["rateLimit"] == source_sync.rate_limit
        assert payload["registryAutoHeal"]["duplicateSourceIdCount"] == 1


def test_sync_service_pull_delegates_and_persists() -> None:
    with workspace_tmpdir("sync-service") as data_dir:
        with SYNC_STATE_LOCK:
            ACTIVE_SYNC_RUNS.clear()
            ACTIVE_SYNC_THREADS.clear()
        source_sync = _FakeSourceSync()
        history = _RunHistory()
        ops_lock = threading.RLock()

        persisted: dict[str, Any] = {
            "active": [{"adapter": "static", "listing_url": "https://a.com/jobs"}],
            "pending": [],
            "rejected": [],
        }

        def load_state() -> dict[str, list[dict[str, Any]]]:
            return {
                "active": list(persisted["active"]),
                "pending": list(persisted["pending"]),
                "rejected": list(persisted["rejected"]),
            }

        def persist_state(
            state: dict[str, list[dict[str, Any]]],
        ) -> dict[str, list[dict[str, Any]]]:
            persisted["active"] = list(state.get("active") or [])
            persisted["pending"] = list(state.get("pending") or [])
            persisted["rejected"] = list(state.get("rejected") or [])
            return load_state()

        def summarize_state(state: dict[str, list[dict[str, Any]]]) -> dict[str, int]:
            return {
                "activeCount": len(state["active"]),
                "pendingCount": len(state["pending"]),
                "rejectedCount": len(state["rejected"]),
            }

        def bridge_log(_level: str, _message: str, **_fields: Any) -> None:
            return

        def get_security_defaults() -> dict[str, Any]:
            return {"github_app_enabled_default": True}

        svc = SyncService(
            data_dir=data_dir,
            source_sync=source_sync,
            bridge_log=bridge_log,
            load_state=load_state,
            persist_state=persist_state,
            summarize_state=summarize_state,
            run_history=history,
            ops_state_lock=ops_lock,
            get_security_defaults=get_security_defaults,
        )

        # Enablement is controlled by settings; write enabled=True and proceed.
        svc.update_saved_sync_settings({"enabled": True})
        result = svc.sync_pull_sources()
        assert bool(result.get("ok")) is True
        assert source_sync.pull_calls == 1
        timing = result.get("timing")
        assert isinstance(timing, dict)
        assert timing["action"] == "pull"
        assert timing["pulled"] is True
        assert timing["stageTotalsMs"]["loadLocalRegistry"] >= 0
        assert timing["stageTotalsMs"]["pullMergeRemote"] >= 0
        assert timing["stageTotalsMs"]["applyLocal"] >= 0
        assert timing["stageTotalsMs"]["summarizeLocal"] >= 0
        assert any(row["stage"] == "pullMergeRemote" for row in timing["stageTop"])

        status = svc.get_sync_status_payload()
        assert status["timing"]["action"] == "pull"
        assert status["timingHistory"][-1]["action"] == "pull"


def test_sync_service_push_returns_and_persists_timing() -> None:
    with workspace_tmpdir("sync-service") as data_dir:
        with SYNC_STATE_LOCK:
            ACTIVE_SYNC_RUNS.clear()
            ACTIVE_SYNC_THREADS.clear()
        source_sync = _FakeSourceSync()
        history = _RunHistory()
        ops_lock = threading.RLock()

        def load_state() -> dict[str, list[dict[str, Any]]]:
            return {
                "active": [{"adapter": "static", "listing_url": "https://a.com/jobs"}],
                "pending": [],
                "rejected": [],
            }

        def persist_state(
            state: dict[str, list[dict[str, Any]]],
        ) -> dict[str, list[dict[str, Any]]]:
            return state

        def summarize_state(state: dict[str, list[dict[str, Any]]]) -> dict[str, int]:
            return {
                "activeCount": len(state["active"]),
                "pendingCount": len(state["pending"]),
                "rejectedCount": len(state["rejected"]),
            }

        svc = SyncService(
            data_dir=data_dir,
            source_sync=source_sync,
            bridge_log=lambda _level, _message, **_fields: None,
            load_state=load_state,
            persist_state=persist_state,
            summarize_state=summarize_state,
            run_history=history,
            ops_state_lock=ops_lock,
            get_security_defaults=lambda: {"github_app_enabled_default": True},
        )

        svc.update_saved_sync_settings({"enabled": True})
        result = svc.sync_push_sources()

        assert bool(result.get("ok")) is True
        assert source_sync.push_calls == 1
        timing = result.get("timing")
        assert isinstance(timing, dict)
        assert timing["action"] == "push"
        assert timing["pushed"] is True
        assert timing["stageTotalsMs"]["loadLocalRegistry"] >= 0
        assert timing["stageTotalsMs"]["pushRemote"] >= 0
        assert timing["stageTotalsMs"]["summarizeSnapshot"] >= 0
        assert any(row["stage"] == "pushRemote" for row in timing["stageTop"])

        status = svc.get_sync_status_payload()
        assert status["timing"]["action"] == "push"
        assert status["timingHistory"][-1]["action"] == "push"


def test_sync_service_start_task_runs_and_finishes() -> None:
    with workspace_tmpdir("sync-service") as data_dir:
        with SYNC_STATE_LOCK:
            ACTIVE_SYNC_RUNS.clear()
            ACTIVE_SYNC_THREADS.clear()
        source_sync = _FakeSourceSync()
        history = _RunHistory()
        ops_lock = threading.RLock()

        def load_state() -> dict[str, list[dict[str, Any]]]:
            return {"active": [], "pending": [], "rejected": []}

        def persist_state(
            state: dict[str, list[dict[str, Any]]],
        ) -> dict[str, list[dict[str, Any]]]:
            return state

        def summarize_state(state: dict[str, list[dict[str, Any]]]) -> dict[str, int]:
            return {"activeCount": 0, "pendingCount": 0, "rejectedCount": 0}

        def bridge_log(_level: str, _message: str, **_fields: Any) -> None:
            return

        def get_security_defaults() -> dict[str, Any]:
            return {"github_app_enabled_default": True}

        svc = SyncService(
            data_dir=data_dir,
            source_sync=source_sync,
            bridge_log=bridge_log,
            load_state=load_state,
            persist_state=persist_state,
            summarize_state=summarize_state,
            run_history=history,
            ops_state_lock=ops_lock,
            get_security_defaults=get_security_defaults,
        )
        svc.update_saved_sync_settings({"enabled": True})

        started = svc.start_sync_task("pull", reason="test", automatic=False)
        assert bool(started.get("started")) is True
        svc.wait_for_sync_tasks(timeout_s=2.0)
        assert source_sync.pull_calls >= 1
        completed_rows = [row for row in history.rows if row.get("status") == "ok"]
        assert completed_rows
        timing = completed_rows[-1]["summary"]["timing"]
        assert timing["action"] == "pull"
        assert "stageTotalsMs" in timing
        assert "stageTop" in timing
