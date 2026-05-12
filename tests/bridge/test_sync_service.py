from __future__ import annotations

import threading
from typing import Any

from src.bridge.sync_service import SyncService
from src.bridge.sync_state import ACTIVE_SYNC_RUNS, ACTIVE_SYNC_THREADS, SYNC_STATE_LOCK
from src.storage import BaluffoStore, TaskRuntimeStore
from tests.helpers.temp_paths import workspace_tmpdir


class _FakeSourceSync:
    def __init__(self) -> None:
        self.pull_calls: int = 0
        self.push_calls: int = 0
        self.remote_reads: int = 0
        self.push_result_extra: dict[str, Any] = {}
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
        return {
            "remoteSha": "abc",
            "remotePreviouslyExisted": True,
            "snapshot": state,
            **dict(self.push_result_extra),
        }

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


class _TaskLifecycle:
    def __init__(self) -> None:
        self.started: list[dict[str, Any]] = []
        self.finished: list[dict[str, Any]] = []
        self.failed: list[dict[str, Any]] = []

    def start_run(self, **kwargs: Any) -> dict[str, Any]:
        self.started.append(dict(kwargs))
        return dict(kwargs)

    def finish_run(self, run_id: str, task_type: str, **kwargs: Any) -> dict[str, Any]:
        row = {"runId": run_id, "taskType": task_type, **dict(kwargs)}
        self.finished.append(row)
        return row

    def fail_run(self, run_id: str, task_type: str, **kwargs: Any) -> dict[str, Any]:
        row = {"runId": run_id, "taskType": task_type, **dict(kwargs)}
        self.failed.append(row)
        return row


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
        lifecycle = _TaskLifecycle()
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
            ops_state_lock=ops_lock,
            get_security_defaults=get_security_defaults,
            task_lifecycle=lifecycle,
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
        source_sync.push_result_extra = {
            "sizeBytes": 6_000_000,
            "maxSnapshotSizeBytes": 100_000_000,
            "sizeWarning": True,
            "snapshotFormat": "sharded-v3",
            "shardCount": 4,
            "changedShardCount": 2,
            "shardsPushedBytes": 40_000,
            "manifestSizeBytes": 900,
            "shardCapBytes": 10 * 1024 * 1024,
            "shardHashes": {"baluffo/source-sync/shards/active/a/hash.json.gz": "hash"},
        }
        history = _RunHistory()
        lifecycle = _TaskLifecycle()
        ops_lock = threading.RLock()
        logs: list[tuple[str, str, dict[str, Any]]] = []

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
            bridge_log=lambda level, message, **fields: logs.append((level, message, fields)),
            load_state=load_state,
            persist_state=persist_state,
            summarize_state=summarize_state,
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
        assert timing["sizeBytes"] == 6_000_000
        assert timing["maxSnapshotSizeBytes"] == 100_000_000
        assert timing["sizeWarning"] is True
        assert timing["snapshotFormat"] == "sharded-v3"
        assert timing["shardCount"] == 4
        assert timing["changedShardCount"] == 2
        assert timing["shardsPushedBytes"] == 40_000
        assert timing["manifestSizeBytes"] == 900
        assert timing["shardCapBytes"] == 10 * 1024 * 1024
        assert timing["shardHashes"] == {"baluffo/source-sync/shards/active/a/hash.json.gz": "hash"}
        assert timing["stageTotalsMs"]["loadLocalRegistry"] >= 0
        assert timing["stageTotalsMs"]["pushRemote"] >= 0
        assert timing["stageTotalsMs"]["summarizeSnapshot"] >= 0
        assert any(row["stage"] == "pushRemote" for row in timing["stageTop"])
        assert result["sizeBytes"] == 6_000_000
        assert result["maxSnapshotSizeBytes"] == 100_000_000
        assert result["sizeWarning"] is True
        assert result["snapshotFormat"] == "sharded-v3"
        assert result["shardCount"] == 4
        assert logs[-1] == (
            "warn",
            "sync_push_snapshot_size_warning",
            {"sizeBytes": 6_000_000, "maxSnapshotSizeBytes": 100_000_000},
        )

        status = svc.get_sync_status_payload()
        assert status["timing"]["action"] == "push"
        assert status["timing"]["sizeBytes"] == 6_000_000
        assert status["timing"]["shardCount"] == 4
        assert status["timingHistory"][-1]["action"] == "push"
        assert status["timingHistory"][-1]["maxSnapshotSizeBytes"] == 100_000_000
        assert status["timingHistory"][-1]["snapshotFormat"] == "sharded-v3"


def test_sync_service_push_noop_returns_size_fields() -> None:
    with workspace_tmpdir("sync-service") as data_dir:
        with SYNC_STATE_LOCK:
            ACTIVE_SYNC_RUNS.clear()
            ACTIVE_SYNC_THREADS.clear()
        source_sync = _FakeSourceSync()
        source_sync.push_result_extra = {
            "pushed": False,
            "sizeBytes": 1234,
            "maxSnapshotSizeBytes": 100_000_000,
            "sizeWarning": False,
        }

        svc = SyncService(
            data_dir=data_dir,
            source_sync=source_sync,
            bridge_log=lambda _level, _message, **_fields: None,
            load_state=lambda: {"active": [], "pending": [], "rejected": []},
            persist_state=lambda state: state,
            summarize_state=lambda state: {
                "activeCount": len(state["active"]),
                "pendingCount": len(state["pending"]),
                "rejectedCount": len(state["rejected"]),
            },
            ops_state_lock=threading.RLock(),
            get_security_defaults=lambda: {"github_app_enabled_default": True},
        )

        svc.update_saved_sync_settings({"enabled": True})
        result = svc.sync_push_sources()

        assert result["pushed"] is False
        assert result["sizeBytes"] == 1234
        assert result["maxSnapshotSizeBytes"] == 100_000_000
        assert result["sizeWarning"] is False
        assert result["timing"]["noOp"] is True
        assert result["timing"]["sizeBytes"] == 1234


def test_sync_service_start_task_runs_and_finishes() -> None:
    with workspace_tmpdir("sync-service") as data_dir:
        with SYNC_STATE_LOCK:
            ACTIVE_SYNC_RUNS.clear()
            ACTIVE_SYNC_THREADS.clear()
        source_sync = _FakeSourceSync()
        history = _RunHistory()
        lifecycle = _TaskLifecycle()
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
            ops_state_lock=ops_lock,
            get_security_defaults=get_security_defaults,
            task_lifecycle=lifecycle,
        )
        svc.update_saved_sync_settings({"enabled": True})

        started = svc.start_sync_task("pull", reason="test", automatic=False)
        assert bool(started.get("started")) is True
        svc.wait_for_sync_tasks(timeout_s=2.0)
        assert source_sync.pull_calls >= 1
        assert history.rows == []
        assert lifecycle.finished
        timing = lifecycle.finished[-1]["summary"]["timing"]
        assert timing["action"] == "pull"
        assert "stageTotalsMs" in timing
        assert "stageTop" in timing


def test_sync_service_push_task_summary_includes_shard_fields() -> None:
    with workspace_tmpdir("sync-service") as data_dir:
        with SYNC_STATE_LOCK:
            ACTIVE_SYNC_RUNS.clear()
            ACTIVE_SYNC_THREADS.clear()
        source_sync = _FakeSourceSync()
        source_sync.push_result_extra = {
            "snapshotFormat": "sharded-v3",
            "shardCount": 2,
            "changedShardCount": 1,
            "shardsPushedBytes": 2048,
            "manifestSizeBytes": 512,
            "shardCapBytes": 10 * 1024 * 1024,
            "shardHashes": {"shard-path": "sha"},
        }
        lifecycle = _TaskLifecycle()

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
            ops_state_lock=threading.RLock(),
            get_security_defaults=lambda: {"github_app_enabled_default": True},
            task_lifecycle=lifecycle,
        )
        svc.update_saved_sync_settings({"enabled": True})

        started = svc.start_sync_task("push", reason="test", automatic=False)
        assert bool(started.get("started")) is True
        svc.wait_for_sync_tasks(timeout_s=2.0)
        summary = lifecycle.finished[-1]["summary"]
        assert summary["snapshotFormat"] == "sharded-v3"
        assert summary["shardCount"] == 2
        assert summary["changedShardCount"] == 1
        assert summary["shardsPushedBytes"] == 2048
        assert summary["manifestSizeBytes"] == 512
        assert summary["shardCapBytes"] == 10 * 1024 * 1024
        assert summary["shardHashes"] == {"shard-path": "sha"}


def test_sync_service_shadow_writes_sync_events_and_runs() -> None:
    with workspace_tmpdir("sync-service-storage") as data_dir:
        with SYNC_STATE_LOCK:
            ACTIVE_SYNC_RUNS.clear()
            ACTIVE_SYNC_THREADS.clear()
        source_sync = _FakeSourceSync()
        source_sync.push_result_extra = {
            "snapshotFormat": "sharded-v3",
            "shardCount": 2,
            "changedShardCount": 1,
            "shardsPushedBytes": 2048,
            "manifestSizeBytes": 512,
            "shardCapBytes": 10 * 1024 * 1024,
            "shardHashes": {"shard-path": "sha"},
        }

        with BaluffoStore(data_dir) as store:
            store.set_authority_mode("taskEvents", "shadow", reason="test-shadow")
            store.set_authority_mode("syncRuns", "shadow", reason="test-shadow")
            runtime = TaskRuntimeStore(store)
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
                ops_state_lock=threading.RLock(),
                get_security_defaults=lambda: {"github_app_enabled_default": True},
                task_runtime_store=lambda: runtime,
            )
            svc.update_saved_sync_settings({"enabled": True})

            started = svc.start_sync_task("push", reason="test", automatic=False)
            assert bool(started.get("started")) is True
            run_id = str(started.get("runId") or "")
            svc.wait_for_sync_tasks(timeout_s=2.0)

            events = runtime.task_events(run_id=run_id, task_type="sync")
            sync_runs = runtime.sync_runs()

        assert source_sync.push_calls == 1
        assert events[0]["message"] == "Starting sync push."
        assert events[-1]["message"] == "Sync push finished with status ok."
        assert sync_runs[-1]["runId"] == run_id
        assert sync_runs[-1]["status"] == "ok"
        assert sync_runs[-1]["summary"]["snapshotFormat"] == "sharded-v3"
        assert sync_runs[-1]["summary"]["shardHashes"] == {"shard-path": "sha"}
