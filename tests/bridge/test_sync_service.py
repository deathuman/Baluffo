from __future__ import annotations

import threading
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict, List, Tuple

from src.bridge.sync_service import SyncService
from src.bridge.sync_state import ACTIVE_SYNC_RUNS, ACTIVE_SYNC_THREADS, SYNC_STATE_LOCK


class _FakeSourceSync:
    def __init__(self) -> None:
        self.pull_calls: int = 0
        self.push_calls: int = 0
        self.remote_reads: int = 0
        self._enabled = True
        self._ready = True

    def config_status(self, config: Any) -> Dict[str, Any]:
        return {"enabled": bool(self._enabled), "ready": bool(self._ready)}

    def resolve_sync_config(self, settings: Dict[str, Any] | None = None, env: Dict[str, str] | None = None) -> Any:
        # Return a simple token object; SyncService treats it as opaque.
        return {"settings": dict(settings or {}), "env": bool(env)}

    def read_remote_snapshot(self, config: Any) -> Dict[str, Any]:
        self.remote_reads += 1
        return {"exists": True, "sha": "abc"}

    def pull_and_merge_sources(self, config: Any, local_state: Dict[str, Any]) -> Dict[str, Any]:
        self.pull_calls += 1
        return {"changed": True, "remoteFound": True, "remoteSha": "abc", "mergedState": {"active": [], "pending": [], "rejected": []}}

    def push_sources_snapshot(self, config: Any, state: Dict[str, Any]) -> Dict[str, Any]:
        self.push_calls += 1
        return {"remoteSha": "abc", "remotePreviouslyExisted": True, "snapshot": state}


class _RunHistory:
    def __init__(self) -> None:
        self.rows: List[Dict[str, Any]] = []

    def append(self, row: Dict[str, Any]) -> Dict[str, Any]:
        self.rows.append(dict(row))
        return self.rows[-1]

    def upsert(self, entry: Dict[str, Any], *, dedupe_fields: Tuple[str, ...]) -> Dict[str, Any]:
        self.rows.append(dict(entry))
        return self.rows[-1]

    def load(self) -> List[Dict[str, Any]]:
        return list(self.rows)

    def prune_started_rows_for_type(self, entry_type: str, finished_at: str) -> None:
        return


def test_sync_service_pull_delegates_and_persists() -> None:
    with TemporaryDirectory() as tmp:
        data_dir = Path(tmp)
        with SYNC_STATE_LOCK:
            ACTIVE_SYNC_RUNS.clear()
            ACTIVE_SYNC_THREADS.clear()
        source_sync = _FakeSourceSync()
        history = _RunHistory()
        ops_lock = threading.RLock()

        persisted: Dict[str, Any] = {"active": [{"adapter": "static", "listing_url": "https://a.com/jobs"}], "pending": [], "rejected": []}

        def load_state() -> Dict[str, List[Dict[str, Any]]]:
            return {"active": list(persisted["active"]), "pending": list(persisted["pending"]), "rejected": list(persisted["rejected"])}

        def persist_state(state: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
            persisted["active"] = list(state.get("active") or [])
            persisted["pending"] = list(state.get("pending") or [])
            persisted["rejected"] = list(state.get("rejected") or [])
            return load_state()

        def summarize_state(state: Dict[str, List[Dict[str, Any]]]) -> Dict[str, int]:
            return {"activeCount": len(state["active"]), "pendingCount": len(state["pending"]), "rejectedCount": len(state["rejected"])}

        def bridge_log(_level: str, _message: str, **_fields: Any) -> None:
            return

        def get_security_defaults() -> Dict[str, Any]:
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


def test_sync_service_start_task_runs_and_finishes() -> None:
    with TemporaryDirectory() as tmp:
        data_dir = Path(tmp)
        with SYNC_STATE_LOCK:
            ACTIVE_SYNC_RUNS.clear()
            ACTIVE_SYNC_THREADS.clear()
        source_sync = _FakeSourceSync()
        history = _RunHistory()
        ops_lock = threading.RLock()

        def load_state() -> Dict[str, List[Dict[str, Any]]]:
            return {"active": [], "pending": [], "rejected": []}

        def persist_state(state: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
            return state

        def summarize_state(state: Dict[str, List[Dict[str, Any]]]) -> Dict[str, int]:
            return {"activeCount": 0, "pendingCount": 0, "rejectedCount": 0}

        def bridge_log(_level: str, _message: str, **_fields: Any) -> None:
            return

        def get_security_defaults() -> Dict[str, Any]:
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

