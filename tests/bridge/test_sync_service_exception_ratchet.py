from __future__ import annotations

import threading
from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast
from unittest import mock

import pytest

from src.bridge.sync_service import SyncService
from tests.helpers.temp_paths import workspace_tmpdir

_EMPTY_REGISTRY_STATE: dict[str, list[dict[str, Any]]] = {
    "active": [],
    "pending": [],
    "rejected": [],
}


class _ReadySourceSync:
    def config_status(self, config: Any) -> dict[str, Any]:
        return {"enabled": True, "ready": True}

    def resolve_sync_config(
        self, settings: dict[str, Any] | None = None, env: Mapping[str, str] | None = None
    ) -> Any:
        return {"settings": dict(settings or {}), "env": bool(env)}

    def read_remote_snapshot(self, config: Any) -> dict[str, Any]:
        return {"exists": True, "sha": "abc"}

    def pull_and_merge_sources(
        self,
        config: Any,
        local_state: dict[str, Any],
        *,
        progress_callback: Any | None = None,
        known_remote_sha: str = "",
    ) -> dict[str, Any]:
        return {"changed": False, "remoteFound": True, "mergedState": local_state}

    def push_sources_snapshot(self, config: Any, state: dict[str, Any]) -> dict[str, Any]:
        return {"remoteSha": "abc", "remotePreviouslyExisted": True}

    def rate_limit_payload(self) -> dict[str, Any]:
        return {"remaining": 42, "limit": 100}


def _service(data_dir: Path, logs: list[tuple[str, str, dict[str, Any]]]) -> SyncService:
    return SyncService(
        data_dir=data_dir,
        source_sync=_ReadySourceSync(),
        bridge_log=lambda level, message, **fields: logs.append((level, message, fields)),
        load_state=lambda: _EMPTY_REGISTRY_STATE,
        persist_state=lambda state: state,
        summarize_state=lambda state: {"activeCount": 0, "pendingCount": 0, "rejectedCount": 0},
        ops_state_lock=threading.RLock(),
        get_security_defaults=lambda: {},
    )


def test_startup_sync_pull_records_expected_operational_failure() -> None:
    logs: list[tuple[str, str, dict[str, Any]]] = []
    with workspace_tmpdir("sync-service-exception-ratchet") as data_dir:
        service = _service(data_dir, logs)
        cast(Any, service).sync_pull_sources = mock.Mock(
            side_effect=ValueError("bad remote snapshot")
        )

        service.startup_sync_pull()

        assert ("warn", "sync_startup_pull_failed", {"error": "bad remote snapshot"}) in logs
        assert service.get_sync_status_payload()["runtime"]["lastError"] == "bad remote snapshot"


def test_startup_sync_pull_does_not_hide_unexpected_bug() -> None:
    logs: list[tuple[str, str, dict[str, Any]]] = []
    with workspace_tmpdir("sync-service-exception-ratchet") as data_dir:
        service = _service(data_dir, logs)
        cast(Any, service).sync_pull_sources = mock.Mock(
            side_effect=AssertionError("unexpected pull bug")
        )

        with pytest.raises(AssertionError, match="unexpected pull bug"):
            service.startup_sync_pull()


def test_schedule_startup_sync_pull_records_expected_operational_failure() -> None:
    logs: list[tuple[str, str, dict[str, Any]]] = []
    with workspace_tmpdir("sync-service-exception-ratchet") as data_dir:
        service = _service(data_dir, logs)
        cast(Any, service).start_sync_task = mock.Mock(
            side_effect=RuntimeError("thread start failed")
        )

        result = service.schedule_startup_sync_pull()

        assert result == {
            "started": False,
            "task": "source_sync",
            "action": "pull",
            "reason": "startup",
            "error": "thread start failed",
        }
        assert ("warn", "sync_startup_schedule_failed", {"error": "thread start failed"}) in logs


def test_schedule_startup_sync_pull_does_not_hide_unexpected_bug() -> None:
    logs: list[tuple[str, str, dict[str, Any]]] = []
    with workspace_tmpdir("sync-service-exception-ratchet") as data_dir:
        service = _service(data_dir, logs)
        cast(Any, service).start_sync_task = mock.Mock(
            side_effect=AssertionError("unexpected schedule bug")
        )

        with pytest.raises(AssertionError, match="unexpected schedule bug"):
            service.schedule_startup_sync_pull()
