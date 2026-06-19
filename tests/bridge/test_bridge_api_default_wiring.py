from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from src.bridge.api import BridgeApi


def _bridge_api(tmp_path: Path) -> BridgeApi:
    return BridgeApi(
        runtime_config=None,
        DISCOVERY_REPORT_PATH=tmp_path / "discovery-report.json",
        JOBS_FETCH_REPORT_PATH=tmp_path / "jobs-fetch-report.json",
        APPROVAL_STATE_PATH=tmp_path / "approval-state.json",
        DISCOVERY_LOG_PATH=tmp_path / "discovery.log",
        FETCHER_LOG_PATH=tmp_path / "fetcher.log",
        STARTUP_METRICS_PATH=tmp_path / "startup-metrics.json",
    )


def test_field_is_default_tolerates_missing_bridge_api_field(tmp_path: Path) -> None:
    api = _bridge_api(tmp_path)

    assert api._field_is_default("missing_field") is False


def test_field_is_default_propagates_unexpected_attribute_failure(tmp_path: Path) -> None:
    class BrokenBridgeApi(BridgeApi):
        def __getattribute__(self, name: str) -> object:
            if name == "get_sync_status_payload":
                raise RuntimeError("descriptor failure")
            return super().__getattribute__(name)

    api = BrokenBridgeApi(
        runtime_config=None,
        DISCOVERY_REPORT_PATH=tmp_path / "discovery-report.json",
        JOBS_FETCH_REPORT_PATH=tmp_path / "jobs-fetch-report.json",
        APPROVAL_STATE_PATH=tmp_path / "approval-state.json",
        DISCOVERY_LOG_PATH=tmp_path / "discovery.log",
        FETCHER_LOG_PATH=tmp_path / "fetcher.log",
        STARTUP_METRICS_PATH=tmp_path / "startup-metrics.json",
    )

    with pytest.raises(RuntimeError, match="descriptor failure"):
        api._field_is_default("get_sync_status_payload")


def test_mark_desktop_session_activity_tolerates_expected_timestamp_failure(
    tmp_path: Path,
) -> None:
    calls: list[str] = []
    api = BridgeApi(
        runtime_config=SimpleNamespace(desktop_mode=True, owner_mode=""),
        DISCOVERY_REPORT_PATH=tmp_path / "discovery-report.json",
        JOBS_FETCH_REPORT_PATH=tmp_path / "jobs-fetch-report.json",
        APPROVAL_STATE_PATH=tmp_path / "approval-state.json",
        DISCOVERY_LOG_PATH=tmp_path / "discovery.log",
        FETCHER_LOG_PATH=tmp_path / "fetcher.log",
        STARTUP_METRICS_PATH=tmp_path / "startup-metrics.json",
        DESKTOP_SESSION_ACTIVITY_AT="2026-03-01T00:00:01+00:00",
        now_iso=lambda: (_ for _ in ()).throw(ValueError("bad timestamp")),
        _mark_desktop_session_activity=lambda path: calls.append(path),
    )

    api.mark_desktop_session_activity("/ops/task-state")

    assert calls == ["/ops/task-state"]
    assert api.DESKTOP_SESSION_ACTIVITY_AT == "2026-03-01T00:00:01+00:00"


def test_mark_desktop_session_activity_propagates_unexpected_timestamp_failure(
    tmp_path: Path,
) -> None:
    calls: list[str] = []
    api = BridgeApi(
        runtime_config=SimpleNamespace(desktop_mode=True, owner_mode=""),
        DISCOVERY_REPORT_PATH=tmp_path / "discovery-report.json",
        JOBS_FETCH_REPORT_PATH=tmp_path / "jobs-fetch-report.json",
        APPROVAL_STATE_PATH=tmp_path / "approval-state.json",
        DISCOVERY_LOG_PATH=tmp_path / "discovery.log",
        FETCHER_LOG_PATH=tmp_path / "fetcher.log",
        STARTUP_METRICS_PATH=tmp_path / "startup-metrics.json",
        DESKTOP_SESSION_ACTIVITY_AT="2026-03-01T00:00:01+00:00",
        now_iso=lambda: (_ for _ in ()).throw(RuntimeError("clock bug")),
        _mark_desktop_session_activity=lambda path: calls.append(path),
    )

    with pytest.raises(RuntimeError, match="clock bug"):
        api.mark_desktop_session_activity("/ops/task-state")

    assert calls == ["/ops/task-state"]
    assert api.DESKTOP_SESSION_ACTIVITY_AT == "2026-03-01T00:00:01+00:00"
