from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from typing import Any

from src.bridge import registry_conflicts_refresh as refresh_module
from src.bridge.registry_conflicts_summary import (
    _registry_conflicts_full_cache_path,
    _registry_conflicts_summary_cache_path,
    build_registry_conflicts_summary_cache_key,
)
from tests.bridge.test_registry_conflicts_route_capability import (
    MinimalRegistryConflictsRouteApi,
)
from tests.helpers.bridge_api import FakeDesktopLocalDataStore, make_stub_bridge_api


def _wait_for(predicate: Any, timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return False


def _source_state_path(api: MinimalRegistryConflictsRouteApi) -> Path:
    return api.JOBS_FETCH_REPORT_PATH.with_name("jobs-source-state.json")


def test_refresh_single_flights_and_writes_both_caches(tmp_path: Path, monkeypatch: Any) -> None:
    api = MinimalRegistryConflictsRouteApi(tmp_path)
    source_state_path = _source_state_path(api)
    started = threading.Event()
    release = threading.Event()
    calls: list[str] = []

    def fake_load(**kwargs: Any) -> dict[str, Any]:
        calls.append("load")
        started.set()
        assert release.wait(5.0)
        return {
            "summary": {"conflictCount": 1},
            "conflicts": [],
            "triage": {},
            "review": {},
            "automation": {},
        }

    monkeypatch.setattr(refresh_module, "load_registry_conflicts_payload", fake_load)

    assert refresh_module.schedule_registry_conflicts_refresh(
        api=api,
        source_state_path=source_state_path,
        cache_key="key-1",
        adjudication_payload={},
    )
    assert started.wait(5.0)
    assert refresh_module.schedule_registry_conflicts_refresh(
        api=api,
        source_state_path=source_state_path,
        cache_key="key-1",
        adjudication_payload={},
    )
    assert refresh_module.is_registry_conflicts_refresh_in_flight("key-1")
    release.set()
    assert _wait_for(lambda: not refresh_module.is_registry_conflicts_refresh_in_flight("key-1"))
    assert calls == ["load"]
    assert _registry_conflicts_full_cache_path(source_state_path).exists()
    assert _registry_conflicts_summary_cache_path(source_state_path).exists()


def test_refresh_clears_in_flight_after_failure(tmp_path: Path, monkeypatch: Any) -> None:
    api = MinimalRegistryConflictsRouteApi(tmp_path)
    source_state_path = _source_state_path(api)

    def fake_load(**kwargs: Any) -> dict[str, Any]:
        raise ValueError("boom")

    monkeypatch.setattr(refresh_module, "load_registry_conflicts_payload", fake_load)

    assert refresh_module.schedule_registry_conflicts_refresh(
        api=api,
        source_state_path=source_state_path,
        cache_key="key-2",
        adjudication_payload={},
    )
    assert _wait_for(lambda: not refresh_module.is_registry_conflicts_refresh_in_flight("key-2"))
    assert not _registry_conflicts_full_cache_path(source_state_path).exists()


def test_refresh_builds_real_payload_and_writes_ready_summary(tmp_path: Path) -> None:
    api = make_stub_bridge_api(tmp_path, FakeDesktopLocalDataStore())
    source_state_path = Path(api.JOBS_FETCH_REPORT_PATH).with_name("jobs-source-state.json")
    source_state_path.write_text("{}", encoding="utf-8")
    api.load_state = lambda: {
        "active": [
            {
                "id": "winner-1",
                "name": "Winner Source",
                "studio": "Studio",
                "adapter": "greenhouse",
                "registryState": "active",
                "jobsFound": 8,
            }
        ],
        "pending": [
            {
                "id": "loser-1",
                "name": "Loser Source",
                "studio": "Studio",
                "adapter": "static",
                "registryState": "pending",
                "jobsFound": 1,
            }
        ],
        "rejected": [],
    }
    cache_key = build_registry_conflicts_summary_cache_key(
        registry_summary=api.get_registry_summary_payload(),
        source_state_path=source_state_path,
        adjudication_payload={},
    )

    assert refresh_module.schedule_registry_conflicts_refresh(
        api=api,
        source_state_path=source_state_path,
        cache_key=cache_key,
        adjudication_payload={},
    )
    assert _wait_for(lambda: not refresh_module.is_registry_conflicts_refresh_in_flight(cache_key))

    full = json.loads(
        _registry_conflicts_full_cache_path(source_state_path).read_text(encoding="utf-8")
    )
    assert full["payload"]["summary"]["conflictCount"] == 1
    summary = json.loads(
        _registry_conflicts_summary_cache_path(source_state_path).read_text(encoding="utf-8")
    )
    assert summary["payload"]["summaryStatus"] == "ready"
    assert summary["payload"]["conflictSampleCount"] == 1
