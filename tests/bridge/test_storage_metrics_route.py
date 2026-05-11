from __future__ import annotations

from pathlib import Path

from src.bridge.routes.get_routes import handle_get
from src.shared.timing_counters import clear_counters, record_duration
from src.storage_metrics import record_json_write, reset_storage_metrics
from tests.helpers.bridge_api import (
    FakeDesktopLocalDataStore,
    FakeHandler,
    make_stub_bridge_api,
)


def test_ops_storage_metrics_route_returns_snapshot(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    reset_storage_metrics(data_dir=tmp_path, remove_file=True)
    clear_counters()
    record_json_write(
        path=tmp_path / "jobs-fetch-report.json",
        target=tmp_path / "jobs-fetch-report.json",
        storage_kind="json",
        serialization_duration_ms=1,
        atomic_replace_duration_ms=2,
        compressed_size_bytes=10,
        uncompressed_size_bytes=12,
        replaced=True,
        data_dir=tmp_path,
    )
    record_duration("bridge_request_get_ops_task_state", 7)

    try:
        handler = FakeHandler()
        result = handle_get(handler, api=api, path="/ops/storage-metrics", query={})

        assert result is True
        assert handler.sent[-1]["status"] == 200
        payload = handler.sent[-1]["payload"]
        assert payload["ok"] is True
        assert payload["storageMetrics"]["writes"]["writeCount"] == 1
        assert payload["routeCounters"]["bridge_request_get_ops_task_state"]["count"] == 1
    finally:
        clear_counters()
