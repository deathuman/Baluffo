"""SQLite-authority registry summaries are keyed by the registry generation.

A summary read was ~0.4 s on a Pi (state row plus JSON export scan) and ran on
every Ops poll. Registry writes bump `generation`, so that key is exact: a stale
count is impossible, only the re-read is skipped.
"""

from __future__ import annotations

from pathlib import Path
from unittest import mock

from src import source_registry as sr
from src.bridge.registry_service import RegistryPaths, RegistryService
from src.bridge.storage_health import close_storage_stores, get_storage_store


def _sqlite_service(tmp_path: Path) -> RegistryService:
    active_path = tmp_path / "source-registry-active.json"
    sr.save_json_atomic(active_path, [{"id": "a", "name": "Active", "adapter": "static"}])
    sr.save_json_atomic(tmp_path / "source-registry-pending.json", [])
    sr.save_json_atomic(tmp_path / "source-registry-rejected.json", [])
    store = get_storage_store(tmp_path)
    service = RegistryService(
        paths=RegistryPaths(
            active=active_path,
            pending=tmp_path / "source-registry-pending.json",
            rejected=tmp_path / "source-registry-rejected.json",
        ),
        default_active=[],
        normalize_manual_static=lambda row: row,
    )
    service.load_state()
    assert store.get_authority_modes()["sourceRegistry"] == "sqlite"
    return service


def test_sqlite_summary_reuses_payload_until_generation_moves(tmp_path: Path) -> None:
    service = _sqlite_service(tmp_path)
    try:
        with mock.patch.object(
            service,
            "_has_registry_json_exports",
            wraps=service._has_registry_json_exports,
        ) as exports_probe:
            first = service.get_summary_payload()
            second = service.get_summary_payload()

            assert exports_probe.call_count == 1

        assert first["generation"]
        assert second == first

        # Callers get a copy, so mutating a returned payload cannot poison the cache.
        first["pendingCount"] = 999
        third = service.get_summary_payload()
        assert third["pendingCount"] != 999
    finally:
        close_storage_stores()


def test_sqlite_summary_refreshes_after_a_registry_write(tmp_path: Path) -> None:
    service = _sqlite_service(tmp_path)
    try:
        with mock.patch.object(
            service,
            "_has_registry_json_exports",
            wraps=service._has_registry_json_exports,
        ) as exports_probe:
            first = service.get_summary_payload()
            assert exports_probe.call_count == 1

            service.get_summary_payload()
            assert exports_probe.call_count == 1

            service.save_tombstones(
                {"a": {"sourceId": "a", "reason": "manual", "bucket": "active"}}
            )
            refreshed = service.get_summary_payload()
            assert refreshed["generation"] != first["generation"]

            # The refreshed payload is cached again: a follow-up read does not rescan.
            calls_after_refresh = exports_probe.call_count
            service.get_summary_payload()
            assert exports_probe.call_count == calls_after_refresh
    finally:
        close_storage_stores()
