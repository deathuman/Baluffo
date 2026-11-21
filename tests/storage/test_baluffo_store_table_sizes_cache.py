from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from src.storage import BaluffoStore
from src.storage.baluffo_store import DEFAULT_TABLE_SIZE_PARTIAL_CACHE_S
from tests.helpers.temp_paths import workspace_tmpdir


def _snapshot(*, status: str, complete: bool, captured_at: str) -> dict[str, Any]:
    return {
        "schemaVersion": 1,
        "status": status,
        "method": "rowid-sample",
        "capturedAt": captured_at,
        "durationMs": 1,
        "budgetMs": 750,
        "estimated": True,
        "complete": complete,
        "tables": [{"name": "jobs", "bytes": 1, "rowCount": 1}],
        "otherBytes": 0,
        "truncated": False,
    }


def test_table_sizes_keeps_last_complete_snapshot_when_refresh_times_out(monkeypatch) -> None:
    measured = [
        _snapshot(status="ok", complete=True, captured_at="2026-11-20T10:00:00Z"),
        _snapshot(status="partial", complete=False, captured_at="2026-11-20T10:01:00Z"),
        _snapshot(status="ok", complete=True, captured_at="2026-11-20T10:02:00Z"),
    ]
    calls: list[Path] = []

    def fake_measure(path: Path) -> dict[str, Any]:
        calls.append(path)
        return measured[min(len(calls) - 1, len(measured) - 1)]

    monkeypatch.setattr("src.storage.db_table_sizes.measure_table_sizes", fake_measure)

    with workspace_tmpdir("baluffo-store-table-sizes-retain") as data_dir:
        with BaluffoStore(data_dir, defer_maintenance=True) as store:
            first = store.table_sizes()
            assert first["status"] == "ok"
            assert first["capturedAt"] == "2026-11-20T10:00:00Z"

            # Force the complete cache window to expire without waiting 10 minutes.
            store._table_sizes_cache_until_monotonic = 0.0
            retained = store.table_sizes()

            assert retained["status"] == "ok"
            assert retained["complete"] is True
            assert retained["capturedAt"] == "2026-11-20T10:00:00Z"
            assert calls[-1] == store.db_path
            # The retained snapshot is retried on the short partial cadence, not
            # frozen for the full complete-snapshot window.
            assert (
                store._table_sizes_cache_until_monotonic
                <= time.monotonic() + DEFAULT_TABLE_SIZE_PARTIAL_CACHE_S + 0.5
            )

            store._table_sizes_cache_until_monotonic = 0.0
            refreshed = store.table_sizes()

            assert refreshed["status"] == "ok"
            assert refreshed["capturedAt"] == "2026-11-20T10:02:00Z"


def test_table_sizes_returns_partial_when_no_complete_snapshot_exists(monkeypatch) -> None:
    def fake_measure(_path: Path) -> dict[str, Any]:
        return _snapshot(status="partial", complete=False, captured_at="2026-11-20T10:00:00Z")

    monkeypatch.setattr("src.storage.db_table_sizes.measure_table_sizes", fake_measure)

    with workspace_tmpdir("baluffo-store-table-sizes-partial") as data_dir:
        with BaluffoStore(data_dir, defer_maintenance=True) as store:
            result = store.table_sizes()

    assert result["status"] == "partial"
    assert result["complete"] is False


def test_table_sizes_does_not_mask_an_unavailable_refresh_with_a_complete_snapshot(
    monkeypatch,
) -> None:
    measured = [
        _snapshot(status="ok", complete=True, captured_at="2026-11-20T10:00:00Z"),
        _snapshot(status="unavailable", complete=False, captured_at="2026-11-20T10:01:00Z"),
    ]
    calls = 0

    def fake_measure(_path: Path) -> dict[str, Any]:
        nonlocal calls
        snapshot = measured[min(calls, len(measured) - 1)]
        calls += 1
        return snapshot

    monkeypatch.setattr("src.storage.db_table_sizes.measure_table_sizes", fake_measure)

    with workspace_tmpdir("baluffo-store-table-sizes-unavailable") as data_dir:
        with BaluffoStore(data_dir, defer_maintenance=True) as store:
            assert store.table_sizes()["status"] == "ok"
            store._table_sizes_cache_until_monotonic = 0.0
            result = store.table_sizes()

    assert result["status"] == "unavailable"
    assert result["complete"] is False
