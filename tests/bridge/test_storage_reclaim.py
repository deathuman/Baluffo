"""GET /ops/storage-reclaim: verdict, status, journal, and the 60 s cache.

The route is what makes the verdict reachable, so these cases pin the properties
the panel depends on: a verdict survives a restart's journal, the cache does not
outlive its window or override an explicit refresh, and an unreadable store
refuses through blockers instead of failing the request.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from src.bridge import storage_reclaim
from src.storage import BaluffoStore, BaluffoStoreError, JobRuntimeStore
from src.storage.baluffo_store import DEFAULT_DB_NAME
from src.storage.runtime_retention import preview_runtime_retention
from tests.helpers.temp_paths import workspace_tmpdir


def _row(index: int) -> dict[str, object]:
    return {
        "id": index,
        "title": f"Job {index}",
        "company": "Studio",
        "city": "Amsterdam",
        "country": "Netherlands",
        "jobLink": f"https://example.test/jobs/{index}",
        "source": "Example",
        "sourceJobId": f"job-{index}",
        "status": "active",
        "firstSeenAt": "2026-11-15T10:00:00Z",
        "lastSeenAt": "2026-11-15T10:00:00Z",
        "sourceBundle": [],
    }


def _seed(data_dir: Path, *, generations: int = 2) -> None:
    with BaluffoStore(data_dir) as store:
        runtime = JobRuntimeStore(store, now_iso=lambda: "2026-11-15T10:00:00Z")
        for index in range(generations):
            runtime.replace_feed(run_id=f"fetch_{index}", rows=[_row(index)])


def _write_journal(data_dir: Path, payload: dict[str, Any]) -> None:
    path = storage_reclaim.reclaim_journal_path(data_dir)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _finished_hours_ago(hours: float) -> str:
    return (datetime.now(UTC) - timedelta(hours=hours)).isoformat()


def test_verdict_is_reachable_for_a_real_store() -> None:
    with workspace_tmpdir("reclaim-route-verdict") as data_dir:
        _seed(data_dir)

        payload = storage_reclaim.get_storage_reclaim_payload(data_dir)

    reclaim = payload["storageReclaim"]
    assert payload["ok"] is True
    assert reclaim["readOnly"] is True
    assert reclaim["scope"] == "modal"
    assert reclaim["status"] == "idle"
    assert reclaim["journal"] is None
    assert reclaim["journalPresent"] is False
    assert reclaim["verdictCached"] is False
    assert reclaim["verdict"]["verdict"] == "ok"
    assert reclaim["verdict"]["currentGenerationEstimatedBytes"] > 0
    assert reclaim["verdict"]["historicalGenerationCount"] == 1


def _counting_preview(monkeypatch: pytest.MonkeyPatch, calls: list[int]) -> None:
    def counting_preview(store: Any) -> dict[str, Any]:
        calls.append(1)
        return preview_runtime_retention(store)

    monkeypatch.setattr(storage_reclaim, "preview_runtime_retention", counting_preview)


def test_verdict_is_cached_and_refresh_bypasses_the_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("reclaim-route-cache") as data_dir:
        _seed(data_dir)
        calls: list[int] = []
        _counting_preview(monkeypatch, calls)

        first = storage_reclaim.get_storage_reclaim_payload(data_dir)["storageReclaim"]
        second = storage_reclaim.get_storage_reclaim_payload(data_dir)["storageReclaim"]

        assert len(calls) == 1
        assert first["verdictCached"] is False
        assert second["verdictCached"] is True
        assert second["verdictAgeMs"] >= 0
        assert second["verdict"] == first["verdict"]

        refreshed = storage_reclaim.get_storage_reclaim_payload(data_dir, refresh=True)[
            "storageReclaim"
        ]
        assert len(calls) == 2
        assert refreshed["verdictCached"] is False
        assert refreshed["verdictAgeMs"] == 0


def test_verdict_cache_expires_at_its_ttl(monkeypatch: pytest.MonkeyPatch) -> None:
    with workspace_tmpdir("reclaim-route-ttl") as data_dir:
        _seed(data_dir)
        calls: list[int] = []
        _counting_preview(monkeypatch, calls)
        monkeypatch.setattr(storage_reclaim, "RECLAIM_VERDICT_TTL_S", 0.0)

        storage_reclaim.get_storage_reclaim_payload(data_dir)
        storage_reclaim.get_storage_reclaim_payload(data_dir)

        assert len(calls) == 2


def test_journal_status_drives_the_live_status() -> None:
    with workspace_tmpdir("reclaim-route-journal") as data_dir:
        _seed(data_dir)
        _write_journal(
            data_dir,
            {
                "status": "complete",
                "phase": "complete",
                "runId": "reclaim-1",
                "mode": "reclaim",
                "finishedAt": _finished_hours_ago(1),
            },
        )

        reclaim = storage_reclaim.get_storage_reclaim_payload(data_dir)["storageReclaim"]

    assert reclaim["status"] == "complete"
    assert reclaim["phase"] == "complete"
    assert reclaim["runId"] == "reclaim-1"
    assert reclaim["journalPresent"] is True
    assert reclaim["journal"]["mode"] == "reclaim"
    # The last non-rehearsal finish starts the 168 h anti-thrash window.
    assert 166.0 <= reclaim["verdict"]["cooldownRemainingHours"] <= 168.0
    assert "cooldown" in reclaim["verdict"]["blockers"]


def test_journaled_running_is_reported_interrupted_after_a_restart() -> None:
    with workspace_tmpdir("reclaim-route-interrupted") as data_dir:
        _seed(data_dir)
        _write_journal(
            data_dir,
            {"status": "running", "phase": "copying", "runId": "reclaim-2", "mode": "reclaim"},
        )

        reclaim = storage_reclaim.get_storage_reclaim_payload(data_dir)["storageReclaim"]

    # No run survives the restart, so a journaled `running` is not activity.
    assert reclaim["status"] == "interrupted"
    assert reclaim["phase"] == "copying"


def test_a_rehearsal_does_not_start_the_cooldown() -> None:
    with workspace_tmpdir("reclaim-route-rehearsal") as data_dir:
        _seed(data_dir)
        _write_journal(
            data_dir,
            {
                "status": "complete",
                "mode": "rehearsal",
                "finishedAt": _finished_hours_ago(0.5),
            },
        )

        reclaim = storage_reclaim.get_storage_reclaim_payload(data_dir)["storageReclaim"]

    assert reclaim["verdict"]["cooldownRemainingHours"] == 0.0
    assert "cooldown" not in reclaim["verdict"]["blockers"]


def test_an_unreadable_store_refuses_through_blockers(monkeypatch: pytest.MonkeyPatch) -> None:
    def failing_store(*_args: Any, **_kwargs: Any) -> Any:
        raise BaluffoStoreError("store unavailable")

    with workspace_tmpdir("reclaim-route-unavailable") as data_dir:
        monkeypatch.setattr(storage_reclaim, "get_storage_store", failing_store)
        reclaim = storage_reclaim.get_storage_reclaim_payload(data_dir)["storageReclaim"]

    assert reclaim["verdict"]["verdict"] == "ok"
    assert reclaim["verdict"]["ratio"] == 0.0
    assert "store-unavailable" in reclaim["verdict"]["blockers"]
    assert "sample-unavailable" in reclaim["verdict"]["blockers"]


def test_scope_is_echoed_and_an_unknown_scope_falls_back_to_modal() -> None:
    with workspace_tmpdir("reclaim-route-scope") as data_dir:
        _seed(data_dir, generations=1)

        inline = storage_reclaim.get_storage_reclaim_payload(data_dir, scope="inline")[
            "storageReclaim"
        ]
        unknown = storage_reclaim.get_storage_reclaim_payload(data_dir, scope="browser")[
            "storageReclaim"
        ]

    assert inline["scope"] == "inline"
    assert unknown["scope"] == "modal"


def test_journal_path_is_beside_the_runtime_store(tmp_path: Path) -> None:
    assert storage_reclaim.reclaim_journal_path(tmp_path).name == (
        f"{DEFAULT_DB_NAME}.reclaim.json"
    )
