from __future__ import annotations

from pathlib import Path

from src.bridge.sync_timing import (
    SyncTimingRecorder,
    append_sync_timing_record,
    load_sync_timing_history,
)


def test_sync_timing_recorder_produces_total_and_stage_durations() -> None:
    ticks = iter([1.0, 1.1, 1.4, 1.5, 2.0, 2.2])
    recorder = SyncTimingRecorder(now=lambda: next(ticks), wall_now=lambda: "2026-05-05T10:00:00Z")

    with recorder.record_stage("loadLocalRegistry"):
        pass
    with recorder.record_stage("pushRemote"):
        pass

    record = recorder.finish({"action": "push", "ok": True})

    assert record["action"] == "push"
    assert record["ok"] is True
    assert record["totalDurationMs"] == 1200
    assert record["stageTotalsMs"] == {
        "loadLocalRegistry": 300,
        "pushRemote": 500,
    }
    assert record["startedAt"] == "2026-05-05T10:00:00Z"
    assert record["finishedAt"] == "2026-05-05T10:00:00Z"


def test_sync_timing_history_retains_last_twenty(tmp_path: Path) -> None:
    path = tmp_path / "sync-timing-history.json"

    for index in range(25):
        append_sync_timing_record(path, {"action": "pull", "index": index})

    history = load_sync_timing_history(path)

    assert len(history) == 20
    assert history[0]["index"] == 5
    assert history[-1]["index"] == 24


def test_sync_timing_history_missing_or_malformed_returns_empty(tmp_path: Path) -> None:
    path = tmp_path / "sync-timing-history.json"

    assert load_sync_timing_history(path) == []
    path.write_text("{bad json", encoding="utf-8")
    assert load_sync_timing_history(path) == []
    path.write_text('{"not":"a list"}', encoding="utf-8")
    assert load_sync_timing_history(path) == []
