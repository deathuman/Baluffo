from __future__ import annotations

import json
from pathlib import Path

from src.jobs.common.contracts_task_state import normalize_task_state_payload
from src.jobs.pipeline_bootstrap import build_pipeline_paths
from src.jobs.pipeline_runtime_writers import FetchPrepProgressWriter
from src.pipeline_io import write_hot_text_if_changed


def test_fetch_prep_progress_writer_emits_compact_task_state_before_source_rows(
    tmp_path: Path,
) -> None:
    paths = build_pipeline_paths(tmp_path)
    writer = FetchPrepProgressWriter(
        run_id="fetch_prep_1",
        started_at="2026-07-02T10:00:00+00:00",
        report_path=str(paths.report_path),
        task_state_path=paths.task_state_path,
        active_snapshot_path=paths.active_task_snapshot_path,
        normalize_task_state_payload=normalize_task_state_payload,
        write_text_if_changed=write_hot_text_if_changed,
    )

    writer.emit(
        "loading_state",
        "Loading fetch state",
        counts={"sourceStateRows": 12},
        force=True,
    )

    raw_task_state = paths.task_state_path.read_text(encoding="utf-8")
    payload = json.loads(raw_task_state)
    progress = payload["taskProgress"]
    assert payload["runId"] == "fetch_prep_1"
    assert payload["active"] is True
    assert progress["phaseKey"] == "loading_state"
    assert progress["phaseLabel"] == "Loading fetch state"
    assert progress["counts"]["sourceStateRows"] == 12
    assert payload["workItems"] == []
    assert payload["summary"] == {"queued": 0, "running": 0, "ok": 0, "error": 0, "excluded": 0}
    assert len(raw_task_state.encode("utf-8")) < 4096

    snapshot = json.loads(paths.active_task_snapshot_path.read_text(encoding="utf-8"))
    assert snapshot["count"] == 1
    assert snapshot["tasks"][0]["taskProgress"]["phaseKey"] == "loading_state"
    summary_sidecar = json.loads(
        paths.report_path.with_name("jobs-fetch-report-summary.json").read_text(encoding="utf-8")
    )
    assert summary_sidecar["taskProgress"]["phaseKey"] == "loading_state"
    assert "sources" not in summary_sidecar


def test_fetch_prep_progress_writer_bounds_same_phase_writes(tmp_path: Path) -> None:
    paths = build_pipeline_paths(tmp_path)
    writes: list[tuple[Path, str]] = []

    def record_write(path: Path, text: str) -> bool:
        writes.append((path, text))
        return True

    writer = FetchPrepProgressWriter(
        run_id="fetch_prep_1",
        started_at="2026-07-02T10:00:00+00:00",
        report_path=str(paths.report_path),
        task_state_path=paths.task_state_path,
        active_snapshot_path=None,
        normalize_task_state_payload=normalize_task_state_payload,
        write_text_if_changed=record_write,
        min_interval_s=60.0,
    )

    writer.emit("seeding_existing_output", "Seeding existing output", force=True)
    writer.emit("seeding_existing_output", "Seeding existing output", counts={"rows": 10})
    writer.emit("selecting_sources", "Selecting sources", counts={"selectedSourceCount": 3})

    assert len(writes) == 4
    assert [path.name for path, _text in writes] == [
        "jobs-fetch-tasks.json",
        "jobs-fetch-report-summary.json",
        "jobs-fetch-tasks.json",
        "jobs-fetch-report-summary.json",
    ]
    first_payload = json.loads(writes[0][1])
    second_payload = json.loads(writes[2][1])
    assert first_payload["taskProgress"]["phaseKey"] == "seeding_existing_output"
    assert second_payload["taskProgress"]["phaseKey"] == "selecting_sources"
    timing = writer.timing_payload()
    assert timing["phaseOrder"] == ["seeding_existing_output", "selecting_sources"]
    assert timing["counts"]["rows"] == 10
    assert "phaseTimingsMs" in timing
