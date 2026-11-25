"""Discovery report progress liveness.

Locks in that the discovery stage's report taskProgress ticks during a run
(unlike the fetch report, which is intentionally sparse during source
execution): the probe loop writes the report every 10 completions with a fresh
``probedCandidateCount``, and the pipeline status payload's active child
projects those counts so the jobs caption's source count keeps advancing
(rendered as ``N of M`` by the Jobs page's plain-language caption).
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from src.bridge.pipeline_stall import compute_pipeline_child_quiet_info
from src.source_discovery.reporting_progress import (
    build_discovery_task_progress,
    update_discovery_subtask_progress_report,
)
from tests._pipeline_execution_shared import (
    Any,
    FakeLock,
    Path,
    PipelineRuntime,
    PipelineService,
    make_parse_iso,
)


def _probing_summary(*, probed: int) -> dict[str, Any]:
    return {
        "phaseKey": "probing_candidates",
        "phaseLabel": "Probing 2135 candidate(s)",
        "foundEndpointCount": 2400,
        "probedCandidateCount": probed,
        "lossAccounting": {
            "generated": 2400,
            "dedupSkipped": 100,
            "validationSkipped": 80,
            "lowEvidenceSkipped": 60,
        },
        "suppressedStaticCount": 25,
        "queuedCandidateCount": 200,
    }


def test_discovery_task_progress_ticks_during_probing() -> None:
    first = build_discovery_task_progress(
        summary=_probing_summary(probed=512),
        finished=False,
        updated_at="2026-08-31T12:00:00Z",
    )
    later = build_discovery_task_progress(
        summary=_probing_summary(probed=530),
        finished=False,
        updated_at="2026-08-31T12:01:00Z",
    )
    # probeTotal = generated - dedupSkipped - validationSkipped -
    # lowEvidenceSkipped - suppressedStaticCount
    probe_total = 2400 - 100 - 80 - 60 - 25
    assert first["phaseKey"] == "probing_candidates"
    assert first["mode"] == "determinate"
    assert first["counts"]["probeTotal"] == probe_total
    assert first["counts"]["probedCandidates"] == 512
    assert later["counts"]["probedCandidates"] == 530
    assert later["ratio"] == 530 / probe_total
    assert later["updatedAt"] == "2026-08-31T12:01:00Z"


def test_refresh_live_child_projects_discovery_report_counts(tmp_path: Path) -> None:
    status: dict[str, Any] = {
        "active": True,
        "runId": "pipeline_1",
        "stage": "discovery",
        "activeChildren": [
            {
                "id": "discovery_1",
                "runId": "discovery_1",
                "taskType": "discovery",
                "type": "discovery",
                "active": True,
                "taskProgress": {
                    "phaseKey": "probing_candidates",
                    "mode": "indeterminate",
                    "ratio": 0.0,
                    "counts": {},
                },
            }
        ],
    }
    report = {
        "runId": "discovery_1",
        "startedAt": "2026-08-31T12:00:01Z",
        "taskProgress": build_discovery_task_progress(
            summary=_probing_summary(probed=512),
            finished=False,
            updated_at="2026-08-31T12:00:10Z",
        ),
    }

    service = PipelineService(
        pipeline_state_lock=FakeLock(),
        pipeline_status=status,
        runtime=PipelineRuntime(),
        bridge_log=lambda *a, **kw: None,
        now_iso=lambda: "2026-08-31T12:00:15Z",
        parse_iso=make_parse_iso(),
        sync_task_running=lambda: False,
        current_fetch_output_count=lambda: 0,
        load_json_object=lambda _path, default: default,
        wait_for_sync_completion=lambda x, y: {"status": "ok", "summary": {}},
        discovery_report_path=tmp_path / "discovery-report.json",
        fetch_report_path=tmp_path / "fetch-report.json",
        trigger_discovery_task=lambda **kw: (200, {"started": True}),
        start_fetcher_task=lambda x: {"started": True, "runId": "fetch_1"},
        start_sync_task=lambda action, reason, automatic: {"started": True, "runId": "sync-123"},
        get_app_version=lambda: "1.0.0",
    )

    service._refresh_live_child_task_progress("discovery", report)

    child = service._status["activeChildren"][0]
    counts = child["taskProgress"]["counts"]
    assert counts["probedCandidates"] == 512
    assert counts["probeTotal"] == 2135
    assert child["taskProgress"]["mode"] == "determinate"
    assert child["taskProgress"]["phaseLabel"] == "Probing 2135 candidate(s)"
    # The worker stamped its counters at 12:00:10; the projection at 12:00:15 only
    # proves the pipeline re-read the report, so it must not restamp the cue.
    assert child["taskProgress"]["countsUpdatedAt"] == "2026-08-31T12:00:10Z"


def test_discovery_task_progress_stamps_counts_updated_at_only_when_counts_move() -> None:
    first = build_discovery_task_progress(
        summary=_probing_summary(probed=512),
        finished=False,
        updated_at="2026-08-31T12:00:00Z",
    )
    quiet = build_discovery_task_progress(
        summary=_probing_summary(probed=512),
        finished=False,
        updated_at="2026-08-31T12:05:00Z",
        previous_progress=first,
    )
    moved = build_discovery_task_progress(
        summary=_probing_summary(probed=530),
        finished=False,
        updated_at="2026-08-31T12:06:00Z",
        previous_progress=quiet,
    )

    assert first["countsUpdatedAt"] == "2026-08-31T12:00:00Z"
    # The report was rewritten on its cadence; the counters did not move.
    assert quiet["updatedAt"] == "2026-08-31T12:05:00Z"
    assert quiet["countsUpdatedAt"] == "2026-08-31T12:00:00Z"
    assert moved["countsUpdatedAt"] == "2026-08-31T12:06:00Z"


def test_quiet_discovery_child_keeps_the_worker_counts_stamp(tmp_path: Path) -> None:
    status: dict[str, Any] = {
        "active": True,
        "runId": "pipeline_1",
        "stage": "discovery",
        "activeChildren": [
            {
                "id": "discovery_1",
                "runId": "discovery_1",
                "taskType": "discovery",
                "type": "discovery",
                "active": True,
                "taskProgress": {
                    "phaseKey": "probing_candidates",
                    "mode": "indeterminate",
                    "ratio": 0.0,
                    "counts": {},
                },
            }
        ],
    }
    progress = build_discovery_task_progress(
        summary=_probing_summary(probed=512),
        finished=False,
        updated_at="2026-08-31T12:00:10Z",
    )
    progress["countsUpdatedAt"] = "2026-08-31T12:02:00Z"
    report = {
        "runId": "discovery_1",
        "startedAt": "2026-08-31T12:00:01Z",
        "taskProgress": progress,
    }

    service = PipelineService(
        pipeline_state_lock=FakeLock(),
        pipeline_status=status,
        runtime=PipelineRuntime(),
        bridge_log=lambda *a, **kw: None,
        now_iso=lambda: "2026-08-31T12:20:00Z",
        parse_iso=make_parse_iso(),
        sync_task_running=lambda: False,
        current_fetch_output_count=lambda: 0,
        load_json_object=lambda _path, default: default,
        wait_for_sync_completion=lambda x, y: {"status": "ok", "summary": {}},
        discovery_report_path=tmp_path / "discovery-report.json",
        fetch_report_path=tmp_path / "fetch-report.json",
        trigger_discovery_task=lambda **kw: (200, {"started": True}),
        start_fetcher_task=lambda x: {"started": True, "runId": "fetch_1"},
        start_sync_task=lambda action, reason, automatic: {"started": True, "runId": "sync-123"},
        get_app_version=lambda: "1.0.0",
    )

    service._refresh_live_child_task_progress("discovery", report)

    child = service._status["activeChildren"][0]
    # A freshly rebuilt child row must not reset the worker's stamp to "now".
    assert child["taskProgress"]["countsUpdatedAt"] == "2026-08-31T12:02:00Z"

    quiet = compute_pipeline_child_quiet_info(
        service._status,
        parse_iso=make_parse_iso(),
        now_utc=lambda: datetime.fromisoformat("2026-08-31T12:20:00+00:00"),
    )
    assert quiet is not None
    assert quiet["inChild"] == "discovery"
    assert quiet["quietSeconds"] >= 1000


def test_subtask_progress_restamps_counts_updated_at_only_when_audit_counts_move(
    tmp_path: Path,
) -> None:
    report_path = tmp_path / "discovery-report.json"

    def save(path: Path, payload: dict[str, Any]) -> None:
        path.write_text(json.dumps(payload), encoding="utf-8")

    def load(path: Path, default: Any) -> Any:
        if not path.exists():
            return default
        return json.loads(path.read_text(encoding="utf-8"))

    save(
        report_path,
        {
            "runId": "discovery_1",
            "summary": {"phase": "scanning_sources"},
            "taskProgress": {
                "active": True,
                "phaseKey": "scanning_sources",
                "updatedAt": "2026-08-31T12:00:00Z",
                "countsUpdatedAt": "2026-08-31T12:00:00Z",
                "counts": {"foundEndpoints": 10, "activeAuditCompletedUrls": 3},
            },
        },
    )

    def write_subtask(completed_urls: int, now: str) -> None:
        update_discovery_subtask_progress_report(
            report_write_path=report_path,
            run_id="discovery_1",
            phase="scanning_sources",
            phase_label="Scanning GameDevMap",
            subtask_progress={"counts": {"activeAuditCompletedUrls": completed_urls}},
            load_json_object_fn=load,
            save_json_atomic_fn=save,
            now_iso_fn=lambda: now,
        )

    write_subtask(4, "2026-08-31T12:01:00Z")
    moved = load(report_path, {})
    assert moved["taskProgress"]["counts"]["activeAuditCompletedUrls"] == 4
    assert moved["taskProgress"]["countsUpdatedAt"] == "2026-08-31T12:01:00Z"

    # A throttled rewrite that leaves the merged counters alone must carry the stamp.
    write_subtask(4, "2026-08-31T12:02:00Z")
    quiet = load(report_path, {})
    assert quiet["taskProgress"]["updatedAt"] == "2026-08-31T12:02:00Z"
    assert quiet["taskProgress"]["countsUpdatedAt"] == "2026-08-31T12:01:00Z"
