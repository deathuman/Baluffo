"""Discovery report progress liveness.

Locks in that the discovery stage's report taskProgress ticks during a run
(unlike the fetch report, which is intentionally sparse during source
execution): the probe loop writes the report every 10 completions with a fresh
``probedCandidateCount``, and the pipeline status payload's active child
projects those counts so the jobs caption shows ``N/M candidates probed``
staying current.
"""

from __future__ import annotations

from typing import Any

from src.source_discovery.reporting_progress import build_discovery_task_progress
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
    assert child["taskProgress"]["countsUpdatedAt"] == "2026-08-31T12:00:15Z"
