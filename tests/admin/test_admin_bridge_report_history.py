import os
from collections.abc import Callable
from dataclasses import dataclass
from unittest import mock

import pytest

from src import admin_bridge
from tests.admin._runtime_helpers import (
    active_progress,
    completed_progress,
    discovery_report,
    fetch_report,
    history_row,
    matching_history_rows,
    task_state_entry,
)

pytestmark = pytest.mark.usefixtures("admin_bridge_entrypoint_root")


def test_append_run_history_enforces_limit():
    for idx in range(8):
        admin_bridge.append_run_history(
            {
                "type": "fetch",
                "status": "ok",
                "startedAt": f"2026-03-01T0{idx}:00:00+00:00",
                "finishedAt": f"2026-03-01T0{idx}:05:00+00:00",
                "durationMs": 300000,
                "summary": {"outputCount": idx + 1, "failedSources": 0, "sourceCount": 1},
            }
        )
    rows = admin_bridge.load_run_history()
    assert len(rows) == 5
    assert rows[-1]["summary"]["outputCount"] == 8


def test_append_run_history_orders_by_started_at():
    admin_bridge.append_run_history(
        {
            "type": "fetch",
            "status": "ok",
            "startedAt": "2026-03-01T08:00:00+00:00",
            "finishedAt": "2026-03-01T10:00:00+00:00",
            "durationMs": 7200000,
            "summary": {"outputCount": 1, "failedSources": 0, "sourceCount": 1},
        }
    )
    admin_bridge.append_run_history(
        {
            "type": "sync",
            "status": "ok",
            "startedAt": "2026-03-01T09:00:00+00:00",
            "finishedAt": "2026-03-01T09:05:00+00:00",
            "durationMs": 300000,
            "summary": {"action": "pull", "activeCount": 1, "pendingCount": 0, "rejectedCount": 0},
        }
    )

    rows = admin_bridge.load_run_history()
    assert [row["type"] for row in rows] == ["fetch", "sync"]


@dataclass(frozen=True)
class _FetchReportCase:
    name: str
    payload: dict[str, object]
    expected_schema_version: int | None = None
    expected_started_at: str | None = None
    expected_finished_at: str | None = None
    expected_source_count: int | None = None
    expected_phase_key: str | None = None
    expected_phase_label: str | None = None
    expected_phase_active: bool | None = None
    expected_row_status: str | None = None
    expected_row_duration_ms: int | None = None
    expected_detail_name: str | None = None
    expected_ratio: float | None = None


def _run_fetch_report_case(case: _FetchReportCase) -> None:
    payload = admin_bridge.normalize_fetch_report_contract(case.payload)
    assert isinstance(payload.get("summary"), dict)
    assert isinstance(payload.get("runtime"), dict)
    assert isinstance(payload.get("taskProgress"), dict)
    if case.expected_schema_version is not None:
        assert int(payload.get("schemaVersion") or 0) == case.expected_schema_version
    if case.expected_started_at is not None:
        assert str(payload.get("startedAt") or "") == case.expected_started_at
    if case.expected_finished_at is not None:
        assert str(payload.get("finishedAt") or "") == case.expected_finished_at
    if case.expected_source_count is not None:
        assert len(payload.get("sources") or []) == case.expected_source_count
    task_progress = payload.get("taskProgress") or {}
    if case.expected_phase_key is not None:
        assert str(task_progress.get("phaseKey") or "") == case.expected_phase_key
    if case.expected_phase_label is not None:
        assert str(task_progress.get("phaseLabel") or "") == case.expected_phase_label
    if case.expected_phase_active is not None:
        assert bool(task_progress.get("active")) is case.expected_phase_active
    if case.expected_ratio is not None:
        assert float(task_progress.get("ratio") or 0) == case.expected_ratio
    if case.expected_row_status is not None or case.expected_row_duration_ms is not None:
        row = payload["sources"][0]
        if case.expected_row_status is not None:
            assert str(row.get("status") or "") == case.expected_row_status
        if case.expected_row_duration_ms is not None:
            assert int(row.get("durationMs") or 0) == case.expected_row_duration_ms
    if case.expected_detail_name is not None:
        row = payload["sources"][0]
        details = row.get("details") or []
        assert len(details) == 1
        assert str(details[0].get("name") or "") == case.expected_detail_name


FETCH_REPORT_CASES = [
    pytest.param(
        _FetchReportCase(
            name="minimal-payload",
            payload={
                "schemaVersion": "1.0",
                "startedAt": 123,
                "finishedAt": None,
                "summary": "bad",
                "sources": [{"name": "x", "status": "OK", "durationMs": "17"}],
            },
            expected_schema_version=1,
            expected_started_at="123",
            expected_finished_at="",
            expected_source_count=1,
            expected_phase_key="executing_sources",
            expected_row_status="ok",
            expected_row_duration_ms=17,
        ),
        id="minimal-payload",
    ),
    pytest.param(
        _FetchReportCase(
            name="blank-report",
            payload={},
            expected_phase_active=False,
            expected_phase_key="",
            expected_phase_label="",
        ),
        id="blank-report",
    ),
    pytest.param(
        _FetchReportCase(
            name="stringified-detail-rows",
            payload={
                "sources": [
                    {
                        "name": "lever_sources",
                        "status": "ok",
                        "details": [
                            "{'adapter': 'lever', 'studio': 'Jagex', 'name': 'Jagex (Lever)', 'status': 'ok', 'fetchedCount': 2, 'keptCount': 2, 'error': ''}"
                        ],
                    }
                ]
            },
            expected_source_count=1,
            expected_detail_name="Jagex (Lever)",
        ),
        id="stringified-detail-rows",
    ),
    pytest.param(
        _FetchReportCase(
            name="completed-progress",
            payload={
                "startedAt": "2026-03-23T16:16:54.905369+00:00",
                "finishedAt": "2026-03-23T16:18:10.053424+00:00",
                "taskProgress": {
                    "active": True,
                    "phaseKey": "executing_sources",
                    "phaseLabel": "Executing sources",
                    "mode": "determinate",
                    "ratio": 0.18,
                    "counts": {
                        "resolvedSources": 61,
                        "sourceCount": 520,
                        "outputCount": 3683,
                        "failedSources": 23,
                        "excludedSources": 0,
                    },
                },
                "summary": {
                    "outputCount": 3683,
                    "failedSources": 23,
                    "excludedSources": 0,
                    "sourceCount": 61,
                    "successfulSources": 38,
                },
                "sources": [],
            },
            expected_phase_active=False,
            expected_phase_key="completed",
            expected_phase_label="Completed",
            expected_ratio=1.0,
        ),
        id="completed-progress",
    ),
]


@pytest.mark.parametrize("case", FETCH_REPORT_CASES, ids=lambda case: case.name)
def test_normalize_fetch_report_contract_cases(case: _FetchReportCase) -> None:
    _run_fetch_report_case(case)


@dataclass(frozen=True)
class _DiscoveryReportCase:
    name: str
    payload: dict[str, object]
    expected_queued_candidates: int
    expected_probed_candidates: int | None = None
    expected_phase_key: str | None = None
    expected_mode: str | None = None
    expected_runtime_total_ms: int | None = None
    expected_stage_probe_ms: int | None = None
    expected_adapter_name: str | None = None
    expected_status: str | None = None


def _run_discovery_report_case(case: _DiscoveryReportCase) -> None:
    if case.expected_status is None:
        payload = admin_bridge.normalize_discovery_report_contract(case.payload)
        task_progress = payload.get("taskProgress") or {}
        assert int((payload.get("summary") or {}).get("queuedCandidateCount") or 0) == case.expected_queued_candidates
        if case.expected_probed_candidates is not None:
            assert int(task_progress.get("counts", {}).get("probedCandidates") or 0) == case.expected_probed_candidates
        if case.expected_phase_key is not None:
            assert str(task_progress.get("phaseKey") or "") == case.expected_phase_key
        if case.expected_mode is not None:
            assert str(task_progress.get("mode") or "") == case.expected_mode
        if case.expected_runtime_total_ms is not None:
            assert int((payload.get("runtime") or {}).get("totalDurationMs") or 0) == case.expected_runtime_total_ms
        if case.expected_stage_probe_ms is not None:
            assert int((((payload.get("runtime") or {}).get("stageTimingsMs") or {}).get("probe")) or 0) == case.expected_stage_probe_ms
        if case.expected_adapter_name is not None:
            assert str((((payload.get("runtime") or {}).get("adapterTimings") or [])[0].get("adapter")) or "") == case.expected_adapter_name
        counts = task_progress.get("counts") or {}
        assert int(counts.get("queuedCandidates") or 0) == case.expected_queued_candidates
    else:
        summary, status = admin_bridge.summarize_discovery_report(case.payload)
        assert int(summary.get("queuedCandidateCount") or 0) == case.expected_queued_candidates
        assert status == case.expected_status


DISCOVERY_REPORT_CASES = [
    pytest.param(
        _DiscoveryReportCase(
            name="normalize-queued-count",
            payload={
                "summary": {"queuedCandidateCount": 0, "probedCandidateCount": 4},
                "runtime": {
                    "totalDurationMs": "123",
                    "stageTimingsMs": {"probe": "45"},
                    "adapterTimings": [
                        {"adapter": "greenhouse", "durationMs": "22", "queuedCount": 1}
                    ],
                },
                "candidates": [
                    {"name": "A", "deferred": False},
                    {"name": "B"},
                    {"name": "C", "deferred": True},
                ],
            },
            expected_queued_candidates=2,
            expected_probed_candidates=4,
            expected_phase_key="starting",
            expected_mode="indeterminate",
            expected_runtime_total_ms=123,
            expected_stage_probe_ms=45,
            expected_adapter_name="greenhouse",
        ),
        id="normalize-queued-count",
    ),
    pytest.param(
        _DiscoveryReportCase(
            name="summarize-prefers-derived-queued-count",
            payload={
                "startedAt": "2026-03-01T00:00:00+00:00",
                "finishedAt": "2026-03-01T00:01:00+00:00",
                "summary": {
                    "queuedCandidateCount": 0,
                    "failedProbeCount": 0,
                    "probedCandidateCount": 2,
                },
                "candidates": [
                    {"name": "A"},
                    {"name": "B", "deferred": False},
                    {"name": "C", "deferred": True},
                ],
            },
            expected_queued_candidates=2,
            expected_status="ok",
        ),
        id="summarize-prefers-derived-queued-count",
    ),
]


@pytest.mark.parametrize("case", DISCOVERY_REPORT_CASES, ids=lambda case: case.name)
def test_discovery_report_queue_count_cases(case: _DiscoveryReportCase) -> None:
    _run_discovery_report_case(case)


def test_history_row_defaults_run_id() -> None:
    row = history_row(started_at="2026-03-01T00:00:00+00:00")
    assert str(row.get("runId") or "") == "fetch:started:2026-03-01T00:00:00+00:00::0"


def test_history_row_allows_explicit_missing_run_id() -> None:
    row = history_row(
        row_id="legacy_fetch_started",
        started_at="2026-03-01T00:00:00+00:00",
        allow_missing_run_id=True,
    )
    assert "runId" not in row


@dataclass(frozen=True)
class _SyncHistoryCase:
    name: str
    setup: Callable[[], None]
    assert_rows: Callable[[list[dict[str, object]]], None]


def _setup_unfinished_fetch_without_run_id() -> None:
    old_started = "2026-03-01T00:00:00+00:00"
    admin_bridge.save_json_atomic(admin_bridge.DISCOVERY_REPORT_PATH, {})
    admin_bridge.save_json_atomic(
        admin_bridge.OPS_HISTORY_PATH,
        [history_row(started_at=old_started, allow_missing_run_id=True)],
    )
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_REPORT_PATH,
        fetch_report(started_at=old_started),
    )
    old_ts = 1_700_000_000
    os.utime(admin_bridge.JOBS_FETCH_REPORT_PATH, (old_ts, old_ts))


def _assert_unfinished_fetch_without_run_id(rows: list[dict[str, object]]) -> None:
    assert rows == []
    report = admin_bridge.load_json_object(admin_bridge.JOBS_FETCH_REPORT_PATH, {})
    assert str(report.get("finishedAt") or "") == ""


def _setup_unfinished_discovery_read_side() -> None:
    old_started = "2026-03-01T00:00:00+00:00"
    admin_bridge.save_json_atomic(
        admin_bridge.DISCOVERY_REPORT_PATH,
        discovery_report(started_at=old_started),
    )
    old_ts = 1_700_000_000
    os.utime(admin_bridge.DISCOVERY_REPORT_PATH, (old_ts, old_ts))


def _assert_unfinished_discovery_read_side(_rows: list[dict[str, object]]) -> None:
    report = admin_bridge.load_json_object(admin_bridge.DISCOVERY_REPORT_PATH, {})
    assert str(report.get("finishedAt") or "").strip() == ""


def _setup_fetch_launcher_report_merge() -> None:
    run_id = "fetch_merge_1"
    started_at = "2026-03-01T00:00:00+00:00"
    finished_at = "2026-03-01T00:03:00+00:00"
    admin_bridge.save_json_atomic(
        admin_bridge.OPS_HISTORY_PATH,
        [history_row(row_id=run_id, run_id=run_id, started_at=started_at)],
    )
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_REPORT_PATH,
        fetch_report(
            run_id=run_id,
            started_at=started_at,
            finished_at=finished_at,
            summary={"outputCount": 10, "failedSources": 1, "sourceCount": 5},
        ),
    )


def _assert_fetch_launcher_report_merge(rows: list[dict[str, object]]) -> None:
    matching = [row for row in rows if str(row.get("runId") or "") == "fetch_merge_1"]
    assert len(matching) == 1
    assert str(matching[0].get("status") or "") == "warning"
    assert str(matching[0].get("finishedAt") or "") == "2026-03-01T00:03:00+00:00"


def _setup_duplicate_run_id_rows() -> None:
    started_at = "2026-03-01T00:00:00+00:00"
    finished_at = "2026-03-01T00:03:00+00:00"
    run_id = "run_a"
    summary = {"outputCount": 10, "failedSources": 1, "sourceCount": 5}
    admin_bridge.save_json_atomic(
        admin_bridge.OPS_HISTORY_PATH,
        [
            history_row(
                row_id=run_id,
                run_id=run_id,
                status="warning",
                started_at=started_at,
                finished_at=finished_at,
                duration_ms=1000,
                summary=summary,
            ),
            history_row(
                row_id="run_b",
                run_id=run_id,
                status="warning",
                started_at=started_at,
                finished_at=finished_at,
                duration_ms=1000,
                summary=summary,
            ),
        ],
    )
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_REPORT_PATH,
        fetch_report(run_id=run_id, started_at=started_at, finished_at=finished_at, summary=summary),
    )


def _assert_duplicate_run_id_rows(rows: list[dict[str, object]]) -> None:
    matching = matching_history_rows(
        rows,
        started_at="2026-03-01T00:00:00+00:00",
        finished_at="2026-03-01T00:03:00+00:00",
    )
    assert len(matching) == 1
    assert str(matching[0].get("runId") or "") == "run_a"


def _setup_project_run_id_row_only() -> None:
    run_id = "fetch_enrich_1"
    started_at = admin_bridge.now_iso()
    admin_bridge.save_json_atomic(
        admin_bridge.OPS_HISTORY_PATH,
        [
            history_row(
                row_id="legacy_fetch_started",
                started_at=started_at,
                allow_missing_run_id=True,
            )
        ],
    )
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_REPORT_PATH,
        fetch_report(run_id=run_id, started_at=started_at),
    )


def _assert_project_run_id_row_only(rows: list[dict[str, object]]) -> None:
    matching = matching_history_rows(rows, run_id="fetch_enrich_1")
    assert len(matching) == 1
    assert str(matching[0].get("status") or "").lower() == "started"


def _setup_discards_rows_without_run_id() -> None:
    run_id = "fetch_stale_1"
    started_at = "2026-03-01T00:00:00+00:00"
    admin_bridge.save_json_atomic(
        admin_bridge.OPS_HISTORY_PATH,
        [
            history_row(row_id=run_id, run_id=run_id, started_at=started_at),
            history_row(
                row_id="legacy_duplicate",
                started_at=started_at,
                allow_missing_run_id=True,
            ),
        ],
    )
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_REPORT_PATH,
        fetch_report(run_id=run_id, started_at=started_at),
    )
    old_ts = 1_700_000_000
    os.utime(admin_bridge.JOBS_FETCH_REPORT_PATH, (old_ts, old_ts))


def _assert_discards_rows_without_run_id(rows: list[dict[str, object]]) -> None:
    matching = matching_history_rows(
        rows,
        started_at="2026-03-01T00:00:00+00:00",
        run_id="fetch_stale_1",
    )
    assert len(matching) == 1
    assert str(matching[0].get("status") or "").lower() == "error"
    assert str((matching[0].get("summary") or {}).get("error") or "") == "owner_inactive_without_terminal_report"
    assert all(str(row.get("runId") or "").strip() for row in rows)


SYNC_HISTORY_CASES = [
    pytest.param(
        _SyncHistoryCase(
            name="discard-unfinished-fetch-without-run-id",
            setup=_setup_unfinished_fetch_without_run_id,
            assert_rows=_assert_unfinished_fetch_without_run_id,
        ),
        id="discard-unfinished-fetch-without-run-id",
    ),
    pytest.param(
        _SyncHistoryCase(
            name="discovery-read-side-does-not-finish",
            setup=_setup_unfinished_discovery_read_side,
            assert_rows=_assert_unfinished_discovery_read_side,
        ),
        id="discovery-read-side-does-not-finish",
    ),
    pytest.param(
        _SyncHistoryCase(
            name="merge-fetch-launcher-report",
            setup=_setup_fetch_launcher_report_merge,
            assert_rows=_assert_fetch_launcher_report_merge,
        ),
        id="merge-fetch-launcher-report",
    ),
    pytest.param(
        _SyncHistoryCase(
            name="collapse-duplicate-run-id",
            setup=_setup_duplicate_run_id_rows,
            assert_rows=_assert_duplicate_run_id_rows,
        ),
        id="collapse-duplicate-run-id",
    ),
    pytest.param(
        _SyncHistoryCase(
            name="project-run-id-row-only",
            setup=_setup_project_run_id_row_only,
            assert_rows=_assert_project_run_id_row_only,
        ),
        id="project-run-id-row-only",
    ),
    pytest.param(
        _SyncHistoryCase(
            name="discard-rows-without-run-id",
            setup=_setup_discards_rows_without_run_id,
            assert_rows=_assert_discards_rows_without_run_id,
        ),
        id="discard-rows-without-run-id",
    ),
]


@pytest.mark.parametrize("case", SYNC_HISTORY_CASES, ids=lambda case: case.name)
def test_sync_history_from_reports_cases(case: _SyncHistoryCase) -> None:
    case.setup()
    rows = admin_bridge.sync_history_from_reports()
    case.assert_rows(rows)


def test_start_fetcher_task_registers_history_before_report_can_duplicate():
    original_save = admin_bridge.save_json_atomic

    def intercepting_save(path, payload):
        original_save(path, payload)
        if path == admin_bridge.JOBS_FETCH_REPORT_PATH:
            rows = admin_bridge.sync_history_from_reports()
            matching = [
                row
                for row in rows
                if str(row.get("type") or "") == "fetch"
                and str(row.get("startedAt") or "") == str(payload.get("startedAt") or "")
            ]
            assert len(matching) == 1
            assert str(matching[0].get("runId") or "") == str(payload.get("runId") or "")

    with (
        mock.patch.object(admin_bridge, "save_json_atomic", side_effect=intercepting_save),
        mock.patch.object(admin_bridge, "run_background_script", return_value=24680),
    ):
        result = admin_bridge.start_fetcher_task({})

    rows = admin_bridge.load_run_history()
    matching = [row for row in rows if str(row.get("runId") or "") == str(result.get("runId") or "")]
    assert len(matching) == 1


def _setup_report_finished_while_owner_active() -> None:
    started_at = admin_bridge.now_iso()
    finished_at = admin_bridge.now_iso()
    run_id = "fetch_report_finished_1"
    admin_bridge.save_json_atomic(
        admin_bridge.TASK_STATE_PATH,
        {
            "fetch": task_state_entry("fetch", run_id=run_id, started_at=started_at),
        },
    )
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_REPORT_PATH,
        fetch_report(
            run_id=run_id,
            started_at=started_at,
            finished_at=finished_at,
            runtime={"lifecycle": {"owner": "fetch_report", "heartbeatAt": started_at}},
            task_progress=active_progress(
                "executing_sources",
                "Executing sources",
                {"resolvedSources": 5, "sourceCount": 10},
            ),
            summary={"outputCount": 10, "failedSources": 1, "sourceCount": 10},
        ),
    )
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_TASKS_PATH,
        {
            "runId": run_id,
            "startedAt": started_at,
            "finishedAt": "",
            "heartbeatAt": started_at,
            "taskProgress": {"active": True},
            "summary": {"queued": 0, "running": 1, "ok": 0, "error": 0},
            "tasks": [],
        },
    )
    admin_bridge.save_json_atomic(
        admin_bridge.OPS_HISTORY_PATH,
        [
            history_row(
                row_id=run_id,
                run_id=run_id,
                status="warning",
                started_at=started_at,
                finished_at=finished_at,
                duration_ms=123,
                summary={"outputCount": 10, "failedSources": 1, "sourceCount": 10},
            )
        ],
    )


def test_projected_run_history_keeps_live_fetch_started_when_report_finishes_early() -> None:
    _setup_report_finished_while_owner_active()

    projection = admin_bridge.get_projected_run_history()
    matching = matching_history_rows(projection.rows, run_id="fetch_report_finished_1")

    assert len(matching) == 1
    assert str(matching[0].get("status") or "") == "started"
    assert str(matching[0].get("finishedAt") or "") == ""
    assert any(
        str(item.get("code") or "") == "report_finished_while_owner_active"
        for item in (projection.diagnostics or [])
    )
