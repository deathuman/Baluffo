import io
import json
import os
from collections.abc import Callable
from contextlib import ExitStack, redirect_stdout
from dataclasses import dataclass
from unittest import mock

import pytest

from src import admin_bridge


@dataclass(frozen=True)
class _RuntimeConfigCase:
    name: str
    cli_args: list[str]
    env: dict[str, str]
    bridge_defaults: dict[str, object] | None = None
    storage_defaults: dict[str, object] | None = None
    expected_host: str | None = None
    expected_port: int | None = None
    expected_data_dir_token: str | None = None
    expected_data_dir_suffix: str | None = None
    expected_log_format: str | None = None
    expected_log_level: str | None = None
    expected_owner_mode: str | None = None
    expected_owner_token: str | None = None
    expected_started_by: str | None = None
    expected_owner_idle_timeout_s: float | None = None
    expected_quiet_requests: bool | None = None


def _run_runtime_config_case(case: _RuntimeConfigCase, admin_bridge_entrypoint_root) -> None:
    entrypoint_root = str(admin_bridge_entrypoint_root.resolve())
    cli_args = [entrypoint_root if arg == "__ENTRYPOINT_ROOT__" else arg for arg in case.cli_args]
    env = {
        key: entrypoint_root if value == "__ENTRYPOINT_ROOT__" else value
        for key, value in case.env.items()
    }
    with ExitStack() as stack:
        if case.bridge_defaults is not None:
            stack.enter_context(
                mock.patch.object(
                    admin_bridge,
                    "get_bridge_defaults",
                    return_value=case.bridge_defaults,
                )
            )
        if case.storage_defaults is not None:
            translated_storage_defaults = {}
            for key, value in case.storage_defaults.items():
                if isinstance(value, str) and value.startswith("__ENTRYPOINT_ROOT__/"):
                    translated_storage_defaults[key] = (
                        admin_bridge_entrypoint_root / value.split("/", 1)[1]
                    )
                elif value == "__ENTRYPOINT_ROOT__":
                    translated_storage_defaults[key] = admin_bridge_entrypoint_root
                else:
                    translated_storage_defaults[key] = value
            stack.enter_context(
                mock.patch.object(
                    admin_bridge,
                    "get_storage_defaults",
                    return_value=translated_storage_defaults,
                )
            )
        cfg = admin_bridge.resolve_runtime_config(cli_args, env=env)

    if case.expected_host is not None:
        assert cfg.host == case.expected_host
    if case.expected_port is not None:
        assert cfg.port == case.expected_port
    if case.expected_data_dir_token == "entrypoint-root":
        assert str(cfg.data_dir) == entrypoint_root
    elif case.expected_data_dir_suffix is not None:
        assert str(cfg.data_dir) == str(
            (admin_bridge_entrypoint_root / case.expected_data_dir_suffix).resolve()
        )
    if case.expected_log_format is not None:
        assert cfg.log_format == case.expected_log_format
    if case.expected_log_level is not None:
        assert cfg.log_level == case.expected_log_level
    if case.expected_owner_mode is not None:
        assert cfg.owner_mode == case.expected_owner_mode
    if case.expected_owner_token is not None:
        assert cfg.owner_token == case.expected_owner_token
    if case.expected_started_by is not None:
        assert cfg.started_by == case.expected_started_by
    if case.expected_owner_idle_timeout_s is not None:
        assert cfg.owner_idle_timeout_s == case.expected_owner_idle_timeout_s
    if case.expected_quiet_requests is not None:
        assert cfg.quiet_requests is case.expected_quiet_requests


RESOLVE_RUNTIME_CONFIG_CASES = [
    pytest.param(
        _RuntimeConfigCase(
            name="cli-env-precedence",
            cli_args=[
                "--port",
                "9001",
                "--host",
                "127.0.0.9",
                "--data-dir",
                "__ENTRYPOINT_ROOT__",
                "--log-format",
                "jsonl",
                "--log-level",
                "debug",
            ],
            env={
                "BALUFFO_BRIDGE_HOST": "1.2.3.4",
                "BALUFFO_BRIDGE_PORT": "9999",
                "BALUFFO_DATA_DIR": "C:\\should-not-win",
                "BALUFFO_BRIDGE_LOG_FORMAT": "human",
                "BALUFFO_BRIDGE_LOG_LEVEL": "info",
                "BALUFFO_BRIDGE_OWNER_MODE": "env-owner",
            },
            expected_host="127.0.0.9",
            expected_port=9001,
            expected_data_dir_token="entrypoint-root",
            expected_log_format="jsonl",
            expected_log_level="debug",
            expected_owner_mode="env-owner",
        ),
        id="cli-env-precedence",
    ),
    pytest.param(
        _RuntimeConfigCase(
            name="env-defaults-when-cli-missing",
            cli_args=[],
            env={
                "BALUFFO_BRIDGE_HOST": "0.0.0.0",
                "BALUFFO_BRIDGE_PORT": "9911",
                "BALUFFO_DATA_DIR": "__ENTRYPOINT_ROOT__",
                "BALUFFO_BRIDGE_LOG_FORMAT": "jsonl",
                "BALUFFO_BRIDGE_LOG_LEVEL": "debug",
                "BALUFFO_BRIDGE_OWNER_MODE": "dev-supervisor",
                "BALUFFO_BRIDGE_OWNER_TOKEN": "token-123",
                "BALUFFO_BRIDGE_STARTED_BY": "unit-test",
                "BALUFFO_BRIDGE_OWNER_IDLE_TIMEOUT_S": "45",
            },
            bridge_defaults={
                "host": "127.0.0.2",
                "port": 8878,
                "log_format": "human",
                "log_level": "info",
                "quiet_requests": False,
            },
            storage_defaults={"data_dir": "__ENTRYPOINT_ROOT__/from-file"},
            expected_host="0.0.0.0",
            expected_port=9911,
            expected_data_dir_token="entrypoint-root",
            expected_log_format="jsonl",
            expected_log_level="debug",
            expected_owner_mode="dev-supervisor",
            expected_owner_token="token-123",
            expected_started_by="unit-test",
            expected_owner_idle_timeout_s=45.0,
        ),
        id="env-defaults-when-cli-missing",
    ),
    pytest.param(
        _RuntimeConfigCase(
            name="file-defaults-when-env-missing",
            cli_args=[],
            env={},
            bridge_defaults={
                "host": "127.0.0.5",
                "port": 9915,
                "log_format": "jsonl",
                "log_level": "debug",
                "quiet_requests": True,
            },
            storage_defaults={"data_dir": "__ENTRYPOINT_ROOT__/from-file"},
            expected_host="127.0.0.5",
            expected_port=9915,
            expected_data_dir_suffix="from-file",
            expected_log_format="jsonl",
            expected_log_level="debug",
            expected_quiet_requests=True,
        ),
        id="file-defaults-when-env-missing",
    ),
]


@pytest.mark.parametrize("case", RESOLVE_RUNTIME_CONFIG_CASES, ids=lambda case: case.name)
def test_resolve_runtime_config_uses_precedence_matrix(
    case: _RuntimeConfigCase,
    admin_bridge_entrypoint_root,
) -> None:
    _run_runtime_config_case(case, admin_bridge_entrypoint_root)


def test_bridge_log_jsonl_output_is_valid_json(admin_bridge_entrypoint_root):
    cfg = admin_bridge.RuntimeConfig(
        root=admin_bridge_entrypoint_root,
        data_dir=admin_bridge_entrypoint_root,
        host="127.0.0.1",
        port=8877,
        log_format="jsonl",
        log_level="info",
        quiet_requests=False,
    )
    admin_bridge.configure_runtime_paths(cfg)
    buf = io.StringIO()
    with redirect_stdout(buf):
        admin_bridge.bridge_log("info", "hello_bridge", runId="abc123")
    line = buf.getvalue().strip()
    payload = json.loads(line)
    assert str(payload.get("message") or "") == "hello_bridge"
    assert str(payload.get("runId") or "") == "abc123"
    assert str(payload.get("level") or "") == "info"


def test_bridge_log_swallows_broken_windows_pipe(admin_bridge_entrypoint_root):
    cfg = admin_bridge.RuntimeConfig(
        root=admin_bridge_entrypoint_root,
        data_dir=admin_bridge_entrypoint_root,
        host="127.0.0.1",
        port=8877,
        log_format="human",
        log_level="info",
        quiet_requests=False,
    )
    admin_bridge.configure_runtime_paths(cfg)
    with mock.patch(
        "builtins.print", side_effect=OSError(233, "No process is on the other end of the pipe")
    ):
        admin_bridge.bridge_log("info", "hello_bridge", runId="abc123")


def test_configure_runtime_paths_updates_bridge_paths(admin_bridge_entrypoint_root):
    data_dir = admin_bridge_entrypoint_root / "runtime-data"
    cfg = admin_bridge.RuntimeConfig(
        root=admin_bridge_entrypoint_root,
        data_dir=data_dir,
        host="127.0.0.1",
        port=8877,
        log_format="human",
        log_level="info",
        quiet_requests=False,
    )
    admin_bridge.configure_runtime_paths(cfg)
    assert admin_bridge.ACTIVE_PATH == data_dir / "source-registry-active.json"
    assert admin_bridge.TASK_STATE_PATH == data_dir / "admin-task-state.json"
    assert admin_bridge.source_registry_module.DATA_DIR == data_dir.resolve()


def test_append_run_history_enforces_limit(admin_bridge_entrypoint_root):
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


def test_append_run_history_orders_by_started_at(admin_bridge_entrypoint_root):
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


def test_compute_ops_health_reports_alerts(admin_bridge_entrypoint_root):
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_REPORT_PATH,
        {
            "startedAt": "2026-03-01T00:00:00+00:00",
            "finishedAt": "2026-03-01T00:10:00+00:00",
            "summary": {"outputCount": 100, "failedSources": 3, "sourceCount": 4},
            "sources": [],
        },
    )
    health = admin_bridge.compute_ops_health()
    assert health["service"] == "baluffo-bridge"
    assert health["appVersion"] == admin_bridge.get_app_version()
    assert health["startupReady"] is True
    assert "desktopMode" in health
    assert bool(health["desktopMode"]) == bool(admin_bridge.RUNTIME_CONFIG.desktop_mode)
    assert "owner" in health
    assert str(health["owner"]["mode"] or "") == str(admin_bridge.RUNTIME_CONFIG.owner_mode or "")
    assert "kpis" in health
    assert "alerts" in health
    assert "updater" in health
    assert str((health["updater"] or {}).get("currentVersion") or "") == admin_bridge.get_app_version()
    assert len(health["alerts"]) >= 1
    assert any(alert["id"] == "degraded_reliability" for alert in health["alerts"])


def test_owner_session_should_exit_when_supervised_bridge_is_idle(admin_bridge_entrypoint_root):
    cfg = admin_bridge.RuntimeConfig(
        root=admin_bridge_entrypoint_root,
        data_dir=admin_bridge_entrypoint_root,
        host="127.0.0.1",
        port=8877,
        log_format="human",
        log_level="info",
        quiet_requests=False,
        desktop_mode=False,
        owner_mode="dev-supervisor",
        owner_token="owner-1",
        started_by="test",
        owner_idle_timeout_s=10.0,
    )
    admin_bridge.configure_runtime_paths(cfg)
    admin_bridge.bridge_runtime_state.OWNER_STATE["lastActivityAt"] = "2026-03-01T00:00:00+00:00"

    with mock.patch.object(
        admin_bridge,
        "now_utc",
        return_value=admin_bridge.parse_iso("2026-03-01T00:00:15+00:00"),
    ):
        assert admin_bridge.owner_session_should_exit() is True


def test_compute_ops_health_includes_social_alerts(admin_bridge_entrypoint_root):
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_REPORT_PATH,
        {
            "startedAt": "2026-03-01T00:00:00+00:00",
            "finishedAt": "2026-03-01T00:10:00+00:00",
            "summary": {"outputCount": 20, "failedSources": 0, "sourceCount": 3},
            "socialSummary": {
                "pilotWindowStartAt": "2026-03-01T00:00:00+00:00",
                "pilotWindowEndAt": "2026-03-01T00:10:00+00:00",
                "scheduledRunCount": 1,
                "keptCount": 2,
                "uniqueKeptCount": 2,
                "officialBoardOverlapCount": 0,
                "duplicateCount": 0,
                "duplicateRate": 0.0,
                "lowConfidenceDropped": 0,
                "sampleSize": 0,
                "reviewedCount": 0,
                "falsePositiveCount": 0,
                "falsePositiveRate": 0.0,
                "reviewArtifactPath": "data/social-experiment-review.json",
                "channels": {
                    "reddit": {
                        "keptCount": 1,
                        "uniqueKeptCount": 1,
                        "officialBoardOverlapCount": 0,
                        "duplicateCount": 0,
                        "duplicateRate": 0.0,
                        "lowConfidenceDropped": 0,
                    },
                    "mastodon": {
                        "keptCount": 1,
                        "uniqueKeptCount": 1,
                        "officialBoardOverlapCount": 0,
                        "duplicateCount": 0,
                        "duplicateRate": 0.0,
                        "lowConfidenceDropped": 0,
                    },
                },
            },
            "sources": [
                {
                    "name": "social_reddit",
                    "status": "error",
                    "fetchedCount": 30,
                    "keptCount": 0,
                    "lowConfidenceDropped": 70,
                },
                {
                    "name": "social_x",
                    "status": "error",
                    "fetchedCount": 20,
                    "keptCount": 0,
                    "lowConfidenceDropped": 60,
                },
                {
                    "name": "social_mastodon",
                    "status": "ok",
                    "fetchedCount": 20,
                    "keptCount": 0,
                    "lowConfidenceDropped": 20,
                },
            ],
        },
    )
    health = admin_bridge.compute_ops_health()
    social_kpis = health.get("kpis", {}).get("socialExperiment", {})
    assert int(social_kpis.get("keptCount") or 0) == 2
    assert int(social_kpis.get("uniqueKeptCount") or 0) == 2
    assert int(social_kpis.get("sampleSize") or 0) == 0
    assert int(social_kpis.get("reviewedCount") or 0) == 0
    assert float(social_kpis.get("falsePositiveRate") or 0) == 0.0
    ids = {str(row.get("id") or "") for row in health.get("alerts", [])}
    assert "social_sources_failing" in ids
    assert "social_zero_matches" in ids
    assert "social_low_confidence_spike" in ids
    assert "social_false_positive_spike" not in ids


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
        assert (
            int((payload.get("summary") or {}).get("queuedCandidateCount") or 0)
            == case.expected_queued_candidates
        )
        if case.expected_probed_candidates is not None:
            assert (
                int(task_progress.get("counts", {}).get("probedCandidates") or 0)
                == case.expected_probed_candidates
            )
        if case.expected_phase_key is not None:
            assert str(task_progress.get("phaseKey") or "") == case.expected_phase_key
        if case.expected_mode is not None:
            assert str(task_progress.get("mode") or "") == case.expected_mode
        if case.expected_runtime_total_ms is not None:
            assert (
                int((payload.get("runtime") or {}).get("totalDurationMs") or 0)
                == case.expected_runtime_total_ms
            )
        if case.expected_stage_probe_ms is not None:
            assert (
                int(
                    (((payload.get("runtime") or {}).get("stageTimingsMs") or {}).get("probe")) or 0
                )
                == case.expected_stage_probe_ms
            )
        if case.expected_adapter_name is not None:
            assert (
                str(
                    (((payload.get("runtime") or {}).get("adapterTimings") or [])[0].get("adapter"))
                    or ""
                )
                == case.expected_adapter_name
            )
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


@dataclass(frozen=True)
class _FetcherArgsCase:
    name: str
    payload: dict[str, object]
    expected_preset: str
    report_payload: dict[str, object] | None = None
    expected_present: tuple[str, ...] = ()
    expected_absent: tuple[str, ...] = ()
    expected_values: tuple[tuple[str, str], ...] = ()


def _run_fetcher_args_case(case: _FetcherArgsCase) -> None:
    if case.report_payload is not None:
        admin_bridge.save_json_atomic(
            admin_bridge.JOBS_FETCH_REPORT_PATH,
            case.report_payload,
        )
    args, preset = admin_bridge.build_fetcher_args_from_payload(case.payload)
    assert preset == case.expected_preset
    for option in case.expected_present:
        assert option in args
    for option in case.expected_absent:
        assert option not in args
    for option, value in case.expected_values:
        assert option in args
        assert args[args.index(option) + 1] == value


FETCHER_ARGS_CASES = [
    pytest.param(
        _FetcherArgsCase(
            name="retry-failed-filters-unknown",
            payload={"preset": "retry_failed"},
            expected_preset="retry_failed",
            report_payload={
                "startedAt": "2026-03-01T00:00:00+00:00",
                "finishedAt": "2026-03-01T00:10:00+00:00",
                "summary": {"failedSources": 3, "sourceCount": 4},
                "sources": [
                    {"name": "remote_ok", "status": "error"},
                    {"name": "unknown_custom_source", "status": "error"},
                    {"name": "google_sheets", "status": "error"},
                    {"name": "remote_ok", "status": "error"},
                ],
            },
            expected_present=(
                "--only-sources",
                "--ignore-circuit-breaker",
                "--fetch-strategy",
                "--adapter-http-concurrency",
            ),
            expected_absent=("--quiet",),
            expected_values=(("--only-sources", "google_sheets,remote_ok"),),
        ),
        id="retry-failed-filters-unknown",
    ),
    pytest.param(
        _FetcherArgsCase(
            name="retry-failed-without-known-failures",
            payload={"preset": "retry_failed"},
            expected_preset="retry_failed",
            report_payload={
                "sources": [
                    {"name": "unknown_custom_source_a", "status": "error"},
                    {"name": "unknown_custom_source_b", "status": "error"},
                ]
            },
            expected_present=("--ignore-circuit-breaker",),
            expected_absent=("--only-sources", "--quiet"),
        ),
        id="retry-failed-without-known-failures",
    ),
    pytest.param(
        _FetcherArgsCase(
            name="cadence-and-strategy-overrides",
            payload={
                "preset": "default",
                "fetchStrategy": "http",
                "adapterHttpConcurrency": 48,
                "respectSourceCadence": True,
                "hotSourceCadenceMinutes": 20,
                "coldSourceCadenceMinutes": 90,
            },
            expected_preset="default",
            expected_present=(
                "--fetch-strategy",
                "--adapter-http-concurrency",
                "--respect-source-cadence",
                "--hot-source-cadence-minutes",
                "--cold-source-cadence-minutes",
            ),
            expected_values=(
                ("--fetch-strategy", "http"),
                ("--adapter-http-concurrency", "48"),
                ("--hot-source-cadence-minutes", "20"),
                ("--cold-source-cadence-minutes", "90"),
            ),
        ),
        id="cadence-and-strategy-overrides",
    ),
    pytest.param(
        _FetcherArgsCase(
            name="social-enabled-default",
            payload={"preset": "default"},
            expected_preset="default",
            expected_present=("--social-enabled",),
        ),
        id="social-enabled-default",
    ),
    pytest.param(
        _FetcherArgsCase(
            name="social-opt-out",
            payload={"preset": "default", "socialEnabled": False},
            expected_preset="default",
            expected_absent=("--social-enabled",),
        ),
        id="social-opt-out",
    ),
    pytest.param(
        _FetcherArgsCase(
            name="uncapped-keeps-social",
            payload={"preset": "uncapped"},
            expected_preset="uncapped",
            expected_present=(
                "--force-refresh-all",
                "--ignore-circuit-breaker",
                "--max-workers",
                "--max-per-domain",
                "--static-detail-concurrency",
                "--source-ttl-minutes",
                "--circuit-breaker-failures",
                "--circuit-breaker-cooldown-minutes",
                "--social-enabled",
            ),
            expected_absent=("--adapter-http-concurrency",),
            expected_values=(
                ("--max-workers", "64"),
                ("--max-per-domain", "6"),
                ("--static-detail-concurrency", "24"),
                ("--source-ttl-minutes", "0"),
                ("--circuit-breaker-failures", "0"),
                ("--circuit-breaker-cooldown-minutes", "0"),
            ),
        ),
        id="uncapped-keeps-social",
    ),
]


@pytest.mark.parametrize("case", FETCHER_ARGS_CASES, ids=lambda case: case.name)
def test_build_fetcher_args_matrix(case: _FetcherArgsCase) -> None:
    _run_fetcher_args_case(case)


def _history_row(
    *,
    row_id: str | None = None,
    run_id: str | None = None,
    status: str = "started",
    started_at: str,
    finished_at: str = "",
    duration_ms: int = 0,
    summary: dict[str, object] | None = None,
) -> dict[str, object]:
    row: dict[str, object] = {
        "type": "fetch",
        "status": status,
        "startedAt": started_at,
        "finishedAt": finished_at,
        "durationMs": duration_ms,
        "summary": summary or {},
    }
    if row_id is not None:
        row["id"] = row_id
    if run_id is not None:
        row["runId"] = run_id
    return row


def _fetch_report(
    *,
    started_at: str,
    run_id: str | None = None,
    finished_at: str = "",
    summary: dict[str, object] | None = None,
    runtime: dict[str, object] | None = None,
    task_progress: dict[str, object] | None = None,
) -> dict[str, object]:
    report: dict[str, object] = {
        "startedAt": started_at,
        "finishedAt": finished_at,
        "summary": summary or {"outputCount": 0, "failedSources": 0, "sourceCount": 0},
        "sources": [],
    }
    if run_id is not None:
        report["runId"] = run_id
    if runtime is not None:
        report["runtime"] = runtime
    if task_progress is not None:
        report["taskProgress"] = task_progress
    return report


def _discovery_report(
    *,
    started_at: str,
    run_id: str | None = None,
    finished_at: str = "",
    summary: dict[str, object] | None = None,
    task_progress: dict[str, object] | None = None,
) -> dict[str, object]:
    report: dict[str, object] = {
        "startedAt": started_at,
        "finishedAt": finished_at,
        "summary": summary
        or {
            "foundEndpointCount": 0,
            "probedCandidateCount": 0,
            "queuedCandidateCount": 0,
            "failedProbeCount": 0,
        },
        "candidates": [],
        "failures": [],
    }
    if run_id is not None:
        report["runId"] = run_id
    if task_progress is not None:
        report["taskProgress"] = task_progress
    return report


def _task_state_entry(
    task_type: str,
    *,
    run_id: str,
    started_at: str,
    pid: int = 111,
    script: str | None = None,
) -> dict[str, object]:
    return {
        "runId": run_id,
        "taskType": task_type,
        "pid": pid,
        "script": script
        or ("source_discovery.py" if task_type == "discovery" else "jobs_fetcher.py"),
        "status": "running",
        "startedAt": started_at,
    }


def _active_progress(
    phase_key: str, phase_label: str, counts: dict[str, object]
) -> dict[str, object]:
    return {
        "active": True,
        "phaseKey": phase_key,
        "phaseLabel": phase_label,
        "mode": "determinate",
        "ratio": 0.5,
        "counts": counts,
    }


def _completed_progress(phase_label: str) -> dict[str, object]:
    return {
        "active": False,
        "phaseKey": "completed",
        "phaseLabel": phase_label,
        "mode": "determinate",
        "ratio": 1,
        "counts": {},
    }


def _matching_history_rows(
    rows: list[dict[str, object]],
    *,
    started_at: str | None = None,
    finished_at: str | None = None,
    run_id: str | None = None,
) -> list[dict[str, object]]:
    return [
        row
        for row in rows
        if str(row.get("type") or "") == "fetch"
        and (started_at is None or str(row.get("startedAt") or "") == started_at)
        and (finished_at is None or str(row.get("finishedAt") or "") == finished_at)
        and (run_id is None or str(row.get("runId") or "") == run_id)
    ]


def _task_row(payload: dict[str, object], task_type: str) -> dict[str, object]:
    return next(
        row for row in (payload.get("tasks") or []) if str(row.get("taskType") or "") == task_type
    )


def _current_task_payload() -> dict[str, object]:
    return admin_bridge.build_bridge_api(
        admin_bridge.RUNTIME_CONFIG
    ).get_current_task_state_payload()


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
        [_history_row(started_at=old_started)],
    )
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_REPORT_PATH,
        _fetch_report(started_at=old_started),
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
        _discovery_report(started_at=old_started),
    )
    old_ts = 1_700_000_000
    os.utime(admin_bridge.DISCOVERY_REPORT_PATH, (old_ts, old_ts))


def _assert_unfinished_discovery_read_side(_rows: list[dict[str, object]]) -> None:
    report = admin_bridge.load_json_object(admin_bridge.DISCOVERY_REPORT_PATH, {})
    assert str(report.get("finishedAt") or "").strip() == ""


def test_infer_studio_name_from_host_skips_www_and_splits_studio_token(
    admin_bridge_entrypoint_root,
):
    studio = admin_bridge.infer_studio_name_from_host("https://www.naconstudiomilan.com/careers/")
    assert studio == "Nacon Studio Milan"


def test_infer_studio_name_from_host_skips_short_placeholder_subdomain(
    admin_bridge_entrypoint_root,
):
    studio = admin_bridge.infer_studio_name_from_host("https://w.nixxes.com/jobs")
    assert studio == "Nixxes"


def test_alert_ack_suppresses_visible_alert(admin_bridge_entrypoint_root):
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_REPORT_PATH,
        {
            "startedAt": "2026-03-01T00:00:00+00:00",
            "finishedAt": "2026-03-01T00:10:00+00:00",
            "summary": {"outputCount": 100, "failedSources": 3, "sourceCount": 4},
            "sources": [],
        },
    )
    initial = admin_bridge.compute_ops_health()
    alert_ids = [row["id"] for row in initial.get("alerts", [])]
    assert "degraded_reliability" in alert_ids
    state = admin_bridge.load_alert_state()
    state["acked"]["degraded_reliability"] = admin_bridge.now_iso()
    admin_bridge.save_alert_state(state)
    updated = admin_bridge.compute_ops_health()
    updated_ids = [row["id"] for row in updated.get("alerts", [])]
    assert "degraded_reliability" not in updated_ids


def _configure_background_script_runtime(
    admin_bridge_entrypoint_root, *, desktop_mode: bool
) -> None:
    cfg = admin_bridge.RuntimeConfig(
        root=admin_bridge_entrypoint_root,
        data_dir=admin_bridge_entrypoint_root,
        host="127.0.0.1",
        port=8877,
        log_format="human",
        log_level="info",
        quiet_requests=True,
        desktop_mode=desktop_mode,
    )
    admin_bridge.configure_runtime_paths(cfg)


@dataclass(frozen=True)
class _BackgroundScriptCommandCase:
    name: str
    frozen: bool
    executable: str
    desktop_mode: bool
    fake_pid: int
    expected_prefix: tuple[str, ...]
    expected_script_arg: str | None = None
    expected_python_script_path: bool = False
    expect_unbuffered_env: bool = False


BACKGROUND_SCRIPT_COMMAND_CASES = [
    pytest.param(
        _BackgroundScriptCommandCase(
            name="frozen-child-script",
            frozen=True,
            executable="C:/tmp/Baluffo.exe",
            desktop_mode=True,
            fake_pid=12345,
            expected_prefix=("C:/tmp/Baluffo.exe", "__child_script__", "--root"),
            expected_script_arg="source_discovery.py",
        ),
        id="frozen-child-script",
    ),
    pytest.param(
        _BackgroundScriptCommandCase(
            name="unbuffered-python",
            frozen=False,
            executable="C:/Python313/python.exe",
            desktop_mode=False,
            fake_pid=24680,
            expected_prefix=("C:/Python313/python.exe", "-u"),
            expected_python_script_path=True,
            expect_unbuffered_env=True,
        ),
        id="unbuffered-python",
    ),
]


@pytest.mark.parametrize("case", BACKGROUND_SCRIPT_COMMAND_CASES, ids=lambda case: case.name)
def test_run_background_script_command_shape(
    case: _BackgroundScriptCommandCase,
    admin_bridge_entrypoint_root,
) -> None:
    _configure_background_script_runtime(
        admin_bridge_entrypoint_root,
        desktop_mode=case.desktop_mode,
    )
    fake_proc = type("FakeProc", (), {"pid": case.fake_pid})()
    with (
        mock.patch.object(admin_bridge.sys, "frozen", case.frozen, create=True),
        mock.patch.object(admin_bridge.sys, "executable", case.executable),
        mock.patch.object(admin_bridge.subprocess, "Popen", return_value=fake_proc) as popen_mock,
    ):
        admin_bridge.run_background_script("source_discovery.py", ["--mode", "dynamic"])
    command = popen_mock.call_args.args[0]
    assert command[: len(case.expected_prefix)] == list(case.expected_prefix)
    if case.expected_script_arg is not None:
        assert command[:5] == [
            case.executable,
            "__child_script__",
            "--root",
            str(admin_bridge_entrypoint_root),
            "--script",
        ]
        assert case.expected_script_arg in command
    if case.expected_python_script_path:
        assert command[2] == str(admin_bridge_entrypoint_root / "src" / "source_discovery.py")
    assert command[-2:] == ["--mode", "dynamic"]
    kwargs = popen_mock.call_args.kwargs
    if case.expect_unbuffered_env:
        assert kwargs["env"]["PYTHONUNBUFFERED"] == "1"
        assert kwargs["stdout"] is kwargs["stderr"]


def test_run_background_script_persists_run_id_in_task_state(admin_bridge_entrypoint_root):
    _configure_background_script_runtime(admin_bridge_entrypoint_root, desktop_mode=False)
    fake_proc = type("FakeProc", (), {"pid": 24680})()
    with (
        mock.patch.object(admin_bridge.sys, "frozen", False, create=True),
        mock.patch.object(admin_bridge.sys, "executable", "C:/Python313/python.exe"),
        mock.patch.object(admin_bridge.subprocess, "Popen", return_value=fake_proc),
    ):
        admin_bridge.run_background_script(
            "jobs_fetcher.py",
            [],
            extra_env={
                "BALUFFO_FETCH_RUN_ID": "fetch_state_1",
                "BALUFFO_FETCH_STARTED_AT": "2026-03-27T14:00:00+00:00",
            },
        )
    task_state = admin_bridge.load_json_object(admin_bridge.TASK_STATE_PATH, {})
    fetch_state = task_state.get("fetch") or {}
    assert str(fetch_state.get("runId") or "") == "fetch_state_1"
    assert str(fetch_state.get("taskType") or "") == "fetch"
    assert str(fetch_state.get("startedAt") or "") == "2026-03-27T14:00:00+00:00"


def test_start_fetcher_task_writes_report_shell_with_run_id(admin_bridge_entrypoint_root):
    with mock.patch.object(admin_bridge, "run_background_script", return_value=24680):
        result = admin_bridge.start_fetcher_task({})

    assert result["started"] is True
    run_id = str(result.get("runId") or "")
    assert run_id.startswith("fetch_")

    report = admin_bridge.load_json_object(admin_bridge.JOBS_FETCH_REPORT_PATH, {})
    assert str(report.get("runId") or "") == run_id
    assert str(report.get("startedAt") or "") == str(result.get("startedAt") or "")
    assert str(report.get("finishedAt") or "") == ""

    rows = admin_bridge.load_run_history()
    assert any(
        str(row.get("type") or "") == "fetch"
        and str(row.get("status") or "") == "started"
        and str(row.get("runId") or "") == run_id
        for row in rows
    )


def _setup_fetch_launcher_report_merge() -> None:
    run_id = "fetch_merge_1"
    started_at = "2026-03-01T00:00:00+00:00"
    finished_at = "2026-03-01T00:03:00+00:00"
    admin_bridge.save_json_atomic(
        admin_bridge.OPS_HISTORY_PATH,
        [_history_row(row_id=run_id, run_id=run_id, started_at=started_at)],
    )
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_REPORT_PATH,
        _fetch_report(
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
            _history_row(
                row_id=run_id,
                run_id=run_id,
                status="warning",
                started_at=started_at,
                finished_at=finished_at,
                duration_ms=1000,
                summary=summary,
            ),
            _history_row(
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
        _fetch_report(
            run_id=run_id, started_at=started_at, finished_at=finished_at, summary=summary
        ),
    )


def _assert_duplicate_run_id_rows(rows: list[dict[str, object]]) -> None:
    matching = _matching_history_rows(
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
        [_history_row(row_id="legacy_fetch_started", started_at=started_at)],
    )
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_REPORT_PATH,
        _fetch_report(run_id=run_id, started_at=started_at),
    )


def _assert_project_run_id_row_only(rows: list[dict[str, object]]) -> None:
    matching = _matching_history_rows(rows, run_id="fetch_enrich_1")
    assert len(matching) == 1
    assert str(matching[0].get("status") or "").lower() == "started"


def _setup_discards_rows_without_run_id() -> None:
    run_id = "fetch_stale_1"
    started_at = "2026-03-01T00:00:00+00:00"
    admin_bridge.save_json_atomic(
        admin_bridge.OPS_HISTORY_PATH,
        [
            _history_row(row_id=run_id, run_id=run_id, started_at=started_at),
            _history_row(row_id="legacy_duplicate", started_at=started_at),
        ],
    )
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_REPORT_PATH,
        _fetch_report(run_id=run_id, started_at=started_at),
    )
    old_ts = 1_700_000_000
    os.utime(admin_bridge.JOBS_FETCH_REPORT_PATH, (old_ts, old_ts))


def _assert_discards_rows_without_run_id(rows: list[dict[str, object]]) -> None:
    matching = _matching_history_rows(
        rows,
        started_at="2026-03-01T00:00:00+00:00",
        run_id="fetch_stale_1",
    )
    assert len(matching) == 1
    assert str(matching[0].get("status") or "").lower() == "error"
    assert (
        str((matching[0].get("summary") or {}).get("error") or "")
        == "owner_inactive_without_terminal_report"
    )
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


def test_start_fetcher_task_registers_history_before_report_can_duplicate(
    admin_bridge_entrypoint_root,
):
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
    matching = [
        row for row in rows if str(row.get("runId") or "") == str(result.get("runId") or "")
    ]
    assert len(matching) == 1


@dataclass(frozen=True)
class _CurrentTaskStateCase:
    name: str
    setup: Callable[[], Callable[[], None] | None]
    assert_payload: Callable[[dict[str, object]], None]
    pid_is_running: bool | None = None


def _setup_active_tasks_projection() -> Callable[[], None]:
    started_at = admin_bridge.now_iso()
    admin_bridge.save_json_atomic(
        admin_bridge.TASK_STATE_PATH,
        {
            "fetch": _task_state_entry("fetch", run_id="fetch_1", started_at=started_at),
            "discovery": _task_state_entry(
                "discovery",
                run_id="discovery_1",
                started_at=started_at,
                pid=222,
            ),
        },
    )
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_REPORT_PATH,
        _fetch_report(
            run_id="fetch_1",
            started_at=started_at,
            task_progress=_active_progress(
                "executing_sources",
                "Executing sources",
                {"resolvedSources": 5, "sourceCount": 10},
            ),
            summary={"outputCount": 10, "failedSources": 1, "sourceCount": 10},
        ),
    )
    admin_bridge.save_json_atomic(
        admin_bridge.DISCOVERY_REPORT_PATH,
        _discovery_report(
            run_id="discovery_1",
            started_at=started_at,
            task_progress={
                **_active_progress(
                    "scanning_sources",
                    "Scanning known careers pages",
                    {"queuedCandidates": 3},
                ),
                "mode": "indeterminate",
                "ratio": 0,
            },
            summary={"queuedCandidateCount": 3},
        ),
    )
    admin_bridge.bridge_runtime_state.PIPELINE_STATUS.update(
        {
            "active": True,
            "runId": "pipeline_1",
            "stage": "fetch",
            "startedAt": started_at,
            "finishedAt": "",
            "progress": {
                "currentStep": 2,
                "totalSteps": 3,
                "percent": 67,
                "label": "Running fetch...",
            },
        }
    )
    admin_bridge.SyncState.add_active_sync_run("sync_1")
    admin_bridge.append_run_history(
        {
            "id": "sync_1",
            "type": "sync",
            "status": "started",
            "startedAt": started_at,
            "finishedAt": "",
            "durationMs": 0,
            "summary": {"action": "push"},
        }
    )

    def cleanup() -> None:
        admin_bridge.SyncState.remove_active_sync_run("sync_1")
        admin_bridge.bridge_runtime_state.PIPELINE_STATUS.update(
            {
                "active": False,
                "runId": "",
                "stage": "idle",
                "startedAt": "",
                "finishedAt": "",
                "progress": {"currentStep": 0, "totalSteps": 3, "percent": 0, "label": "Idle"},
            }
        )

    return cleanup


def _assert_active_tasks_projection(payload: dict[str, object]) -> None:
    tasks = payload.get("tasks") or []
    task_types = {str(row.get("taskType") or "") for row in tasks}
    assert payload.get("count") == 4
    assert {"fetch", "discovery", "pipeline", "sync"} <= task_types
    fetch_row = _task_row(payload, "fetch")
    assert str((fetch_row.get("taskProgress") or {}).get("phaseKey") or "") == "executing_sources"
    pipeline_row = _task_row(payload, "pipeline")
    assert (
        str((pipeline_row.get("taskProgress") or {}).get("phaseLabel") or "") == "Running fetch..."
    )


def _setup_finished_reports_clear_stale_state() -> None:
    started_at = "2026-03-08T10:00:30.000Z"
    finished_at = "2026-03-08T10:05:30.000Z"
    admin_bridge.save_json_atomic(
        admin_bridge.TASK_STATE_PATH,
        {
            "fetch": _task_state_entry("fetch", run_id="fetch_1", started_at=started_at),
            "discovery": _task_state_entry(
                "discovery",
                run_id="discovery_1",
                started_at=started_at,
                pid=222,
            ),
        },
    )
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_REPORT_PATH,
        _fetch_report(
            run_id="fetch_1",
            started_at=started_at,
            finished_at=finished_at,
            task_progress=_completed_progress("Fetcher completed"),
            summary={"outputCount": 10, "failedSources": 1, "sourceCount": 10},
        ),
    )
    admin_bridge.save_json_atomic(
        admin_bridge.DISCOVERY_REPORT_PATH,
        _discovery_report(
            run_id="discovery_1",
            started_at=started_at,
            finished_at=finished_at,
            task_progress=_completed_progress("Discovery completed"),
            summary={"queuedCandidateCount": 3},
        ),
    )


def _assert_finished_reports_clear_stale_state(payload: dict[str, object]) -> None:
    assert payload.get("count") == 0
    assert payload.get("tasks") == []
    assert admin_bridge.load_json_object(admin_bridge.TASK_STATE_PATH, {}) == {}


def _setup_heartbeat_gap_fetch() -> None:
    started_at = admin_bridge.now_iso()
    admin_bridge.save_json_atomic(
        admin_bridge.TASK_STATE_PATH,
        {
            "fetch": _task_state_entry("fetch", run_id="fetch_1", started_at=started_at),
        },
    )
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_REPORT_PATH,
        _fetch_report(
            run_id="fetch_1",
            started_at=started_at,
            runtime={"lifecycle": {"owner": "fetch_report", "heartbeatAt": ""}},
            task_progress={
                **_active_progress(
                    "executing_sources",
                    "Executing sources",
                    {"resolvedSources": 5, "sourceCount": 10},
                ),
                "active": False,
            },
            summary={"outputCount": 10, "failedSources": 1, "sourceCount": 10},
        ),
    )


def _assert_heartbeat_gap_fetch(payload: dict[str, object]) -> None:
    fetch_row = _task_row(payload, "fetch")
    assert payload.get("count") == 1
    assert fetch_row.get("active") is True
    assert str(fetch_row.get("status") or "") == "running"


def _setup_active_owner_over_finished_history() -> None:
    started_at = admin_bridge.now_iso()
    run_id = "fetch_live_1"
    admin_bridge.save_json_atomic(
        admin_bridge.TASK_STATE_PATH,
        {
            "fetch": _task_state_entry("fetch", run_id=run_id, started_at=started_at),
        },
    )
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_REPORT_PATH,
        _fetch_report(
            run_id=run_id,
            started_at=started_at,
            runtime={"lifecycle": {"owner": "fetch_report", "heartbeatAt": started_at}},
            task_progress=_active_progress(
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
            _history_row(
                row_id=run_id,
                run_id=run_id,
                status="warning",
                started_at=started_at,
                finished_at=admin_bridge.now_iso(),
                duration_ms=123,
                summary={"outputCount": 10, "failedSources": 1, "sourceCount": 10},
            )
        ],
    )


def _assert_active_owner_over_finished_history(payload: dict[str, object]) -> None:
    fetch_row = _task_row(payload, "fetch")
    assert fetch_row["active"] is True
    assert str(fetch_row.get("runId") or "") == "fetch_live_1"
    diagnostics = payload.get("diagnostics") or []
    assert any(
        str(item.get("code") or "") == "history_finished_while_owner_active" for item in diagnostics
    )


CURRENT_TASK_STATE_CASES = [
    pytest.param(
        _CurrentTaskStateCase(
            name="active-task-projection",
            setup=_setup_active_tasks_projection,
            assert_payload=_assert_active_tasks_projection,
        ),
        id="active-task-projection",
    ),
    pytest.param(
        _CurrentTaskStateCase(
            name="finished-reports-clear-stale-state",
            setup=_setup_finished_reports_clear_stale_state,
            assert_payload=_assert_finished_reports_clear_stale_state,
        ),
        id="finished-reports-clear-stale-state",
    ),
    pytest.param(
        _CurrentTaskStateCase(
            name="heartbeat-gap-keeps-fetch-visible",
            setup=_setup_heartbeat_gap_fetch,
            assert_payload=_assert_heartbeat_gap_fetch,
            pid_is_running=True,
        ),
        id="heartbeat-gap-keeps-fetch-visible",
    ),
    pytest.param(
        _CurrentTaskStateCase(
            name="active-owner-over-finished-history",
            setup=_setup_active_owner_over_finished_history,
            assert_payload=_assert_active_owner_over_finished_history,
        ),
        id="active-owner-over-finished-history",
    ),
]


@pytest.mark.parametrize("case", CURRENT_TASK_STATE_CASES, ids=lambda case: case.name)
def test_get_current_task_state_payload_cases(case: _CurrentTaskStateCase) -> None:
    cleanup = case.setup()
    try:
        if case.pid_is_running is None:
            payload = _current_task_payload()
        else:
            with mock.patch.object(
                admin_bridge, "pid_is_running", return_value=case.pid_is_running
            ):
                payload = _current_task_payload()
        case.assert_payload(payload)
    finally:
        if cleanup is not None:
            cleanup()
