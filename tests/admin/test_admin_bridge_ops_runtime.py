import io
import json
import os
from contextlib import redirect_stdout
from unittest import mock

from src import admin_bridge


def test_resolve_runtime_config_cli_env_precedence(admin_bridge_entrypoint_root):
    cfg = admin_bridge.resolve_runtime_config(
        [
            "--port",
            "9001",
            "--host",
            "127.0.0.9",
            "--data-dir",
            str(admin_bridge_entrypoint_root),
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
    )
    assert cfg.host == "127.0.0.9"
    assert cfg.port == 9001
    assert str(cfg.data_dir) == str(admin_bridge_entrypoint_root.resolve())
    assert cfg.log_format == "jsonl"
    assert cfg.log_level == "debug"
    assert cfg.owner_mode == "env-owner"


def test_resolve_runtime_config_env_defaults_when_cli_missing(admin_bridge_entrypoint_root):
    with (
        mock.patch.object(
            admin_bridge,
            "get_bridge_defaults",
            return_value={
                "host": "127.0.0.2",
                "port": 8878,
                "log_format": "human",
                "log_level": "info",
                "quiet_requests": False,
            },
        ),
        mock.patch.object(
            admin_bridge,
            "get_storage_defaults",
            return_value={"data_dir": admin_bridge_entrypoint_root / "from-file"},
        ),
    ):
        cfg = admin_bridge.resolve_runtime_config(
            [],
            env={
                "BALUFFO_BRIDGE_HOST": "0.0.0.0",
                "BALUFFO_BRIDGE_PORT": "9911",
                "BALUFFO_DATA_DIR": str(admin_bridge_entrypoint_root),
                "BALUFFO_BRIDGE_LOG_FORMAT": "jsonl",
                "BALUFFO_BRIDGE_LOG_LEVEL": "debug",
                "BALUFFO_BRIDGE_OWNER_MODE": "dev-supervisor",
                "BALUFFO_BRIDGE_OWNER_TOKEN": "token-123",
                "BALUFFO_BRIDGE_STARTED_BY": "unit-test",
                "BALUFFO_BRIDGE_OWNER_IDLE_TIMEOUT_S": "45",
            },
        )
    assert cfg.host == "0.0.0.0"
    assert cfg.port == 9911
    assert str(cfg.data_dir) == str(admin_bridge_entrypoint_root.resolve())
    assert cfg.log_format == "jsonl"
    assert cfg.log_level == "debug"
    assert cfg.owner_mode == "dev-supervisor"
    assert cfg.owner_token == "token-123"
    assert cfg.started_by == "unit-test"
    assert cfg.owner_idle_timeout_s == 45.0


def test_resolve_runtime_config_uses_file_defaults_when_env_missing(admin_bridge_entrypoint_root):
    with (
        mock.patch.object(
            admin_bridge,
            "get_bridge_defaults",
            return_value={
                "host": "127.0.0.5",
                "port": 9915,
                "log_format": "jsonl",
                "log_level": "debug",
                "quiet_requests": True,
            },
        ),
        mock.patch.object(
            admin_bridge,
            "get_storage_defaults",
            return_value={"data_dir": admin_bridge_entrypoint_root / "from-file"},
        ),
    ):
        cfg = admin_bridge.resolve_runtime_config([], env={})
    assert cfg.host == "127.0.0.5"
    assert cfg.port == 9915
    assert str(cfg.data_dir) == str((admin_bridge_entrypoint_root / "from-file").resolve())
    assert cfg.log_format == "jsonl"
    assert cfg.log_level == "debug"
    assert cfg.quiet_requests


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
    assert "desktopMode" in health
    assert bool(health["desktopMode"]) == bool(admin_bridge.RUNTIME_CONFIG.desktop_mode)
    assert "owner" in health
    assert str(health["owner"]["mode"] or "") == str(admin_bridge.RUNTIME_CONFIG.owner_mode or "")
    assert "kpis" in health
    assert "alerts" in health
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


def test_normalize_fetch_report_contract_sanitizes_minimal_payload(admin_bridge_entrypoint_root):
    payload = admin_bridge.normalize_fetch_report_contract(
        {
            "schemaVersion": "1.0",
            "startedAt": 123,
            "finishedAt": None,
            "summary": "bad",
            "sources": [{"name": "x", "status": "OK", "durationMs": "17"}],
        }
    )
    assert int(payload.get("schemaVersion") or 0) == 1
    assert str(payload.get("startedAt") or "") == "123"
    assert str(payload.get("finishedAt") or "") == ""
    assert isinstance(payload.get("summary"), dict)
    assert isinstance(payload.get("runtime"), dict)
    assert len(payload.get("sources") or []) == 1
    assert isinstance(payload.get("taskProgress"), dict)
    assert str(payload["taskProgress"].get("phaseKey") or "") == "executing_sources"
    row = payload["sources"][0]
    assert str(row.get("status") or "") == "ok"
    assert int(row.get("durationMs") or 0) == 17


def test_normalize_fetch_report_contract_keeps_blank_report_inactive(
    admin_bridge_entrypoint_root,
):
    payload = admin_bridge.normalize_fetch_report_contract({})
    task_progress = payload.get("taskProgress") or {}
    assert task_progress["active"] is False
    assert str(task_progress.get("phaseKey") or "") == ""
    assert str(task_progress.get("phaseLabel") or "") == ""


def test_normalize_fetch_report_contract_parses_stringified_detail_rows(
    admin_bridge_entrypoint_root,
):
    payload = admin_bridge.normalize_fetch_report_contract(
        {
            "sources": [
                {
                    "name": "lever_sources",
                    "status": "ok",
                    "details": [
                        "{'adapter': 'lever', 'studio': 'Jagex', 'name': 'Jagex (Lever)', 'status': 'ok', 'fetchedCount': 2, 'keptCount': 2, 'error': ''}"
                    ],
                }
            ]
        }
    )
    assert len(payload.get("sources") or []) == 1
    row = payload["sources"][0]
    details = row.get("details") or []
    assert len(details) == 1
    assert str(details[0].get("name") or "") == "Jagex (Lever)"


def test_normalize_fetch_report_contract_forces_completed_task_progress_when_finished(
    admin_bridge_entrypoint_root,
):
    payload = admin_bridge.normalize_fetch_report_contract(
        {
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
        }
    )
    assert payload["taskProgress"]["active"] is False
    assert str(payload["taskProgress"].get("phaseKey") or "") == "completed"
    assert str(payload["taskProgress"].get("phaseLabel") or "") == "Completed"
    assert float(payload["taskProgress"].get("ratio") or 0) == 1.0


def test_normalize_discovery_report_contract_derives_queued_count_from_candidates(
    admin_bridge_entrypoint_root,
):
    payload = admin_bridge.normalize_discovery_report_contract(
        {
            "summary": {"queuedCandidateCount": 0, "probedCandidateCount": 4},
            "runtime": {
                "totalDurationMs": "123",
                "stageTimingsMs": {"probe": "45"},
                "adapterTimings": [{"adapter": "greenhouse", "durationMs": "22", "queuedCount": 1}],
            },
            "candidates": [
                {"name": "A", "deferred": False},
                {"name": "B"},
                {"name": "C", "deferred": True},
            ],
        }
    )
    task_progress = payload.get("taskProgress") or {}
    assert str(task_progress.get("phaseKey") or "") == "starting"
    assert str(task_progress.get("mode") or "") == "indeterminate"
    counts = task_progress.get("counts") or {}
    assert int(counts.get("queuedCandidates") or 0) == 2
    assert int(counts.get("probedCandidates") or 0) == 4
    assert int((payload.get("summary") or {}).get("queuedCandidateCount") or 0) == 2
    assert int((payload.get("runtime") or {}).get("totalDurationMs") or 0) == 123
    assert (
        int((((payload.get("runtime") or {}).get("stageTimingsMs") or {}).get("probe")) or 0) == 45
    )
    assert (
        str((((payload.get("runtime") or {}).get("adapterTimings") or [])[0].get("adapter")) or "")
        == "greenhouse"
    )


def test_summarize_discovery_report_prefers_derived_queued_count(admin_bridge_entrypoint_root):
    summary, status = admin_bridge.summarize_discovery_report(
        {
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
        }
    )
    assert int(summary.get("queuedCandidateCount") or 0) == 2
    assert status == "ok"


def test_build_fetcher_args_retry_failed_is_deterministic_and_filters_unknown(
    admin_bridge_entrypoint_root,
):
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_REPORT_PATH,
        {
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
    )
    args, preset = admin_bridge.build_fetcher_args_from_payload({"preset": "retry_failed"})
    assert preset == "retry_failed"
    assert "--only-sources" in args
    idx = args.index("--only-sources")
    assert args[idx + 1] == "google_sheets,remote_ok"
    assert "--ignore-circuit-breaker" in args
    assert "--quiet" not in args
    assert "--fetch-strategy" in args
    assert "--adapter-http-concurrency" in args


def test_build_fetcher_args_retry_failed_omits_only_sources_when_no_known_failures(
    admin_bridge_entrypoint_root,
):
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_REPORT_PATH,
        {
            "sources": [
                {"name": "unknown_custom_source_a", "status": "error"},
                {"name": "unknown_custom_source_b", "status": "error"},
            ]
        },
    )
    args, preset = admin_bridge.build_fetcher_args_from_payload({"preset": "retry_failed"})
    assert preset == "retry_failed"
    assert "--only-sources" not in args
    assert "--ignore-circuit-breaker" in args
    assert "--quiet" not in args


def test_build_fetcher_args_accepts_cadence_and_strategy_overrides(admin_bridge_entrypoint_root):
    args, preset = admin_bridge.build_fetcher_args_from_payload(
        {
            "preset": "default",
            "fetchStrategy": "http",
            "adapterHttpConcurrency": 48,
            "respectSourceCadence": True,
            "hotSourceCadenceMinutes": 20,
            "coldSourceCadenceMinutes": 90,
        }
    )
    assert preset == "default"
    assert "--fetch-strategy" in args
    assert args[args.index("--fetch-strategy") + 1] == "http"
    assert "--adapter-http-concurrency" in args
    assert args[args.index("--adapter-http-concurrency") + 1] == "48"
    assert "--respect-source-cadence" in args
    assert "--hot-source-cadence-minutes" in args
    assert args[args.index("--hot-source-cadence-minutes") + 1] == "20"
    assert "--cold-source-cadence-minutes" in args
    assert args[args.index("--cold-source-cadence-minutes") + 1] == "90"


def test_build_fetcher_args_enables_social_by_default(admin_bridge_entrypoint_root):
    args, preset = admin_bridge.build_fetcher_args_from_payload({"preset": "default"})
    assert preset == "default"
    assert "--social-enabled" in args


def test_build_fetcher_args_allows_social_opt_out(admin_bridge_entrypoint_root):
    args, preset = admin_bridge.build_fetcher_args_from_payload(
        {
            "preset": "default",
            "socialEnabled": False,
        }
    )
    assert preset == "default"
    assert "--social-enabled" not in args


def test_build_fetcher_args_uncapped_bypasses_admin_caps_and_keeps_social(
    admin_bridge_entrypoint_root,
):
    args, preset = admin_bridge.build_fetcher_args_from_payload({"preset": "uncapped"})
    assert preset == "uncapped"
    assert "--force-refresh-all" in args
    assert "--ignore-circuit-breaker" in args
    assert "--max-workers" in args
    assert args[args.index("--max-workers") + 1] == "16"
    assert "--max-per-domain" in args
    assert args[args.index("--max-per-domain") + 1] == "6"
    assert "--static-detail-concurrency" in args
    assert args[args.index("--static-detail-concurrency") + 1] == "24"
    assert "--source-ttl-minutes" in args
    assert args[args.index("--source-ttl-minutes") + 1] == "0"
    assert "--circuit-breaker-failures" in args
    assert args[args.index("--circuit-breaker-failures") + 1] == "0"
    assert "--circuit-breaker-cooldown-minutes" in args
    assert args[args.index("--circuit-breaker-cooldown-minutes") + 1] == "0"
    assert "--adapter-http-concurrency" not in args
    assert "--social-enabled" in args


def test_sync_history_from_reports_discards_unfinished_fetch_without_run_id(
    admin_bridge_entrypoint_root,
):
    old_started = "2026-03-01T00:00:00+00:00"
    admin_bridge.save_json_atomic(
        admin_bridge.OPS_HISTORY_PATH,
        [
            {
                "type": "fetch",
                "status": "started",
                "startedAt": old_started,
                "finishedAt": "",
                "durationMs": 0,
                "summary": {},
            }
        ],
    )
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_REPORT_PATH,
        {
            "startedAt": old_started,
            "finishedAt": "",
            "summary": {"outputCount": 0, "failedSources": 0, "sourceCount": 0},
            "sources": [],
        },
    )
    old_ts = 1_700_000_000
    os.utime(admin_bridge.JOBS_FETCH_REPORT_PATH, (old_ts, old_ts))
    rows = admin_bridge.sync_history_from_reports()
    assert rows == []
    report = admin_bridge.load_json_object(admin_bridge.JOBS_FETCH_REPORT_PATH, {})
    assert str(report.get("finishedAt") or "") == ""


def test_sync_history_from_reports_does_not_finish_discovery_from_read_side(
    admin_bridge_entrypoint_root,
):
    old_started = "2026-03-01T00:00:00+00:00"
    admin_bridge.save_json_atomic(
        admin_bridge.DISCOVERY_REPORT_PATH,
        {
            "startedAt": old_started,
            "finishedAt": "",
            "summary": {
                "foundEndpointCount": 0,
                "probedCandidateCount": 0,
                "queuedCandidateCount": 0,
                "failedProbeCount": 0,
            },
            "candidates": [],
            "failures": [],
        },
    )
    old_ts = 1_700_000_000
    os.utime(admin_bridge.DISCOVERY_REPORT_PATH, (old_ts, old_ts))

    admin_bridge.sync_history_from_reports()

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


def test_run_background_script_uses_child_script_mode_when_frozen(admin_bridge_entrypoint_root):
    cfg = admin_bridge.RuntimeConfig(
        root=admin_bridge_entrypoint_root,
        data_dir=admin_bridge_entrypoint_root,
        host="127.0.0.1",
        port=8877,
        log_format="human",
        log_level="info",
        quiet_requests=True,
        desktop_mode=True,
    )
    admin_bridge.configure_runtime_paths(cfg)
    fake_proc = type("FakeProc", (), {"pid": 12345})()
    with (
        mock.patch.object(admin_bridge.sys, "frozen", True, create=True),
        mock.patch.object(admin_bridge.sys, "executable", "C:/tmp/Baluffo.exe"),
        mock.patch.object(admin_bridge.subprocess, "Popen", return_value=fake_proc) as popen_mock,
    ):
        admin_bridge.run_background_script("source_discovery.py", ["--mode", "dynamic"])
    command = popen_mock.call_args.args[0]
    assert command[:5] == [
        "C:/tmp/Baluffo.exe",
        "__child_script__",
        "--root",
        str(admin_bridge_entrypoint_root),
        "--script",
    ]
    assert "source_discovery.py" in command
    assert command[-2:] == ["--mode", "dynamic"]


def test_run_background_script_uses_unbuffered_python_for_live_logs(admin_bridge_entrypoint_root):
    cfg = admin_bridge.RuntimeConfig(
        root=admin_bridge_entrypoint_root,
        data_dir=admin_bridge_entrypoint_root,
        host="127.0.0.1",
        port=8877,
        log_format="human",
        log_level="info",
        quiet_requests=True,
        desktop_mode=False,
    )
    admin_bridge.configure_runtime_paths(cfg)
    fake_proc = type("FakeProc", (), {"pid": 24680})()
    with (
        mock.patch.object(admin_bridge.sys, "frozen", False, create=True),
        mock.patch.object(admin_bridge.sys, "executable", "C:/Python313/python.exe"),
        mock.patch.object(admin_bridge.subprocess, "Popen", return_value=fake_proc) as popen_mock,
    ):
        admin_bridge.run_background_script("source_discovery.py", ["--mode", "dynamic"])
    command = popen_mock.call_args.args[0]
    kwargs = popen_mock.call_args.kwargs
    assert command[:2] == ["C:/Python313/python.exe", "-u"]
    assert command[2] == str(admin_bridge_entrypoint_root / "src" / "source_discovery.py")
    assert command[-2:] == ["--mode", "dynamic"]
    assert kwargs["env"]["PYTHONUNBUFFERED"] == "1"
    assert kwargs["stdout"] is kwargs["stderr"]


def test_run_background_script_persists_run_id_in_task_state(admin_bridge_entrypoint_root):
    cfg = admin_bridge.RuntimeConfig(
        root=admin_bridge_entrypoint_root,
        data_dir=admin_bridge_entrypoint_root,
        host="127.0.0.1",
        port=8877,
        log_format="human",
        log_level="info",
        quiet_requests=True,
        desktop_mode=False,
    )
    admin_bridge.configure_runtime_paths(cfg)
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


def test_sync_history_from_reports_merges_fetch_launcher_and_report_rows_by_run_id(
    admin_bridge_entrypoint_root,
):
    run_id = "fetch_merge_1"
    started_at = "2026-03-01T00:00:00+00:00"
    finished_at = "2026-03-01T00:03:00+00:00"
    admin_bridge.save_json_atomic(
        admin_bridge.OPS_HISTORY_PATH,
        [
            {
                "id": run_id,
                "runId": run_id,
                "type": "fetch",
                "status": "started",
                "startedAt": started_at,
                "finishedAt": "",
                "durationMs": 0,
                "summary": {},
            }
        ],
    )
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_REPORT_PATH,
        {
            "runId": run_id,
            "startedAt": started_at,
            "finishedAt": finished_at,
            "summary": {"outputCount": 10, "failedSources": 1, "sourceCount": 5},
            "sources": [],
        },
    )

    rows = admin_bridge.sync_history_from_reports()
    matching = [row for row in rows if str(row.get("runId") or "") == run_id]
    assert len(matching) == 1
    assert str(matching[0].get("status") or "") == "warning"
    assert str(matching[0].get("finishedAt") or "") == finished_at


def test_sync_history_from_reports_collapses_duplicate_run_id_rows(
    admin_bridge_entrypoint_root,
):
    started_at = "2026-03-01T00:00:00+00:00"
    finished_at = "2026-03-01T00:03:00+00:00"
    run_id = "run_a"
    admin_bridge.save_json_atomic(
        admin_bridge.OPS_HISTORY_PATH,
        [
            {
                "id": run_id,
                "runId": run_id,
                "type": "fetch",
                "status": "warning",
                "startedAt": started_at,
                "finishedAt": finished_at,
                "durationMs": 1000,
                "summary": {"outputCount": 10, "failedSources": 1, "sourceCount": 5},
            },
            {
                "id": "run_b",
                "runId": run_id,
                "type": "fetch",
                "status": "warning",
                "startedAt": started_at,
                "finishedAt": finished_at,
                "durationMs": 1000,
                "summary": {"outputCount": 10, "failedSources": 1, "sourceCount": 5},
            },
        ],
    )
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_REPORT_PATH,
        {
            "runId": run_id,
            "startedAt": started_at,
            "finishedAt": finished_at,
            "summary": {"outputCount": 10, "failedSources": 1, "sourceCount": 5},
            "sources": [],
        },
    )

    rows = admin_bridge.sync_history_from_reports()
    matching = [
        row
        for row in rows
        if str(row.get("type") or "") == "fetch"
        and str(row.get("startedAt") or "") == started_at
        and str(row.get("finishedAt") or "") == finished_at
    ]
    assert len(matching) == 1
    assert str(matching[0].get("runId") or "") == run_id


def test_sync_history_from_reports_projects_run_id_row_only(
    admin_bridge_entrypoint_root,
):
    run_id = "fetch_enrich_1"
    started_at = admin_bridge.now_iso()
    admin_bridge.save_json_atomic(
        admin_bridge.OPS_HISTORY_PATH,
        [
            {
                "id": "legacy_fetch_started",
                "type": "fetch",
                "status": "started",
                "startedAt": started_at,
                "finishedAt": "",
                "durationMs": 0,
                "summary": {},
            }
        ],
    )
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_REPORT_PATH,
        {
            "runId": run_id,
            "startedAt": started_at,
            "finishedAt": "",
            "summary": {"outputCount": 0, "failedSources": 0, "sourceCount": 0},
            "sources": [],
        },
    )

    rows = admin_bridge.sync_history_from_reports()
    matching = [
        row
        for row in rows
        if str(row.get("type") or "") == "fetch" and str(row.get("startedAt") or "") == started_at
    ]
    assert len(matching) == 1
    assert str(matching[0].get("runId") or "") == run_id
    assert str(matching[0].get("status") or "").lower() == "started"


def test_sync_history_from_reports_discards_rows_without_run_id(
    admin_bridge_entrypoint_root,
):
    run_id = "fetch_stale_1"
    started_at = "2026-03-01T00:00:00+00:00"
    admin_bridge.save_json_atomic(
        admin_bridge.OPS_HISTORY_PATH,
        [
            {
                "id": run_id,
                "runId": run_id,
                "type": "fetch",
                "status": "started",
                "startedAt": started_at,
                "finishedAt": "",
                "durationMs": 0,
                "summary": {},
            },
            {
                "id": "legacy_duplicate",
                "type": "fetch",
                "status": "started",
                "startedAt": started_at,
                "finishedAt": "",
                "durationMs": 0,
                "summary": {},
            },
        ],
    )
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_REPORT_PATH,
        {
            "runId": run_id,
            "startedAt": started_at,
            "finishedAt": "",
            "summary": {"outputCount": 0, "failedSources": 0, "sourceCount": 0},
            "sources": [],
        },
    )
    old_ts = 1_700_000_000
    os.utime(admin_bridge.JOBS_FETCH_REPORT_PATH, (old_ts, old_ts))

    rows = admin_bridge.sync_history_from_reports()
    matching = [
        row
        for row in rows
        if str(row.get("type") or "") == "fetch" and str(row.get("startedAt") or "") == started_at
    ]
    assert len(matching) == 1
    assert str(matching[0].get("runId") or "") == run_id
    assert str(matching[0].get("status") or "").lower() == "error"
    assert (
        str((matching[0].get("summary") or {}).get("error") or "")
        == "owner_inactive_without_terminal_report"
    )
    assert all(str(row.get("runId") or "").strip() for row in rows)


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


def test_get_current_task_state_payload_projects_active_tasks(admin_bridge_entrypoint_root):
    started_at = admin_bridge.now_iso()
    admin_bridge.save_json_atomic(
        admin_bridge.TASK_STATE_PATH,
        {
            "fetch": {
                "runId": "fetch_1",
                "taskType": "fetch",
                "pid": 111,
                "script": "jobs_fetcher.py",
                "status": "running",
                "startedAt": started_at,
            },
            "discovery": {
                "runId": "discovery_1",
                "taskType": "discovery",
                "pid": 222,
                "script": "source_discovery.py",
                "status": "running",
                "startedAt": started_at,
            },
        },
    )
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_REPORT_PATH,
        {
            "runId": "fetch_1",
            "startedAt": started_at,
            "finishedAt": "",
            "taskProgress": {
                "active": True,
                "phaseKey": "executing_sources",
                "phaseLabel": "Executing sources",
                "mode": "determinate",
                "ratio": 0.5,
                "counts": {"resolvedSources": 5, "sourceCount": 10},
            },
            "summary": {"outputCount": 10, "failedSources": 1, "sourceCount": 10},
            "sources": [],
        },
    )
    admin_bridge.save_json_atomic(
        admin_bridge.DISCOVERY_REPORT_PATH,
        {
            "runId": "discovery_1",
            "startedAt": started_at,
            "finishedAt": "",
            "taskProgress": {
                "active": True,
                "phaseKey": "scanning_sources",
                "phaseLabel": "Scanning known careers pages",
                "mode": "indeterminate",
                "ratio": 0,
                "counts": {"queuedCandidates": 3},
            },
            "summary": {"queuedCandidateCount": 3},
            "candidates": [],
            "failures": [],
        },
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

    try:
        payload = admin_bridge.build_bridge_api(
            admin_bridge.RUNTIME_CONFIG
        ).get_current_task_state_payload()
        tasks = payload.get("tasks") or []
        task_types = {str(row.get("taskType") or "") for row in tasks}
        assert payload.get("count") == 4
        assert {"fetch", "discovery", "pipeline", "sync"} <= task_types
        fetch_row = next(row for row in tasks if str(row.get("taskType") or "") == "fetch")
        assert (
            str((fetch_row.get("taskProgress") or {}).get("phaseKey") or "") == "executing_sources"
        )
        pipeline_row = next(row for row in tasks if str(row.get("taskType") or "") == "pipeline")
        assert (
            str((pipeline_row.get("taskProgress") or {}).get("phaseLabel") or "")
            == "Running fetch..."
        )
    finally:
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


def test_get_current_task_state_payload_clears_stale_task_state_once_report_finishes(
    admin_bridge_entrypoint_root,
):
    started_at = "2026-03-08T10:00:30.000Z"
    finished_at = "2026-03-08T10:05:30.000Z"
    admin_bridge.save_json_atomic(
        admin_bridge.TASK_STATE_PATH,
        {
            "fetch": {
                "runId": "fetch_1",
                "taskType": "fetch",
                "pid": 111,
                "script": "jobs_fetcher.py",
                "status": "running",
                "startedAt": started_at,
            },
            "discovery": {
                "runId": "discovery_1",
                "taskType": "discovery",
                "pid": 222,
                "script": "source_discovery.py",
                "status": "running",
                "startedAt": started_at,
            },
        },
    )
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_REPORT_PATH,
        {
            "runId": "fetch_1",
            "startedAt": started_at,
            "finishedAt": finished_at,
            "taskProgress": {
                "active": False,
                "phaseKey": "completed",
                "phaseLabel": "Fetcher completed",
                "mode": "determinate",
                "ratio": 1,
                "counts": {},
            },
            "summary": {"outputCount": 10, "failedSources": 1, "sourceCount": 10},
            "sources": [],
        },
    )
    admin_bridge.save_json_atomic(
        admin_bridge.DISCOVERY_REPORT_PATH,
        {
            "runId": "discovery_1",
            "startedAt": started_at,
            "finishedAt": finished_at,
            "taskProgress": {
                "active": False,
                "phaseKey": "completed",
                "phaseLabel": "Discovery completed",
                "mode": "determinate",
                "ratio": 1,
                "counts": {},
            },
            "summary": {"queuedCandidateCount": 3},
            "candidates": [],
            "failures": [],
        },
    )

    payload = admin_bridge.build_bridge_api(
        admin_bridge.RUNTIME_CONFIG
    ).get_current_task_state_payload()
    assert payload.get("count") == 0
    assert payload.get("tasks") == []
    assert admin_bridge.load_json_object(admin_bridge.TASK_STATE_PATH, {}) == {}


def test_get_current_task_state_payload_prefers_active_fetch_owner_over_finished_history(
    admin_bridge_entrypoint_root,
):
    started_at = admin_bridge.now_iso()
    run_id = "fetch_live_1"
    admin_bridge.save_json_atomic(
        admin_bridge.TASK_STATE_PATH,
        {
            "fetch": {
                "runId": run_id,
                "taskType": "fetch",
                "pid": 111,
                "script": "jobs_fetcher.py",
                "status": "running",
                "startedAt": started_at,
            }
        },
    )
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_REPORT_PATH,
        {
            "runId": run_id,
            "startedAt": started_at,
            "finishedAt": "",
            "runtime": {"lifecycle": {"owner": "fetch_report", "heartbeatAt": started_at}},
            "taskProgress": {
                "active": True,
                "phaseKey": "executing_sources",
                "phaseLabel": "Executing sources",
                "mode": "determinate",
                "ratio": 0.5,
                "counts": {"resolvedSources": 5, "sourceCount": 10},
            },
            "summary": {"outputCount": 10, "failedSources": 1, "sourceCount": 10},
            "sources": [],
        },
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
            {
                "id": run_id,
                "runId": run_id,
                "type": "fetch",
                "status": "warning",
                "startedAt": started_at,
                "finishedAt": admin_bridge.now_iso(),
                "durationMs": 123,
                "summary": {"outputCount": 10, "failedSources": 1, "sourceCount": 10},
            }
        ],
    )

    payload = admin_bridge.build_bridge_api(
        admin_bridge.RUNTIME_CONFIG
    ).get_current_task_state_payload()
    fetch_row = next(row for row in (payload.get("tasks") or []) if row.get("taskType") == "fetch")
    assert fetch_row["active"] is True
    assert str(fetch_row.get("runId") or "") == run_id
    diagnostics = payload.get("diagnostics") or []
    assert any(
        str(item.get("code") or "") == "history_finished_while_owner_active" for item in diagnostics
    )
