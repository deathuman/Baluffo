from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.bridge.task_launch_api import (
    BOOTSTRAP_COVERAGE_SCOPE,
    BOOTSTRAP_SHEET_SOURCE_NAMES,
    TaskLaunchApi,
    TaskLaunchDeps,
    TaskLaunchPaths,
    TaskLaunchRuntime,
)
from src.jobs.state_lifecycle import read_job_lifecycle_state, write_job_lifecycle_state
from src.jobs.state_source_records import read_source_state, write_source_state
from src.pipeline_io import write_atomic_if_changed
from src.shared.json_io import existing_json_candidate, read_json
from src.storage import BaluffoStore, JobRuntimeStore, SourceRuntimeStore
from tests.helpers.temp_paths import workspace_tmpdir


def _save_json_atomic(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _task_launch_api(
    data_dir: Path,
    *,
    source_runtime_store: Any | None = None,
    job_runtime_store: Any | None = None,
    diagnostics: list[dict[str, Any]] | None = None,
    pid_is_running: Any | None = None,
) -> TaskLaunchApi:
    return TaskLaunchApi(
        runtime=TaskLaunchRuntime(root=data_dir, data_dir=data_dir),
        paths=TaskLaunchPaths(
            discovery_log=data_dir / "source-discovery.log",
            discovery_report=data_dir / "source-discovery-report.json",
            fetcher_log=data_dir / "jobs-fetcher.log",
            task_state=data_dir / "admin-task-state.json",
            jobs_fetch_report=data_dir / "jobs-fetch-report.json",
            jobs_fetch_tasks=data_dir / "jobs-fetch-tasks.json",
            approval_state=data_dir / "source-approval-state.json",
        ),
        deps=TaskLaunchDeps(
            now_iso=lambda: "2026-05-17T12:00:00+00:00",
            bridge_log=lambda *_args, **_kwargs: None,
            load_json_object=lambda _path, default: dict(default or {}),
            save_json_atomic=_save_json_atomic,
            task_state_lock=None,
            default_source_loaders=lambda: [],
            failed_source_names_from_latest_report=lambda _allowed: [],
            safe_int=lambda value, default, floor, ceil: max(
                floor, min(ceil, int(value or default))
            ),
            source_runtime_store=source_runtime_store,
            job_runtime_store=job_runtime_store,
            pid_is_running=pid_is_running or (lambda _pid: False),
            record_storage_diagnostic=(
                (lambda **fields: diagnostics.append(dict(fields)))
                if diagnostics is not None
                else None
            ),
        ),
    )


def _write_bootstrap_artifacts(staging_dir: Path) -> None:
    rows = [
        {
            "id": "job-1",
            "title": "Tools Programmer",
            "company": "Studio A",
            "jobLink": "https://example.test/jobs/1",
            "sourceBundle": [],
        }
    ]
    write_atomic_if_changed(staging_dir / "jobs-unified.json", json.dumps(rows))
    write_atomic_if_changed(staging_dir / "jobs-unified-light.json", json.dumps(rows))
    write_atomic_if_changed(staging_dir / "jobs-unified.csv", "id,title\njob-1,Tools Programmer\n")
    _save_json_atomic(staging_dir / "jobs-fetch-report.json", _successful_bootstrap_report())


def _successful_bootstrap_report() -> dict[str, Any]:
    return {
        "runId": "jobs_bootstrap_test",
        "startedAt": "2026-05-17T12:00:00+00:00",
        "finishedAt": "2026-05-17T12:00:10+00:00",
        "runtime": {
            "seedFromExistingOutput": False,
            "incrementalCacheEnabled": False,
        },
        "summary": {
            "status": "ok",
            "outputCount": 1,
            "sourceCount": 3,
            "failedSources": 0,
        },
        "sources": [
            {
                "name": BOOTSTRAP_SHEET_SOURCE_NAMES[0],
                "status": "ok",
                "keptCount": 1,
            }
        ],
    }


def _write_successful_runtime_feed_shell(data_dir: Path) -> None:
    _save_json_atomic(
        data_dir / "jobs-fetch-report.json",
        {
            "runId": "jobs_fetch_existing",
            "startedAt": "2026-05-17T11:00:00+00:00",
            "finishedAt": "2026-05-17T11:00:10+00:00",
            "summary": {"status": "ok", "outputCount": 1},
        },
    )


def test_jobs_bootstrap_start_forces_isolated_targeted_fetch() -> None:
    with workspace_tmpdir("task-launch-bootstrap-start") as data_dir:
        api = _task_launch_api(data_dir)
        api._start_bootstrap_lifecycle_watch = lambda **_kwargs: None  # type: ignore[method-assign]  # noqa: SLF001
        captured: dict[str, Any] = {}

        def run_background_script(script: str, args: list[str], **kwargs: Any) -> int:
            captured["script"] = script
            captured["args"] = args
            captured["extra_env"] = dict(kwargs.get("extra_env") or {})
            return 1234

        result = api.start_jobs_bootstrap_task(
            normalize_fetch_report_contract=lambda payload: payload,
            run_background_script=run_background_script,
            save_json_atomic=_save_json_atomic,
            schema_version=1,
            start_lifecycle_run=lambda **_kwargs: {},
            get_lifecycle_current_runs=lambda: [],
            get_lifecycle_run_history_rows=lambda: [],
        )

        assert result["started"] is True
        assert result["task"] == "jobs_bootstrap"
        assert result["coverageScope"] == BOOTSTRAP_COVERAGE_SCOPE
        assert captured["script"] == "jobs_fetcher.py"
        assert "--no-seed-existing-output" in captured["args"]
        assert "--no-preserve-previous-on-empty" in captured["args"]
        assert "--force-refresh-all" in captured["args"]
        assert "--ignore-circuit-breaker" in captured["args"]
        assert captured["extra_env"]["BALUFFO_FETCH_SEED_EXISTING_OUTPUT"] == "0"
        only_sources = captured["args"][captured["args"].index("--only-sources") + 1]
        assert only_sources == ",".join(BOOTSTRAP_SHEET_SOURCE_NAMES)
        report = read_json(data_dir / "jobs-fetch-report.json", {})
        assert report["summary"]["coverageScope"] == BOOTSTRAP_COVERAGE_SCOPE


def test_packaged_smoke_bootstrap_mode_starts_controlled_running_report(
    monkeypatch: Any,
) -> None:
    monkeypatch.setenv("BALUFFO_PACKAGED_SMOKE_RUNTIME", "1")
    monkeypatch.setenv("BALUFFO_PACKAGED_SMOKE_BOOTSTRAP_MODE", "controlled-success")
    with workspace_tmpdir("task-launch-bootstrap-smoke-start") as data_dir:
        api = _task_launch_api(data_dir, pid_is_running=lambda _pid: True)
        watched: list[dict[str, Any]] = []
        lifecycle_rows: list[dict[str, Any]] = []
        api._start_bootstrap_lifecycle_watch = (  # type: ignore[method-assign]  # noqa: SLF001
            lambda **kwargs: watched.append(dict(kwargs))
        )
        api._complete_packaged_smoke_bootstrap_after_delay = (  # type: ignore[method-assign]  # noqa: SLF001
            lambda **_kwargs: None
        )

        def run_background_script(*_args: Any, **_kwargs: Any) -> int:
            raise AssertionError("controlled bootstrap smoke must not spawn live fetcher")

        result = api.start_jobs_bootstrap_task(
            normalize_fetch_report_contract=lambda payload: payload,
            run_background_script=run_background_script,
            save_json_atomic=_save_json_atomic,
            schema_version=1,
            start_lifecycle_run=lambda **kwargs: lifecycle_rows.append(dict(kwargs)) or {},
            get_lifecycle_current_runs=lambda: [],
            get_lifecycle_run_history_rows=lambda: [],
        )

        assert result["started"] is True
        assert result["smokeMode"] == "controlled-success"
        assert result["task"] == "jobs_bootstrap"
        assert result["coverageScope"] == BOOTSTRAP_COVERAGE_SCOPE
        assert watched and watched[-1]["run_id"] == result["runId"]
        assert lifecycle_rows and lifecycle_rows[-1]["owner_kind"] == "packaged_smoke"
        report = read_json(data_dir / "jobs-fetch-report.json", {})
        assert report["runId"] == result["runId"]
        assert not report.get("finishedAt")
        assert report["runtime"]["lifecycle"]["owner"] == "process"
        assert int(report["runtime"]["lifecycle"]["ownerPid"]) == int(result["pid"])
        assert report["summary"]["coverageScope"] == BOOTSTRAP_COVERAGE_SCOPE


def test_jobs_bootstrap_lifecycle_start_failure_still_tracks_running_process() -> None:
    with workspace_tmpdir("task-launch-bootstrap-lifecycle-start-fails") as data_dir:
        api = _task_launch_api(data_dir, pid_is_running=lambda pid: int(pid) == 1234)
        watched: list[dict[str, Any]] = []
        api._start_bootstrap_lifecycle_watch = (  # type: ignore[method-assign]  # noqa: SLF001
            lambda **kwargs: watched.append(dict(kwargs))
        )
        spawn_calls = 0

        def run_background_script(_script: str, _args: list[str], **_kwargs: Any) -> int:
            nonlocal spawn_calls
            spawn_calls += 1
            return 1234

        def fail_start_lifecycle(**_kwargs: Any) -> dict[str, Any]:
            raise RuntimeError("lifecycle store locked")

        result = api.start_jobs_bootstrap_task(
            normalize_fetch_report_contract=lambda payload: payload,
            run_background_script=run_background_script,
            save_json_atomic=_save_json_atomic,
            schema_version=1,
            start_lifecycle_run=fail_start_lifecycle,
            get_lifecycle_current_runs=lambda: [],
            get_lifecycle_run_history_rows=lambda: [],
        )
        duplicate = api.start_jobs_bootstrap_task(
            normalize_fetch_report_contract=lambda payload: payload,
            run_background_script=run_background_script,
            save_json_atomic=_save_json_atomic,
            schema_version=1,
            get_lifecycle_current_runs=lambda: [],
            get_lifecycle_run_history_rows=lambda: [],
        )

        assert result["started"] is True
        assert watched and watched[-1]["pid"] == 1234
        report = read_json(data_dir / "jobs-fetch-report.json", {})
        assert report["runtime"]["lifecycle"]["owner"] == "process"
        assert report["runtime"]["lifecycle"]["ownerPid"] == 1234
        assert duplicate["alreadyRunning"] is True
        assert duplicate["runId"] == result["runId"]
        assert spawn_calls == 1


def test_jobs_bootstrap_reattaches_from_non_terminal_report_without_lifecycle_row() -> None:
    with workspace_tmpdir("task-launch-bootstrap-report-reattach") as data_dir:
        api = _task_launch_api(data_dir, pid_is_running=lambda pid: int(pid) == 4321)
        _save_json_atomic(
            data_dir / "jobs-fetch-report.json",
            {
                "runId": "jobs_bootstrap_restarted",
                "startedAt": "2026-05-17T12:00:00+00:00",
                "finishedAt": "",
                "runtime": {
                    "coverageScope": BOOTSTRAP_COVERAGE_SCOPE,
                    "lifecycle": {
                        "owner": "process",
                        "ownerPid": 4321,
                        "heartbeatAt": "2026-05-17T12:00:01+00:00",
                    },
                },
                "summary": {"coverageScope": BOOTSTRAP_COVERAGE_SCOPE, "outputCount": 0},
            },
        )
        spawn_called = False

        def run_background_script(*_args: Any, **_kwargs: Any) -> int:
            nonlocal spawn_called
            spawn_called = True
            return 1234

        result = api.start_jobs_bootstrap_task(
            normalize_fetch_report_contract=lambda payload: payload,
            run_background_script=run_background_script,
            save_json_atomic=_save_json_atomic,
            schema_version=1,
            get_lifecycle_current_runs=lambda: [],
            get_lifecycle_run_history_rows=lambda: [],
        )

        assert result["started"] is False
        assert result["alreadyRunning"] is True
        assert result["runId"] == "jobs_bootstrap_restarted"
        assert result["pid"] == 4321
        assert spawn_called is False


def test_jobs_bootstrap_failure_report_survives_lifecycle_failure_write_error() -> None:
    with workspace_tmpdir("task-launch-bootstrap-failure-lifecycle-write") as data_dir:
        api = _task_launch_api(data_dir)
        staging_dir = api._bootstrap_staging_dir("jobs_bootstrap_test")  # noqa: SLF001
        staging_dir.mkdir(parents=True)
        _write_bootstrap_artifacts(staging_dir)
        seeded = _successful_bootstrap_report()
        seeded["runtime"]["seedFromExistingOutput"] = True
        _save_json_atomic(staging_dir / "jobs-fetch-report.json", seeded)

        def fail_lifecycle(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
            raise RuntimeError("lifecycle failure write locked")

        closed = api._close_bootstrap_from_staging(  # noqa: SLF001
            run_id="jobs_bootstrap_test",
            staging_dir=staging_dir,
            report_shell=api._bootstrap_report_shell(  # noqa: SLF001
                run_id="jobs_bootstrap_test",
                started_at="2026-05-17T12:00:00+00:00",
                schema_version=1,
            ),
            normalize_fetch_report_contract=lambda payload: payload,
            save_json_atomic=_save_json_atomic,
            finish_lifecycle_run=lambda *_args, **_kwargs: {},
            fail_lifecycle_run=fail_lifecycle,
        )

        report = read_json(data_dir / "jobs-fetch-report.json", {})
        assert closed is True
        assert report["summary"]["status"] == "error"
        assert "unexpectedly seeded" in report["summary"]["error"]


def test_jobs_bootstrap_rejects_after_full_pipeline_success() -> None:
    with workspace_tmpdir("task-launch-bootstrap-reject") as data_dir:
        api = _task_launch_api(data_dir)
        called = False

        def run_background_script(*_args: Any, **_kwargs: Any) -> int:
            nonlocal called
            called = True
            return 1234

        result = api.start_jobs_bootstrap_task(
            {"forceBootstrap": True, "source": "jobs_first_run"},
            normalize_fetch_report_contract=lambda payload: payload,
            run_background_script=run_background_script,
            save_json_atomic=_save_json_atomic,
            schema_version=1,
            get_lifecycle_run_history_rows=lambda: [
                {
                    "type": "pipeline",
                    "status": "ok",
                    "finishedAt": "2026-05-17T13:00:00+00:00",
                }
            ],
        )

        assert result["started"] is False
        assert result["alreadyCompleted"] is True
        assert result["error"] == "full_pipeline_already_completed"
        assert called is False


def test_jobs_bootstrap_rejects_when_runtime_feed_artifacts_are_loadable() -> None:
    with workspace_tmpdir("task-launch-bootstrap-runtime-feed-valid") as data_dir:
        api = _task_launch_api(data_dir)
        _write_successful_runtime_feed_shell(data_dir)
        write_atomic_if_changed(data_dir / "jobs-unified.json", '[{"id":"job-1"}]')
        write_atomic_if_changed(data_dir / "jobs-unified-light.json", '[{"id":"job-1"}]')
        write_atomic_if_changed(data_dir / "jobs-unified.csv", "id,title\njob-1,Tools Programmer\n")
        called = False

        def run_background_script(*_args: Any, **_kwargs: Any) -> int:
            nonlocal called
            called = True
            return 1234

        result = api.start_jobs_bootstrap_task(
            normalize_fetch_report_contract=lambda payload: payload,
            run_background_script=run_background_script,
            save_json_atomic=_save_json_atomic,
            schema_version=1,
        )

        assert result["started"] is False
        assert result["alreadyCompleted"] is True
        assert result["error"] == "runtime_feed_already_available"
        assert called is False


def test_jobs_bootstrap_first_run_force_does_not_bypass_successful_runtime_feed_guard() -> None:
    with workspace_tmpdir("task-launch-bootstrap-runtime-feed-first-run-force") as data_dir:
        api = _task_launch_api(data_dir)
        api._start_bootstrap_lifecycle_watch = lambda **_kwargs: None  # type: ignore[method-assign]  # noqa: SLF001
        _write_successful_runtime_feed_shell(data_dir)
        write_atomic_if_changed(data_dir / "jobs-unified.json", '[{"id":"job-1"}]')
        write_atomic_if_changed(data_dir / "jobs-unified-light.json", '[{"id":"job-1"}]')
        write_atomic_if_changed(data_dir / "jobs-unified.csv", "id,title\njob-1,Tools Programmer\n")
        called = False

        def run_background_script(*_args: Any, **_kwargs: Any) -> int:
            nonlocal called
            called = True
            return 1234

        result = api.start_jobs_bootstrap_task(
            {"forceBootstrap": True, "source": "jobs_first_run"},
            normalize_fetch_report_contract=lambda payload: payload,
            run_background_script=run_background_script,
            save_json_atomic=_save_json_atomic,
            schema_version=1,
        )

        assert result["started"] is False
        assert result["alreadyCompleted"] is True
        assert result["error"] == "runtime_feed_already_available"
        assert called is False


def test_jobs_bootstrap_internal_force_can_bypass_successful_runtime_feed_guard() -> None:
    with workspace_tmpdir("task-launch-bootstrap-runtime-feed-internal-force") as data_dir:
        api = _task_launch_api(data_dir)
        api._start_bootstrap_lifecycle_watch = lambda **_kwargs: None  # type: ignore[method-assign]  # noqa: SLF001
        _write_successful_runtime_feed_shell(data_dir)
        write_atomic_if_changed(data_dir / "jobs-unified.json", '[{"id":"job-1"}]')
        write_atomic_if_changed(data_dir / "jobs-unified-light.json", '[{"id":"job-1"}]')
        write_atomic_if_changed(data_dir / "jobs-unified.csv", "id,title\njob-1,Tools Programmer\n")
        called = False

        def run_background_script(*_args: Any, **_kwargs: Any) -> int:
            nonlocal called
            called = True
            return 1234

        result = api.start_jobs_bootstrap_task(
            {"forceBootstrap": True, "source": "packaged_smoke"},
            normalize_fetch_report_contract=lambda payload: payload,
            run_background_script=run_background_script,
            save_json_atomic=_save_json_atomic,
            schema_version=1,
        )

        assert result["started"] is True
        assert result["task"] == "jobs_bootstrap"
        assert called is True


def test_jobs_bootstrap_lifecycle_heartbeat_uses_fresh_owner_time() -> None:
    with workspace_tmpdir("task-launch-bootstrap-fresh-heartbeat") as data_dir:
        api = _task_launch_api(data_dir)
        staging_dir = api._bootstrap_staging_dir("jobs_bootstrap_test")  # noqa: SLF001
        staging_dir.mkdir(parents=True)
        _save_json_atomic(
            staging_dir / "jobs-fetch-tasks.json",
            {
                "runId": "jobs_bootstrap_test",
                "heartbeatAt": "2026-05-17T11:59:00+00:00",
                "taskProgress": {
                    "active": True,
                    "phaseKey": "normalizing_rows",
                    "updatedAt": "2026-05-17T11:59:00+00:00",
                },
                "summary": {"outputCount": 12},
            },
        )
        heartbeats: list[dict[str, Any]] = []

        def heartbeat_lifecycle_run(run_id: str, task_type: str, **kwargs: Any) -> dict[str, Any]:
            heartbeats.append({"runId": run_id, "taskType": task_type, **kwargs})
            return {}

        api._heartbeat_bootstrap_lifecycle_from_staging(  # noqa: SLF001
            run_id="jobs_bootstrap_test",
            staging_dir=staging_dir,
            heartbeat_lifecycle_run=heartbeat_lifecycle_run,
        )

        assert heartbeats[-1]["heartbeat_at"] == "2026-05-17T12:00:00+00:00"
        assert heartbeats[-1]["progress"]["updatedAt"] == "2026-05-17T12:00:00+00:00"
        assert heartbeats[-1]["progress"]["active"] is True
        assert heartbeats[-1]["summary"]["coverageScope"] == BOOTSTRAP_COVERAGE_SCOPE


def test_jobs_bootstrap_allows_recovery_when_successful_report_has_corrupt_rows() -> None:
    with workspace_tmpdir("task-launch-bootstrap-runtime-feed-corrupt") as data_dir:
        api = _task_launch_api(data_dir)
        api._start_bootstrap_lifecycle_watch = lambda **_kwargs: None  # type: ignore[method-assign]  # noqa: SLF001
        _write_successful_runtime_feed_shell(data_dir)
        write_atomic_if_changed(data_dir / "jobs-unified.json", "{")
        write_atomic_if_changed(data_dir / "jobs-unified-light.json", '[{"id":"job-1"}]')
        write_atomic_if_changed(data_dir / "jobs-unified.csv", "id,title\njob-1,Tools Programmer\n")
        called = False

        def run_background_script(*_args: Any, **_kwargs: Any) -> int:
            nonlocal called
            called = True
            return 1234

        result = api.start_jobs_bootstrap_task(
            normalize_fetch_report_contract=lambda payload: payload,
            run_background_script=run_background_script,
            save_json_atomic=_save_json_atomic,
            schema_version=1,
            get_lifecycle_current_runs=lambda: [],
            get_lifecycle_run_history_rows=lambda: [],
        )

        assert result["started"] is True
        assert result["task"] == "jobs_bootstrap"
        assert called is True


def test_jobs_bootstrap_validation_rejects_seeded_staged_report() -> None:
    with workspace_tmpdir("task-launch-bootstrap-validation") as data_dir:
        api = _task_launch_api(data_dir)
        staging_dir = api._bootstrap_staging_dir("jobs_bootstrap_test")  # noqa: SLF001
        staging_dir.mkdir(parents=True)
        _write_bootstrap_artifacts(staging_dir)
        report = _successful_bootstrap_report()
        report["runtime"]["seedFromExistingOutput"] = True

        error = api._validate_bootstrap_staging(  # noqa: SLF001
            staging_dir=staging_dir,
            report=report,
        )

        assert error == "bootstrap unexpectedly seeded existing output"


def test_jobs_bootstrap_promotes_non_empty_output_and_merges_state() -> None:
    with workspace_tmpdir("task-launch-bootstrap-promote") as data_dir:
        api = _task_launch_api(data_dir)
        staging_dir = api._bootstrap_staging_dir("jobs_bootstrap_test")  # noqa: SLF001
        staging_dir.mkdir(parents=True)
        _write_bootstrap_artifacts(staging_dir)
        write_source_state(
            data_dir / "jobs-source-state.json",
            {"non_sheet": {"name": "non_sheet", "consecutiveFailures": 2}},
        )
        write_source_state(
            staging_dir / "jobs-source-state.json",
            {
                BOOTSTRAP_SHEET_SOURCE_NAMES[0]: {
                    "name": BOOTSTRAP_SHEET_SOURCE_NAMES[0],
                    "consecutiveFailures": 1,
                },
                "non_sheet": {"name": "non_sheet", "consecutiveFailures": 99},
            },
        )
        write_job_lifecycle_state(
            data_dir / "jobs-lifecycle-state.json",
            {"existing-job": {"jobKey": "existing-job", "status": "active"}},
        )
        write_job_lifecycle_state(
            staging_dir / "jobs-lifecycle-state.json",
            {"job-1": {"jobKey": "job-1", "status": "active"}},
        )

        promoted = api._promote_bootstrap_output(  # noqa: SLF001
            staging_dir=staging_dir,
            report=_successful_bootstrap_report(),
            normalize_fetch_report_contract=lambda payload: payload,
            save_json_atomic=_save_json_atomic,
        )

        assert promoted["summary"]["coverageScope"] == BOOTSTRAP_COVERAGE_SCOPE
        assert promoted["runtime"]["coverageScope"] == BOOTSTRAP_COVERAGE_SCOPE
        assert len(read_json(data_dir / "jobs-unified.json", [])) == 1
        source_state = read_source_state(data_dir / "jobs-source-state.json")
        assert source_state["non_sheet"]["consecutiveFailures"] == 2
        assert source_state[BOOTSTRAP_SHEET_SOURCE_NAMES[0]]["consecutiveFailures"] == 1
        lifecycle_state = read_job_lifecycle_state(data_dir / "jobs-lifecycle-state.json")
        assert "existing-job" in lifecycle_state
        assert "job-1" in lifecycle_state


def test_jobs_bootstrap_restores_existing_feed_when_state_merge_fails() -> None:
    with workspace_tmpdir("task-launch-bootstrap-rollback-state") as data_dir:
        api = _task_launch_api(data_dir)
        staging_dir = api._bootstrap_staging_dir("jobs_bootstrap_test")  # noqa: SLF001
        staging_dir.mkdir(parents=True)
        _write_bootstrap_artifacts(staging_dir)
        write_atomic_if_changed(data_dir / "jobs-unified.json", '[{"id":"old-job"}]')
        write_atomic_if_changed(data_dir / "jobs-unified.csv", "id,title\nold-job,Old Role\n")
        write_source_state(
            data_dir / "jobs-source-state.json",
            {"non_sheet": {"name": "non_sheet", "consecutiveFailures": 2}},
        )

        def fail_merge(_staging_dir: Path) -> None:
            raise OSError("state locked")

        api._merge_bootstrap_state_artifacts = fail_merge  # type: ignore[method-assign]  # noqa: SLF001
        failed: list[dict[str, Any]] = []

        closed = api._close_bootstrap_from_staging(  # noqa: SLF001
            run_id="jobs_bootstrap_test",
            staging_dir=staging_dir,
            report_shell=api._bootstrap_report_shell(  # noqa: SLF001
                run_id="jobs_bootstrap_test",
                started_at="2026-05-17T12:00:00+00:00",
                schema_version=1,
            ),
            normalize_fetch_report_contract=lambda payload: payload,
            save_json_atomic=_save_json_atomic,
            finish_lifecycle_run=lambda *_args, **_kwargs: {},
            fail_lifecycle_run=lambda run_id, task_type, **kwargs: (
                failed.append({"runId": run_id, "taskType": task_type, **kwargs}) or {}
            ),
        )

        assert closed is True
        assert read_json(data_dir / "jobs-unified.json", [])[0]["id"] == "old-job"
        assert "old-job" in (data_dir / "jobs-unified.csv").read_text(encoding="utf-8")
        assert existing_json_candidate(data_dir / "jobs-unified-light.json") is None
        assert (
            read_source_state(data_dir / "jobs-source-state.json")["non_sheet"][
                "consecutiveFailures"
            ]
            == 2
        )
        report = read_json(data_dir / "jobs-fetch-report.json", {})
        assert report["summary"]["status"] == "error"
        assert "state locked" in report["summary"]["error"]
        assert failed[-1]["terminal_reason"] == "failed"


def test_jobs_bootstrap_restores_existing_feed_when_report_write_fails() -> None:
    with workspace_tmpdir("task-launch-bootstrap-rollback-report") as data_dir:
        api = _task_launch_api(data_dir)
        staging_dir = api._bootstrap_staging_dir("jobs_bootstrap_test")  # noqa: SLF001
        staging_dir.mkdir(parents=True)
        _write_bootstrap_artifacts(staging_dir)
        write_atomic_if_changed(data_dir / "jobs-unified.json", '[{"id":"old-job"}]')
        write_atomic_if_changed(data_dir / "jobs-unified.csv", "id,title\nold-job,Old Role\n")

        def fail_promoted_report(path: Path, payload: Any) -> None:
            summary = payload.get("summary") if isinstance(payload, dict) else {}
            if path.name == "jobs-fetch-report.json" and summary.get("status") != "error":
                raise OSError("report locked")
            _save_json_atomic(path, payload)

        closed = api._close_bootstrap_from_staging(  # noqa: SLF001
            run_id="jobs_bootstrap_test",
            staging_dir=staging_dir,
            report_shell=api._bootstrap_report_shell(  # noqa: SLF001
                run_id="jobs_bootstrap_test",
                started_at="2026-05-17T12:00:00+00:00",
                schema_version=1,
            ),
            normalize_fetch_report_contract=lambda payload: payload,
            save_json_atomic=fail_promoted_report,
            finish_lifecycle_run=lambda *_args, **_kwargs: {},
            fail_lifecycle_run=lambda *_args, **_kwargs: {},
        )

        assert closed is True
        assert read_json(data_dir / "jobs-unified.json", [])[0]["id"] == "old-job"
        assert "old-job" in (data_dir / "jobs-unified.csv").read_text(encoding="utf-8")
        assert existing_json_candidate(data_dir / "jobs-unified-light.json") is None
        report = read_json(data_dir / "jobs-fetch-report.json", {})
        assert report["summary"]["status"] == "error"
        assert "report locked" in report["summary"]["error"]


def test_jobs_bootstrap_restores_existing_feed_when_lifecycle_finish_fails() -> None:
    with workspace_tmpdir("task-launch-bootstrap-rollback-finish") as data_dir:
        api = _task_launch_api(data_dir)
        staging_dir = api._bootstrap_staging_dir("jobs_bootstrap_test")  # noqa: SLF001
        staging_dir.mkdir(parents=True)
        _write_bootstrap_artifacts(staging_dir)
        write_atomic_if_changed(data_dir / "jobs-unified.json", '[{"id":"old-job"}]')
        write_atomic_if_changed(data_dir / "jobs-unified.csv", "id,title\nold-job,Old Role\n")
        api._mirror_bootstrap_runtime_state = lambda _report: None  # type: ignore[method-assign]  # noqa: SLF001
        failed: list[dict[str, Any]] = []

        def fail_finish(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
            raise RuntimeError("lifecycle store locked")

        closed = api._close_bootstrap_from_staging(  # noqa: SLF001
            run_id="jobs_bootstrap_test",
            staging_dir=staging_dir,
            report_shell=api._bootstrap_report_shell(  # noqa: SLF001
                run_id="jobs_bootstrap_test",
                started_at="2026-05-17T12:00:00+00:00",
                schema_version=1,
            ),
            normalize_fetch_report_contract=lambda payload: payload,
            save_json_atomic=_save_json_atomic,
            finish_lifecycle_run=fail_finish,
            fail_lifecycle_run=lambda run_id, task_type, **kwargs: (
                failed.append({"runId": run_id, "taskType": task_type, **kwargs}) or {}
            ),
        )

        assert closed is True
        assert read_json(data_dir / "jobs-unified.json", [])[0]["id"] == "old-job"
        assert "old-job" in (data_dir / "jobs-unified.csv").read_text(encoding="utf-8")
        assert existing_json_candidate(data_dir / "jobs-unified-light.json") is None
        report = read_json(data_dir / "jobs-fetch-report.json", {})
        assert report["summary"]["status"] == "error"
        assert "lifecycle store locked" in report["summary"]["error"]
        assert failed[-1]["terminal_reason"] == "failed"


def test_jobs_bootstrap_restores_storage_mirror_when_lifecycle_finish_fails() -> None:
    with workspace_tmpdir("task-launch-bootstrap-rollback-storage") as data_dir:
        with BaluffoStore(data_dir) as store:
            store.set_authority_mode("sourceRuns", "sqlite", reason="test-cutover")
            store.set_authority_mode("jobsFeed", "sqlite", reason="test-cutover")
            source_runtime = SourceRuntimeStore(
                store,
                now_iso=lambda: "2026-05-17T11:00:00+00:00",
            )
            jobs_runtime = JobRuntimeStore(
                store,
                now_iso=lambda: "2026-05-17T11:00:00+00:00",
            )
            source_runtime.upsert_source_runs(
                run_id="previous_fetch",
                rows=[
                    {
                        "name": BOOTSTRAP_SHEET_SOURCE_NAMES[0],
                        "status": "ok",
                        "adapter": "old-adapter",
                        "keptCount": 7,
                    }
                ],
            )
            old_rows = [
                {
                    "id": "old-job",
                    "title": "Old Role",
                    "company": "Old Studio",
                    "jobLink": "https://example.test/jobs/old",
                    "sourceBundle": [],
                }
            ]
            jobs_runtime.replace_feed(
                run_id="previous_fetch",
                rows=old_rows,
                generation="previous-generation",
            )
            write_atomic_if_changed(data_dir / "jobs-unified.json", '[{"id":"old-job"}]')
            write_atomic_if_changed(data_dir / "jobs-unified.csv", "id,title\nold-job,Old Role\n")
            diagnostics: list[dict[str, Any]] = []
            api = _task_launch_api(
                data_dir,
                source_runtime_store=lambda: source_runtime,
                job_runtime_store=lambda: jobs_runtime,
                diagnostics=diagnostics,
            )
            staging_dir = api._bootstrap_staging_dir("jobs_bootstrap_test")  # noqa: SLF001
            staging_dir.mkdir(parents=True)
            _write_bootstrap_artifacts(staging_dir)

            def fail_finish(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
                raise RuntimeError("lifecycle store locked")

            closed = api._close_bootstrap_from_staging(  # noqa: SLF001
                run_id="jobs_bootstrap_test",
                staging_dir=staging_dir,
                report_shell=api._bootstrap_report_shell(  # noqa: SLF001
                    run_id="jobs_bootstrap_test",
                    started_at="2026-05-17T12:00:00+00:00",
                    schema_version=1,
                ),
                normalize_fetch_report_contract=lambda payload: payload,
                save_json_atomic=_save_json_atomic,
                finish_lifecycle_run=fail_finish,
                fail_lifecycle_run=lambda *_args, **_kwargs: {},
            )

            assert closed is True
            assert jobs_runtime.current_generation() == "previous-generation"
            assert jobs_runtime.current_rows()[0]["id"] == "old-job"
            assert (
                store.execute_read(
                    "SELECT COUNT(*) AS count FROM jobs WHERE run_id = ?",
                    ("jobs_bootstrap_test",),
                )[0]["count"]
                == 0
            )
            assert source_runtime.source_runs(run_id="jobs_bootstrap_test") == []
            assert (
                source_runtime.source_runs(run_id="previous_fetch")[0]["adapter"] == "old-adapter"
            )
            source_row = store.execute_read(
                "SELECT adapter FROM sources WHERE id = ?",
                ("fetch:google_sheets",),
            )[0]
            assert source_row["adapter"] == "old-adapter"
            assert store.get_authority_modes()["jobsFeed"] == "sqlite"
            assert store.get_authority_modes()["sourceRuns"] == "sqlite"
            assert any(row["code"] == "jobs_feed_projection_match" for row in diagnostics)
            report = read_json(data_dir / "jobs-fetch-report.json", {})
            assert report["summary"]["status"] == "error"
            assert "lifecycle store locked" in report["summary"]["error"]
