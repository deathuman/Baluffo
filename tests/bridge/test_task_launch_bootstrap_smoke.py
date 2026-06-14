from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.bridge.task_launch_api import (
    BOOTSTRAP_COVERAGE_SCOPE,
    TaskLaunchApi,
    TaskLaunchDeps,
    TaskLaunchPaths,
    TaskLaunchRuntime,
)
from src.shared.json_io import read_json
from tests.helpers.temp_paths import workspace_tmpdir


def _save_json_atomic(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _task_launch_api(
    data_dir: Path,
    *,
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
            pid_is_running=pid_is_running or (lambda _pid: False),
        ),
    )


def test_packaged_smoke_heartbeat_bootstrap_mode_starts_controlled_running_report(
    monkeypatch: Any,
) -> None:
    monkeypatch.setenv("BALUFFO_PACKAGED_SMOKE_RUNTIME", "1")
    monkeypatch.setenv("BALUFFO_PACKAGED_SMOKE_BOOTSTRAP_MODE", "controlled-heartbeat-success")
    with workspace_tmpdir("task-launch-bootstrap-smoke-heartbeat-start") as data_dir:
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
            raise AssertionError("controlled heartbeat smoke must not spawn live fetcher")

        result = api.start_jobs_bootstrap_task(
            {"source": "jobs_first_run"},
            normalize_fetch_report_contract=lambda payload: payload,
            run_background_script=run_background_script,
            save_json_atomic=_save_json_atomic,
            schema_version=1,
            start_lifecycle_run=lambda **kwargs: lifecycle_rows.append(dict(kwargs)) or {},
            get_lifecycle_current_runs=lambda: [],
            get_lifecycle_run_history_rows=lambda: [],
        )

        assert result["started"] is True
        assert result["smokeMode"] == "controlled-heartbeat-success"
        assert watched and watched[-1]["run_id"] == result["runId"]
        assert lifecycle_rows and lifecycle_rows[-1]["owner_kind"] == "packaged_smoke"
        report = read_json(data_dir / "jobs-fetch-report.json", {})
        assert report["runId"] == result["runId"]
        assert not report.get("finishedAt")


def test_packaged_smoke_heartbeat_bootstrap_writes_running_heartbeats_and_terminal_staging(
    monkeypatch: Any,
) -> None:
    monkeypatch.setenv("BALUFFO_PACKAGED_SMOKE_RUNTIME", "1")
    monkeypatch.setenv("BALUFFO_PACKAGED_SMOKE_BOOTSTRAP_MODE", "controlled-heartbeat-success")
    monkeypatch.setenv("BALUFFO_PACKAGED_SMOKE_BOOTSTRAP_DELAY_MS", "25")
    monkeypatch.setenv("BALUFFO_PACKAGED_SMOKE_BOOTSTRAP_HEARTBEAT_MS", "5")
    with workspace_tmpdir("task-launch-bootstrap-smoke-heartbeat-terminal") as data_dir:
        api = _task_launch_api(data_dir)
        staging_dir = api._bootstrap_staging_dir("jobs_bootstrap_abcdef1234")  # noqa: SLF001
        staging_dir.mkdir(parents=True)
        saved_tasks: list[dict[str, Any]] = []

        def capture_save(path: Path, payload: Any) -> None:
            if path.name == "jobs-fetch-tasks.json":
                saved_tasks.append(dict(payload))
            _save_json_atomic(path, payload)

        api._complete_packaged_smoke_bootstrap_after_delay(  # noqa: SLF001
            run_id="jobs_bootstrap_abcdef1234",
            started_at="2026-05-17T12:00:00+00:00",
            staging_dir=staging_dir,
            schema_version=1,
            report_shell=api._bootstrap_report_shell(  # noqa: SLF001
                run_id="jobs_bootstrap_abcdef1234",
                started_at="2026-05-17T12:00:00+00:00",
                schema_version=1,
            ),
            normalize_fetch_report_contract=lambda payload: payload,
            save_json_atomic=capture_save,
        )

        assert any(row.get("taskProgress", {}).get("active") is True for row in saved_tasks)
        assert saved_tasks[-1]["taskProgress"]["active"] is False
        report = read_json(staging_dir / "jobs-fetch-report.json", {})
        fetcher_log = (data_dir / "jobs-fetcher.log").read_text(encoding="utf-8")
        rows = read_json(staging_dir / "jobs-unified-light.json", [])
        startup_rows = read_json(staging_dir / "jobs-unified-startup.json", [])
        assert report["summary"]["smokeMode"] == "controlled-heartbeat-success"
        assert report["summary"]["coverageScope"] == BOOTSTRAP_COVERAGE_SCOPE
        assert "Packaged smoke bootstrap heartbeat for jobs_bootstrap_abcdef1234" in fetcher_log
        assert rows[0]["title"] == "Packaged First-Run Technical Cinematic Animator"
        assert startup_rows == rows
