from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from src.bridge.task_launch_api import (
    TaskLaunchApi,
    TaskLaunchDeps,
    TaskLaunchPaths,
    TaskLaunchRuntime,
)
from src.pipeline_io import serialize_rows_for_json, write_atomic_if_changed
from src.shared.json_io import read_json
from src.storage import BaluffoStore, JobRuntimeStore
from tests.helpers.temp_paths import workspace_tmpdir


def _task_launch_api(
    data_dir: Path,
    *,
    job_runtime_store: Any,
    diagnostics: list[dict[str, Any]],
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
            now_iso=lambda: "2026-05-12T12:00:00+00:00",
            bridge_log=lambda *_args, **_kwargs: None,
            load_json_object=lambda _path, default: dict(default or {}),
            save_json_atomic=lambda _path, _payload: None,
            task_state_lock=None,
            default_source_loaders=lambda: [],
            failed_source_names_from_latest_report=lambda _allowed: [],
            safe_int=lambda value, default, floor, ceil: max(
                floor, min(ceil, int(value or default))
            ),
            job_runtime_store=job_runtime_store,
            record_storage_diagnostic=lambda **fields: diagnostics.append(dict(fields)),
        ),
    )


def _job_row() -> dict[str, object]:
    return {
        "id": 1,
        "title": "Tools Programmer",
        "company": "Studio A",
        "city": "Amsterdam",
        "country": "Netherlands",
        "workType": "Hybrid",
        "contractType": "Full-time",
        "jobLink": "https://example.test/jobs/1",
        "sector": "Games",
        "profession": "Engineering",
        "source": "Studio A",
        "sourceJobId": "job-1",
        "status": "active",
        "sourceBundle": [
            {"sourceName": "Studio A", "sourceJobId": "job-1"},
            {"sourceName": "Studio A", "sourceJobId": "job-1"},
        ],
    }


def _close_fetch(api: TaskLaunchApi, report: dict[str, Any]) -> bool:
    return api._close_fetch_lifecycle_from_report(  # noqa: SLF001
        run_id=str(report["runId"]),
        normalize_fetch_report_contract=lambda payload: payload,
        load_json_object=lambda _path, _default: report,
        finish_lifecycle_run=lambda *_args, **_kwargs: {},
        fail_lifecycle_run=lambda *_args, **_kwargs: {},
    )


def test_fetch_lifecycle_close_shadow_writes_jobs_feed() -> None:
    rows = [_job_row()]
    with workspace_tmpdir("task-launch-jobs-feed") as data_dir:
        with BaluffoStore(data_dir) as store:
            store.set_authority_mode("jobsFeed", "shadow", reason="test-shadow")
            runtime = JobRuntimeStore(store, now_iso=lambda: "2026-05-12T12:00:00+00:00")
            diagnostics: list[dict[str, Any]] = []
            api = _task_launch_api(
                data_dir,
                job_runtime_store=lambda: runtime,
                diagnostics=diagnostics,
            )
            write_atomic_if_changed(
                data_dir / "jobs-unified.json",
                serialize_rows_for_json(rows, list(rows[0].keys())),
            )
            report = {
                "runId": "fetch_jobs_1",
                "finishedAt": "2026-05-12T12:00:00+00:00",
                "summary": {"outputCount": 1},
                "sources": [],
            }

            assert _close_fetch(api, report) is True

            assert runtime.current_rows() == rows
            assert store.get_authority_modes()["jobsFeed"] == "shadow"
            assert diagnostics[-1]["surface"] == "jobsFeed"
            assert diagnostics[-1]["code"] == "jobs_feed_projection_match"


def test_fetch_lifecycle_close_rolls_jobs_feed_back_on_shadow_failure() -> None:
    class FailingJobRuntime:
        def __init__(self, store: BaluffoStore) -> None:
            self.store = store

        def stage_feed(self, **_kwargs: Any) -> object:
            raise sqlite3.OperationalError("database is locked")

    with workspace_tmpdir("task-launch-jobs-feed-failure") as data_dir:
        with BaluffoStore(data_dir) as store:
            diagnostics: list[dict[str, Any]] = []
            api = _task_launch_api(
                data_dir,
                job_runtime_store=lambda: FailingJobRuntime(store),
                diagnostics=diagnostics,
            )
            write_atomic_if_changed(
                data_dir / "jobs-unified.json",
                serialize_rows_for_json([_job_row()], list(_job_row().keys())),
            )
            report = {
                "runId": "fetch_jobs_1",
                "finishedAt": "2026-05-12T12:00:00+00:00",
                "summary": {"outputCount": 1},
                "sources": [],
            }

            assert _close_fetch(api, report) is True

            assert store.get_authority_modes()["jobsFeed"] == "json"
            assert diagnostics[-1]["code"] == "jobs_feed_shadow_write_failed"


def test_fetch_lifecycle_close_writes_exports_when_jobs_feed_is_authoritative() -> None:
    rows = [
        {
            **_job_row(),
            "id": index,
            "title": f"Tools Programmer {index}",
            "sourceJobId": f"job-{index}",
        }
        for index in range(12)
    ]
    with workspace_tmpdir("task-launch-jobs-feed-sqlite") as data_dir:
        with BaluffoStore(data_dir) as store:
            store.set_authority_mode("jobsFeed", "sqlite", reason="test-cutover")
            runtime = JobRuntimeStore(store, now_iso=lambda: "2026-05-12T12:00:00+00:00")
            diagnostics: list[dict[str, Any]] = []
            api = _task_launch_api(
                data_dir,
                job_runtime_store=lambda: runtime,
                diagnostics=diagnostics,
            )
            write_atomic_if_changed(
                data_dir / "jobs-unified.json",
                serialize_rows_for_json(rows, list(rows[0].keys())),
            )
            report = {
                "runId": "fetch_jobs_1",
                "finishedAt": "2026-05-12T12:00:00+00:00",
                "summary": {"outputCount": 1},
                "sources": [],
            }

            assert _close_fetch(api, report) is True

            full_rows = read_json(data_dir / "jobs-unified.json", [])
            light_rows = read_json(data_dir / "jobs-unified-light.json", [])
            startup_rows = read_json(data_dir / "jobs-unified-startup.json", [])
            assert full_rows[0]["title"] == "Tools Programmer 0"
            assert full_rows[0]["sourceBundle"] == rows[0]["sourceBundle"]
            assert light_rows[0]["title"] == "Tools Programmer 0"
            assert "sourceBundle" not in light_rows[0]
            assert len(light_rows) == 12
            assert len(startup_rows) == 10
            assert startup_rows == light_rows[:10]
            assert not (data_dir / "jobs-unified.csv").exists()
            assert store.get_authority_modes()["jobsFeed"] == "sqlite"
            assert [row["code"] for row in diagnostics if row["surface"] == "jobsFeed"] == [
                "jobs_feed_sqlite_export_written",
                "jobs_feed_projection_match",
            ]


def test_fetch_lifecycle_close_rolls_back_jobs_feed_when_sqlite_export_fails(
    monkeypatch: Any,
) -> None:
    rows = [_job_row()]
    with workspace_tmpdir("task-launch-jobs-feed-export-failure") as data_dir:
        with BaluffoStore(data_dir) as store:
            store.set_authority_mode("jobsFeed", "sqlite", reason="test-cutover")
            runtime = JobRuntimeStore(store, now_iso=lambda: "2026-05-12T12:00:00+00:00")
            diagnostics: list[dict[str, Any]] = []
            api = _task_launch_api(
                data_dir,
                job_runtime_store=lambda: runtime,
                diagnostics=diagnostics,
            )
            write_atomic_if_changed(
                data_dir / "jobs-unified.json",
                serialize_rows_for_json(rows, list(rows[0].keys())),
            )

            def fail_export(*_args: Any, **_kwargs: Any) -> bool:
                raise OSError("disk full")

            monkeypatch.setattr(
                "src.bridge.task_launch_jobs_feed.write_atomic_if_changed", fail_export
            )
            report = {
                "runId": "fetch_jobs_1",
                "finishedAt": "2026-05-12T12:00:00+00:00",
                "summary": {"outputCount": 1},
                "sources": [],
            }

            assert _close_fetch(api, report) is True

            assert store.get_authority_modes()["jobsFeed"] == "json"
            assert diagnostics[-1]["code"] == "jobs_feed_sqlite_export_failed"


def test_failed_fetch_does_not_publish_jobs_feed_generation() -> None:
    with workspace_tmpdir("task-launch-jobs-feed-failed-fetch") as data_dir:
        with BaluffoStore(data_dir) as store:
            runtime = JobRuntimeStore(store, now_iso=lambda: "2026-05-12T12:00:00+00:00")
            diagnostics: list[dict[str, Any]] = []
            api = _task_launch_api(
                data_dir,
                job_runtime_store=lambda: runtime,
                diagnostics=diagnostics,
            )
            write_atomic_if_changed(
                data_dir / "jobs-unified.json",
                serialize_rows_for_json([_job_row()], list(_job_row().keys())),
            )
            report = {
                "runId": "fetch_jobs_failed",
                "finishedAt": "2026-05-12T12:00:00+00:00",
                "summary": {"outputCount": 1, "error": "failed"},
                "sources": [],
            }

            assert _close_fetch(api, report) is True

            assert runtime.current_rows() == []
            assert [row for row in diagnostics if row.get("surface") == "jobsFeed"] == []
