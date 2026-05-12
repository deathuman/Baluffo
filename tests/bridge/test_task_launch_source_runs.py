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
from src.storage import BaluffoStore, SourceRuntimeStore
from tests.helpers.temp_paths import workspace_tmpdir


def _task_launch_api(
    data_dir: Path,
    *,
    source_runtime_store: Any,
    diagnostics: list[dict[str, Any]],
    save_json_atomic: Any | None = None,
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
            save_json_atomic=save_json_atomic or (lambda _path, _payload: None),
            task_state_lock=None,
            default_source_loaders=lambda: [],
            failed_source_names_from_latest_report=lambda _allowed: [],
            safe_int=lambda value, default, floor, ceil: max(
                floor, min(ceil, int(value or default))
            ),
            source_runtime_store=source_runtime_store,
            record_storage_diagnostic=lambda **fields: diagnostics.append(dict(fields)),
        ),
    )


def test_fetch_lifecycle_close_mirrors_source_runs() -> None:
    with workspace_tmpdir("task-launch-source-runs") as data_dir:
        with BaluffoStore(data_dir) as store:
            runtime = SourceRuntimeStore(
                store,
                now_iso=lambda: "2026-05-12T12:00:00+00:00",
            )
            diagnostics: list[dict[str, Any]] = []
            api = _task_launch_api(
                data_dir,
                source_runtime_store=lambda: runtime,
                diagnostics=diagnostics,
            )
            finished: list[dict[str, Any]] = []
            report = {
                "runId": "fetch_1",
                "startedAt": "2026-05-12T11:00:00+00:00",
                "finishedAt": "2026-05-12T11:02:00+00:00",
                "summary": {"outputCount": 2, "failedSources": 0, "sourceCount": 1},
                "sources": [
                    {
                        "name": "Studio A",
                        "status": "ok",
                        "adapter": "static",
                        "fetchStrategy": "http",
                        "studio": "Studio A",
                        "fetchedCount": 3,
                        "keptCount": 2,
                        "durationMs": 120,
                    }
                ],
            }

            closed = api._close_fetch_lifecycle_from_report(  # noqa: SLF001
                run_id="fetch_1",
                normalize_fetch_report_contract=lambda payload: payload,
                load_json_object=lambda _path, _default: report,
                finish_lifecycle_run=lambda run_id, task_type, **kwargs: (
                    finished.append({"runId": run_id, "taskType": task_type, **kwargs}) or {}
                ),
                fail_lifecycle_run=lambda *_args, **_kwargs: {},
            )

            assert closed is True
            assert runtime.source_runs(run_id="fetch_1")[0]["name"] == "Studio A"
            assert store.get_authority_modes()["sourceRuns"] == "sqlite"
            assert diagnostics[-1]["code"] == "source_runs_projection_match"
            assert finished[0]["terminal_reason"] == "completed"


def test_fetch_lifecycle_close_rolls_source_runs_back_on_shadow_failure() -> None:
    class FailingSourceRuntime:
        def __init__(self, store: BaluffoStore) -> None:
            self.store = store

        def upsert_source_runs(self, **_kwargs: Any) -> int:
            raise sqlite3.OperationalError("database is locked")

    with workspace_tmpdir("task-launch-source-runs-failure") as data_dir:
        with BaluffoStore(data_dir) as store:
            diagnostics: list[dict[str, Any]] = []
            api = _task_launch_api(
                data_dir,
                source_runtime_store=lambda: FailingSourceRuntime(store),
                diagnostics=diagnostics,
            )
            report = {
                "runId": "fetch_1",
                "finishedAt": "2026-05-12T11:02:00+00:00",
                "summary": {"outputCount": 0},
                "sources": [{"name": "Studio A", "status": "ok"}],
            }

            closed = api._close_fetch_lifecycle_from_report(  # noqa: SLF001
                run_id="fetch_1",
                normalize_fetch_report_contract=lambda payload: payload,
                load_json_object=lambda _path, _default: report,
                finish_lifecycle_run=lambda *_args, **_kwargs: {},
                fail_lifecycle_run=lambda *_args, **_kwargs: {},
            )

            assert closed is True
            assert store.get_authority_modes()["sourceRuns"] == "json"
            assert diagnostics[-1]["code"] == "source_runs_shadow_write_failed"


def test_fetch_lifecycle_close_compacts_report_when_source_runs_are_authoritative() -> None:
    saved_reports: list[dict[str, Any]] = []

    def save_json_atomic(path: Path, payload: Any) -> None:
        saved_reports.append(dict(payload))
        path.write_text("{}", encoding="utf-8")

    with workspace_tmpdir("task-launch-source-runs-compact") as data_dir:
        with BaluffoStore(data_dir) as store:
            runtime = SourceRuntimeStore(
                store,
                now_iso=lambda: "2026-05-12T12:00:00+00:00",
            )
            diagnostics: list[dict[str, Any]] = []
            api = _task_launch_api(
                data_dir,
                source_runtime_store=lambda: runtime,
                diagnostics=diagnostics,
                save_json_atomic=save_json_atomic,
            )
            report = {
                "runId": "fetch_compact_1",
                "startedAt": "2026-05-12T11:00:00+00:00",
                "finishedAt": "2026-05-12T11:02:00+00:00",
                "summary": {"outputCount": 2, "failedSources": 0, "sourceCount": 1},
                "sources": [
                    {
                        "name": "Studio A",
                        "status": "ok",
                        "adapter": "static",
                        "fetchedCount": 3,
                        "keptCount": 2,
                        "details": [{"url": "https://example.com/job/1"}],
                    }
                ],
            }

            closed = api._close_fetch_lifecycle_from_report(  # noqa: SLF001
                run_id="fetch_compact_1",
                normalize_fetch_report_contract=lambda payload: payload,
                load_json_object=lambda _path, _default: report,
                finish_lifecycle_run=lambda *_args, **_kwargs: {},
                fail_lifecycle_run=lambda *_args, **_kwargs: {},
            )

            assert closed is True
            compact_report = saved_reports[-1]
            assert "details" not in compact_report["sources"][0]
            archive_ref = compact_report["sourceRuns"]["sourceDetailsArchive"]
            assert archive_ref["path"].endswith("source-details.json.gz")
            assert (data_dir / "evidence-archive-manifest.json").exists()
            stored_row = runtime.source_runs(run_id="fetch_compact_1")[0]
            assert stored_row["evidenceRefs"]["sourceDetailsArchive"]["path"] == archive_ref["path"]
            assert diagnostics[-1]["code"] == "fetch_report_compacted"


def test_packaged_smoke_fetch_mode_exercises_source_run_closeout(monkeypatch: Any) -> None:
    monkeypatch.setenv("BALUFFO_PACKAGED_SMOKE_FETCH_MODE", "source-runs")
    saved_reports: list[dict[str, Any]] = []

    def save_json_atomic(path: Path, payload: Any) -> None:
        if path.name == "jobs-fetch-report.json":
            saved_reports.append(dict(payload))
        path.write_text("{}", encoding="utf-8")

    with workspace_tmpdir("task-launch-source-runs-smoke") as data_dir:
        with BaluffoStore(data_dir) as store:
            runtime = SourceRuntimeStore(
                store,
                now_iso=lambda: "2026-05-12T12:00:00+00:00",
            )
            diagnostics: list[dict[str, Any]] = []
            finished: list[dict[str, Any]] = []
            api = _task_launch_api(
                data_dir,
                source_runtime_store=lambda: runtime,
                diagnostics=diagnostics,
                save_json_atomic=save_json_atomic,
            )

            result = api.start_fetcher_task(
                {"preset": "default", "quiet": True, "socialEnabled": False},
                append_run_history=lambda row: row,
                normalize_fetch_report_contract=lambda payload: payload,
                prune_started_rows_for_type=lambda *_args, **_kwargs: None,
                run_background_script=lambda *_args, **_kwargs: (_ for _ in ()).throw(
                    AssertionError("smoke mode must not spawn the fetcher")
                ),
                save_json_atomic=save_json_atomic,
                schema_version=1,
                load_json_object=lambda _path, default: dict(default or {}),
                start_lifecycle_run=lambda **kwargs: kwargs,
                finish_lifecycle_run=lambda run_id, task_type, **kwargs: (
                    finished.append({"runId": run_id, "taskType": task_type, **kwargs}) or {}
                ),
                fail_lifecycle_run=lambda *_args, **_kwargs: {},
            )

            assert result["started"] is True
            assert result["smokeMode"] == "source-runs"
            assert finished[0]["terminal_reason"] == "completed"
            compact_report = saved_reports[-1]
            archive_ref = compact_report["sourceRuns"]["sourceDetailsArchive"]
            assert archive_ref["path"].endswith("source-details.json.gz")
            stored_row = runtime.source_runs(run_id=str(result["runId"]))[0]
            assert stored_row["details"][0]["url"] == "https://example.com/jobs/packaged-smoke"
            assert stored_row["evidenceRefs"]["sourceDetailsArchive"]["path"] == archive_ref["path"]
