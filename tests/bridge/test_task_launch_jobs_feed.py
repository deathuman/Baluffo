from __future__ import annotations

import sqlite3
import threading
from pathlib import Path
from typing import Any

from src.bridge import task_launch_jobs_feed as jobs_feed_module
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


def _content_row() -> dict[str, object]:
    return {
        **_job_row(),
        "firstSeenAt": "2026-05-12T10:00:00+00:00",
        "lastSeenAt": "2026-05-12T12:00:00+00:00",
        "availabilityId": "availability_1",
        "availabilityStatus": "available",
        "availabilityCheckedAt": "2026-05-12T12:00:00+00:00",
        "availabilityVerifiedAt": "2026-05-12T12:00:00+00:00",
        "availabilityEvidence": {
            "kind": "source_present",
            "confidence": "definitive",
            "checkedAt": "2026-05-12T12:00:00+00:00",
            "source": "Studio A",
        },
    }


def _reobserved_content_row(row: dict[str, object]) -> dict[str, object]:
    evidence = row.get("availabilityEvidence")
    return {
        **row,
        "lastSeenAt": "2026-05-12T13:00:00+00:00",
        "availabilityCheckedAt": "2026-05-12T13:00:00+00:00",
        "availabilityVerifiedAt": "2026-05-12T13:00:00+00:00",
        "availabilityEvidence": {
            **(evidence if isinstance(evidence, dict) else {}),
            "checkedAt": "2026-05-12T13:00:00+00:00",
        },
    }


def _close_fetch(api: TaskLaunchApi, report: dict[str, Any]) -> bool:
    closed = api._close_fetch_lifecycle_from_report(  # noqa: SLF001
        run_id=str(report["runId"]),
        normalize_fetch_report_contract=lambda payload: payload,
        load_json_object=lambda _path, _default: report,
        finish_lifecycle_run=lambda *_args, **_kwargs: {},
        fail_lifecycle_run=lambda *_args, **_kwargs: {},
    )
    if closed:
        assert jobs_feed_module.wait_for_jobs_feed_generation_cleanup(
            api._runtime.data_dir  # noqa: SLF001
        )
    return closed


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


def test_fetch_finalization_skips_an_unchanged_content_generation() -> None:
    base_row = _content_row()
    with workspace_tmpdir("task-launch-jobs-feed-unchanged") as data_dir:
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
                serialize_rows_for_json([base_row], list(base_row.keys())),
            )
            first_report = {
                "runId": "fetch_jobs_1",
                "finishedAt": "2026-05-12T12:00:00+00:00",
                "summary": {"outputCount": 1},
                "sources": [],
            }

            assert _close_fetch(api, first_report) is True
            generation = runtime.current_generation()
            assert generation
            first_rows = runtime.current_rows()

            reobserved_row = _reobserved_content_row(base_row)
            write_atomic_if_changed(
                data_dir / "jobs-unified.json",
                serialize_rows_for_json([reobserved_row], list(reobserved_row.keys())),
            )
            second_report = {
                "runId": "fetch_jobs_2",
                "finishedAt": "2026-05-12T13:00:00+00:00",
                "summary": {"outputCount": 1},
                "sources": [],
            }

            assert _close_fetch(api, second_report) is True

            assert runtime.current_generation() == generation
            assert runtime.current_rows() == first_rows
            assert diagnostics[-1]["code"] == "jobs_feed_content_unchanged"
            assert store.execute_scalar("SELECT COUNT(*) FROM job_feed_generation_stats") == 1

            changed_row = {**reobserved_row, "title": "Senior Tools Programmer"}
            write_atomic_if_changed(
                data_dir / "jobs-unified.json",
                serialize_rows_for_json([changed_row], list(changed_row.keys())),
            )
            third_report = {
                "runId": "fetch_jobs_3",
                "finishedAt": "2026-05-12T14:00:00+00:00",
                "summary": {"outputCount": 1},
                "sources": [],
            }

            assert _close_fetch(api, third_report) is True

            assert runtime.current_generation() != generation
            assert diagnostics[-1]["code"] == "jobs_feed_projection_match"


def test_sqlite_fetch_finalization_skips_unchanged_content_without_rewriting_exports() -> None:
    base_row = _content_row()
    reobserved_row = _reobserved_content_row(base_row)
    projection_names = (
        "jobs-unified.json",
        "jobs-unified-light.json",
        "jobs-unified-startup.json",
    )
    with workspace_tmpdir("task-launch-jobs-feed-sqlite-unchanged") as data_dir:
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
                serialize_rows_for_json([base_row], list(base_row.keys())),
            )
            first_report = {
                "runId": "fetch_jobs_1",
                "finishedAt": "2026-05-12T12:00:00+00:00",
                "summary": {"outputCount": 1},
                "sources": [],
            }

            assert _close_fetch(api, first_report) is True
            generation = runtime.current_generation()
            assert generation
            first_rows = runtime.current_rows()
            assert diagnostics[-1]["code"] == "jobs_feed_projection_match"
            diagnostics.clear()

            # A fetch finalization writes all three projections before closeout.
            for name in projection_names:
                write_atomic_if_changed(
                    data_dir / name,
                    serialize_rows_for_json([reobserved_row], list(reobserved_row.keys())),
                )
            second_report = {
                "runId": "fetch_jobs_2",
                "finishedAt": "2026-05-12T13:00:00+00:00",
                "summary": {"outputCount": 1},
                "sources": [],
            }

            assert _close_fetch(api, second_report) is True

            assert runtime.current_generation() == generation
            assert runtime.current_rows() == first_rows
            assert [row["code"] for row in diagnostics] == ["jobs_feed_content_unchanged"]
            assert store.execute_scalar("SELECT COUNT(*) FROM job_feed_generation_stats") == 1
            assert store.get_authority_modes()["jobsFeed"] == "sqlite"
            for name in projection_names:
                assert read_json(data_dir / name, [])[0]["lastSeenAt"] == (
                    "2026-05-12T13:00:00+00:00"
                )


def test_sqlite_fetch_finalization_falls_back_to_staging_when_hash_read_fails(
    monkeypatch: Any,
) -> None:
    base_row = _content_row()
    reobserved_row = _reobserved_content_row(base_row)
    with workspace_tmpdir("task-launch-jobs-feed-sqlite-hash-failure") as data_dir:
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
                serialize_rows_for_json([base_row], list(base_row.keys())),
            )
            first_report = {
                "runId": "fetch_jobs_1",
                "finishedAt": "2026-05-12T12:00:00+00:00",
                "summary": {"outputCount": 1},
                "sources": [],
            }
            assert _close_fetch(api, first_report) is True
            generation = runtime.current_generation()
            assert generation
            diagnostics.clear()

            def fail_content_hash() -> str:
                raise sqlite3.OperationalError("database is locked")

            monkeypatch.setattr(runtime, "current_feed_content_hash", fail_content_hash)
            write_atomic_if_changed(
                data_dir / "jobs-unified.json",
                serialize_rows_for_json([reobserved_row], list(reobserved_row.keys())),
            )
            second_report = {
                "runId": "fetch_jobs_2",
                "finishedAt": "2026-05-12T13:00:00+00:00",
                "summary": {"outputCount": 1},
                "sources": [],
            }

            assert _close_fetch(api, second_report) is True

            assert runtime.current_generation() != generation, diagnostics
            assert runtime.current_rows()[0]["lastSeenAt"] == "2026-05-12T13:00:00+00:00"
            assert [row["code"] for row in diagnostics] == [
                "jobs_feed_sqlite_export_written",
                "jobs_feed_projection_match",
            ]
            assert (
                store.execute_scalar("SELECT feed_generation FROM job_feed_generation_stats")
                == runtime.current_generation()
            )


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


def test_project_jobs_feed_for_pipeline_returns_published_generation_evidence() -> None:
    base_row = _content_row()
    with workspace_tmpdir("task-launch-jobs-feed-pipeline-projection") as data_dir:
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
                serialize_rows_for_json([base_row], list(base_row.keys())),
            )

            progress: list[dict[str, Any]] = []
            first = api._project_jobs_feed_for_pipeline(  # noqa: SLF001
                {
                    "runId": "fetch_jobs_1",
                    "finishedAt": "2026-05-12T12:00:00+00:00",
                    "summary": {"outputCount": 1},
                    "sources": [],
                },
                on_progress=progress.append,
            )

            assert first["ok"] is True
            assert first["status"] == "published"
            assert first["skipped"] is False
            assert first["feedRunId"] == "fetch_jobs_1"
            assert first["generation"]
            assert first["publishedAt"] == "2026-05-12T12:00:00+00:00"
            assert first["rowCount"] == 1
            assert first["sourceCount"] >= 1
            assert first["mode"] == "sqlite"
            phases = [event["phase"] for event in progress]
            assert phases[0] == "lock"
            assert "read" in phases
            assert "stage" in phases
            assert "publish" in phases
            assert "export" in phases
            assert phases[-1] == "complete"
            assert any(
                event.get("detail") == "write_jobs" and event.get("processedRowCount") == 1
                for event in progress
            )
            generation = first["generation"]

            reobserved_row = _reobserved_content_row(base_row)
            write_atomic_if_changed(
                data_dir / "jobs-unified.json",
                serialize_rows_for_json([reobserved_row], list(reobserved_row.keys())),
            )
            second = api._project_jobs_feed_for_pipeline(  # noqa: SLF001
                {
                    "runId": "fetch_jobs_2",
                    "finishedAt": "2026-05-12T13:00:00+00:00",
                    "summary": {"outputCount": 1},
                    "sources": [],
                }
            )

            assert second["ok"] is True
            assert second["status"] == "unchanged"
            assert second["unchanged"] is True
            assert second["feedRunId"] == "fetch_jobs_1"
            assert second["generation"] == generation


def test_mirror_jobs_feed_rows_reports_cleanup_phase_progress() -> None:
    old_row = _content_row()
    new_row = {**old_row, "title": "Senior Tools Programmer"}
    with workspace_tmpdir("task-launch-jobs-feed-cleanup-progress") as data_dir:
        with BaluffoStore(data_dir) as store:
            store.set_authority_mode("jobsFeed", "shadow", reason="test-shadow")
            runtime = JobRuntimeStore(store, now_iso=lambda: "2026-05-12T12:00:00+00:00")
            diagnostics: list[dict[str, Any]] = []
            api = _task_launch_api(
                data_dir,
                job_runtime_store=lambda: runtime,
                diagnostics=diagnostics,
            )
            old = runtime.replace_feed(run_id="fetch_old", rows=[old_row])
            write_atomic_if_changed(
                data_dir / "jobs-unified.json",
                serialize_rows_for_json([new_row], list(new_row.keys())),
            )
            report = {
                "runId": "fetch_new",
                "finishedAt": "2026-05-12T13:00:00+00:00",
                "summary": {"outputCount": 1},
                "sources": [],
            }
            progress: list[dict[str, Any]] = []

            assert api._mirror_jobs_feed_rows(  # noqa: SLF001
                report,
                on_progress=progress.append,
                cleanup_in_background=False,
            )

            phases = [event["phase"] for event in progress]
            assert "cleanup" in phases
            cleanup_events = [event for event in progress if event["phase"] == "cleanup"]
            assert cleanup_events[-1]["detail"] == "complete"
            assert cleanup_events[-1]["deletedGenerationCount"] == 1
            assert runtime.rows_for_generation(old.generation) == []


def test_mirror_jobs_feed_rows_schedules_cleanup_without_blocking(
    monkeypatch: Any,
) -> None:
    old_row = _content_row()
    new_row = {**old_row, "title": "Senior Tools Programmer"}
    entered_cleanup = threading.Event()
    release_cleanup = threading.Event()
    cleanup_complete = threading.Event()
    cleanup_reported = threading.Event()

    def blocking_cleanup(
        _ctx: Any,
        *,
        keep_generations: int = 1,
        on_progress: Any = None,
        **_kwargs: Any,
    ) -> int:
        entered_cleanup.set()
        assert release_cleanup.wait(2.0)
        cleanup_complete.set()
        return 1

    monkeypatch.setattr(
        jobs_feed_module,
        "cleanup_jobs_feed_generations",
        blocking_cleanup,
    )
    with workspace_tmpdir("task-launch-jobs-feed-background-cleanup") as data_dir:
        with BaluffoStore(data_dir) as store:
            store.set_authority_mode("jobsFeed", "shadow", reason="test-shadow")
            runtime = JobRuntimeStore(store, now_iso=lambda: "2026-05-12T12:00:00+00:00")
            diagnostics: list[dict[str, Any]] = []
            api = _task_launch_api(
                data_dir,
                job_runtime_store=lambda: runtime,
                diagnostics=diagnostics,
            )
            runtime.replace_feed(run_id="fetch_old", rows=[old_row])
            write_atomic_if_changed(
                data_dir / "jobs-unified.json",
                serialize_rows_for_json([new_row], list(new_row.keys())),
            )
            report = {
                "runId": "fetch_new",
                "finishedAt": "2026-05-12T13:00:00+00:00",
                "summary": {"outputCount": 1},
                "sources": [],
            }
            progress: list[dict[str, Any]] = []

            def record_progress(update: dict[str, Any]) -> None:
                progress.append(update)
                if update.get("phase") == "cleanup" and update.get("detail") == "complete":
                    cleanup_reported.set()

            try:
                assert api._mirror_jobs_feed_rows(  # noqa: SLF001
                    report,
                    on_progress=record_progress,
                )
                assert entered_cleanup.wait(1.0)
                assert not cleanup_complete.is_set()
            finally:
                release_cleanup.set()

            assert cleanup_complete.wait(1.0)
            assert cleanup_reported.wait(1.0)
            assert jobs_feed_module.wait_for_jobs_feed_generation_cleanup(data_dir, timeout_s=2.0)
            assert any(
                event.get("phase") == "cleanup" and event.get("detail") == "complete"
                for event in progress
            )


def test_fetch_lifecycle_finishes_before_background_cleanup_completes(
    monkeypatch: Any,
) -> None:
    old_row = _content_row()
    new_row = {**old_row, "title": "Senior Tools Programmer"}
    entered_cleanup = threading.Event()
    release_cleanup = threading.Event()
    cleanup_complete = threading.Event()
    lifecycle_finished = threading.Event()
    finished_calls: list[dict[str, Any]] = []
    finished_generations: list[str] = []

    def blocking_cleanup(
        _ctx: Any,
        *,
        keep_generations: int = 1,
        on_progress: Any = None,
        **_kwargs: Any,
    ) -> int:
        entered_cleanup.set()
        assert release_cleanup.wait(2.0)
        cleanup_complete.set()
        return 1

    monkeypatch.setattr(
        jobs_feed_module,
        "cleanup_jobs_feed_generations",
        blocking_cleanup,
    )
    with workspace_tmpdir("task-launch-jobs-feed-lifecycle-publish-time") as data_dir:
        with BaluffoStore(data_dir) as store:
            store.set_authority_mode("jobsFeed", "shadow", reason="test-shadow")
            runtime = JobRuntimeStore(store, now_iso=lambda: "2026-05-12T12:00:00+00:00")
            diagnostics: list[dict[str, Any]] = []
            api = _task_launch_api(
                data_dir,
                job_runtime_store=lambda: runtime,
                diagnostics=diagnostics,
            )
            old = runtime.replace_feed(run_id="fetch_old", rows=[old_row])
            write_atomic_if_changed(
                data_dir / "jobs-unified.json",
                serialize_rows_for_json([new_row], list(new_row.keys())),
            )
            report = {
                "runId": "fetch_new",
                "finishedAt": "2026-05-12T13:00:00+00:00",
                "summary": {"outputCount": 1},
                "sources": [],
            }

            def finish_lifecycle(_run_id: str, _task_type: str, **kwargs: Any) -> dict[str, Any]:
                finished_calls.append(dict(kwargs))
                finished_generations.append(runtime.current_generation())
                lifecycle_finished.set()
                return {}

            try:
                assert api._close_fetch_lifecycle_from_report(  # noqa: SLF001
                    run_id="fetch_new",
                    normalize_fetch_report_contract=lambda payload: payload,
                    load_json_object=lambda _path, _default: report,
                    finish_lifecycle_run=finish_lifecycle,
                    fail_lifecycle_run=lambda *_args, **_kwargs: {},
                )
                assert lifecycle_finished.is_set()
                assert entered_cleanup.wait(1.0)
                assert not cleanup_complete.is_set()
            finally:
                release_cleanup.set()

            assert cleanup_complete.wait(1.0)
            assert jobs_feed_module.wait_for_jobs_feed_generation_cleanup(data_dir, timeout_s=2.0)
            assert finished_calls[0]["finished_at"] == report["finishedAt"]
            assert finished_calls[0]["terminal_reason"] == "completed"
            assert finished_generations[0] == runtime.current_generation()
            assert finished_generations[0] != old.generation
