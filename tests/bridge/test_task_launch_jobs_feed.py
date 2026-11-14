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

            assert runtime.current_generation() != generation
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
