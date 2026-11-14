from __future__ import annotations

import threading
from typing import Any

from src.bridge import task_launch_jobs_feed as jobs_feed_module
from src.bridge.feed_cleanup_telemetry import (
    get_feed_cleanup_telemetry,
    record_feed_cleanup_progress,
)
from src.pipeline_io import serialize_rows_for_json, write_atomic_if_changed
from src.storage import BaluffoStore, JobRuntimeStore
from tests.bridge.test_task_launch_jobs_feed import _content_row, _task_launch_api
from tests.helpers.pipeline_service_factory import make_pipeline_service
from tests.helpers.temp_paths import workspace_tmpdir


def test_pipeline_status_surfaces_background_cleanup_telemetry(
    monkeypatch: Any,
) -> None:
    old_row = _content_row()
    new_row = {**old_row, "title": "Senior Tools Programmer"}
    entered_cleanup = threading.Event()
    release_cleanup = threading.Event()

    def blocking_cleanup(
        _ctx: Any,
        *,
        keep_generations: int = 1,
        on_progress: Any = None,
        **_kwargs: Any,
    ) -> int:
        if on_progress is not None:
            on_progress(
                {
                    "phase": "cleanup",
                    "detail": "deleting",
                    "deletedGenerationCount": 0,
                    "deletedRowCount": 500,
                    "totalGenerationCount": 1,
                    "processedGenerationCount": 0,
                    "totalGenerationRowCount": 500,
                    "generationRowCountKnown": True,
                    "generation": "old-generation",
                }
            )
        entered_cleanup.set()
        assert release_cleanup.wait(2.0)
        return 1

    monkeypatch.setattr(
        jobs_feed_module,
        "cleanup_jobs_feed_generations",
        blocking_cleanup,
    )
    with workspace_tmpdir("task-launch-jobs-feed-cleanup-status") as data_dir:
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
            service = make_pipeline_service(
                pipeline_status={
                    "active": False,
                    "runId": "pipeline_1",
                    "fetchRunId": "fetch_new",
                    "stage": "completed",
                    "progress": {"label": "Pipeline completed"},
                    "feedProjection": {"phase": "complete"},
                    "feedProjectionPhase": "complete",
                },
                control_data_dir=data_dir,
            )

            try:
                assert api._mirror_jobs_feed_rows(report)  # noqa: SLF001
                assert entered_cleanup.wait(1.0)
                payload = service.get_status_payload()
                cleanup = payload["feedProjection"]["cleanup"]
                assert cleanup["phase"] == "cleanup"
                assert cleanup["detail"] == "deleting"
                assert cleanup["counts"]["deletedRowCount"] == 500
                assert cleanup["counts"]["totalGenerationCount"] == 1
                assert cleanup["heartbeatAt"]
                assert payload["feedProjectionPhase"] == "complete"
                assert payload["active"] is False
            finally:
                release_cleanup.set()

            assert jobs_feed_module.wait_for_jobs_feed_generation_cleanup(data_dir, timeout_s=2.0)
            payload = service.get_status_payload()
            cleanup = payload["feedProjection"]["cleanup"]
            assert cleanup["status"] == "completed"
            assert cleanup["counts"]["deletedGenerationCount"] == 1


def test_background_cleanup_failure_is_not_reported_as_complete(
    monkeypatch: Any,
) -> None:
    old_row = _content_row()
    new_row = {**old_row, "title": "Senior Tools Programmer"}

    def failing_cleanup(_self: Any, **_kwargs: Any) -> int:
        raise RuntimeError("cleanup boom")

    # Patch the runtime operation rather than ``cleanup_jobs_feed_generations``:
    # the regression was that its internal catch reported a failed cleanup as
    # complete. A direct fake that raises would bypass that catch and make the
    # test pass for the wrong reason.
    monkeypatch.setattr(JobRuntimeStore, "cleanup_old_generations", failing_cleanup)
    with workspace_tmpdir("task-launch-jobs-feed-cleanup-failure-status") as data_dir:
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
            service = make_pipeline_service(
                pipeline_status={
                    "active": False,
                    "runId": "pipeline_1",
                    "fetchRunId": "fetch_new",
                    "stage": "completed",
                    "progress": {"label": "Pipeline completed"},
                    "feedProjection": {"phase": "complete"},
                    "feedProjectionPhase": "complete",
                },
                control_data_dir=data_dir,
            )

            assert api._mirror_jobs_feed_rows(report)  # noqa: SLF001
            assert jobs_feed_module.wait_for_jobs_feed_generation_cleanup(data_dir, timeout_s=2.0)

            payload = service.get_status_payload()
            cleanup = payload["feedProjection"]["cleanup"]
            assert cleanup["status"] == "failed"
            assert cleanup["status"] != "completed"
            assert cleanup["detail"] == "failed"
            assert "cleanup boom" in cleanup.get("error", "")
            failed_heartbeat = cleanup["heartbeatAt"]

            record_feed_cleanup_progress(
                data_dir=data_dir,
                fetch_run_id="fetch_new",
                update={
                    "phase": "cleanup",
                    "detail": "heartbeat",
                    "deletedGenerationCount": 99,
                },
                now_iso=lambda: "2026-05-12T14:00:00+00:00",
            )
            cleanup = service.get_status_payload()["feedProjection"]["cleanup"]
            assert cleanup["status"] == "failed"
            assert cleanup["detail"] == "failed"
            assert cleanup["heartbeatAt"] == failed_heartbeat
            assert cleanup["counts"].get("deletedGenerationCount", 0) != 99


def test_cleanup_key_cache_preserves_equivalent_identities() -> None:
    with workspace_tmpdir("feed-cleanup-key-equivalence") as data_dir:
        now = lambda: "2026-05-12T13:00:00+00:00"  # noqa: E731
        record_feed_cleanup_progress(
            data_dir=data_dir,
            fetch_run_id=" fetch_1 ",
            update={"phase": "cleanup", "detail": "scheduled"},
            now_iso=now,
        )
        record_feed_cleanup_progress(
            data_dir=str(data_dir),
            fetch_run_id="fetch_1",
            update={
                "phase": "cleanup",
                "detail": "deleting",
                "deletedRowCount": 7,
            },
            now_iso=now,
        )

        by_path = get_feed_cleanup_telemetry(data_dir=data_dir, fetch_run_id="fetch_1")
        by_string = get_feed_cleanup_telemetry(data_dir=str(data_dir), fetch_run_id=" fetch_1 ")
        by_dot = get_feed_cleanup_telemetry(data_dir=data_dir / ".", fetch_run_id="fetch_1")
        assert by_path is not None
        assert by_path == by_string == by_dot
        assert by_path["status"] == "running"
        assert by_path["detail"] == "deleting"
        assert by_path["counts"]["deletedRowCount"] == 7

        record_feed_cleanup_progress(
            data_dir=data_dir / ".",
            fetch_run_id="fetch_1",
            update={
                "phase": "cleanup",
                "detail": "complete",
                "deletedGenerationCount": 1,
            },
            now_iso=now,
        )
        record_feed_cleanup_progress(
            data_dir=data_dir,
            fetch_run_id=" fetch_1 ",
            update={
                "phase": "cleanup",
                "detail": "heartbeat",
                "deletedRowCount": 999,
            },
            now_iso=lambda: "2026-05-12T14:00:00+00:00",
        )
        terminal = get_feed_cleanup_telemetry(data_dir=str(data_dir), fetch_run_id="fetch_1")
        assert terminal is not None
        assert terminal["status"] == "completed"
        assert terminal["detail"] == "complete"
        assert terminal["counts"]["deletedGenerationCount"] == 1
        assert terminal["counts"]["deletedRowCount"] == 7


def test_feed_cleanup_terminal_status_ignores_late_heartbeat() -> None:
    with workspace_tmpdir("feed-cleanup-late-heartbeat") as data_dir:
        now = lambda: "2026-05-12T13:00:00+00:00"  # noqa: E731
        record_feed_cleanup_progress(
            data_dir=data_dir,
            fetch_run_id="fetch_1",
            update={"phase": "cleanup", "detail": "scheduled"},
            now_iso=now,
        )
        record_feed_cleanup_progress(
            data_dir=data_dir,
            fetch_run_id="fetch_1",
            update={"phase": "cleanup", "detail": "complete", "deletedGenerationCount": 1},
            now_iso=now,
        )
        record_feed_cleanup_progress(
            data_dir=data_dir,
            fetch_run_id="fetch_1",
            update={"phase": "cleanup", "detail": "heartbeat", "deletedRowCount": 999},
            now_iso=now,
        )
        record_feed_cleanup_progress(
            data_dir=data_dir,
            fetch_run_id="fetch_1",
            update={"phase": "cleanup", "detail": "failed", "error": "late failure"},
            now_iso=now,
        )
        cleanup = get_feed_cleanup_telemetry(data_dir=data_dir, fetch_run_id="fetch_1")
        assert cleanup is not None
        assert cleanup["status"] == "completed"
        assert cleanup["detail"] == "complete"
        assert "error" not in cleanup
        assert cleanup["counts"]["deletedGenerationCount"] == 1
        assert cleanup["counts"].get("deletedRowCount", 0) == 0
