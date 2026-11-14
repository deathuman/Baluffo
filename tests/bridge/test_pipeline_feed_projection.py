from __future__ import annotations

import threading
import time
from datetime import UTC, datetime
from typing import Any

from tests.helpers.mutation import append_and_return
from tests.helpers.pipeline_service_factory import make_pipeline_service


def _status() -> dict[str, Any]:
    return {
        "active": True,
        "runId": "pipeline_1",
        "stage": "starting",
        "progress": {},
        "startedAt": "2026-05-06T18:00:00Z",
        "finishedAt": "",
        "error": "",
        "baselineOutputCount": 10,
        "jobsPageLoadedCount": 10,
        "finalOutputCount": 0,
        "_stageLedger": [],
    }


def _projection_result() -> dict[str, Any]:
    return {
        "ok": True,
        "status": "published",
        "runId": "fetch_1",
        "feedRunId": "fetch_1",
        "generation": "gen_1",
        "publishedAt": "2026-05-06T18:20:00Z",
        "rowCount": 42,
        "sourceCount": 7,
        "mode": "sqlite",
        "unchanged": False,
        "error": "",
    }


def test_pipeline_worker_projects_feed_before_sync_and_records_completion_evidence() -> None:
    calls: list[str] = []
    finished_runs: list[dict[str, Any]] = []
    post_publish_payloads: list[dict[str, Any]] = []
    completion_notifications: list[dict[str, Any]] = []

    def project_feed(
        report: dict[str, Any],
        *,
        on_progress: Any = None,
    ) -> dict[str, Any]:
        assert report["runId"] == "fetch_1"
        calls.append("feed")
        if on_progress is not None:
            on_progress({"phase": "read", "detail": "complete", "rowCount": 42})
            on_progress(
                {
                    "phase": "stage",
                    "detail": "write_jobs",
                    "rowCount": 42,
                    "processedRowCount": 21,
                    "batchIndex": 1,
                    "batchCount": 2,
                }
            )
            on_progress(
                {
                    "phase": "publish",
                    "detail": "complete",
                    "generation": "gen_1",
                    "rowCount": 42,
                    "sourceCount": 7,
                }
            )
            on_progress({"phase": "export", "detail": "complete", "generation": "gen_1"})
        return _projection_result()

    def record_post_publish(payload: dict[str, Any]) -> dict[str, Any]:
        post_publish_payloads.append(payload)
        return {"projected": 0, "sweep": {}}

    def record_completion(payload: dict[str, Any]) -> dict[str, Any]:
        completion_notifications.append(payload)
        return {"notified": True}

    def sync_stage(_run_id: str) -> None:
        calls.append("sync")

    service = make_pipeline_service(
        pipeline_status=_status(),
        current_fetch_output_count=lambda: 42,
        start_fetcher_task=lambda _payload: {
            "started": True,
            "runId": "fetch_1",
            "startedAt": "2026-05-06T18:00:00Z",
        },
        project_feed=project_feed,
        finish_lifecycle_run=lambda run_id, task_type, **kwargs: append_and_return(
            finished_runs, {"runId": run_id, "taskType": task_type, **kwargs}, {}
        ),
        pipeline_post_publish_callback=record_post_publish,
        pipeline_completion_notifier=record_completion,
    )
    service._run_discovery_stage = lambda run_id: None  # type: ignore[method-assign]
    service._wait_for_child_report = lambda **_kwargs: {  # type: ignore[method-assign]
        "runId": "fetch_1",
        "startedAt": "2026-05-06T18:00:00Z",
        "finishedAt": "2026-05-06T18:20:00Z",
        "summary": {"outputCount": 42},
    }
    service._run_registry_conflict_adjudication_stage = lambda run_id: None  # type: ignore[method-assign]
    service._run_sync_push_stage = sync_stage  # type: ignore[assignment]

    service._run_worker("pipeline_1")

    assert calls == ["feed", "sync"]
    payload = service.get_status_payload()
    assert payload["stage"] == "completed"
    assert payload["fetchRunId"] == "fetch_1"
    assert payload["feedRunId"] == "fetch_1"
    assert payload["feedGeneration"] == "gen_1"
    assert payload["feedPublishedAt"] == "2026-05-06T18:20:00Z"
    assert payload["feedRowCount"] == 42
    assert payload["feedSourceCount"] == 7
    assert payload["feedMode"] == "sqlite"
    assert payload["feedProjectionStatus"] == "published"
    assert payload["feedUnchanged"] is False
    assert [entry["stage"] for entry in payload["_stageLedger"]] == [
        "fetch",
        "feed_projection",
        "completed",
    ]
    projection = payload["feedProjection"]
    assert projection["phase"] == "complete"
    assert projection["heartbeatAt"]
    assert [entry["phase"] for entry in projection["phases"]] == [
        "lock",
        "read",
        "stage",
        "publish",
        "export",
        "complete",
    ]
    assert projection["phases"][-1]["finishedAt"]
    assert projection["processedRowCount"] == 21
    assert payload["feedProjectionPhase"] == "complete"
    assert payload["feedProjectionHeartbeatAt"]
    summary = finished_runs[-1]["summary"]
    assert summary["fetchRunId"] == "fetch_1"
    assert summary["feedRunId"] == "fetch_1"
    assert summary["feedGeneration"] == "gen_1"
    assert summary["feedRowCount"] == 42
    assert summary["feedProjection"]["phase"] == "complete"
    assert post_publish_payloads[-1]["feedRunId"] == "fetch_1"
    assert post_publish_payloads[-1]["feedGeneration"] == "gen_1"
    assert completion_notifications[-1]["feedRunId"] == "fetch_1"
    assert completion_notifications[-1]["feedGeneration"] == "gen_1"


def test_pipeline_worker_does_not_complete_or_sync_when_feed_projection_fails() -> None:
    calls: list[str] = []
    failed_runs: list[dict[str, Any]] = []

    def project_feed(
        _report: dict[str, Any],
        *,
        on_progress: Any = None,
    ) -> dict[str, Any]:
        calls.append("feed")
        return {"ok": False, "error": "jobs feed publication evidence is incomplete"}

    def sync_stage(_run_id: str) -> None:
        calls.append("sync")

    service = make_pipeline_service(
        pipeline_status=_status(),
        current_fetch_output_count=lambda: 42,
        start_fetcher_task=lambda _payload: {
            "started": True,
            "runId": "fetch_1",
            "startedAt": "2026-05-06T18:00:00Z",
        },
        project_feed=project_feed,
        fail_lifecycle_run=lambda run_id, task_type, **kwargs: append_and_return(
            failed_runs, {"runId": run_id, "taskType": task_type, **kwargs}, {}
        ),
    )
    service._run_discovery_stage = lambda run_id: None  # type: ignore[method-assign]
    service._wait_for_child_report = lambda **_kwargs: {  # type: ignore[method-assign]
        "runId": "fetch_1",
        "startedAt": "2026-05-06T18:00:00Z",
        "finishedAt": "2026-05-06T18:20:00Z",
        "summary": {"outputCount": 42},
    }
    service._run_registry_conflict_adjudication_stage = lambda run_id: None  # type: ignore[method-assign]
    service._run_sync_push_stage = sync_stage  # type: ignore[assignment]

    service._run_worker("pipeline_1")

    assert calls == ["feed"]
    payload = service.get_status_payload()
    assert payload["stage"] == "error"
    assert payload["error"] == ("feed_projection: jobs feed publication evidence is incomplete")
    assert payload["feedRunId"] == ""
    assert payload["feedGeneration"] == ""
    assert payload["feedProjectionStatus"] == "failed"
    assert payload["feedProjectionPhase"] == "failed"
    assert payload["feedProjection"]["error"].startswith("feed_projection:")
    assert payload["fetchRunId"] == "fetch_1"
    summary = failed_runs[-1]["summary"]
    assert summary["error"] == "feed_projection: jobs feed publication evidence is incomplete"
    assert summary["fetchRunId"] == "fetch_1"


def test_feed_projection_exception_finalizes_after_heartbeat_stops() -> None:
    def project_feed(
        _report: dict[str, Any],
        *,
        on_progress: Any = None,
    ) -> dict[str, Any]:
        if on_progress is not None:
            on_progress({"phase": "read", "detail": "reading"})
        time.sleep(0.05)
        raise RuntimeError("boom")

    service = make_pipeline_service(project_feed=project_feed)
    service._status.update({"runId": "pipeline_1", "active": True, "_stageLedger": []})

    try:
        service._run_feed_projection_stage(
            run_id="pipeline_1",
            fetch_run_id="fetch_1",
            report={"runId": "fetch_1", "summary": {"outputCount": 42}},
        )
    except RuntimeError as exc:
        assert str(exc) == "feed_projection: boom"
    else:  # pragma: no cover - asserted above
        raise AssertionError("expected the projector exception to propagate")

    projection = service._status["feedProjection"]
    assert projection["phase"] == "failed"
    assert projection["error"] == "feed_projection: boom"
    assert not any(
        thread.name == "feed-projection-heartbeat-pipeline_1" and thread.is_alive()
        for thread in threading.enumerate()
    )


def test_feed_projection_stage_exposes_phase_labels_and_counters() -> None:
    labels: list[str] = []

    def project_feed(
        _report: dict[str, Any],
        *,
        on_progress: Any = None,
    ) -> dict[str, Any]:
        assert on_progress is not None
        on_progress(
            {
                "phase": "stage",
                "detail": "write_jobs",
                "rowCount": 10,
                "processedRowCount": 5,
                "batchIndex": 1,
                "batchCount": 2,
            }
        )
        labels.append(service._status["progress"]["label"])
        return _projection_result()

    service = make_pipeline_service(project_feed=project_feed)
    service._status.update({"runId": "pipeline_1", "active": True, "_stageLedger": []})

    service._run_feed_projection_stage(
        run_id="pipeline_1",
        fetch_run_id="fetch_1",
        report={"runId": "fetch_1", "summary": {"outputCount": 42}},
    )

    assert labels == ["Staging feed generation... 5/10 job rows"]
    assert service._status["feedProjection"]["processedRowCount"] == 5
    assert service._status["feedProjection"]["phases"][-1]["phase"] == "complete"
    assert service._status["feedProjectionPhaseLabel"] == "Feed generation published."


def test_feed_projection_heartbeat_updates_while_projector_blocks(
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(
        "src.bridge.pipeline_service_stages.FEED_PROJECTION_HEARTBEAT_INTERVAL_S",
        0.05,
    )
    started = threading.Event()
    release = threading.Event()

    def project_feed(
        _report: dict[str, Any],
        *,
        on_progress: Any = None,
    ) -> dict[str, Any]:
        if on_progress is not None:
            on_progress({"phase": "read", "detail": "reading"})
        started.set()
        release.wait(2.0)
        return _projection_result()

    service = make_pipeline_service(
        project_feed=project_feed,
        now_iso=lambda: datetime.now(UTC).isoformat(),
    )
    service._status.update({"runId": "pipeline_1", "active": True, "_stageLedger": []})
    errors: list[BaseException] = []

    def run_stage() -> None:
        try:
            service._run_feed_projection_stage(
                run_id="pipeline_1",
                fetch_run_id="fetch_1",
                report={"runId": "fetch_1", "summary": {"outputCount": 42}},
            )
        except BaseException as exc:  # pragma: no cover - surfaced by the assertion
            errors.append(exc)

    thread = threading.Thread(target=run_stage)
    thread.start()
    try:
        assert started.wait(1.0)
        first_heartbeat = service._status["feedProjectionHeartbeatAt"]
        deadline = time.monotonic() + 2.0
        changed = False
        while time.monotonic() < deadline:
            current = service._status["feedProjectionHeartbeatAt"]
            if current and current != first_heartbeat:
                changed = True
                break
            time.sleep(0.02)
        assert changed, "expected the feed projection heartbeat to advance"
    finally:
        release.set()
        thread.join(2.0)
    assert not errors


def test_feed_projection_stage_skips_staging_after_a_failed_fetch() -> None:
    calls: list[dict[str, Any]] = []

    def project_feed(
        report: dict[str, Any],
        *,
        on_progress: Any = None,
    ) -> dict[str, Any]:
        calls.append(report)
        return _projection_result()

    service = make_pipeline_service(project_feed=project_feed)
    service._status.update({"runId": "pipeline_1", "active": True, "_stageLedger": []})

    result = service._run_feed_projection_stage(
        run_id="pipeline_1",
        fetch_run_id="fetch_1",
        report={
            "runId": "fetch_1",
            "status": "error",
            "summary": {"error": "fetch failed", "outputCount": 0},
        },
    )

    assert result["skipped"] is True
    assert calls == []
    assert service._status["feedProjectionStatus"] == "skipped_fetch_failed"
    assert service._status["fetchRunId"] == "fetch_1"
    assert service._status["feedRunId"] == ""
