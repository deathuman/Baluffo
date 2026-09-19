from __future__ import annotations

from typing import Any

from tests.helpers.mutation import append_and_return
from tests.helpers.pipeline_service_factory import make_pipeline_service


def test_post_publish_callback_runs_once_for_short_success() -> None:
    calls: list[dict[str, Any]] = []
    service = make_pipeline_service(
        now_iso=lambda: "2026-05-06T19:00:01Z",
        pipeline_post_publish_callback=lambda payload: append_and_return(calls, payload, {}),
    )
    service._status.update(
        {
            "runId": "pipeline_short",
            "startedAt": "2026-05-06T19:00:00Z",
            "baselineOutputCount": 0,
            "jobsPageLoadedCount": 0,
        }
    )

    service._set_completed(status="ok", final_output_count=1)
    service._set_completed(status="ok", final_output_count=1)

    assert len(calls) == 1
    assert calls[0]["runId"] == "pipeline_short"
    assert calls[0]["status"] == "completed"


def test_post_publish_callback_skips_failed_and_canceled_runs() -> None:
    calls: list[dict[str, Any]] = []
    for run_id, status in (("pipeline_failed", "error"), ("pipeline_canceled", "canceled")):
        service = make_pipeline_service(
            pipeline_post_publish_callback=lambda payload: append_and_return(calls, payload, {}),
        )
        service._status.update(
            {
                "runId": run_id,
                "startedAt": "2026-05-06T18:59:00Z",
                "baselineOutputCount": 0,
                "jobsPageLoadedCount": 0,
            }
        )
        service._set_completed(status=status)

    assert calls == []
