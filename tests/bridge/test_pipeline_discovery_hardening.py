from __future__ import annotations

import pytest

from tests.helpers.pipeline_service_factory import make_pipeline_service


def test_pipeline_discovery_stage_fails_on_terminal_error_report() -> None:
    terminal_report = {
        "runId": "discovery_failed_1",
        "startedAt": "2026-05-06T18:00:00Z",
        "finishedAt": "2026-05-06T18:01:00Z",
        "status": "error",
        "summary": {"error": "owner_inactive_without_terminal_report"},
    }
    service = make_pipeline_service(
        pipeline_status={"active": True, "runId": "pipeline_1"},
        trigger_discovery_task=lambda **_kwargs: (
            200,
            {
                "started": True,
                "runId": "discovery_failed_1",
                "startedAt": "2026-05-06T18:00:00Z",
            },
        ),
        load_runtime_evidence=lambda _path, _default=None: dict(terminal_report),
    )

    with pytest.raises(RuntimeError, match="owner_inactive_without_terminal_report"):
        service._run_discovery_stage("pipeline_1")  # noqa: SLF001
