from __future__ import annotations

from typing import Any

from tests.helpers.mutation import append_and_return
from tests.helpers.pipeline_service_factory import make_pipeline_service


def test_pipeline_fetch_child_uses_bounded_container_profile(monkeypatch) -> None:
    payloads: list[dict[str, Any]] = []
    service = make_pipeline_service(
        container_mode=True,
        start_fetcher_task=lambda payload: append_and_return(
            payloads, dict(payload), {"runId": "fetch_1"}
        ),
    )

    monkeypatch.delenv("BALUFFO_CONTAINER_PIPELINE_FETCH_MAX_WORKERS", raising=False)
    service._start_fetch_child()

    assert payloads[-1] == {
        "preset": "default",
        "maxWorkers": 12,
        "maxPerDomain": 3,
        "adapterHttpConcurrency": 32,
        "staticDetailConcurrency": 6,
        "browserFallbackMaxWorkers": 4,
    }


def test_pipeline_fetch_child_clamps_container_profile_env(monkeypatch) -> None:
    payloads: list[dict[str, Any]] = []
    service = make_pipeline_service(
        container_mode=True,
        start_fetcher_task=lambda payload: append_and_return(
            payloads, dict(payload), {"runId": "fetch_1"}
        ),
    )

    monkeypatch.setenv("BALUFFO_CONTAINER_PIPELINE_FETCH_MAX_WORKERS", "99")
    monkeypatch.setenv("BALUFFO_CONTAINER_PIPELINE_BROWSER_FALLBACK_MAX_WORKERS", "99")
    service._start_fetch_child()
    monkeypatch.setenv("BALUFFO_CONTAINER_PIPELINE_FETCH_MAX_WORKERS", "bad")
    monkeypatch.setenv("BALUFFO_CONTAINER_PIPELINE_BROWSER_FALLBACK_MAX_WORKERS", "bad")
    service._start_fetch_child()

    assert payloads[0]["maxWorkers"] == 12
    assert payloads[0]["adapterHttpConcurrency"] == 32
    assert payloads[0]["browserFallbackMaxWorkers"] == 6
    assert payloads[1]["maxWorkers"] == 12
    assert payloads[1]["adapterHttpConcurrency"] == 32
    assert payloads[1]["browserFallbackMaxWorkers"] == 4


def test_pipeline_fetch_child_keeps_desktop_fetch_defaults_unmodified() -> None:
    payloads: list[dict[str, Any]] = []
    service = make_pipeline_service(
        container_mode=False,
        start_fetcher_task=lambda payload: append_and_return(
            payloads, dict(payload), {"runId": "fetch_1"}
        ),
    )

    service._start_fetch_child()

    assert payloads[-1] == {"preset": "default"}
