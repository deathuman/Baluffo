from __future__ import annotations

import pytest

from tests.bridge.test_pipeline_service import _make_pipeline_service


def test_pipeline_child_boundaries_wrap_expected_operational_failures() -> None:
    service = _make_pipeline_service(
        wait_for_sync_completion=lambda _run_id, _timeout_s: (_ for _ in ()).throw(
            TimeoutError("sync timed out")
        ),
        trigger_discovery_task=lambda **_kwargs: (_ for _ in ()).throw(
            RuntimeError("discovery failed")
        ),
        start_fetcher_task=lambda _payload: (_ for _ in ()).throw(
            OSError("fetch launch io failed")
        ),
        start_sync_task=lambda _action, **_kwargs: (_ for _ in ()).throw(
            ValueError("sync payload invalid")
        ),
    )
    service.wait_for_report_completion = lambda **_kwargs: (_ for _ in ()).throw(  # type: ignore[method-assign]
        RuntimeError("wait failed")
    )

    with pytest.raises(RuntimeError, match="discovery_wait: wait failed"):
        service._wait_for_child_report(phase="discovery_wait")
    with pytest.raises(RuntimeError, match="sync_push: sync timed out"):
        service._wait_for_sync_push_row("sync_1")
    with pytest.raises(RuntimeError, match="discovery_launch: discovery failed"):
        service._trigger_discovery_child()
    with pytest.raises(RuntimeError, match="fetch_launch: fetch launch io failed"):
        service._start_fetch_child()
    with pytest.raises(RuntimeError, match="sync_push: sync payload invalid"):
        service._start_sync_push_child()


def test_pipeline_child_boundaries_do_not_swallow_programming_bugs() -> None:
    service = _make_pipeline_service(
        wait_for_sync_completion=lambda _run_id, _timeout_s: (_ for _ in ()).throw(
            TypeError("sync signature bug")
        ),
        trigger_discovery_task=lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("discovery invariant bug")
        ),
        start_fetcher_task=lambda _payload: (_ for _ in ()).throw(TypeError("fetch signature bug")),
        start_sync_task=lambda _action, **_kwargs: (_ for _ in ()).throw(
            AssertionError("sync invariant bug")
        ),
    )
    service.wait_for_report_completion = lambda **_kwargs: (_ for _ in ()).throw(  # type: ignore[method-assign]
        AssertionError("wait invariant bug")
    )

    with pytest.raises(AssertionError, match="wait invariant bug"):
        service._wait_for_child_report(phase="discovery_wait")
    with pytest.raises(TypeError, match="sync signature bug"):
        service._wait_for_sync_push_row("sync_1")
    with pytest.raises(AssertionError, match="discovery invariant bug"):
        service._trigger_discovery_child()
    with pytest.raises(TypeError, match="fetch signature bug"):
        service._start_fetch_child()
    with pytest.raises(AssertionError, match="sync invariant bug"):
        service._start_sync_push_child()
