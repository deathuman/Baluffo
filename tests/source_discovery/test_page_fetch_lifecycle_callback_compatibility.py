from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest

from src.shared.page_fetch_lifecycle import PageFetchLifecycle, PageFetchLifecycleContext
from src.source_discovery.directory_adapter_templates import (
    _accepts_page_fetch_lifecycle,
    run_directory_website_scan,
)
from src.source_discovery.directory_fetch import accepts_keyword
from src.source_discovery.directory_page_recovery import (
    DirectoryRecoveryRequest,
    DirectoryRecoveryResult,
    _merge_page_fetch_lifecycle_snapshots,
    fetch_recovery_jobs,
    run_recovery_for_requests,
)


def _scan(fetch_pages: Any, receipts: list[PageFetchLifecycle], **kwargs: Any) -> dict[str, Any]:
    return run_directory_website_scan(
        1,
        entries=[{"url": "https://example.test/"}],
        url_field="url",
        adapter="compatibility",
        failure_stage="fetch",
        fetcher=None,
        fetch_pages=fetch_pages,
        fetch_concurrency=1,
        per_host_concurrency=1,
        progress_label="homepage",
        analyze_result=lambda result: {},
        enable_recovery=False,
        recovery_analyze_result=lambda result, request: {},
        recovery_progress_label="recovery",
        unique_sources_fn=lambda rows: rows,
        batch_timing={},
        summary={},
        progress_cursor=0,
        write_cache=False,
        page_fetch_lifecycles=receipts,
        **kwargs,
    )


def _fetch_recovery(fetch_pages: Any, receipts: list[PageFetchLifecycle], **kwargs: Any):
    return fetch_recovery_jobs(
        1,
        [{"url": "https://example.test/careers"}],
        fetcher=None,
        total_concurrency=1,
        per_host_concurrency=1,
        progress_label="careers",
        recovery_cache={},
        fetch_pages=fetch_pages,
        page_fetch_lifecycles=receipts,
        **kwargs,
    )


def test_template_positional_only_callback_runs_without_fabricated_receipt() -> None:
    calls = []

    def fetch_pages(
        timeout_s,
        jobs,
        page_fetch_lifecycle=None,
        /,
        *,
        fetcher,
        total_concurrency,
        per_host_concurrency,
        progress_label,
        progress_callback=None,
    ):
        calls.append(page_fetch_lifecycle)
        return []

    receipts: list[PageFetchLifecycle] = []
    result = _scan(fetch_pages, receipts)
    assert calls == [None]
    assert receipts == []
    assert "pageFetchLifecycle" not in result


def test_collector_enabled_legacy_fetch_recovery_has_no_fabricated_receipt() -> None:
    calls = []

    def fetch_pages(
        timeout_s,
        jobs,
        *,
        fetcher,
        total_concurrency,
        per_host_concurrency,
        progress_label,
        progress_callback,
    ):
        calls.append(jobs)
        return [{"url": jobs[0]["url"], "ok": True, "text": "careers"}]

    receipts: list[PageFetchLifecycle] = []
    results, planned, fetched = _fetch_recovery(fetch_pages, receipts)
    assert len(calls) == 1
    assert (planned, fetched) == (1, 1)
    assert results[0]["text"] == "careers"
    assert receipts == []


def test_snapshot_merge_keeps_same_batch_in_distinct_scopes_and_encounter_order() -> None:
    snapshots = [
        {"runId": "run-a", "source": "source-a", "phase": "homepage", "batchId": "1"},
        {"runId": "run-b", "source": "source-a", "phase": "homepage", "batchId": "1"},
        {"runId": "run-a", "source": "source-b", "phase": "homepage", "batchId": "1"},
        {"runId": "run-a", "source": "source-a", "phase": "recovery", "batchId": "1"},
        {"batchId": "1"},
        {"legacy": True},
        {"legacy": True},
    ]
    merged = _merge_page_fetch_lifecycle_snapshots(snapshots[:3], snapshots[3:], snapshots[:5])
    assert merged == snapshots
    assert all(actual is not original for actual, original in zip(merged, snapshots, strict=True))


def _receiving_callback(kind: str, seen: list[PageFetchLifecycle]):
    def record(receipt):
        snapshot = receipt.snapshot()
        assert snapshot["runId"] == "run-a"
        assert snapshot["source"] == "source-a"
        assert snapshot["deadlineExit"] == "not_started"
        assert snapshot["submitted"] == 0
        seen.append(receipt)
        return []

    def normal(
        timeout_s,
        jobs,
        page_fetch_lifecycle=None,
        *,
        fetcher,
        total_concurrency,
        per_host_concurrency,
        progress_label,
        progress_callback=None,
    ):
        return record(page_fetch_lifecycle)

    def keyword_only(
        timeout_s,
        jobs,
        *,
        fetcher,
        total_concurrency,
        per_host_concurrency,
        progress_label,
        progress_callback=None,
        page_fetch_lifecycle=None,
    ):
        return record(page_fetch_lifecycle)

    def catch_all(timeout_s, jobs, **kwargs):
        return record(kwargs["page_fetch_lifecycle"])

    def positional_and_kwargs(timeout_s, jobs, page_fetch_lifecycle=None, /, **kwargs):
        assert page_fetch_lifecycle is None
        return record(kwargs["page_fetch_lifecycle"])

    return {
        "normal": normal,
        "keyword_only": keyword_only,
        "kwargs": catch_all,
        "positional_and_kwargs": positional_and_kwargs,
    }[kind]


@pytest.mark.parametrize("kind", ["normal", "keyword_only", "kwargs", "positional_and_kwargs"])
@pytest.mark.parametrize("route", ["template", "cache_recovery"])
def test_receivers_stamp_supported_callback_before_fetch(kind: str, route: str) -> None:
    context = PageFetchLifecycleContext(run_id="run-a", source="source-a")
    receipts: list[PageFetchLifecycle] = []
    seen: list[PageFetchLifecycle] = []
    callback = _receiving_callback(kind, seen)
    assert accepts_keyword(callback, "page_fetch_lifecycle")
    assert _accepts_page_fetch_lifecycle(callback)
    if route == "template":
        result = _scan(callback, receipts, page_fetch_context=context)
        assert result["pageFetchLifecycle"] == [receipt.snapshot() for receipt in receipts]
        phase = "directory_page_fetch"
    else:
        _fetch_recovery(
            callback, receipts, page_fetch_context=context, page_fetch_phase="http_recovery"
        )
        phase = "http_recovery"
    assert seen == receipts
    assert len(receipts) == 1
    assert receipts[0].snapshot()["phase"] == phase
    assert receipts[0].snapshot()["batchId"] == "1"
    assert receipts[0].snapshot()["deadlineExit"] == "not_started"


@pytest.mark.parametrize("signature_error", [TypeError, ValueError])
def test_unavailable_signature_runs_without_receipt(signature_error) -> None:
    calls = []

    class OpaqueCallback:
        @property
        def __signature__(self):
            raise signature_error("signature unavailable")

        def __call__(self, timeout_s, jobs, **kwargs):
            calls.append(kwargs)
            assert "page_fetch_lifecycle" not in kwargs
            return []

    callback = OpaqueCallback()
    context = PageFetchLifecycleContext(run_id="run-a", source="source-a")
    receipts: list[PageFetchLifecycle] = []
    assert not accepts_keyword(callback, "page_fetch_lifecycle")
    assert not _accepts_page_fetch_lifecycle(callback)
    _scan(callback, receipts, page_fetch_context=context)
    _fetch_recovery(callback, receipts, page_fetch_context=context)
    assert len(calls) == 2
    assert receipts == []
    assert context.new_receipt("careers").snapshot()["batchId"] == "1"


def test_context_has_read_only_identity_and_locked_per_phase_allocation() -> None:
    context = PageFetchLifecycleContext(run_id="run-a", source="source-a")
    assert (context.run_id, context.source) == ("run-a", "source-a")
    for attribute in ("run_id", "source"):
        with pytest.raises(AttributeError):
            setattr(context, attribute, "changed")
    with ThreadPoolExecutor(max_workers=8) as executor:
        receipts = list(executor.map(context.new_receipt, ["homepage"] * 64))
    assert sorted(int(str(receipt.snapshot()["batchId"])) for receipt in receipts) == list(
        range(1, 65)
    )
    assert all(receipt.snapshot()["runId"] == "run-a" for receipt in receipts)
    assert all(receipt.snapshot()["source"] == "source-a" for receipt in receipts)
    assert all(receipt.snapshot()["phase"] == "homepage" for receipt in receipts)
    assert context.new_receipt("recovery").snapshot()["batchId"] == "1"
    assert context.new_receipt("recovery").snapshot()["batchId"] == "2"
    receipts.clear()
    assert context.new_receipt("homepage").snapshot()["batchId"] == "65"
    standalone = PageFetchLifecycle().snapshot()
    assert all(standalone.get(field) is None for field in ("runId", "source", "phase"))


def _requests():
    return [
        DirectoryRecoveryRequest(
            key="studio",
            adapter="compatibility",
            discovery_method="directory",
            name="Studio",
            studio="studio",
            page_url="https://example.test/",
            html="",
        )
    ]


def test_context_is_not_blindly_forwarded_to_strict_legacy_runner() -> None:
    calls = []
    expected = DirectoryRecoveryResult()

    def runner(
        timeout_s,
        requests,
        *,
        fetcher,
        total_concurrency,
        per_host_concurrency,
        analyze_result,
        progress_label,
        url_limit,
    ):
        calls.append(requests)
        return expected

    result = run_recovery_for_requests(
        1,
        _requests(),
        fetcher=None,
        total_concurrency=1,
        per_host_concurrency=1,
        analyze_result=lambda result, request: ([], []),
        progress_label="careers",
        recovery_runner=runner,
        page_fetch_context=PageFetchLifecycleContext(run_id="run-a", source="source-a"),
    )
    assert result is expected
    assert len(calls) == 1


def test_builtin_nested_recovery_receives_context() -> None:
    context = PageFetchLifecycleContext(run_id="run-a", source="source-a")
    result = run_recovery_for_requests(
        1,
        _requests(),
        fetcher=lambda url, timeout_s: "<html>careers</html>",
        total_concurrency=1,
        per_host_concurrency=1,
        analyze_result=lambda result, request: ([], []),
        progress_label="careers",
        url_limit=1,
        page_fetch_context=context,
    )
    snapshots = [receipt.snapshot() for receipt in result.page_fetch_lifecycles]
    assert snapshots
    assert all(snapshot["runId"] == "run-a" for snapshot in snapshots)
    assert all(snapshot["source"] == "source-a" for snapshot in snapshots)
    assert all(
        str(snapshot["phase"]).startswith("careers recovery wave ") for snapshot in snapshots
    )
    assert all(snapshot["outstandingNow"] == snapshot["queuedNow"] == 0 for snapshot in snapshots)


def test_real_cache_fetch_is_stamped_before_worker_start_and_gate_is_released() -> None:
    context = PageFetchLifecycleContext(run_id="run-a", source="source-a")
    receipts: list[PageFetchLifecycle] = []
    started = threading.Event()
    release = threading.Event()

    def fetcher(url, timeout_s):
        started.set()
        assert release.wait(5), "test gate not released"
        return "<html>careers</html>"

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(
            fetch_recovery_jobs,
            3,
            [{"url": "https://example.test/careers"}],
            fetcher=fetcher,
            total_concurrency=1,
            per_host_concurrency=1,
            progress_label="careers",
            recovery_cache={},
            page_fetch_lifecycles=receipts,
            page_fetch_context=context,
        )
        try:
            assert started.wait(3), "fetch worker did not start"
            assert len(receipts) == 1
            snapshot = receipts[0].snapshot()
            assert (
                snapshot["runId"],
                snapshot["source"],
                snapshot["phase"],
                snapshot["batchId"],
            ) == ("run-a", "source-a", "careers", "1")
            assert snapshot["started"] == 1
            assert snapshot["finished"] == 0
        finally:
            release.set()
        results, planned, fetched = future.result(timeout=5)
    assert (planned, fetched) == (1, 1)
    assert results[0]["ok"]
    assert receipts[0].snapshot()["finished"] == 1
    assert receipts[0].snapshot()["outstandingNow"] == receipts[0].snapshot()["queuedNow"] == 0
