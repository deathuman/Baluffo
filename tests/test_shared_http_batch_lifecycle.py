from __future__ import annotations

import asyncio
import queue
import threading
import time
from typing import Any

from src.shared.http_batch import fetch_pages_batched
from src.shared.page_fetch_lifecycle import PageFetchLifecycle, rollup_page_fetch_lifecycles


def _batch_result(
    result_queue: queue.Queue[tuple[str, object]],
    lifecycle: PageFetchLifecycle,
    *,
    sync_fetch: Any = None,
    async_fetch: Any = None,
) -> None:
    try:
        results = fetch_pages_batched(
            1,
            [{"url": "https://lifecycle.example/page", "payload": {"id": "page"}}],
            sync_fetch=sync_fetch,
            async_fetch=async_fetch,
            total_concurrency=1,
            per_host_concurrency=1,
            page_fetch_lifecycle=lifecycle,
        )
    except BaseException as error:
        result_queue.put(("error", error))
    else:
        result_queue.put(("result", results))


def test_sync_straggler_remains_outstanding_after_total_deadline() -> None:
    worker_gate = threading.Event()
    worker_started = threading.Event()
    result_queue: queue.Queue[tuple[str, object]] = queue.Queue()
    lifecycle = PageFetchLifecycle(
        batch_id="sync-straggler", source="test", run_id="run", phase="pages"
    )

    def sync_fetch(_job: dict[str, object], _url: str, _timeout_s: int) -> str:
        worker_started.set()
        worker_gate.wait()
        return "<html>done</html>"

    batch_thread = threading.Thread(
        target=_batch_result,
        args=(result_queue, lifecycle),
        kwargs={"sync_fetch": sync_fetch},
    )
    batch_thread.start()
    try:
        outcome, value = result_queue.get(timeout=5)
        assert outcome == "result", f"page batch rejected its lifecycle receipt: {value!r}"
        assert worker_started.wait(timeout=1)

        at_batch_exit = lifecycle.snapshot()
        frozen_exit = lifecycle.exit_snapshot()
        assert frozen_exit is not None
        assert frozen_exit["deadlineExit"] == "deadline_exceeded"
        assert at_batch_exit["deadlineExit"] == "deadline_exceeded"
        assert at_batch_exit["submitted"] == 1
        assert at_batch_exit["started"] == 1
        assert at_batch_exit["finished"] == 0
        assert at_batch_exit["outstandingNow"] == 1
        assert at_batch_exit["asyncCancellationRequested"] == 1
        assert at_batch_exit["asyncCleanupFinished"] == 1

        worker_gate.set()
        settle_by = time.monotonic() + 2
        while lifecycle.snapshot()["finished"] == 0 and time.monotonic() < settle_by:
            time.sleep(0.01)
        settled = lifecycle.snapshot()
        assert settled["finished"] == 1
        assert settled["outstandingNow"] == 0
        assert at_batch_exit["finished"] == 0, "published point-in-time snapshots must not mutate"
        assert frozen_exit["finished"] == 0, "the captured batch-exit receipt must remain frozen"
        assert frozen_exit["outstandingNow"] == 1
    finally:
        worker_gate.set()
        batch_thread.join(timeout=5)
        assert not batch_thread.is_alive()


def test_async_cancellation_request_is_distinct_from_cleanup_completion() -> None:
    fetch_started = threading.Event()
    cleanup_started = threading.Event()
    cleanup_gate = threading.Event()
    result_queue: queue.Queue[tuple[str, object]] = queue.Queue()
    lifecycle = PageFetchLifecycle(
        batch_id="async-cleanup", source="test", run_id="run", phase="pages"
    )

    async def async_fetch(_client: Any, _job: dict[str, object], _url: str, _timeout_s: int) -> str:
        fetch_started.set()
        try:
            await asyncio.sleep(30)
        finally:
            cleanup_started.set()
            while not cleanup_gate.is_set():
                try:
                    await asyncio.sleep(0.01)
                except asyncio.CancelledError:
                    continue
        return "<html>done</html>"

    batch_thread = threading.Thread(
        target=_batch_result,
        args=(result_queue, lifecycle),
        kwargs={"async_fetch": async_fetch},
    )
    batch_thread.start()
    try:
        assert fetch_started.wait(timeout=2)
        assert cleanup_started.wait(timeout=5)
        during_cleanup = lifecycle.snapshot()
        frozen_exit = lifecycle.exit_snapshot()
        assert frozen_exit is not None
        assert frozen_exit["asyncCancellationRequested"] == 1
        assert frozen_exit["asyncCleanupFinished"] == 0
        assert during_cleanup["asyncCancellationRequested"] == 1
        assert during_cleanup["asyncCleanupFinished"] == 0
        assert during_cleanup["started"] == 1
        assert during_cleanup["finished"] == 0
        assert during_cleanup["outstandingNow"] == 1

        cleanup_gate.set()
        outcome, value = result_queue.get(timeout=5)
        assert outcome == "result", f"page batch rejected its lifecycle receipt: {value!r}"
        after_cleanup = lifecycle.snapshot()
        assert after_cleanup["asyncCancellationRequested"] == 1
        assert after_cleanup["asyncCleanupFinished"] == 1
        assert after_cleanup["finished"] == 1
        assert after_cleanup["outstandingNow"] == 0
        assert frozen_exit["asyncCleanupFinished"] == 0
    finally:
        cleanup_gate.set()
        batch_thread.join(timeout=5)
        assert not batch_thread.is_alive()


def test_empty_batch_and_no_receipt_preserve_clean_legacy_results() -> None:
    job = {"url": "https://lifecycle.example/page", "payload": {"id": "page"}}
    legacy_results = fetch_pages_batched(
        1,
        [job],
        sync_fetch=lambda _job, _url, _timeout_s: "<html>ok</html>",
        total_concurrency=1,
        per_host_concurrency=1,
    )
    assert len(legacy_results) == 1
    assert legacy_results[0]["ok"] is True

    empty_lifecycle = PageFetchLifecycle(batch_id="empty-batch", source="test", run_id="run")
    assert (
        fetch_pages_batched(
            1,
            [],
            sync_fetch=lambda _job, _url, _timeout_s: "unused",
            total_concurrency=1,
            per_host_concurrency=1,
            page_fetch_lifecycle=empty_lifecycle,
        )
        == []
    )
    empty_snapshot = empty_lifecycle.snapshot()
    assert empty_lifecycle.exit_snapshot() == empty_snapshot
    assert empty_snapshot["deadlineExit"] == "completed"
    assert empty_snapshot["transport"] == "sync"
    assert empty_snapshot["ownedClientState"] == "not_applicable"
    assert empty_snapshot["submitted"] == 0
    assert empty_snapshot["started"] == 0
    assert empty_snapshot["finished"] == 0
    assert empty_snapshot["queuedNow"] == 0
    assert empty_snapshot["outstandingNow"] == 0
    assert rollup_page_fetch_lifecycles([empty_lifecycle, empty_lifecycle]) == {
        "schemaVersion": 1,
        "receiptType": "pageFetchLifecycleRollup",
        "batchCount": 1,
        "submitted": 0,
        "started": 0,
        "finished": 0,
        "failed": 0,
        "cancelledBeforeStart": 0,
        "queuedNow": 0,
        "outstandingNow": 0,
        "asyncCancellationRequested": 0,
        "asyncCleanupFinished": 0,
        "pendingAsyncCleanup": 0,
        "state": "clean",
    }


def test_ticket_transitions_and_rollups_are_idempotent() -> None:
    lifecycle = PageFetchLifecycle(batch_id="idempotent", source="test", run_id="run")
    lifecycle.start_batch(transport="sync", deadline_seconds=4)
    queued = lifecycle.submit()
    assert queued.request_async_cancel()
    assert not queued.request_async_cancel()
    assert queued.cancel_before_start()
    assert not queued.cancel_before_start()
    assert not queued.start()
    assert not queued.finish()
    assert queued.finish_async_cleanup()
    assert not queued.finish_async_cleanup()

    worker = lifecycle.submit()
    assert rollup_page_fetch_lifecycles([lifecycle])["state"] == "pending"
    assert worker.mark_worker_submitted()
    assert worker.worker_was_submitted()
    assert worker.start()
    assert not worker.start()
    assert worker.finish(failed=True)
    assert not worker.finish(failed=True)
    lifecycle.mark_batch_exit("completed")
    lifecycle.mark_batch_exit("deadline_exceeded")

    snapshot = lifecycle.snapshot()
    assert snapshot["deadlineExit"] == "completed"
    assert snapshot["submitted"] == 2
    assert snapshot["started"] == 1
    assert snapshot["finished"] == 1
    assert snapshot["failed"] == 1
    assert snapshot["cancelledBeforeStart"] == 1
    assert snapshot["queuedNow"] == 0
    assert snapshot["outstandingNow"] == 0
    assert snapshot["asyncCancellationRequested"] == 1
    assert snapshot["asyncCleanupFinished"] == 1
    assert rollup_page_fetch_lifecycles([lifecycle, lifecycle])["batchCount"] == 1


def test_sync_queued_task_cancellation_is_counted_once() -> None:
    worker_gate = threading.Event()
    worker_started = threading.Event()
    result_queue: queue.Queue[tuple[str, object]] = queue.Queue()
    lifecycle = PageFetchLifecycle(batch_id="queued-cancel", source="test", run_id="run")
    jobs = [
        {"url": f"https://lifecycle.example/{index}", "payload": {"id": index}}
        for index in range(2)
    ]

    def sync_fetch(_job: dict[str, object], _url: str, _timeout_s: int) -> str:
        worker_started.set()
        worker_gate.wait()
        return "<html>done</html>"

    def call_batch() -> None:
        try:
            result = fetch_pages_batched(
                1,
                jobs,
                sync_fetch=sync_fetch,
                total_concurrency=1,
                per_host_concurrency=1,
                page_fetch_lifecycle=lifecycle,
            )
        except BaseException as error:
            result_queue.put(("error", error))
        else:
            result_queue.put(("result", result))

    batch_thread = threading.Thread(target=call_batch)
    batch_thread.start()
    try:
        assert worker_started.wait(timeout=3)
        outcome, value = result_queue.get(timeout=7)
        assert outcome == "result", f"page batch rejected its lifecycle receipt: {value!r}"
        at_batch_exit = lifecycle.snapshot()
        frozen_exit = lifecycle.exit_snapshot()
        assert frozen_exit is not None
        assert frozen_exit["submitted"] == 2
        assert at_batch_exit["submitted"] == 2
        assert at_batch_exit["started"] == 1
        assert at_batch_exit["cancelledBeforeStart"] == 1
        assert at_batch_exit["queuedNow"] == 0
        assert at_batch_exit["outstandingNow"] == 1
        assert at_batch_exit["asyncCancellationRequested"] == 2
        assert at_batch_exit["asyncCleanupFinished"] == 2

        worker_gate.set()
        settle_by = time.monotonic() + 2
        while lifecycle.snapshot()["finished"] == 0 and time.monotonic() < settle_by:
            time.sleep(0.01)
        settled = lifecycle.snapshot()
        assert settled["cancelledBeforeStart"] == 1
        assert settled["finished"] == 1
        assert settled["outstandingNow"] == 0
    finally:
        worker_gate.set()
        batch_thread.join(timeout=5)
        assert not batch_thread.is_alive()
