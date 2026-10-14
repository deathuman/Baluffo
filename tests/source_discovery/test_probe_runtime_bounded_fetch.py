"""Regression guard for the bounded probe fetch in source_discovery/probe_runtime.py.

Defect one: `run_bounded_probe_batch_async._call_fetch` had an unbounded
`await asyncio.to_thread(fetcher, url, call_timeout_s)`. The module contains no
`asyncio.wait_for` anywhere, so a fetcher whose socket read never returns parks
`_probe_one` forever: its total/bucket semaphores stay held, `as_completed`
never yields, and the whole probe batch stops with no error and no timeout.

Defect two, the default path: `httpx.Timeout` is a PER-OPERATION budget and the read
timeout re-arms on every chunk, so a server that trickles bytes holds the request open far
past `timeout_s` -- and then returns SUCCESSFULLY. Measured before the fix on the loopback
drip server below: 40.3 s against a 2 s timeout, every candidate counted as a success. The
shared batcher documents the same loophole (40.7 s against `Timeout(2.0)`, and a GameDevMap
batch parked 900 s), and the fix follows its shape: bound each call, then bound the batch by
rows/concurrency so one candidate cannot hold the stage.

Both are the same class as the shared/http_batch total-deadline defect found via the
GameDevMap stall.

The batch's other two contracts have their own modules, because this one is at the repo's
test-shape budget with them included: the permit order in
`tests/source_discovery/test_probe_batch_permits.py`, and the shared client's lifecycle -- who owns
it when a fetch outlives its own deadline -- in
`tests/source_discovery/test_probe_batch_client_lifecycle.py`.

Test style follows the repo convention (`asyncio.run` inside sync test functions;
there are no `async def test_` functions in this suite).
"""

from __future__ import annotations

import asyncio
import http.server
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import httpx
import pytest

from src.source_discovery import probe_runtime
from tests.helpers.concurrency import BlockingActiveCounter

# Models a stuck socket read. `time.sleep` is uninterruptible, so the orphaned
# thread holds the default executor open until `asyncio.run` drains it -- paid
# once per test that triggers a hang.
_HANG_S = 12.0

# The bounded-batch test needs the fake to outlast the assertion threshold,
# otherwise unfixed code also returns "fast enough" and the guard stops
# discriminating. 60 s sleep vs a 25 s threshold makes the distinction sharp
# while keeping the total suite cost acceptable.
_UNINTERRUPTIBLE_HANG_S = 60.0
_BATCH_DEADLINE_ASSERT_S = 25.0


def _uninterruptible_hang_fetcher(url: str, timeout_s: int) -> str:
    time.sleep(_UNINTERRUPTIBLE_HANG_S)
    return "<html>never</html>"


async def _probe_calls_fetcher_once(
    row: dict[str, Any],
    timeout_s: int,
    *,
    fetcher: Any,
    **_kwargs: Any,
) -> tuple[bool, int, str]:
    await fetcher(f"https://example.invalid/{row.get('id')}", timeout_s)
    return True, 1, ""


def _hanging_fetcher(url: str, timeout_s: int) -> str:
    """Models a stuck socket read: ignores its timeout entirely."""
    time.sleep(_HANG_S)
    return "<html>never</html>"


def _fast_fetcher(url: str, timeout_s: int) -> str:
    return "<html>ok</html>"


def _run(
    candidates: list[dict[str, Any]],
    fetcher: Any,
    timeout_s: int,
    *,
    async_probe: Any = _probe_calls_fetcher_once,
) -> tuple[list[Any], float]:
    """Run the probe batch and return (results, batch_elapsed_seconds).

    The elapsed time is measured INSIDE the coroutine, around the batch call
    only. Measuring the surrounding `asyncio.run` instead would include the
    default ThreadPoolExecutor draining the uninterruptible fake thread at loop
    shutdown (~_HANG_S), which no amount of cancellation can prevent and which
    no real socket-backed fetcher exhibits. The defect being guarded is that the
    BATCH parks forever; that is what this measures.
    """
    batch_elapsed = 0.0

    async def _inner() -> list[Any]:
        nonlocal batch_elapsed
        started = time.perf_counter()
        try:
            return await asyncio.wait_for(
                probe_runtime.run_bounded_probe_batch_async(
                    candidates,
                    timeout_s=timeout_s,
                    fetcher=fetcher,
                    async_probe=async_probe,
                    default_fetcher=probe_runtime.fetch_text,
                ),
                timeout=30,
            )
        finally:
            batch_elapsed = time.perf_counter() - started

    return asyncio.run(_inner()), batch_elapsed


def test_probe_batch_is_bounded_when_the_fetcher_hangs() -> None:
    """A hung sync fetcher must not park the batch past its deadline.

    Calibration note: the fake sleep must be comfortably LONGER than the
    assertion threshold, otherwise the unfixed code also returns "quickly"
    (once the sleep happens to finish) and the guard stops discriminating.
    Earlier at 12 s this test passed against unfixed code -- the batch came back
    at 12.3 s because the fake itself completed. It is now 60 s against a 25 s
    threshold, and the batch is measured without the executor drain.
    """
    candidates = [{"id": f"c{i}", "adapter": "static"} for i in range(3)]
    timeout_s = 2

    results, batch_elapsed = _run(candidates, _uninterruptible_hang_fetcher, timeout_s)

    assert len(results) == len(candidates), "one result per candidate is the contract"
    assert batch_elapsed < _BATCH_DEADLINE_ASSERT_S, (
        f"probe batch took {batch_elapsed:.1f}s to return; a hung fetcher must be "
        f"bounded to about timeout_s ({timeout_s}s) per call, not parked until the "
        f"fetcher happens to finish"
    )


def test_a_hung_fetcher_is_reported_as_a_per_candidate_failure() -> None:
    """The bound must surface as an ordinary failure, not an aborted batch."""
    candidates = [{"id": "c0", "adapter": "static"}]

    results, _elapsed = _run(candidates, _hanging_fetcher, 1)

    assert len(results) == 1
    row, ok, _jobs_found, _error, duration_ms = results[0]
    assert row == candidates[0], "the candidate identity must be preserved"
    assert not ok, "a timed-out probe is a failure, not a success"
    assert isinstance(duration_ms, int)


def test_one_hung_candidate_does_not_abort_its_siblings() -> None:
    """A single hung probe must still yield a result for every candidate.

    Without the per-candidate guard the TimeoutError escaped through
    as_completed and the batch returned NO results at all.
    """
    candidates = [{"id": f"c{i}", "adapter": "static"} for i in range(4)]

    def fetcher(url: str, timeout_s: int) -> str:
        if url.endswith("c1"):
            time.sleep(_HANG_S)
        return "<html>ok</html>"

    results, _elapsed = _run(candidates, fetcher, 2)

    assert len(results) == 4, "every candidate must produce exactly one result"
    by_id = {row["id"]: ok for row, ok, _j, _e, _ms in results}
    assert by_id["c0"] and by_id["c2"] and by_id["c3"], "healthy siblings must survive"
    assert not by_id["c1"], "the hung candidate must be reported as a failure"


def test_healthy_fetcher_is_unaffected_by_the_bound() -> None:
    """The bound must not clip legitimate probes."""
    candidates = [{"id": f"c{i}", "adapter": "static"} for i in range(5)]

    results, batch_elapsed = _run(candidates, _fast_fetcher, 5)

    assert len(results) == 5
    assert all(ok for _row, ok, _j, _e, _ms in results), "healthy probes must succeed"
    assert batch_elapsed < 10, "healthy probes must not be delayed by the bound"


# --- the default httpx path: a trickling server must not park the batch ----------------

_DRIP_S = 30.0
_DRIP_CHUNK_INTERVAL_S = 0.3
_DRIP_BOUND_ASSERT_S = 15.0


class _DripHandler(http.server.BaseHTTPRequestHandler):
    """Answers slowly and forever: one chunk per interval, far past any per-request timeout."""

    def do_GET(self) -> None:  # noqa: N802 - http.server's naming
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()
        chunk = b"<html>" + b" " * 64
        deadline = time.monotonic() + _DRIP_S
        try:
            while time.monotonic() < deadline:
                self.wfile.write(b"%x\r\n" % len(chunk) + chunk + b"\r\n")
                self.wfile.flush()
                time.sleep(_DRIP_CHUNK_INTERVAL_S)
            self.wfile.write(b"0\r\n\r\n")
        except (BrokenPipeError, ConnectionResetError, OSError, ValueError):
            return None

    def log_message(self, *args: Any) -> None:
        return None


@contextmanager
def _drip_server() -> Iterator[str]:
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _DripHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}/drip"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


async def _probe_fetching_its_url(
    row: dict[str, Any],
    timeout_s: int,
    *,
    fetcher: Any,
    **_kwargs: Any,
) -> tuple[bool, int, str]:
    await fetcher(str(row.get("url") or ""), timeout_s)
    return True, 1, ""


def test_slow_drip_probe_is_bounded_on_the_default_httpx_path() -> None:
    """A trickling board must fail at its own timeout, not park the batch until it finishes.

    The receipt is read here too, on the real client rather than a fake: this is the one test that
    drives the default path end to end against a peer that outlives its per-call bound, so it is the
    one that proves the counters a live run reads are the real path's -- in particular that
    `fetchesThroughClient` counts these fetches at all (it reads `fetcher is default_fetcher`, and a
    live artifact whose client counts were silently zero would say nothing). What it reports at the
    batch's exit is the leak shape: every fetch abandoned, the client still open.
    """
    with _drip_server() as drip_url:
        candidates = [
            {"id": f"c{i}", "url": f"{drip_url}?i={i}", "adapter": "static"} for i in range(3)
        ]
        timeout_s = 2
        lifecycle = probe_runtime.ProbeBatchLifecycle()

        def _call() -> tuple[list[Any], float]:
            elapsed = 0.0

            async def _inner() -> list[Any]:
                nonlocal elapsed
                started = time.perf_counter()
                try:
                    return await probe_runtime.run_bounded_probe_batch_async(
                        candidates,
                        timeout_s=timeout_s,
                        fetcher=probe_runtime.fetch_text,  # IS default_fetcher
                        async_probe=_probe_fetching_its_url,
                        default_fetcher=probe_runtime.fetch_text,
                        lifecycle=lifecycle,
                    )
                finally:
                    elapsed = time.perf_counter() - started

            return asyncio.run(_inner()), elapsed

        results, batch_elapsed = _call()
        receipt = lifecycle.snapshot()

    assert len(results) == len(candidates), "one result per candidate is the contract"
    assert batch_elapsed < _DRIP_BOUND_ASSERT_S, (
        f"the probe batch took {batch_elapsed:.1f}s against a {timeout_s}s timeout; a "
        f"trickling response must not hold the stage until the server stops trickling"
    )
    assert not any(ok for _row, ok, _j, _e, _ms in results), (
        "a response that never completes is a failed probe, never a success"
    )
    assert all("timed out" in str(error) for _row, _ok, _j, error, _ms in results), [
        error for _row, _ok, _j, error, _ms in results
    ]
    assert receipt["poolWorkers"] == len(candidates)
    assert receipt["fetchesStarted"] == len(candidates)
    assert receipt["fetchesThroughClient"] == len(candidates), (
        "the default httpx path must be counted as client-carried, or a live run's client "
        "counters read zero however many fetches it abandons"
    )
    assert receipt["fetchesBoundedOut"] == len(candidates)
    assert receipt["fetchesOutstandingAtExit"] == len(candidates), "every fetch outlived the batch"
    assert receipt["clientFetchesOutstandingAtExit"] == len(candidates)
    assert receipt["clientClosed"] is False, (
        "this is the leak shape the receipt exists to surface: a live run reads it as an open client"
    )
    assert receipt["clientRetiredBy"] == "unclosed"
    assert receipt["deadlineExit"] is False, "the batch returned on its own, not on its deadline"


def test_batch_deadline_reports_the_candidates_it_cut_short() -> None:
    """A candidate cannot hold the batch past rows/concurrency of its own timeout."""
    candidates = [{"id": f"c{i}", "adapter": "static"} for i in range(4)]
    # timeout_s=2 over 4 candidates at 40-way concurrency: deadline = 2 * (4/4) + 2 = 4s.
    probe_sleep_s = 6.0

    async def slow_probe(
        _row: dict[str, Any], _timeout_s: int, **_kwargs: Any
    ) -> tuple[bool, int, str]:
        await asyncio.sleep(probe_sleep_s)
        return True, 1, ""

    elapsed = 0.0

    async def _inner() -> list[Any]:
        nonlocal elapsed
        started = time.perf_counter()
        try:
            return await probe_runtime.run_bounded_probe_batch_async(
                candidates,
                timeout_s=2,
                fetcher=_fast_fetcher,
                async_probe=slow_probe,
                default_fetcher=probe_runtime.fetch_text,
            )
        finally:
            elapsed = time.perf_counter() - started

    results = asyncio.run(_inner())

    assert len(results) == len(candidates), "one result per candidate is the contract"
    assert elapsed < probe_sleep_s, f"batch parked for {elapsed:.1f}s past its own deadline"
    assert all(
        not ok and "total probe deadline exceeded" in str(error)
        for _row, ok, _j, error, _ms in results
    ), results


def test_default_fetcher_path_runs_its_httpx_request_on_the_batch_pool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The default path still speaks httpx, but its request runs off the loop.

    `httpx`'s async client executes the TLS handshake, the HTTP/1.1 parse and the body accumulation
    on the event loop thread, which is the wave's one thread: measured 7.33 ms of loop CPU per
    candidate against 1.64 ms with the same requests on the wave's own pool. So the default path must
    reach httpx through the pool -- the client it hands the helper is still an `httpx.Client` (same
    request shape, same certifi trust), and the thread doing the work must be the batch's.
    """
    calls: list[tuple[str, int, str, Any]] = []

    def fake_sync_fetch(client: Any, url: str, timeout_s: int) -> str:
        calls.append((url, timeout_s, threading.current_thread().name, client))
        return "<html>ok</html>"

    monkeypatch.setattr(probe_runtime, "fetch_text_httpx", fake_sync_fetch)

    async def probe_using_fetcher(
        row: dict[str, Any], timeout_s: int, *, fetcher: Any, **_kwargs: Any
    ) -> tuple[bool, int, str]:
        await fetcher(f"https://default.invalid/{row.get('id')}", timeout_s)
        return True, 2, ""

    loop_thread_name = ""

    async def _inner() -> list[Any]:
        nonlocal loop_thread_name
        loop_thread_name = threading.current_thread().name
        return await asyncio.wait_for(
            probe_runtime.run_bounded_probe_batch_async(
                [{"id": "c0", "adapter": "static"}],
                timeout_s=3,
                fetcher=probe_runtime.fetch_text,  # IS default_fetcher
                async_probe=probe_using_fetcher,
                default_fetcher=probe_runtime.fetch_text,
            ),
            timeout=20,
        )

    results = asyncio.run(_inner())

    assert len(results) == 1
    assert calls, "the default path must still fetch through httpx"
    _url, _timeout, thread_name, client = calls[0]
    assert thread_name.startswith("probe-fetch"), (
        f"the default httpx request must run on this batch's pool, ran on {thread_name!r}"
    )
    assert thread_name != loop_thread_name, "the loop thread must not do the request"
    assert isinstance(client, httpx.Client), (
        "the default path must keep an httpx client (shared keep-alive pool and certifi trust)"
    )


def test_default_path_fetches_run_at_the_configured_lanes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The batch pool carries the default (httpx) path too, sized to the configured lanes.

    Same contract as the wrapping-fetcher case beside it, for the path every audit run takes: the
    default requests must reach the configured lane count, not the shared default executor's
    `min(32, cpu_count + 4)`.
    """
    lanes = int(probe_runtime.probe_concurrency_defaults()["total"])
    candidates = [{"id": f"c{i}", "adapter": "greenhouse"} for i in range(lanes)]
    fetches = BlockingActiveCounter()
    released = threading.Event()
    thread_names: set[str] = set()
    lock = threading.Lock()

    def fake_sync_fetch(_client: Any, url: str, _timeout_s: int) -> str:
        with lock:
            thread_names.add(threading.current_thread().name)
        fetches.enter()
        try:
            if fetches.peak >= lanes:
                released.set()
            released.wait(timeout=2.0)
            return f"<html>{url}</html>"
        finally:
            fetches.exit()

    monkeypatch.setattr(probe_runtime, "fetch_text_httpx", fake_sync_fetch)

    async def _inner() -> list[Any]:
        return await asyncio.wait_for(
            probe_runtime.run_bounded_probe_batch_async(
                candidates,
                timeout_s=5,
                fetcher=probe_runtime.fetch_text,  # IS default_fetcher
                async_probe=_probe_calls_fetcher_once,
                default_fetcher=probe_runtime.fetch_text,
            ),
            timeout=60,
        )

    results = asyncio.run(_inner())

    assert len(results) == lanes, "one result per candidate is the contract"
    assert fetches.peak == lanes, (
        f"peak in-flight was {fetches.peak} of {lanes} configured probe lanes"
    )
    assert thread_names and all(name.startswith("probe-fetch") for name in thread_names), (
        f"default-path fetches must run on this module's own pool, saw {sorted(thread_names)}"
    )


# --- the lane contract: the sync branch runs at the configured probe lanes, not the machine's ----


def test_sync_probe_fetches_run_at_the_configured_lanes_not_the_shared_executor() -> None:
    """The probe's lanes are `probe_concurrency_defaults()["total"]`, not the machine's threads.

    The sync branch used `asyncio.to_thread`, i.e. the event loop's shared default executor
    (`min(32, cpu_count + 4)`, 20 on the 16-core box the audit runs use), so a wrapping fetcher --
    the bench's instrumented one, say -- had its probe fetches capped below the configured lane
    count with no error and no log line: `probe_candidates` peaked at exactly 20 in-flight of its
    configured 40 in both `lane-ceiling` audit runs. The configured lanes are above the shared
    executor's ceiling on every machine (`min(32, ...)` is at most 32), so this floor discriminates
    here rather than depending on this box's core count.
    """
    lanes = int(probe_runtime.probe_concurrency_defaults()["total"])
    # Provider-bucket candidates: the static bucket is deliberately narrower (16), and this
    # contract is about the batch's total lanes, not a bucket's own cap.
    candidates = [{"id": f"c{i}", "adapter": "greenhouse"} for i in range(lanes)]
    fetches = BlockingActiveCounter()
    released = threading.Event()
    thread_names: set[str] = set()
    lock = threading.Lock()

    def fake_fetch(url: str, _timeout_s: int) -> str:
        with lock:
            thread_names.add(threading.current_thread().name)
        fetches.enter()
        try:
            # Block only long enough for every lane to come alive at once: a batch that cannot
            # reach its lane count then fails below with the count it did reach, rather than
            # hanging here or raising a message-free assert out of a pool thread.
            if fetches.peak >= lanes:
                released.set()
            released.wait(timeout=2.0)
            return f"<html>{url}</html>"
        finally:
            fetches.exit()

    results, _elapsed = _run(candidates, fake_fetch, 5)

    assert len(results) == lanes, "one result per candidate is the contract"
    assert fetches.peak == lanes, (
        f"peak in-flight was {fetches.peak} of {lanes} configured probe lanes"
    )
    assert thread_names and all(name.startswith("probe-fetch") for name in thread_names), (
        f"probe fetches must run on this module's own pool, saw {sorted(thread_names)}"
    )


# The batch's permit order (`test_probe_batch_permits.py`) and its shared client's lifecycle
# (`test_probe_batch_client_lifecycle.py`) have their own modules: both are contracts with a
# measured before/after, and this file is at the repo's test-shape budget without them.
