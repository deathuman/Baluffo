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

import pytest

from src.source_discovery import probe_runtime

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
    """A trickling board must fail at its own timeout, not park the batch until it finishes."""
    with _drip_server() as drip_url:
        candidates = [
            {"id": f"c{i}", "url": f"{drip_url}?i={i}", "adapter": "static"} for i in range(3)
        ]
        timeout_s = 2

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
                    )
                finally:
                    elapsed = time.perf_counter() - started

            return asyncio.run(_inner()), elapsed

        results, batch_elapsed = _call()

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


def test_default_fetcher_path_still_uses_the_httpx_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The fix must not disturb the bounded default branch."""
    calls: list[tuple[str, int]] = []

    async def fake_async_fetch(client: Any, url: str, timeout_s: int) -> str:
        calls.append((url, timeout_s))
        return "<html>ok</html>"

    monkeypatch.setattr(probe_runtime, "async_fetch_text_httpx", fake_async_fetch)

    async def probe_using_fetcher(
        row: dict[str, Any], timeout_s: int, *, fetcher: Any, **_kwargs: Any
    ) -> tuple[bool, int, str]:
        await fetcher(f"https://default.invalid/{row.get('id')}", timeout_s)
        return True, 2, ""

    results = asyncio.run(
        asyncio.wait_for(
            probe_runtime.run_bounded_probe_batch_async(
                [{"id": "c0", "adapter": "static"}],
                timeout_s=3,
                fetcher=probe_runtime.fetch_text,  # IS default_fetcher
                async_probe=probe_using_fetcher,
                default_fetcher=probe_runtime.fetch_text,
            ),
            timeout=20,
        )
    )

    assert len(results) == 1
    assert calls, "the default path must still call the httpx-based fetcher"
