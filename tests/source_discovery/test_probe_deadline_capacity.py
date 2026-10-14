"""P4 — Deadline-capacity correctness: multi-request / family-constrained valid work.

Both deadline formulas (http_batch and probe_runtime) assume each row makes exactly ONE request of
`timeout_s` and all rows run at full `effective_concurrency`. This file reproduces the false deadline
failure a valid *multi-request* probe wave hits when enough rows exceed the single-request budget, and
asserts the corrected `requests_per_row` bound lets the same valid work settle while a pathological
row is still cut off promptly.

Test style follows the repo convention: sync test functions, ``asyncio.run`` inside them; no
``async def test_`` functions.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from typing import Any

from src.source_discovery import probe_runtime


def _multi_request_probe(requests_per_row: int):
    """Build an async_probe that makes `requests_per_row` sequential requests, each taking its
    per-call budget. Models a valid probe that tries an endpoint then each fallback (and Workday's
    structured POST with retries + backoff) — a row whose total HTTP work exceeds one `timeout_s`."""

    async def probe(
        row: dict[str, Any],
        timeout_s: int,
        *,
        fetcher: Callable[..., Awaitable[str]],
        **_kwargs: Any,
    ) -> tuple[bool, int, str]:
        for i in range(int(requests_per_row)):
            await fetcher(f"https://multi.example/{row.get('id')}/req{i}", timeout_s)
        return True, 1, ""

    return probe


def _counting_fetcher(delay_s: float, calls: list[tuple[float, str]]):
    """Return a fetcher that records (finish_time_offset, url) and sleeps `delay_s`.

    Note: `run_bounded_probe_batch_async` runs the fetcher inside a pool worker thread
    (`_call_fetch`), so a blocking `time.sleep` is correct here — it does not stall the event loop.
    """

    def fetcher(url: str, _timeout_s: int) -> str:
        calls.append((time.perf_counter(), url))
        if delay_s:
            time.sleep(delay_s)
        return "<html>ok</html>"

    return fetcher


def _hang_async(url: str, _timeout_s: int) -> str:  # noqa: ARG001
    """A sync fetcher that never settles — models a stuck socket read outliving its budget."""
    time.sleep(30)
    return "<html>never</html>"


def _run_batch(
    candidates,
    async_probe,
    fetcher,
    timeout_s: int,
    *,
    requests_per_row: int = 1,
) -> tuple[list[Any], float]:
    elapsed = 0.0

    async def _inner() -> list[Any]:
        nonlocal elapsed
        started = time.perf_counter()
        try:
            return await asyncio.wait_for(
                probe_runtime.run_bounded_probe_batch_async(
                    candidates,
                    timeout_s=timeout_s,
                    fetcher=fetcher,
                    async_probe=async_probe,
                    default_fetcher=probe_runtime.fetch_text,
                    requests_per_row=requests_per_row,
                ),
                timeout=60,
            )
        finally:
            elapsed = time.perf_counter() - started

    return asyncio.run(_inner()), elapsed


def test_multi_request_valid_work_is_not_truncated() -> None:
    """Failing-before oracle: with `requests_per_row`, valid multi-request work completes.

    Pre-fix (requests_per_row=1): each row makes 5 sequential requests each of `timeout_s`, but the
    deadline is derived assuming one request per row, so the batch is cut off and valid rows are
    marked deadline_exceeded.

    Post-fix (requests_per_row=5): the deadline covers the real per-row work and all rows settle.
    """
    requests_per_row = 5  # one endpoint + four fallbacks (or Workday POST + retries + backoff)
    timeout_s = 2  # per-request budget (each request must complete well inside it)
    per_request_s = 1.0  # each request takes 1s, comfortably within the 2s per-call budget
    row_count = 5

    candidates = [{"id": f"c{i}", "adapter": "static"} for i in range(row_count)]
    probe = _multi_request_probe(requests_per_row)
    calls: list[tuple[float, str]] = []
    fetcher = _counting_fetcher(per_request_s, calls)

    results, batch_elapsed = _run_batch(
        candidates, probe, fetcher, timeout_s, requests_per_row=requests_per_row
    )

    # Real serial work = 25 requests x 1s across 5 lanes = 5 waves = ~5s. The single-request
    # deadline (2*(5/5)+2 = 4s) truncates it; the requests_per_row=5 deadline (20s) lets it settle.
    ok = [r for r in results if r[1]]
    total_calls = len(calls)

    assert len(ok) == row_count, (
        f"valid multi-request probe truncated with requests_per_row={requests_per_row}: "
        f"{len(ok)}/{row_count} ok (was {len(ok)} pre-fix). total calls={total_calls}"
    )
    assert total_calls == row_count * requests_per_row, (
        f"expected {row_count * requests_per_row} fetches, saw {total_calls}"
    )


def test_requests_per_row_multiplies_the_deadline_but_stays_bounded() -> None:
    """The corrected deadline scales with `requests_per_row` yet a hanging row is still cut off.

    A row whose fetch never settles must not get an unbounded window because `requests_per_row` grew:
    the whole-batch deadline must still fire. With a single hanging row at requests_per_row=3 and
    timeout_s=2: row_work=6, deadline = 6*(1/1)+6 = 12s (per formula). Assert the batch returns well
    before the hang could finish, i.e. the deadline still bounds it.
    """
    requests_per_row = 3
    timeout_s = 2
    row_count = 1

    candidates = [{"id": "c0", "adapter": "static"}]
    probe = _multi_request_probe(requests_per_row)
    fetcher = _hang_async

    results, batch_elapsed = _run_batch(
        candidates, probe, fetcher, timeout_s, requests_per_row=requests_per_row
    )

    # Unexpectedly NOT (an unbounded extension): the batch must return promptly, cutting the hanging
    # row off and reporting it as a deadline failure, not hang for requests_per_row * hang_time.
    assert batch_elapsed < timeout_s * requests_per_row * 3, (
        f"hanging row was not bounded: batch ran {batch_elapsed:.1f}s with "
        f"requests_per_row={requests_per_row} (hang=30s)"
    )
    assert len(results) == row_count
    assert not results[0][1], "the hanging row must be reported as a failure, not a silent success"


def test_probe_row_timeout_units_are_adapter_aware() -> None:
    """The bound helper returns >1 for multi-request adapters and 1 for a plain single-fetch row.

    This is the unit seam that `probe_candidates_async` uses to derive `requests_per_row`.
    """
    from src.source_discovery.probe import probe_row_timeout_units

    # A plain static row with a single URL probes one request (default timeout-attempts = 2, but the
    # bound is len(probe_urls) * attempts => a normal no-fallback static row is 1 or more).
    plain = {"id": "c0", "adapter": "static"}
    # Workday with a listing URL must report a large ceiling (structured POST + 2 retries + backoff).
    workday = {"id": "w0", "adapter": "workday", "listing_url": "https://wd.example/jobs"}

    plain_units = probe_row_timeout_units(plain, timeout_s=2)
    workday_units = probe_row_timeout_units(workday, timeout_s=2)

    assert plain_units >= 1
    assert workday_units >= 1
    # The Workday structured path is strictly multi-request: it must be larger than any single-fetch
    # assumption, and larger than the plain row's bound (which itself may include a fallback URL).
    assert workday_units > plain_units, (
        f"expected Workday ({workday_units}) to exceed the plain row ({plain_units}) request bound"
    )
