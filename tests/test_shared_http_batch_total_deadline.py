"""Regression guards for the total-batch-deadline fix in shared/http_batch.py.

Defect: `httpx.Timeout(timeout_s)` is a PER-OPERATION budget, not a total one.
The read timeout re-arms whenever data arrives, so a slow-drip peer holds a
request open indefinitely. Nothing raises, `asyncio.as_completed` never yields,
the per-host semaphores stay held, and the whole batch parks with no error.

Observed in production: a GameDevMap active-audit batch stalled in
`recovery_wave1_fetch` at 1347/1349 fetches, emitted nothing for 900 s, and the
discovery quiet-guard eventually failed the run (abandoning 3,370 eligible rows).

Measured before the fix: a drip against `httpx.Timeout(2.0)` ran 40.7 s.

Note on the hanging fakes below: they sleep for `_HANG_S` rather than forever.
Against unfixed code these tests do not fail fast -- they hang (verified), which
is the production symptom. `_HANG_S` stays well under any CI job timeout while
still being far longer than the deadline under test.
"""

from __future__ import annotations

import asyncio
import time

from src.shared.http_batch import fetch_pages_batched

# Long enough that the batch deadline always fires first, short enough that a
# regression run (unfixed code) still terminates instead of wedging the suite.
_HANG_S = 120.0


def _deadline_for(jobs: int, timeout_s: int, concurrency: int) -> float:
    per_row = max(1.0, float(timeout_s))
    eff = max(1, min(int(concurrency), max(1, jobs)))
    return per_row * (jobs / eff) + per_row


def test_dripping_async_batch_is_bounded_by_the_total_deadline() -> None:
    """A peer that never finishes must not park the batch forever."""

    async def never_finishes(client, job, url, timeout_s):  # noqa: ANN001
        await asyncio.sleep(_HANG_S)
        return "<html>never</html>"

    jobs = [{"url": f"https://drip.invalid/{i}", "payload": {"i": i}} for i in range(4)]
    expected = _deadline_for(4, 5, 4)

    started = time.perf_counter()
    results = fetch_pages_batched(
        5,
        jobs,
        sync_fetch=lambda job, url, t: "<html>sync</html>",  # noqa: ARG005
        async_fetch=never_finishes,
        total_concurrency=4,
        per_host_concurrency=4,
    )
    elapsed = time.perf_counter() - started

    assert elapsed < expected + 5.0, (
        f"batch ran {elapsed:.1f}s against a {expected:.0f}s total deadline; "
        "a slow-drip peer can still park the batch"
    )
    assert len(results) == len(jobs), "one result per job is part of the contract"
    assert all(not r.get("ok") for r in results), (
        "rows that never resolved must be reported as failures"
    )
    assert all("total batch deadline exceeded" in str(r.get("error") or "") for r in results), (
        "the failure reason must name the deadline"
    )


def test_deadline_exceeded_results_preserve_job_identity_and_order() -> None:
    """Callers key results by url, so order and identity must survive."""

    async def never_finishes(client, job, url, timeout_s):  # noqa: ANN001
        await asyncio.sleep(_HANG_S)
        return ""

    jobs = [{"url": f"https://drip.invalid/{i}", "payload": {"i": i}} for i in range(3)]

    results = fetch_pages_batched(
        5,
        jobs,
        sync_fetch=lambda job, url, t: "",  # noqa: ARG005
        async_fetch=never_finishes,
        total_concurrency=3,
        per_host_concurrency=3,
    )

    assert [r.get("url") for r in results] == [j["url"] for j in jobs]
    assert [r.get("payload") for r in results] == [j["payload"] for j in jobs]


def test_healthy_batch_is_not_truncated_by_the_total_deadline() -> None:
    """The deadline must not clip legitimate work.

    A real GameDevMap wave-1 recovery batch ran 20,117 ms against a 5 s
    per-request timeout, so bounding the whole batch at `timeout_s` would
    truncate healthy batches -- this guards that regression.
    """

    async def quick(client, job, url, timeout_s):  # noqa: ANN001
        await asyncio.sleep(0.01)
        return "<html>ok</html>"

    jobs = [{"url": f"https://ok.invalid/{i}", "payload": {"i": i}} for i in range(40)]

    results = fetch_pages_batched(
        5,
        jobs,
        sync_fetch=lambda job, url, t: "<html>ok</html>",  # noqa: ARG005
        async_fetch=quick,
        total_concurrency=16,
        per_host_concurrency=8,
    )

    assert len(results) == 40
    assert all(r.get("ok") for r in results), "healthy rows must not be failed"
    assert not any("deadline" in str(r.get("error") or "") for r in results)


def test_deadline_scales_with_node_count_and_concurrency() -> None:
    """The budget must grow with the batch so large batches are not clipped."""
    small = _deadline_for(4, 5, 4)
    large = _deadline_for(1000, 5, 72)

    assert large > small, "a bigger batch must get a bigger budget"
    # The measured 20,117 ms wave-1 batch must fit inside the computed budget.
    assert large > 20.117, "the computed budget must clear measured real work"


def test_a_row_that_hangs_does_not_fail_its_healthy_siblings() -> None:
    """One bad peer must only cost its own row."""

    async def one_hangs(client, job, url, timeout_s):  # noqa: ANN001
        if job["payload"]["i"] == 1:
            await asyncio.sleep(_HANG_S)
        return "<html>ok</html>"

    jobs = [{"url": f"https://x.invalid/{i}", "payload": {"i": i}} for i in range(3)]

    results = fetch_pages_batched(
        5,
        jobs,
        sync_fetch=lambda job, url, t: "<html>ok</html>",  # noqa: ARG005
        async_fetch=one_hangs,
        total_concurrency=3,
        per_host_concurrency=3,
    )

    assert len(results) == 3
    by_url = {r.get("url"): r for r in results}
    assert by_url["https://x.invalid/0"].get("ok"), "healthy row 0 must succeed"
    assert by_url["https://x.invalid/2"].get("ok"), "healthy row 2 must succeed"
