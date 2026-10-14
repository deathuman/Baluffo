"""P3 — Page-batch fairness: total-before-host vs host-ready acquisition.

The scheduler in `http_batch._fetch_one` acquires the total semaphore before the host semaphore:

    async with total_sem:
        async with host_sem:
            ...

With this order, the first tasks in the work queue occupy *all* total lanes regardless of host,
so a slow host whose jobs arrived early in the list can block every other host from even checking
their per-host availability. The same total lanes would be occupied either way, but the *fairness*
measure is whether fast-host jobs can *start and finish* while slow-host jobs are still running.

This test builds a grouped-host fixture that reproduces this starvation and compares it with the
alternative ordering (host semaphore first), measuring fast-host job throughput under both.

Test style follows the repo convention: sync test functions, ``asyncio.run`` inside them.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from typing import Any
from urllib.parse import urlparse

_SLOW_HOST = "slow.example"
_FAST_HOSTS = ["fast-a.example", "fast-b.example", "fast-c.example"]
_TOTAL_CAP = 6
_PER_HOST_CAP = 2
_SLOW_DELAY_S = 0.2
_JOBS_PER_FAST_HOST = 4
_SLOW_JOB_COUNT = 12


def _make_jobs(*, slow_first: bool) -> list[dict[str, Any]]:
    """Build a job list with slow jobs either first (worst-case) or last (best-case)."""
    fast: list[dict[str, Any]] = []
    for host in _FAST_HOSTS:
        for i in range(_JOBS_PER_FAST_HOST):
            fast.append({"url": f"https://{host}/page{i}", "payload": {}})
    slow = [{"url": f"https://{_SLOW_HOST}/page{i}", "payload": {}} for i in range(_SLOW_JOB_COUNT)]
    return slow + fast if slow_first else fast + slow


async def _timed_fetch(client: Any, job: dict[str, Any], url: str, timeout_s: int) -> str:
    if _SLOW_HOST in url:
        await asyncio.sleep(_SLOW_DELAY_S)
    return f"<html>{url}</html>"


def _make_async_fetch() -> Callable[..., Awaitable[str]]:
    """Return an async fetch whose delay depends on the URL host."""
    return _timed_fetch


def _host_for(url: str) -> str:
    return (urlparse(url).netloc.strip().lower()) or "__default__"


async def _run_batch(
    jobs: list[dict[str, Any]],
    *,
    host_first: bool,
) -> dict[str, Any]:
    """Run one batch with the chosen semaphore order and return timing breakdown.

    Returns:
        total_wall_s: wall clock for the whole batch.
        per_host_mean_wall: dict[host, mean completion time in seconds].
        host_wait_count: dict[host, count of tasks that waited on *both* semaphores].
        fast_host_mean_wall_s: mean completion time across all fast hosts. This is the key
            fairness metric: lower means fast hosts are less starved by the slow one.
    """
    total_sem = asyncio.Semaphore(_TOTAL_CAP)
    host_sems: dict[str, asyncio.Semaphore] = {}
    results: list[dict[str, Any] | None] = [None] * len(jobs)
    # Timing measurements, indexed by job position.
    acquired_first_sem_at: list[float] = [0.0] * len(jobs)
    completed_at: list[float] = [0.0] * len(jobs)
    fetch = _make_async_fetch()

    async def _fetch_one(index: int, job: dict[str, Any]) -> None:
        url = str(job.get("url") or "").strip()
        host = _host_for(url)
        host_sem = host_sems.setdefault(host, asyncio.Semaphore(_PER_HOST_CAP))

        sem_a = host_sem if host_first else total_sem
        sem_b = total_sem if host_first else host_sem

        async with sem_a:
            acquired_first_sem_at[index] = time.perf_counter()
            async with sem_b:
                text = await fetch(None, job, url, 10)
                completed_at[index] = time.perf_counter()
                results[index] = {"ok": True, "text": text, "url": url}

    async def _run() -> list[dict[str, Any]]:
        tasks = [asyncio.create_task(_fetch_one(i, job)) for i, job in enumerate(jobs)]
        await asyncio.gather(*tasks)
        return [r for r in results if r is not None]

    started = time.perf_counter()
    await asyncio.wait_for(_run(), timeout=30)
    total_wall = time.perf_counter() - started

    # Per-host stats.
    per_host_times: dict[str, list[float]] = {}
    per_host_waits: dict[str, int] = {}
    for i, job in enumerate(jobs):
        url = str(job.get("url", ""))
        host = _host_for(url)
        per_host_times.setdefault(host, []).append(completed_at[i] - started)
        # A task that waited > 0 on the first sem (relative to the batch start) had to wait.
        wait = acquired_first_sem_at[i] - started
        if wait > 0.01:  # 10ms threshold for "waited"
            per_host_waits[host] = per_host_waits.get(host, 0) + 1

    per_host_mean = {
        host: sum(times) / len(times) if times else 0.0 for host, times in per_host_times.items()
    }
    fast_mean = sum(
        per_host_mean[h] for h in _FAST_HOSTS if h in per_host_mean and per_host_mean[h] > 0
    ) / len(_FAST_HOSTS)

    return {
        "total_wall_s": total_wall,
        "per_host_mean_wall": per_host_mean,
        "fast_host_mean_wall_s": fast_mean,
        "slow_host_mean_wall_s": per_host_mean.get(_SLOW_HOST, 0.0),
        "host_wait_count": per_host_waits,
        "total_jobs": len([r for r in results if r is not None]),
    }


def test_total_before_host_starves_fast_hosts_in_worst_case_ordering() -> None:
    """With total-first and slow jobs at the front, fast hosts are blocked.

    Slow jobs consume all total lanes first. Fast jobs cannot proceed until slow jobs finish.
    The fast-host mean wall time should be dominated by slow serial throughput.
    """
    jobs = _make_jobs(slow_first=True)
    result = asyncio.run(_run_batch(jobs, host_first=False))

    assert result["total_jobs"] == _SLOW_JOB_COUNT + _JOBS_PER_FAST_HOST * len(_FAST_HOSTS)
    # Fast hosts had to wait for slow jobs to drain through total sem.
    # With total=6, slow=12 at .2s each: 2 waves of 6 → ~.4s minimum for slow.
    # Fast jobs can only start after slow jobs release total slots.
    # Interleaving means some fast jobs might start earlier, but the mean should be high.
    fast_mean = result["fast_host_mean_wall_s"]
    # The most optimistic: some fast jobs fit between slow waves.
    # But at least some fast jobs wait for the slow host's entire serial throughput.
    assert fast_mean > 0.1, (
        f"total-before-host: fast host mean wall was {fast_mean:.3f}s, "
        f"expected at least some waiting"
    )
    # At least two fast hosts should have recorded a wait.
    waited = sum(1 for h in _FAST_HOSTS if result["host_wait_count"].get(h, 0) > 0)
    assert waited >= 2, (
        f"only {waited}/{len(_FAST_HOSTS)} fast hosts had tasks that waited on the total sem"
    )


def test_host_ready_reduces_fast_host_starvation() -> None:
    """With host-first and slow jobs at the front, fast hosts start without waiting for slow.

    Each host acquires its own host semaphore first. The total sem is only checked after, so
    fast hosts with available host capacity can compete for total lanes alongside slow-host jobs.
    """
    jobs = _make_jobs(slow_first=True)
    result = asyncio.run(_run_batch(jobs, host_first=True))

    assert result["total_jobs"] == _SLOW_JOB_COUNT + _JOBS_PER_FAST_HOST * len(_FAST_HOSTS)
    fast_mean = result["fast_host_mean_wall_s"]
    # With host-first, each fast host has 2 private slots. Their 4 instant jobs each take
    # 2 waves: fast tasks compete for total alongside slow, but host reservation means they
    # are first in line for total when slots free up.
    # At minimum: fast hosts don't wait for slow jobs to fully drain.
    assert fast_mean < 0.5, (
        f"host-ready: fast host mean wall was {fast_mean:.3f}s, "
        f"expected fast hosts to finish more quickly"
    )
    # Ideally fewer fast-host tasks wait at all.
    waited = sum(result["host_wait_count"].get(h, 0) for h in _FAST_HOSTS)
    # With host-ready, fewer fast tasks should experience a wait.
    total_fast = _JOBS_PER_FAST_HOST * len(_FAST_HOSTS)
    assert waited < total_fast, (
        f"all {waited}/{total_fast} fast tasks still waited even with host-ready ordering"
    )


def test_host_ready_fast_host_advantage_over_total_before_host() -> None:
    """Discriminating oracle: host-ready should give strictly lower fast-host mean wall time.

    This is the PASSING condition for the hypothesis. Run both orderings on the same worst-case
    job list and compare the fast-host mean wall time.
    """
    jobs = _make_jobs(slow_first=True)
    tb_result = asyncio.run(_run_batch(jobs, host_first=False))
    hr_result = asyncio.run(_run_batch(jobs, host_first=True))

    tb_fast = tb_result["fast_host_mean_wall_s"]
    hr_fast = hr_result["fast_host_mean_wall_s"]

    # The host-ready ordering should give strictly better (lower) fast-host mean wall time.
    improvement = (tb_fast - hr_fast) / tb_fast * 100 if tb_fast > 0 else 0
    assert hr_fast < tb_fast * 0.9, (
        f"fast-host mean wall: total-before-host={tb_fast:.3f}s, "
        f"host-ready={hr_fast:.3f}s, "
        f"improvement={improvement:.0f}% — "
        f"expected at least 10% improvement from host-ready ordering"
    )


def test_both_orderings_complete_all_jobs() -> None:
    """Control: both orderings complete every job within bounds."""
    jobs = _make_jobs(slow_first=True)
    tb_result = asyncio.run(_run_batch(jobs, host_first=False))
    hr_result = asyncio.run(_run_batch(jobs, host_first=True))

    expected = _SLOW_JOB_COUNT + _JOBS_PER_FAST_HOST * len(_FAST_HOSTS)
    assert tb_result["total_jobs"] == expected
    assert hr_result["total_jobs"] == expected
    # Total wall should be bounded by the slow host's serial throughput.
    slow_waves = _SLOW_JOB_COUNT / _PER_HOST_CAP
    expected_wall = slow_waves * _SLOW_DELAY_S
    assert tb_result["total_wall_s"] >= expected_wall * 0.8
    assert hr_result["total_wall_s"] >= expected_wall * 0.8
