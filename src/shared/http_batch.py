from __future__ import annotations

"""Leaf bounded-concurrency page fetch helpers shared across stacks."""

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any
from urllib.parse import urlparse

try:
    import httpx as httpx_mod
except ImportError:
    httpx: Any | None = None
else:
    httpx = httpx_mod


PageSyncFetch = Callable[[dict[str, Any], str, int], str]
PageAsyncFetch = Callable[[Any, dict[str, Any], str, int], Awaitable[str]]
PageProgressCallback = Callable[[int, int], None]


async def _fetch_pages_batched_async(
    timeout_s: int,
    jobs: list[dict[str, Any]],
    *,
    sync_fetch: PageSyncFetch,
    async_fetch: PageAsyncFetch | None,
    total_concurrency: int,
    per_host_concurrency: int,
    progress_callback: PageProgressCallback | None,
) -> list[dict[str, Any]]:
    if not jobs:
        return []

    total_limit = max(1, int(total_concurrency))
    per_host_limit = max(1, int(per_host_concurrency))
    total_sem = asyncio.Semaphore(total_limit)
    host_sems: dict[str, asyncio.Semaphore] = {}
    results: list[dict[str, Any] | None] = [None] * len(jobs)

    def _host_for(url: str) -> str:
        host = urlparse(url).netloc.strip().lower()
        return host or "__default__"

    async def _call_fetch(client: Any, job: dict[str, Any], url: str) -> str:
        if callable(async_fetch) and client is not None:
            return await async_fetch(client, job, url, timeout_s)
        return await asyncio.to_thread(sync_fetch, job, url, timeout_s)

    async def _fetch_one(
        index: int, job: dict[str, Any], client: Any
    ) -> tuple[int, dict[str, Any]]:
        url = str(job.get("url") or "").strip()
        host_sem = host_sems.setdefault(_host_for(url), asyncio.Semaphore(per_host_limit))
        async with total_sem:
            async with host_sem:
                try:
                    text = await _call_fetch(client, job, url)
                except Exception as exc:  # noqa: BLE001
                    return index, {
                        "job": job,
                        "payload": job.get("payload"),
                        "url": url,
                        "ok": False,
                        "text": "",
                        "error": str(exc),
                    }
                return index, {
                    "job": job,
                    "payload": job.get("payload"),
                    "url": url,
                    "ok": True,
                    "text": text,
                    "error": "",
                }

    async def _run(client: Any) -> list[dict[str, Any]]:
        tasks = [
            asyncio.create_task(_fetch_one(index, job, client)) for index, job in enumerate(jobs)
        ]
        completed = 0
        total = len(jobs)
        for future in asyncio.as_completed(tasks):
            index, result = await future
            results[index] = result
            completed += 1
            if callable(progress_callback):
                try:
                    progress_callback(completed, total)
                except Exception:  # noqa: BLE001
                    pass
        return [result for result in results if isinstance(result, dict)]

    if callable(async_fetch) and httpx is not None:
        async with httpx.AsyncClient(timeout=httpx.Timeout(timeout_s)) as client:
            return await _run(client)
    return await _run(None)


def fetch_pages_batched(
    timeout_s: int,
    jobs: list[dict[str, Any]],
    *,
    sync_fetch: PageSyncFetch,
    async_fetch: PageAsyncFetch | None = None,
    total_concurrency: int,
    per_host_concurrency: int,
    progress_callback: PageProgressCallback | None = None,
) -> list[dict[str, Any]]:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(
            _fetch_pages_batched_async(
                timeout_s,
                jobs,
                sync_fetch=sync_fetch,
                async_fetch=async_fetch,
                total_concurrency=total_concurrency,
                per_host_concurrency=per_host_concurrency,
                progress_callback=progress_callback,
            )
        )
    raise RuntimeError("fetch_pages_batched cannot run inside an active event loop")
