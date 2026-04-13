from __future__ import annotations

"""Shared bounded-concurrency fetch helpers for directory adapters."""

import asyncio
import os
from typing import Any
from urllib.parse import urlparse

import httpx

from .web_search import async_fetch_text_httpx, fetch_text


def _env_int(name: str, default: int) -> int:
    raw = str(os.getenv(name) or "").strip()
    try:
        return max(1, int(raw)) if raw else int(default)
    except ValueError:
        return int(default)


def directory_fetch_concurrency_defaults() -> dict[str, int]:
    return {
        "total": _env_int("BALUFFO_DISCOVERY_DIRECTORY_FETCH_CONCURRENCY_TOTAL", 16),
        "perHost": _env_int("BALUFFO_DISCOVERY_DIRECTORY_FETCH_CONCURRENCY_PER_HOST", 2),
    }


def resolve_directory_fetch_limits(config: dict[str, Any] | None = None) -> tuple[int, int]:
    cfg = config if isinstance(config, dict) else {}
    defaults = directory_fetch_concurrency_defaults()
    try:
        total = max(0, int(cfg.get("fetchConcurrency") or 0))
    except (TypeError, ValueError):
        total = 0
    try:
        per_host = max(0, int(cfg.get("perHostConcurrency") or 0))
    except (TypeError, ValueError):
        per_host = 0
    return (
        total or int(defaults["total"]),
        per_host or int(defaults["perHost"]),
    )


async def _fetch_directory_pages_async(
    timeout_s: int,
    jobs: list[dict[str, Any]],
    *,
    fetcher,
    total_concurrency: int,
    per_host_concurrency: int,
    progress_label: str,
    progress_every: int,
) -> list[dict[str, Any]]:
    from .reporting import emit_log

    if not jobs:
        return []

    total_limit = max(1, int(total_concurrency))
    per_host_limit = max(1, int(per_host_concurrency))
    report_every = max(0, int(progress_every))
    total_sem = asyncio.Semaphore(total_limit)
    host_sems: dict[str, asyncio.Semaphore] = {}
    progress_name = str(progress_label or "").strip()
    results: list[dict[str, Any] | None] = [None] * len(jobs)

    def _host_for(url: str) -> str:
        host = urlparse(url).netloc.strip().lower()
        return host or "__default__"

    async def _call_fetch(client: httpx.AsyncClient | None, url: str) -> str:
        if fetcher is not fetch_text:
            return await asyncio.to_thread(fetcher, url, timeout_s)
        if client is None:
            raise RuntimeError("shared directory fetch client unavailable")
        return await async_fetch_text_httpx(client, url, timeout_s)

    async def _fetch_one(
        index: int, job: dict[str, Any], client: httpx.AsyncClient | None
    ) -> tuple[int, dict[str, Any]]:
        url = str(job.get("url") or "").strip()
        payload = job.get("payload")
        host_sem = host_sems.setdefault(_host_for(url), asyncio.Semaphore(per_host_limit))
        async with total_sem:
            async with host_sem:
                try:
                    text = await _call_fetch(client, url)
                except Exception as exc:  # noqa: BLE001
                    return index, {
                        "job": job,
                        "payload": payload,
                        "url": url,
                        "ok": False,
                        "text": "",
                        "error": str(exc),
                        "failure": {
                            "name": str(job.get("name") or url),
                            "adapter": str(job.get("adapter") or ""),
                            "error": str(exc),
                            "stage": str(job.get("failureStage") or ""),
                        },
                    }
                return index, {
                    "job": job,
                    "payload": payload,
                    "url": url,
                    "ok": True,
                    "text": text,
                    "error": "",
                    "failure": None,
                }

    async def _run(client: httpx.AsyncClient | None) -> list[dict[str, Any]]:
        tasks = [
            asyncio.create_task(_fetch_one(index, job, client)) for index, job in enumerate(jobs)
        ]
        completed = 0
        for future in asyncio.as_completed(tasks):
            index, result = await future
            results[index] = result
            completed += 1
            if (
                progress_name
                and report_every
                and (completed == len(jobs) or completed % report_every == 0)
            ):
                emit_log(f"{progress_name}: fetched {completed}/{len(jobs)} pages.")
        return [result for result in results if isinstance(result, dict)]

    if fetcher is fetch_text:
        async with httpx.AsyncClient(timeout=httpx.Timeout(timeout_s)) as client:
            return await _run(client)
    return await _run(None)


def fetch_directory_pages(
    timeout_s: int,
    jobs: list[dict[str, Any]],
    *,
    fetcher=fetch_text,
    total_concurrency: int,
    per_host_concurrency: int,
    progress_label: str,
    progress_every: int = 25,
) -> list[dict[str, Any]]:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(
            _fetch_directory_pages_async(
                timeout_s,
                jobs,
                fetcher=fetcher,
                total_concurrency=total_concurrency,
                per_host_concurrency=per_host_concurrency,
                progress_label=progress_label,
                progress_every=progress_every,
            )
        )
    raise RuntimeError("fetch_directory_pages cannot run inside an active event loop")
