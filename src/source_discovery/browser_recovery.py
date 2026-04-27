from __future__ import annotations

"""Shared browser-recovery mechanics for source-discovery audit artifacts."""

import asyncio
import time
from typing import Any

from src.shared.utils import now_iso

BrowserFetchResult = tuple[dict[str, Any], str, str, int]


def browser_recovery_processed_key(row: dict[str, Any]) -> str:
    url = str(row.get("url") or "").strip()
    if url:
        return f"url:{url}"
    entry_url = str(row.get("sourceDirectoryEntryUrl") or "").strip()
    if entry_url:
        return f"entry:{entry_url}"
    return str(row.get("name") or "").strip()


def processed_keys(browser_recovery: dict[str, Any]) -> set[str]:
    return {
        str(item).strip()
        for item in list(browser_recovery.get("processedKeys") or [])
        if str(item).strip()
    }


def select_unprocessed_candidates(
    rows: list[dict[str, Any]],
    *,
    browser_recovery: dict[str, Any],
    limit: int = 0,
) -> tuple[list[dict[str, Any]], set[str]]:
    processed = processed_keys(browser_recovery)
    candidates = [
        dict(row)
        for row in rows
        if isinstance(row, dict)
        and browser_recovery_processed_key(row)
        and browser_recovery_processed_key(row) not in processed
    ]
    capped = candidates[: max(0, int(limit or 0))] if int(limit or 0) > 0 else candidates
    return capped, processed


async def fetch_browser_recovery_pages_async(
    rows: list[dict[str, Any]],
    *,
    timeout_s: int,
    browser_fetcher,
    concurrency: int,
) -> list[BrowserFetchResult]:
    sem = asyncio.Semaphore(max(1, int(concurrency or 1)))

    async def _one(row: dict[str, Any]) -> BrowserFetchResult:
        url = str(row.get("url") or "").strip()
        async with sem:
            started = time.perf_counter()
            html, error = await asyncio.to_thread(browser_fetcher, url, timeout_s)
            duration_ms = max(0, int((time.perf_counter() - started) * 1000))
            return row, str(html or ""), str(error or ""), duration_ms

    tasks = [asyncio.create_task(_one(row)) for row in rows]
    results: list[BrowserFetchResult] = []
    for fut in asyncio.as_completed(tasks):
        results.append(await fut)
    return results


def append_fetch_sample(
    browser_recovery: dict[str, Any],
    *,
    source_url: str,
    duration_ms: int,
    html: str,
    limit: int = 25,
) -> None:
    samples = list(browser_recovery.get("fetchSamples") or [])
    if len(samples) < max(0, int(limit or 0)):
        samples.append({"url": source_url, "durationMs": int(duration_ms), "htmlBytes": len(html)})
    browser_recovery["fetchSamples"] = samples[: max(0, int(limit or 0))]


def append_failure_sample(
    browser_recovery: dict[str, Any],
    sample: dict[str, Any],
    *,
    limit: int = 25,
) -> None:
    samples = list(browser_recovery.get("failureSamples") or [])
    if len(samples) < max(0, int(limit or 0)):
        samples.append(dict(sample))
    browser_recovery["failureSamples"] = samples[: max(0, int(limit or 0))]


def update_browser_recovery_state(
    browser_recovery: dict[str, Any],
    *,
    processed: set[str],
    started: float,
    candidate_count: int,
    **counts: int,
) -> None:
    browser_recovery.update(
        {
            "processedKeys": sorted(str(key) for key in processed if str(key).strip()),
            "processedCount": len(processed),
            "lastRunAt": now_iso(),
            "lastDurationMs": max(0, int((time.perf_counter() - started) * 1000)),
            "candidateCount": max(0, int(candidate_count or 0)),
        }
    )
    for key, value in counts.items():
        browser_recovery[str(key)] = max(0, int(value or 0))
