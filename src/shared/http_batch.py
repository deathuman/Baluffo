"""Leaf bounded-concurrency page fetch helpers shared across stacks.

AI boundary owns: bounded concurrent HTTP fetch orchestration and per-request result shaping.
AI boundary implement in: this file for batch fetch behavior; callers own source-specific parsing and retries.
AI boundary search before contracts: source discovery fetchers, static-source adapters, and shared HTTP batch tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused shared HTTP batch tests.

The lane contract: `total_concurrency` is what the caller gets, on both paths. The async path is
one coroutine per job. The sync path runs the fetcher on a thread pool this module owns and sizes
to the batch, never on the event loop's shared default executor -- that executor is
`min(32, cpu_count + 4)` (20 on the 16-core box the audit runs on), so submitting to it silently
replaced the configured lane count with the machine's. See `_fetch_pages_batched_async`.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any
from urllib.parse import urlparse

from src.shared.page_fetch_lifecycle import (
    OwnedClientState,
    PageFetchLifecycle,
    PageFetchWorkTicket,
)

try:
    import httpx as httpx_mod
except ImportError:
    httpx: Any | None = None
else:
    httpx = httpx_mod

_EXPECTED_PAGE_FETCH_EXCEPTIONS: tuple[type[BaseException], ...] = (
    KeyError,
    OSError,
    RuntimeError,
    TimeoutError,
    ValueError,
)
if httpx is not None:
    # ponytail: httpx.InvalidURL subclasses Exception (not HTTPError) and escapes
    # redirect handling (e.g. a redirect Location with an empty path raised
    # "For absolute URLs, path must be empty or begin with '/'" mid-audit and
    # killed the whole discovery worker). It is a per-page fetch failure, not a
    # fetcher bug — isolate it to the row like other network errors.
    _EXPECTED_PAGE_FETCH_EXCEPTIONS = (
        *_EXPECTED_PAGE_FETCH_EXCEPTIONS,
        httpx.HTTPError,
        httpx.InvalidURL,
    )
_EXPECTED_PROGRESS_CALLBACK_EXCEPTIONS = (OSError, RuntimeError, TypeError, ValueError)


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
    receipt: Any | None = None,
    page_fetch_lifecycle: PageFetchLifecycle | None = None,
) -> list[dict[str, Any]]:
    if not jobs:
        if page_fetch_lifecycle is not None:
            http_client_module = httpx if callable(async_fetch) else None
            page_fetch_lifecycle.start_batch(
                transport="async" if http_client_module is not None else "sync",
                deadline_seconds=max(1.0, float(timeout_s)),
                owned_client_applicable=http_client_module is not None,
            )
            page_fetch_lifecycle.mark_batch_exit("completed")
        return []

    total_limit = max(1, int(total_concurrency))
    per_host_limit = max(1, int(per_host_concurrency))
    # `receipt` remains the frozen phaseRuntime metric contract. It records coroutine lane
    # occupancy only; physical page worker lifetimes are separately accounted by
    # `page_fetch_lifecycle` and must not be inferred from phase hooks.
    if receipt is not None:
        receipt.note_configured_lanes(total_limit)
        receipt.note_items(len(jobs))
    total_sem = asyncio.Semaphore(total_limit)
    host_sems: dict[str, asyncio.Semaphore] = {}
    results: list[dict[str, Any] | None] = [None] * len(jobs)

    def _host_for(url: str) -> str:
        host = urlparse(url).netloc.strip().lower()
        return host or "__default__"

    async def _call_fetch(
        client: Any,
        pool: Any,
        job: dict[str, Any],
        url: str,
        ticket: PageFetchWorkTicket | None,
    ) -> str:
        if callable(async_fetch) and client is not None:
            if ticket is None:
                return await async_fetch(client, job, url, timeout_s)
            ticket.start()
            failed = False
            try:
                return await async_fetch(client, job, url, timeout_s)
            except asyncio.CancelledError:
                raise
            except BaseException:
                failed = True
                raise
            finally:
                ticket.finish(failed=failed)
        if pool is None:  # pragma: no cover - the sync path is only entered with a pool
            # Loud on purpose: a None pool would submit to the loop's shared default executor and
            # silently replace the caller's lane count with `min(32, cpu_count + 4)` again.
            raise RuntimeError(
                "sync page fetch requires this batch's pool; refusing the shared default executor"
            )
        if ticket is None:
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(pool, sync_fetch, job, url, timeout_s)

        def _run_sync_fetch() -> str:
            ticket.start()
            failed = False
            try:
                return sync_fetch(job, url, timeout_s)
            except BaseException:
                failed = True
                raise
            finally:
                ticket.finish(failed=failed)

        try:
            worker_future = pool.submit(_run_sync_fetch)
        except BaseException:
            ticket.cancel_before_start()
            raise
        ticket.mark_worker_submitted()

        def _record_cancelled_future(future: Future[str]) -> None:
            if future.cancelled():
                ticket.cancel_before_start()

        worker_future.add_done_callback(_record_cancelled_future)
        return await asyncio.wrap_future(worker_future)

    async def _fetch_one(
        index: int,
        job: dict[str, Any],
        client: Any,
        pool: Any,
        ticket: PageFetchWorkTicket | None,
    ) -> tuple[int, dict[str, Any]]:
        url = str(job.get("url") or "").strip()
        host_sem = host_sems.setdefault(_host_for(url), asyncio.Semaphore(per_host_limit))
        # host-first acquisition: each host checks its own capacity before competing for total
        # lanes. This prevents one saturated host from occupying every total lane and starving
        # other hosts (see http_batch_fairness_experiment). The original total-before-host order
        # is preserved as commentary: `async with total_sem: async with host_sem:`.
        async with host_sem:
            async with total_sem:
                # Counted here rather than around the whole task: a row waiting on a semaphore is
                # not occupying a lane, and the lane occupancy number exists to say whether the
                # configured budget was met. These phaseRuntime hooks bracket the awaiting coroutine.
                # A sync worker may survive cancellation; pageFetchLifecycle tracks actual settlement.
                if receipt is not None:
                    receipt.note_lane_enter()
                try:
                    try:
                        text = await _call_fetch(client, pool, job, url, ticket)
                    except _EXPECTED_PAGE_FETCH_EXCEPTIONS as exc:
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
                finally:
                    if receipt is not None:
                        receipt.note_lane_leave()

    tasks: list[asyncio.Task[tuple[int, dict[str, Any]]]] = []
    tickets: list[PageFetchWorkTicket] = (
        [page_fetch_lifecycle.submit() for _ in jobs] if page_fetch_lifecycle is not None else []
    )

    def _observe_task_completion(
        task: asyncio.Task[tuple[int, dict[str, Any]]], ticket: PageFetchWorkTicket
    ) -> None:
        if task.cancelled() and not ticket.worker_was_submitted():
            ticket.cancel_before_start()
        ticket.finish_async_cleanup()

    async def _run(client: Any, pool: Any) -> list[dict[str, Any]]:
        for index, job in enumerate(jobs):
            ticket = tickets[index] if tickets else None
            task = asyncio.create_task(_fetch_one(index, job, client, pool, ticket))
            tasks.append(task)
            if ticket is not None:

                def _on_task_done(
                    done: asyncio.Task[tuple[int, dict[str, Any]]],
                    page_ticket: PageFetchWorkTicket = ticket,
                ) -> None:
                    _observe_task_completion(done, page_ticket)

                task.add_done_callback(_on_task_done)
        completed = 0
        total = len(jobs)
        for future in asyncio.as_completed(tasks):
            index, result = await future
            results[index] = result
            completed += 1
            if callable(progress_callback):
                try:
                    progress_callback(completed, total)
                except _EXPECTED_PROGRESS_CALLBACK_EXCEPTIONS:
                    pass
        return [result for result in results if isinstance(result, dict)]

    def _cancel_pending_tasks() -> None:
        for index, task in enumerate(tasks):
            ticket = tickets[index] if index < len(tickets) else None
            if not task.done() and task.cancel() and ticket is not None:
                ticket.request_async_cancel()
        for ticket in tickets[len(tasks) :]:
            ticket.cancel_before_start()

    # ponytail: httpx.Timeout(timeout_s) is a PER-OPERATION budget, not a total
    # one -- the read timeout re-arms on every chunk received. A slow-drip server
    # therefore holds a request open indefinitely: measured 40.7 s against a
    # Timeout(2.0) (see tests/test_shared_http_batch_total_deadline.py). Because
    # nothing raises, as_completed never yields, the per-host semaphores stay
    # held, and the whole batch parks with no error -- exactly how a GameDevMap
    # active-audit batch stalled at 1347/1349 in recovery_wave1_fetch and sat
    # silent for 900 s until the discovery quiet-guard failed the run.
    #
    # The deadline must NOT be timeout_s: that is a per-request budget, and a
    # legitimate batch is much larger. Measured batch 5 needed 20,117 ms for its
    # wave-1 recovery fetch against a 5 s per-request timeout, so bounding the
    # whole batch at 5 s would truncate healthy work. Instead allow every row to
    # spend its own timeout, divided by the concurrency actually available, plus
    # a small grace for scheduling.
    per_row_s = max(1.0, float(timeout_s))
    effective_concurrency = max(1, min(int(total_concurrency), max(1, len(jobs))))
    total_deadline_s = per_row_s * (len(jobs) / effective_concurrency) + per_row_s
    # ponytail: the sync path gets its own pool, sized to this batch. `asyncio.to_thread` submits
    # to the event loop's SHARED default executor, whose size is `min(32, cpu_count + 4)` -- 20 on
    # the 16-core box these runs use. A caller asking for 32 lanes therefore got 20, with no error
    # and no log line, and which regime a batch ran under came down to whether its fetcher was
    # `fetch_text` *by identity*: the GameDevMap audit's own bench wraps that fetcher to record
    # per-phase timings, so its homepage phase peaked at 20 in-flight of 32 configured and cost
    # 39.2 s where the unwrapped async path cost 21.5 s on the same 1,000 pages. The lane count is
    # a caller's budget, so it is now met on both paths.
    http_client_module = httpx if callable(async_fetch) else None
    if page_fetch_lifecycle is not None:
        page_fetch_lifecycle.start_batch(
            transport="async" if http_client_module is not None else "sync",
            deadline_seconds=total_deadline_s,
            owned_client_applicable=http_client_module is not None,
        )
    pool = (
        None
        if http_client_module is not None
        else ThreadPoolExecutor(max_workers=effective_concurrency, thread_name_prefix="page-fetch")
    )
    client_manager: Any | None = None
    client_entered = False
    try:
        if http_client_module is not None:
            client_manager = http_client_module.AsyncClient(
                timeout=http_client_module.Timeout(timeout_s)
            )
            try:
                async with client_manager as client:
                    client_entered = True
                    if page_fetch_lifecycle is not None:
                        page_fetch_lifecycle.set_owned_client_state("open")
                    try:
                        batch_results = await asyncio.wait_for(
                            _run(client, None), timeout=total_deadline_s
                        )
                    except TimeoutError:
                        _cancel_pending_tasks()
                        if page_fetch_lifecycle is not None:
                            page_fetch_lifecycle.mark_batch_exit("deadline_exceeded")
                        raise
                    finally:
                        if page_fetch_lifecycle is not None:
                            page_fetch_lifecycle.set_owned_client_state("close_requested")
                if page_fetch_lifecycle is not None:
                    page_fetch_lifecycle.set_owned_client_state("closed")
            except BaseException:
                if page_fetch_lifecycle is not None and client_entered:
                    is_closed = getattr(client_manager, "is_closed", None)
                    client_state: OwnedClientState
                    if is_closed is True:
                        client_state = "closed"
                    elif is_closed is False:
                        client_state = "close_failed"
                    else:
                        client_state = "unknown"
                    page_fetch_lifecycle.set_owned_client_state(client_state)
                raise
        else:
            batch_results = await asyncio.wait_for(_run(None, pool), timeout=total_deadline_s)
    except TimeoutError:
        _cancel_pending_tasks()
        if page_fetch_lifecycle is not None:
            page_fetch_lifecycle.mark_batch_exit("deadline_exceeded")
        if pool is not None:
            # The batch returns now. A sync worker already running cannot be cancelled and remains
            # outstanding in pageFetchLifecycle until its actual thread function settles.
            pool.shutdown(wait=False)
        return [
            result
            if isinstance(result, dict)
            else {
                "job": job,
                "payload": job.get("payload"),
                "url": str(job.get("url") or "").strip(),
                "ok": False,
                "text": "",
                "error": f"total batch deadline exceeded ({total_deadline_s:.0f}s)",
            }
            for result, job in zip(results, jobs, strict=False)
        ]
    except asyncio.CancelledError:
        _cancel_pending_tasks()
        if page_fetch_lifecycle is not None:
            page_fetch_lifecycle.mark_batch_exit("cancelled")
        if pool is not None:
            pool.shutdown(wait=False)
        raise
    except BaseException:
        _cancel_pending_tasks()
        if page_fetch_lifecycle is not None:
            page_fetch_lifecycle.mark_batch_exit("failed")
        if pool is not None:
            pool.shutdown(wait=False)
        raise
    if pool is not None:
        # Every submitted fetch has completed, so this is immediate: it retires the threads rather
        # than leaving one pool per batch alive for the life of the run.
        pool.shutdown(wait=True)
    if page_fetch_lifecycle is not None:
        page_fetch_lifecycle.mark_batch_exit("completed")
    return batch_results


def fetch_pages_batched(
    timeout_s: int,
    jobs: list[dict[str, Any]],
    *,
    sync_fetch: PageSyncFetch,
    async_fetch: PageAsyncFetch | None = None,
    total_concurrency: int,
    per_host_concurrency: int,
    progress_callback: PageProgressCallback | None = None,
    receipt: Any | None = None,
    page_fetch_lifecycle: PageFetchLifecycle | None = None,
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
                receipt=receipt,
                page_fetch_lifecycle=page_fetch_lifecycle,
            )
        )
    raise RuntimeError("fetch_pages_batched cannot run inside an active event loop")
