"""Jobs pipeline source execution loop helpers.

AI boundary owns: concurrent per-source execution loop, browser fallback coordination, and source runner dispatch.
AI boundary implement in: this file for source loop mechanics; result normalization and progress reporting stay in sibling leaves.
AI boundary search before contracts: pipeline stage source execution, source result helpers, adapter runners, and pipeline tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused pipeline source-loop tests.
"""

from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from typing import Any, Protocol, cast

from src.jobs.browser_fallback import BrowserFallbackCircuitBreaker
from src.jobs.models import CanonicalJob
from src.shared.profile_utils import run_profiled, run_profiled_alloc

from .pipeline_runtime_summary import PipelineTaskRuntime
from .pipeline_source_progress import (
    emit_progress_line,
    fallback_error_report,
    mark_task_finished,
    mark_task_started,
)
from .pipeline_source_results import execute_loader


class _RootLike(Protocol):
    def resolve_fetch_browser_fallback_helper(
        self,
    ) -> Callable[[str, int], tuple[str, str]] | None: ...

    def _build_capped_try_playwright(
        self,
        try_playwright: Callable[[str, int], tuple[str, str]],
        *,
        max_concurrent: int,
    ) -> Callable[[str, int], tuple[str, str]]: ...

    def set_browser_fallback_state(
        self,
        source_state_rows: dict[str, dict[str, Any]] | None,
        state_row: dict[str, Any],
    ) -> None: ...


root: _RootLike | None = None
_EXPECTED_PROFILED_SOURCE_FAILURES = (OSError, TimeoutError, ValueError)


def _root_module() -> _RootLike:
    if root is not None:
        return cast(_RootLike, root)
    from src import jobs_fetcher as jobs_fetcher_pkg

    return cast(_RootLike, jobs_fetcher_pkg)


def run_source_execution_stage(
    *,
    config,
    selected_loaders,
    fetch_text_limited,
    fetch_text_static_limited=None,
    static_listing_async_fetch=None,
    source_state_rows,
    redirect_resolver,
    task_runtime: PipelineTaskRuntime | None = None,
    task_rows,
    task_lock: Lock,
    thread_local,
    write_task_state,
    write_progress_report,
    canonical_rows: list[CanonicalJob],
    source_reports: list[dict[str, Any]],
) -> None:
    if task_runtime is None:
        task_runtime = PipelineTaskRuntime(
            task_lock=task_lock,
            task_rows=task_rows,
            thread_local=thread_local,
            current_phase_key="",
            current_phase_label="",
            current_output_count=0,
            show_progress=bool(config.show_progress),
        )
    root_mod = _root_module()
    browser_fallback_guard, guarded_try_playwright = _browser_fallback_runtime(
        root_mod,
        source_state_rows,
        cooldown_minutes=config.browser_fallback_cooldown_minutes,
        max_workers=config.max_workers,
        browser_fallback_max_workers=getattr(config, "browser_fallback_max_workers", -1),
    )
    _emit_browser_fallback_status(bool(config.show_progress), guarded_try_playwright, config)

    def execute_started(source_name, loader):
        return _execute_loader_started(
            source_name=source_name,
            loader=loader,
            config=config,
            fetch_text_limited=fetch_text_limited,
            fetch_text_static_limited=fetch_text_static_limited,
            static_listing_async_fetch=static_listing_async_fetch,
            source_state_rows=source_state_rows,
            redirect_resolver=redirect_resolver,
            task_runtime=task_runtime,
            task_rows=task_rows,
            task_lock=task_lock,
            thread_local=thread_local,
            write_task_state=write_task_state,
            write_progress_report=write_progress_report,
            guarded_try_playwright=guarded_try_playwright,
            show_progress=bool(config.show_progress),
        )

    write_progress_report(force=True)
    write_task_state(force=True)
    _run_selected_loaders(
        selected_loaders=selected_loaders,
        max_workers=config.max_workers,
        execute_loader_started=execute_started,
        canonical_rows=canonical_rows,
        source_reports=source_reports,
    )
    root_mod.set_browser_fallback_state(source_state_rows, browser_fallback_guard.to_state_row())


def _browser_fallback_runtime(
    root_mod: _RootLike,
    source_state_rows: dict[str, dict[str, Any]] | None,
    *,
    cooldown_minutes: int,
    max_workers: int,
    browser_fallback_max_workers: int = -1,
) -> tuple[BrowserFallbackCircuitBreaker, Callable[[str, int], tuple[str, str]] | None]:
    try_playwright = root_mod.resolve_fetch_browser_fallback_helper()
    browser_fallback_guard = BrowserFallbackCircuitBreaker.from_state(
        source_state_rows,
        cooldown_minutes=cooldown_minutes,
    )
    if try_playwright is None:
        return browser_fallback_guard, None
    fallback_cap = int(browser_fallback_max_workers or 0)
    if browser_fallback_max_workers == 0:
        return browser_fallback_guard, None
    if fallback_cap < 0:
        fallback_cap = int(max_workers or 1)
    capped_try_playwright = root_mod._build_capped_try_playwright(
        try_playwright,
        max_concurrent=max(1, min(fallback_cap, int(max_workers or 1))),
    )
    return browser_fallback_guard, browser_fallback_guard.wrap(capped_try_playwright)


def _emit_browser_fallback_status(
    show_progress: bool,
    guarded_try_playwright: Callable[[str, int], tuple[str, str]] | None,
    config: Any,
) -> None:
    if not show_progress:
        return
    if guarded_try_playwright is not None:
        fallback_cap = int(getattr(config, "browser_fallback_max_workers", -1))
        if fallback_cap < 0:
            fallback_cap = int(config.max_workers or 1)
        emit_progress_line(
            f"[jobs_fetcher] INFO browserFallbackEnabled=true browserFallbackCap={max(1, fallback_cap)}"
        )
        return
    emit_progress_line("[jobs_fetcher] INFO browserFallbackEnabled=false")


def _execute_loader_started(
    *,
    source_name,
    loader,
    config,
    fetch_text_limited,
    fetch_text_static_limited,
    static_listing_async_fetch,
    source_state_rows,
    redirect_resolver,
    task_runtime: PipelineTaskRuntime,
    task_rows,
    task_lock: Lock,
    thread_local,
    write_task_state,
    write_progress_report,
    guarded_try_playwright,
    show_progress: bool,
) -> tuple[dict[str, Any], list[CanonicalJob]]:
    mark_task_started(
        source_name=source_name,
        task_runtime=task_runtime,
        task_rows=task_rows,
        task_lock=task_lock,
        write_task_state=write_task_state,
        show_progress=show_progress,
    )

    def _invoke_run_profiled():
        return run_profiled(
            execute_loader,
            name=source_name,
            loader=loader,
            config=config,
            fetch_text_limited=fetch_text_limited,
            fetch_text_static_limited=fetch_text_static_limited,
            static_listing_async_fetch=static_listing_async_fetch,
            source_state_rows=source_state_rows,
            redirect_resolver=redirect_resolver,
            task_runtime=task_runtime,
            task_rows=task_rows,
            task_lock=task_lock,
            thread_local=thread_local,
            write_task_state=write_task_state,
            guarded_try_playwright=guarded_try_playwright,
            profile_name=f"adapter_{source_name}",
        )

    try:
        # ponytail: alloc profiling wraps cprofile (outermost) so per-source
        # peak-RSS capture includes any overhead of the cprofile shim itself.
        report, canonical_batch = run_profiled_alloc(
            _invoke_run_profiled,
            profile_name=f"adapter_alloc_{source_name}",
            source_name=source_name,
        )
    except _EXPECTED_PROFILED_SOURCE_FAILURES as exc:
        report = fallback_error_report(source_name, exc)
        canonical_batch = []
    mark_task_finished(
        source_name=source_name,
        report=report,
        task_runtime=task_runtime,
        task_rows=task_rows,
        task_lock=task_lock,
        write_progress_report=write_progress_report,
        write_task_state=write_task_state,
        show_progress=show_progress,
    )
    return report, canonical_batch


def _append_loader_result(
    result: tuple[dict[str, Any], list[CanonicalJob]],
    *,
    canonical_rows: list[CanonicalJob],
    source_reports: list[dict[str, Any]],
) -> None:
    report, canonical_batch = result
    canonical_rows.extend(canonical_batch)
    source_reports.append(report)


def _run_selected_loaders(
    *,
    selected_loaders,
    max_workers: int,
    execute_loader_started,
    canonical_rows: list[CanonicalJob],
    source_reports: list[dict[str, Any]],
) -> None:
    if max_workers <= 1 or len(selected_loaders) <= 1:
        for source_name, loader in selected_loaders:
            _append_loader_result(
                execute_loader_started(source_name, loader),
                canonical_rows=canonical_rows,
                source_reports=source_reports,
            )
        return

    # ponytail: bound the live future set. Submitting all 2k loaders at once pins
    # every queued adapter/future in memory and can push the fetch container over
    # the 1.5 GiB cap before fetch finishes.
    window = max(8, max_workers * 4)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for start in range(0, len(selected_loaders), window):
            futures = {
                executor.submit(execute_loader_started, source_name, loader): source_name
                for source_name, loader in selected_loaders[start : start + window]
            }
            for future in as_completed(futures):
                _append_loader_result(
                    future.result(),
                    canonical_rows=canonical_rows,
                    source_reports=source_reports,
                )
