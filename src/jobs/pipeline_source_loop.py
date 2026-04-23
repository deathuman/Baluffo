from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from types import SimpleNamespace
from typing import Any

from src.jobs.browser_fallback import BrowserFallbackCircuitBreaker
from src.jobs.models import CanonicalJob

from .pipeline_runtime_summary import PipelineTaskRuntime
from .pipeline_source_progress import (
    emit_progress_line,
    fallback_error_report,
    mark_task_finished,
    mark_task_started,
)
from .pipeline_source_results import execute_loader

root = None


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
        task_runtime = SimpleNamespace(
            task_lock=task_lock,
            task_rows=task_rows,
            recent_events=[],
            run_id="",
            current_phase_key="",
            current_phase_label="",
            current_output_count=0,
            show_progress=bool(config.show_progress),
        )
    try_playwright = root.resolve_fetch_browser_fallback_helper()
    browser_fallback_guard = BrowserFallbackCircuitBreaker.from_state(
        source_state_rows, cooldown_minutes=config.browser_fallback_cooldown_minutes
    )
    capped_try_playwright = (
        root._build_capped_try_playwright(
            try_playwright,
            max_concurrent=config.max_workers,
        )
        if try_playwright is not None
        else None
    )
    guarded_try_playwright = (
        browser_fallback_guard.wrap(capped_try_playwright)
        if capped_try_playwright is not None
        else None
    )
    if config.show_progress:
        if guarded_try_playwright is not None:
            emit_progress_line(
                f"[jobs_fetcher] INFO browserFallbackEnabled=true browserFallbackCap={max(1, int(config.max_workers or 1))}"
            )
        else:
            emit_progress_line("[jobs_fetcher] INFO browserFallbackEnabled=false")

    def _execute_loader_started(source_name, loader):
        mark_task_started(
            source_name=source_name,
            task_runtime=task_runtime,
            task_rows=task_rows,
            task_lock=task_lock,
            write_task_state=write_task_state,
            show_progress=bool(config.show_progress),
        )
        try:
            report, canonical_batch = execute_loader(
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
            )
        except Exception as exc:  # noqa: BLE001
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
            show_progress=bool(config.show_progress),
        )
        return report, canonical_batch

    def run_stage() -> None:
        if config.max_workers <= 1 or len(selected_loaders) <= 1:
            for source_name, loader in selected_loaders:
                report, canonical_batch = _execute_loader_started(source_name, loader)
                canonical_rows.extend(canonical_batch)
                source_reports.append(report)
            return

        with ThreadPoolExecutor(max_workers=config.max_workers) as executor:
            futures = {}
            for source_name, loader in selected_loaders:
                futures[executor.submit(_execute_loader_started, source_name, loader)] = source_name
            for future in as_completed(futures):
                report, canonical_batch = future.result()
                canonical_rows.extend(canonical_batch)
                source_reports.append(report)

    write_progress_report(force=True)
    write_task_state(force=True)
    run_stage()
    root.set_browser_fallback_state(source_state_rows, browser_fallback_guard.to_state_row())
