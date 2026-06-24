from __future__ import annotations

"""Shared bounded-concurrency fetch helpers for directory adapters."""

from typing import Any

from src.shared.http_batch import fetch_pages_batched

from .config import env_int
from .web_search_fetch import async_fetch_text_httpx, fetch_text


def directory_fetch_concurrency_defaults() -> dict[str, int]:
    return {
        "total": env_int("BALUFFO_DISCOVERY_DIRECTORY_FETCH_CONCURRENCY_TOTAL", 16),
        "perHost": env_int("BALUFFO_DISCOVERY_DIRECTORY_FETCH_CONCURRENCY_PER_HOST", 2),
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


def fetch_directory_pages(
    timeout_s: int,
    jobs: list[dict[str, Any]],
    *,
    fetcher=fetch_text,
    total_concurrency: int,
    per_host_concurrency: int,
    progress_label: str,
    progress_every: int = 25,
    progress_callback: Any | None = None,
    emit_progress_log: bool = True,
) -> list[dict[str, Any]]:
    from .reporting import emit_log

    report_every = max(0, int(progress_every))
    progress_name = str(progress_label or "").strip()

    def _progress(completed: int, total: int) -> None:
        if (
            emit_progress_log
            and progress_name
            and report_every
            and (completed == total or completed % report_every == 0)
        ):
            emit_log(f"{progress_name}: fetched {completed}/{total} pages.")
        if progress_callback is not None:
            progress_callback({"completed": completed, "total": total, "label": progress_name})

    results = fetch_pages_batched(
        timeout_s,
        jobs,
        sync_fetch=lambda job, url, _timeout_s: fetcher(url, _timeout_s),
        async_fetch=(
            (lambda client, job, url, _timeout_s: async_fetch_text_httpx(client, url, _timeout_s))
            if fetcher is fetch_text
            else None
        ),
        total_concurrency=total_concurrency,
        per_host_concurrency=per_host_concurrency,
        progress_callback=_progress,
    )
    normalized_results: list[dict[str, Any]] = []
    for result in results:
        row = dict(result)
        url = str(row.get("url") or "").strip()
        if bool(row.get("ok")):
            row["failure"] = None
        else:
            job_value = row.get("job")
            job = job_value if isinstance(job_value, dict) else {}
            error = str(row.get("error") or "")
            row["failure"] = {
                "name": str(job.get("name") or url),
                "adapter": str(job.get("adapter") or ""),
                "error": error,
                "stage": str(job.get("failureStage") or ""),
                "recoveryUrlSource": str(job.get("recoveryUrlSource") or ""),
                "recoveryUrlPath": str(job.get("recoveryUrlPath") or ""),
            }
        normalized_results.append(row)
    return normalized_results
