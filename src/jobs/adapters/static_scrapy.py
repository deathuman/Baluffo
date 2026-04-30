"""Scrapy-only path for static adapter: runner invocation, envelope handling, result parsing."""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import time
from collections.abc import Callable
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from pathlib import Path
from typing import Any, cast

from src.jobs.common.datetime_utils import to_iso
from src.jobs.common.diagnostics import set_source_diagnostics
from src.jobs.common.taxonomy import (
    ClassificationContext,
    classify_zero_kept,
    map_error_to_failure_bucket,
)
from src.jobs.models import RawJob
from src.jobs.registry import registry_entries
from src.jobs.text_utils import clean_text, norm_text, normalize_url
from src.shared.utils import coerce_int, env_flag

from ..common import config as common_config
from .static_runtime_support import _as_dict, update_source_detail_taxonomy

TIMEOUT_BUCKET_SOURCE_NAMES = {
    "andarion games gmbh (gamesmap)",
    "kevuru games (manual website)",
    "tequilaworks (manual website)",
}
SCRAPY_STATIC_QUEUE_MAX_WORKERS = 4
SCRAPY_STATIC_QUEUE_POLL_S = 0.5
SCRAPY_STATIC_QUEUE_WAIT_PROGRESS_S = 5.0
SCRAPY_STATS_INT_FIELDS = (
    "downloader/request_count",
    "downloader/response_count",
    "downloader/response_status_count/200",
    "retry/count",
    "item_scraped_count",
    "candidate_links_found",
    "detail_pages_visited",
    "jobs_emitted",
    "jobs_rejected_validation",
)


def _as_list(value: object) -> list[Any]:
    return cast(list[Any], value) if isinstance(value, list) else []


def _page_text_list(value: object) -> list[str]:
    return [text for item in _as_list(value) if (text := clean_text(item))]


def _clean_errors(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    return [text for item in values if (text := clean_text(item))]


def _base_detail(
    source_row: dict[str, Any],
    *,
    status: str = "error",
    error: str = "",
    signal_quality: str = "weak",
) -> dict[str, Any]:
    source_name = clean_text(source_row.get("name")) or "unknown"
    studio_name = clean_text(source_row.get("studio")) or source_name
    pages = _page_text_list(source_row.get("pages"))
    source_id = clean_text(source_row.get("id"))
    if not source_id:
        seed = "|".join([source_name, studio_name, *pages])
        source_id = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]
    classification = "parse_error" if norm_text(status) == "error" else "ok_no_jobs"
    context = ClassificationContext(
        status=status,
        error=error or "",
        classification=classification,
        signal_quality=signal_quality,
    )
    failure_bucket = map_error_to_failure_bucket(context)
    zero_kept_classification = None
    if classification in ("ok_no_jobs", "parser_stale") or status == "ok":
        zero_kept_class = classify_zero_kept(context)
        zero_kept_classification = zero_kept_class.value
    return {
        "adapter": "scrapy_static",
        "studio": studio_name,
        "name": source_name,
        "status": status,
        "fetchedCount": 0,
        "keptCount": 0,
        "error": clean_text(error),
        "classification": classification,
        "failureBucket": failure_bucket.value,
        "zeroKeptClassification": zero_kept_classification,
        "top_reject_reasons": [],
        "deadListingPageCount": 0,
        "deadListingPageExamples": [],
        "browserFallbackRecommended": False,
        "signalQuality": clean_text(signal_quality) or "weak",
        "sourceId": source_id,
        "pages": pages,
        "loss": {
            "scrapyRunnerRejectedValidation": 0,
            "scrapyParentInvalidPayload": 0,
            "scrapyDeadListingPageRejected": 0,
        },
    }


def _runner_command(runner_path: Path) -> list[str]:
    if getattr(sys, "frozen", False):
        src_root = Path(__file__).resolve().parents[2]
        runtime_root = src_root.parent
        relative_runner = runner_path.relative_to(src_root).as_posix()
        return [
            sys.executable,
            "__child_script__",
            "--root",
            str(runtime_root),
            "--script",
            relative_runner,
            "--",
        ]
    return [sys.executable, str(runner_path)]


def _coerce_int(value: Any) -> int:
    return coerce_int(value, 0, minimum=0, maximum=2**31 - 1)


def _normalize_job(raw: Any, source_row: dict[str, Any]) -> RawJob | None:
    if not isinstance(raw, dict):
        return None
    strict_validation = env_flag(
        "BALUFFO_SCRAPY_VALIDATION_STRICT", common_config.DEFAULT_SCRAPY_VALIDATION_STRICT
    )
    source_name = clean_text(raw.get("source")) or (
        clean_text(source_row.get("name")) or "scrapy_static"
    )
    studio_name = clean_text(raw.get("studio")) or (
        clean_text(source_row.get("studio")) or clean_text(source_row.get("name")) or "unknown"
    )
    title = clean_text(raw.get("title"))
    company = clean_text(raw.get("company"))
    job_link = normalize_url(raw.get("jobLink"))
    source_job_id = clean_text(raw.get("sourceJobId"))
    if not title or not company:
        return None
    if not job_link and not strict_validation:
        job_link = _job_link_from_source_bundle(raw.get("sourceBundle"))
    if not job_link:
        return None
    if not source_job_id:
        source_job_id = hashlib.sha1(f"{title}|{company}|{job_link}".encode()).hexdigest()[:12]
    posted_at = to_iso(raw.get("postedAt"))
    source_bundle = raw.get("sourceBundle")
    if not isinstance(source_bundle, list) or not source_bundle:
        source_bundle = [
            {
                "source": source_name,
                "sourceJobId": source_job_id,
                "jobLink": job_link,
                "postedAt": posted_at,
                "adapter": "scrapy_static",
                "studio": studio_name,
            }
        ]

    return {
        "sourceJobId": source_job_id,
        "title": title,
        "company": company,
        "city": clean_text(raw.get("city")),
        "country": clean_text(raw.get("country")) or "Unknown",
        "workType": clean_text(raw.get("workType")),
        "contractType": clean_text(raw.get("contractType")),
        "jobLink": job_link,
        "sector": clean_text(raw.get("sector")) or "Game",
        "postedAt": posted_at,
        "source": source_name,
        "studio": studio_name,
        "adapter": clean_text(raw.get("adapter")) or "scrapy_static",
        "sourceBundle": source_bundle,
    }


def _job_link_from_source_bundle(source_bundle: Any) -> str:
    if not isinstance(source_bundle, list):
        return ""
    for item in source_bundle:
        if isinstance(item, dict) and (candidate := normalize_url(item.get("jobLink"))):
            return candidate
    return ""


def _build_runner_config(
    source: dict[str, Any], *, timeout_s: int, retries: int, backoff_s: float
) -> tuple[dict[str, Any], bool]:
    source_name = clean_text(source.get("name")) or "unknown"
    studio_name = clean_text(source.get("studio")) or source_name
    pages = _as_list(source.get("pages"))
    source_config: dict[str, Any] = {
        "name": source_name,
        "studio": studio_name,
        "pages": pages,
        "nlPriority": bool(source.get("nlPriority", False)),
    }
    runtime_config: dict[str, Any] = {
        "timeout_s": int(timeout_s),
        "retries": int(retries),
        "backoff_s": float(backoff_s),
        "download_delay": 1.0,
        "use_browser": True,
    }
    config: dict[str, dict[str, Any]] = {"source": source_config, "runtime": runtime_config}
    timeout_bucket = source_name.lower() in TIMEOUT_BUCKET_SOURCE_NAMES
    if timeout_bucket:
        runtime_config["timeout_s"] = min(int(timeout_s), 10)
    return config, timeout_bucket


def _child_timeout_window_s(*, source_name: str, timeout_s: int, pages: list[Any]) -> int:
    timeout_bucket = clean_text(source_name).lower() in TIMEOUT_BUCKET_SOURCE_NAMES
    effective_timeout_s = min(int(timeout_s), 10) if timeout_bucket else int(timeout_s)
    return min(
        90 if timeout_bucket else 300,
        max(1, int(effective_timeout_s)) * max(1, len(pages)) * 4,
    )


def _finalize_source_detail(source_detail: dict[str, Any]) -> None:
    update_source_detail_taxonomy(
        source_detail, include_browser_escalation=False, skip_dead_listing=True
    )


def _collect_normalized_jobs(
    jobs: list[Any], source: dict[str, Any], *, source_name: str
) -> tuple[list[RawJob], int, list[str]]:
    rows: list[RawJob] = []
    errors: list[str] = []
    invalid = 0
    for item in jobs:
        normalized = _normalize_job(item, source)
        if normalized:
            rows.append(normalized)
        else:
            invalid += 1
            errors.append(f"{source_name}: dropped invalid job payload from runner")
    return rows, invalid, errors


def _reject_invalid_envelope(
    envelope: Any, source_detail: dict[str, Any], source_errors: list[str], *, source_name: str
) -> bool:
    if isinstance(envelope, dict) and "ok" in envelope:
        return False
    source_detail.update(
        {
            "status": "error",
            "error": "Invalid envelope from scraper runner",
            "classification": "parse_error",
            "browserFallbackRecommended": False,
        }
    )
    problem = "type" if not isinstance(envelope, dict) else "missing 'ok'"
    source_errors.append(f"{source_name}: invalid envelope {problem}")
    _finalize_source_detail(source_detail)
    return True


def _run_runner_envelope(
    *,
    source_name: str,
    pages: list[Any],
    runner_path: Path,
    config: dict[str, Any],
    timeout_s: int,
) -> tuple[Any, list[str]]:
    source_errors: list[str] = []
    result = subprocess.run(
        _runner_command(runner_path),
        input=json.dumps(config).encode("utf-8"),
        capture_output=True,
        timeout=_child_timeout_window_s(
            source_name=source_name,
            timeout_s=timeout_s,
            pages=pages,
        ),
        check=False,
    )
    stderr_text = clean_text(result.stderr.decode("utf-8", errors="replace"))
    if result.returncode != 0:
        source_errors.append(f"{source_name}: subprocess exit {result.returncode}")
    if stderr_text and result.returncode != 0:
        source_errors.append(f"{source_name}: stderr: {stderr_text[:500]}")

    stdout_text = result.stdout.decode("utf-8", errors="replace")
    try:
        return json.loads(stdout_text), source_errors
    except json.JSONDecodeError as exc:
        source_errors.append(f"{source_name}: JSON parse error: {exc}")
        if stderr_text:
            source_errors.append(f"{source_name}: stderr: {stderr_text[:500]}")
        return {}, source_errors


def _merge_envelope_detail(source_detail: dict[str, Any], envelope_dict: dict[str, Any]) -> None:
    envelope_details = _as_list(envelope_dict.get("details"))
    detail_0 = _as_dict(envelope_details[0]) if envelope_details else {}
    if not detail_0:
        return
    source_detail.update(
        {
            "status": "ok" if clean_text(detail_0.get("status")).lower() == "ok" else "error",
            "fetchedCount": _coerce_int(detail_0.get("fetchedCount")),
            "keptCount": _coerce_int(detail_0.get("keptCount")),
            "error": clean_text(detail_0.get("error")),
            "classification": clean_text(detail_0.get("classification"))
            or source_detail.get("classification"),
            "browserFallbackRecommended": bool(detail_0.get("browserFallbackRecommended")),
            "top_reject_reasons": _as_list(detail_0.get("top_reject_reasons")),
            "deadListingPageCount": _coerce_int(detail_0.get("deadListingPageCount")),
            "deadListingPageExamples": _as_list(detail_0.get("deadListingPageExamples")),
            "sourceId": clean_text(detail_0.get("sourceId")) or source_detail.get("sourceId"),
            "pages": _page_text_list(detail_0.get("pages")) or source_detail.get("pages"),
        }
    )


def _merge_envelope_jobs(
    *,
    envelope_dict: dict[str, Any],
    source_detail: dict[str, Any],
    source: dict[str, Any],
    source_name: str,
) -> tuple[list[RawJob], list[str]]:
    jobs = _as_list(envelope_dict.get("jobs"))
    if not (bool(envelope_dict.get("ok")) and jobs):
        source_detail["status"] = "error"
        if not clean_text(source_detail.get("error")):
            source_detail["error"] = "crawl failed"
        source_detail["classification"] = "parse_error"
        return [], [f"{source_name}: crawl failed"]

    source_rows, parent_invalid_payload, job_errors = _collect_normalized_jobs(
        jobs, source, source_name=source_name
    )
    kept = len(source_rows)
    source_detail_loss = _as_dict(source_detail.get("loss"))
    source_detail_loss["scrapyParentInvalidPayload"] = int(parent_invalid_payload)
    source_detail["loss"] = source_detail_loss
    source_detail["keptCount"] = max(int(source_detail.get("keptCount") or 0), kept)
    source_detail["status"] = "ok"
    if not clean_text(source_detail.get("classification")):
        source_detail["classification"] = "ok_with_jobs" if kept > 0 else "ok_no_jobs"
    source_detail["browserFallbackRecommended"] = False
    return source_rows, job_errors


def _merge_envelope_stats(source_detail: dict[str, Any], envelope_dict: dict[str, Any]) -> None:
    stats = _as_dict(envelope_dict.get("stats"))
    if not stats:
        return
    stats_payload = {key: _coerce_int(stats.get(key)) for key in SCRAPY_STATS_INT_FIELDS}
    stats_payload["finish_reason"] = clean_text(stats.get("finish_reason"))
    source_detail["stats"] = stats_payload
    source_detail_loss = _as_dict(source_detail.get("loss"))
    source_detail_loss["scrapyRunnerRejectedValidation"] = _coerce_int(
        stats.get("jobs_rejected_validation")
    )
    source_detail_loss["scrapyDeadListingPageRejected"] = _coerce_int(
        stats.get("dead_listing_pages_rejected")
    )
    source_detail["loss"] = source_detail_loss
    if int(source_detail.get("fetchedCount") or 0) <= 0:
        source_detail["fetchedCount"] = _coerce_int(stats_payload.get("downloader/response_count"))


def _entry_error_result(
    source_detail: dict[str, Any],
    source_name: str,
    *,
    error: str,
    classification: str,
    source_error: str = "",
) -> tuple[list[RawJob], dict[str, Any], list[str]]:
    source_detail.update(
        {
            "status": "error",
            "error": clean_text(error)[:500],
            "classification": classification,
            "browserFallbackRecommended": False,
        }
    )
    _finalize_source_detail(source_detail)
    return [], source_detail, [f"{source_name}: {clean_text(source_error or error)[:200]}"]


def _ordered_rows(ordered_rows: list[list[RawJob] | None]) -> list[RawJob]:
    return [row for rows in ordered_rows if rows for row in rows]


def _ordered_errors(ordered_errors: list[list[str] | None]) -> list[str]:
    return [error for errors in ordered_errors if errors for error in errors]


def _ordered_details(ordered_details: list[dict[str, Any] | None]) -> list[dict[str, Any]]:
    return [detail for detail in ordered_details if isinstance(detail, dict)]


def _run_scrapy_static_source_entry(
    source: dict[str, Any],
    *,
    runner_path: Path,
    timeout_s: int,
    retries: int,
    backoff_s: float,
) -> tuple[list[RawJob], dict[str, Any], list[str]]:
    source_name = clean_text(source.get("name")) or "unknown"
    pages = _as_list(source.get("pages"))
    config, _ = _build_runner_config(
        source,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
    )
    source_detail = _base_detail(source, signal_quality="weak")
    source_errors: list[str] = []
    source_rows: list[RawJob] = []
    try:
        envelope, runner_errors = _run_runner_envelope(
            source_name=source_name,
            pages=pages,
            runner_path=runner_path,
            config=config,
            timeout_s=timeout_s,
        )
        source_errors.extend(runner_errors)
        if _reject_invalid_envelope(
            envelope, source_detail, source_errors, source_name=source_name
        ):
            return source_rows, source_detail, source_errors

        envelope_dict = _as_dict(envelope)
        _merge_envelope_detail(source_detail, envelope_dict)
        source_errors.extend(
            f"{source_name}: {item}" for item in _clean_errors(envelope_dict.get("partialErrors"))
        )
        source_rows, job_errors = _merge_envelope_jobs(
            envelope_dict=envelope_dict,
            source_detail=source_detail,
            source=source,
            source_name=source_name,
        )
        source_errors.extend(job_errors)
        _merge_envelope_stats(source_detail, envelope_dict)
        _finalize_source_detail(source_detail)
        return source_rows, source_detail, source_errors
    except subprocess.TimeoutExpired:
        return _entry_error_result(
            source_detail,
            source_name,
            error="subprocess timeout",
            classification="browser_timeout",
        )
    except Exception as exc:  # noqa: BLE001
        return _entry_error_result(
            source_detail,
            source_name,
            error=clean_text(exc),
            classification="parse_error",
            source_error=f"{type(exc).__name__}: {clean_text(exc)[:200]}",
        )


def _report_no_scrapy_sources() -> list[RawJob]:
    set_source_diagnostics(
        "scrapy_static_sources",
        adapter="scrapy_static",
        studio="multiple",
        details=[],
        partial_errors=["No enabled scrapy_static sources"],
    )
    return []


def _report_missing_scrapy_runner(runner_path: Path) -> list[RawJob]:
    msg = f"scrapy_static runner missing: {runner_path}"
    set_source_diagnostics(
        "scrapy_static_sources",
        adapter="scrapy_static",
        studio="multiple",
        details=[_base_detail({"name": "scrapy_static"}, error=msg)],
        partial_errors=[msg],
    )
    return []


def _scrapy_progress_payload(
    *,
    total_sources: int,
    completed: int,
    running: int,
    error_count: int,
    target_label: str = "",
    target_url: str = "",
    wait_reason: str = "",
    message: str = "",
    event_level: str = "muted",
) -> dict[str, Any]:
    return {
        "phase_key": "loading_source",
        "phase_label": "Processing browser fallback queue",
        "counts": {
            "totalSources": int(total_sources),
            "completedSources": int(completed),
            "runningSources": int(running),
            "queuedSources": max(0, int(total_sources) - int(completed) - int(running)),
            "errorSources": int(error_count),
        },
        "target_label": clean_text(target_label),
        "target_url": clean_text(target_url),
        "wait_reason": clean_text(wait_reason),
        "message": clean_text(message),
        "event_level": clean_text(event_level) or "muted",
    }


def _emit_scrapy_progress(
    progress_callback: Callable[..., None] | None,
    last_signature: str,
    payload: dict[str, Any],
) -> str:
    if not callable(progress_callback):
        return last_signature
    signature = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    if signature != last_signature:
        progress_callback(**payload)
        return signature
    return last_signature


def _future_scrapy_result(
    future: Future[tuple[list[RawJob], dict[str, Any], list[str]]],
    source: dict[str, Any],
) -> tuple[list[RawJob], dict[str, Any], list[str]]:
    source_name = clean_text(source.get("name")) or "unknown"
    try:
        return future.result()
    except Exception as exc:  # noqa: BLE001
        detail = _base_detail(source, signal_quality="weak")
        detail.update(
            {
                "status": "error",
                "error": clean_text(exc)[:500],
                "classification": "parse_error",
                "browserFallbackRecommended": False,
            }
        )
        _finalize_source_detail(detail)
        return [], detail, [f"{source_name}: {type(exc).__name__}: {clean_text(exc)[:200]}"]


def _write_scrapy_diagnostics_snapshot(
    ordered_details: list[dict[str, Any] | None],
    ordered_errors: list[list[str] | None],
) -> None:
    set_source_diagnostics(
        "scrapy_static_sources",
        adapter="scrapy_static",
        studio="multiple",
        details=_ordered_details(ordered_details),
        partial_errors=_ordered_errors(ordered_errors),
    )


def _handle_scrapy_wait_tick(
    *,
    heartbeat_callback: Callable[[], None] | None,
    progress_callback: Callable[..., None] | None,
    last_progress_signature: str,
    last_wait_progress: float,
    total: int,
    completed: int,
    running: int,
    error_count: int,
) -> tuple[float, str]:
    if callable(heartbeat_callback):
        heartbeat_callback()
    now = time.monotonic()
    if (now - last_wait_progress) < SCRAPY_STATIC_QUEUE_WAIT_PROGRESS_S:
        return last_wait_progress, last_progress_signature
    payload = _scrapy_progress_payload(
        total_sources=total,
        completed=completed,
        running=running,
        error_count=error_count,
        wait_reason="awaiting_runner_completion",
        message=f"Waiting for scrapy_static fallback queue ({completed}/{total} completed).",
    )
    return now, _emit_scrapy_progress(progress_callback, last_progress_signature, payload)


def _run_scrapy_static_queue(
    *,
    sources: list[dict[str, Any]],
    runner_path: Path,
    timeout_s: int,
    retries: int,
    backoff_s: float,
    heartbeat_callback: Callable[[], None] | None,
    progress_callback: Callable[..., None] | None,
    max_workers: int | None,
) -> list[RawJob]:
    total = len(sources)
    queue_workers = min(max(1, int(max_workers or 1)), SCRAPY_STATIC_QUEUE_MAX_WORKERS, total)
    ordered_rows: list[list[RawJob] | None] = [None] * total
    ordered_details: list[dict[str, Any] | None] = [None] * total
    ordered_errors: list[list[str] | None] = [None] * total
    inflight: dict[Future[tuple[list[RawJob], dict[str, Any], list[str]]], int] = {}
    completed = error_count = next_source_index = 0
    last_wait_progress = 0.0
    last_progress_signature = ""

    def emit(**kwargs: Any) -> None:
        nonlocal last_progress_signature
        last_progress_signature = _emit_scrapy_progress(
            progress_callback,
            last_progress_signature,
            _scrapy_progress_payload(
                total_sources=total,
                completed=completed,
                running=len(inflight),
                error_count=error_count,
                **kwargs,
            ),
        )

    def submit_source(executor: ThreadPoolExecutor, source_index: int) -> None:
        source = sources[source_index]
        inflight[
            executor.submit(
                _run_scrapy_static_source_entry,
                source,
                runner_path=runner_path,
                timeout_s=timeout_s,
                retries=retries,
                backoff_s=backoff_s,
            )
        ] = source_index
        pages = _as_list(source.get("pages"))
        emit(
            target_label=clean_text(source.get("name")),
            target_url=clean_text(pages[0]) if pages else "",
            message=f"Running scrapy_static fallback {clean_text(source.get('name')) or 'unknown'}.",
        )

    def completed_progress(source: dict[str, Any]) -> None:
        source_name = clean_text(source.get("name")) or "unknown"
        pages = _as_list(source.get("pages"))
        emit(
            target_label=source_name,
            target_url=clean_text(pages[0]) if pages else "",
            message=f"Completed scrapy_static fallback {source_name}.",
        )

    def fill_queue(executor: ThreadPoolExecutor) -> None:
        nonlocal next_source_index
        while next_source_index < total and len(inflight) < queue_workers:
            submit_source(executor, next_source_index)
            next_source_index += 1

    with ThreadPoolExecutor(max_workers=queue_workers) as executor:
        fill_queue(executor)
        while inflight:
            done, _pending = wait(
                tuple(inflight.keys()),
                timeout=SCRAPY_STATIC_QUEUE_POLL_S,
                return_when=FIRST_COMPLETED,
            )
            if not done:
                last_wait_progress, last_progress_signature = _handle_scrapy_wait_tick(
                    heartbeat_callback=heartbeat_callback,
                    progress_callback=progress_callback,
                    last_progress_signature=last_progress_signature,
                    last_wait_progress=last_wait_progress,
                    total=total,
                    completed=completed,
                    running=len(inflight),
                    error_count=error_count,
                )
                continue
            for future in sorted(done, key=lambda item: inflight[item]):
                source_index = inflight.pop(future)
                source = sources[source_index]
                rows, detail, errors = _future_scrapy_result(future, source)
                ordered_rows[source_index] = rows
                ordered_details[source_index] = detail
                ordered_errors[source_index] = errors
                completed += 1
                error_count += int(norm_text(detail.get("status")) != "ok")
                _write_scrapy_diagnostics_snapshot(ordered_details, ordered_errors)
                completed_progress(source)
                if callable(heartbeat_callback):
                    heartbeat_callback()
            fill_queue(executor)
    return _ordered_rows(ordered_rows)


def run_scrapy_static_source(
    *,
    fetch_text: Any,
    timeout_s: int,
    retries: int,
    backoff_s: float,
    heartbeat_callback: Callable[[], None] | None = None,
    progress_callback: Callable[..., None] | None = None,
    max_workers: int | None = None,
) -> list[RawJob]:
    del fetch_text

    sources = registry_entries("scrapy_static")
    if not sources:
        return _report_no_scrapy_sources()

    runner_path = Path(__file__).resolve().parents[2] / "scrapers" / "runner.py"
    if not runner_path.exists():
        return _report_missing_scrapy_runner(runner_path)

    return _run_scrapy_static_queue(
        sources=sources,
        runner_path=runner_path,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        heartbeat_callback=heartbeat_callback,
        progress_callback=progress_callback,
        max_workers=max_workers,
    )
