from __future__ import annotations

"""
Source execution stage for `src.jobs.pipeline`.

This module is intentionally focused on the "execute loaders and update task/source reports" part of
the pipeline. It keeps the public `run_pipeline(...)` facade in `src/jobs/pipeline.py` while reducing
the size/complexity of that file (AI-coder context switching).
"""

import inspect
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from threading import Lock
from typing import Any, Callable, Dict, List, Optional, Tuple

from src.jobs.canonicalize import CanonicalNormalizer
from src.jobs.models import CanonicalJob
from src.jobs.state import source_rows_fingerprint
from src.jobs.reporting import format_source_error
from src.jobs.common import SOURCE_DIAGNOSTICS, SOURCE_REPORT_META
from src.jobs.text_utils import clean_text, norm_text
from src.shared.utils import now_iso


FetchTextLimited = Callable[[str, int], str]
WriteTaskStateFunc = Callable[..., None]
WriteProgressReportFunc = Callable[[], None]


def _rows_to_legacy_dicts(rows: List[CanonicalJob]) -> List[Dict[str, Any]]:
    return [row.to_dict() if isinstance(row, CanonicalJob) else dict(row) for row in rows]  # type: ignore[arg-type]


def _best_effort_get_try_playwright() -> Optional[Callable[[str, int], Tuple[str, str]]]:
    try:
        from src.bridge.source_check_http import try_fetch_with_playwright

        return try_fetch_with_playwright
    except ImportError:
        return None


@dataclass(frozen=True)
class SourceExecutionStageConfig:
    max_workers: int
    timeout_s: int
    retries: int
    backoff_s: float
    static_detail_concurrency: int
    google_sheets_redirect_concurrency: int
    started_at: str
    show_progress: bool


def run_source_execution_stage(
    *,
    config: SourceExecutionStageConfig,
    selected_loaders: List[Tuple[str, Callable[..., List[Dict[str, Any]]]]],
    fetch_text_limited: FetchTextLimited,
    source_state_rows: Dict[str, Dict[str, Any]],
    redirect_resolver: Any,
    task_rows: Dict[str, Dict[str, Any]],
    task_lock: Lock,
    thread_local: Any,
    write_task_state: WriteTaskStateFunc,
    write_progress_report: WriteProgressReportFunc,
    canonical_rows: List[CanonicalJob],
    source_reports: List[Dict[str, Any]],
) -> None:
    """
    Executes selected source loaders, mutating `canonical_rows` and `source_reports` in-place,
    and updating `task_rows` via `write_task_state(...)` plus periodic progress writes.
    """

    # Resolve optional Playwright fetch helper.
    _try_playwright = _best_effort_get_try_playwright()

    def execute_loader(name: str, loader: Callable[..., List[Dict[str, Any]]]) -> Tuple[Dict[str, Any], List[CanonicalJob]]:
        source_started = time.perf_counter()
        base_meta = SOURCE_REPORT_META.get(name, {})
        report: Dict[str, Any] = {
            "name": name,
            "status": "ok",
            "adapter": clean_text(base_meta.get("adapter")) or "custom",
            "fetchStrategy": clean_text(base_meta.get("fetchStrategy")) or "auto",
            "studio": clean_text(base_meta.get("studio")) or "",
            "fetchedCount": 0,
            "keptCount": 0,
            "lowConfidenceDropped": 0,
            "error": "",
            "durationMs": 0,
            "loss": {
                "rawFetched": 0,
                "canonicalDropped": 0,
                "canonicalKept": 0,
                "dedupMerged": 0,
                "finalOutput": 0,
                "canonicalDropReasons": {
                    "missing_title": 0,
                    "missing_company": 0,
                    "missing_job_link": 0,
                    "invalid_url": 0,
                    "invalid_payload": 0,
                },
                "scrapyRunnerRejectedValidation": 0,
                "scrapyParentInvalidPayload": 0,
                "staticNonJobUrlRejected": 0,
                "staticDuplicateLinkRejected": 0,
                "staticDetailParseEmpty": 0,
            },
        }
        canonical_batch: List[CanonicalJob] = []

        try:
            thread_local.source_name = name
            loader_kwargs: Dict[str, Any] = {
                "fetch_text": fetch_text_limited,
                "timeout_s": config.timeout_s,
                "retries": config.retries,
                "backoff_s": config.backoff_s,
            }
            if norm_text(report.get("adapter")) == "static":
                loader_kwargs["static_detail_concurrency"] = config.static_detail_concurrency
                loader_kwargs["source_state_rows"] = source_state_rows
                if _try_playwright is not None:
                    loader_kwargs["try_playwright"] = _try_playwright

            try:
                signature = inspect.signature(loader)
                accepts_var_kwargs = any(
                    parameter.kind == inspect.Parameter.VAR_KEYWORD
                    for parameter in signature.parameters.values()
                )
                accepted_kwargs = loader_kwargs if accepts_var_kwargs else {
                    key: value for key, value in loader_kwargs.items() if key in signature.parameters
                }
            except (TypeError, ValueError):
                accepted_kwargs = {
                    "fetch_text": fetch_text_limited,
                    "timeout_s": config.timeout_s,
                    "retries": config.retries,
                    "backoff_s": config.backoff_s,
                }

            raw_rows = loader(**accepted_kwargs)
            report["fetchedCount"] = len(raw_rows)
            report_loss = report["loss"] if isinstance(report.get("loss"), dict) else {}
            report_loss["rawFetched"] = int(len(raw_rows))

            normalizer = CanonicalNormalizer(
                source=name,
                fetched_at=config.started_at,
                resolve_redirect_url=redirect_resolver.resolve,
                redirect_resolver=redirect_resolver,
                redirect_concurrency=config.google_sheets_redirect_concurrency,
            )
            canonical_batch = normalizer.process(raw_rows)  # type: ignore[arg-type]
            drop_reasons = normalizer.drop_reasons

            kept = len(canonical_batch)
            google_sheet_redirect_stats: Dict[str, int] = {}
            if name.startswith("google_sheets"):
                google_sheet_redirect_stats = normalizer.stats
                canonicalization_ms = int(google_sheet_redirect_stats.get("canonicalize_ms") or 0)
            else:
                canonicalization_ms = int(normalizer.stats.get("canonicalize_ms") or 0)

            report["keptCount"] = kept
            report_loss["canonicalKept"] = int(kept)
            report_loss["canonicalDropped"] = max(0, int(len(raw_rows)) - int(kept))
            report_loss["canonicalDropReasons"] = {
                "missing_title": int(drop_reasons.get("missing_title", 0)),
                "missing_company": int(drop_reasons.get("missing_company", 0)),
                "missing_job_link": int(drop_reasons.get("missing_job_link", 0)),
                "invalid_url": int(drop_reasons.get("invalid_url", 0)),
                "invalid_payload": int(drop_reasons.get("invalid_payload", 0)),
            }

            current_fingerprint = source_rows_fingerprint(_rows_to_legacy_dicts(canonical_batch))
            previous_fingerprint = clean_text((source_state_rows.get(name) or {}).get("lastFingerprint"))
            report["sourceFingerprint"] = current_fingerprint
            report["fingerprintChanged"] = bool(current_fingerprint != previous_fingerprint)

            diag = SOURCE_DIAGNOSTICS.get(name) or {}
            if clean_text(diag.get("adapter")):
                report["adapter"] = clean_text(diag.get("adapter"))
            if clean_text(diag.get("studio")):
                report["studio"] = clean_text(diag.get("studio"))

            details = diag.get("details")
            if isinstance(details, list) and details:
                report["details"] = details
            detail_rows = details if isinstance(details, list) else []

            stage_timings = report.get("stageTimingsMs") if isinstance(report.get("stageTimingsMs"), dict) else {}
            if norm_text(report.get("adapter")) == "static":
                listing_fetch_ms = 0
                candidate_extraction_ms = 0
                detail_fetch_ms = 0
                for detail in detail_rows:
                    if not isinstance(detail, dict):
                        continue
                    stats = detail.get("stats") if isinstance(detail.get("stats"), dict) else {}
                    listing_fetch_ms += int(stats.get("listing_fetch_ms") or 0)
                    candidate_extraction_ms += int(stats.get("candidate_extraction_ms") or 0)
                    detail_fetch_ms += int(stats.get("detail_fetch_ms") or 0)
                stage_timings.update(
                    {
                        "listingFetch": int(listing_fetch_ms),
                        "candidateExtraction": int(candidate_extraction_ms),
                        "detailFetch": int(detail_fetch_ms),
                    }
                )

            if norm_text(report.get("adapter")) == "csv":
                parse_csv_ms = 0
                for detail in detail_rows:
                    if not isinstance(detail, dict):
                        continue
                    stats = detail.get("stats") if isinstance(detail.get("stats"), dict) else {}
                    parse_csv_ms += int(stats.get("parse_csv_ms") or 0)
                    if google_sheet_redirect_stats:
                        stats["redirect_candidates"] = int(google_sheet_redirect_stats.get("redirect_candidates") or 0)
                        stats["redirect_resolved"] = int(google_sheet_redirect_stats.get("redirect_resolved") or 0)
                        stats["redirect_cache_hits"] = int(google_sheet_redirect_stats.get("redirect_cache_hits") or 0)
                        stats["redirect_resolve_ms"] = int(google_sheet_redirect_stats.get("redirect_resolve_ms") or 0)
                        stats["canonicalize_ms"] = int(google_sheet_redirect_stats.get("canonicalize_ms") or 0)
                stage_timings.update(
                    {
                        "parseCsv": int(parse_csv_ms),
                        "redirectResolve": int(google_sheet_redirect_stats.get("redirect_resolve_ms") or 0),
                    }
                )

            stage_timings["canonicalization"] = int(canonicalization_ms)
            if any(int(value or 0) > 0 for value in stage_timings.values()):
                report["stageTimingsMs"] = stage_timings

            partial_errors = [clean_text(err) for err in (diag.get("partialErrors") or []) if clean_text(err)]
            if partial_errors:
                report["error"] = "; ".join(format_source_error(name, err) for err in partial_errors[:6])
            report["lowConfidenceDropped"] = int(diag.get("lowConfidenceDropped") or 0)

            if name == "scrapy_static_sources":
                runner_rejected = 0
                parent_invalid = 0
                for detail in detail_rows:
                    if not isinstance(detail, dict):
                        continue
                    stats = detail.get("stats") if isinstance(detail.get("stats"), dict) else {}
                    runner_rejected += int(stats.get("jobs_rejected_validation") or 0)
                    loss_detail = detail.get("loss") if isinstance(detail.get("loss"), dict) else {}
                    parent_invalid += int(loss_detail.get("scrapyParentInvalidPayload") or 0)
                report_loss["scrapyRunnerRejectedValidation"] = int(runner_rejected)
                report_loss["scrapyParentInvalidPayload"] = int(parent_invalid)

            if norm_text(report.get("adapter")) == "static":
                static_non_job = 0
                static_dup = 0
                static_empty = 0
                for detail in detail_rows:
                    if not isinstance(detail, dict):
                        continue
                    loss_detail = detail.get("loss") if isinstance(detail.get("loss"), dict) else {}
                    static_non_job += int(loss_detail.get("staticNonJobUrlRejected") or 0)
                    static_dup += int(loss_detail.get("staticDuplicateLinkRejected") or 0)
                    static_empty += int(loss_detail.get("staticDetailParseEmpty") or 0)
                report_loss["staticNonJobUrlRejected"] = int(static_non_job)
                report_loss["staticDuplicateLinkRejected"] = int(static_dup)
                report_loss["staticDetailParseEmpty"] = int(static_empty)

            report["loss"] = report_loss
        except Exception as exc:  # noqa: BLE001
            report["status"] = "error"
            report["error"] = format_source_error(name, exc)
        finally:
            thread_local.source_name = ""

        report["durationMs"] = int((time.perf_counter() - source_started) * 1000)
        return report, canonical_batch

    def run_stage() -> None:
        if config.max_workers <= 1 or len(selected_loaders) <= 1:
            for source_name, loader in selected_loaders:
                mark_task_started(source_name)
                report, canonical_batch = execute_loader(source_name, loader)
                canonical_rows.extend(canonical_batch)
                source_reports.append(report)
                mark_task_finished(source_name, report)
            return

        with ThreadPoolExecutor(max_workers=config.max_workers) as executor:
            futures = {}
            for source_name, loader in selected_loaders:
                mark_task_started(source_name)
                futures[executor.submit(execute_loader, source_name, loader)] = source_name
            for future in as_completed(futures):
                source_name = futures[future]
                try:
                    report, canonical_batch = future.result()
                except Exception as exc:  # noqa: BLE001
                    report = fallback_error_report(source_name, exc)
                    canonical_batch = []
                canonical_rows.extend(canonical_batch)
                source_reports.append(report)
                mark_task_finished(source_name, report)

    # The closure below expects `now_iso()` and `fallback_error_report()`/`mark_task_finished()`.
    def fallback_error_report(source_name: str, exc: Exception) -> Dict[str, Any]:
        return {
            "name": source_name,
            "status": "error",
            "adapter": clean_text(SOURCE_REPORT_META.get(source_name, {}).get("adapter")) or "custom",
            "fetchStrategy": clean_text(SOURCE_REPORT_META.get(source_name, {}).get("fetchStrategy")) or "auto",
            "studio": clean_text(SOURCE_REPORT_META.get(source_name, {}).get("studio")) or "",
            "fetchedCount": 0,
            "keptCount": 0,
            "error": format_source_error(source_name, exc),
            "durationMs": 0,
            "loss": {
                "rawFetched": 0,
                "canonicalDropped": 0,
                "canonicalKept": 0,
                "dedupMerged": 0,
                "finalOutput": 0,
                "canonicalDropReasons": {
                    "missing_title": 0,
                    "missing_company": 0,
                    "missing_job_link": 0,
                    "invalid_url": 0,
                    "invalid_payload": 0,
                },
            },
        }

    def mark_task_started(source_name: str) -> None:
        start_time = now_iso()
        with task_lock:
            task_rows[source_name]["status"] = "running"
            task_rows[source_name]["startedAt"] = start_time
            task_rows[source_name]["heartbeatAt"] = start_time
        write_task_state(force=True)
        if config.show_progress:
            print(f"[jobs_fetcher] START source={source_name}", flush=True)

    def mark_task_finished(source_name: str, report: Dict[str, Any]) -> None:
        end_time = now_iso()
        with task_lock:
            task_rows[source_name]["status"] = "ok" if report.get("status") == "ok" else "error"
            task_rows[source_name]["finishedAt"] = end_time
            task_rows[source_name]["durationMs"] = int(report.get("durationMs") or 0)
            task_rows[source_name]["heartbeatAt"] = end_time
            task_rows[source_name]["error"] = clean_text(report.get("error"))
        write_progress_report()
        write_task_state(force=True)
        if config.show_progress:
            print(
                f"[jobs_fetcher] DONE source={source_name} status={report['status']} "
                f"fetched={int(report.get('fetchedCount') or 0)} "
                f"kept={int(report.get('keptCount') or 0)} "
                f"durationMs={int(report.get('durationMs') or 0)}",
                flush=True,
            )

    write_progress_report()
    write_task_state(force=True)
    run_stage()


__all__ = ["SourceExecutionStageConfig", "run_source_execution_stage"]

