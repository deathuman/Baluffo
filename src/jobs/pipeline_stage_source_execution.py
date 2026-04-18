from __future__ import annotations

"""
Source execution stage for `src.jobs.pipeline`.

This module is intentionally focused on the "execute loaders and update task/source reports" part of
the pipeline. It keeps the public `run_pipeline(...)` facade in `src/jobs/pipeline.py` while reducing
the size/complexity of that file (AI-coder context switching).
"""

import inspect
import sys
import time
from collections import Counter
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from threading import BoundedSemaphore, Lock
from types import SimpleNamespace
from typing import Any

from src.jobs.browser_fallback import BrowserFallbackCircuitBreaker
from src.jobs.canonicalize import CanonicalNormalizer
from src.jobs.common.config import SOURCE_DIAGNOSTICS
from src.jobs.common.taxonomy import (
    ClassificationContext,
    FailureBucket,
    ZeroExtractDiagnosis,
    ZeroKeptClassification,
    assess_zero_extract,
)
from src.jobs.models import CanonicalJob
from src.jobs.pipeline_runtime import (
    PipelineTaskRuntime,
    update_fetch_work_item_progress,
)
from src.jobs.reporting import format_source_error
from src.jobs.state import (
    set_browser_fallback_state,
    should_browser_escalate_source,
    source_rows_fingerprint,
)
from src.jobs.text_utils import clean_text, norm_text
from src.jobs_fetcher_registry import SOURCE_REPORT_META
from src.shared.utils import now_iso

FetchTextLimited = Callable[[str, int], str]
WriteTaskStateFunc = Callable[..., None]
WriteProgressReportFunc = Callable[[], None]
TryPlaywrightFn = Callable[[str, int], tuple[str, str]]


def resolve_fetch_browser_fallback_helper() -> TryPlaywrightFn | None:
    try:
        from src.bridge.source_check_http import try_fetch_with_playwright

        return try_fetch_with_playwright
    except ImportError:
        return None


def _build_capped_try_playwright(
    try_playwright: TryPlaywrightFn,
    *,
    max_concurrent: int,
) -> TryPlaywrightFn:
    gate = BoundedSemaphore(max(1, int(max_concurrent or 1)))

    def capped_try_playwright(url: str, timeout_s: int) -> tuple[str, str]:
        gate.acquire()
        try:
            return try_playwright(url, timeout_s)
        finally:
            gate.release()

    return capped_try_playwright


def _default_adapter_for_loader(name: str, base_meta: dict[str, Any]) -> str:
    adapter = clean_text(base_meta.get("adapter"))
    if adapter:
        return adapter
    if clean_text(name).startswith("static_source::"):
        return "static"
    return "custom"


def _is_provider_family_adapter(adapter_name: str) -> bool:
    return norm_text(adapter_name) in {
        "ashby",
        "breezy",
        "greenhouse",
        "jazzhr",
        "lever",
        "personio",
        "pinpoint",
        "recruitee",
        "smartrecruiters",
        "teamtailor",
        "workable",
    }


def _is_social_subsource_report(source_name: str, adapter_name: str) -> bool:
    return norm_text(adapter_name) == "social" and clean_text(source_name) in {
        "social_x",
        "social_mastodon",
    }


def _failure_bucket_from_zero_extract_context(
    cls_context: ClassificationContext,
    zero_kept_classification: str = "",
) -> FailureBucket:
    if clean_text(zero_kept_classification) == ZeroKeptClassification.LEGIT_EMPTY.value:
        return FailureBucket.NO_OPENINGS
    diagnosis = assess_zero_extract(cls_context).diagnosis
    if diagnosis == ZeroExtractDiagnosis.EMPTY_CONFIRMED:
        return FailureBucket.NO_OPENINGS
    if diagnosis == ZeroExtractDiagnosis.JS_REQUIRED:
        return FailureBucket.JS_REQUIRED
    if diagnosis == ZeroExtractDiagnosis.SITE_CHANGED:
        return FailureBucket.SITE_CHANGED
    if diagnosis == ZeroExtractDiagnosis.ANTI_BOT_OR_CHALLENGE:
        return FailureBucket.ANTI_BOT_OR_CHALLENGE
    if diagnosis == ZeroExtractDiagnosis.NEEDS_REVIEW:
        return FailureBucket.NEEDS_REVIEW
    return FailureBucket.UNKNOWN


def _console_safe_text(value: Any) -> str:
    text = str(value or "")
    stream = getattr(sys, "stdout", None)
    encoding = str(getattr(stream, "encoding", "") or "").strip() or "utf-8"
    try:
        return text.encode(encoding, errors="backslashreplace").decode(encoding)
    except Exception:
        return text.encode("ascii", errors="backslashreplace").decode("ascii")


def _emit_progress_line(message: str) -> None:
    print(_console_safe_text(message), flush=True)


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
    force_refresh_all: bool
    browser_fallback_cooldown_minutes: int


def run_source_execution_stage(
    *,
    config: SourceExecutionStageConfig,
    selected_loaders: list[tuple[str, Callable[..., list[dict[str, Any]]]]],
    fetch_text_limited: FetchTextLimited,
    source_state_rows: dict[str, dict[str, Any]],
    redirect_resolver: Any,
    task_runtime: PipelineTaskRuntime | None = None,
    task_rows: dict[str, dict[str, Any]],
    task_lock: Lock,
    thread_local: Any,
    write_task_state: WriteTaskStateFunc,
    write_progress_report: WriteProgressReportFunc,
    canonical_rows: list[CanonicalJob],
    source_reports: list[dict[str, Any]],
) -> None:
    """
    Executes selected source loaders, mutating `canonical_rows` and `source_reports` in-place,
    and updating live fetch task state via `write_task_state(...)` plus periodic progress writes.
    """

    # Resolve optional Playwright fetch helper.
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
    _try_playwright = resolve_fetch_browser_fallback_helper()
    browser_fallback_guard = BrowserFallbackCircuitBreaker.from_state(
        source_state_rows, cooldown_minutes=config.browser_fallback_cooldown_minutes
    )
    capped_try_playwright = (
        _build_capped_try_playwright(_try_playwright, max_concurrent=config.max_workers)
        if _try_playwright is not None
        else None
    )
    guarded_try_playwright = (
        browser_fallback_guard.wrap(capped_try_playwright)
        if capped_try_playwright is not None
        else None
    )
    if config.show_progress:
        if guarded_try_playwright is not None:
            _emit_progress_line(
                f"[jobs_fetcher] INFO browserFallbackEnabled=true browserFallbackCap={max(1, int(config.max_workers or 1))}"
            )
        else:
            _emit_progress_line("[jobs_fetcher] INFO browserFallbackEnabled=false")

    def execute_loader(
        name: str, loader: Callable[..., list[dict[str, Any]]]
    ) -> tuple[dict[str, Any], list[CanonicalJob]]:
        source_started = time.perf_counter()
        base_meta = SOURCE_REPORT_META.get(name, {})
        report: dict[str, Any] = {
            "name": name,
            "status": "ok",
            "adapter": _default_adapter_for_loader(name, base_meta),
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
        canonical_batch: list[CanonicalJob] = []
        last_heartbeat_write = 0.0

        def loader_heartbeat_callback() -> None:
            nonlocal last_heartbeat_write
            now_mono = time.perf_counter()
            if (now_mono - last_heartbeat_write) < 4.0:
                return
            last_heartbeat_write = now_mono
            with task_lock:
                row = task_rows.get(name)
                if isinstance(row, dict) and row.get("status") == "running":
                    row["heartbeatAt"] = now_iso()
            write_task_state(force=True)

        loader_progress_callback = None

        def loader_progress_callback(
            *,
            phase_key: str = "",
            phase_label: str = "",
            counts: dict[str, Any] | None = None,
            target_label: str = "",
            target_url: str = "",
            event_level: str = "muted",
            message: str = "",
        ) -> None:
            update_fetch_work_item_progress(
                task_runtime,
                name,
                phase_key=phase_key,
                phase_label=phase_label,
                counts=counts,
                target_label=target_label,
                target_url=target_url,
                emit_event=bool(message),
                event_level=event_level,
                event_message=message,
            )
            write_task_state()

        try:
            thread_local.source_name = name
            loader_started = time.perf_counter()
            loader_kwargs: dict[str, Any] = {
                "fetch_text": fetch_text_limited,
                "timeout_s": config.timeout_s,
                "retries": config.retries,
                "backoff_s": config.backoff_s,
                "source_state_rows": source_state_rows,
                "force_refresh_all": config.force_refresh_all,
            }
            if norm_text(report.get("adapter")) == "static":
                loader_kwargs["static_detail_concurrency"] = config.static_detail_concurrency
                if guarded_try_playwright is not None and should_browser_escalate_source(
                    name, source_state_rows
                ):
                    loader_kwargs["try_playwright"] = guarded_try_playwright
            if loader_heartbeat_callback is not None:
                loader_kwargs["heartbeat_callback"] = loader_heartbeat_callback
            loader_kwargs["progress_callback"] = loader_progress_callback

            try:
                signature = inspect.signature(loader)
                accepts_var_kwargs = any(
                    parameter.kind == inspect.Parameter.VAR_KEYWORD
                    for parameter in signature.parameters.values()
                )
                accepted_kwargs = (
                    loader_kwargs
                    if accepts_var_kwargs
                    else {
                        key: value
                        for key, value in loader_kwargs.items()
                        if key in signature.parameters
                    }
                )
            except (TypeError, ValueError):
                accepted_kwargs = {
                    "fetch_text": fetch_text_limited,
                    "timeout_s": config.timeout_s,
                    "retries": config.retries,
                    "backoff_s": config.backoff_s,
                }
                if loader_heartbeat_callback is not None:
                    accepted_kwargs["heartbeat_callback"] = loader_heartbeat_callback
                accepted_kwargs["progress_callback"] = loader_progress_callback

            loader_progress_callback(
                phase_key="loading_source",
                phase_label="Loading source",
                counts={"adapter": clean_text(report.get("adapter")) or "custom"},
                message=f"Loading source {name}.",
            )
            raw_rows = loader(**accepted_kwargs)
            fetch_and_parse_ms = int((time.perf_counter() - loader_started) * 1000)
            report["fetchedCount"] = len(raw_rows)
            report_loss = report["loss"] if isinstance(report.get("loss"), dict) else {}
            report_loss["rawFetched"] = int(len(raw_rows))
            loader_progress_callback(
                phase_key="normalizing_rows",
                phase_label="Normalizing rows",
                counts={"fetchedCount": int(len(raw_rows)), "fetchMs": fetch_and_parse_ms},
                message=(
                    f"Fetched source {name}: {int(len(raw_rows))} row"
                    f"{'' if int(len(raw_rows)) == 1 else 's'} ready for normalization."
                ),
            )

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
            google_sheet_redirect_stats: dict[str, int] = {}
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
            loader_progress_callback(
                phase_key="normalized_rows",
                phase_label="Rows normalized",
                counts={
                    "fetchedCount": int(len(raw_rows)),
                    "keptCount": int(kept),
                    "canonicalDropped": max(0, int(len(raw_rows)) - int(kept)),
                    "canonicalizationMs": int(canonicalization_ms),
                },
                message=(
                    f"Normalized source {name}: kept {int(kept)} of {int(len(raw_rows))} fetched rows."
                ),
            )

            current_fingerprint = source_rows_fingerprint(
                [row.to_dict() for row in canonical_batch]
            )
            previous_fingerprint = clean_text(
                (source_state_rows.get(name) or {}).get("lastFingerprint")
            )
            report["sourceFingerprint"] = current_fingerprint
            report["fingerprintChanged"] = bool(current_fingerprint != previous_fingerprint)

            diag = SOURCE_DIAGNOSTICS.get(name) or {}
            if clean_text(diag.get("adapter")):
                report["adapter"] = clean_text(diag.get("adapter"))
            if clean_text(diag.get("studio")):
                report["studio"] = clean_text(diag.get("studio"))
            if clean_text(diag.get("providerUrl")):
                report["providerUrl"] = clean_text(diag.get("providerUrl"))

            details = diag.get("details")
            if isinstance(details, list) and details:
                report["details"] = details
            detail_rows = details if isinstance(details, list) else []
            if detail_rows and _is_provider_family_adapter(clean_text(report.get("adapter"))):
                board_decision_counts = Counter(
                    clean_text(detail.get("cacheDecision"))
                    for detail in detail_rows
                    if isinstance(detail, dict) and clean_text(detail.get("cacheDecision"))
                )
                report["boardCount"] = len(
                    [detail for detail in detail_rows if isinstance(detail, dict)]
                )
                report["boardCacheDecisionCounts"] = dict(board_decision_counts)
                report["boardSkippedCount"] = sum(
                    1
                    for detail in detail_rows
                    if isinstance(detail, dict)
                    and norm_text(detail.get("status")) == "excluded"
                    and clean_text(detail.get("cacheDecision")) in {"skip_fresh", "cooldown_skip"}
                )
                report["boardRevalidatedCount"] = sum(
                    1
                    for detail in detail_rows
                    if isinstance(detail, dict)
                    and clean_text(detail.get("cacheDecision")) == "revalidate_only"
                )
                report["boardNotModifiedCount"] = sum(
                    1
                    for detail in detail_rows
                    if isinstance(detail, dict)
                    and clean_text(detail.get("cacheDecisionReason")) == "not_modified_304"
                )
                report["boardRefreshedCount"] = sum(
                    1
                    for detail in detail_rows
                    if isinstance(detail, dict)
                    and norm_text(detail.get("status")) in {"ok", "error"}
                )
            if detail_rows and _is_social_subsource_report(name, clean_text(report.get("adapter"))):
                subsource_decision_counts = Counter(
                    clean_text(detail.get("cacheDecision"))
                    for detail in detail_rows
                    if isinstance(detail, dict) and clean_text(detail.get("cacheDecision"))
                )
                report["subsourceCount"] = len(
                    [detail for detail in detail_rows if isinstance(detail, dict)]
                )
                report["subsourceCacheDecisionCounts"] = dict(subsource_decision_counts)
                report["subsourceSkippedCount"] = sum(
                    1
                    for detail in detail_rows
                    if isinstance(detail, dict)
                    and norm_text(detail.get("status")) == "excluded"
                    and clean_text(detail.get("cacheDecision")) in {"skip_fresh", "cooldown_skip"}
                )
                report["subsourceRevalidatedCount"] = sum(
                    1
                    for detail in detail_rows
                    if isinstance(detail, dict)
                    and clean_text(detail.get("cacheDecision")) == "revalidate_only"
                )
                report["subsourceNotModifiedCount"] = sum(
                    1
                    for detail in detail_rows
                    if isinstance(detail, dict)
                    and clean_text(detail.get("cacheDecisionReason")) == "not_modified_304"
                )
                report["subsourceRefreshedCount"] = sum(
                    1
                    for detail in detail_rows
                    if isinstance(detail, dict)
                    and norm_text(detail.get("status")) in {"ok", "error"}
                )

            stage_timings = (
                report.get("stageTimingsMs")
                if isinstance(report.get("stageTimingsMs"), dict)
                else {}
            )
            stage_timings["fetchAndParse"] = int(fetch_and_parse_ms)
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
                        stats["redirect_candidates"] = int(
                            google_sheet_redirect_stats.get("redirect_candidates") or 0
                        )
                        stats["redirect_resolved"] = int(
                            google_sheet_redirect_stats.get("redirect_resolved") or 0
                        )
                        stats["redirect_cache_hits"] = int(
                            google_sheet_redirect_stats.get("redirect_cache_hits") or 0
                        )
                        stats["redirect_resolve_ms"] = int(
                            google_sheet_redirect_stats.get("redirect_resolve_ms") or 0
                        )
                        stats["canonicalize_ms"] = int(
                            google_sheet_redirect_stats.get("canonicalize_ms") or 0
                        )
                stage_timings.update(
                    {
                        "parseCsv": int(parse_csv_ms),
                        "redirectResolve": int(
                            google_sheet_redirect_stats.get("redirect_resolve_ms") or 0
                        ),
                    }
                )

            stage_timings["canonicalization"] = int(canonicalization_ms)
            if any(int(value or 0) > 0 for value in stage_timings.values()):
                report["stageTimingsMs"] = stage_timings

            partial_errors = [
                clean_text(err) for err in (diag.get("partialErrors") or []) if clean_text(err)
            ]
            if partial_errors:
                report["error"] = "; ".join(
                    format_source_error(name, err) for err in partial_errors[:6]
                )
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

        from src.jobs.common.taxonomy import (
            ClassificationContext,
            classify_zero_kept,
            map_error_to_failure_bucket,
        )

        cls_context = ClassificationContext(
            status=str(report.get("status") or ""),
            error=str(report.get("error") or ""),
            classification="",
            http_status=None,
            fetched_count=int(report.get("fetchedCount") or 0),
        )
        zero_kept_classification = classify_zero_kept(cls_context)
        failure_bucket = map_error_to_failure_bucket(cls_context)
        if (
            int(report.get("keptCount") or 0) == 0
            and report["status"] != "excluded"
            and failure_bucket == FailureBucket.UNKNOWN
        ):
            failure_bucket = _failure_bucket_from_zero_extract_context(
                cls_context,
                zero_kept_classification.value,
            )
        if (
            report["status"] == "error"
            or report.get("error")
            or int(report.get("keptCount") or 0) == 0
        ):
            report["failureBucket"] = failure_bucket.value
        if int(report.get("keptCount") or 0) == 0 and report["status"] != "excluded":
            report["zeroKeptClassification"] = zero_kept_classification.value

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
    def fallback_error_report(source_name: str, exc: Exception) -> dict[str, Any]:
        report: dict[str, Any] = {
            "name": source_name,
            "status": "error",
            "adapter": clean_text(SOURCE_REPORT_META.get(source_name, {}).get("adapter"))
            or "custom",
            "fetchStrategy": clean_text(
                SOURCE_REPORT_META.get(source_name, {}).get("fetchStrategy")
            )
            or "auto",
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
        from src.jobs.common.taxonomy import (
            ClassificationContext,
            classify_zero_kept,
            map_error_to_failure_bucket,
        )

        cls_context = ClassificationContext(
            status="error",
            error=report["error"],
            classification="",
            http_status=None,
            fetched_count=0,
        )
        zero_kept_classification = classify_zero_kept(cls_context)
        failure_bucket = map_error_to_failure_bucket(cls_context)
        if failure_bucket == FailureBucket.UNKNOWN:
            failure_bucket = _failure_bucket_from_zero_extract_context(
                cls_context,
                zero_kept_classification.value,
            )
        report["failureBucket"] = failure_bucket.value
        report["zeroKeptClassification"] = zero_kept_classification.value
        return report

    def mark_task_started(source_name: str) -> None:
        start_time = now_iso()
        with task_lock:
            task_rows[source_name]["status"] = "running"
            task_rows[source_name]["startedAt"] = start_time
            task_rows[source_name]["heartbeatAt"] = start_time
            task_rows[source_name]["_startedMonotonic"] = time.perf_counter()
            task_rows[source_name]["_slowWarned"] = False
        update_fetch_work_item_progress(
            task_runtime,
            source_name,
            phase_key="starting_source",
            phase_label="Starting source",
            emit_event=True,
            event_level="info",
            event_message=f"Started source {source_name}.",
        )
        write_task_state(force=True)
        if config.show_progress:
            _emit_progress_line(f"[jobs_fetcher] START source={source_name}")

    def mark_task_finished(source_name: str, report: dict[str, Any]) -> None:
        end_time = now_iso()
        report_status = str(report.get("status") or "").strip().lower()
        with task_lock:
            task_rows[source_name]["status"] = (
                "excluded"
                if report_status == "excluded"
                else "ok"
                if report_status == "ok"
                else "error"
            )
            task_rows[source_name]["finishedAt"] = end_time
            task_rows[source_name]["durationMs"] = int(report.get("durationMs") or 0)
            task_rows[source_name]["heartbeatAt"] = end_time
            task_rows[source_name]["error"] = clean_text(report.get("error"))
            task_rows[source_name]["_slowWarned"] = False
        update_fetch_work_item_progress(
            task_runtime,
            source_name,
            phase_key="completed_source"
            if report_status in {"ok", "excluded"}
            else "failed_source",
            phase_label="Completed"
            if report_status == "ok"
            else "Excluded"
            if report_status == "excluded"
            else "Failed",
            counts={
                "fetchedCount": int(report.get("fetchedCount") or 0),
                "keptCount": int(report.get("keptCount") or 0),
                "durationMs": int(report.get("durationMs") or 0),
            },
            emit_event=True,
            event_level="success"
            if report_status == "ok"
            else "warn"
            if report_status == "excluded"
            else "error",
            event_message=(
                f"Finished source {source_name}: status={report_status or 'ok'}, "
                f"fetched={int(report.get('fetchedCount') or 0)}, "
                f"kept={int(report.get('keptCount') or 0)}."
            ),
        )
        write_progress_report()
        write_task_state(force=True)
        if config.show_progress:
            error_text = clean_text(report.get("error"))
            if report.get("status") == "error" and error_text:
                _emit_progress_line(f"[jobs_fetcher] ERROR source={source_name} error={error_text}")
            elif error_text:
                _emit_progress_line(f"[jobs_fetcher] WARN source={source_name} error={error_text}")
            _emit_progress_line(
                f"[jobs_fetcher] DONE source={source_name} status={report['status']} "
                f"fetched={int(report.get('fetchedCount') or 0)} "
                f"kept={int(report.get('keptCount') or 0)} "
                f"durationMs={int(report.get('durationMs') or 0)}"
            )

    write_progress_report()
    write_task_state(force=True)
    run_stage()
    set_browser_fallback_state(source_state_rows, browser_fallback_guard.to_state_row())


__all__ = [
    "SourceExecutionStageConfig",
    "resolve_fetch_browser_fallback_helper",
    "run_source_execution_stage",
]
