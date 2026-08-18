"""Jobs pipeline source result assembly helpers.

AI boundary owns: per-source result records, diagnostics aggregation, and source-state report rows.
AI boundary implement in: this file for source result shaping; execution loop and final report writes stay in sibling leaves.
AI boundary search before contracts: pipeline source loop, source reports contracts, fetch-report normalization, and source-state tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused source result tests.
"""

from __future__ import annotations

import inspect
import time
from collections import Counter
from collections.abc import Mapping
from typing import Any, Protocol

from src.jobs.canonicalize import (
    CanonicalNormalizer,
    GoogleSheetsCategoryLinkStatusResolver,
    GoogleSheetsProviderTitleResolver,
)
from src.jobs.common.taxonomy import (
    ClassificationContext,
    FailureBucket,
    classification_context_from_source_detail,
    classify_zero_kept,
    map_error_to_failure_bucket,
)
from src.jobs.models import CanonicalJob
from src.jobs.text_utils import clean_text, norm_text
from src.jobs_fetcher_registry import SOURCE_REPORT_META
from src.shared.json_shapes import as_json_list, as_json_object, json_object_rows

from .reporting_summary import format_source_error
from .state_source_records import source_rows_fingerprint

_CANONICAL_DROP_REASON_KEYS = (
    "missing_title",
    "missing_company",
    "missing_job_link",
    "invalid_url",
    "invalid_payload",
    "non_job_static_page",
    "google_sheets_category_row",
)
_GOOGLE_SHEETS_CATEGORY_LINK_STATUS_TIMEOUT_S = 4
_EXPECTED_SOURCE_OPERATION_EXCEPTIONS = (OSError, RuntimeError, TimeoutError, ValueError)


class _SourceExecutionFailure(RuntimeError):
    """Expected per-source loader/canonicalizer failure safe to report as source error."""


def _build_loader_kwargs(
    *,
    name: str,
    adapter_name: str,
    config: Any,
    fetch_text_limited: Any,
    fetch_text_static_limited: Any,
    static_listing_async_fetch: Any,
    source_state_rows: Any,
    guarded_try_playwright: Any,
) -> dict[str, Any]:
    loader_kwargs: dict[str, Any] = {
        "fetch_text": fetch_text_limited,
        "timeout_s": config.timeout_s,
        "retries": config.retries,
        "backoff_s": config.backoff_s,
        "source_state_rows": source_state_rows,
        "force_refresh_all": config.force_refresh_all,
    }
    if adapter_name == "static":
        loader_kwargs["fetch_text"] = fetch_text_static_limited or fetch_text_limited
        loader_kwargs["static_detail_concurrency"] = config.static_detail_concurrency
        if static_listing_async_fetch is not None:
            loader_kwargs["listing_async_fetch"] = static_listing_async_fetch
        if guarded_try_playwright is not None:
            loader_kwargs["try_playwright"] = guarded_try_playwright
    elif adapter_name in {"ashby", "breezy", "jazzhr"} and guarded_try_playwright is not None:
        loader_kwargs["try_playwright"] = guarded_try_playwright
    if name == "scrapy_static_sources":
        loader_kwargs["max_workers"] = config.max_workers
    return loader_kwargs


def _accepted_loader_kwargs(loader: Any, loader_kwargs: dict[str, Any]) -> dict[str, Any]:
    signature = inspect.signature(loader)
    accepts_var_kwargs = any(
        parameter.kind == inspect.Parameter.VAR_KEYWORD
        for parameter in signature.parameters.values()
    )
    if accepts_var_kwargs:
        return loader_kwargs
    return {key: value for key, value in loader_kwargs.items() if key in signature.parameters}


class _PipelineSourceResultsRoot(Protocol):
    SOURCE_DIAGNOSTICS: Mapping[str, dict[str, Any]]

    def _default_adapter_for_loader(self, name: str, base_meta: Mapping[str, Any]) -> str: ...

    def now_iso(self) -> str: ...

    def update_fetch_work_item_progress(
        self,
        task_runtime: Any,
        source_name: str,
        *,
        phase_key: str = "",
        phase_label: str = "",
        counts: dict[str, Any] | None = None,
        target_label: str = "",
        target_url: str = "",
        wait_reason: str = "",
        emit_event: bool = False,
        event_level: str = "muted",
        event_message: str = "",
    ) -> None: ...

    def _is_provider_family_adapter(self, adapter_name: str) -> bool: ...

    def _is_social_subsource_report(self, source_name: str, adapter_name: str) -> bool: ...

    def _failure_bucket_from_zero_extract_context(
        self,
        context: ClassificationContext,
        zero_kept_classification: str,
    ) -> FailureBucket: ...


class _ZeroKeptFailureBucketResolver(Protocol):
    """Only the zero-kept failure-bucket hook ``_classify_report_outcome`` needs."""

    def _failure_bucket_from_zero_extract_context(
        self,
        context: ClassificationContext,
        zero_kept_classification: str,
    ) -> FailureBucket: ...


root: _PipelineSourceResultsRoot | None = None


def _require_root() -> _PipelineSourceResultsRoot:
    if root is None:
        raise RuntimeError("jobs.pipeline_source_results root is not bound")
    return root


def _build_initial_report(
    *,
    name: str,
    base_meta: Mapping[str, Any],
    root_module: _PipelineSourceResultsRoot,
) -> dict[str, Any]:
    return {
        "name": name,
        "status": "ok",
        "adapter": root_module._default_adapter_for_loader(name, base_meta),
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


def _build_loader_callbacks(
    *,
    name: str,
    root_module: _PipelineSourceResultsRoot,
    task_runtime: Any,
    task_rows: Any,
    task_lock: Any,
    write_task_state: Any,
) -> tuple[Any, Any]:
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
                row["heartbeatAt"] = root_module.now_iso()
        write_task_state(force=True)

    def loader_progress_callback(
        *,
        phase_key: str = "",
        phase_label: str = "",
        counts: dict[str, Any] | None = None,
        target_label: str = "",
        target_url: str = "",
        wait_reason: str = "",
        event_level: str = "muted",
        message: str = "",
    ) -> None:
        root_module.update_fetch_work_item_progress(
            task_runtime,
            name,
            phase_key=phase_key,
            phase_label=phase_label,
            counts=counts,
            target_label=target_label,
            target_url=target_url,
            wait_reason=wait_reason,
            emit_event=bool(message),
            event_level=event_level,
            event_message=message,
        )
        write_task_state()

    return loader_heartbeat_callback, loader_progress_callback


def _accepted_or_default_loader_kwargs(
    *,
    loader: Any,
    loader_kwargs: dict[str, Any],
    config: Any,
    heartbeat_callback: Any,
    progress_callback: Any,
) -> dict[str, Any]:
    try:
        return _accepted_loader_kwargs(loader, loader_kwargs)
    except (TypeError, ValueError):
        return {
            "fetch_text": loader_kwargs["fetch_text"],
            "timeout_s": config.timeout_s,
            "retries": config.retries,
            "backoff_s": config.backoff_s,
            "heartbeat_callback": heartbeat_callback,
            "progress_callback": progress_callback,
        }


def _run_loader_and_record_fetch(
    *,
    name: str,
    loader: Any,
    accepted_kwargs: dict[str, Any],
    report: dict[str, Any],
    report_loss: dict[str, Any],
    progress_callback: Any,
) -> tuple[list[Any], int]:
    loader_started = time.perf_counter()
    progress_callback(
        phase_key="loading_source",
        phase_label="Loading source",
        counts={"adapter": clean_text(report.get("adapter")) or "custom"},
        message=f"Loading source {name}.",
    )
    try:
        raw_rows = loader(**accepted_kwargs)
    except _EXPECTED_SOURCE_OPERATION_EXCEPTIONS as exc:
        raise _SourceExecutionFailure(str(exc)) from exc
    fetch_and_parse_ms = int((time.perf_counter() - loader_started) * 1000)
    report["fetchedCount"] = len(raw_rows)
    report_loss["rawFetched"] = int(len(raw_rows))
    progress_callback(
        phase_key="normalizing_rows",
        phase_label="Normalizing rows",
        counts={"fetchedCount": int(len(raw_rows)), "fetchMs": fetch_and_parse_ms},
        message=(
            f"Fetched source {name}: {int(len(raw_rows))} row"
            f"{'' if int(len(raw_rows)) == 1 else 's'} ready for normalization."
        ),
    )
    return raw_rows, fetch_and_parse_ms


def _canonicalize_source_rows(
    *,
    name: str,
    raw_rows: list[Any],
    config: Any,
    fetch_text_limited: Any,
    redirect_resolver: Any,
    report: dict[str, Any],
    report_loss: dict[str, Any],
    progress_callback: Any,
) -> tuple[list[CanonicalJob], dict[str, int], int]:
    title_hydration_resolver = None
    category_link_status_resolver = None
    if name.startswith("google_sheets"):
        title_hydration_resolver = GoogleSheetsProviderTitleResolver(
            fetch_text=fetch_text_limited,
            timeout_s=config.timeout_s,
            retries=config.retries,
            backoff_s=config.backoff_s,
        )
        category_link_status_resolver = GoogleSheetsCategoryLinkStatusResolver(
            timeout_s=min(
                int(config.timeout_s or _GOOGLE_SHEETS_CATEGORY_LINK_STATUS_TIMEOUT_S),
                _GOOGLE_SHEETS_CATEGORY_LINK_STATUS_TIMEOUT_S,
            )
        )
    normalizer = CanonicalNormalizer(
        source=name,
        fetched_at=config.started_at,
        resolve_redirect_url=redirect_resolver.resolve,
        redirect_resolver=redirect_resolver,
        redirect_concurrency=config.google_sheets_redirect_concurrency,
        title_hydration_resolver=title_hydration_resolver,
        category_link_status_resolver=category_link_status_resolver,
        progress_callback=progress_callback,
    )
    try:
        canonical_batch = normalizer.process(raw_rows)
    except _EXPECTED_SOURCE_OPERATION_EXCEPTIONS as exc:
        raise _SourceExecutionFailure(str(exc)) from exc
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
        reason: int(normalizer.drop_reasons.get(reason, 0))
        for reason in _CANONICAL_DROP_REASON_KEYS
    }
    for reason, count in sorted(normalizer.drop_reasons.items()):
        reason_key = clean_text(reason)
        if reason_key:
            report_loss["canonicalDropReasons"][reason_key] = int(count)
    progress_callback(
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
    return canonical_batch, google_sheet_redirect_stats, canonicalization_ms


def _apply_source_fingerprint(
    *,
    name: str,
    source_state_rows: Any,
    canonical_batch: list[CanonicalJob],
    report: dict[str, Any],
) -> None:
    current_fingerprint = source_rows_fingerprint([row.to_dict() for row in canonical_batch])
    previous_fingerprint = clean_text((source_state_rows.get(name) or {}).get("lastFingerprint"))
    report["sourceFingerprint"] = current_fingerprint
    report["fingerprintChanged"] = bool(current_fingerprint != previous_fingerprint)


def _apply_diagnostics(
    *,
    name: str,
    root_module: _PipelineSourceResultsRoot,
    report: dict[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    diag = as_json_object(root_module.SOURCE_DIAGNOSTICS.get(name))
    if clean_text(diag.get("adapter")):
        report["adapter"] = clean_text(diag.get("adapter"))
    if clean_text(diag.get("studio")):
        report["studio"] = clean_text(diag.get("studio"))
    if clean_text(diag.get("providerUrl")):
        report["providerUrl"] = clean_text(diag.get("providerUrl"))

    details = diag.get("details")
    if isinstance(details, list) and details:
        report["details"] = details
    return diag, json_object_rows(details)


def _apply_detail_cache_counts(
    *,
    report: dict[str, Any],
    detail_rows: list[dict[str, Any]],
    prefix: str,
) -> None:
    decision_counts = Counter(
        clean_text(detail.get("cacheDecision"))
        for detail in detail_rows
        if isinstance(detail, dict) and clean_text(detail.get("cacheDecision"))
    )
    report[f"{prefix}Count"] = len([detail for detail in detail_rows if isinstance(detail, dict)])
    report[f"{prefix}CacheDecisionCounts"] = dict(decision_counts)
    report[f"{prefix}SkippedCount"] = sum(
        1
        for detail in detail_rows
        if isinstance(detail, dict)
        and norm_text(detail.get("status")) == "excluded"
        and clean_text(detail.get("cacheDecision")) in {"skip_fresh", "cooldown_skip"}
    )
    report[f"{prefix}RevalidatedCount"] = sum(
        1
        for detail in detail_rows
        if isinstance(detail, dict) and clean_text(detail.get("cacheDecision")) == "revalidate_only"
    )
    report[f"{prefix}NotModifiedCount"] = sum(
        1
        for detail in detail_rows
        if isinstance(detail, dict)
        and clean_text(detail.get("cacheDecisionReason")) == "not_modified_304"
    )
    report[f"{prefix}RefreshedCount"] = sum(
        1
        for detail in detail_rows
        if isinstance(detail, dict) and norm_text(detail.get("status")) in {"ok", "error"}
    )


def _apply_provider_or_social_counts(
    *,
    name: str,
    root_module: _PipelineSourceResultsRoot,
    report: dict[str, Any],
    detail_rows: list[dict[str, Any]],
) -> None:
    if not detail_rows:
        return
    adapter = clean_text(report.get("adapter"))
    if root_module._is_provider_family_adapter(adapter):
        _apply_detail_cache_counts(report=report, detail_rows=detail_rows, prefix="board")
    if root_module._is_social_subsource_report(name, adapter):
        _apply_detail_cache_counts(report=report, detail_rows=detail_rows, prefix="subsource")


def _static_detail_stats(detail_rows: list[dict[str, Any]]) -> dict[str, int]:
    totals = {
        "listing_fetch_ms": 0,
        "candidate_extraction_ms": 0,
        "detail_fetch_ms": 0,
        "domain_gate_wait_ms": 0,
        "domain_gate_wait_count": 0,
        "listing_batch_count": 0,
        "detail_batch_count": 0,
        "detail_pages_skipped_by_adaptive_stop": 0,
    }
    for detail in detail_rows:
        stats = as_json_object(detail.get("stats"))
        for key in totals:
            totals[key] += int(stats.get(key) or 0)
    return totals


def _apply_static_stage_timings(
    *,
    detail_rows: list[dict[str, Any]],
    task_rows: Any,
    task_lock: Any,
    name: str,
    stage_timings: dict[str, Any],
) -> None:
    totals = _static_detail_stats(detail_rows)
    stage_timings.update(
        {
            "listingFetch": int(totals["listing_fetch_ms"]),
            "candidateExtraction": int(totals["candidate_extraction_ms"]),
            "detailFetch": int(totals["detail_fetch_ms"]),
        }
    )
    for detail in detail_rows:
        stats = as_json_object(detail.get("stats"))
        stats["domain_gate_wait_ms"] = int(totals["domain_gate_wait_ms"])
        stats["domain_gate_wait_count"] = int(totals["domain_gate_wait_count"])
        stats["listing_batch_count"] = int(totals["listing_batch_count"])
        stats["detail_batch_count"] = int(totals["detail_batch_count"])
        stats["detail_pages_skipped_by_adaptive_stop"] = int(
            totals["detail_pages_skipped_by_adaptive_stop"]
        )
    with task_lock:
        row = task_rows.get(name)
        if isinstance(row, dict):
            gate_wait_ms = int(row.get("_staticDomainGateWaitMs") or 0)
            gate_wait_count = int(row.get("_staticDomainGateWaitCount") or 0)
        else:
            gate_wait_ms = 0
            gate_wait_count = 0
    if detail_rows:
        first_stats = as_json_object(detail_rows[0].get("stats"))
        first_stats["domain_gate_wait_ms"] = int(gate_wait_ms)
        first_stats["domain_gate_wait_count"] = int(gate_wait_count)
        first_stats["listing_batch_count"] = int(totals["listing_batch_count"])
        first_stats["detail_batch_count"] = int(totals["detail_batch_count"])
        first_stats["detail_pages_skipped_by_adaptive_stop"] = int(
            totals["detail_pages_skipped_by_adaptive_stop"]
        )


def _apply_csv_stage_timings(
    *,
    detail_rows: list[dict[str, Any]],
    google_sheet_redirect_stats: dict[str, int],
    stage_timings: dict[str, Any],
) -> None:
    parse_csv_ms = 0
    for detail in detail_rows:
        stats = as_json_object(detail.get("stats"))
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
            stats["canonicalize_ms"] = int(google_sheet_redirect_stats.get("canonicalize_ms") or 0)
            for hydration_key in (
                "title_hydration_candidates",
                "title_hydration_feed_fetches",
                "title_hydration_cache_hits",
                "title_hydration_repaired",
                "title_hydration_missed",
                "title_hydration_errors",
                "title_hydration_ms",
            ):
                stats[hydration_key] = int(google_sheet_redirect_stats.get(hydration_key) or 0)
            for link_status_key in (
                "category_link_status_candidates",
                "category_link_status_checked",
                "category_link_status_cache_hits",
                "category_link_status_stale_dropped",
                "category_link_status_errors",
                "category_link_status_ms",
            ):
                stats[link_status_key] = int(google_sheet_redirect_stats.get(link_status_key) or 0)
    stage_timings.update(
        {
            "parseCsv": int(parse_csv_ms),
            "redirectResolve": int(google_sheet_redirect_stats.get("redirect_resolve_ms") or 0),
        }
    )


def _apply_stage_timings(
    *,
    name: str,
    adapter_name: str,
    report: dict[str, Any],
    detail_rows: list[dict[str, Any]],
    task_rows: Any,
    task_lock: Any,
    fetch_and_parse_ms: int,
    canonicalization_ms: int,
    google_sheet_redirect_stats: dict[str, int],
) -> None:
    stage_timings = as_json_object(report.get("stageTimingsMs"))
    stage_timings["fetchAndParse"] = int(fetch_and_parse_ms)
    if adapter_name == "static":
        _apply_static_stage_timings(
            detail_rows=detail_rows,
            task_rows=task_rows,
            task_lock=task_lock,
            name=name,
            stage_timings=stage_timings,
        )
    if norm_text(report.get("adapter")) == "csv":
        _apply_csv_stage_timings(
            detail_rows=detail_rows,
            google_sheet_redirect_stats=google_sheet_redirect_stats,
            stage_timings=stage_timings,
        )
    stage_timings["canonicalization"] = int(canonicalization_ms)
    if any(int(value or 0) > 0 for value in stage_timings.values()):
        report["stageTimingsMs"] = stage_timings


def _apply_partial_errors_and_low_confidence(
    *,
    name: str,
    diag: dict[str, Any],
    report: dict[str, Any],
) -> None:
    partial_errors = [
        clean_text(err) for err in as_json_list(diag.get("partialErrors")) if clean_text(err)
    ]
    if partial_errors:
        report["error"] = "; ".join(format_source_error(name, err) for err in partial_errors[:6])
    report["lowConfidenceDropped"] = int(diag.get("lowConfidenceDropped") or 0)


def _apply_scrapy_loss(detail_rows: list[dict[str, Any]], report_loss: dict[str, Any]) -> None:
    runner_rejected = 0
    parent_invalid = 0
    for detail in detail_rows:
        stats = as_json_object(detail.get("stats"))
        runner_rejected += int(stats.get("jobs_rejected_validation") or 0)
        loss_detail = as_json_object(detail.get("loss"))
        parent_invalid += int(loss_detail.get("scrapyParentInvalidPayload") or 0)
    report_loss["scrapyRunnerRejectedValidation"] = int(runner_rejected)
    report_loss["scrapyParentInvalidPayload"] = int(parent_invalid)


def _apply_static_loss(detail_rows: list[dict[str, Any]], report_loss: dict[str, Any]) -> None:
    static_non_job = 0
    static_dup = 0
    static_empty = 0
    for detail in detail_rows:
        loss_detail = as_json_object(detail.get("loss"))
        static_non_job += int(loss_detail.get("staticNonJobUrlRejected") or 0)
        static_dup += int(loss_detail.get("staticDuplicateLinkRejected") or 0)
        static_empty += int(loss_detail.get("staticDetailParseEmpty") or 0)
    report_loss["staticNonJobUrlRejected"] = int(static_non_job)
    report_loss["staticDuplicateLinkRejected"] = int(static_dup)
    report_loss["staticDetailParseEmpty"] = int(static_empty)


def _apply_source_specific_loss(
    *,
    name: str,
    report: dict[str, Any],
    detail_rows: list[dict[str, Any]],
    report_loss: dict[str, Any],
) -> None:
    if name == "scrapy_static_sources":
        _apply_scrapy_loss(detail_rows, report_loss)
    if norm_text(report.get("adapter")) == "static":
        _apply_static_loss(detail_rows, report_loss)
    report["loss"] = report_loss


def _classify_report_outcome(
    *,
    report: dict[str, Any],
    root_module: _ZeroKeptFailureBucketResolver,
) -> None:
    cls_context = classification_context_from_source_detail(report)
    zero_kept_classification = classify_zero_kept(cls_context)
    failure_bucket = map_error_to_failure_bucket(cls_context)
    if (
        int(report.get("keptCount") or 0) == 0
        and report["status"] != "excluded"
        and failure_bucket == FailureBucket.UNKNOWN
    ):
        failure_bucket = root_module._failure_bucket_from_zero_extract_context(
            cls_context,
            zero_kept_classification.value,
        )
    if report["status"] == "error" or report.get("error") or int(report.get("keptCount") or 0) == 0:
        report["failureBucket"] = failure_bucket.value
    if int(report.get("keptCount") or 0) == 0 and report["status"] != "excluded":
        report["zeroKeptClassification"] = zero_kept_classification.value


def execute_loader(
    *,
    name: str,
    loader,
    config,
    fetch_text_limited,
    fetch_text_static_limited,
    static_listing_async_fetch,
    source_state_rows,
    redirect_resolver,
    task_runtime,
    task_rows,
    task_lock,
    thread_local,
    write_task_state,
    guarded_try_playwright,
) -> tuple[dict[str, Any], list[CanonicalJob]]:
    root_module = _require_root()
    source_started = time.perf_counter()
    base_meta = as_json_object(SOURCE_REPORT_META.get(name))
    report = _build_initial_report(name=name, base_meta=base_meta, root_module=root_module)
    report_loss = as_json_object(report.get("loss"))
    adapter_name = norm_text(report.get("adapter"))
    canonical_batch: list[CanonicalJob] = []
    heartbeat_callback, progress_callback = _build_loader_callbacks(
        name=name,
        root_module=root_module,
        task_runtime=task_runtime,
        task_rows=task_rows,
        task_lock=task_lock,
        write_task_state=write_task_state,
    )

    try:
        thread_local.source_name = name
        loader_kwargs = _build_loader_kwargs(
            name=name,
            adapter_name=adapter_name,
            config=config,
            fetch_text_limited=fetch_text_limited,
            fetch_text_static_limited=fetch_text_static_limited,
            static_listing_async_fetch=static_listing_async_fetch,
            source_state_rows=source_state_rows,
            guarded_try_playwright=guarded_try_playwright,
        )
        loader_kwargs["heartbeat_callback"] = heartbeat_callback
        loader_kwargs["progress_callback"] = progress_callback
        accepted_kwargs = _accepted_or_default_loader_kwargs(
            loader=loader,
            loader_kwargs=loader_kwargs,
            config=config,
            heartbeat_callback=heartbeat_callback,
            progress_callback=progress_callback,
        )
        raw_rows, fetch_and_parse_ms = _run_loader_and_record_fetch(
            name=name,
            loader=loader,
            accepted_kwargs=accepted_kwargs,
            report=report,
            report_loss=report_loss,
            progress_callback=progress_callback,
        )
        canonical_batch, redirect_stats, canonicalization_ms = _canonicalize_source_rows(
            name=name,
            raw_rows=raw_rows,
            config=config,
            fetch_text_limited=fetch_text_limited,
            redirect_resolver=redirect_resolver,
            report=report,
            report_loss=report_loss,
            progress_callback=progress_callback,
        )
        _apply_source_fingerprint(
            name=name,
            source_state_rows=source_state_rows,
            canonical_batch=canonical_batch,
            report=report,
        )
        diag, detail_rows = _apply_diagnostics(name=name, root_module=root_module, report=report)
        _apply_provider_or_social_counts(
            name=name,
            root_module=root_module,
            report=report,
            detail_rows=detail_rows,
        )
        _apply_stage_timings(
            name=name,
            adapter_name=adapter_name,
            report=report,
            detail_rows=detail_rows,
            task_rows=task_rows,
            task_lock=task_lock,
            fetch_and_parse_ms=fetch_and_parse_ms,
            canonicalization_ms=canonicalization_ms,
            google_sheet_redirect_stats=redirect_stats,
        )
        _apply_partial_errors_and_low_confidence(name=name, diag=diag, report=report)
        _apply_source_specific_loss(
            name=name,
            report=report,
            detail_rows=detail_rows,
            report_loss=report_loss,
        )
    except _SourceExecutionFailure as exc:
        report["status"] = "error"
        report["error"] = format_source_error(name, exc)
        diag, detail_rows = _apply_diagnostics(name=name, root_module=root_module, report=report)
        _apply_provider_or_social_counts(
            name=name,
            root_module=root_module,
            report=report,
            detail_rows=detail_rows,
        )
        _apply_partial_errors_and_low_confidence(name=name, diag=diag, report=report)
        _apply_source_specific_loss(
            name=name,
            report=report,
            detail_rows=detail_rows,
            report_loss=report_loss,
        )
    finally:
        thread_local.source_name = ""

    report["durationMs"] = int((time.perf_counter() - source_started) * 1000)
    _classify_report_outcome(report=report, root_module=root_module)
    return report, canonical_batch
