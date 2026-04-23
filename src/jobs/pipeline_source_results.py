from __future__ import annotations

import inspect
import time
from collections import Counter
from typing import Any

from src.jobs.canonicalize import CanonicalNormalizer
from src.jobs.common.taxonomy import ClassificationContext, FailureBucket, classify_zero_kept, map_error_to_failure_bucket
from src.jobs.models import CanonicalJob
from src.jobs.text_utils import clean_text, norm_text
from src.jobs_fetcher_registry import SOURCE_REPORT_META

from .pipeline_source_progress import fallback_error_report
from .reporting_summary import format_source_error
from .state_source_records import source_rows_fingerprint

root = None


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
    source_started = time.perf_counter()
    base_meta = SOURCE_REPORT_META.get(name, {})
    report: dict[str, Any] = {
        "name": name,
        "status": "ok",
        "adapter": getattr(root, "_default_adapter_for_loader")(name, base_meta),
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
                row["heartbeatAt"] = root.now_iso()
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
        root.update_fetch_work_item_progress(
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

    try:
        thread_local.source_name = name
        loader_started = time.perf_counter()
        adapter_name = norm_text(report.get("adapter"))
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
        if name == "scrapy_static_sources":
            loader_kwargs["max_workers"] = config.max_workers
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
                "fetch_text": loader_kwargs["fetch_text"],
                "timeout_s": config.timeout_s,
                "retries": config.retries,
                "backoff_s": config.backoff_s,
                "heartbeat_callback": loader_heartbeat_callback,
                "progress_callback": loader_progress_callback,
            }

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
        previous_fingerprint = clean_text((source_state_rows.get(name) or {}).get("lastFingerprint"))
        report["sourceFingerprint"] = current_fingerprint
        report["fingerprintChanged"] = bool(current_fingerprint != previous_fingerprint)

        diag = getattr(root, "SOURCE_DIAGNOSTICS").get(name) or {}
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
        if detail_rows and getattr(root, "_is_provider_family_adapter")(clean_text(report.get("adapter"))):
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
        if detail_rows and getattr(root, "_is_social_subsource_report")(name, clean_text(report.get("adapter"))):
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
        if adapter_name == "static":
            listing_fetch_ms = 0
            candidate_extraction_ms = 0
            detail_fetch_ms = 0
            static_domain_gate_wait_ms = 0
            static_domain_gate_wait_count = 0
            static_listing_batch_count = 0
            static_detail_batch_count = 0
            static_detail_pages_skipped = 0
            for detail in detail_rows:
                if not isinstance(detail, dict):
                    continue
                stats = detail.get("stats") if isinstance(detail.get("stats"), dict) else {}
                listing_fetch_ms += int(stats.get("listing_fetch_ms") or 0)
                candidate_extraction_ms += int(stats.get("candidate_extraction_ms") or 0)
                detail_fetch_ms += int(stats.get("detail_fetch_ms") or 0)
                static_domain_gate_wait_ms += int(stats.get("domain_gate_wait_ms") or 0)
                static_domain_gate_wait_count += int(stats.get("domain_gate_wait_count") or 0)
                static_listing_batch_count += int(stats.get("listing_batch_count") or 0)
                static_detail_batch_count += int(stats.get("detail_batch_count") or 0)
                static_detail_pages_skipped += int(
                    stats.get("detail_pages_skipped_by_adaptive_stop") or 0
                )
            stage_timings.update(
                {
                    "listingFetch": int(listing_fetch_ms),
                    "candidateExtraction": int(candidate_extraction_ms),
                    "detailFetch": int(detail_fetch_ms),
                }
            )
            if detail_rows:
                for detail in detail_rows:
                    if not isinstance(detail, dict):
                        continue
                    stats = detail.get("stats") if isinstance(detail.get("stats"), dict) else {}
                    stats["domain_gate_wait_ms"] = int(static_domain_gate_wait_ms)
                    stats["domain_gate_wait_count"] = int(static_domain_gate_wait_count)
                    stats["listing_batch_count"] = int(static_listing_batch_count)
                    stats["detail_batch_count"] = int(static_detail_batch_count)
                    stats["detail_pages_skipped_by_adaptive_stop"] = int(
                        static_detail_pages_skipped
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
                first_detail = detail_rows[0] if isinstance(detail_rows[0], dict) else None
                if isinstance(first_detail, dict):
                    first_stats = (
                        first_detail.get("stats")
                        if isinstance(first_detail.get("stats"), dict)
                        else {}
                    )
                    first_stats["domain_gate_wait_ms"] = int(gate_wait_ms)
                    first_stats["domain_gate_wait_count"] = int(gate_wait_count)
                    first_stats["listing_batch_count"] = int(static_listing_batch_count)
                    first_stats["detail_batch_count"] = int(static_detail_batch_count)
                    first_stats["detail_pages_skipped_by_adaptive_stop"] = int(
                        static_detail_pages_skipped
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
        failure_bucket = getattr(root, "_failure_bucket_from_zero_extract_context")(
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
