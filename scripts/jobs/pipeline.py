"""Package-owned pipeline entrypoints."""

from __future__ import annotations

import argparse
import inspect
import json
import os
import sys
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from scripts.jobs import canonicalize as canonicalize_pkg
from scripts.jobs import dedup as dedup_pkg
from scripts.jobs import common
from scripts.jobs import reporting as reporting_pkg
from scripts.jobs import state as state_pkg
from scripts.jobs import transport as transport_pkg
from scripts.jobs.adapters import default_source_loaders as package_default_source_loaders
from scripts.jobs.models import CanonicalJob

SourceLoader = common.SourceLoader
RawJob = common.RawJob

DEFAULT_OUTPUT_DIR = common.DEFAULT_OUTPUT_DIR
DEFAULT_TIMEOUT_S = common.DEFAULT_TIMEOUT_S
DEFAULT_RETRIES = common.DEFAULT_RETRIES
DEFAULT_BACKOFF_S = common.DEFAULT_BACKOFF_S
DEFAULT_FETCH_STRATEGY = common.DEFAULT_FETCH_STRATEGY
DEFAULT_ADAPTER_HTTP_CONCURRENCY = common.DEFAULT_ADAPTER_HTTP_CONCURRENCY
DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY = common.DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY
DEFAULT_STATIC_DETAIL_CONCURRENCY = common.DEFAULT_STATIC_DETAIL_CONCURRENCY
DEFAULT_HOT_SOURCE_CADENCE_MINUTES = common.DEFAULT_HOT_SOURCE_CADENCE_MINUTES
DEFAULT_COLD_SOURCE_CADENCE_MINUTES = common.DEFAULT_COLD_SOURCE_CADENCE_MINUTES
DEFAULT_SOCIAL_CONFIG_PATH = common.DEFAULT_SOCIAL_CONFIG_PATH
DEFAULT_SOCIAL_LOOKBACK_MINUTES = common.DEFAULT_SOCIAL_LOOKBACK_MINUTES
DEFAULT_SOCIAL_MIN_CONFIDENCE = common.DEFAULT_SOCIAL_MIN_CONFIDENCE
DEFAULT_STATIC_DETAIL_HEURISTICS_PROFILE = common.DEFAULT_STATIC_DETAIL_HEURISTICS_PROFILE
DEFAULT_SCRAPY_VALIDATION_STRICT = common.DEFAULT_SCRAPY_VALIDATION_STRICT
DEFAULT_CANONICAL_STRICT_URL = common.DEFAULT_CANONICAL_STRICT_URL
SCHEMA_VERSION = common.SCHEMA_VERSION
STUDIO_SOURCE_REGISTRY = common.STUDIO_SOURCE_REGISTRY
SOURCE_REPORT_META = common.SOURCE_REPORT_META
SOURCE_DIAGNOSTICS = common.SOURCE_DIAGNOSTICS
OUTPUT_FIELDS = common.OUTPUT_FIELDS
LIGHTWEIGHT_OUTPUT_FIELDS = common.LIGHTWEIGHT_OUTPUT_FIELDS

clean_text = common.clean_text
norm_text = common.norm_text
now_iso = common.now_iso
load_social_config = common.load_social_config
default_fetch_text = transport_pkg.default_fetch_text
resolve_fetch_text_impl = transport_pkg.resolve_fetch_text_impl
build_redirect_resolver = transport_pkg.build_redirect_resolver
write_text_if_changed = common.write_text_if_changed
serialize_rows_for_json = common.serialize_rows_for_json
serialize_rows_for_csv = common.serialize_rows_for_csv
read_existing_output_from_file = common.read_existing_output_from_file
load_registry_from_file = common.load_registry_from_file
read_approved_since_last_run = common.read_approved_since_last_run
env_flag = common.env_flag

canonicalize_job = canonicalize_pkg.canonicalize_job
canonicalize_job_with_reason = canonicalize_pkg.canonicalize_job_with_reason
canonicalize_google_sheets_rows = canonicalize_pkg.canonicalize_google_sheets_rows
deduplicate_jobs = dedup_pkg.deduplicate_jobs
format_source_error = reporting_pkg.format_source_error
build_pipeline_summary = reporting_pkg.build_pipeline_summary
build_browser_fallback_queue = reporting_pkg.build_browser_fallback_queue
normalize_runtime_payload = reporting_pkg.normalize_runtime_payload
normalize_fetch_report_payload = reporting_pkg.normalize_fetch_report_payload
source_rows_fingerprint = state_pkg.source_rows_fingerprint
read_source_state = state_pkg.read_source_state
write_source_state = state_pkg.write_source_state
read_job_lifecycle_state = state_pkg.read_job_lifecycle_state
write_job_lifecycle_state = state_pkg.write_job_lifecycle_state
lifecycle_counts = state_pkg.lifecycle_counts
apply_job_lifecycle_state = state_pkg.apply_job_lifecycle_state
read_previously_successful_sources = state_pkg.read_previously_successful_sources
read_success_cache = state_pkg.read_success_cache
write_success_cache = state_pkg.write_success_cache
normalize_task_state_payload = state_pkg.normalize_task_state_payload
should_skip_source_by_ttl = state_pkg.should_skip_source_by_ttl
should_skip_source_by_cadence = state_pkg.should_skip_source_by_cadence
apply_circuit_breaker_exclusions = state_pkg.apply_circuit_breaker_exclusions
append_excluded_default_sources = state_pkg.append_excluded_default_sources
update_source_state_rows = state_pkg.update_source_state_rows


def _rows_to_legacy_dicts(rows: List[CanonicalJob]) -> List[Dict[str, Any]]:
    return [row.to_dict() if isinstance(row, CanonicalJob) else dict(row) for row in rows]


def _canonicalize_existing_output_row(row: Dict[str, Any], *, source: str, fetched_at: str) -> Dict[str, Any] | None:
    normalized = canonicalize_job(row, source=source, fetched_at=fetched_at)
    if not normalized:
        return None
    payload = normalized.to_dict()
    if clean_text(row.get("dedupKey")):
        payload["dedupKey"] = clean_text(row.get("dedupKey"))
    return payload


def default_source_loaders(
    *,
    social_enabled: bool = False,
    social_config: Optional[Dict[str, Any]] = None,
) -> List[Tuple[str, SourceLoader]]:
    facade = sys.modules.get("scripts.jobs_fetcher")
    facade_loader = getattr(facade, "default_source_loaders", None) if facade is not None else None
    if callable(facade_loader) and facade_loader is not default_source_loaders:
        try:
            return facade_loader(
                social_enabled=social_enabled,
                social_config=social_config,
            )
        except TypeError:
            return facade_loader()
    try:
        return common.default_source_loaders(
            social_enabled=social_enabled,
            social_config=social_config,
        )
    except TypeError:
        return common.default_source_loaders()


def _build_excluded_source_report(source_name: str, reason: str) -> Dict[str, Any]:
    return {
        "name": source_name,
        "status": "excluded",
        "adapter": clean_text(SOURCE_REPORT_META.get(source_name, {}).get("adapter")) or "custom",
        "fetchStrategy": clean_text(SOURCE_REPORT_META.get(source_name, {}).get("fetchStrategy")) or "auto",
        "studio": clean_text(SOURCE_REPORT_META.get(source_name, {}).get("studio")) or "",
        "fetchedCount": 0,
        "keptCount": 0,
        "error": clean_text(reason),
        "exclusionReason": clean_text(reason),
        "durationMs": 0,
    }


def run_pipeline(
    *,
    output_dir: Path,
    timeout_s: int = DEFAULT_TIMEOUT_S,
    retries: int = DEFAULT_RETRIES,
    backoff_s: float = DEFAULT_BACKOFF_S,
    preserve_previous_on_empty: bool = True,
    fetch_text: Callable[[str, int], str] = default_fetch_text,
    source_loaders: Optional[List[Tuple[str, SourceLoader]]] = None,
    seed_from_existing_output: bool = False,
    source_ttl_minutes: int = 0,
    max_workers: int = 1,
    max_per_domain: int = 2,
    fetch_strategy: str = DEFAULT_FETCH_STRATEGY,
    adapter_http_concurrency: int = DEFAULT_ADAPTER_HTTP_CONCURRENCY,
    google_sheets_redirect_concurrency: int = DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY,
    respect_source_cadence: bool = False,
    hot_source_cadence_minutes: int = DEFAULT_HOT_SOURCE_CADENCE_MINUTES,
    cold_source_cadence_minutes: int = DEFAULT_COLD_SOURCE_CADENCE_MINUTES,
    circuit_breaker_failures: int = 3,
    circuit_breaker_cooldown_minutes: int = 180,
    ignore_circuit_breaker: bool = False,
    social_enabled: bool = False,
    social_config_path: Optional[Path] = None,
    social_lookback_minutes: int = DEFAULT_SOCIAL_LOOKBACK_MINUTES,
    static_detail_concurrency: int = DEFAULT_STATIC_DETAIL_CONCURRENCY,
    show_progress: bool = True,
    selection_exclusions: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    json_path = output_dir / "jobs-unified.json"
    csv_path = output_dir / "jobs-unified.csv"
    light_json_path = output_dir / "jobs-unified-light.json"
    report_path = output_dir / "jobs-fetch-report.json"
    success_cache_path = output_dir / "jobs-success-cache.json"
    source_state_path = output_dir / "jobs-source-state.json"
    lifecycle_state_path = output_dir / "jobs-lifecycle-state.json"
    browser_fallback_queue_path = output_dir / "jobs-browser-fallback-queue.json"
    task_state_path = output_dir / "jobs-fetch-tasks.json"
    pending_registry_path = output_dir / "source-registry-pending.json"
    approval_state_path = output_dir / "source-approval-state.json"
    SOURCE_DIAGNOSTICS.clear()

    started_at = now_iso()
    source_reports: List[Dict[str, Any]] = []
    if isinstance(selection_exclusions, list):
        source_reports.extend([row for row in selection_exclusions if isinstance(row, dict)])
    canonical_rows: List[CanonicalJob] = []
    max_workers = max(1, int(max_workers or 1))
    max_per_domain = max(1, int(max_per_domain or 1))
    adapter_http_concurrency = max(1, int(adapter_http_concurrency or 1))
    google_sheets_redirect_concurrency = max(1, int(google_sheets_redirect_concurrency or 1))
    static_detail_concurrency = max(1, int(static_detail_concurrency or 1))
    hot_source_cadence_minutes = max(1, int(hot_source_cadence_minutes or 1))
    cold_source_cadence_minutes = max(1, int(cold_source_cadence_minutes or 1))
    fetch_text_impl, fetch_client, async_fetcher = resolve_fetch_text_impl(
        fetch_text=fetch_text,
        fetch_strategy=fetch_strategy,
        adapter_http_concurrency=adapter_http_concurrency,
    )
    redirect_resolver = build_redirect_resolver(
        timeout_s=timeout_s,
        max_connections=google_sheets_redirect_concurrency,
    )
    source_state_rows = read_source_state(source_state_path)
    lifecycle_rows = read_job_lifecycle_state(lifecycle_state_path)
    if seed_from_existing_output:
        seeded_rows = read_existing_output_from_file(
            json_path,
            started_at,
            canonicalize_job=_canonicalize_existing_output_row,
            clean_text=clean_text,
        )
        canonical_rows.extend(CanonicalJob.from_mapping(row) for row in seeded_rows)

    runtime_payload: Dict[str, Any] = {}

    def write_progress_report() -> None:
        deduped_progress_rows, dedup_progress_stats = deduplicate_jobs(canonical_rows)
        dedup_progress_stats["outputCount"] = len(deduped_progress_rows)
        progress_lifecycle_counts = lifecycle_counts(lifecycle_rows)
        progress_payload = normalize_fetch_report_payload({
            "schemaVersion": SCHEMA_VERSION,
            "startedAt": started_at,
            "finishedAt": "",
            "runtime": runtime_payload,
            "summary": build_pipeline_summary(
                dedup_progress_stats,
                deduped_progress_rows,
                source_reports,
                len(canonical_rows),
                False,
                len([row for row in STUDIO_SOURCE_REGISTRY if bool(row.get("enabledByDefault", True))]),
                len(load_registry_from_file(pending_registry_path, [])),
                read_approved_since_last_run(approval_state_path),
                json_bytes=0,
                csv_bytes=0,
                light_json_bytes=0,
                lifecycle_counts_map=progress_lifecycle_counts,
            ),
            "sources": source_reports,
            "outputs": {
                "json": str(json_path),
                "csv": str(csv_path),
                "lightJson": str(light_json_path),
                "report": str(report_path),
                "lifecycleState": str(lifecycle_state_path),
                "changed": {"json": False, "csv": False, "lightJson": False},
            },
        })
        write_text_if_changed(report_path, json.dumps(progress_payload, indent=2, ensure_ascii=False))

    effective_social_config_path = Path(social_config_path) if social_config_path else (output_dir / "social-sources-config.json")
    social_config = load_social_config(
        config_path=effective_social_config_path,
        enabled=bool(social_enabled),
        lookback_minutes=social_lookback_minutes,
    )

    if source_loaders is None:
        try:
            selected_loaders = default_source_loaders(
                social_enabled=bool(social_enabled),
                social_config=social_config,
            )
        except TypeError:
            selected_loaders = default_source_loaders()
    else:
        selected_loaders = list(source_loaders)

    def _source_priority(item: Tuple[str, SourceLoader]) -> Tuple[int, int]:
        source_name = clean_text(item[0])
        adapter = clean_text(SOURCE_REPORT_META.get(source_name, {}).get("adapter"))
        state = source_state_rows.get(source_name) if isinstance(source_state_rows.get(source_name), dict) else {}
        duration_ms = int((state or {}).get("lastDurationMs") or 0)
        detail_pages = int((state or {}).get("lastDetailPagesVisited") or 0)
        static_priority = 0 if adapter == "static" else 1
        return (static_priority, -(duration_ms + (detail_pages * 25)))

    selected_loaders = sorted(selected_loaders, key=_source_priority)
    using_default_loaders = source_loaders is None
    runtime_payload = normalize_runtime_payload({
        "maxWorkers": max_workers,
        "maxPerDomain": max_per_domain,
        "fetchStrategy": clean_text(fetch_strategy) or DEFAULT_FETCH_STRATEGY,
        "fetchClient": fetch_client,
        "adapterHttpConcurrency": adapter_http_concurrency,
        "staticDetailConcurrency": static_detail_concurrency,
        "googleSheetsRedirectConcurrency": google_sheets_redirect_concurrency,
        "seedFromExistingOutput": bool(seed_from_existing_output),
        "sourceTtlMinutes": int(source_ttl_minutes or 0),
        "respectSourceCadence": bool(respect_source_cadence),
        "hotSourceCadenceMinutes": hot_source_cadence_minutes,
        "coldSourceCadenceMinutes": cold_source_cadence_minutes,
        "circuitBreakerFailures": int(circuit_breaker_failures or 0),
        "circuitBreakerCooldownMinutes": int(circuit_breaker_cooldown_minutes or 0),
        "ignoreCircuitBreaker": bool(ignore_circuit_breaker),
        "socialEnabled": bool(social_enabled),
        "socialConfigPath": str(effective_social_config_path),
        "socialLookbackMinutes": int(social_config.get("lookbackMinutes") or DEFAULT_SOCIAL_LOOKBACK_MINUTES),
        "socialMinConfidence": int(social_config.get("minConfidence") or DEFAULT_SOCIAL_MIN_CONFIDENCE),
        "staticDetailHeuristicsProfile": norm_text(os.getenv("BALUFFO_STATIC_DETAIL_HEURISTICS_PROFILE"))
        or DEFAULT_STATIC_DETAIL_HEURISTICS_PROFILE,
        "scrapyValidationStrict": env_flag("BALUFFO_SCRAPY_VALIDATION_STRICT", DEFAULT_SCRAPY_VALIDATION_STRICT),
        "canonicalStrictUrlValidation": env_flag("BALUFFO_CANONICAL_STRICT_URL", DEFAULT_CANONICAL_STRICT_URL),
        "selectedSourceCount": len(selected_loaders),
    }, selected_source_count=len(selected_loaders))

    if respect_source_cadence:
        cadence_skipped: List[Dict[str, Any]] = []
        filtered_loaders: List[Tuple[str, SourceLoader]] = []
        for name, loader in selected_loaders:
            if should_skip_source_by_cadence(
                name,
                source_state_rows,
                hot_minutes=hot_source_cadence_minutes,
                cold_minutes=cold_source_cadence_minutes,
            ):
                cadence_skipped.append(_build_excluded_source_report(name, "skipped_by_source_cadence"))
                continue
            filtered_loaders.append((name, loader))
        selected_loaders = filtered_loaders
        source_reports.extend(cadence_skipped)
        runtime_payload["selectedSourceCount"] = len(selected_loaders)

    selected_loaders, excluded_by_circuit = apply_circuit_breaker_exclusions(
        selected_loaders,
        source_state_rows=source_state_rows,
        circuit_breaker_failures=circuit_breaker_failures,
        circuit_breaker_cooldown_minutes=circuit_breaker_cooldown_minutes,
        ignore_circuit_breaker=ignore_circuit_breaker,
    )
    source_reports.extend(excluded_by_circuit)

    task_rows: Dict[str, Dict[str, Any]] = {
        name: {
            "name": name,
            "status": "queued",
            "startedAt": "",
            "finishedAt": "",
            "durationMs": 0,
            "heartbeatAt": "",
            "error": "",
        }
        for name, _ in selected_loaders
    }
    task_lock = threading.Lock()
    last_task_write_monotonic = 0.0
    last_heartbeat_write: Dict[str, float] = {}

    def write_task_state(finished_at: str = "", *, force: bool = False) -> None:
        nonlocal last_task_write_monotonic
        now_mono = time.perf_counter()
        if not force and (now_mono - last_task_write_monotonic) < 0.9:
            return
        last_task_write_monotonic = now_mono
        with task_lock:
            rows_snapshot = [dict(row) for row in task_rows.values()]
        payload = normalize_task_state_payload({
            "startedAt": started_at,
            "finishedAt": finished_at,
            "summary": {
                "queued": sum(1 for row in rows_snapshot if row.get("status") == "queued"),
                "running": sum(1 for row in rows_snapshot if row.get("status") == "running"),
                "ok": sum(1 for row in rows_snapshot if row.get("status") == "ok"),
                "error": sum(1 for row in rows_snapshot if row.get("status") == "error"),
            },
            "tasks": rows_snapshot,
            "outputs": {"report": str(report_path)},
        }, started_at=started_at, finished_at=finished_at, report_path=str(report_path))
        write_text_if_changed(task_state_path, json.dumps(payload, indent=2, ensure_ascii=False))

    thread_local = threading.local()
    domain_lock = threading.Lock()
    domain_gates: Dict[str, threading.BoundedSemaphore] = {}

    def fetch_text_limited(url: str, timeout: int) -> str:
        host = clean_text(urlparse(url).netloc).lower() or "_unknown"
        with domain_lock:
            gate = domain_gates.get(host)
            if gate is None:
                gate = threading.BoundedSemaphore(max_per_domain)
                domain_gates[host] = gate
        gate.acquire()
        try:
            current = clean_text(getattr(thread_local, "source_name", ""))
            if current and current in task_rows:
                now_mono = time.perf_counter()
                if (now_mono - float(last_heartbeat_write.get(current) or 0.0)) >= 4.0:
                    with task_lock:
                        if task_rows[current].get("status") == "running":
                            task_rows[current]["heartbeatAt"] = now_iso()
                    last_heartbeat_write[current] = now_mono
                    write_task_state()
            return fetch_text_impl(url, timeout)
        finally:
            gate.release()

    def execute_loader(name: str, loader: SourceLoader) -> Tuple[Dict[str, Any], List[CanonicalJob]]:
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
            loader_kwargs = {
                "fetch_text": fetch_text_limited,
                "timeout_s": timeout_s,
                "retries": retries,
                "backoff_s": backoff_s,
            }
            if norm_text(report.get("adapter")) == "static":
                loader_kwargs["static_detail_concurrency"] = static_detail_concurrency
                loader_kwargs["source_state_rows"] = source_state_rows
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
                    "timeout_s": timeout_s,
                    "retries": retries,
                    "backoff_s": backoff_s,
                }
            raw_rows = loader(**accepted_kwargs)
            report["fetchedCount"] = len(raw_rows)
            report_loss = report["loss"] if isinstance(report.get("loss"), dict) else {}
            report_loss["rawFetched"] = int(len(raw_rows))
            drop_reasons = Counter()
            kept = 0
            google_sheet_redirect_stats: Dict[str, int] = {}
            if name.startswith("google_sheets"):
                canonical_batch, drop_reasons, google_sheet_redirect_stats = canonicalize_google_sheets_rows(
                    raw_rows,
                    source=name,
                    fetched_at=started_at,
                    redirect_resolver=redirect_resolver,
                    redirect_concurrency=google_sheets_redirect_concurrency,
                )
                kept = len(canonical_batch)
                canonicalization_ms = int(google_sheet_redirect_stats.get("canonicalize_ms") or 0)
            else:
                canonicalization_started = time.perf_counter()
                for raw in raw_rows:
                    normalized, drop_reason = canonicalize_job_with_reason(
                        raw,
                        source=name,
                        fetched_at=started_at,
                        resolve_redirect_url=redirect_resolver.resolve,
                    )
                    if normalized:
                        canonical_batch.append(normalized)
                        kept += 1
                    elif drop_reason:
                        drop_reasons[drop_reason] += 1
                canonicalization_ms = int((time.perf_counter() - canonicalization_started) * 1000)
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
                stage_timings.update({
                    "listingFetch": int(listing_fetch_ms),
                    "candidateExtraction": int(candidate_extraction_ms),
                    "detailFetch": int(detail_fetch_ms),
                })
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
                stage_timings.update({
                    "parseCsv": int(parse_csv_ms),
                    "redirectResolve": int(google_sheet_redirect_stats.get("redirect_resolve_ms") or 0),
                })
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

    def mark_task_started(source_name: str) -> None:
        start_time = now_iso()
        with task_lock:
            task_rows[source_name]["status"] = "running"
            task_rows[source_name]["startedAt"] = start_time
            task_rows[source_name]["heartbeatAt"] = start_time
        write_task_state(force=True)
        if show_progress:
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
        if show_progress:
            print(
                f"[jobs_fetcher] DONE source={source_name} status={report['status']} "
                f"fetched={int(report.get('fetchedCount') or 0)} "
                f"kept={int(report.get('keptCount') or 0)} "
                f"durationMs={int(report.get('durationMs') or 0)}",
                flush=True,
            )

    def persist_source_result(source_name: str, report: Dict[str, Any], canonical_batch: List[CanonicalJob]) -> None:
        canonical_rows.extend(canonical_batch)
        source_reports.append(report)
        mark_task_finished(source_name, report)

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
                    "invalid_url": 0,
                    "invalid_payload": 0,
                },
            },
        }

    def run_source_execution_stage() -> None:
        if max_workers <= 1 or len(selected_loaders) <= 1:
            for source_name, loader in selected_loaders:
                mark_task_started(source_name)
                report, canonical_batch = execute_loader(source_name, loader)
                persist_source_result(source_name, report, canonical_batch)
            return

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
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
                persist_source_result(source_name, report, canonical_batch)

    write_progress_report()
    write_task_state(force=True)
    try:
        run_source_execution_stage()
    finally:
        if async_fetcher is not None:
            async_fetcher.close()
        close_redirect_resolver = getattr(redirect_resolver, "close", None)
        if callable(close_redirect_resolver):
            close_redirect_resolver()

    if using_default_loaders:
        append_excluded_default_sources(source_reports)

    deduped_rows, dedup_stats = deduplicate_jobs(canonical_rows)

    preserved_previous = False
    if preserve_previous_on_empty and not deduped_rows:
        previous_rows = read_existing_output_from_file(
            json_path,
            started_at,
            canonicalize_job=_canonicalize_existing_output_row,
            clean_text=clean_text,
        )
        if previous_rows:
            deduped_rows = [CanonicalJob.from_mapping(row) for row in previous_rows]
            preserved_previous = True

    selected_loader_names = {name for name, _ in selected_loaders}
    selected_reports = [row for row in source_reports if clean_text(row.get("name")) in selected_loader_names]
    run_is_healthy = all(norm_text(row.get("status")) == "ok" for row in selected_reports) if selected_reports else False
    successful_source_names = {
        clean_text(row.get("name"))
        for row in selected_reports
        if norm_text(row.get("status")) == "ok" and clean_text(row.get("name"))
    }
    allow_mark_missing = bool(using_default_loaders and not seed_from_existing_output and run_is_healthy)
    eligible_missing_sources = (
        successful_source_names if using_default_loaders and not seed_from_existing_output else set()
    )
    lifecycle_finished_at = now_iso()
    deduped_rows, lifecycle_rows, lifecycle_counts_map = apply_job_lifecycle_state(
        deduped_rows=deduped_rows,
        lifecycle_rows=lifecycle_rows,
        finished_at=lifecycle_finished_at,
        allow_mark_missing=allow_mark_missing,
        eligible_missing_sources=eligible_missing_sources,
    )

    dedup_stats["outputCount"] = len(deduped_rows)
    deduped_payload_rows = _rows_to_legacy_dicts(deduped_rows)
    final_output_by_source: Counter[str] = Counter(
        clean_text(row.get("source")) for row in deduped_payload_rows if clean_text(row.get("source"))
    )
    for report in source_reports:
        if not isinstance(report, dict):
            continue
        loss = report.get("loss")
        if not isinstance(loss, dict):
            continue
        source_name = clean_text(report.get("name"))
        canonical_kept = int(loss.get("canonicalKept") or report.get("keptCount") or 0)
        final_output = int(final_output_by_source.get(source_name, 0))
        loss["finalOutput"] = max(0, final_output)
        loss["dedupMerged"] = max(0, canonical_kept - final_output)

    wrote_json = False
    wrote_csv = False
    wrote_light_json = False
    if deduped_payload_rows:
        wrote_json = write_text_if_changed(json_path, serialize_rows_for_json(deduped_payload_rows, OUTPUT_FIELDS))
        wrote_csv = write_text_if_changed(csv_path, serialize_rows_for_csv(deduped_payload_rows, OUTPUT_FIELDS))
        wrote_light_json = write_text_if_changed(
            light_json_path,
            serialize_rows_for_json(deduped_payload_rows, LIGHTWEIGHT_OUTPUT_FIELDS),
        )

    json_bytes = json_path.stat().st_size if json_path.exists() else 0
    csv_bytes = csv_path.stat().st_size if csv_path.exists() else 0
    light_json_bytes = light_json_path.stat().st_size if light_json_path.exists() else 0
    browser_fallback_queue_rows = build_browser_fallback_queue(source_reports, generated_at=lifecycle_finished_at)
    write_text_if_changed(browser_fallback_queue_path, json.dumps(browser_fallback_queue_rows, indent=2, ensure_ascii=False))

    report_payload = normalize_fetch_report_payload({
        "schemaVersion": SCHEMA_VERSION,
        "startedAt": started_at,
        "finishedAt": lifecycle_finished_at,
        "runtime": runtime_payload,
        "summary": build_pipeline_summary(
            dedup_stats,
            deduped_rows,
            source_reports,
            len(canonical_rows),
            preserved_previous,
            len([row for row in STUDIO_SOURCE_REGISTRY if bool(row.get("enabledByDefault", True))]),
            len(load_registry_from_file(pending_registry_path, [])),
            read_approved_since_last_run(approval_state_path),
            json_bytes=json_bytes,
            csv_bytes=csv_bytes,
            light_json_bytes=light_json_bytes,
            lifecycle_counts_map=lifecycle_counts_map,
        ),
        "sources": source_reports,
        "outputs": {
            "json": str(json_path),
            "csv": str(csv_path),
            "lightJson": str(light_json_path),
            "report": str(report_path),
            "lifecycleState": str(lifecycle_state_path),
            "browserFallbackQueue": str(browser_fallback_queue_path),
            "changed": {"json": wrote_json, "csv": wrote_csv, "lightJson": wrote_light_json},
        },
    })
    report_payload["runtime"]["slowestSources"] = [
        {
            "name": clean_text(row.get("name")),
            "adapter": clean_text(row.get("adapter")),
            "durationMs": int(row.get("durationMs") or 0),
            "keptCount": int(row.get("keptCount") or 0),
            "detailPagesVisited": int(
                (((row.get("details") or [{}])[0] if isinstance(row.get("details"), list) and row.get("details") else {}).get("stats") or {}).get("detail_pages_visited") or 0
            ),
            "detailYieldPct": int(
                (((row.get("details") or [{}])[0] if isinstance(row.get("details"), list) and row.get("details") else {}).get("stats") or {}).get("detail_yield_percent") or 0
            ),
        }
        for row in sorted(
            [row for row in report_payload.get("sources", []) if isinstance(row, dict)],
            key=lambda item: int(item.get("durationMs") or 0),
            reverse=True,
        )[:10]
    ]
    write_text_if_changed(report_path, json.dumps(report_payload, indent=2, ensure_ascii=False))
    finished_at = clean_text(report_payload.get("finishedAt")) or now_iso()
    write_task_state(finished_at=finished_at, force=True)
    write_success_cache(success_cache_path, source_reports)

    source_state_rows = update_source_state_rows(
        source_state_rows=source_state_rows,
        source_reports=source_reports,
        canonical_rows=deduped_payload_rows,
        finished_at=finished_at,
        circuit_breaker_failures=circuit_breaker_failures,
        circuit_breaker_cooldown_minutes=circuit_breaker_cooldown_minutes,
    )
    write_source_state(source_state_path, source_state_rows)
    write_job_lifecycle_state(lifecycle_state_path, lifecycle_rows)
    return report_payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch and merge game jobs into unified output feeds.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Directory to write output files.")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_S, help="Per-source request timeout in seconds.")
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES, help="Retry count per request.")
    parser.add_argument("--backoff", type=float, default=DEFAULT_BACKOFF_S, help="Base retry backoff in seconds.")
    parser.add_argument(
        "--no-preserve-previous-on-empty",
        action="store_true",
        help="Do not preserve previous output if current run yields no jobs.",
    )
    parser.add_argument("--quiet", action="store_true", help="Suppress per-source progress logs.")
    parser.add_argument(
        "--skip-successful-sources",
        action="store_true",
        help="Skip sources that were previously successful with non-zero kept jobs in the last report.",
    )
    parser.add_argument(
        "--only-sources",
        default="",
        help="Comma-separated source loader names to run (for targeted/incremental fetches).",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=6,
        help="Max concurrent source workers. Use >1 to run source loaders in parallel.",
    )
    parser.add_argument(
        "--max-per-domain",
        type=int,
        default=2,
        help="Max concurrent in-flight requests allowed per domain across workers.",
    )
    parser.add_argument(
        "--fetch-strategy",
        choices=("auto", "http", "browser"),
        default=DEFAULT_FETCH_STRATEGY,
        help="Fetch transport strategy. 'http' prefers async httpx, 'auto' falls back safely, 'browser' keeps HTTP mode in this runtime.",
    )
    parser.add_argument(
        "--adapter-http-concurrency",
        type=int,
        default=DEFAULT_ADAPTER_HTTP_CONCURRENCY,
        help="Connection pool size used by async HTTP fetch transport.",
    )
    parser.add_argument(
        "--google-sheets-redirect-concurrency",
        type=int,
        default=DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY,
        help="Max concurrent redirect resolutions for supported Google Sheets redirect links.",
    )
    parser.add_argument(
        "--static-detail-concurrency",
        type=int,
        default=DEFAULT_STATIC_DETAIL_CONCURRENCY,
        help="Max concurrent static detail-page fetches per source before per-domain limiting is applied.",
    )
    parser.add_argument(
        "--source-ttl-minutes",
        type=int,
        default=360,
        help="Freshness window for --skip-successful-sources. Recently successful sources are skipped until TTL expires.",
    )
    parser.add_argument(
        "--respect-source-cadence",
        action="store_true",
        help="Apply source-level hot/cold cadence skipping using source state history.",
    )
    parser.add_argument(
        "--hot-source-cadence-minutes",
        type=int,
        default=DEFAULT_HOT_SOURCE_CADENCE_MINUTES,
        help="Cadence for recently changed sources when --respect-source-cadence is enabled.",
    )
    parser.add_argument(
        "--cold-source-cadence-minutes",
        type=int,
        default=DEFAULT_COLD_SOURCE_CADENCE_MINUTES,
        help="Cadence for stable sources when --respect-source-cadence is enabled.",
    )
    parser.add_argument(
        "--circuit-breaker-failures",
        type=int,
        default=3,
        help="Consecutive failures required before a source is temporarily quarantined.",
    )
    parser.add_argument(
        "--circuit-breaker-cooldown-minutes",
        type=int,
        default=180,
        help="Minutes to quarantine a source after it trips the circuit breaker.",
    )
    parser.add_argument(
        "--ignore-circuit-breaker",
        action="store_true",
        help="Force execution of sources even if currently quarantined.",
    )
    parser.add_argument(
        "--social-enabled",
        action="store_true",
        help="Enable social sources (Reddit, X, Mastodon) in the fetch run.",
    )
    parser.add_argument(
        "--social-config-path",
        default=str(DEFAULT_SOCIAL_CONFIG_PATH),
        help="Path to social source config JSON file.",
    )
    parser.add_argument(
        "--social-lookback-minutes",
        type=int,
        default=DEFAULT_SOCIAL_LOOKBACK_MINUTES,
        help="Lookback window for social source polling.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    source_loaders: Optional[List[Tuple[str, SourceLoader]]] = None
    seed_from_existing_output = False
    selection_exclusions: List[Dict[str, Any]] = []
    social_config = load_social_config(
        config_path=Path(args.social_config_path),
        enabled=bool(args.social_enabled),
        lookback_minutes=int(args.social_lookback_minutes or DEFAULT_SOCIAL_LOOKBACK_MINUTES),
    )
    try:
        default_loaders = default_source_loaders(
            social_enabled=bool(args.social_enabled),
            social_config=social_config,
        )
    except TypeError:
        default_loaders = default_source_loaders()

    only_sources = [clean_text(part) for part in str(args.only_sources or "").split(",") if clean_text(part)]
    if only_sources:
        wanted = set(only_sources)
        source_loaders = [(name, loader) for name, loader in default_loaders if name in wanted]
        seed_from_existing_output = True
        for name, _loader in default_loaders:
            if name in wanted:
                continue
            selection_exclusions.append(_build_excluded_source_report(name, "only_sources_filter"))
        missing = [name for name in only_sources if name not in {item[0] for item in source_loaders}]
        if missing:
            print(f"[jobs_fetcher] WARN unknown --only-sources entries: {', '.join(missing)}", flush=True)

    if args.skip_successful_sources:
        selected = source_loaders if source_loaders is not None else list(default_loaders)
        source_state_path = Path(args.output_dir) / "jobs-source-state.json"
        state_rows = read_source_state(source_state_path)
        successful = {
            name
            for name, _ in selected
            if should_skip_source_by_ttl(name, state_rows, int(args.source_ttl_minutes or 0))
        }
        if not successful:
            previous_report = Path(args.output_dir) / "jobs-fetch-report.json"
            success_cache_path = Path(args.output_dir) / "jobs-success-cache.json"
            successful = read_success_cache(success_cache_path)
            if not successful:
                successful = read_previously_successful_sources(previous_report)
        if successful:
            selected = [(name, loader) for name, loader in selected if name not in successful]
            for source_name in sorted(successful):
                selection_exclusions.append(_build_excluded_source_report(source_name, "skip_successful_ttl"))
        source_loaders = selected
        seed_from_existing_output = True
        if not args.quiet:
            print(
                f"[jobs_fetcher] Incremental mode: skipping {len(successful)} previously successful sources; running {len(selected)}",
                flush=True,
            )

    forced_only_sources = bool(only_sources)
    deduped_selection_exclusions: List[Dict[str, Any]] = []
    seen_selection_exclusions = set()
    for row in selection_exclusions:
        name = clean_text(row.get("name"))
        reason = clean_text(row.get("exclusionReason") or row.get("error"))
        token = f"{name}|{reason}"
        if not name or token in seen_selection_exclusions:
            continue
        seen_selection_exclusions.add(token)
        deduped_selection_exclusions.append(row)
    report = run_pipeline(
        output_dir=Path(args.output_dir),
        timeout_s=args.timeout,
        retries=args.retries,
        backoff_s=args.backoff,
        preserve_previous_on_empty=not args.no_preserve_previous_on_empty,
        source_loaders=source_loaders,
        seed_from_existing_output=seed_from_existing_output,
        source_ttl_minutes=args.source_ttl_minutes,
        max_workers=args.max_workers,
        max_per_domain=args.max_per_domain,
        fetch_strategy=args.fetch_strategy,
        adapter_http_concurrency=args.adapter_http_concurrency,
        google_sheets_redirect_concurrency=args.google_sheets_redirect_concurrency,
        static_detail_concurrency=args.static_detail_concurrency,
        circuit_breaker_failures=args.circuit_breaker_failures,
        circuit_breaker_cooldown_minutes=args.circuit_breaker_cooldown_minutes,
        respect_source_cadence=bool(args.respect_source_cadence),
        hot_source_cadence_minutes=args.hot_source_cadence_minutes,
        cold_source_cadence_minutes=args.cold_source_cadence_minutes,
        ignore_circuit_breaker=bool(args.ignore_circuit_breaker or forced_only_sources),
        social_enabled=bool(args.social_enabled),
        social_config_path=Path(args.social_config_path),
        social_lookback_minutes=int(args.social_lookback_minutes or DEFAULT_SOCIAL_LOOKBACK_MINUTES),
        show_progress=not args.quiet,
        selection_exclusions=deduped_selection_exclusions,
    )
    summary = report.get("summary", {})
    output_count = int(summary.get("outputCount") or 0)
    failed_sources = int(summary.get("failedSources") or 0)
    print(
        f"Jobs fetch completed. Output jobs: {output_count}. "
        f"Failed sources: {failed_sources}. Report: {report['outputs']['report']}"
    )
    return 0 if output_count > 0 else 2

