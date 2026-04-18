"""Package-owned pipeline entrypoints."""

from __future__ import annotations

import argparse
import os
import sys
import time
from collections import Counter
from collections.abc import Callable
from pathlib import Path
from typing import Any

from src.contracts import SCHEMA_VERSION
from src.core.contracts import (
    validate_canonical_jobs_payload as _validate_canonical_jobs_payload,
)
from src.jobs import canonicalize as canonicalize_pkg
from src.jobs import contamination_audit as contamination_audit_pkg
from src.jobs import dedup as dedup_pkg
from src.jobs import pipeline_finalize as pipeline_finalize_pkg
from src.jobs import pipeline_timing as pipeline_timing_pkg
from src.jobs import reporting as reporting_pkg
from src.jobs import state as state_pkg
from src.jobs import transport as transport_pkg
from src.jobs.adapters import community as community_adapter
from src.jobs.adapters import static as static_adapter
from src.jobs.adapters.api import default_source_loaders as adapters_default_source_loaders
from src.jobs.common import health as _health_module
from src.jobs.common.config import SOURCE_DIAGNOSTICS
from src.jobs.common.contracts import normalize_task_state_payload
from src.jobs.interfaces import SourceLoader
from src.jobs.models import CanonicalJob
from src.jobs.pipeline_bootstrap import build_pipeline_paths
from src.jobs.pipeline_loader_selection import (
    apply_incremental_cache_exclusions,
    apply_source_cadence_exclusions,
    build_excluded_source_report,
    build_pipeline_runtime_payload,
    select_pipeline_loaders,
    sort_selected_loaders,
)
from src.jobs.pipeline_runtime import (
    build_fetch_task_progress_payload as _build_fetch_task_progress_payload,
)
from src.jobs.pipeline_runtime import (
    initialize_task_runtime,
    make_fetch_text_limited,
    make_task_state_writer,
)
from src.jobs.pipeline_runtime import (
    snapshot_task_rows as _snapshot_task_rows,
)
from src.jobs.pipeline_runtime import (
    write_progress_report as write_pipeline_progress_report,
)
from src.jobs.pipeline_stage_source_execution import (
    SourceExecutionStageConfig,
    run_source_execution_stage,
)
from src.jobs.registry import STUDIO_SOURCE_REGISTRY, registry_entries
from src.jobs.text_utils import clean_text, norm_text, sanitize_location_text
from src.jobs_fetcher_registry import SOURCE_REPORT_META
from src.pipeline_io import (
    read_existing_output as read_existing_output_from_file,
)
from src.pipeline_io import (
    serialize_rows_for_csv as _serialize_rows_for_csv,
)
from src.pipeline_io import (
    serialize_rows_for_json as _serialize_rows_for_json,
)
from src.pipeline_io import (
    write_atomic_if_changed as _write_atomic_if_changed,
)
from src.pipeline_io import (
    write_text_if_changed,
)
from src.shared.utils import now_iso

from .common import config as common_config
from .common import social as common_social
from .common import sources as common_sources

DEFAULT_OUTPUT_DIR = common_config.DEFAULT_OUTPUT_DIR
DEFAULT_TIMEOUT_S = common_config.DEFAULT_TIMEOUT_S
DEFAULT_RETRIES = common_config.DEFAULT_RETRIES
DEFAULT_BACKOFF_S = common_config.DEFAULT_BACKOFF_S
DEFAULT_FETCH_STRATEGY = common_config.DEFAULT_FETCH_STRATEGY
DEFAULT_ADAPTER_HTTP_CONCURRENCY = common_config.DEFAULT_ADAPTER_HTTP_CONCURRENCY
DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY = (
    community_adapter.DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY
)
DEFAULT_STATIC_DETAIL_CONCURRENCY = common_config.DEFAULT_STATIC_DETAIL_CONCURRENCY
DEFAULT_HOT_SOURCE_CADENCE_MINUTES = common_config.DEFAULT_HOT_SOURCE_CADENCE_MINUTES
DEFAULT_COLD_SOURCE_CADENCE_MINUTES = common_config.DEFAULT_COLD_SOURCE_CADENCE_MINUTES
DEFAULT_SOCIAL_CONFIG_PATH = common_config.DEFAULT_SOCIAL_CONFIG_PATH
DEFAULT_SOCIAL_LOOKBACK_MINUTES = common_config.DEFAULT_SOCIAL_LOOKBACK_MINUTES
DEFAULT_SOCIAL_MIN_CONFIDENCE = common_config.DEFAULT_SOCIAL_MIN_CONFIDENCE
DEFAULT_STATIC_DETAIL_HEURISTICS_PROFILE = common_config.DEFAULT_STATIC_DETAIL_HEURISTICS_PROFILE
DEFAULT_SCRAPY_VALIDATION_STRICT = common_config.DEFAULT_SCRAPY_VALIDATION_STRICT
DEFAULT_CANONICAL_STRICT_URL = common_config.DEFAULT_CANONICAL_STRICT_URL
OUTPUT_FIELDS = common_config.OUTPUT_FIELDS
LIGHTWEIGHT_OUTPUT_FIELDS = common_config.LIGHTWEIGHT_OUTPUT_FIELDS

load_social_config = common_social.load_social_config
default_fetch_text = transport_pkg.default_fetch_text
resolve_fetch_text_impl = transport_pkg.resolve_fetch_text_impl
build_redirect_resolver = transport_pkg.build_redirect_resolver
load_registry_from_file = common_sources.load_registry_from_file
read_approved_since_last_run = common_sources.read_approved_since_last_run

# Compatibility surface for split helper modules that still call through module attributes.
validate_canonical_jobs_payload = _validate_canonical_jobs_payload
health_module = _health_module
build_fetch_task_progress_payload = _build_fetch_task_progress_payload
snapshot_task_rows = _snapshot_task_rows
serialize_rows_for_csv = _serialize_rows_for_csv
serialize_rows_for_json = _serialize_rows_for_json
write_atomic_if_changed = _write_atomic_if_changed

canonicalize_job = canonicalize_pkg.canonicalize_job
CanonicalNormalizer = canonicalize_pkg.CanonicalNormalizer
reset_location_quality_audit = canonicalize_pkg.reset_location_quality_audit
reset_sector_quality_audit = canonicalize_pkg.reset_sector_quality_audit
snapshot_sector_quality_audit = canonicalize_pkg.snapshot_sector_quality_audit
build_public_text_quality_report = contamination_audit_pkg.build_public_text_quality_report
CanonicalDeduplicator = dedup_pkg.CanonicalDeduplicator
format_source_error = reporting_pkg.format_source_error
build_pipeline_summary = reporting_pkg.build_pipeline_summary
build_browser_fallback_queue = reporting_pkg.build_browser_fallback_queue
build_parser_regression_queue = reporting_pkg.build_parser_regression_queue
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
normalize_source_state_payload = state_pkg.normalize_source_state_payload
should_skip_source_by_ttl = state_pkg.should_skip_source_by_ttl
should_skip_source_by_cadence = state_pkg.should_skip_source_by_cadence
get_incremental_cache_decision = state_pkg.get_incremental_cache_decision
apply_circuit_breaker_exclusions = state_pkg.apply_circuit_breaker_exclusions
append_excluded_default_sources = state_pkg.append_excluded_default_sources
update_source_state_rows = state_pkg.update_source_state_rows


def _apply_final_location_quality_guardrail(rows: list[dict[str, Any]]) -> dict[str, Any]:
    field_counts: Counter[str] = Counter()
    reason_counts: Counter[str] = Counter()
    examples: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        city_value, city_reason = sanitize_location_text(row.get("city"), field_name="city")
        if city_reason:
            if len(examples) < 20:
                examples.append(
                    {
                        "company": clean_text(row.get("company")),
                        "title": clean_text(row.get("title")),
                        "source": clean_text(row.get("source")),
                        "jobLink": clean_text(row.get("jobLink")),
                        "field": "city",
                        "reason": city_reason,
                        "value": clean_text(row.get("city")),
                    }
                )
            row["city"] = city_value
            field_counts["city"] += 1
            reason_counts[city_reason] += 1
        country_value, country_reason = sanitize_location_text(
            row.get("country"), field_name="country"
        )
        if country_reason:
            if len(examples) < 20:
                examples.append(
                    {
                        "company": clean_text(row.get("company")),
                        "title": clean_text(row.get("title")),
                        "source": clean_text(row.get("source")),
                        "jobLink": clean_text(row.get("jobLink")),
                        "field": "country",
                        "reason": country_reason,
                        "value": clean_text(row.get("country")),
                    }
                )
            row["country"] = country_value
            field_counts["country"] += 1
            reason_counts[country_reason] += 1
    return {
        "totalRows": len(rows),
        "invalidLocationFieldCount": int(sum(field_counts.values())),
        "fieldCounts": dict(field_counts),
        "reasonCounts": dict(reason_counts),
        "examples": examples,
    }


def _canonicalize_existing_output_row(
    row: dict[str, Any], *, source: str, fetched_at: str
) -> dict[str, Any] | None:
    normalized = canonicalize_job(row, source=source, fetched_at=fetched_at)
    if not normalized:
        return None
    payload = normalized.to_dict()
    if clean_text(row.get("dedupKey")):
        payload["dedupKey"] = clean_text(row.get("dedupKey"))
    return payload


def _percentile_ms(values: list[int], percentile: float) -> int:
    return pipeline_timing_pkg.percentile_ms(values, percentile)


def build_runtime_timing_summary(
    source_reports: list[dict[str, Any]], *, wall_clock_duration_ms: int = 0
) -> dict[str, Any]:
    return pipeline_timing_pkg.build_runtime_timing_summary(
        source_reports,
        wall_clock_duration_ms=wall_clock_duration_ms,
        clean_text_fn=clean_text,
        norm_text_fn=norm_text,
        percentile_ms_fn=_percentile_ms,
    )


def default_source_loaders(
    *,
    social_enabled: bool = False,
    social_config: dict[str, Any] | None = None,
) -> list[tuple[str, SourceLoader]]:
    facade = sys.modules.get("src.jobs_fetcher")
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
        # Prefer adapter public surface to avoid hopping between `jobs.common/*`
        # and adapter internals while keeping the `jobs_fetcher` facade patch surface.
        return adapters_default_source_loaders(
            social_enabled=social_enabled,
            social_config=social_config,
        )
    except TypeError:
        return adapters_default_source_loaders()


def run_pipeline(
    *,
    output_dir: Path,
    run_id: str = "",
    started_at_override: str = "",
    timeout_s: int = DEFAULT_TIMEOUT_S,
    retries: int = DEFAULT_RETRIES,
    backoff_s: float = DEFAULT_BACKOFF_S,
    preserve_previous_on_empty: bool = True,
    fetch_text: Callable[[str, int], str] = default_fetch_text,
    source_loaders: list[tuple[str, SourceLoader]] | None = None,
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
    browser_fallback_cooldown_minutes: int = 30,
    circuit_breaker_zero_kept: int = 3,
    ignore_circuit_breaker: bool = False,
    social_enabled: bool = False,
    social_config_path: Path | None = None,
    social_lookback_minutes: int = DEFAULT_SOCIAL_LOOKBACK_MINUTES,
    static_detail_concurrency: int = DEFAULT_STATIC_DETAIL_CONCURRENCY,
    show_progress: bool = True,
    selection_exclusions: list[dict[str, Any]] | None = None,
    force_refresh_all: bool = False,
) -> dict[str, Any]:
    output_dir = Path(output_dir)
    run_started_mono = time.perf_counter()
    paths = build_pipeline_paths(output_dir)
    SOURCE_DIAGNOSTICS.clear()
    reset_location_quality_audit()
    reset_sector_quality_audit()

    started_at = clean_text(started_at_override) or now_iso()
    source_reports: list[dict[str, Any]] = []
    if isinstance(selection_exclusions, list):
        source_reports.extend([row for row in selection_exclusions if isinstance(row, dict)])
    canonical_rows: list[CanonicalJob] = []
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
    source_state_rows = read_source_state(paths.source_state_path)
    google_sheets_redirect_cache: dict[str, str] = {}
    for _state in source_state_rows.values():
        if not isinstance(_state, dict):
            continue
        raw_cache = _state.get("googleSheetsRedirectCache")
        if not isinstance(raw_cache, dict) or not raw_cache:
            continue
        for _raw_url, _resolved_url in raw_cache.items():
            _url = clean_text(_raw_url)
            _resolved = clean_text(_resolved_url)
            if _url and _resolved:
                google_sheets_redirect_cache[_url] = _resolved
    seed_redirect_cache = getattr(redirect_resolver, "seed_cache", None)
    if callable(seed_redirect_cache) and google_sheets_redirect_cache:
        seed_redirect_cache(google_sheets_redirect_cache)
    lifecycle_rows = read_job_lifecycle_state(paths.lifecycle_state_path)
    incremental_cache_enabled = bool(not force_refresh_all and paths.json_path.exists())
    effective_seed_from_existing_output = bool(
        seed_from_existing_output or incremental_cache_enabled
    )
    if effective_seed_from_existing_output:
        seeded_rows = read_existing_output_from_file(
            paths.json_path,
            started_at,
            canonicalize_job=_canonicalize_existing_output_row,
            clean_text=clean_text,
        )
        canonical_rows.extend(CanonicalJob.from_mapping(row) for row in seeded_rows)
    runtime_payload: dict[str, Any] = {}

    effective_social_config_path = (
        Path(social_config_path)
        if social_config_path
        else (output_dir / "social-sources-config.json")
    )
    social_config = load_social_config(
        config_path=effective_social_config_path,
        enabled=bool(social_enabled),
        lookback_minutes=social_lookback_minutes,
    )

    selected_loaders, using_default_loaders = select_pipeline_loaders(
        source_loaders=source_loaders,
        social_enabled=bool(social_enabled),
        social_config=social_config,
        default_source_loaders=default_source_loaders,
    )
    selected_loaders = sort_selected_loaders(
        selected_loaders,
        source_report_meta=SOURCE_REPORT_META,
        source_state_rows=source_state_rows,
    )
    runtime_payload = build_pipeline_runtime_payload(
        selected_loaders=selected_loaders,
        max_workers=max_workers,
        max_per_domain=max_per_domain,
        fetch_strategy=fetch_strategy,
        fetch_client=fetch_client,
        adapter_http_concurrency=adapter_http_concurrency,
        static_detail_concurrency=static_detail_concurrency,
        google_sheets_redirect_concurrency=google_sheets_redirect_concurrency,
        seed_from_existing_output=effective_seed_from_existing_output,
        incremental_cache_enabled=incremental_cache_enabled,
        force_refresh_all=force_refresh_all,
        source_ttl_minutes=source_ttl_minutes,
        respect_source_cadence=respect_source_cadence,
        hot_source_cadence_minutes=hot_source_cadence_minutes,
        cold_source_cadence_minutes=cold_source_cadence_minutes,
        circuit_breaker_failures=circuit_breaker_failures,
        circuit_breaker_cooldown_minutes=circuit_breaker_cooldown_minutes,
        browser_fallback_cooldown_minutes=browser_fallback_cooldown_minutes,
        ignore_circuit_breaker=ignore_circuit_breaker,
        social_enabled=bool(social_enabled),
        effective_social_config_path=str(effective_social_config_path),
        social_config=social_config,
        default_social_lookback_minutes=DEFAULT_SOCIAL_LOOKBACK_MINUTES,
        default_social_min_confidence=DEFAULT_SOCIAL_MIN_CONFIDENCE,
        default_fetch_strategy=DEFAULT_FETCH_STRATEGY,
        default_static_detail_heuristics_profile=DEFAULT_STATIC_DETAIL_HEURISTICS_PROFILE,
        default_scrapy_validation_strict=DEFAULT_SCRAPY_VALIDATION_STRICT,
        default_canonical_strict_url=DEFAULT_CANONICAL_STRICT_URL,
        normalize_runtime_payload=normalize_runtime_payload,
    )
    selected_loaders, incremental_skipped = apply_incremental_cache_exclusions(
        selected_loaders,
        incremental_cache_enabled=incremental_cache_enabled,
        force_refresh_all=force_refresh_all,
        source_state_rows=source_state_rows,
        get_incremental_cache_decision=get_incremental_cache_decision,
        build_excluded_source_report=lambda name, reason: build_excluded_source_report(
            name,
            reason,
            source_report_meta=SOURCE_REPORT_META,
        ),
        source_report_meta=SOURCE_REPORT_META,
    )
    if incremental_skipped:
        source_reports.extend(incremental_skipped)
        runtime_payload["selectedSourceCount"] = len(selected_loaders)
    selected_loaders, cadence_skipped = apply_source_cadence_exclusions(
        selected_loaders,
        respect_source_cadence=respect_source_cadence,
        source_state_rows=source_state_rows,
        hot_source_cadence_minutes=hot_source_cadence_minutes,
        cold_source_cadence_minutes=cold_source_cadence_minutes,
        should_skip_source_by_cadence=should_skip_source_by_cadence,
        build_excluded_source_report=lambda name, reason: build_excluded_source_report(
            name,
            reason,
            source_report_meta=SOURCE_REPORT_META,
        ),
    )
    if cadence_skipped:
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

    task_runtime = initialize_task_runtime(
        selected_loaders,
        run_id=run_id,
        started_at=started_at,
        show_progress=show_progress,
    )
    write_task_state = make_task_state_writer(
        runtime=task_runtime,
        run_id=run_id,
        started_at=started_at,
        report_path=str(paths.report_path),
        task_state_path=paths.task_state_path,
        normalize_task_state_payload=normalize_task_state_payload,
        write_text_if_changed=write_text_if_changed,
    )
    fetch_text_limited = make_fetch_text_limited(
        runtime=task_runtime,
        max_per_domain=max_per_domain,
        fetch_text_impl=fetch_text_impl,
        write_task_state=write_task_state,
    )
    progress_phase = {
        "key": "selecting_sources",
        "label": "Selecting sources",
    }
    write_progress_report = lambda: write_pipeline_progress_report(
        runtime=task_runtime,
        canonical_rows=canonical_rows,
        lifecycle_rows=lifecycle_rows,
        source_reports=source_reports,
        runtime_payload=runtime_payload,
        started_at=started_at,
        paths=paths,
        schema_version=SCHEMA_VERSION,
        studio_source_registry=STUDIO_SOURCE_REGISTRY,
        load_registry_from_file=load_registry_from_file,
        read_approved_since_last_run=read_approved_since_last_run,
        lifecycle_counts=lifecycle_counts,
        build_pipeline_summary=build_pipeline_summary,
        normalize_fetch_report_payload=normalize_fetch_report_payload,
        write_text_if_changed=write_text_if_changed,
        deduplicator_factory=CanonicalDeduplicator,
        phase_key=str(progress_phase["key"]),
        phase_label=str(progress_phase["label"]),
        run_id=run_id,
    )
    write_progress_report()

    stage_config = SourceExecutionStageConfig(
        max_workers=max_workers,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        static_detail_concurrency=static_detail_concurrency,
        google_sheets_redirect_concurrency=google_sheets_redirect_concurrency,
        started_at=started_at,
        show_progress=show_progress,
        force_refresh_all=force_refresh_all,
        browser_fallback_cooldown_minutes=browser_fallback_cooldown_minutes,
    )
    progress_phase["key"] = "executing_sources"
    progress_phase["label"] = "Executing sources"
    try:
        run_source_execution_stage(
            config=stage_config,
            selected_loaders=selected_loaders,
            fetch_text_limited=fetch_text_limited,
            source_state_rows=source_state_rows,
            redirect_resolver=redirect_resolver,
            task_runtime=task_runtime,
            task_rows=task_runtime.task_rows,
            task_lock=task_runtime.task_lock,
            thread_local=task_runtime.thread_local,
            write_task_state=write_task_state,
            write_progress_report=write_progress_report,
            canonical_rows=canonical_rows,
            source_reports=source_reports,
        )
    finally:
        if async_fetcher is not None:
            async_fetcher.close()
        close_redirect_resolver = getattr(redirect_resolver, "close", None)
        if callable(close_redirect_resolver):
            close_redirect_resolver()

    if using_default_loaders:
        append_excluded_default_sources(source_reports)

    # Attach provenance for static sources (e.g. game_studios_sheet) so fetch report can filter by sourceDirectory
    _static_name_to_row: dict[str, dict[str, Any]] = {}
    for _row in registry_entries("static"):
        _name = static_adapter.static_source_name_for_registry_row(_row)
        _static_name_to_row[_name] = _row
    for _report in source_reports:
        if not isinstance(_report, dict):
            continue
        _name = clean_text(_report.get("name"))
        _reg = _static_name_to_row.get(_name)
        if _reg is not None:
            if clean_text(_reg.get("sourceDirectory")):
                _report["sourceDirectory"] = clean_text(_reg.get("sourceDirectory"))
            if clean_text(_reg.get("sourceDirectoryUrl")):
                _report["sourceDirectoryUrl"] = clean_text(_reg.get("sourceDirectoryUrl"))
            if clean_text(_reg.get("listing_url")):
                _report["listingUrl"] = clean_text(_reg.get("listing_url"))

    progress_phase["key"] = "merging_results"
    progress_phase["label"] = "Merging results"
    write_progress_report()
    return pipeline_finalize_pkg.finalize_pipeline_run(
        sys.modules[__name__],
        paths=paths,
        source_reports=source_reports,
        canonical_rows=canonical_rows,
        using_default_loaders=using_default_loaders,
        selected_loaders=selected_loaders,
        effective_seed_from_existing_output=effective_seed_from_existing_output,
        preserve_previous_on_empty=preserve_previous_on_empty,
        source_state_rows=source_state_rows,
        lifecycle_rows=lifecycle_rows,
        runtime_payload=runtime_payload,
        redirect_resolver=redirect_resolver,
        task_runtime=task_runtime,
        progress_phase=progress_phase,
        write_progress_report=write_progress_report,
        write_task_state=write_task_state,
        started_at=started_at,
        run_started_mono=run_started_mono,
        run_id=run_id,
        circuit_breaker_failures=circuit_breaker_failures,
        circuit_breaker_cooldown_minutes=circuit_breaker_cooldown_minutes,
        circuit_breaker_zero_kept=circuit_breaker_zero_kept,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch and merge game jobs into unified output feeds."
    )
    parser.add_argument(
        "--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Directory to write output files."
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT_S,
        help="Per-source request timeout in seconds.",
    )
    parser.add_argument(
        "--retries", type=int, default=DEFAULT_RETRIES, help="Retry count per request."
    )
    parser.add_argument(
        "--backoff", type=float, default=DEFAULT_BACKOFF_S, help="Base retry backoff in seconds."
    )
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
        default=12,
        help="Max concurrent source workers. Use >1 to run source loaders in parallel.",
    )
    parser.add_argument(
        "--max-per-domain",
        type=int,
        default=3,
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
        "--force-refresh-all",
        action="store_true",
        help="Bypass incremental freshness skips and source revalidation so all sources run fully.",
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
        "--browser-fallback-cooldown-minutes",
        type=int,
        default=30,
        help="Minutes to disable browser fallback after an environment-level Playwright failure.",
    )
    parser.add_argument(
        "--circuit-breaker-zero-kept",
        type=int,
        default=3,
        help="Consecutive zero-kept runs required before a source is temporarily quarantined.",
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
    env_run_id = clean_text(os.environ.get("BALUFFO_FETCH_RUN_ID"))
    env_started_at = clean_text(os.environ.get("BALUFFO_FETCH_STARTED_AT"))
    source_loaders: list[tuple[str, SourceLoader]] | None = None
    seed_from_existing_output = False
    selection_exclusions: list[dict[str, Any]] = []
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

    only_sources = [
        clean_text(part) for part in str(args.only_sources or "").split(",") if clean_text(part)
    ]
    if only_sources:
        wanted = set(only_sources)
        source_loaders = [(name, loader) for name, loader in default_loaders if name in wanted]
        seed_from_existing_output = True
        for name, _loader in default_loaders:
            if name in wanted:
                continue
            selection_exclusions.append(
                build_excluded_source_report(
                    name, "only_sources_filter", source_report_meta=SOURCE_REPORT_META
                )
            )
        missing = [
            name for name in only_sources if name not in {item[0] for item in source_loaders}
        ]
        if missing:
            print(
                f"[jobs_fetcher] WARN unknown --only-sources entries: {', '.join(missing)}",
                flush=True,
            )

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
                selection_exclusions.append(
                    build_excluded_source_report(
                        source_name, "skip_successful_ttl", source_report_meta=SOURCE_REPORT_META
                    )
                )
        source_loaders = selected
        seed_from_existing_output = True
        if not args.quiet:
            print(
                f"[jobs_fetcher] Incremental mode: skipping {len(successful)} previously successful sources; running {len(selected)}",
                flush=True,
            )

    forced_only_sources = bool(only_sources)
    deduped_selection_exclusions: list[dict[str, Any]] = []
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
        run_id=env_run_id,
        started_at_override=env_started_at,
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
        browser_fallback_cooldown_minutes=args.browser_fallback_cooldown_minutes,
        circuit_breaker_zero_kept=args.circuit_breaker_zero_kept,
        respect_source_cadence=bool(args.respect_source_cadence),
        hot_source_cadence_minutes=args.hot_source_cadence_minutes,
        cold_source_cadence_minutes=args.cold_source_cadence_minutes,
        ignore_circuit_breaker=bool(args.ignore_circuit_breaker or forced_only_sources),
        social_enabled=bool(args.social_enabled),
        social_config_path=Path(args.social_config_path),
        social_lookback_minutes=int(
            args.social_lookback_minutes or DEFAULT_SOCIAL_LOOKBACK_MINUTES
        ),
        show_progress=not args.quiet,
        selection_exclusions=deduped_selection_exclusions,
        force_refresh_all=bool(args.force_refresh_all),
    )
    summary = report.get("summary", {})
    output_count = int(summary.get("outputCount") or 0)
    failed_sources = int(summary.get("failedSources") or 0)
    runtime = report.get("runtime") if isinstance(report.get("runtime"), dict) else {}
    timing_summary = (
        runtime.get("timingSummary") if isinstance(runtime.get("timingSummary"), dict) else {}
    )
    stage_top = (
        timing_summary.get("stageTop") if isinstance(timing_summary.get("stageTop"), list) else []
    )
    slowest_sources = (
        runtime.get("slowestSources") if isinstance(runtime.get("slowestSources"), list) else []
    )
    print(
        f"Jobs fetch completed. Output jobs: {output_count}. "
        f"Failed sources: {failed_sources}. Report: {report['outputs']['report']}"
    )
    if stage_top:
        top_stage_summary = " | ".join(
            f"{clean_text(item.get('stage'))}={int(item.get('durationMs') or 0)}ms"
            for item in stage_top[:3]
            if isinstance(item, dict)
        )
        if top_stage_summary:
            print(f"[jobs_fetcher] TIMING top-stages {top_stage_summary}", flush=True)
    if slowest_sources:
        slowest_summary = " | ".join(
            f"{clean_text(item.get('name'))}={int(item.get('durationMs') or 0)}ms"
            for item in slowest_sources[:3]
            if isinstance(item, dict)
        )
        if slowest_summary:
            print(f"[jobs_fetcher] TIMING slowest-sources {slowest_summary}", flush=True)
    return 0 if output_count > 0 else 2


if __name__ == "__main__":
    sys.exit(main())
