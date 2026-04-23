"""Pipeline runtime setup helpers for the package-owned pipeline entrypoint."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.contracts import SCHEMA_VERSION
from src.jobs.canonicalize import (
    canonicalize_job,
    reset_location_quality_audit,
    reset_sector_quality_audit,
)
from src.jobs.common import config as common_config
from src.jobs.common import social as common_social
from src.jobs.common import sources as common_sources
from src.jobs.common.config import SOURCE_DIAGNOSTICS
from src.jobs.common.contracts import normalize_task_state_payload
from src.jobs.interfaces import SourceLoader
from src.jobs.models import CanonicalJob
from src.jobs.pipeline_bootstrap import PipelinePaths, build_pipeline_paths
from src.jobs.pipeline_loader_selection import (
    apply_incremental_cache_exclusions,
    apply_source_cadence_exclusions,
    build_excluded_source_report,
    build_pipeline_runtime_payload,
    select_pipeline_loaders,
    sort_selected_loaders,
)
from src.jobs.pipeline_runtime import (
    PipelineTaskRuntime,
    initialize_task_runtime,
    make_fetch_text_limited,
    make_progress_report_dispatcher,
    make_task_state_writer,
)
from src.jobs.pipeline_runtime import (
    write_progress_report as write_pipeline_progress_report,
)
from src.jobs.pipeline_stage_source_execution import (
    SourceExecutionStageConfig,
    resolve_fetch_browser_fallback_helper,
)
from src.jobs.registry import STUDIO_SOURCE_REGISTRY
from src.jobs.reporting import (
    build_pipeline_summary,
    normalize_fetch_report_payload,
    normalize_runtime_payload,
)
from src.jobs.state_incremental import (
    get_incremental_cache_decision,
    should_skip_source_by_cadence,
)
from src.jobs.state_lifecycle import lifecycle_counts, read_job_lifecycle_state
from src.jobs.state_source_state import (
    apply_circuit_breaker_exclusions,
    read_source_state,
)
from src.jobs.text_utils import clean_text
from src.jobs.transport import (
    async_fetch_text_httpx,
    default_fetch_text,
    resolve_fetch_text_impl,
)
from src.jobs.transport import (
    build_redirect_resolver as transport_build_redirect_resolver,
)
from src.jobs_fetcher_registry import SOURCE_REPORT_META
from src.pipeline_io import read_existing_output, write_hot_text_if_changed
from src.shared.utils import env_flag, now_iso


@dataclass
class PipelineRunSetup:
    paths: PipelinePaths
    started_at: str
    source_reports: list[dict[str, Any]]
    canonical_rows: list[CanonicalJob]
    selected_loaders: list[tuple[str, SourceLoader]]
    using_default_loaders: bool
    source_state_rows: dict[str, dict[str, Any]]
    lifecycle_rows: dict[str, dict[str, Any]]
    runtime_payload: dict[str, Any]
    async_fetcher: Any
    redirect_resolver: Any
    static_listing_async_fetch: Callable[[Any, Any, str, int], Any] | None
    task_runtime: PipelineTaskRuntime
    write_task_state: Callable[..., None]
    fetch_text_limited: Callable[[str, int], str]
    fetch_text_static_limited: Callable[[str, int], str]
    progress_phase: dict[str, str]
    write_progress_report: Callable[..., None]
    stop_progress_reporter: Callable[[], None]
    stage_config: SourceExecutionStageConfig
    effective_seed_from_existing_output: bool


def canonicalize_existing_output_row(
    row: dict[str, Any], *, source: str, fetched_at: str
) -> dict[str, Any] | None:
    normalized = canonicalize_job(row, source=source, fetched_at=fetched_at)
    if not normalized:
        return None
    payload = normalized.to_dict()
    if clean_text(row.get("dedupKey")):
        payload["dedupKey"] = clean_text(row.get("dedupKey"))
    return payload


def _seed_redirect_cache_from_state(
    *,
    redirect_resolver: Any,
    source_state_rows: dict[str, dict[str, Any]],
) -> None:
    google_sheets_redirect_cache: dict[str, str] = {}
    for state_row in source_state_rows.values():
        if not isinstance(state_row, dict):
            continue
        raw_cache = state_row.get("googleSheetsRedirectCache")
        if not isinstance(raw_cache, dict) or not raw_cache:
            continue
        for raw_url, resolved_url in raw_cache.items():
            url = clean_text(raw_url)
            resolved = clean_text(resolved_url)
            if url and resolved:
                google_sheets_redirect_cache[url] = resolved
    seed_redirect_cache = getattr(redirect_resolver, "seed_cache", None)
    if callable(seed_redirect_cache) and google_sheets_redirect_cache:
        seed_redirect_cache(google_sheets_redirect_cache)


def prepare_pipeline_run(
    *,
    output_dir: Path,
    run_id: str = "",
    started_at_override: str = "",
    timeout_s: int = common_config.DEFAULT_TIMEOUT_S,
    retries: int = common_config.DEFAULT_RETRIES,
    backoff_s: float = common_config.DEFAULT_BACKOFF_S,
    fetch_text: Callable[[str, int], str] = default_fetch_text,
    source_loaders: list[tuple[str, SourceLoader]] | None = None,
    seed_from_existing_output: bool = False,
    source_ttl_minutes: int = 0,
    max_workers: int = 1,
    max_per_domain: int = 2,
    fetch_strategy: str = common_config.DEFAULT_FETCH_STRATEGY,
    adapter_http_concurrency: int = common_config.DEFAULT_ADAPTER_HTTP_CONCURRENCY,
    google_sheets_redirect_concurrency: int = 1,
    respect_source_cadence: bool = False,
    hot_source_cadence_minutes: int = common_config.DEFAULT_HOT_SOURCE_CADENCE_MINUTES,
    cold_source_cadence_minutes: int = common_config.DEFAULT_COLD_SOURCE_CADENCE_MINUTES,
    circuit_breaker_failures: int = 3,
    circuit_breaker_cooldown_minutes: int = 180,
    browser_fallback_cooldown_minutes: int = 30,
    ignore_circuit_breaker: bool = False,
    social_enabled: bool = False,
    social_config_path: Path | None = None,
    social_lookback_minutes: int = common_config.DEFAULT_SOCIAL_LOOKBACK_MINUTES,
    static_detail_concurrency: int = common_config.DEFAULT_STATIC_DETAIL_CONCURRENCY,
    show_progress: bool = True,
    selection_exclusions: list[dict[str, Any]] | None = None,
    force_refresh_all: bool = False,
    default_source_loaders: Callable[..., list[tuple[str, SourceLoader]]] | None = None,
    build_redirect_resolver_fn: Callable[..., Any] | None = None,
) -> PipelineRunSetup:
    output_dir = Path(output_dir)
    if default_source_loaders is None:
        raise ValueError("default_source_loaders is required")
    if build_redirect_resolver_fn is None:
        build_redirect_resolver_fn = transport_build_redirect_resolver
    paths = build_pipeline_paths(output_dir)
    SOURCE_DIAGNOSTICS.clear()
    reset_location_quality_audit()
    reset_sector_quality_audit()

    started_at = clean_text(started_at_override) or now_iso()
    source_reports = [row for row in (selection_exclusions or []) if isinstance(row, dict)]
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
    static_listing_async_fetch = (
        (lambda client, _job, url, timeout_s: async_fetch_text_httpx(client, url, timeout_s))
        if fetch_client == "httpx_async"
        else None
    )
    redirect_resolver = build_redirect_resolver_fn(
        timeout_s=timeout_s,
        max_connections=google_sheets_redirect_concurrency,
    )
    source_state_rows = read_source_state(paths.source_state_path)
    _seed_redirect_cache_from_state(
        redirect_resolver=redirect_resolver,
        source_state_rows=source_state_rows,
    )
    lifecycle_rows = read_job_lifecycle_state(paths.lifecycle_state_path)

    seed_existing_output_override = env_flag("BALUFFO_FETCH_SEED_EXISTING_OUTPUT", False)
    incremental_cache_enabled = bool(not force_refresh_all and paths.json_path.exists())
    effective_seed_from_existing_output = bool(
        seed_from_existing_output or incremental_cache_enabled or seed_existing_output_override
    )
    if effective_seed_from_existing_output:
        seeded_rows = read_existing_output(
            paths.json_path,
            started_at,
            canonicalize_job=canonicalize_existing_output_row,
            clean_text=clean_text,
        )
        canonical_rows.extend(CanonicalJob.from_mapping(row) for row in seeded_rows)

    effective_social_config_path = (
        Path(social_config_path)
        if social_config_path
        else (output_dir / "social-sources-config.json")
    )
    social_config = common_social.load_social_config(
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

    browser_fallback_enabled = resolve_fetch_browser_fallback_helper() is not None
    browser_fallback_cap = max_workers if browser_fallback_enabled else 0
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
        browser_fallback_enabled=browser_fallback_enabled,
        browser_fallback_cap=browser_fallback_cap,
        ignore_circuit_breaker=ignore_circuit_breaker,
        social_enabled=bool(social_enabled),
        effective_social_config_path=str(effective_social_config_path),
        social_config=social_config,
        default_social_lookback_minutes=common_config.DEFAULT_SOCIAL_LOOKBACK_MINUTES,
        default_social_min_confidence=common_config.DEFAULT_SOCIAL_MIN_CONFIDENCE,
        default_fetch_strategy=common_config.DEFAULT_FETCH_STRATEGY,
        default_static_detail_heuristics_profile=(
            common_config.DEFAULT_STATIC_DETAIL_HEURISTICS_PROFILE
        ),
        default_scrapy_validation_strict=common_config.DEFAULT_SCRAPY_VALIDATION_STRICT,
        default_canonical_strict_url=common_config.DEFAULT_CANONICAL_STRICT_URL,
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
    if canonical_rows:
        task_runtime.current_output_count = len(canonical_rows)
    write_task_state = make_task_state_writer(
        runtime=task_runtime,
        run_id=run_id,
        started_at=started_at,
        report_path=str(paths.report_path),
        task_state_path=paths.task_state_path,
        normalize_task_state_payload=normalize_task_state_payload,
        write_text_if_changed=write_hot_text_if_changed,
    )
    fetch_text_limited = make_fetch_text_limited(
        runtime=task_runtime,
        max_per_domain=max_per_domain,
        fetch_text_impl=fetch_text_impl,
        write_task_state=write_task_state,
    )
    fetch_text_static_limited = make_fetch_text_limited(
        runtime=task_runtime,
        max_per_domain=common_config.DEFAULT_STATIC_FETCH_MAX_PER_DOMAIN,
        fetch_text_impl=fetch_text_impl,
        write_task_state=write_task_state,
        gate_namespace="static",
        wait_reason_label="domain_gate",
        collect_wait_stats=True,
    )
    progress_phase = {"key": "selecting_sources", "label": "Selecting sources"}

    def write_progress_report_sync(force: bool = False) -> None:
        write_pipeline_progress_report(
            runtime=task_runtime,
            canonical_rows=canonical_rows,
            lifecycle_rows=lifecycle_rows,
            source_reports=source_reports,
            runtime_payload=runtime_payload,
            started_at=started_at,
            paths=paths,
            schema_version=SCHEMA_VERSION,
            studio_source_registry=STUDIO_SOURCE_REGISTRY,
            load_registry_from_file=common_sources.load_registry_from_file,
            read_approved_since_last_run=common_sources.read_approved_since_last_run,
            lifecycle_counts=lifecycle_counts,
            build_pipeline_summary=build_pipeline_summary,
            normalize_fetch_report_payload=normalize_fetch_report_payload,
            write_text_if_changed=write_hot_text_if_changed,
            phase_key=str(progress_phase["key"]),
            phase_label=str(progress_phase["label"]),
            run_id=run_id,
            force=bool(force),
        )

    write_progress_report, stop_progress_reporter = make_progress_report_dispatcher(
        runtime=task_runtime,
        write_progress_report=write_progress_report_sync,
    )
    write_progress_report(force=True)

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
    return PipelineRunSetup(
        paths=paths,
        started_at=started_at,
        source_reports=source_reports,
        canonical_rows=canonical_rows,
        selected_loaders=selected_loaders,
        using_default_loaders=using_default_loaders,
        source_state_rows=source_state_rows,
        lifecycle_rows=lifecycle_rows,
        runtime_payload=runtime_payload,
        async_fetcher=async_fetcher,
        redirect_resolver=redirect_resolver,
        static_listing_async_fetch=static_listing_async_fetch,
        task_runtime=task_runtime,
        write_task_state=write_task_state,
        fetch_text_limited=fetch_text_limited,
        fetch_text_static_limited=fetch_text_static_limited,
        progress_phase=progress_phase,
        write_progress_report=write_progress_report,
        stop_progress_reporter=stop_progress_reporter,
        stage_config=stage_config,
        effective_seed_from_existing_output=effective_seed_from_existing_output,
    )
