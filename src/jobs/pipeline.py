"""Package-owned pipeline entrypoints."""

from __future__ import annotations

import argparse
import os
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

from src.jobs.adapters import community as community_adapter
from src.jobs.adapters.api import default_source_loaders as adapters_default_source_loaders
from src.jobs.common import config as common_config
from src.jobs.interfaces import SourceLoader
from src.jobs.pipeline_loader_selection import build_excluded_source_report
from src.jobs.state_incremental import should_skip_source_by_ttl
from src.jobs.state_source_state import (
    read_previously_successful_sources,
    read_source_state,
    read_success_cache,
)
from src.jobs.text_utils import clean_text
from src.jobs.transport import build_redirect_resolver as transport_build_redirect_resolver
from src.jobs.transport import default_fetch_text
from src.jobs_fetcher_registry import SOURCE_REPORT_META

from . import pipeline_execution_flow as pipeline_execution_flow_mod
from . import pipeline_finalize as pipeline_finalize_pkg
from . import pipeline_run_setup as pipeline_run_setup_mod
from .common import social as common_social

DEFAULT_OUTPUT_DIR = common_config.DEFAULT_OUTPUT_DIR
DEFAULT_TIMEOUT_S = common_config.DEFAULT_TIMEOUT_S
DEFAULT_RETRIES = common_config.DEFAULT_RETRIES
DEFAULT_BACKOFF_S = common_config.DEFAULT_BACKOFF_S
DEFAULT_FETCH_STRATEGY = common_config.DEFAULT_FETCH_STRATEGY
DEFAULT_FETCH_MAX_WORKERS = common_config.DEFAULT_FETCH_MAX_WORKERS
DEFAULT_FETCH_MAX_PER_DOMAIN = common_config.DEFAULT_FETCH_MAX_PER_DOMAIN
DEFAULT_ADAPTER_HTTP_CONCURRENCY = common_config.DEFAULT_ADAPTER_HTTP_CONCURRENCY
DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY = (
    community_adapter.DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY
)
DEFAULT_STATIC_DETAIL_CONCURRENCY = common_config.DEFAULT_STATIC_DETAIL_CONCURRENCY
DEFAULT_HOT_SOURCE_CADENCE_MINUTES = common_config.DEFAULT_HOT_SOURCE_CADENCE_MINUTES
DEFAULT_COLD_SOURCE_CADENCE_MINUTES = common_config.DEFAULT_COLD_SOURCE_CADENCE_MINUTES
DEFAULT_SOCIAL_CONFIG_PATH = common_config.DEFAULT_SOCIAL_CONFIG_PATH
DEFAULT_SOCIAL_LOOKBACK_MINUTES = common_config.DEFAULT_SOCIAL_LOOKBACK_MINUTES

load_social_config = common_social.load_social_config


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _pipeline_redirect_resolver_builder():
    return getattr(
        sys.modules[__name__],
        "build_redirect_resolver",
        transport_build_redirect_resolver,
    )


def default_source_loaders(
    *,
    social_enabled: bool = False,
    social_config: dict[str, Any] | None = None,
) -> list[tuple[str, SourceLoader]]:
    facade = sys.modules.get("src.jobs_fetcher")
    facade_loader = getattr(facade, "default_source_loaders", None) if facade is not None else None
    if callable(facade_loader) and facade_loader is not default_source_loaders:
        typed_facade_loader = cast(
            Callable[..., list[tuple[str, SourceLoader]]],
            facade_loader,
        )
        try:
            return typed_facade_loader(
                social_enabled=social_enabled,
                social_config=social_config,
            )
        except TypeError:
            return typed_facade_loader()
    try:
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
    fetch_text=default_fetch_text,
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
    run_started_mono = time.perf_counter()
    setup: pipeline_run_setup_mod.PipelineRunSetup | None = None
    try:
        setup = pipeline_run_setup_mod.prepare_pipeline_run(
            output_dir=Path(output_dir),
            run_id=run_id,
            started_at_override=started_at_override,
            timeout_s=timeout_s,
            retries=retries,
            backoff_s=backoff_s,
            fetch_text=fetch_text,
            source_loaders=source_loaders,
            seed_from_existing_output=seed_from_existing_output,
            source_ttl_minutes=source_ttl_minutes,
            max_workers=max_workers,
            max_per_domain=max_per_domain,
            fetch_strategy=fetch_strategy,
            adapter_http_concurrency=adapter_http_concurrency,
            google_sheets_redirect_concurrency=google_sheets_redirect_concurrency,
            respect_source_cadence=respect_source_cadence,
            hot_source_cadence_minutes=hot_source_cadence_minutes,
            cold_source_cadence_minutes=cold_source_cadence_minutes,
            circuit_breaker_failures=circuit_breaker_failures,
            circuit_breaker_cooldown_minutes=circuit_breaker_cooldown_minutes,
            browser_fallback_cooldown_minutes=browser_fallback_cooldown_minutes,
            ignore_circuit_breaker=ignore_circuit_breaker,
            social_enabled=social_enabled,
            social_config_path=social_config_path,
            social_lookback_minutes=social_lookback_minutes,
            static_detail_concurrency=static_detail_concurrency,
            show_progress=show_progress,
            selection_exclusions=selection_exclusions,
            force_refresh_all=force_refresh_all,
            default_source_loaders=default_source_loaders,
            build_redirect_resolver_fn=_pipeline_redirect_resolver_builder(),
        )
        setup.progress_phase["key"] = "executing_sources"
        setup.progress_phase["label"] = "Executing sources"
        pipeline_execution_flow_mod.execute_pipeline_sources(setup)
        setup.progress_phase["key"] = "merging_results"
        setup.progress_phase["label"] = "Merging results"
        setup.write_progress_report(force=True)
        setup.stop_progress_reporter()
        return pipeline_finalize_pkg.finalize_pipeline_run(
            paths=setup.paths,
            source_reports=setup.source_reports,
            canonical_rows=setup.canonical_rows,
            using_default_loaders=setup.using_default_loaders,
            selected_loaders=setup.selected_loaders,
            effective_seed_from_existing_output=setup.effective_seed_from_existing_output,
            preserve_previous_on_empty=preserve_previous_on_empty,
            source_state_rows=setup.source_state_rows,
            lifecycle_rows=setup.lifecycle_rows,
            runtime_payload=setup.runtime_payload,
            redirect_resolver=setup.redirect_resolver,
            task_runtime=setup.task_runtime,
            progress_phase=setup.progress_phase,
            write_progress_report=setup.write_progress_report,
            write_task_state=setup.write_task_state,
            started_at=setup.started_at,
            run_started_mono=run_started_mono,
            run_id=run_id,
            circuit_breaker_failures=circuit_breaker_failures,
            circuit_breaker_cooldown_minutes=circuit_breaker_cooldown_minutes,
            circuit_breaker_zero_kept=circuit_breaker_zero_kept,
        )
    finally:
        if setup is not None:
            setup.stop_progress_reporter()


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
        default=DEFAULT_FETCH_MAX_WORKERS,
        help="Max concurrent source workers. Use >1 to run source loaders in parallel.",
    )
    parser.add_argument(
        "--max-per-domain",
        type=int,
        default=DEFAULT_FETCH_MAX_PER_DOMAIN,
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
                "[jobs_fetcher] Incremental mode: "
                f"skipping {len(successful)} previously successful sources; "
                f"running {len(selected)}",
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
    summary = _as_dict(report.get("summary"))
    output_count = int(summary.get("outputCount") or 0)
    failed_sources = int(summary.get("failedSources") or 0)
    runtime = _as_dict(report.get("runtime"))
    timing_summary = _as_dict(runtime.get("timingSummary"))
    stage_top = _as_list(timing_summary.get("stageTop"))
    slowest_sources = _as_list(runtime.get("slowestSources"))
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
