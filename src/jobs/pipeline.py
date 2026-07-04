"""Package-owned pipeline entrypoints.

AI boundary owns: jobs pipeline package entrypoints over setup, execution, finalization, and CLI flow.
AI boundary implement in: this file for package-level pipeline coordination; stage behavior belongs in pipeline_* leaves.
AI boundary search before contracts: jobs_fetcher facade, bridge task launch callers, fetcher runtime contracts, and pipeline tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused pipeline/fetcher tests.
"""

from __future__ import annotations

import argparse
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

from src.jobs import registry as jobs_registry
from src.jobs.adapters import community as community_adapter
from src.jobs.adapters.api import default_source_loaders as adapters_default_source_loaders
from src.jobs.common import config as common_config
from src.jobs.interfaces import SourceLoader
from src.jobs.transport import build_redirect_resolver as transport_build_redirect_resolver
from src.jobs.transport import default_fetch_text

from . import pipeline_cli as pipeline_cli_pkg
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
    browser_fallback_max_workers: int = -1,
    circuit_breaker_zero_kept: int = 3,
    ignore_circuit_breaker: bool = False,
    social_enabled: bool = False,
    social_config_path: Path | None = None,
    social_lookback_minutes: int = DEFAULT_SOCIAL_LOOKBACK_MINUTES,
    static_detail_concurrency: int = DEFAULT_STATIC_DETAIL_CONCURRENCY,
    show_progress: bool = True,
    selection_exclusions: list[dict[str, Any]] | None = None,
    force_refresh_all: bool = False,
    include_linked_static_validation: bool = False,
    include_pending_provider_migration: bool = False,
) -> dict[str, Any]:
    run_started_mono = time.perf_counter()
    setup: pipeline_run_setup_mod.PipelineRunSetup | None = None
    output_dir_path = Path(output_dir)
    previous_studio_source_registry: list[dict[str, Any]] | None = None
    previous_include_pending = jobs_registry.set_include_pending_provider_migration(
        include_pending_provider_migration
    )
    try:
        previous_studio_source_registry = jobs_registry.activate_runtime_studio_source_registry(
            output_dir_path / "source-registry-active.json"
        )
        setup = pipeline_run_setup_mod.prepare_pipeline_run(
            output_dir=output_dir_path,
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
            browser_fallback_max_workers=browser_fallback_max_workers,
            ignore_circuit_breaker=ignore_circuit_breaker,
            social_enabled=social_enabled,
            social_config_path=social_config_path,
            social_lookback_minutes=social_lookback_minutes,
            static_detail_concurrency=static_detail_concurrency,
            show_progress=show_progress,
            selection_exclusions=selection_exclusions,
            force_refresh_all=force_refresh_all,
            include_linked_static_validation=include_linked_static_validation,
            include_pending_provider_migration=include_pending_provider_migration,
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
            static_suppression_policy=setup.runtime_payload.get("staticSuppressionPolicy"),
        )
    finally:
        jobs_registry.restore_studio_source_registry(previous_studio_source_registry)
        jobs_registry.set_include_pending_provider_migration(previous_include_pending)
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
        "--no-seed-existing-output",
        action="store_true",
        help="Do not carry existing jobs into a targeted --only-sources run.",
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
        "--include-linked-static-validation",
        action="store_true",
        help=(
            "Validation-only: include ready linked static sources that are otherwise filtered "
            "by redundant-static rules so dynamic suppression evidence can be observed."
        ),
    )
    parser.add_argument(
        "--include-pending-provider-migration",
        action="store_true",
        help=(
            "Validation-only: include pending provider migration rows in provider fetch loaders."
        ),
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
        "--browser-fallback-max-workers",
        type=int,
        default=-1,
        help="Optional max concurrent browser fallback workers; defaults to --max-workers when omitted.",
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
    return pipeline_cli_pkg.run_cli(
        parse_args(),
        run_pipeline=run_pipeline,
        default_source_loaders=default_source_loaders,
    )


if __name__ == "__main__":
    sys.exit(main())
