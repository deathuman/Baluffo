from __future__ import annotations

import argparse
import os
from collections.abc import Callable
from pathlib import Path
from typing import Any

from src.jobs.common import config as common_config
from src.jobs.common import social as common_social
from src.jobs.interfaces import SourceLoader
from src.jobs.pipeline_loader_selection import build_excluded_source_report
from src.jobs.state_incremental import should_skip_source_by_ttl
from src.jobs.state_source_state import (
    read_previously_successful_sources,
    read_source_state,
    read_success_cache,
)
from src.jobs.text_utils import clean_text
from src.jobs_fetcher_registry import SOURCE_REPORT_META

DEFAULT_SOCIAL_LOOKBACK_MINUTES = common_config.DEFAULT_SOCIAL_LOOKBACK_MINUTES
load_social_config = common_social.load_social_config


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _default_loaders_for_args(
    args: argparse.Namespace,
    social_config: dict[str, Any],
    default_source_loaders: Callable[..., list[tuple[str, SourceLoader]]],
) -> list[tuple[str, SourceLoader]]:
    try:
        return default_source_loaders(
            social_enabled=bool(args.social_enabled),
            social_config=social_config,
        )
    except TypeError:
        return default_source_loaders()


def _only_sources_selection(
    args: argparse.Namespace,
    default_loaders: list[tuple[str, SourceLoader]],
) -> tuple[list[tuple[str, SourceLoader]] | None, bool, list[dict[str, Any]], bool]:
    selection_exclusions: list[dict[str, Any]] = []
    only_sources = [
        clean_text(part) for part in str(args.only_sources or "").split(",") if clean_text(part)
    ]
    if not only_sources:
        return None, False, selection_exclusions, False
    wanted = set(only_sources)
    source_loaders = [(name, loader) for name, loader in default_loaders if name in wanted]
    for name, _loader in default_loaders:
        if name in wanted:
            continue
        selection_exclusions.append(
            build_excluded_source_report(
                name, "only_sources_filter", source_report_meta=SOURCE_REPORT_META
            )
        )
    missing = [name for name in only_sources if name not in {item[0] for item in source_loaders}]
    if missing:
        print(
            f"[jobs_fetcher] WARN unknown --only-sources entries: {', '.join(missing)}",
            flush=True,
        )
    return source_loaders, True, selection_exclusions, True


def _successful_sources_to_skip(
    *,
    args: argparse.Namespace,
    selected: list[tuple[str, SourceLoader]],
) -> set[str]:
    source_state_path = Path(args.output_dir) / "jobs-source-state.json"
    state_rows = read_source_state(source_state_path)
    successful = {
        name
        for name, _ in selected
        if should_skip_source_by_ttl(name, state_rows, int(args.source_ttl_minutes or 0))
    }
    if successful:
        return successful
    previous_report = Path(args.output_dir) / "jobs-fetch-report.json"
    success_cache_path = Path(args.output_dir) / "jobs-success-cache.json"
    successful = read_success_cache(success_cache_path)
    return successful or read_previously_successful_sources(previous_report)


def _apply_skip_successful_selection(
    *,
    args: argparse.Namespace,
    source_loaders: list[tuple[str, SourceLoader]] | None,
    default_loaders: list[tuple[str, SourceLoader]],
    selection_exclusions: list[dict[str, Any]],
) -> tuple[list[tuple[str, SourceLoader]] | None, bool]:
    if not args.skip_successful_sources:
        return source_loaders, False
    selected = source_loaders if source_loaders is not None else list(default_loaders)
    successful = _successful_sources_to_skip(args=args, selected=selected)
    if successful:
        selected = [(name, loader) for name, loader in selected if name not in successful]
        for source_name in sorted(successful):
            selection_exclusions.append(
                build_excluded_source_report(
                    source_name, "skip_successful_ttl", source_report_meta=SOURCE_REPORT_META
                )
            )
    if not args.quiet:
        print(
            "[jobs_fetcher] Incremental mode: "
            f"skipping {len(successful)} previously successful sources; "
            f"running {len(selected)}",
            flush=True,
        )
    return selected, True


def _dedupe_selection_exclusions(
    selection_exclusions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
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
    return deduped_selection_exclusions


def _run_pipeline_from_args(
    *,
    args: argparse.Namespace,
    run_pipeline: Callable[..., dict[str, Any]],
    source_loaders: list[tuple[str, SourceLoader]] | None,
    seed_from_existing_output: bool,
    selection_exclusions: list[dict[str, Any]],
    forced_only_sources: bool,
    env_run_id: str,
    env_started_at: str,
) -> dict[str, Any]:
    return run_pipeline(
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
        selection_exclusions=selection_exclusions,
        force_refresh_all=bool(args.force_refresh_all),
    )


def _print_timing_summary(label: str, rows: list[Any], value_key: str = "durationMs") -> None:
    if not rows:
        return
    summary = " | ".join(
        f"{clean_text(item.get('stage' if label == 'top-stages' else 'name'))}={int(item.get(value_key) or 0)}ms"
        for item in rows[:3]
        if isinstance(item, dict)
    )
    if summary:
        print(f"[jobs_fetcher] TIMING {label} {summary}", flush=True)


def _print_pipeline_summary(report: dict[str, Any]) -> int:
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
    _print_timing_summary("top-stages", stage_top)
    _print_timing_summary("slowest-sources", slowest_sources)
    return 0 if output_count > 0 else 2


def run_cli(
    args: argparse.Namespace,
    *,
    run_pipeline: Callable[..., dict[str, Any]],
    default_source_loaders: Callable[..., list[tuple[str, SourceLoader]]],
) -> int:
    social_config = load_social_config(
        config_path=Path(args.social_config_path),
        enabled=bool(args.social_enabled),
        lookback_minutes=int(args.social_lookback_minutes or DEFAULT_SOCIAL_LOOKBACK_MINUTES),
    )
    default_loaders = _default_loaders_for_args(args, social_config, default_source_loaders)
    source_loaders, seed_from_existing_output, selection_exclusions, forced_only_sources = (
        _only_sources_selection(args, default_loaders)
    )
    skipped_loaders, skipped_seed = _apply_skip_successful_selection(
        args=args,
        source_loaders=source_loaders,
        default_loaders=default_loaders,
        selection_exclusions=selection_exclusions,
    )
    report = _run_pipeline_from_args(
        args=args,
        run_pipeline=run_pipeline,
        source_loaders=skipped_loaders,
        seed_from_existing_output=bool(seed_from_existing_output or skipped_seed),
        selection_exclusions=_dedupe_selection_exclusions(selection_exclusions),
        forced_only_sources=forced_only_sources,
        env_run_id=clean_text(os.environ.get("BALUFFO_FETCH_RUN_ID")),
        env_started_at=clean_text(os.environ.get("BALUFFO_FETCH_STARTED_AT")),
    )
    return _print_pipeline_summary(report)
