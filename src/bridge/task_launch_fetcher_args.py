"""Fetcher CLI argument parsing extracted from TaskLaunchApi.

Thin, pure functions that build the argument list and extra environment
for fetch/uncapped/discovery launches.  No coordinator import — all
dependencies are passed explicitly.

Owns ``RunFetcherRequest`` (moved from the coordinator so callers can
import it without a coordinator cycle).
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypedDict

from src.jobs.common import config as jobs_common_config


class RunFetcherRequest(TypedDict, total=False):
    preset: str
    maxWorkers: int
    maxPerDomain: int
    fetchStrategy: str
    adapterHttpConcurrency: int
    sourceTtlMinutes: int
    hotSourceCadenceMinutes: int
    coldSourceCadenceMinutes: int
    circuitBreakerFailures: int
    circuitBreakerCooldownMinutes: int
    browserFallbackCooldownMinutes: int
    skipSuccessfulSources: bool
    respectSourceCadence: bool
    ignoreCircuitBreaker: bool
    quiet: bool
    socialEnabled: bool
    onlySources: list[str]


# ── small pure helpers ──────────────────────────────────────────────


def _set_cli_option(args: list[str], option: str, value: str) -> None:
    try:
        index = args.index(option)
    except ValueError:
        args.extend([option, value])
        return
    if index + 1 < len(args):
        args[index + 1] = value
    else:
        args.append(value)


def _apply_fetcher_shared_runtime_args(
    args: list[str],
    *,
    max_workers: int,
    max_per_domain: int,
    fetch_strategy: str,
    adapter_http_concurrency: int,
    hot_cadence: int,
    cold_cadence: int,
    circuit_failures: int,
    circuit_cooldown: int,
    browser_fallback_cooldown: int,
) -> None:
    args.extend(["--max-workers", str(max_workers), "--max-per-domain", str(max_per_domain)])
    args.extend(
        [
            "--fetch-strategy",
            fetch_strategy,
            "--adapter-http-concurrency",
            str(adapter_http_concurrency),
        ]
    )
    args.extend(["--circuit-breaker-failures", str(circuit_failures)])
    args.extend(["--circuit-breaker-cooldown-minutes", str(circuit_cooldown)])
    args.extend(["--browser-fallback-cooldown-minutes", str(browser_fallback_cooldown)])
    args.extend(
        [
            "--hot-source-cadence-minutes",
            str(hot_cadence),
            "--cold-source-cadence-minutes",
            str(cold_cadence),
        ]
    )


# ── public API ──────────────────────────────────────────────────────


def build_fetcher_args_from_payload(
    payload: RunFetcherRequest | dict[str, Any],
    *,
    safe_int: Callable[[Any, int, int, int], int],
    default_source_loaders: Callable[[], list[tuple[str, Any]]],
    failed_source_names_from_latest_report: Callable[[set[str] | None], list[str]],
) -> tuple[list[str], str]:
    data = payload if isinstance(payload, dict) else {}
    preset = str(data.get("preset") or "default").strip().lower()
    args: list[str] = []

    max_workers = safe_int(
        data.get("maxWorkers"), jobs_common_config.DEFAULT_FETCH_MAX_WORKERS, 1, 16
    )
    max_per_domain = safe_int(
        data.get("maxPerDomain"), jobs_common_config.DEFAULT_FETCH_MAX_PER_DOMAIN, 1, 6
    )
    fetch_strategy = str(data.get("fetchStrategy") or "auto").strip().lower()
    if fetch_strategy not in {"auto", "http", "browser"}:
        fetch_strategy = "auto"
    adapter_http_concurrency = safe_int(
        data.get("adapterHttpConcurrency"),
        jobs_common_config.DEFAULT_ADAPTER_HTTP_CONCURRENCY,
        1,
        128,
    )
    source_ttl = safe_int(data.get("sourceTtlMinutes"), 360, 0, 1440)
    hot_cadence = safe_int(data.get("hotSourceCadenceMinutes"), 15, 1, 240)
    cold_cadence = safe_int(data.get("coldSourceCadenceMinutes"), 60, 1, 1440)
    circuit_failures = safe_int(data.get("circuitBreakerFailures"), 3, 0, 20)
    circuit_cooldown = safe_int(data.get("circuitBreakerCooldownMinutes"), 180, 0, 24 * 60)
    browser_fallback_cooldown = safe_int(data.get("browserFallbackCooldownMinutes"), 30, 0, 24 * 60)

    _apply_fetcher_shared_runtime_args(
        args,
        max_workers=max_workers,
        max_per_domain=max_per_domain,
        fetch_strategy=fetch_strategy,
        adapter_http_concurrency=adapter_http_concurrency,
        hot_cadence=hot_cadence,
        cold_cadence=cold_cadence,
        circuit_failures=circuit_failures,
        circuit_cooldown=circuit_cooldown,
        browser_fallback_cooldown=browser_fallback_cooldown,
    )

    if preset == "incremental":
        args.extend(["--skip-successful-sources", "--source-ttl-minutes", str(source_ttl)])
    elif preset == "retry_failed":
        available_names = {name for name, _loader in default_source_loaders()}
        failed_names = failed_source_names_from_latest_report(available_names)
        if failed_names:
            args.extend(["--only-sources", ",".join(failed_names)])
        args.extend(["--ignore-circuit-breaker"])
    elif preset == "uncapped":
        args.extend(["--force-refresh-all", "--ignore-circuit-breaker"])
        _set_cli_option(args, "--max-workers", "50")
        _set_cli_option(args, "--max-per-domain", "5")
        _set_cli_option(
            args,
            "--static-detail-concurrency",
            str(jobs_common_config.DEFAULT_STATIC_DETAIL_CONCURRENCY),
        )
        _set_cli_option(args, "--source-ttl-minutes", "0")
    elif preset == "force_full":
        args.extend(["--ignore-circuit-breaker"])
    else:
        preset = "default"

    if bool(data.get("skipSuccessfulSources")) and "--skip-successful-sources" not in args:
        args.append("--skip-successful-sources")
        args.extend(["--source-ttl-minutes", str(source_ttl)])
    if bool(data.get("respectSourceCadence")) and "--respect-source-cadence" not in args:
        args.append("--respect-source-cadence")
    if bool(data.get("ignoreCircuitBreaker")) and "--ignore-circuit-breaker" not in args:
        args.append("--ignore-circuit-breaker")
    if bool(data.get("quiet")) and "--quiet" not in args:
        args.append("--quiet")
    social_enabled = data.get("socialEnabled")
    if social_enabled is None:
        social_enabled = True
    if bool(social_enabled) and "--social-enabled" not in args:
        args.append("--social-enabled")

    only_sources = data.get("onlySources")
    if isinstance(only_sources, list):
        sanitized = [str(item).strip() for item in only_sources if str(item).strip()]
        if sanitized:
            args.extend(["--only-sources", ",".join(sanitized)])
    return args, preset


def build_fetcher_extra_env_from_preset(preset: str) -> dict[str, str]:
    normalized_preset = str(preset or "").strip().lower()
    if normalized_preset != "uncapped":
        return {}
    return {
        "BALUFFO_FETCH_SEED_EXISTING_OUTPUT": "1",
        "BALUFFO_STATIC_SOURCE_TIME_BUDGET_S": "180",
        "BALUFFO_STATIC_LOW_YIELD_DETAIL_CAP": "0",
        "BALUFFO_STATIC_VERY_LOW_YIELD_DETAIL_CAP": "0",
        "BALUFFO_STATIC_DETAIL_HEURISTICS_PROFILE": "broad",
        "BALUFFO_UNCAPPED_DEEP_STATIC": "1",
    }


__all__ = [
    "RunFetcherRequest",
    "build_fetcher_args_from_payload",
    "build_fetcher_extra_env_from_preset",
]
