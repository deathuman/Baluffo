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

CONTAINER_DEFAULT_FETCH_MAX_WORKERS = 4
CONTAINER_DEFAULT_FETCH_MAX_PER_DOMAIN = 2
CONTAINER_DEFAULT_ADAPTER_HTTP_CONCURRENCY = 16
CONTAINER_DEFAULT_STATIC_DETAIL_CONCURRENCY = 4


class RunFetcherRequest(TypedDict, total=False):
    preset: str
    maxWorkers: int
    maxPerDomain: int
    fetchStrategy: str
    adapterHttpConcurrency: int
    staticDetailConcurrency: int
    browserFallbackMaxWorkers: int
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


class OnlySourcesValidationError(ValueError):
    def __init__(self, requested: list[str]) -> None:
        self.requested = list(requested)
        joined = ", ".join(self.requested) if self.requested else "<empty>"
        super().__init__(f"No requested onlySources matched available loaders: {joined}")


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


def _remove_cli_flag(args: list[str], option: str) -> None:
    while option in args:
        args.pop(args.index(option))


def _apply_fetcher_shared_runtime_args(
    args: list[str],
    *,
    max_workers: int,
    max_per_domain: int,
    fetch_strategy: str,
    adapter_http_concurrency: int,
    static_detail_concurrency: int | None,
    hot_cadence: int,
    cold_cadence: int,
    circuit_failures: int,
    circuit_cooldown: int,
    browser_fallback_cooldown: int,
    browser_fallback_max_workers: int | None,
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
    if static_detail_concurrency is not None:
        args.extend(["--static-detail-concurrency", str(static_detail_concurrency)])
    args.extend(["--circuit-breaker-failures", str(circuit_failures)])
    args.extend(["--circuit-breaker-cooldown-minutes", str(circuit_cooldown)])
    args.extend(["--browser-fallback-cooldown-minutes", str(browser_fallback_cooldown)])
    if browser_fallback_max_workers is not None:
        args.extend(["--browser-fallback-max-workers", str(browser_fallback_max_workers)])
    args.extend(
        [
            "--hot-source-cadence-minutes",
            str(hot_cadence),
            "--cold-source-cadence-minutes",
            str(cold_cadence),
        ]
    )


def _fetcher_runtime_defaults(container_mode: bool) -> tuple[int, int, int]:
    if container_mode:
        return (
            CONTAINER_DEFAULT_FETCH_MAX_WORKERS,
            CONTAINER_DEFAULT_FETCH_MAX_PER_DOMAIN,
            CONTAINER_DEFAULT_ADAPTER_HTTP_CONCURRENCY,
        )
    return (
        jobs_common_config.DEFAULT_FETCH_MAX_WORKERS,
        jobs_common_config.DEFAULT_FETCH_MAX_PER_DOMAIN,
        jobs_common_config.DEFAULT_ADAPTER_HTTP_CONCURRENCY,
    )


def _static_detail_concurrency_from_payload(
    data: dict[str, Any],
    *,
    safe_int: Callable[[Any, int, int, int], int],
    container_mode: bool,
) -> int | None:
    if not container_mode and "staticDetailConcurrency" not in data:
        return None
    default_value = (
        CONTAINER_DEFAULT_STATIC_DETAIL_CONCURRENCY
        if container_mode
        else jobs_common_config.DEFAULT_STATIC_DETAIL_CONCURRENCY
    )
    return safe_int(data.get("staticDetailConcurrency"), default_value, 1, 64)


def _apply_fetcher_preset_args(
    args: list[str],
    *,
    preset: str,
    source_ttl: int,
    default_source_loaders: Callable[[], list[tuple[str, Any]]],
    failed_source_names_from_latest_report: Callable[[set[str] | None], list[str]],
) -> str:
    if preset == "incremental":
        args.extend(["--skip-successful-sources", "--source-ttl-minutes", str(source_ttl)])
        return preset
    if preset == "retry_failed":
        available_names = {name for name, _loader in default_source_loaders()}
        failed_names = failed_source_names_from_latest_report(available_names)
        if failed_names:
            args.extend(["--only-sources", ",".join(failed_names)])
        args.extend(["--ignore-circuit-breaker"])
        return preset
    if preset == "uncapped":
        args.extend(["--force-refresh-all", "--ignore-circuit-breaker"])
        _set_cli_option(args, "--max-workers", "50")
        _set_cli_option(args, "--max-per-domain", "5")
        _set_cli_option(
            args,
            "--adapter-http-concurrency",
            str(jobs_common_config.DEFAULT_ADAPTER_HTTP_CONCURRENCY),
        )
        _set_cli_option(
            args,
            "--static-detail-concurrency",
            str(jobs_common_config.DEFAULT_STATIC_DETAIL_CONCURRENCY),
        )
        _set_cli_option(args, "--source-ttl-minutes", "0")
        return preset
    if preset == "force_full":
        args.extend(["--ignore-circuit-breaker"])
        return preset
    return "default"


def _apply_fetcher_flag_overrides(
    args: list[str],
    data: dict[str, Any],
    *,
    source_ttl: int,
) -> None:
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


def _apply_only_sources_override(
    args: list[str],
    data: dict[str, Any],
    *,
    default_source_loaders: Callable[[], list[tuple[str, Any]]],
) -> None:
    only_sources = data.get("onlySources")
    if not isinstance(only_sources, list):
        return
    sanitized = [str(item).strip() for item in only_sources if str(item).strip()]
    if not sanitized:
        return
    available_names = {name for name, _loader in default_source_loaders()}
    matched = [name for name in sanitized if name in available_names]
    if not matched:
        raise OnlySourcesValidationError(sanitized)
    _remove_cli_flag(args, "--skip-successful-sources")
    _remove_cli_flag(args, "--respect-source-cadence")
    if "--force-refresh-all" not in args:
        args.append("--force-refresh-all")
    if "--ignore-circuit-breaker" not in args:
        args.append("--ignore-circuit-breaker")
    _set_cli_option(args, "--source-ttl-minutes", "0")
    args.extend(["--only-sources", ",".join(matched)])


# ── public API ──────────────────────────────────────────────────────


def build_fetcher_args_from_payload(
    payload: RunFetcherRequest | dict[str, Any],
    *,
    safe_int: Callable[[Any, int, int, int], int],
    default_source_loaders: Callable[[], list[tuple[str, Any]]],
    failed_source_names_from_latest_report: Callable[[set[str] | None], list[str]],
    container_mode: bool = False,
) -> tuple[list[str], str]:
    data = payload if isinstance(payload, dict) else {}
    preset = str(data.get("preset") or "default").strip().lower()
    args: list[str] = []

    (
        default_max_workers,
        default_max_per_domain,
        default_adapter_http_concurrency,
    ) = _fetcher_runtime_defaults(container_mode)
    max_workers = safe_int(data.get("maxWorkers"), default_max_workers, 1, 16)
    max_per_domain = safe_int(data.get("maxPerDomain"), default_max_per_domain, 1, 6)
    fetch_strategy = str(data.get("fetchStrategy") or "auto").strip().lower()
    if fetch_strategy not in {"auto", "http", "browser"}:
        fetch_strategy = "auto"
    adapter_http_concurrency = safe_int(
        data.get("adapterHttpConcurrency"),
        default_adapter_http_concurrency,
        1,
        128,
    )
    static_detail_concurrency = _static_detail_concurrency_from_payload(
        data,
        safe_int=safe_int,
        container_mode=container_mode,
    )
    source_ttl = safe_int(data.get("sourceTtlMinutes"), 360, 0, 1440)
    hot_cadence = safe_int(data.get("hotSourceCadenceMinutes"), 15, 1, 240)
    cold_cadence = safe_int(data.get("coldSourceCadenceMinutes"), 60, 1, 1440)
    circuit_failures = safe_int(data.get("circuitBreakerFailures"), 3, 0, 20)
    circuit_cooldown = safe_int(data.get("circuitBreakerCooldownMinutes"), 180, 0, 24 * 60)
    browser_fallback_cooldown = safe_int(data.get("browserFallbackCooldownMinutes"), 30, 0, 24 * 60)
    browser_fallback_max_workers = (
        safe_int(data.get("browserFallbackMaxWorkers"), 0, 0, 16)
        if "browserFallbackMaxWorkers" in data
        else None
    )

    _apply_fetcher_shared_runtime_args(
        args,
        max_workers=max_workers,
        max_per_domain=max_per_domain,
        fetch_strategy=fetch_strategy,
        adapter_http_concurrency=adapter_http_concurrency,
        static_detail_concurrency=static_detail_concurrency,
        hot_cadence=hot_cadence,
        cold_cadence=cold_cadence,
        circuit_failures=circuit_failures,
        circuit_cooldown=circuit_cooldown,
        browser_fallback_cooldown=browser_fallback_cooldown,
        browser_fallback_max_workers=browser_fallback_max_workers,
    )

    preset = _apply_fetcher_preset_args(
        args,
        preset=preset,
        source_ttl=source_ttl,
        default_source_loaders=default_source_loaders,
        failed_source_names_from_latest_report=failed_source_names_from_latest_report,
    )
    _apply_fetcher_flag_overrides(args, data, source_ttl=source_ttl)
    _apply_only_sources_override(args, data, default_source_loaders=default_source_loaders)
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
    "OnlySourcesValidationError",
    "RunFetcherRequest",
    "build_fetcher_args_from_payload",
    "build_fetcher_extra_env_from_preset",
]
