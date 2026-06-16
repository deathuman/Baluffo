"""Incremental fetch TTL, cadence, and cache-decision helpers."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from src.jobs.common.datetime_utils import parse_datetime
from src.jobs.text_utils import clean_text, norm_text
from src.jobs_fetcher_registry import SOURCE_REPORT_META

from .common import config as common_config

DEFAULT_INCREMENTAL_FETCH_ENABLED = common_config.DEFAULT_INCREMENTAL_FETCH_ENABLED
DEFAULT_INCREMENTAL_PROVIDER_STABLE_MINUTES = (
    common_config.DEFAULT_INCREMENTAL_PROVIDER_STABLE_MINUTES
)
DEFAULT_INCREMENTAL_STATIC_LISTING_MINUTES = (
    common_config.DEFAULT_INCREMENTAL_STATIC_LISTING_MINUTES
)
DEFAULT_INCREMENTAL_EMPTY_SOURCE_MINUTES = common_config.DEFAULT_INCREMENTAL_EMPTY_SOURCE_MINUTES
DEFAULT_INCREMENTAL_DEAD_SOURCE_MINUTES = common_config.DEFAULT_INCREMENTAL_DEAD_SOURCE_MINUTES

_STATIC_DEAD_SOURCE_TOKENS = (
    "dead_listing_page",
    "parser_stale",
    "fetch_ok_extract_zero",
    "js_required",
    "site_changed",
    "anti_bot_or_challenge",
    "empty_confirmed",
    "needs_review",
)


def should_skip_source_by_ttl(
    source_name: str,
    state_rows: dict[str, dict[str, Any]],
    ttl_minutes: int,
) -> bool:
    if ttl_minutes <= 0:
        return False
    entry = state_rows.get(source_name)
    if not isinstance(entry, dict):
        return False
    if int(entry.get("consecutiveFailures") or 0) > 0:
        return False
    last_success = parse_datetime(entry.get("lastSuccessAt"))
    if not last_success:
        return False
    age_seconds = max(0.0, (datetime.now(UTC) - last_success).total_seconds())
    return age_seconds < float(ttl_minutes * 60)


def should_skip_source_by_cadence(
    source_name: str,
    state_rows: dict[str, dict[str, Any]],
    *,
    hot_minutes: int,
    cold_minutes: int,
) -> bool:
    entry = state_rows.get(source_name)
    if not isinstance(entry, dict):
        return False
    if int(entry.get("consecutiveFailures") or 0) > 0:
        return False
    baseline = parse_datetime(entry.get("lastSuccessAt"))
    if not baseline:
        return False
    cadence_minutes = max(1, int(cold_minutes or 1))
    last_changed = parse_datetime(entry.get("lastChangedAt"))
    if last_changed:
        age_since_change_seconds = max(0.0, (datetime.now(UTC) - last_changed).total_seconds())
        if age_since_change_seconds <= 24 * 60 * 60:
            cadence_minutes = max(1, int(hot_minutes or 1))
    age_seconds = max(0.0, (datetime.now(UTC) - baseline).total_seconds())
    return age_seconds < float(cadence_minutes * 60)


def adapter_for_cache(source_name: str, entry: dict[str, Any], adapter: str = "") -> str:
    explicit = clean_text(adapter)
    if explicit:
        return explicit
    if clean_text(source_name).startswith("static_source::"):
        return "static"
    from_meta = clean_text(SOURCE_REPORT_META.get(source_name, {}).get("adapter"))
    if from_meta:
        return from_meta
    return clean_text(entry.get("lastAdapter"))


def decision_window_minutes(entry: dict[str, Any], *, adapter: str) -> int:
    last_kept = int(entry.get("lastKeptCount") or entry.get("lastJobsFound") or 0)
    last_status = norm_text(entry.get("lastStatus"))
    last_error = norm_text(entry.get("lastError"))
    if adapter == "static":
        if last_status == "error" and any(
            token in last_error for token in _STATIC_DEAD_SOURCE_TOKENS
        ):
            return DEFAULT_INCREMENTAL_DEAD_SOURCE_MINUTES
        if last_kept <= 0:
            return DEFAULT_INCREMENTAL_EMPTY_SOURCE_MINUTES
        return DEFAULT_INCREMENTAL_STATIC_LISTING_MINUTES
    if last_kept <= 0:
        return DEFAULT_INCREMENTAL_EMPTY_SOURCE_MINUTES
    changed_at = parse_datetime(entry.get("lastChangedAt"))
    if changed_at:
        age_since_change_seconds = max(0.0, (datetime.now(UTC) - changed_at).total_seconds())
        if age_since_change_seconds <= 24 * 60 * 60:
            return common_config.DEFAULT_HOT_SOURCE_CADENCE_MINUTES
    return DEFAULT_INCREMENTAL_PROVIDER_STABLE_MINUTES


def compute_next_eligible_check_at(entry: dict[str, Any], *, adapter: str, checked_at: str) -> str:
    checked_dt = parse_datetime(checked_at)
    if not checked_dt:
        return ""
    window_minutes = decision_window_minutes(entry, adapter=adapter)
    return (checked_dt + timedelta(minutes=max(1, int(window_minutes or 1)))).isoformat()


def get_incremental_cache_decision(
    source_name: str,
    state_rows: dict[str, dict[str, Any]],
    *,
    adapter: str = "",
    force_refresh_all: bool = False,
) -> dict[str, str]:
    if force_refresh_all or not DEFAULT_INCREMENTAL_FETCH_ENABLED:
        return {"cacheDecision": "run_now", "cacheDecisionReason": "force_refresh_all"}
    entry = state_rows.get(source_name)
    if not isinstance(entry, dict):
        return {"cacheDecision": "run_now", "cacheDecisionReason": "no_cache_state"}
    effective_adapter = adapter_for_cache(source_name, entry, adapter=adapter)
    next_eligible = parse_datetime(entry.get("nextEligibleCheckAt"))
    now_dt = datetime.now(UTC)
    if next_eligible and next_eligible > now_dt:
        if effective_adapter == "static" and not _has_fetch_success_history(entry):
            return {"cacheDecision": "run_now", "cacheDecisionReason": "no_success_history"}
        return _future_next_eligible_decision(entry)
    personio_decision = _personio_cooldown_decision(entry, now_dt, effective_adapter)
    if personio_decision:
        return personio_decision
    last_success = parse_datetime(entry.get("lastSuccessAt"))
    if not last_success:
        return {"cacheDecision": "run_now", "cacheDecisionReason": "no_success_history"}
    age_seconds = max(0.0, (now_dt - last_success).total_seconds())
    if effective_adapter == "static":
        return _static_cache_decision(entry, age_seconds)
    return _provider_cache_decision(entry, age_seconds, now_dt)


def _has_fetch_success_history(entry: dict[str, Any]) -> bool:
    return bool(
        parse_datetime(entry.get("lastSuccessAt"))
        or parse_datetime(entry.get("lastSuccessfulFetchAt"))
        or parse_datetime(entry.get("lastNonEmptyAt"))
    )


def _future_next_eligible_decision(entry: dict[str, Any]) -> dict[str, str]:
    last_decision = clean_text(entry.get("cacheDecision")) or "skip_fresh"
    last_reason = clean_text(entry.get("cacheDecisionReason")) or "within_freshness_window"
    if last_decision == "run_now":
        last_decision = "skip_fresh"
        last_reason = "within_freshness_window"
    return {"cacheDecision": last_decision, "cacheDecisionReason": last_reason}


def _personio_cooldown_decision(
    entry: dict[str, Any],
    now_dt: datetime,
    adapter: str,
) -> dict[str, str] | None:
    if adapter != "personio":
        return None
    last_error = norm_text(entry.get("lastError"))
    last_failure_at = parse_datetime(entry.get("lastFailureAt"))
    cooldown_cutoff = now_dt - timedelta(
        minutes=max(1, int(common_config.DEFAULT_INCREMENTAL_EMPTY_SOURCE_MINUTES))
    )
    if "429" in last_error and last_failure_at and last_failure_at >= cooldown_cutoff:
        return {
            "cacheDecision": "cooldown_skip",
            "cacheDecisionReason": "personio_rate_limited",
        }
    return None


def _static_cache_decision(entry: dict[str, Any], age_seconds: float) -> dict[str, str]:
    last_kept = int(entry.get("lastKeptCount") or entry.get("lastJobsFound") or 0)
    last_status = norm_text(entry.get("lastStatus"))
    last_error = norm_text(entry.get("lastError"))
    if (
        clean_text(entry.get("lastListingFingerprint"))
        and last_kept > 0
        and age_seconds < float(DEFAULT_INCREMENTAL_STATIC_LISTING_MINUTES * 60)
    ):
        return {"cacheDecision": "listing_only", "cacheDecisionReason": "static_listing_fresh"}
    if (
        last_status == "error"
        and any(token in last_error for token in _STATIC_DEAD_SOURCE_TOKENS)
        and age_seconds < float(DEFAULT_INCREMENTAL_DEAD_SOURCE_MINUTES * 60)
    ):
        return {
            "cacheDecision": "skip_fresh",
            "cacheDecisionReason": "static_dead_or_stale_fresh",
        }
    if last_kept <= 0 and age_seconds < float(DEFAULT_INCREMENTAL_EMPTY_SOURCE_MINUTES * 60):
        return {"cacheDecision": "skip_fresh", "cacheDecisionReason": "static_empty_fresh"}
    return {"cacheDecision": "run_now", "cacheDecisionReason": "static_refresh_due"}


def _provider_cache_decision(
    entry: dict[str, Any],
    age_seconds: float,
    now_dt: datetime,
) -> dict[str, str]:
    last_kept = int(entry.get("lastKeptCount") or entry.get("lastJobsFound") or 0)
    has_validators = bool(
        clean_text(entry.get("lastHttpEtag")) or clean_text(entry.get("lastHttpLastModified"))
    )
    if last_kept <= 0 and age_seconds < float(DEFAULT_INCREMENTAL_EMPTY_SOURCE_MINUTES * 60):
        return {"cacheDecision": "skip_fresh", "cacheDecisionReason": "empty_source_fresh"}
    changed_at = parse_datetime(entry.get("lastChangedAt"))
    if changed_at:
        age_since_change_seconds = max(0.0, (now_dt - changed_at).total_seconds())
        if age_since_change_seconds <= 24 * 60 * 60 and age_seconds < float(
            common_config.DEFAULT_HOT_SOURCE_CADENCE_MINUTES * 60
        ):
            return {"cacheDecision": "skip_fresh", "cacheDecisionReason": "provider_hot_fresh"}
    if age_seconds < float(DEFAULT_INCREMENTAL_PROVIDER_STABLE_MINUTES * 60):
        if has_validators:
            return {
                "cacheDecision": "revalidate_only",
                "cacheDecisionReason": "provider_stable_revalidate",
            }
        return {"cacheDecision": "skip_fresh", "cacheDecisionReason": "provider_stable_fresh"}
    return {"cacheDecision": "run_now", "cacheDecisionReason": "provider_refresh_due"}
