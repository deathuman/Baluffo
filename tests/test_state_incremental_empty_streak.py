from datetime import UTC, datetime, timedelta

from src.jobs import state_incremental as si


def _entry(**overrides: object) -> dict:
    base = {
        "lastStatus": "ok",
        "lastKeptCount": 0,
        "lastSuccessAt": (datetime.now(UTC) - timedelta(minutes=30)).isoformat(),
        "zeroJobStreak": 1,
        "consecutiveZeroKept": 1,
    }
    base.update(overrides)
    return base


def test_single_zero_kept_static_run_is_not_cached_as_empty() -> None:
    decision = si.get_incremental_cache_decision(
        "static_source::test",
        {"static_source::test": _entry()},
        adapter="static",
    )
    assert decision == {"cacheDecision": "run_now", "cacheDecisionReason": "static_refresh_due"}


def test_two_consecutive_zero_kept_static_runs_are_cached_as_empty() -> None:
    entry = _entry(zeroJobStreak=2, consecutiveZeroKept=2)
    decision = si.get_incremental_cache_decision(
        "static_source::test",
        {"static_source::test": entry},
        adapter="static",
    )
    assert decision == {"cacheDecision": "skip_fresh", "cacheDecisionReason": "static_empty_fresh"}


def test_single_zero_kept_provider_run_is_not_cached_as_empty() -> None:
    decision = si.get_incremental_cache_decision(
        "provider_test",
        {"provider_test": _entry()},
        adapter="greenhouse",
    )
    assert decision["cacheDecisionReason"] != "empty_source_fresh"


def test_two_consecutive_zero_kept_provider_runs_are_cached_as_empty() -> None:
    entry = _entry(zeroJobStreak=2, consecutiveZeroKept=2)
    decision = si.get_incremental_cache_decision(
        "provider_test",
        {"provider_test": entry},
        adapter="greenhouse",
    )
    assert decision == {"cacheDecision": "skip_fresh", "cacheDecisionReason": "empty_source_fresh"}


def test_force_refresh_overrides_streak_skip() -> None:
    entry = _entry(zeroJobStreak=2, consecutiveZeroKept=2)
    decision = si.get_incremental_cache_decision(
        "static_source::test",
        {"static_source::test": entry},
        adapter="static",
        force_refresh_all=True,
    )
    assert decision == {"cacheDecision": "run_now", "cacheDecisionReason": "force_refresh_all"}
