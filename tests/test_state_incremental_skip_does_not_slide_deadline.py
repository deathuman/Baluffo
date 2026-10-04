"""A skip must not push a source's next-eligible time forward.

``apply_excluded_source_state`` never moves ``lastSuccessAt``, yet the deadline used to be
advanced for every terminal status including ``excluded``. Each skip therefore pushed
``nextEligibleCheckAt`` out by another window while the success clock stayed frozen, so
with a cadence shorter than the window the board was never fetched again.
``lastCheckedAt`` did not move either -- a skip is not a check -- which is why the live
report showed 94% of boards skipped with no visible staleness anywhere in it.
"""

from datetime import UTC, datetime, timedelta

from src.jobs import state_incremental as si
from src.jobs.state_source_state import _apply_status_state

_SOURCE = "static_source::static:listing_url:https://example.test/careers"
_FINISHED_AT = "2026-10-04T12:00:00+00:00"


def _state() -> dict:
    """A source last fetched successfully 121 days ago, as the live report showed."""
    return {
        "lastStatus": "ok",
        "lastKeptCount": 0,
        "lastSuccessAt": (datetime.now(UTC) - timedelta(days=121)).isoformat(),
        "lastCheckedAt": (datetime.now(UTC) - timedelta(days=121)).isoformat(),
        "nextEligibleCheckAt": (
            datetime.now(UTC) + timedelta(minutes=si.DEFAULT_INCREMENTAL_EMPTY_SOURCE_MINUTES)
        ).isoformat(),
    }


def _skip(entry: dict, reason: str) -> None:
    entry["lastStatus"] = "excluded"
    _apply_status_state(
        entry,
        report={"exclusionReason": reason, "cacheDecisionReason": reason},
        source_name=_SOURCE,
        canonical_rows=[],
        finished_at=_FINISHED_AT,
        circuit_breaker_failures=3,
        circuit_breaker_cooldown_minutes=720,
        circuit_breaker_zero_kept=2,
    )


def test_freshness_skip_leaves_the_deadline_untouched() -> None:
    entry = _state()
    before = entry["nextEligibleCheckAt"]
    _skip(entry, "cache_within_freshness_window")
    assert entry["nextEligibleCheckAt"] == before


def test_repeated_skips_do_not_slide_the_deadline_forward() -> None:
    """The invariant that breaks the loop: a skip is idempotent on the deadline."""
    entry = _state()
    deadline = entry["nextEligibleCheckAt"]
    for _ in range(50):
        _skip(entry, "cache_within_freshness_window")
        assert entry["nextEligibleCheckAt"] == deadline


def test_an_expired_deadline_makes_the_source_fetchable_again() -> None:
    """The deadline still governs eligibility, and it expires -- a sliding one never did.

    A board whose success is 121 days old is only ever re-fetched because its deadline is
    allowed to pass. This asserts both halves: expired means eligible, live means skipped.
    """
    entry = _state()
    _skip(entry, "cache_within_freshness_window")
    state = {_SOURCE: entry}

    expired = {
        **entry,
        "nextEligibleCheckAt": (datetime.now(UTC) - timedelta(minutes=1)).isoformat(),
    }
    live_decision = si.get_incremental_cache_decision(_SOURCE, state, adapter="static")
    expired_decision = si.get_incremental_cache_decision(
        _SOURCE, {_SOURCE: expired}, adapter="static"
    )

    assert live_decision["cacheDecision"] == "skip_fresh"
    assert expired_decision["cacheDecision"] != "skip_fresh"


def test_not_modified_304_leaves_the_deadline_untouched() -> None:
    entry = _state()
    before = entry["nextEligibleCheckAt"]
    _skip(entry, "not_modified_304")
    assert entry["nextEligibleCheckAt"] == before


def test_a_real_fetch_still_advances_the_deadline() -> None:
    """Guard against over-correcting into a pipeline that never caches anything."""
    for status, report in (
        ("ok", {"keptCount": 12}),
        ("error", {"error": "HTTP Error 500"}),
    ):
        entry = _state()
        entry["lastStatus"] = status
        entry["lastKeptCount"] = 12 if status == "ok" else 0
        entry["lastError"] = "" if status == "ok" else "HTTP Error 500"
        _apply_status_state(
            entry,
            report=report,
            source_name=_SOURCE,
            canonical_rows=[],
            finished_at=_FINISHED_AT,
            circuit_breaker_failures=3,
            circuit_breaker_cooldown_minutes=720,
            circuit_breaker_zero_kept=2,
        )
        assert entry["nextEligibleCheckAt"] != _state()["nextEligibleCheckAt"], status


def test_non_fetch_skip_vocabulary() -> None:
    for reason in (
        "cache_within_freshness_window",
        "skip_fresh",
        "within_freshness_window",
        "dynamic_redundant_provider",
        "not_modified_304",
        "CACHE_WITHIN_FRESHNESS_WINDOW",
    ):
        assert si.is_non_fetch_skip_reason(reason), reason
    for fetched_reason in ("", None, "ok", "time_budget_exceeded", "http_error"):
        assert not si.is_non_fetch_skip_reason(fetched_reason), fetched_reason
