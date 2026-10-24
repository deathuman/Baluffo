"""A skip must not push a source's next-eligible time forward.

``apply_excluded_source_state`` moves ``lastSuccessAt`` only on a 304, yet the deadline used
to be advanced for every terminal status including ``excluded``. Each skip therefore pushed
``nextEligibleCheckAt`` out by another window while the success clock stayed frozen, so
with a cadence shorter than the window the board was never fetched again.
``lastCheckedAt`` did not move either -- a skip is not a check -- which is why the live
report showed 94% of boards skipped with no visible staleness anywhere in it.
"""

from datetime import UTC, datetime, timedelta

from src.jobs import state_incremental as si
from src.jobs.state_source_records import apply_excluded_source_state
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


def test_a_304_report_refreshes_the_success_clock() -> None:
    """The applier has to match the spelling the flows actually write.

    ``excluded_source_skip_reason`` prefers ``exclusionReason``, and both the static and
    provider flows write ``cache_not_modified_304`` there while putting the bare
    ``not_modified_304`` token on ``cacheDecisionReason``. Matching only the bare token
    meant a 304 refreshed nothing, so the board re-asked on every run instead of waiting
    out a window. Both spellings, and either field on its own, now count: a live state row
    keeps only the decision reason, while the report builder writes both.
    """
    for report in (
        {
            "exclusionReason": "cache_not_modified_304",
            "cacheDecisionReason": "not_modified_304",
        },
        {"exclusionReason": "not_modified_304"},
        {"cacheDecisionReason": "cache_not_modified_304"},
    ):
        entry = {
            "lastStatus": "excluded",
            "lastSuccessAt": (datetime.now(UTC) - timedelta(days=40)).isoformat(),
            "consecutiveFailures": 2,
            "lastFailureAt": (datetime.now(UTC) - timedelta(hours=1)).isoformat(),
            "lastError": "HTTP Error 503",
            "quarantinedUntilAt": (datetime.now(UTC) + timedelta(hours=6)).isoformat(),
        }
        apply_excluded_source_state(entry, report=report, finished_at=_FINISHED_AT)
        assert entry["lastSuccessAt"] == _FINISHED_AT, report
        assert entry["consecutiveFailures"] == 0, report
        for key in ("lastError", "lastFailureAt", "quarantinedUntilAt"):
            assert key not in entry, (key, report)


def test_a_304_through_the_status_applier_backs_the_board_off() -> None:
    """The writer/applier pair as the pipeline runs it, not as a hand-built report.

    The flows report ``exclusionReason: cache_not_modified_304`` with
    ``cacheDecisionReason: not_modified_304``; this goes through the same applier the
    finalizer uses, then asks the decision what happens next.
    """
    finished_at = datetime.now(UTC).isoformat()
    entry = {
        "lastStatus": "excluded",
        "lastKeptCount": 0,
        "consecutiveZeroKept": 3,
        "lastHttpEtag": '"etag-1"',
        "lastSuccessAt": (datetime.now(UTC) - timedelta(days=20)).isoformat(),
        "lastCheckedAt": (datetime.now(UTC) - timedelta(days=20)).isoformat(),
        "nextEligibleCheckAt": (datetime.now(UTC) - timedelta(minutes=1)).isoformat(),
    }
    _apply_status_state(
        entry,
        report={
            "status": "excluded",
            "keptCount": 0,
            "cacheDecision": "revalidate_only",
            "exclusionReason": "cache_not_modified_304",
            "cacheDecisionReason": "not_modified_304",
        },
        source_name=_SOURCE,
        canonical_rows=[],
        finished_at=finished_at,
        circuit_breaker_failures=3,
        circuit_breaker_cooldown_minutes=720,
        circuit_breaker_zero_kept=2,
    )

    assert entry["lastSuccessAt"] == finished_at
    assert si.get_incremental_cache_decision(_SOURCE, {_SOURCE: entry}, adapter="static") == {
        "cacheDecision": "skip_fresh",
        "cacheDecisionReason": "static_empty_fresh",
    }


def test_a_freshness_skip_still_leaves_the_success_clock_alone() -> None:
    """Guard against the fix widening: only a 304 asserts the copy is current."""
    entry = _state()
    before = entry["lastSuccessAt"]
    apply_excluded_source_state(
        entry,
        report={"exclusionReason": "cache_within_freshness_window"},
        finished_at=_FINISHED_AT,
    )
    assert entry["lastSuccessAt"] == before


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


def test_the_window_a_304_earns_is_also_a_skip() -> None:
    """The skip a 304 earned must not slide the deadline, or the cadence decides the probe."""
    entry = _state()
    before = entry["nextEligibleCheckAt"]
    _skip(entry, "cache_static_revalidated_fresh")
    assert entry["nextEligibleCheckAt"] == before


def test_non_fetch_skip_vocabulary() -> None:
    for reason in (
        "cache_within_freshness_window",
        "skip_fresh",
        "within_freshness_window",
        "dynamic_redundant_provider",
        "not_modified_304",
        "static_revalidated_fresh",
        "cache_static_revalidated_fresh",
        "CACHE_WITHIN_FRESHNESS_WINDOW",
    ):
        assert si.is_non_fetch_skip_reason(reason), reason
    for fetched_reason in ("", None, "ok", "time_budget_exceeded", "http_error"):
        assert not si.is_non_fetch_skip_reason(fetched_reason), fetched_reason
