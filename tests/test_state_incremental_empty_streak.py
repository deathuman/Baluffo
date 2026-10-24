from datetime import UTC, datetime, timedelta

from src.jobs import state_incremental as si
from src.jobs.state_source_records import apply_excluded_source_state
from src.jobs.state_source_state import update_source_state_rows


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


def test_kept_static_board_past_the_listing_window_revalidates_first() -> None:
    """A kept board past 3 h asks HEAD before fetching; a 304 skips the whole read."""
    entry = _entry(
        lastKeptCount=12,
        lastSuccessAt=(datetime.now(UTC) - timedelta(hours=20)).isoformat(),
    )
    decision = si.get_incremental_cache_decision(
        "static_source::test",
        {"static_source::test": entry},
        adapter="static",
    )
    assert decision == {
        "cacheDecision": "revalidate_only",
        "cacheDecisionReason": "static_revalidate_due",
    }


def test_kept_static_board_inside_the_listing_window_stays_listing_only() -> None:
    entry = _entry(
        lastKeptCount=12,
        lastListingFingerprint="fingerprint-1",
        lastSuccessAt=(datetime.now(UTC) - timedelta(minutes=30)).isoformat(),
    )
    decision = si.get_incremental_cache_decision(
        "static_source::test",
        {"static_source::test": entry},
        adapter="static",
    )
    assert decision == {
        "cacheDecision": "listing_only",
        "cacheDecisionReason": "static_listing_fresh",
    }


def test_empty_static_board_past_the_empty_window_asks_before_it_downloads() -> None:
    """A board with nothing to offer can only reproduce that zero by re-reading it."""
    entry = _entry(
        lastKeptCount=0,
        consecutiveZeroKept=2,
        zeroJobStreak=2,
        lastHttpEtag='"etag-1"',
        lastSuccessAt=(datetime.now(UTC) - timedelta(hours=20)).isoformat(),
    )
    decision = si.get_incremental_cache_decision(
        "static_source::test",
        {"static_source::test": entry},
        adapter="static",
    )
    assert decision == {
        "cacheDecision": "revalidate_only",
        "cacheDecisionReason": "static_empty_revalidate_due",
    }


def test_a_single_zero_kept_run_with_a_validator_asks_before_the_streak_is_confirmed() -> None:
    """The tag its own empty read stored is the confirmation a second read used to buy.

    This is the confirming read the probe replaces: past the empty window with one
    zero-kept run and a validator in hand, the board used to fall through to a full
    read, because a HEAD could not be trusted until the streak reached two.
    """
    name = "static_source::test"
    entry = _entry(
        lastKeptCount=0,
        consecutiveZeroKept=1,
        zeroJobStreak=1,
        lastHttpEtag='"etag-1"',
        lastSuccessAt=(datetime.now(UTC) - timedelta(hours=20)).isoformat(),
    )
    decision = si.get_incremental_cache_decision(name, {name: entry}, adapter="static")
    assert decision == {
        "cacheDecision": "revalidate_only",
        "cacheDecisionReason": "static_empty_revalidate_due",
    }


def test_a_single_zero_kept_run_without_a_validator_is_still_read_in_full() -> None:
    """Nothing to ask with: the first check has to be the read that learns a tag."""
    name = "static_source::test"
    entry = _entry(
        lastKeptCount=0,
        consecutiveZeroKept=1,
        zeroJobStreak=1,
        lastSuccessAt=(datetime.now(UTC) - timedelta(hours=20)).isoformat(),
    )
    decision = si.get_incremental_cache_decision(name, {name: entry}, adapter="static")
    assert decision == {"cacheDecision": "run_now", "cacheDecisionReason": "static_refresh_due"}


def test_a_304_at_streak_one_is_the_confirmation_and_earns_the_empty_window() -> None:
    """The probe's answer is persisted as the confirmation, so no read follows it.

    Without the empty-window branch a zero-kept board's 304 would fall to the listing
    window and spend a HEAD every three hours, or fall through to ``run_now`` entirely.
    """
    name = "static_source::test"
    now = datetime.now(UTC)
    rows = {
        name: _entry(
            lastKeptCount=0,
            consecutiveZeroKept=1,
            zeroJobStreak=1,
            lastHttpEtag='"etag-1"',
            lastSuccessAt=(now - timedelta(hours=13)).isoformat(),
        )
    }
    assert si.get_incremental_cache_decision(name, rows, adapter="static") == {
        "cacheDecision": "revalidate_only",
        "cacheDecisionReason": "static_empty_revalidate_due",
    }

    rows = update_source_state_rows(
        source_state_rows=rows,
        source_reports=[
            {
                "name": name,
                "adapter": "static",
                "status": "excluded",
                "error": "not_modified_304",
                "exclusionReason": "cache_not_modified_304",
                "cacheDecision": "revalidate_only",
                "cacheDecisionReason": "not_modified_304",
                "httpStatus": 304,
                "keptCount": 0,
                "durationMs": 0,
            }
        ],
        canonical_rows=[],
        finished_at=now.isoformat(),
        circuit_breaker_failures=3,
        circuit_breaker_cooldown_minutes=720,
    )
    assert rows[name]["cacheDecisionReason"] == "not_modified_304"
    assert rows[name]["consecutiveZeroKept"] == 1, "a probe is not a read; the streak stays put"
    assert si.get_incremental_cache_decision(name, rows, adapter="static") == {
        "cacheDecision": "skip_fresh",
        "cacheDecisionReason": "static_empty_fresh",
    }, "the 304 satisfies the empty window instead of being followed by the read"

    # The window still expires into another HEAD, never into a read.
    rows[name]["lastSuccessAt"] = (now - timedelta(hours=13)).isoformat()
    assert si.get_incremental_cache_decision(name, rows, adapter="static") == {
        "cacheDecision": "revalidate_only",
        "cacheDecisionReason": "static_empty_revalidate_due",
    }


def test_empty_static_board_whose_last_run_failed_is_fetched_again() -> None:
    """A 304 would clear the failure record without ever fetching the board."""
    entry = _entry(
        lastStatus="error",
        lastError="HTTP Error 500",
        lastKeptCount=0,
        consecutiveZeroKept=2,
        zeroJobStreak=2,
        lastHttpEtag='"etag-1"',
        lastSuccessAt=(datetime.now(UTC) - timedelta(hours=20)).isoformat(),
    )
    decision = si.get_incremental_cache_decision(
        "static_source::test",
        {"static_source::test": entry},
        adapter="static",
    )
    assert decision == {"cacheDecision": "run_now", "cacheDecisionReason": "static_refresh_due"}


def test_a_304_backs_a_zero_kept_board_off_for_its_empty_window() -> None:
    """A 304 is a successful check, so it earns the same window a real empty read does.

    Before the applier matched the ``cache_not_modified_304`` spelling the flows write, a
    304 refreshed nothing and the board re-asked on every run.
    """
    entry = _entry(
        lastStatus="excluded",
        lastKeptCount=0,
        consecutiveZeroKept=3,
        zeroJobStreak=3,
        lastHttpEtag='"etag-1"',
        lastSuccessAt=(datetime.now(UTC) - timedelta(days=20)).isoformat(),
    )
    apply_excluded_source_state(
        entry,
        report={
            "exclusionReason": "cache_not_modified_304",
            "cacheDecisionReason": "not_modified_304",
        },
        finished_at=datetime.now(UTC).isoformat(),
    )
    assert si.get_incremental_cache_decision(
        "static_source::test", {"static_source::test": entry}, adapter="static"
    ) == {"cacheDecision": "skip_fresh", "cacheDecisionReason": "static_empty_fresh"}

    # The window still expires: one HEAD per window, not a permanent park.
    expired = {
        **entry,
        "lastSuccessAt": (datetime.now(UTC) - timedelta(minutes=13 * 60)).isoformat(),
    }
    assert si.get_incremental_cache_decision(
        "static_source::test", {"static_source::test": expired}, adapter="static"
    ) == {
        "cacheDecision": "revalidate_only",
        "cacheDecisionReason": "static_empty_revalidate_due",
    }


def test_a_304_kept_board_waits_out_the_listing_window_before_probing_again() -> None:
    """A 304 report keeps nothing, so lastKeptCount alone would end the cheap path.

    The validators still say the stored copy is current. The listing read has its own
    window, so the board sleeps through it and spends one HEAD when the window expires,
    rather than one HEAD on every run in between.

    The zero streak is load-bearing: a 304 is not a zero read, so a board that had jobs
    arrives here with no streak and keeps the listing window, where a genuinely empty
    board's 304 satisfies the empty one.
    """
    entry = _entry(
        lastStatus="excluded",
        lastKeptCount=0,
        consecutiveZeroKept=0,
        zeroJobStreak=0,
        lastHttpEtag='"etag-1"',
        cacheDecisionReason="not_modified_304",
        lastSuccessAt=(datetime.now(UTC) - timedelta(minutes=40)).isoformat(),
    )
    decision = si.get_incremental_cache_decision(
        "static_source::test",
        {"static_source::test": entry},
        adapter="static",
    )
    assert decision == {
        "cacheDecision": "skip_fresh",
        "cacheDecisionReason": "static_revalidated_fresh",
    }

    due = {
        **entry,
        "lastSuccessAt": (datetime.now(UTC) - timedelta(hours=3, minutes=10)).isoformat(),
    }
    assert si.get_incremental_cache_decision(
        "static_source::test", {"static_source::test": due}, adapter="static"
    ) == {"cacheDecision": "revalidate_only", "cacheDecisionReason": "static_revalidate_due"}

    # The skip it earned keeps the same window: the stored reason is the state.
    skipped = {
        **entry,
        "cacheDecisionReason": "static_revalidated_fresh",
        "lastSuccessAt": (datetime.now(UTC) - timedelta(minutes=40)).isoformat(),
    }
    assert si.get_incremental_cache_decision(
        "static_source::test", {"static_source::test": skipped}, adapter="static"
    ) == {"cacheDecision": "skip_fresh", "cacheDecisionReason": "static_revalidated_fresh"}


def test_static_board_without_validators_after_a_304_runs_now() -> None:
    entry = _entry(
        lastStatus="excluded",
        lastKeptCount=0,
        consecutiveZeroKept=0,
        zeroJobStreak=0,
        cacheDecisionReason="not_modified_304",
        lastSuccessAt=(datetime.now(UTC) - timedelta(minutes=40)).isoformat(),
    )
    decision = si.get_incremental_cache_decision(
        "static_source::test",
        {"static_source::test": entry},
        adapter="static",
    )
    assert decision == {"cacheDecision": "run_now", "cacheDecisionReason": "static_refresh_due"}


def test_static_board_whose_last_run_failed_is_fetched_again() -> None:
    """A 304 would clear the failure record without ever fetching the board."""
    entry = _entry(
        lastStatus="error",
        lastError="timeout",
        lastKeptCount=12,
        lastHttpEtag='"etag-1"',
        lastSuccessAt=(datetime.now(UTC) - timedelta(hours=20)).isoformat(),
    )
    decision = si.get_incremental_cache_decision(
        "static_source::test",
        {"static_source::test": entry},
        adapter="static",
    )
    assert decision == {"cacheDecision": "run_now", "cacheDecisionReason": "static_refresh_due"}
