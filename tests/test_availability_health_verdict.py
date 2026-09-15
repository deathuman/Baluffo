"""Availability-health verdict: coverage-miss / overdue-rising / identity signals.

The verdict must reflect reality: a large but stable overdue population with
healthy coverage is not degraded, and the structural sweep deferral (the
1,000-check budget defers most of a ~46k-row registry every pass) must never
drive the verdict by itself. Per-source attribution (``overdueBySource`` +
run-over-run deltas) makes any overdue change instantly attributable.

Baseline-IO tests (the dedicated terminal-only baseline artifact and the
summary fallback) live in ``test_availability_health_baseline_io.py``.
"""

from typing import Any, cast

from src.jobs.availability_schedule import (
    OVERDUE_BY_SOURCE_LIMIT,
    OVERDUE_RISE_ABSOLUTE,
    _overdue_by_source_counts,
    evaluate_availability_health,
)
from src.shared.availability_report import normalize_availability_health


def _identity_summary(**overrides: int) -> dict[str, int]:
    summary = {
        "rejectedRowCount": 0,
        "unresolvedMissingIdentityCount": 0,
        "unresolvedIdentityConflictCount": 0,
    }
    summary.update(overrides)
    return summary


def _verdict(**overrides: Any) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "verified_within_seven_days_coverage": 0.9796,
        "overdue_count": 289,
        "previous_overdue_count": 291,
        "identity_summary": _identity_summary(),
    }
    kwargs.update(overrides)
    return evaluate_availability_health(**kwargs)


def _overdue_row(source: str) -> dict[str, Any]:
    return {
        "availabilityStatus": "verification_overdue",
        "source": source,
        "jobLink": "https://jobs.example.com/x",
    }


def test_run5_shape_is_healthy_despite_deferral_and_stable_overdue() -> None:
    """The 2026-09-08 full-pass shape: 97.96% coverage, 289 overdue (was 291).

    The old verdict read ``degraded`` purely from structural sweep deferral;
    the evidence-based verdict must read healthy with an explicit negative
    overdue delta.
    """

    verdict = _verdict()
    assert verdict["status"] == "healthy"
    assert verdict["healthReasons"] == []
    assert verdict["coverageTargetMissed"] is False
    assert verdict["overdueRising"] is False
    assert verdict["overdueDelta"] == -2
    assert verdict["overdueBySource"] == {}
    assert verdict["overdueSourceCount"] == 0
    assert verdict["overdueBySourceDelta"] == {}


def test_degraded_when_coverage_target_missed() -> None:
    verdict = _verdict(verified_within_seven_days_coverage=0.90)
    assert verdict["status"] == "degraded"
    assert verdict["healthReasons"] == ["coverage_target_missed"]
    assert verdict["coverageTargetMissed"] is True


def test_coverage_exactly_at_target_is_met() -> None:
    verdict = _verdict(verified_within_seven_days_coverage=0.95)
    assert verdict["status"] == "healthy"
    assert verdict["coverageTargetMissed"] is False


def test_degraded_when_overdue_rises_beyond_hysteresis_band() -> None:
    verdict = _verdict(overdue_count=291 + OVERDUE_RISE_ABSOLUTE + 1)
    assert verdict["status"] == "degraded"
    assert "overdue_rising" in verdict["healthReasons"]
    assert verdict["overdueRising"] is True
    assert verdict["overdueDelta"] == OVERDUE_RISE_ABSOLUTE + 1


def test_small_overdue_churn_within_hysteresis_is_not_rising() -> None:
    verdict = _verdict(overdue_count=291 + OVERDUE_RISE_ABSOLUTE)
    assert verdict["overdueRising"] is False
    assert verdict["status"] == "healthy"


def test_degraded_when_identity_invariants_unresolved() -> None:
    verdict = _verdict(identity_summary=_identity_summary(rejectedRowCount=3))
    assert verdict["status"] == "degraded"
    assert "identity_unresolved" in verdict["healthReasons"]


def test_unknown_previous_overdue_skips_delta_signal() -> None:
    """No persisted baseline (first run, failed run, missing artifact): the
    overdue-delta signal is skipped, never guessed."""

    verdict = _verdict(previous_overdue_count=None)
    assert verdict["overdueDelta"] is None
    assert verdict["overdueRising"] is False
    assert verdict["status"] == "healthy"


def test_multiple_reasons_accumulate() -> None:
    verdict = _verdict(
        verified_within_seven_days_coverage=0.5,
        overdue_count=400,
        identity_summary=_identity_summary(unresolvedIdentityConflictCount=2),
    )
    assert verdict["status"] == "degraded"
    assert verdict["healthReasons"] == [
        "coverage_target_missed",
        "overdue_rising",
        "identity_unresolved",
    ]


def test_overdue_by_source_counts_rows_and_skips_blank_sources() -> None:
    rows: list[Any] = [
        _overdue_row("static_source::static:listing_url:https://a.example/"),
        _overdue_row("static_source::static:listing_url:https://a.example/"),
        _overdue_row("provider_source::google_sheets:jobs"),
        _overdue_row("static_source::static:listing_url:https://b.example/"),
        {"availabilityStatus": "verification_overdue"},  # no source
        "not-a-row",  # non-mapping
    ]
    counts = _overdue_by_source_counts(rows)
    assert counts == {
        "static_source::static:listing_url:https://a.example/": 2,
        "provider_source::google_sheets:jobs": 1,
        "static_source::static:listing_url:https://b.example/": 1,
    }


def test_overdue_by_source_top_n_sorted_with_tiebreak() -> None:
    rows = [_overdue_row(f"src_{index:02d}") for index in range(OVERDUE_BY_SOURCE_LIMIT + 5)]
    rows += [_overdue_row("src_tie")]  # ties with the smallest counted source
    verdict = _verdict(overdue_count=len(rows), overdue_rows=rows, previous_overdue_count=None)
    by_source = verdict["overdueBySource"]
    assert len(by_source) == OVERDUE_BY_SOURCE_LIMIT
    counts = list(by_source.values())
    assert counts == sorted(counts, reverse=True)
    assert verdict["overdueSourceCount"] == OVERDUE_BY_SOURCE_LIMIT + 6
    # Deterministic alphabetical tie-break for equal counts.
    assert list(by_source.items()) == sorted(
        by_source.items(), key=lambda item: (-item[1], item[0])
    )


def test_per_source_delta_from_previous_health_payload() -> None:
    previous = {
        "status": "healthy",
        "overdueCount": 291,
        "overdueBySource": {"src_a": 30, "src_b": 12, "src_gone": 9},
    }
    rows = [_overdue_row("src_a")] * 28 + [_overdue_row("src_b")] * 20 + [_overdue_row("src_new")]
    verdict = _verdict(
        overdue_count=49,
        overdue_rows=rows,
        previous_overdue_count=None,
        previous_availability_health=previous,
    )
    assert verdict["overdueDelta"] == 49 - 291
    assert verdict["overdueBySource"] == {"src_b": 20, "src_a": 28, "src_new": 1}
    assert verdict["overdueBySourceDelta"] == {"src_a": -2, "src_b": 8, "src_new": 1}
    assert "src_gone" not in verdict["overdueBySourceDelta"]


def test_explicit_previous_overdue_count_wins_for_run_level_delta() -> None:
    previous = {"overdueCount": 500, "overdueBySource": {"src_a": 30}}
    rows = [_overdue_row("src_a")] * 12
    verdict = _verdict(
        overdue_count=12,
        overdue_rows=rows,
        previous_overdue_count=291,
        previous_availability_health=previous,
    )
    assert verdict["overdueDelta"] == 12 - 291


def test_previous_payload_without_overdue_count_keeps_per_source_delta_only() -> None:
    previous = {"status": "failed", "overdueBySource": {"src_a": 30}}
    rows = [_overdue_row("src_a")] * 12
    verdict = _verdict(
        overdue_count=12,
        overdue_rows=rows,
        previous_overdue_count=None,
        previous_availability_health=previous,
    )
    assert verdict["overdueDelta"] is None
    assert verdict["overdueRising"] is False
    assert verdict["overdueBySourceDelta"] == {"src_a": -18}


def test_previous_health_non_mapping_is_ignored() -> None:
    verdict = _verdict(
        overdue_rows=[_overdue_row("src_a")],
        previous_overdue_count=None,
        previous_availability_health="garbage",
    )
    assert verdict["overdueDelta"] is None
    assert verdict["overdueBySourceDelta"] == {}
    assert verdict["overdueBySource"] == {"src_a": 1}


def test_run5_shape_with_attribution_uses_source_keys() -> None:
    rows = [
        _overdue_row("static_source::static:listing_url:https://welevel.jobs.personio.com/")
    ] * 53
    rows += [_overdue_row("static_source::static:listing_url:https://penrosestudios.com/")] * 31
    verdict = _verdict(
        overdue_count=289,
        overdue_rows=rows,
        previous_overdue_count=292,
        previous_availability_health={
            "overdueCount": 292,
            "overdueBySource": {
                "static_source::static:listing_url:https://welevel.jobs.personio.com/": 55
            },
        },
    )
    assert verdict["status"] == "healthy"
    assert (
        verdict["overdueBySource"][
            "static_source::static:listing_url:https://welevel.jobs.personio.com/"
        ]
        == 53
    )
    assert (
        verdict["overdueBySourceDelta"][
            "static_source::static:listing_url:https://welevel.jobs.personio.com/"
        ]
        == -2
    )


def test_overdue_rows_attribution_requires_overdue_status() -> None:
    """Attribution contract: the builder counts whatever it is handed, so the
    caller must pass overdue rows only. The old wiring passed the full
    lifecycle map, attributing ~46k available/unavailable rows as overdue
    (google_sheets=61,472 vs overdueCount=101)."""

    rows = [
        {"source": "google_sheets", "availabilityStatus": "available"},
        {"source": "google_sheets", "availabilityStatus": "unavailable"},
        {"source": "grand", "availabilityStatus": "verification_overdue"},
        {"source": "grand", "availabilityStatus": "verification_overdue"},
        {"availabilityStatus": "verification_overdue"},  # no source: dropped
    ]
    # The builder itself cannot distinguish: it counts all sourced rows.
    assert _overdue_by_source_counts(rows) == {"google_sheets": 2, "grand": 2}

    # The corrected caller-side filter (see pipeline_finalize) restores the
    # scalar/attribution consistency the payload contract promises.
    overdue_only = (row for row in rows if row.get("availabilityStatus") == "verification_overdue")
    verdict = evaluate_availability_health(
        verified_within_seven_days_coverage=0.98,
        overdue_count=2,
        overdue_rows=overdue_only,
    )
    assert verdict["overdueCount"] == 2
    assert verdict["overdueBySource"] == {"grand": 2}
    assert verdict["overdueSourceCount"] == 1
    assert sum(verdict["overdueBySource"].values()) == verdict["overdueCount"]


def test_normalized_health_preserves_signed_delta_and_reasons() -> None:
    """Bridge round-trip: falling overdue deltas stay negative and builder
    reasons survive under the canonical ``healthReasons`` name."""

    verdict = evaluate_availability_health(
        verified_within_seven_days_coverage=0.9796,
        overdue_count=289,
        previous_overdue_count=292,
        overdue_rows=[_overdue_row("src_a")] * 5,
        identity_summary=_identity_summary(),
    )
    normalized = normalize_availability_health(verdict)
    assert normalized is not None
    assert normalized["status"] == "healthy"
    assert normalized["overdueDelta"] == -3
    assert normalized["healthReasons"] == []
    assert normalized["overdueBySource"] == {"src_a": 5}

    degraded = normalize_availability_health(
        evaluate_availability_health(
            verified_within_seven_days_coverage=0.5,
            overdue_count=400,
            previous_overdue_count=292,
        )
    )
    assert degraded is not None
    assert degraded["status"] == "degraded"
    assert degraded["healthReasons"] == ["coverage_target_missed", "overdue_rising"]
    assert degraded["overdueDelta"] == 108


def test_normalize_availability_health_fabricates_nothing_for_absent_input() -> None:
    """Absent input (None / empty / non-dict) normalizes to None — the
    fabricated ``overdueCount: 0`` default that used to ride progress
    payloads into the summary artifact was the baseline-poisoning vector."""

    absent_inputs: tuple[Any, ...] = (None, {}, "nope", 42)
    for absent in absent_inputs:
        payload = cast("Any", absent)
        assert normalize_availability_health(payload) is None, absent


def test_normalize_availability_health_preserves_present_payload_fields() -> None:
    """A present payload keeps field-level coercion: empty status and zero
    counts are honest readings of what the producer sent, and the
    terminal-baseline shape gate (not the normalizer) decides baseline
    eligibility."""

    present = normalize_availability_health(
        {
            "status": "",
            "overdueCount": 0,
            "overdueBySource": {},
            "overdueDelta": None,
            "healthReasons": [""],
        }
    )
    assert present == {
        "status": "",
        "overdueCount": 0,
        "overdueBySource": {},
        "overdueSourceCount": 0,
        "overdueBySourceDelta": {},
        "overdueDelta": None,
        "overdueRising": False,
        "coverageTargetMissed": False,
        "healthReasons": [],
        "verifiedWithinDaysTarget": 0,
        "verifiedCoverageTarget": 0.0,
        "verifiedWithinSevenDaysCoverage": 0.0,
        "sweepSelectedCount": 0,
        "sweepDeferredCount": 0,
        "degradedCoverage": False,
        "shadowClassifier": False,
        "identity": {
            "shadowClassifierCounts": {},
            "rejectionReasonCounts": {},
        },
    }
