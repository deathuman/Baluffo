"""`browserFallbackRecommendedSources` must not read 0 while rows recommend fallback.

A live 0.3.008 run reported `sourceHealth.browserFallbackRecommendedSources: 0` while **199**
source rows carried `browserFallbackRecommended: true`. Not one of those rows had the flag at
the top level -- every one of them had it inside `details[0]`, which is where the pipeline
lands `entry_report`.

`_source_health_row` reads the top level, so the derived count was zero, and the ops surface
said no source needed browser escalation while 78 boards were failing HTTP 403 with fallback
recommended. An empty escalation queue is indistinguishable from a healthy one, which is what
kept this class invisible.

The same lift carries the quarantine fields, which were empty on every row of that run for the
same reason -- with 988 of 1,032 fallback attempts refused by a per-source 30-minute cooldown,
the reason each source entered cooldown was never recorded.

These tests build the shape the live report actually had, rather than a convenient one.
"""

from __future__ import annotations

from src.jobs.common.contracts_source_health import derive_source_health
from src.jobs.common.contracts_source_reports import normalize_source_report_row

RECOMMENDED_COUNT = 3


def _row(*, name: str, recommended: bool, error: str = "", top_level: bool = False) -> dict:
    """A source row shaped like the live report's.

    ``top_level=False`` reproduces the defect: the flag exists only inside ``details[0]``.
    """
    row: dict = {
        "adapter": "static",
        "details": [{"browserFallbackRecommended": recommended, "stats": "{}"}],
        "error": error,
        "keptCount": 0,
        "name": name,
        "status": "error" if error else "ok",
    }
    if top_level:
        row["browserFallbackRecommended"] = recommended
    return row


def _rows() -> list[dict]:
    return [
        _row(name=f"blocked-{i}", recommended=True, error="HTTP 403")
        for i in range(RECOMMENDED_COUNT)
    ] + [_row(name="healthy", recommended=False)]


def test_the_live_shape_no_longer_reports_zero():
    normalized = [normalize_source_report_row(row) for row in _rows()]
    assert all("browserFallbackRecommended" in row for row in normalized), (
        "the flag must reach the normalised row or every summary built from it reads zero"
    )
    health = derive_source_health(normalized)
    assert health["browserFallbackRecommendedSources"] == RECOMMENDED_COUNT


def test_the_recommendation_is_a_bool_not_a_string():
    """`clean_text(True)` is `"True"` and `clean_text(False)` is `""`.

    Routing a bool through the text path would therefore have *looked* right -- `bool("")` is
    False -- while writing a string where the contract declares a bool.
    """
    normalized = [normalize_source_report_row(row) for row in _rows()]
    for row in normalized:
        assert isinstance(row["browserFallbackRecommended"], bool)


def test_a_false_recommendation_stays_false_rather_than_going_missing():
    """An absent flag and a present `False` must not collapse into the same summary."""
    normalized = [normalize_source_report_row(row) for row in _rows()]
    assert normalized[-1]["browserFallbackRecommended"] is False


def test_an_explicit_top_level_value_is_not_overwritten_by_the_detail():
    """Top level wins, so a caller that already hoisted deliberately is not clobbered."""
    row = _row(name="conflict", recommended=True, top_level=True)
    row["details"] = [{"browserFallbackRecommended": False}]
    normalized = normalize_source_report_row(row)
    assert normalized["browserFallbackRecommended"] is True


def test_the_quarantine_fields_are_lifted_too():
    """The cooldown reason was unrecorded, which is why 988 refusals were not diagnosable."""
    row = _row(name="quarantined", recommended=True, error="browser fallback unavailable")
    row["details"] = [
        {
            "browserFallbackFailureCount": 3,
            "browserFallbackLastError": "failed to launch browser",
            "browserFallbackQuarantinedUntilAt": "2026-10-06T00:00:00+00:00",
            "browserFallbackRecommended": True,
        }
    ]
    normalized = normalize_source_report_row(row)
    assert normalized["browserFallbackFailureCount"] == 3
    assert normalized["browserFallbackLastError"] == "failed to launch browser"
    assert normalized["browserFallbackQuarantinedUntilAt"] == "2026-10-06T00:00:00+00:00"


def test_a_row_with_no_details_is_untouched_rather_than_raising():
    """`details` is optional on provider rows; a missing or non-list value must be inert."""
    for details in (None, [], "not-a-list", [{}]):
        row = _row(name="providerish", recommended=False)
        row["details"] = details
        normalized = normalize_source_report_row(row)
        assert (
            "browserFallbackRecommended" not in normalized
            or normalized["browserFallbackRecommended"] is False
        )


def test_a_detail_list_whose_first_entry_is_not_an_object_is_skipped():
    row = _row(name="odd", recommended=True)
    row["details"] = ["junk", {"browserFallbackRecommended": True}]
    normalized = normalize_source_report_row(row)
    assert normalized["browserFallbackRecommended"] is True
