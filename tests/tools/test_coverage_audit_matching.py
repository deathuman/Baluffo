"""Match basis: URL is definitive, title matching is a fallback that over-claims.

The catalogue headline has to distinguish how much coverage is *proven* from how
much is merely similar. Two matchers were tried and rejected against the real
data, and these tests pin why, so nobody re-adds them:

  token-set equality after stripping parentheticals recovered 139 rows and broke
  real distinctions, because the parenthetical is often the qualifier that
  separates two openings ("US Operations (West)" vs "(East)").

  fuzzy ratio matching was worse: the 0.88 pairs it wanted to merge were
  'Staff Platform Engineer' vs 'Data Platform Engineer' and 'Senior Backend
  Software Engineer' vs 'Backend Software Engineer' -- different jobs.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest

_ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location(
    "coverage_audit", _ROOT / "tools" / "coverage_audit.py"
)
assert _spec and _spec.loader
audit_mod: ModuleType = importlib.util.module_from_spec(_spec)
sys.modules["coverage_audit"] = audit_mod
_spec.loader.exec_module(audit_mod)


def _gji(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "title": "Technical Artist",
        "company": "Example Studio",
        "country": "FR",
        "source_ats": "greenhouse",
        "source_url": "https://job-boards.greenhouse.io/example/jobs/1",
    }
    row.update(overrides)
    return row


def test_url_match_is_preferred_even_when_the_title_differs() -> None:
    """Both boards publish the same posting URL, so a URL hit is the same opening."""
    feed = [
        {"title": "Completely Different Words", "company": "Other Co", "jobLink": "https://x/1"}
    ]
    index = audit_mod.build_feed_index(feed, company_key="company")
    row, _fuzzy, known, basis = audit_mod.find_match(
        _gji(company="Example Studio", title="Technical Artist", source_url="https://x/1"),
        index,
        company_key="company",
        url_index=audit_mod.build_url_index(feed),
    )
    assert row is not None
    assert basis == "url"
    assert known is True


def test_title_match_is_labelled_as_such() -> None:
    index = audit_mod.build_feed_index(
        [{"title": "Technical Artist", "company": "Example Studio"}], company_key="company"
    )
    _, _, _, basis = audit_mod.find_match(_gji(), index, company_key="company")
    assert basis == "title"


def test_no_match_reports_an_empty_basis() -> None:
    index = audit_mod.build_feed_index(
        [{"title": "Producer", "company": "Other"}], company_key="company"
    )
    assert audit_mod.find_match(_gji(), index, company_key="company")[3] == ""


def test_url_normalisation_keeps_the_query_string() -> None:
    """Dropping the query collapses a whole board onto one 'matched' opening.

    For Ashby the job id lives in ``?ashby_jid=``. Upstream, 218 distinct jobs
    share one base URL, so stripping the query would have inflated matches and
    hidden real gaps behind them.
    """
    a = "https://boards.ashbyhq.com/x?ashby_jid=aaa"
    b = "https://boards.ashbyhq.com/x?ashby_jid=bbb"
    assert audit_mod.normalize_url(a) != audit_mod.normalize_url(b)


def test_url_normalisation_ignores_scheme_www_and_fragment() -> None:
    assert audit_mod.normalize_url("https://www.example.com/jobs/1#top") == audit_mod.normalize_url(
        "http://example.com/jobs/1"
    )


def test_url_index_ignores_rows_without_a_link() -> None:
    index = audit_mod.build_url_index([{"title": "x"}, {"jobLink": ""}, {"jobLink": "https://y/1"}])
    assert list(index) == ["y/1"]


def test_audit_separates_url_matches_from_title_only_matches() -> None:
    gji = [
        {**_gji(company="Alpha", title="Technical Artist"), "source_url": "https://x/a/1"},
        _gji(company="Beta", title="Sound Designer"),
    ]
    feed = [
        {
            "title": "Technical Artist",
            "company": "Alpha",
            "country": "FR",
            "jobLink": "https://x/a/1",
        },
        {"title": "Sound Designer", "company": "Beta", "country": "FR"},
    ]
    result = audit_mod.audit(gji, feed)
    assert result["gjiMatched"] == 2
    assert result["gjiMatchedByUrl"] == 1
    assert result["gjiMatchedByTitleOnly"] == 1


def test_url_match_survives_a_different_studio_label() -> None:
    """A URL hit is the same posting even when the studio is labelled differently."""
    feed = [{"title": "Technical Artist", "company": "Electronic Arts", "jobLink": "https://x/1"}]
    index = audit_mod.build_feed_index(feed, company_key="company")
    row, fuzzy, _known, basis = audit_mod.find_match(
        _gji(company="Electronic Arts (EA)", source_url="https://x/1"),
        index,
        company_key="company",
        url_index=audit_mod.build_url_index(feed),
    )
    assert row is not None
    assert basis == "url"
    assert fuzzy is True, "a label difference is still reported, not hidden"


# --- Rejected matchers, pinned so they are not reintroduced ----------------


@pytest.mark.parametrize(
    ("gji_title", "feed_title", "distinct_jobs"),
    [
        # The qualifier in the parenthetical is the ONLY difference: two openings.
        (
            "HR Business Partner - US Operations (West)",
            "HR Business Partner - US Operations (East)",
            True,
        ),
        # Seniority variants are different jobs, not a matcher gap.
        ("Senior Backend Software Engineer", "Backend Software Engineer", True),
        ("Staff Platform Engineer", "Data Platform Engineer", True),
        ("Senior Technical Product Manager, AI", "Senior Technical Product Manager", True),
    ],
)
def test_distinct_openings_are_not_collapsed_by_the_matcher(
    gji_title: str, feed_title: str, distinct_jobs: bool
) -> None:
    """Title-only matching must not claim these are the same opening.

    Measured against the catalogue: a ratio matcher wanted to merge every one of
    these at 0.83-0.97 similarity. They are separate requisitions, and merging
    them is how a real gap gets reported as covered.
    """
    feed = [{"title": feed_title, "company": "Example Studio"}]
    index = audit_mod.build_feed_index(feed, company_key="company")
    row, _fuzzy, _known, basis = audit_mod.find_match(
        _gji(title=gji_title, company="Example Studio"), index, company_key="company"
    )
    if distinct_jobs:
        assert row is None, f"{gji_title!r} must not match {feed_title!r} on title alone"
        assert basis == ""
    else:  # pragma: no cover - documents the intended positive case
        assert row is not None
