"""Coverage audit: filter parity and miss classification.

The tool exists to be trusted, so these pin the two things that silently corrupt
a comparison: the replicated GJI filters (a wrong filter invents phantom gaps) and
the distinction between "the studio is missing" and "this one role is missing".
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import ModuleType

import pytest

_TOOL_PATH = Path(__file__).resolve().parents[2] / "tools" / "coverage_audit.py"
_spec = importlib.util.spec_from_file_location("coverage_audit", _TOOL_PATH)
assert _spec and _spec.loader
audit_mod: ModuleType = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(audit_mod)


# --- Europe bucket parity -------------------------------------------------


def test_europe_matches_the_sites_country_set() -> None:
    # The site excludes GB and TR from EUROPE; including them would manufacture a
    # gap for every UK role.
    for iso in ("FR", "DE", "PL", "SE", "CH", "IS", "NO", "GR", "MT"):
        assert iso in audit_mod.EUROPEAN_COUNTRIES
    for iso in ("GB", "UK", "TR", "US", "CA", "UA", "RS"):
        assert iso not in audit_mod.EUROPEAN_COUNTRIES


def test_region_accepts_iso_codes_and_country_names() -> None:
    # The feed stores 73 country values that are not ISO-2, including plain names.
    assert audit_mod.in_europe({"country": "FR"})
    assert audit_mod.in_europe({"country": "Poland"})
    assert audit_mod.in_europe({"country": "Czechia"})
    assert not audit_mod.in_europe({"country": "US"})
    assert not audit_mod.in_europe({"country": "Japan"})


def test_region_falls_back_to_location_text_when_country_is_blank() -> None:
    assert audit_mod.in_europe({"country": "", "locationSummary": "Wroclaw, Poland"})
    assert not audit_mod.in_europe({"country": "", "locationSummary": "Austin, USA"})


def test_region_handles_structured_locations() -> None:
    assert audit_mod.in_europe(
        {"country": None, "locations": [{"city": "Tallinn", "country": "EE"}]}
    )


# --- Query parity ---------------------------------------------------------


def test_query_matches_raw_substring() -> None:
    assert audit_mod.matches_query({"title": "Senior Technical Artist"}, "technical artist")


def test_query_matches_normalised_substring() -> None:
    # Punctuation and spacing must not hide a match; this mirrors normalizeSearchText.
    assert audit_mod.matches_query({"title": "Sr. Technical-Artist (AI)"}, "Technical Artist")
    assert audit_mod.matches_query({"title": "TechnicalArtist"}, "Technical Artist")


def test_query_matches_company_as_well_as_title() -> None:
    assert audit_mod.matches_query(
        {"title": "Artist", "company": "Technical Arts Ltd"}, "technical art"
    )


def test_query_does_not_match_unrelated_titles() -> None:
    assert not audit_mod.matches_query({"title": "Senior Gameplay Engineer"}, "Technical Artist")


def test_empty_query_matches_everything() -> None:
    assert audit_mod.matches_query({"title": "Anything"}, "")


def test_bracket_prefixed_gji_titles_are_compared_without_it() -> None:
    assert (
        audit_mod.normalize_token(audit_mod.strip_bracket_prefix("[Flyway] Animator")) == "animator"
    )


# --- Classification -------------------------------------------------------


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


def test_unregistered_board_when_no_registry_row_matches() -> None:
    bucket = audit_mod.classify_miss(
        _gji(), studio_known=False, registry_ids={"greenhouse:slug:other"}
    )
    assert bucket == audit_mod.BUCKET_UNREGISTERED


def test_registered_no_role_when_a_registry_row_exists_for_the_board() -> None:
    bucket = audit_mod.classify_miss(
        _gji(),
        studio_known=False,
        registry_ids={"greenhouse:slug:example"},
    )
    assert bucket == audit_mod.BUCKET_REGISTERED_NO_ROLE


def test_studio_covered_is_distinct_from_a_missing_board() -> None:
    """The studio is in the feed but the role is not: a per-role gap, not a board gap."""
    bucket = audit_mod.classify_miss(_gji(), studio_known=True, registry_ids=set())
    assert bucket == audit_mod.BUCKET_STUDIO_COVERED


def test_live_probe_overrides_a_covered_studio_to_delisted() -> None:
    board = "https://job-boards.greenhouse.io/example/jobs/1"
    bucket = audit_mod.classify_miss(
        _gji(source_url=board),
        studio_known=True,
        registry_ids=set(),
        live_boards={board: False},
    )
    assert bucket == audit_mod.BUCKET_NOT_ON_BOARD


def test_live_probe_confirms_unregistered_when_the_board_is_live() -> None:
    board = "https://job-boards.greenhouse.io/example/jobs/1"
    bucket = audit_mod.classify_miss(
        _gji(source_url=board),
        studio_known=False,
        registry_ids=set(),
        live_boards={board: True},
    )
    assert bucket == audit_mod.BUCKET_UNREGISTERED


def test_without_a_registry_or_probe_the_bucket_is_unknown() -> None:
    assert (
        audit_mod.classify_miss(_gji(), studio_known=False, registry_ids=set())
        == audit_mod.BUCKET_UNKNOWN
    )


# --- Matching -------------------------------------------------------------


def test_label_variant_is_matched_not_reported_as_a_gap() -> None:
    index = audit_mod.build_feed_index(
        [{"title": "Technical Artist", "company": "Electronic Arts"}], company_key="company"
    )
    row, fuzzy, _ = audit_mod.find_match(
        _gji(company="Electronic Arts (EA)", title="Technical Artist"), index, company_key="company"
    )
    assert row is not None
    assert fuzzy is True


def test_same_studio_different_role_is_a_gap_not_a_label_mismatch() -> None:
    index = audit_mod.build_feed_index(
        [{"title": "Producer", "company": "Example Studio"}], company_key="company"
    )
    row, fuzzy, studio_known = audit_mod.find_match(_gji(), index, company_key="company")
    assert row is None
    assert fuzzy is False
    assert studio_known is True


def test_unrelated_studio_is_not_treated_as_known() -> None:
    index = audit_mod.build_feed_index(
        [{"title": "Producer", "company": "Totally Other Co"}], company_key="company"
    )
    _, _, studio_known = audit_mod.find_match(_gji(), index, company_key="company")
    assert studio_known is False


def test_label_related_requires_a_prefix_stem() -> None:
    assert audit_mod._labels_related("welevelstudios", "welevelgmbh") is False
    assert audit_mod._labels_related("example studio", "example studios") is True
    assert audit_mod._labels_related("sega", "sega europe") is False


# --- End to end -----------------------------------------------------------


def test_audit_counts_matches_misses_and_feed_only(tmp_path: Path) -> None:
    gji = [
        _gji(company="Alpha", title="Technical Artist"),
        _gji(company="Beta", title="Technical Artist", source_url="https://boards.example/beta/1"),
        _gji(company="Gamma", title="Technical Artist", country="US"),
    ]
    feed = [
        {"title": "Technical Artist", "company": "Alpha", "country": "FR"},
        {"title": "Technical Artist", "company": "Alpha", "country": "FR", "source": "sheet"},
        {"title": "Sound Designer", "company": "Delta", "country": "DE"},
    ]
    result = audit_mod.audit(gji, feed, query="Technical Artist", registry_ids=set())

    assert result["gjiConsidered"] == 2  # Gamma is US, outside the EU bucket
    assert result["gjiMatched"] == 1
    assert result["gjiMissing"] == 1
    # The duplicate Alpha row is genuinely feed-only; the matched one must NOT be,
    # which only holds because the index keeps rows by reference.
    assert result["feedOnlyCount"] == 1
    # Beta's studio is not in the feed and no registry/probe was supplied, so the
    # tool must say "unknown" rather than guess at a cause.
    assert result["buckets"] == {audit_mod.BUCKET_UNKNOWN: 1}


def test_audit_reports_the_delisted_bucket_only_with_a_probe() -> None:
    gji = [_gji(company="Alpha")]
    feed: list[dict[str, object]] = []
    board = "https://job-boards.greenhouse.io/example/jobs/1"

    without = audit_mod.audit(gji, feed, query="Technical Artist")
    assert without["buckets"] == {audit_mod.BUCKET_UNKNOWN: 1}

    with_probe = audit_mod.audit(gji, feed, query="Technical Artist", live_boards={board: False})
    assert with_probe["buckets"] == {audit_mod.BUCKET_NOT_ON_BOARD: 1}


def test_audit_always_carries_its_caveats() -> None:
    result = audit_mod.audit([], [], query="Technical Artist")
    joined = " ".join(result["caveats"])
    assert "live probe" in joined
    assert "lower bound" in joined
    assert "detail page" in joined


def test_registry_ids_load_from_plain_and_gzipped_json(tmp_path: Path) -> None:
    plain = tmp_path / "registry.json"
    plain.write_text(json.dumps([{"id": "greenhouse:slug:example"}]), encoding="utf-8")
    assert audit_mod.load_registry_ids(plain) == {"greenhouse:slug:example"}

    import gzip

    packed = tmp_path / "registry.json.gz"
    with gzip.open(packed, "wt", encoding="utf-8") as handle:
        json.dump([{"id": "lever:account:example"}], handle)
    assert audit_mod.load_registry_ids(packed) == {"lever:account:example"}

    assert audit_mod.load_registry_ids(None) == set()


@pytest.mark.parametrize("payload", [{"records": [{"title": "x"}]}, [{"title": "x"}]])
def test_gji_loader_accepts_both_shapes(payload: object, tmp_path: Path) -> None:
    path = tmp_path / "jobs.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    assert audit_mod.load_gji_records(path) == [{"title": "x"}]
