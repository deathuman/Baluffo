from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

from tools.repo_health import source_registry_duplicate_url_policy as policy

ROOT = Path(__file__).resolve().parents[1]


def _row(source_id: str, url: str) -> dict:
    return {"id": source_id, "board_url": None, "listing_url": url}


# canonicalize_careers_url ---------------------------------------------------


def test_canonicalize_collapses_scheme_www_slash_and_fragment() -> None:
    assert policy.canonicalize_careers_url("https://scopely.com/en/join-us") == (
        "scopely.com/en/join-us"
    )
    assert policy.canonicalize_careers_url("http://www.scopely.com/en/join-us/") == (
        "scopely.com/en/join-us"
    )
    assert policy.canonicalize_careers_url("https://www.scopely.com/en/join-us/#openings") == (
        "scopely.com/en/join-us"
    )


def test_canonicalize_lowercases_host_and_path() -> None:
    assert policy.canonicalize_careers_url("https://WWW.IOI.DK/Careers/") == "ioi.dk/careers"


def test_canonicalize_preserves_query_to_avoid_false_merges() -> None:
    a = "https://jobs.careers.microsoft.com/global/en/search?q=games&l=en_us"
    b = "https://jobs.careers.microsoft.com/global/en/search?q=xbox"
    assert policy.canonicalize_careers_url(a) != policy.canonicalize_careers_url(b)
    assert "?q=games" in policy.canonicalize_careers_url(a)


def test_canonicalize_blank_returns_empty() -> None:
    assert policy.canonicalize_careers_url("") == ""
    assert policy.canonicalize_careers_url(None) == ""


def test_canonicalize_uses_listing_url_when_no_board_url() -> None:
    assert policy.canonicalize_careers_url(_row("x", "https://a4vr.com/jobs")["listing_url"]) == (
        "a4vr.com/jobs"
    )


# list_active_url_collisions -------------------------------------------------


def test_flags_twin_active_rows_sharing_canonicalized_url() -> None:
    rows = [
        _row("static:listing_url:https://a4vr.com/jobs", "https://a4vr.com/jobs"),
        _row("static:listing_url:https://www.a4vr.com/jobs/", "https://www.a4vr.com/jobs/"),
    ]
    failures = policy.list_active_url_collisions(rows)
    assert len(failures) == 1
    assert "a4vr.com/jobs" in failures[0]
    assert "static:listing_url:https://a4vr.com/jobs" in failures[0]


def test_single_row_does_not_fail() -> None:
    rows = [_row("static:listing_url:https://a4vr.com/jobs", "https://a4vr.com/jobs")]
    assert policy.list_active_url_collisions(rows) == []


def test_distinct_pages_do_not_fail() -> None:
    rows = [
        _row("a", "https://jobs.careers.microsoft.com/global/en/search?q=games"),
        _row("b", "https://jobs.careers.microsoft.com/global/en/search?q=xbox"),
    ]
    assert policy.list_active_url_collisions(rows) == []


def test_known_collisions_are_grandfathered() -> None:
    canonical = "a4vr.com/jobs"
    rows = [
        _row("static:listing_url:https://a4vr.com/jobs", "https://a4vr.com/jobs"),
        _row("static:listing_url:https://www.a4vr.com/jobs/", "https://www.a4vr.com/jobs/"),
    ]
    assert policy.list_active_url_collisions(rows, known_urls=[canonical]) == []


def test_rows_without_urls_are_skipped() -> None:
    rows = [
        {"id": "greenhouse:slug:scopely", "board_url": None, "listing_url": None},
        {"id": "lever:account:larian", "board_url": None, "listing_url": None},
    ]
    assert policy.list_active_url_collisions(rows) == []


# list_stale_known_collisions ---------------------------------------------------


def test_stale_entry_with_two_active_rows_is_kept() -> None:
    canonical = "shared.board/careers"
    rows = [
        _row("static:listing_url:https://studio-a.com/careers", "https://shared.board/careers"),
        _row("static:listing_url:https://studio-b.com/careers", "https://shared.board/careers"),
    ]
    assert policy.list_stale_known_collisions([canonical], rows) == []


def test_single_row_marks_entry_stale() -> None:
    canonical = "shared.board/careers"
    rows = [_row("static:listing_url:https://studio-a.com/careers", "https://shared.board/careers")]
    failures = policy.list_stale_known_collisions([canonical], rows)
    assert len(failures) == 1
    assert canonical in failures[0]
    assert "only one" in failures[0]
    assert "prune" in failures[0].lower()


def test_zero_rows_marks_entry_stale() -> None:
    canonical = "shared.board/careers"
    failures = policy.list_stale_known_collisions([canonical], [])
    assert len(failures) == 1
    assert "none" in failures[0].lower()


def test_none_baselined_is_never_stale() -> None:
    failures = policy.list_stale_known_collisions([], [_row("a", "https://a4vr.com/jobs")])
    assert failures == []


def test_stale_only_reports_baselined_urls() -> None:
    # a URL that is not baselined is never evaluated for staleness
    rows = [_row("a", "https://bandainamcoent.com/careers")]
    assert policy.list_stale_known_collisions([], rows) == []


def test_multiple_stale_entries_are_all_reported() -> None:
    stale = policy.list_stale_known_collisions(["aaa.com/careers", "bbb.com/jobs"], [])
    assert len(stale) == 2
    assert any("aaa.com/careers" in msg for msg in stale)
    assert any("bbb.com/jobs" in msg for msg in stale)


def test_stale_plus_healthy_mix_reports_only_stale() -> None:
    rows = [
        _row("static:listing_url:https://studio-a.com/careers", "https://good.com/careers"),
        _row("static:listing_url:https://studio-b.com/careers", "https://good.com/careers"),
    ]
    failures = policy.list_stale_known_collisions(["good.com/careers", "bad.com/jobs"], rows)
    assert len(failures) == 1
    assert "bad.com/jobs" in failures[0]


# integration: the committed baseline keeps the guardrail green ------------------


def _active_seed() -> list[dict]:
    path = ROOT / "data" / "defaults" / "source-registry-active.seed.json"
    return cast(list[dict], json.loads(path.read_text(encoding="utf-8")))


def test_guardrail_passes_on_committed_seed() -> None:
    assert policy.check_active_seed_twin_career_urls(ROOT) == []


def test_baseline_covers_every_current_active_collision() -> None:
    """Every real twin in the seed must be recorded in the baseline file."""
    known = policy._load_known_collisions(ROOT)
    uncovered = policy.list_active_url_collisions(_active_seed(), known_urls=[])
    missing = [
        (collision.split(" is registered")[0], collision)
        for collision in uncovered
        if collision.split(" is registered")[0] not in known
    ]
    assert missing == [], f"baseline missing coverage for {len(missing)} collision(s): {missing}"


def test_no_stale_entries_in_committed_baseline() -> None:
    """Every baseline entry must still be backed by 2+ active seed rows."""
    assert policy.check_active_seed_stale_baseline(ROOT) == []


def test_every_current_baseline_entry_backed_by_two_rows() -> None:
    """Direct invariant: no baselined URL collapsed below two active rows."""
    known = policy._load_known_collisions(ROOT)
    stale = policy.list_stale_known_collisions(known, _active_seed())
    assert stale == [], f"stale baseline entries: {stale}"


# definition consistency (batch-3 lean-registry trap) -------------------------


def _static_row(source_id: str, **fields: object) -> dict:
    row: dict = {"id": source_id, "adapter": "static", "name": "X", "studio": "X"}
    row.update(fields)
    return row


def test_definitionless_static_rows_flags_missing_listing_and_pages() -> None:
    rows = [_static_row("static:listing_url:https://a.example/jobs")]
    failures = policy.list_definitionless_static_rows(rows)
    assert len(failures) == 1
    assert "static:listing_url:https://a.example/jobs" in failures[0]
    assert "definition-less" in failures[0]


def test_definitionless_static_rows_accepts_listing_url() -> None:
    rows = [
        _static_row(
            "static:listing_url:https://a.example/jobs", listing_url="https://a.example/jobs"
        )
    ]
    assert policy.list_definitionless_static_rows(rows) == []


def test_definitionless_static_rows_accepts_pages() -> None:
    rows = [_static_row("static:name:little chicken", pages=["https://a.example/jobs/"])]
    assert policy.list_definitionless_static_rows(rows) == []


def test_definitionless_static_rows_rejects_empty_pages_list() -> None:
    rows = [_static_row("static:listing_url:https://a.example/jobs", pages=[])]
    assert len(policy.list_definitionless_static_rows(rows)) == 1


def test_definitionless_static_rows_ignores_provider_rows() -> None:
    rows = [{"id": "lever:account:aofl", "adapter": "lever", "account": "aofl"}]
    assert policy.list_definitionless_static_rows(rows) == []


def test_definitionless_static_rows_ignores_careers_url_only() -> None:
    """careersUrl alone is advisory metadata — it must NOT satisfy the check."""
    rows = [
        _static_row(
            "static:listing_url:https://a.example/jobs", careersUrl="https://a.example/jobs"
        )
    ]
    assert len(policy.list_definitionless_static_rows(rows)) == 1


def test_definition_guardrail_passes_on_committed_seed() -> None:
    assert policy.check_active_seed_definitions(ROOT) == []


# duplicate registry ids ------------------------------------------------------


def test_duplicate_ids_flags_one_id_claimed_twice() -> None:
    rows = [
        _static_row("static:listing_url:https://a.example/jobs"),
        _static_row("static:listing_url:https://a.example/jobs"),
    ]
    failures = policy.list_duplicate_source_ids(rows)
    assert len(failures) == 1
    assert "static:listing_url:https://a.example/jobs" in failures[0]
    assert "claimed by 2" in failures[0]


def test_duplicate_ids_accepts_distinct_ids_on_a_shared_url() -> None:
    """A shared board is a reviewed URL collision, never an id collision."""
    rows = [
        _row("static:listing_url:https://ea.com/careers", "https://ea.com/careers"),
        _row("static:listing_url:https://www.ea.com/careers", "https://www.ea.com/careers"),
    ]
    assert policy.list_duplicate_source_ids(rows) == []


def test_duplicate_ids_ignores_blank_ids_and_junk_rows() -> None:
    rows: list[object] = [{"id": ""}, {"id": "   "}, None, 42, "x"]
    assert policy.list_duplicate_source_ids(cast(Any, rows)) == []


def test_duplicate_ids_is_not_baselineable() -> None:
    """Unlike a shared URL, a duplicate id has no allowlist escape hatch."""
    rows = [_static_row("dup"), _static_row("dup")]
    assert policy.list_duplicate_source_ids(rows) != []


def test_duplicate_id_guardrail_passes_on_committed_seed() -> None:
    assert policy.check_active_seed_duplicate_ids(ROOT) == []


# malformed page references ---------------------------------------------------


def test_malformed_page_refs_flags_relative_and_inline_entries() -> None:
    rows = [
        _static_row("static:listing_url:https://a.example/jobs", pages=["/jobs/team"]),
        _static_row(
            "static:listing_url:https://b.example/jobs", pages=["data:image/png;base64,AA"]
        ),
    ]
    failures = policy.list_rows_with_malformed_page_refs(rows)
    assert len(failures) == 2
    assert any("a.example" in failure for failure in failures)
    assert any("b.example" in failure for failure in failures)


def test_malformed_page_refs_flags_empty_and_hostless_entries() -> None:
    rows = [
        _static_row("static:listing_url:https://a.example/jobs", pages=["  "]),
        _static_row("static:listing_url:https://b.example/jobs", pages=["https:///jobs"]),
    ]
    failures = policy.list_rows_with_malformed_page_refs(rows)
    assert len(failures) == 2
    assert any("<empty>" in failure for failure in failures)
    assert any("https:///jobs" in failure for failure in failures)


def test_malformed_page_refs_covers_detail_pages_sample() -> None:
    rows = [
        _static_row(
            "static:listing_url:https://a.example/jobs",
            pages=["https://a.example/jobs"],
            detailPagesSample=["blob:https://a.example/abc"],
        )
    ]
    failures = policy.list_rows_with_malformed_page_refs(rows)
    assert len(failures) == 1
    assert "detailPagesSample" in failures[0]


def test_malformed_page_refs_accepts_absolute_http_urls() -> None:
    rows = [
        _static_row(
            "static:listing_url:https://a.example/jobs",
            pages=["https://a.example/jobs", "http://b.example/careers"],
        )
    ]
    assert policy.list_rows_with_malformed_page_refs(rows) == []


def test_malformed_page_refs_ignores_non_list_fields_and_junk() -> None:
    rows: list[object] = [
        _static_row("static:listing_url:https://a.example/jobs", pages="https://a.example/jobs"),
        _static_row("static:listing_url:https://b.example/jobs", pages=None),
        None,
        42,
    ]
    assert policy.list_rows_with_malformed_page_refs(cast(Any, rows)) == []


def test_malformed_page_ref_guardrail_passes_on_committed_seed() -> None:
    assert policy.check_active_seed_no_malformed_page_refs(ROOT) == []
