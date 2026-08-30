from __future__ import annotations

import json
from pathlib import Path
from typing import cast

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
