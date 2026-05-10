from __future__ import annotations

from src.source_discovery.static_candidates import build_static_candidate_from_page


def test_static_candidate_rejects_weak_root_homepage_jobish_link() -> None:
    row = build_static_candidate_from_page(
        "https://www.playstation.com",
        '<a href="/jobs/one-role">One role</a>',
        studio="Sony Computer Entertainment",
        nl_priority=False,
        discovery_method="gamedevmap",
    )

    assert row is None


def test_static_candidate_allows_root_homepage_with_multiple_concrete_jobs() -> None:
    row = build_static_candidate_from_page(
        "https://studio.example.com",
        """
        <a href="/jobs/one-role">One role</a>
        <a href="/jobs/two-role">Two role</a>
        """,
        studio="Studio",
        nl_priority=False,
        discovery_method="gamedevmap",
    )

    assert row is not None
    assert row["listing_url"] == "https://studio.example.com"
    assert row["detailPageCount"] == 2
