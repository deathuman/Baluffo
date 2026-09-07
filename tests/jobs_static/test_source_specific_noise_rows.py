"""Tests for source-specific static noise-row gating (page_gating).

The itch.io jobs board cases come verbatim from the 2026-09-06 contamination
sweep (docs/snapshots/sector-signal-contamination-2026-09-06.md): the static
parser picked up board navigation (/jobs/<skill>/<type> filters, near-*
location filters), the /games/ directory (platform/CC-license rows), site
pages, and <studio>.itch.io game/devlog pages — while real postings live only
at itch.io/j/<numeric-id>/<slug>.
"""

from __future__ import annotations

from src import jobs_fetcher as jf
from src.jobs.page_gating import looks_like_source_specific_static_noise_row

ITCH_SOURCE = "static_source::static:listing_url:https://itch.io/jobs"


def _gated(title: str, job_link: str, source_name: str = ITCH_SOURCE) -> bool:
    return looks_like_source_specific_static_noise_row(
        title=title,
        job_link=job_link,
        source_name=source_name,
    )


def test_itch_category_filter_rows_are_gated() -> None:
    junk = [
        ("Unity", "https://itch.io/jobs/permanent/unity"),
        ("Windows", "https://itch.io/jobs/permanent/windows"),
        ("XBox One", "https://itch.io/jobs/permanent/xbox-one"),
        ("Remote friendly", "https://itch.io/jobs/permanent/remote"),
        ("Contract work", "https://itch.io/jobs/remote/temporary"),
        ("2D", "https://itch.io/jobs/2d/temporary"),
        ("Game Designer", "https://itch.io/jobs/game-designer/permanent"),
        ("Adobe Photoshop", "https://itch.io/jobs/permanent/photoshop"),
    ]
    for title, link in junk:
        assert _gated(title, link), (title, link)


def test_itch_location_filter_rows_are_gated() -> None:
    junk = [
        ("United States", "https://itch.io/jobs/near-ayfuWVHpZV9pnXvUAWxN17k6o4LAniY9y39XBqaVXKuj"),
        ("Hamburg, Germany", "https://itch.io/jobs/near-eZsMe8SX293tXo4JgSLKTrdKSiAfegPw8C"),
        ("Brighton, UK", "https://itch.io/jobs/near-zdcDUxNXn3CNGFNdU2aTL8jadZWYaySo4A"),
        ("Permanent", "https://itch.io/jobs/near-SAM3y94iTAt75coyaeHMamwyJyW1Fpv6rN"),
        (
            "12322 Exposition Boulevard, Los Angeles, CA, USA",
            "https://itch.io/jobs/near-JTbALLLwRM7z3nUk8rHwCG65MhVzke6U1H",
        ),
    ]
    for title, link in junk:
        assert _gated(title, link), (title, link)


def test_itch_game_directory_rows_are_gated() -> None:
    junk = [
        ("With Webcam support", "https://itch.io/games/input-webcam"),
        ("With Guitar controller support", "https://itch.io/games/input-guitar-controller"),
        ("With code under Artistic License 2.0", "https://itch.io/games/code-artistic"),
        (
            "With assets under Creative Commons Attribution v4.0 International",
            "https://itch.io/games/assets-cc4-by",
        ),
        ("Made with AI-assisted audio", "https://itch.io/games/ai-audio"),
        ("For Web", "https://itch.io/games/platform-web"),
        ("For Linux", "https://itch.io/games/platform-linux"),
        ("Unity Web Player (Legacy)", "https://itch.io/games/unity"),
        ("Software framework", "https://itch.io/game-development/frameworks"),
    ]
    for title, link in junk:
        assert _gated(title, link), (title, link)


def test_itch_site_pages_and_studio_game_pages_are_gated() -> None:
    junk = [
        ("Directory", "https://itch.io/directory"),
        ("Developer Logs", "https://itch.io/devlogs"),
        ("The Ruins of Calaworm", "https://erdbeerscherge.itch.io/ruins-of-calaworm"),
        ("Silver Thread : Episode I", "https://spicaze.itch.io/silver-thread"),
        (
            "Mythic Mahjong - Support Thread",
            "https://thalamusdigital.itch.io/mythic-mahjong/devlog/165150",
        ),
        (
            "Silver Thread Ep1 Remake is out! + Kickstarter",
            "https://spicaze.itch.io/silver-thread/devlog/1648945/silver-thread-ep1",
        ),
    ]
    for title, link in junk:
        assert _gated(title, link), (title, link)


def test_itch_job_detail_rows_are_kept() -> None:
    real = [
        ("Technical 3D Artist", "https://itch.io/j/16/technical-3d-artist"),
        ("3D Artist (m/f/d)", "https://itch.io/j/20/3d-artist-mfd"),
        ("Senior Systems Designer", "https://itch.io/j/30/senior-systems-designer"),
        ("Game Design Lead / Director", "https://itch.io/j/890/game-design-lead-director"),
        (
            "Senior Community Manager - Contract",
            "https://itch.io/j/896/senior-community-manager-contract",
        ),
    ]
    for title, link in real:
        assert not _gated(title, link), (title, link)


def test_itch_title_slug_mismatch_guard_is_preserved() -> None:
    """The pre-existing /j/ title-vs-slug disagreement check still fires."""
    assert _gated("Unrelated Marketing Title", "https://itch.io/j/16/technical-3d-artist")


def test_non_itch_sources_are_unaffected() -> None:
    assert not _gated(
        "Unity",
        "https://itch.io/jobs/permanent/unity",
        source_name="static_source::static:listing_url:https://example.com/careers",
    )
    assert not _gated(
        "Senior Gameplay Programmer",
        "https://jobs.example.com/j/16/senior-gameplay-programmer",
        source_name="greenhouse_boards",
    )


def test_canonicalize_drops_itch_junk_with_non_job_static_page_reason() -> None:
    row, reason = jf.canonicalize_job_with_reason(
        {
            "title": "With Webcam support",
            "company": "Teo Chhim",
            "city": "",
            "country": "Unknown",
            "jobLink": "https://itch.io/games/input-webcam",
            "sector": "Game",
        },
        source=ITCH_SOURCE,
        fetched_at="2026-09-06T00:00:00Z",
    )
    assert row is None
    assert reason == "non_job_static_page"


def test_canonicalize_keeps_itch_job_detail_rows() -> None:
    row, reason = jf.canonicalize_job_with_reason(
        {
            "title": "Technical 3D Artist",
            "company": "Teo Chhim",
            "city": "",
            "country": "Unknown",
            "jobLink": "https://itch.io/j/16/technical-3d-artist",
            "sector": "Game",
        },
        source=ITCH_SOURCE,
        fetched_at="2026-09-06T00:00:00Z",
    )
    assert reason == ""
    assert row is not None
    payload = row if isinstance(row, dict) else row.to_dict()
    assert payload["title"] == "Technical 3D Artist"
