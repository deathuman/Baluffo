from __future__ import annotations

import pytest

from src.jobs.adapters.static_detail_heuristics import (
    is_known_non_job_detail_url,
    process_detail_html,
)
from src.jobs.adapters.static_scrapy import _normalize_job
from src.jobs.page_gating import looks_like_static_parser_noise_title


@pytest.mark.parametrize(
    "title",
    [
        "Your browser does not support the video tag.",
        "Dev insights",
        "Welcome To Talentnetwork",
        "Find a thrilling career in game development.",
        "Skip to main content",
        "Dizzaract FZ LLC .css-1jtd2m7{inline-size:1.5rem;block-size:1.5rem;}",
        "Have an account? Log in",
        "Join the community",
        "Reply",
        "Vacancies",
    ],
)
def test_static_parser_noise_titles_are_classified_as_noise(title: str) -> None:
    assert looks_like_static_parser_noise_title(title)


@pytest.mark.parametrize(
    "url",
    [
        "https://gamejobs.co/search?c=Zynga",
        "https://gamejobs.co/search?t=Audio",
        "http://on-5.com/jobs?replytocom=52",
        "https://account.ycombinator.com/authenticate?continue=https%3A%2F%2Fwww.workatastartup.com%2Fapplication%3Fsignup_job_id%3D90564",
    ],
)
def test_static_non_job_detail_urls_reject_sampled_directory_noise(url: str) -> None:
    assert is_known_non_job_detail_url(url)


def test_static_scrapy_normalization_rejects_sampled_directory_noise_row() -> None:
    assert (
        _normalize_job(
            {
                "title": "Reply",
                "company": "On5",
                "jobLink": "http://on-5.com/jobs?replytocom=52",
                "sourceJobId": "reply-52",
            },
            {"name": "On5", "studio": "On5"},
        )
        is None
    )


def test_static_scrapy_normalization_rejects_itch_inherited_title_mismatch() -> None:
    assert (
        _normalize_job(
            {
                "title": "Creative Director",
                "company": "Teo Chhim",
                "jobLink": "https://itch.io/j/30/senior-systems-designer",
                "sourceJobId": "itch-30",
            },
            {
                "name": "static_source::static:listing_url:https://itch.io/jobs",
                "studio": "itch.io",
            },
        )
        is None
    )


def test_static_scrapy_normalization_keeps_itch_title_matching_slug() -> None:
    row = _normalize_job(
        {
            "title": "Senior Systems Designer",
            "company": "Example Studio",
            "jobLink": "https://itch.io/j/30/senior-systems-designer",
            "sourceJobId": "itch-30",
        },
        {
            "name": "static_source::static:listing_url:https://itch.io/jobs",
            "studio": "itch.io",
        },
    )

    assert row is not None
    assert row["title"] == "Senior Systems Designer"


def test_static_scrapy_normalization_rejects_stillfront_cross_board_teamtailor_row() -> None:
    assert (
        _normalize_job(
            {
                "title": "Initiativbewerbung - Playa Games",
                "company": "Stillfront",
                "jobLink": "https://playagames.teamtailor.com/jobs/5852132-initiativbewerbung-praktikum-werkstudierende-playa-games",
                "sourceJobId": "stillfront-playa",
            },
            {
                "name": "static_source::static:listing_url:https://www.stillfront.com/en/career/join-the-team/",
                "studio": "Stillfront",
            },
        )
        is None
    )


def test_static_scrapy_normalization_keeps_normal_same_site_static_job_row() -> None:
    row = _normalize_job(
        {
            "title": "Senior Systems Designer",
            "company": "Example Studio",
            "jobLink": "https://example.com/jobs/senior-systems-designer",
            "sourceJobId": "example-senior-systems-designer",
        },
        {
            "name": "static_source::static:listing_url:https://example.com/jobs",
            "studio": "Example Studio",
        },
    )

    assert row is not None
    assert row["jobLink"] == "https://example.com/jobs/senior-systems-designer"


def test_static_scrapy_normalization_rejects_static_container_artifact_row() -> None:
    assert (
        _normalize_job(
            {
                "title": "Creative",
                "company": "Example Studio",
                "jobLink": "https://example.com/careers/creative",
                "sourceJobId": "example-creative",
            },
            {
                "name": "static_source::static:listing_url:https://example.com/careers",
                "studio": "Example Studio",
            },
        )
        is None
    )


def test_static_scrapy_normalization_keeps_real_role_with_container_word() -> None:
    row = _normalize_job(
        {
            "title": "Creative Producer",
            "company": "Example Studio",
            "jobLink": "https://example.com/careers/creative-producer",
            "sourceJobId": "example-creative-producer",
        },
        {
            "name": "static_source::static:listing_url:https://example.com/careers",
            "studio": "Example Studio",
        },
    )

    assert row is not None
    assert row["title"] == "Creative Producer"


def test_static_detail_fallback_rejects_talentnetwork_parser_noise_title() -> None:
    result = process_detail_html(
        detail="https://koeitecmo.vn/en",
        detail_title="Welcome To Talentnetwork",
        detail_html="""
            <html>
              <head><title>Welcome To Talentnetwork</title></head>
              <body>
                <h1>Welcome To Talentnetwork</h1>
                <p>Job description</p>
                <p>Requirements</p>
                <a href="/apply">Apply now</a>
              </body>
            </html>
        """,
        fetch_ms=1,
        cache_hit=False,
        company="Koei Tecmo Vietnam",
        source_name="static_source::static:listing_url:https://koeitecmo.vn",
        source={},
        ignored_link_titles=set(),
    )

    assert result["rows"] == []
    assert result["rejectedClassification"] == "dead_listing_page"
    assert "Welcome To Talentnetwork" in result["rejectedExample"]


def test_static_detail_fallback_keeps_real_talent_job_title() -> None:
    result = process_detail_html(
        detail="https://example.com/jobs/talent-acquisition-manager",
        detail_title="Talent Acquisition Manager",
        detail_html="""
            <html>
              <head><title>Talent Acquisition Manager</title></head>
              <body>
                <h1>Talent Acquisition Manager</h1>
                <p>Job description</p>
                <p>Requirements</p>
                <a href="/apply">Apply now</a>
              </body>
            </html>
        """,
        fetch_ms=1,
        cache_hit=False,
        company="Example Studio",
        source={},
        source_name="static_source::static:listing_url:https://example.com/careers",
        ignored_link_titles=set(),
    )

    assert len(result["rows"]) == 1
    assert result["rows"][0]["title"] == "Talent Acquisition Manager"
