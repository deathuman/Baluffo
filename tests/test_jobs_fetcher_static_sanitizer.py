import pytest

from src import jobs_fetcher as jf


def test_canonicalize_job_with_reason_drops_static_page_noise_in_city_field() -> None:
    row, reason = jf.canonicalize_job_with_reason(
        {
            "title": "Studio Operations",
            "company": "Warner Bros. Games",
            "city": "Content & Editorial",
            "country": "Unknown",
            "jobLink": "https://careers.wbd.com/global/en/c/studio-operations-jobs",
            "sector": "Game",
        },
        source="static_source::static:listing_url:https://careers.wbd.com/global/en/wb-games-jobs",
        fetched_at="2026-03-20T00:00:00Z",
    )
    assert row is None
    assert reason == "non_job_static_page"


@pytest.mark.parametrize(
    ("raw", "source"),
    [
        (
            {
                "title": "Software",
                "company": "Stardock",
                "city": "Increase productivity, design intelligent controls and reinforce branding with our enterprise products.",
                "country": "Unknown",
                "jobLink": "https://www.stardock.com/products",
                "sector": "Game",
            },
            "static_source::static:listing_url:https://www.stardock.com/careers",
        ),
        (
            {
                "title": "Purpose-built for gaming.",
                "company": "Immutable",
                "city": "Know which channels are driving players likely to purchase, and which are driving empty wishlists.",
                "country": "Unknown",
                "jobLink": "https://www.immutable.com/chain",
                "sector": "Game",
            },
            "static_source::static:listing_url:https://www.immutable.com/jobs",
        ),
        (
            {
                "title": "JOB OFFERS",
                "company": "GS Studio",
                "city": "Full-time",
                "country": "Unknown",
                "jobLink": "https://www.gs-studio.eu/career/no-open-positions",
                "sector": "Game",
            },
            "static_source::static:listing_url:https://www.gs-studio.eu/career",
        ),
        (
            {
                "title": "Speculative Application - Art UK Remote / West Midlands",
                "company": "Flix Interactive",
                "city": "Don't see an Art role available at Flix right now? We still want to hear from you",
                "country": "UK",
                "jobLink": "https://www.flixinteractive.com/vacancies/speculative-application-art",
                "sector": "Game",
            },
            "static_source::static:listing_url:https://www.flixinteractive.com/",
        ),
        (
            {
                "title": "Medical Scribe",
                "company": "Mercor",
                "city": "Remote",
                "country": "US",
                "jobLink": "https://www.mercor.com/jobs/medical-scribe",
                "sector": "Tech",
            },
            "static_source::static:listing_url:https://doradogames.com/careers",
        ),
        (
            {
                "title": "Farming Team Lead",
                "company": "MediaAlta",
                "city": "Remote",
                "country": "Unknown",
                "jobLink": "https://djinni.co/jobs/farming-team-lead",
                "sector": "Tech",
            },
            "static_source::static:listing_url:https://hitica.games",
        ),
        (
            {
                "title": "Social Media Manager",
                "company": "Mind Friend",
                "city": "Remote",
                "country": "US",
                "jobLink": "https://www.linkedin.com/jobs/view/123",
                "sector": "Tech",
            },
            "static_source::static:listing_url:https://baobabstudios.com/about",
        ),
        (
            {
                "title": "General Application - Customer Support",
                "company": "Ares Interactive",
                "city": "Remote",
                "country": "Unknown",
                "jobLink": "https://ares.rippling-ats.com/job/general-application-customer-support",
                "sector": "Game",
            },
            "static_source::static:listing_url:https://aresinteractive.com/careers/",
        ),
        (
            {
                "title": "Open Application for Game Programmers",
                "company": "Paradox Interactive",
                "city": "Stockholm",
                "country": "SE",
                "jobLink": "https://paradoxinteractive.teamtailor.com/jobs/open-application-game-programmers",
                "sector": "Game",
            },
            "teamtailor_sources",
        ),
    ],
)
def test_canonicalize_job_with_reason_drops_high_confidence_non_job_static_pages(
    raw: dict[str, str],
    source: str,
) -> None:
    row, reason = jf.canonicalize_job_with_reason(
        raw,
        source=source,
        fetched_at="2026-03-20T00:00:00Z",
    )
    assert row is None
    assert reason == "non_job_static_page"


@pytest.mark.parametrize(
    "title",
    [
        "Application Security Engineer",
        "Talent Acquisition Partner",
        "Open World Gameplay Engineer",
    ],
)
def test_canonicalize_job_with_reason_preserves_application_and_talent_job_titles(
    title: str,
) -> None:
    row, reason = jf.canonicalize_job_with_reason(
        {
            "title": title,
            "company": "Example Games",
            "city": "Remote",
            "country": "US",
            "jobLink": "https://example.com/jobs/application-security-engineer",
            "sector": "Game",
        },
        source="static_source::example",
        fetched_at="2026-03-20T00:00:00Z",
    )
    assert reason == ""
    assert row is not None
