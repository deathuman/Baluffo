import pytest

from src.jobs.adapters.plugins.static import blizzard, littlechicken
from src.jobs.adapters.static_runtime_support import is_static_fetch_fallback_exception

from ._helpers import jf


def test_static_fetch_fallback_predicate_keeps_unexpected_runtime_errors_visible() -> None:
    assert is_static_fetch_fallback_exception(RuntimeError("HTTP 429 Too Many Requests"))
    assert is_static_fetch_fallback_exception(RuntimeError("Network error for https://example.com"))
    assert is_static_fetch_fallback_exception(TimeoutError("timed out"))
    assert not is_static_fetch_fallback_exception(RuntimeError("Unexpected URL"))


def test_blizzard_plugin_detail_fetch_fallback_is_expected_fetch_failures_only() -> None:
    listing_html = """
        <a href="/global/en/unavailable">Unavailable</a>
        <a href="/global/en/engineering-technology">ENGINEERING & TECHNOLOGY</a>
        """
    role_html = (
        '<a href="https://careers.blizzard.com/global/en/search-results?'
        'rk=l-engineering-technology">View Open Jobs</a>'
    )
    results_html = """
        <a href="https://careers.blizzard.com/global/en/job/R026699/Software-Engineer-Server-World-of-Warcraft-Irvine-CA">Software Engineer, Server - World of Warcraft | Irvine, CA</a>
        <div>Location Irvine, California, United States of America Posted Date January 30 2026 Category Engineering Job Id R026699</div>
        """

    def fake_fetch(url: str, _: int) -> str:
        if url == "https://careers.blizzard.com/global/en":
            return listing_html
        if url == "https://careers.blizzard.com/global/en/unavailable":
            raise RuntimeError(f"HTTP 404 for {url}")
        if url == "https://careers.blizzard.com/global/en/engineering-technology":
            return role_html
        if "search-results?rk=l-engineering-technology" in url:
            return results_html
        raise RuntimeError(f"Unexpected URL: {url}")

    rows = blizzard.run(
        fetch_text=fake_fetch,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        pages=["https://careers.blizzard.com/global/en"],
        source_row={
            "name": "Blizzard Entertainment",
            "company": "Blizzard Entertainment",
            "id": "blizzard",
        },
        parse_jobpostings_from_html=lambda *_args, **_kwargs: [],
    )

    assert [row["title"] for row in rows] == [
        "Software Engineer, Server - World of Warcraft | Irvine, CA"
    ]

    def unexpected_fetch(url: str, _: int) -> str:
        if url == "https://careers.blizzard.com/global/en":
            return '<a href="/global/en/engineering-technology">ENGINEERING & TECHNOLOGY</a>'
        raise RuntimeError(f"Unexpected URL: {url}")

    with pytest.raises(RuntimeError, match="Unexpected URL"):
        blizzard.run(
            fetch_text=unexpected_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            pages=["https://careers.blizzard.com/global/en"],
            source_row={
                "name": "Blizzard Entertainment",
                "company": "Blizzard Entertainment",
                "id": "blizzard",
            },
            parse_jobpostings_from_html=lambda *_args, **_kwargs: [],
        )


def test_littlechicken_plugin_detail_fetch_fallback_is_expected_fetch_failures_only() -> None:
    listing_html = """
        <article><h2>3D Artist Internship</h2><a href="/job/3d-artist-internship/">Read more</a></article>
        """

    def http_failure_fetch(url: str, _: int) -> str:
        if url == "https://www.littlechicken.nl/jobs/":
            return listing_html
        raise RuntimeError(f"HTTP 404 for {url}")

    rows = littlechicken.run(
        fetch_text=http_failure_fetch,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        pages=["https://www.littlechicken.nl/jobs/"],
        source_row={
            "name": "Little Chicken",
            "company": "Little Chicken",
            "id": "littlechicken",
        },
        parse_jobpostings_from_html=jf.parse_jobpostings_from_html,
    )

    assert [row["title"] for row in rows] == ["Read more"]

    def unexpected_fetch(url: str, _: int) -> str:
        if url == "https://www.littlechicken.nl/jobs/":
            return listing_html
        raise RuntimeError(f"Unexpected URL: {url}")

    with pytest.raises(RuntimeError, match="Unexpected URL"):
        littlechicken.run(
            fetch_text=unexpected_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            pages=["https://www.littlechicken.nl/jobs/"],
            source_row={
                "name": "Little Chicken",
                "company": "Little Chicken",
                "id": "littlechicken",
            },
            parse_jobpostings_from_html=jf.parse_jobpostings_from_html,
        )
