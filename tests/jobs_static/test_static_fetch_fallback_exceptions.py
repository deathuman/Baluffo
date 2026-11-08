import pytest

from src.jobs.adapters.plugins.static import blizzard, kojima, littlechicken
from src.jobs.adapters.static_runtime_support import (
    classify_static_fetch_exception,
    is_static_fetch_fallback_exception,
)
from src.jobs.common.http import HttpStatusError

from ._helpers import jf


def test_a_linkedin_999_is_a_block_rather_than_an_empty_board() -> None:
    """A denied request says nothing about the board's jobs, so it must not read as one.

    Ironbelly's sheet board swings between 143 rows and zero on this status alone; classifying
    it as a plain error retired the board's evidence instead of retrying it.
    """
    url = "https://www.linkedin.com/company/ironbelly-studios/"
    exc = HttpStatusError(999, url)

    assert classify_static_fetch_exception(exc, target_url=url) == (
        "blocked_or_challenge",
        True,
    ), "blocked is retryable and belongs in the browser queue"
    assert classify_static_fetch_exception(exc, target_url=url, anti_bot_retry=True) == (
        "anti_bot_or_challenge",
        True,
    )


def test_ssl_hostname_mismatch_is_classified_separately_from_timeout() -> None:
    url = "https://careers.liquidswords.com/jobs/1851070-open-application"
    exc = RuntimeError(
        f"Network error for {url}: [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify "
        "failed: Hostname mismatch, certificate is not valid for 'careers.liquidswords.com'"
    )

    assert classify_static_fetch_exception(exc, target_url=url) == (
        "ssl_certificate_error",
        False,
    ), "a certificate mismatch cannot be repaired by retrying or a browser"


def test_plain_network_errors_still_classify_as_timeout() -> None:
    exc = RuntimeError("Network error for https://example.com/jobs: connection reset by peer")

    assert classify_static_fetch_exception(exc, target_url="https://example.com/jobs") == (
        "timeout",
        True,
    )


def test_dns_resolution_failure_is_not_a_browser_fallback_candidate() -> None:
    url = "https://missing.example/careers"
    exc = RuntimeError(f"Network error for {url}: [Errno 11001] getaddrinfo failed")

    assert classify_static_fetch_exception(exc, target_url=url) == (
        "timeout",
        False,
    ), "a browser cannot repair a DNS resolution failure"


def test_expired_certificate_is_not_a_browser_fallback_candidate() -> None:
    url = "https://expired.example/careers"
    exc = RuntimeError(
        f"Network error for {url}: [SSL: CERTIFICATE_VERIFY_FAILED] "
        "certificate has expired (_ssl.c:1032)"
    )

    assert classify_static_fetch_exception(exc, target_url=url) == (
        "ssl_certificate_error",
        False,
    )


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


def test_redirect_refusal_precedes_warnings_in_partial_errors() -> None:
    from src.jobs.adapters.static_sources import _static_partial_errors

    redirect = (
        "static:Coldwood:https://coldwood.com: Unsafe static redirect from "
        "https://coldwood.com to https://www.stillfront.com/en [site_gone_or_moved]"
    )
    partial_errors = _static_partial_errors(
        ["static:Coldwood:https://coldwood.com: time_budget_exceeded"] * 2,
        [redirect, "static:Coldwood:https://coldwood.com: HTTP 500"],
    )

    assert partial_errors[0] == redirect
    assert partial_errors.count("static:Coldwood:https://coldwood.com: time_budget_exceeded") == 1
    assert partial_errors[-1] == "static:Coldwood:https://coldwood.com: HTTP 500"


def test_kojima_dynamic_listing_fallback_is_expected_fetch_failures_only() -> None:
    listing_html = """
        <div data-viewref="kjp_job_listing">
          <a href="/en/game-programmer">Game Programmer</a>
        </div>
        """

    def fake_fetch(url: str, _: int) -> str:
        if url == "https://www.kojimaproductions.jp/en/careers":
            return listing_html
        raise RuntimeError(f"Unexpected URL: {url}")

    def http_failure_dynamic(**_: object) -> str:
        raise RuntimeError("HTTP 500 for https://www.kojimaproductions.jp/kjpviewloader/load")

    rows = kojima.run(
        fetch_text=fake_fetch,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        pages=["https://www.kojimaproductions.jp/en/careers"],
        source_row={
            "name": "Kojima Productions",
            "company": "Kojima Productions",
            "id": "kojima",
        },
        parse_jobpostings_from_html=lambda *_args, **_kwargs: [],
        maybe_fetch_kojima_job_listing_html=http_failure_dynamic,
    )

    assert [row["title"] for row in rows] == ["Game Programmer"]

    def unexpected_dynamic(**_: object) -> str:
        raise RuntimeError("programmer bug")

    with pytest.raises(RuntimeError, match="programmer bug"):
        kojima.run(
            fetch_text=fake_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            pages=["https://www.kojimaproductions.jp/en/careers"],
            source_row={
                "name": "Kojima Productions",
                "company": "Kojima Productions",
                "id": "kojima",
            },
            parse_jobpostings_from_html=lambda *_args, **_kwargs: [],
            maybe_fetch_kojima_job_listing_html=unexpected_dynamic,
        )
