from __future__ import annotations

import src.admin_bridge as admin_bridge
from src.bridge import html_extractor, source_checker
from src.jobs.parsers import parse_jobpostings_from_html
from src.source_registry import source_identity


def test_static_source_check_does_not_count_empty_redirect_alternate_as_job() -> None:
    def fetch_page_with_alternates(_url: str, _timeout_s: int) -> tuple[str, str, bool, bool, str]:
        return (
            "<html><body>No current openings</body></html>",
            "",
            False,
            False,
            ("https://beamdog.example/careers"),
        )

    def fetch_page(_url: str, _timeout_s: int) -> tuple[str, str, bool, bool]:
        raise AssertionError("unexpected detail fetch")

    ok, jobs_found, error, weak_signal, _meta = source_checker.check_static_source(
        {
            "name": "Beamdog",
            "studio": "Beamdog",
            "adapter": "static",
            "pages": ["https://beamdog.example/jobs"],
            "listing_url": "https://beamdog.example/jobs",
        },
        12,
        fetch_page_with_alternates=fetch_page_with_alternates,
        fetch_page=fetch_page,
        fetch_text=lambda _url, _timeout_s: "",
        html_extractor=html_extractor,
        parse_jobpostings_from_html=parse_jobpostings_from_html,
        normalize_job_url=admin_bridge.normalize_job_url,
        source_identity=source_identity,
        suggest_alternate_career_urls=lambda _url: [],
    )

    assert ok is False
    assert jobs_found == 0
    assert weak_signal is False
    assert error == "no job postings found"


def test_static_source_check_does_not_count_hash_link_on_jobish_alternate_page() -> None:
    def fetch_page_with_alternates(_url: str, _timeout_s: int) -> tuple[str, str, bool, bool, str]:
        return (
            '<html><body><a href="#">Menu</a></body></html>',
            "",
            False,
            False,
            ("https://beamdog.example/careers"),
        )

    ok, jobs_found, error, weak_signal, _meta = source_checker.check_static_source(
        {
            "name": "Beamdog",
            "studio": "Beamdog",
            "adapter": "static",
            "pages": ["https://beamdog.example/vacancies"],
            "listing_url": "https://beamdog.example/vacancies",
        },
        12,
        fetch_page_with_alternates=fetch_page_with_alternates,
        fetch_page=lambda _url, _timeout_s: ("", "", False, False),
        fetch_text=lambda _url, _timeout_s: "",
        html_extractor=html_extractor,
        parse_jobpostings_from_html=parse_jobpostings_from_html,
        normalize_job_url=admin_bridge.normalize_job_url,
        source_identity=source_identity,
        suggest_alternate_career_urls=lambda _url: [],
    )

    assert ok is False
    assert jobs_found == 0
    assert weak_signal is False
    assert error == "no job postings found"


def test_static_source_check_treats_empty_bamboohr_board_as_valid_zero_job_source() -> None:
    html = """
    <html><body>
      <meta property="og:image" content="https://beamdog.bamboohr.com/jobs/share_image/35">
      <p>There are currently no openings.</p>
    </body></html>
    """

    ok, jobs_found, error, weak_signal, _meta = source_checker.check_static_source(
        {
            "name": "Beamdog",
            "studio": "Beamdog",
            "adapter": "static",
            "pages": ["https://beamdog.bamboohr.com/careers"],
            "listing_url": "https://beamdog.bamboohr.com/careers",
        },
        12,
        fetch_page_with_alternates=lambda _url, _timeout_s: (html, "", False, False, ""),
        fetch_page=lambda _url, _timeout_s: ("", "", False, False),
        fetch_text=lambda _url, _timeout_s: "",
        html_extractor=html_extractor,
        parse_jobpostings_from_html=parse_jobpostings_from_html,
        normalize_job_url=admin_bridge.normalize_job_url,
        source_identity=source_identity,
        suggest_alternate_career_urls=lambda _url: [],
    )

    assert ok is True
    assert jobs_found == 0
    assert weak_signal is False
    assert error == ""


def test_static_source_check_prefers_empty_primary_page_over_alternate_errors() -> None:
    def fetch_page_with_alternates(url: str, _timeout_s: int) -> tuple[str, str, bool, bool, str]:
        if url == "https://beamdog.example/careers":
            return "<html><body>No current openings</body></html>", "", False, False, ""
        return "", f"{url}: DNS failed", False, False, ""

    ok, jobs_found, error, weak_signal, _meta = source_checker.check_static_source(
        {
            "name": "Beamdog",
            "studio": "Beamdog",
            "adapter": "static",
            "pages": ["https://beamdog.example/careers"],
            "listing_url": "https://beamdog.example/careers",
        },
        12,
        fetch_page_with_alternates=fetch_page_with_alternates,
        fetch_page=lambda _url, _timeout_s: ("", "", False, False),
        fetch_text=lambda _url, _timeout_s: "",
        html_extractor=html_extractor,
        parse_jobpostings_from_html=parse_jobpostings_from_html,
        normalize_job_url=admin_bridge.normalize_job_url,
        source_identity=source_identity,
        suggest_alternate_career_urls=lambda _url: ["https://jobs.beamdog.example"],
    )

    assert ok is False
    assert jobs_found == 0
    assert weak_signal is False
    assert error == "no job postings found"


def test_static_source_check_ignores_greenhouse_open_application_link() -> None:
    html = """
    <html><body>
      <a href="https://job-boards.greenhouse.io/azragames/jobs/4978306007">Apply now</a>
      <a href="https://boards.greenhouse.io/azragamesoa/jobs/4345814007">Submit Your Application</a>
    </body></html>
    """

    def fetch_page_with_alternates(_url: str, _timeout_s: int) -> tuple[str, str, bool, bool, str]:
        return html, "", False, False, ""

    ok, jobs_found, error, weak_signal, _meta = source_checker.check_static_source(
        {
            "name": "Azra Games",
            "studio": "Azra Games",
            "adapter": "static",
            "pages": ["https://azragames.com/careers/#opening"],
            "listing_url": "https://azragames.com/careers/#opening",
        },
        12,
        fetch_page_with_alternates=fetch_page_with_alternates,
        fetch_page=lambda _url, _timeout_s: ("", "", False, False),
        fetch_text=lambda _url, _timeout_s: "",
        html_extractor=html_extractor,
        parse_jobpostings_from_html=parse_jobpostings_from_html,
        normalize_job_url=admin_bridge.normalize_job_url,
        source_identity=source_identity,
        suggest_alternate_career_urls=lambda _url: [],
    )

    assert ok is True
    assert jobs_found == 1
    assert weak_signal is True
    assert error == ""


def test_static_source_check_does_not_count_repeated_lever_board_links_as_jobs() -> None:
    html = """
    <html><body>
      <a href="https://jobs.lever.co/bigtime">WORK WITH US</a>
      <a href="https://jobs.lever.co/bigtime">EXPLORE A CAREER</a>
      <a href="https://jobs.lever.co/bigtime">OPEN POSITIONS</a>
    </body></html>
    """

    ok, jobs_found, error, weak_signal, _meta = source_checker.check_static_source(
        {
            "name": "Big Time Studios",
            "studio": "Big Time Studios",
            "adapter": "static",
            "pages": ["https://www.bigtime.gg/careers"],
            "listing_url": "https://www.bigtime.gg/careers",
        },
        12,
        fetch_page_with_alternates=lambda _url, _timeout_s: (html, "", False, False, ""),
        fetch_page=lambda _url, _timeout_s: ("", "", False, False),
        fetch_text=lambda _url, _timeout_s: "",
        html_extractor=html_extractor,
        parse_jobpostings_from_html=parse_jobpostings_from_html,
        normalize_job_url=admin_bridge.normalize_job_url,
        source_identity=source_identity,
        suggest_alternate_career_urls=lambda _url: [],
    )

    assert ok is False
    assert jobs_found == 0
    assert weak_signal is False
    assert error == "no job postings found"
