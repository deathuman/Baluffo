from __future__ import annotations

from collections.abc import Callable

import pytest

import src.admin_bridge as admin_bridge
from src.bridge import html_extractor, source_checker
from src.bridge.source_check_api import _reconstruct_probe_candidate
from src.jobs.adapters.html_parsers import parse_jobpostings_from_html
from src.source_registry import source_identity


def _check_static_source_for_html(
    html: str,
    *,
    fetch_text: Callable[[str, int], str],
) -> tuple[bool, int, str, bool, dict[str, object]]:
    return source_checker.check_static_source(
        {
            "name": "Example Studio",
            "studio": "Example Studio",
            "adapter": "static",
            "pages": ["https://example.com/careers"],
            "listing_url": "https://example.com/careers",
        },
        12,
        fetch_page_with_alternates=lambda _url, _timeout_s: (html, "", False, False, ""),
        fetch_page=lambda _url, _timeout_s: ("", "", False, False),
        fetch_text=fetch_text,
        html_extractor=html_extractor,
        parse_jobpostings_from_html=parse_jobpostings_from_html,
        normalize_job_url=admin_bridge.normalize_job_url,
        source_identity=source_identity,
        suggest_alternate_career_urls=lambda _url: [],
    )


def test_provider_source_check_reconstructs_endpoint_from_compact_source_id() -> None:
    row = {
        "id": "smartrecruiters:company_id:cdprojektred",
        "adapter": "smartrecruiters",
        "name": "CD PROJEKT RED (SmartRecruiters)",
    }

    reconstructed = _reconstruct_probe_candidate(row)

    assert reconstructed["company_id"] == "cdprojektred"
    assert (
        reconstructed["api_url"]
        == "https://api.smartrecruiters.com/v1/companies/cdprojektred/postings"
    )


def test_static_source_check_embedded_fetch_records_expected_network_errors() -> None:
    html = """
    <html><body>
      <a href="https://studio.jobs.personio.de/search.json">Personio</a>
      <a href="https://apply.workable.com/selfstudio/">Workable</a>
      <script src="https://cdn.jobylon.com/embedder.js"></script>
      <script>jbl_company_id = 123; jbl_version = "v2";</script>
    </body></html>
    """
    attempted_urls: list[str] = []

    def fetch_text(url: str, _timeout_s: int) -> str:
        attempted_urls.append(url)
        raise RuntimeError(f"Network error for {url}: down")

    ok, jobs_found, error, weak_signal, _meta = _check_static_source_for_html(
        html,
        fetch_text=fetch_text,
    )

    assert ok is True
    assert jobs_found >= 3
    assert weak_signal is True
    assert error == ""
    assert "https://studio.jobs.personio.de/search.json" in attempted_urls
    assert "https://apply.workable.com/api/v1/widget/accounts/selfstudio?details=true" in (
        attempted_urls
    )
    assert (
        "https://cdn.jobylon.com/jobs/companies/123/embed/v2?page_size=30&target=jobylon-jobs-widget"
        in attempted_urls
    )


@pytest.mark.parametrize(
    ("html", "expected_url_fragment"),
    [
        (
            '<a href="https://studio.jobs.personio.de/search.json">Personio</a>',
            "studio.jobs.personio.de/search.json",
        ),
        (
            '<a href="https://apply.workable.com/selfstudio/">Workable</a>',
            "apply.workable.com/api/v1/widget/accounts/selfstudio",
        ),
        (
            """
            <script src="https://cdn.jobylon.com/embedder.js"></script>
            <script>jbl_company_id = 123; jbl_version = "v2";</script>
            """,
            "cdn.jobylon.com/jobs/companies/123",
        ),
    ],
)
def test_static_source_check_embedded_fetch_does_not_hide_unexpected_runtime_errors(
    html: str,
    expected_url_fragment: str,
) -> None:
    def fetch_text(url: str, _timeout_s: int) -> str:
        raise RuntimeError(f"Unexpected URL: {url}")

    with pytest.raises(RuntimeError, match=f"Unexpected URL: .*{expected_url_fragment}"):
        _check_static_source_for_html(html, fetch_text=fetch_text)


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


def test_static_source_check_ignores_recruitee_no_job_bucket() -> None:
    html = """
    <html><body>
      <a href="https://careers.blooberteam.com/o/senior-ai-gameplay-programmer">
        Senior AI Gameplay Programmer
      </a>
      <a href="https://careers.blooberteam.com/o/no-job-that-suits-you">
        No Job that suits you?
      </a>
    </body></html>
    """

    ok, jobs_found, error, weak_signal, _meta = source_checker.check_static_source(
        {
            "name": "Bloober Team",
            "studio": "Bloober Team",
            "adapter": "static",
            "pages": ["https://careers.blooberteam.com/jobs"],
            "listing_url": "https://careers.blooberteam.com/jobs",
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
    assert jobs_found == 1
    assert weak_signal is True
    assert error == ""


def test_static_source_check_counts_elevato_comma_job_links() -> None:
    html = """
    <html><body>
      <a href="https://qloc.elevato.net/en/translator-proofreader,j,242">
        Translator / Proofreader
      </a>
      <a href="https://qloc.elevato.net/en/technical-artist,j,240?source=10">
        Technical Artist
      </a>
      <a href="https://q-loc.com/privacy-policy/personal-data-processing/">Privacy</a>
      <a href="https://qloc.elevato.net/en/join-qloc,j,83">Join QLOC!</a>
      <a href="https://qloc.elevato.net/en/job-offers,j">Show all job offers</a>
    </body></html>
    """

    ok, jobs_found, error, weak_signal, _meta = source_checker.check_static_source(
        {
            "name": "QLOC (Sheet)",
            "studio": "QLOC",
            "company": "QLOC",
            "adapter": "static",
            "pages": ["https://qloc.elevato.net/en/"],
            "listing_url": "https://qloc.elevato.net/en/",
        },
        12,
        fetch_page_with_alternates=lambda _url, _timeout_s: (html, "", False, False, ""),
        fetch_page=lambda _url, _timeout_s: ("<html><body></body></html>", "", False, False),
        fetch_text=lambda _url, _timeout_s: "",
        html_extractor=html_extractor,
        parse_jobpostings_from_html=parse_jobpostings_from_html,
        normalize_job_url=admin_bridge.normalize_job_url,
        source_identity=source_identity,
        suggest_alternate_career_urls=lambda _url: [],
    )

    assert ok is True
    assert jobs_found == 2
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
