"""Tests for jobs fetcher providers niche job boards."""

from src import jobs_fetcher as jf
from tests.helpers.job_fixtures import _fixture


def _gamejobs_card(title: str, slug: str, company: str, location: str) -> str:
    return (
        f'<div class="job"><a class="title" href="{slug}">{title}</a><div>'
        f'<a href="/search?c={company.replace(" ", "+")}" class="c">{company}</a> · '
        f'<a href="/search?w={location.replace(",", "%2C").replace(" ", "+")}" class="w">{location}</a>'
        " · 2 hours ago · <form class='form-inline save-job' method='post' action='/jobs/1/save'>"
        '<button class="btn-inline" type="submit">Save</button></form></div></div>'
    )


def test_parse_gamejobs_html_fixture() -> None:
    rows = jf.parse_gamejobs_html(_fixture("gamejobs.html"), base_url="https://gamejobs.co/")
    assert len(rows) == 2
    assert rows[0]["company"] == "Pixel Forge"
    assert rows[0]["title"] == "Senior Gameplay Programmer"
    assert rows[1]["company"] == "Nebula Games"
    assert any(row["workType"] == "Remote" for row in rows)
    assert all(not row["jobLink"].startswith("https://gamejobs.co/search") for row in rows)


def test_parse_gamejobs_html_ignores_search_directory_and_facet_artifacts() -> None:
    """The 'Apple 50'/'Wargaming 51' pagination-artifact class never becomes a row.

    Regression for the contaminated feed rows whose company/title were directory
    counts ('Digipen 47', 'Hasbro 168') and whose links were /search?c= pages.
    """
    html = """
        <html><body>
          <div><a href="/search?c=Apple" class="c">Apple 50</a> · <a href="/search?c=Wargaming" class="c">Wargaming 51</a> · <a href="/search?w=Remote" class="w">Remote</a></div>
          <a href="/search?t=Design" class="c">Design 510</a> · <a href="/search?e=Graduate" class="c">Graduate 17</a>
          <a href="/search?page=2">Next</a>
          <div class="job"><a class="title" href="/lead-producer-at-real-studio">Lead Producer</a><div><a href="/search?c=Real&#43;Studio" class="c">Real Studio</a> · <a href="/search?w=Remote" class="w">Remote</a></div></div>
        </body></html>
        """
    rows = jf.parse_gamejobs_html(html, base_url="https://gamejobs.co/")
    assert [row["title"] for row in rows] == ["Lead Producer"]
    assert [row["company"] for row in rows] == ["Real Studio"]
    assert rows[0]["jobLink"] == "https://gamejobs.co/lead-producer-at-real-studio"


def test_run_gamejobs_source_paginates_search_pages() -> None:
    page_one = """
        <html><body>
          {}
          {}
          <div class="job"><a class="title" href="/jobs/lead-gameplay-programmer">Lead Gameplay Programmer</a><div><a href="/search?w=Remote" class="w">Remote</a> · Next</div></div>
        </body></html>
        """.format(
        _gamejobs_card(
            "Senior Gameplay Programmer",
            "/jobs/senior-gameplay-programmer",
            "Pixel Forge",
            "Amsterdam, Netherlands",
        ),
        _gamejobs_card(
            "Technical Artist", "/jobs/technical-artist", "Nebula Games", "Worldwide Remote"
        ),
    )
    page_two = """
        <html><body>
          {}
          <div class="job"><a class="title" href="/jobs/lead-gameplay-programmer">Lead Gameplay Programmer</a><div><a href="/search?w=Remote" class="w">Remote</a></div></div>
        </body></html>
        """.format(
        _gamejobs_card(
            "Economy Designer",
            "/jobs/economy-designer",
            "Rainfall Interactive",
            "London, United Kingdom",
        ),
    )
    seen_urls: list[str] = []

    def fake_fetch_text(url: str, timeout: int) -> str:
        _ = timeout
        seen_urls.append(url)
        if url == "https://gamejobs.co/":
            return page_one
        if url == "https://gamejobs.co/search?page=2":
            return page_two
        if url == "https://gamejobs.co/search?page=3":
            return "<html><body>No jobs</body></html>"
        raise AssertionError(f"unexpected url {url}")

    rows = jf.run_gamejobs_source(fetch_text=fake_fetch_text, timeout_s=5, retries=0, backoff_s=0)
    assert len(rows) == 3
    assert any(row["title"] == "Economy Designer" for row in rows)
    assert seen_urls[:3] == [
        "https://gamejobs.co/",
        "https://gamejobs.co/search?page=2",
        "https://gamejobs.co/search?page=3",
    ]


def test_run_gamejobs_source_skips_nav_only_page_and_still_paginates() -> None:
    """A nav-only page (real incident shape: hundreds of /search links, no cards)
    contributes zero rows — and never aborts the source's pagination."""
    seen_urls: list[str] = []

    def fake_fetch_text(url: str, timeout: int) -> str:
        _ = timeout
        seen_urls.append(url)
        if url == "https://gamejobs.co/":
            return (
                '<html><body><a href="/search?c=Apple" class="c">Apple 50</a> · '
                '<a href="/search?c=Wargaming" class="c">Wargaming 51</a> · '
                '<a href="/search?page=2">Next</a></body></html>'
            )
        if url == "https://gamejobs.co/search?page=2":
            return '<html><body>{}<a href="/search?page=3">Next</a></body></html>'.format(
                _gamejobs_card(
                    "Lead Producer", "/lead-producer-at-real-studio", "Real Studio", "Remote"
                )
            )
        if url == "https://gamejobs.co/search?page=3":
            return "<html><body>No jobs</body></html>"
        raise AssertionError(f"unexpected url {url}")

    rows = jf.run_gamejobs_source(fetch_text=fake_fetch_text, timeout_s=5, retries=0, backoff_s=0)
    assert [row["title"] for row in rows] == ["Lead Producer"]
    assert seen_urls[:2] == ["https://gamejobs.co/", "https://gamejobs.co/search?page=2"]


def test_parse_workwithindies_html_fixture() -> None:
    rows = jf.parse_workwithindies_html(
        _fixture("workwithindies.html"),
        base_url="https://www.workwithindies.com/",
    )
    assert len(rows) == 2
    assert rows[0]["company"] == "Moonshot Games"
    assert any(row["workType"] == "Remote" for row in rows)
    assert any(row["country"] == "CA" for row in rows)


def test_parse_8bitplay_html_fixture() -> None:
    rows = jf.parse_8bitplay_html(
        _fixture("8bitplay_jobs.html"),
        base_url="https://8bitplay.com/jobs/",
    )
    assert len(rows) == 2
    assert rows[0]["company"] == "Pixel Dominion"
    assert any(row["workType"] == "Remote" for row in rows)


def test_run_8bitplay_source_paginates_job_board_pages() -> None:
    page_one = _fixture("8bitplay_jobs.html")
    page_two = """
        <html><body>
          <a href="https://8bitplay.com/job/rendering-engineer/" class="post__similar-job">
            <div class="acf-job-board__top">
              <div class="acf-job-board__logo"><p class="acf-job-board__img-text">Nebula Forge</p></div>
              <h2 class="acf-job-board__props"><span>PC/Console</span><span>Europe</span></h2>
            </div>
            <h3 class="post__similar-job-title acf-jtw__title">Rendering Engineer</h3>
          </a>
        </body></html>
        """
    seen_urls: list[str] = []

    def fake_fetch_text(url: str, timeout: int) -> str:
        _ = timeout
        seen_urls.append(url)
        if url == "https://8bitplay.com/jobs/":
            return page_one
        if url == "https://8bitplay.com/jobs/?job-board-paged=2":
            return page_two
        if url == "https://8bitplay.com/jobs/?job-board-paged=3":
            return "<html><body>No more jobs</body></html>"
        raise AssertionError(f"unexpected url {url}")

    rows = jf.run_8bitplay_source(fetch_text=fake_fetch_text, timeout_s=5, retries=0, backoff_s=0)
    assert len(rows) == 3
    assert any(row["title"] == "Rendering Engineer" for row in rows)
    assert seen_urls[:3] == [
        "https://8bitplay.com/jobs/",
        "https://8bitplay.com/jobs/?job-board-paged=2",
        "https://8bitplay.com/jobs/?job-board-paged=3",
    ]


def test_parse_gracklehq_html_fixture() -> None:
    rows = jf.parse_gracklehq_html(
        _fixture("gracklehq_jobs.html"),
        base_url="https://gracklehq.com/jobs",
    )
    assert len(rows) == 2
    assert rows[0]["company"] == "Ubisoft"
    assert any(row["workType"] == "Remote" for row in rows)


def test_run_gracklehq_source_follows_next_pages() -> None:
    page_one = (
        _fixture("gracklehq_jobs.html")
        + '<a href="./jobs?pageidx=2" class="btn btn-default ">Next</a>'
    )
    page_two = """
        <html><body>
          <div class="joblisting">
            <a href="/rd/372395" target="_blank">Gameplay Programmer</a>
            <div>Robot Eclipse - Remote</div>
            <div class="bottomright">&lt;1d</div>
          </div>
        </body></html>
        """
    seen_urls: list[str] = []

    def fake_fetch_text(url: str, timeout: int) -> str:
        _ = timeout
        seen_urls.append(url)
        if url == "https://gracklehq.com/jobs":
            return page_one
        if url == "https://gracklehq.com/jobs?pageidx=2":
            return page_two
        raise AssertionError(f"unexpected url {url}")

    rows = jf.run_gracklehq_source(fetch_text=fake_fetch_text, timeout_s=5, retries=0, backoff_s=0)
    assert len(rows) == 3
    assert any(row["title"] == "Gameplay Programmer" for row in rows)
    assert seen_urls == [
        "https://gracklehq.com/jobs",
        "https://gracklehq.com/jobs?pageidx=2",
    ]


def test_run_gracklehq_source_stops_on_repeated_next_page() -> None:
    page_one = (
        _fixture("gracklehq_jobs.html")
        + '<a href="./jobs?pageidx=2" class="btn btn-default ">Next</a>'
    )
    page_two = """
        <html><body>
          <div class="joblisting">
            <a href="/rd/372395" target="_blank">Gameplay Programmer</a>
            <div>Robot Eclipse - Remote</div>
          </div>
          <a href="./jobs?pageidx=2" class="btn btn-default ">Next</a>
        </body></html>
        """
    seen_urls: list[str] = []

    def fake_fetch_text(url: str, timeout: int) -> str:
        _ = timeout
        seen_urls.append(url)
        if url == "https://gracklehq.com/jobs":
            return page_one
        if url == "https://gracklehq.com/jobs?pageidx=2":
            return page_two
        raise AssertionError(f"unexpected url {url}")

    rows = jf.run_gracklehq_source(fetch_text=fake_fetch_text, timeout_s=5, retries=0, backoff_s=0)
    assert len(rows) == 3
    assert seen_urls == [
        "https://gracklehq.com/jobs",
        "https://gracklehq.com/jobs?pageidx=2",
    ]
