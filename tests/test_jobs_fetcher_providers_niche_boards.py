"""Tests for jobs fetcher providers niche job boards."""

from src import jobs_fetcher as jf
from tests.helpers.job_fixtures import _fixture


def test_parse_gamejobs_html_fixture() -> None:
    rows = jf.parse_gamejobs_html(_fixture("gamejobs.html"), base_url="https://gamejobs.co/")
    assert len(rows) == 2
    assert rows[0]["company"] == "Pixel Forge"
    assert any(row["workType"] == "Remote" for row in rows)


def test_run_gamejobs_source_paginates_search_pages() -> None:
    page_one = """
        <html><body>
          <a href="/jobs/lead-gameplay-programmer">Lead Gameplay Programmer</a>
          <a href="/companies/pixel-forge">Pixel Forge</a>
          <a href="/locations/amsterdam-netherlands">Amsterdam, Netherlands</a>
          <a href="/jobs/technical-artist">Technical Artist</a>
          <a href="/companies/nebula-games">Nebula Games</a>
          <a href="/locations/worldwide-remote">Worldwide Remote</a>
        </body></html>
        """
    page_two = """
        <html><body>
          <a href="/jobs/economy-designer">Economy Designer</a>
          <a href="/companies/rainfall-interactive">Rainfall Interactive</a>
          <a href="/locations/london-united-kingdom">London, United Kingdom</a>
          <a href="/jobs/lead-gameplay-programmer">Lead Gameplay Programmer</a>
          <a href="/companies/pixel-forge">Pixel Forge</a>
          <a href="/locations/amsterdam-netherlands">Amsterdam, Netherlands</a>
        </body></html>
        """
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
