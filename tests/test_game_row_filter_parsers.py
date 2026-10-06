"""The game row filter, pinned at every parser that gained it.

The asymmetry this closes: `looks_like_game_job` ran on the JSON-feed providers but not on
the HTML-board providers, the structured listing parsers, the community boards, or the
static lanes -- so a studio's back-office roles could reach the feed from those paths
(measured: 4,214 of 6,887 static rows in the live feed failed it, and one greenhouse board
was read as collecting its whole board).

The Google Sheets path is deliberately **exempt**: its rows carry the sheet's own
``Game``/``Tech`` sector and the default feed intentionally includes Tech-sector sheet rows
(see docs/snapshots/non-game-employer-evidence-2026-08-12.md). Applying the game filter
there would delete the product's Tech half, which is why the exemption is pinned here next
to the filters.
"""

from __future__ import annotations

from src import jobs_fetcher as jf
from src.jobs.adapters.community import parse_gamejobs_html
from src.jobs.adapters.html_parsers import _append_gamesindustry_job
from src.jobs.adapters.parsers.json_payloads import parse_greenhouse_jobs_payload
from src.jobs.adapters.parsers.phenom import parse_phenom_jobs_html
from src.jobs.adapters.parsers.provider_html import (
    parse_ashby_jobs_from_html,
    parse_breezy_jobs_html,
    parse_jazzhr_jobs_html,
)
from src.jobs.adapters.parsers.structured_listing import parse_bamboohr_jobs_html

# --- provider parsers -----------------------------------------------------------------


def _greenhouse(title: str) -> list[dict[str, object]]:
    return parse_greenhouse_jobs_payload(
        {
            "jobs": [
                {
                    "id": 1,
                    "title": title,
                    "company_name": "Acme Studios",
                    "absolute_url": "https://boards.greenhouse.io/acme/jobs/1",
                    "location": {"name": "Berlin, DE"},
                }
            ]
        },
        "acme",
    )


def test_greenhouse_payload_keeps_game_rows_and_drops_back_office_rows() -> None:
    assert len(_greenhouse("Senior Gameplay Engineer")) == 1
    assert _greenhouse("Senior Accountant") == []


def _ashby(title: str) -> list[dict[str, object]]:
    html = (
        "<html><body>"
        '<a href="https://jobs.ashbyhq.com/acme/123e4567-e89b-12d3-a456-426614174000">'
        f"{title}</a>"
        "</body></html>"
    )
    return parse_ashby_jobs_from_html(html, "https://jobs.ashbyhq.com/acme", "Acme")


def test_ashby_html_keeps_game_rows_and_drops_back_office_rows() -> None:
    assert len(_ashby("Game Designer")) == 1
    assert _ashby("Senior Accountant") == []


def _breezy(title: str) -> list[dict[str, object]]:
    html = (
        '<section><li class="position">'
        '<a href="/p/abc123-job"><h2>'
        f"{title}"
        '</h2><ul class="meta"><li class="location"><span>Seattle, US</span></li></ul></a>'
        "</li></section>"
    )
    return parse_breezy_jobs_html(html, "https://acme.breezy.hr/", "Acme")


def test_breezy_html_keeps_game_rows_and_drops_back_office_rows() -> None:
    assert len(_breezy("Game Designer")) == 1
    assert _breezy("Office Manager") == []


def _jazzhr(title: str) -> list[dict[str, object]]:
    html = (
        f'<a href="https://acme.applytojob.com/apply/xyz/{title.replace(" ", "-")}">{title}</a>'
        "<div>Berlin, Germany</div><div>Full Time</div>"
    )
    return parse_jazzhr_jobs_html(html, "https://acme.applytojob.com/apply", "Acme")


def test_jazzhr_html_keeps_game_rows_and_drops_back_office_rows() -> None:
    assert len(_jazzhr("Game Designer")) == 1
    assert _jazzhr("Office Manager") == []


def test_structured_listing_keeps_game_rows_and_drops_back_office_rows() -> None:
    def rows(title: str) -> list[dict[str, object]]:
        html = f'<a href="/jobs/view/{title.lower().replace(" ", "-")}">{title}</a>'
        jobs, _next_pages = parse_bamboohr_jobs_html(
            html, "https://acme.bamboohr.com/careers", fallback_company="Acme"
        )
        return jobs

    assert len(rows("Gameplay Programmer")) == 1
    assert rows("Senior Accountant") == []


def test_phenom_keeps_game_rows_and_drops_back_office_rows() -> None:
    def rows(title: str) -> list[dict[str, object]]:
        html = (
            '<table id="searchresults"><tr><td>'
            f'<a class="jobTitle-link" href="/Acme/job/slug/123/">{title}</a>'
            "</td></tr></table>"
        )
        jobs, _next_pages = parse_phenom_jobs_html(
            html, "https://jobsearch.createyourowncareer.com/Acme/", fallback_company="Acme"
        )
        return jobs

    assert len(rows("Game Designer")) == 1
    assert rows("Senior Accountant") == []


# --- community boards -----------------------------------------------------------------


def test_gamejobs_keeps_game_cards_and_drops_back_office_cards() -> None:
    def rows(title: str) -> list[dict[str, object]]:
        html = (
            f'<div class="job"><a class="title" href="/jobs/one">{title}</a><div>'
            '<a href="/search?c=Acme" class="c">Acme</a> · '
            '<a href="/search?w=Remote" class="w">Remote</a></div></div>'
        )
        return parse_gamejobs_html(html, base_url="https://gamejobs.co/")

    assert len(rows("Game Designer")) == 1
    assert rows("Office Manager") == []


def test_gamesindustry_row_helper_applies_the_filter() -> None:
    jobs: list[dict[str, object]] = []
    seen: set[str] = set()
    _append_gamesindustry_job(
        jobs,
        seen,
        {
            "title": "Game Designer",
            "company": "Acme",
            "jobLink": "https://jobs.gamesindustry.biz/job/game-designer-1",
        },
    )
    _append_gamesindustry_job(
        jobs,
        seen,
        {
            "title": "Office Manager",
            "company": "Acme",
            "jobLink": "https://jobs.gamesindustry.biz/job/office-manager-2",
        },
    )
    assert [row["title"] for row in jobs] == ["Game Designer"]


# --- the Sheets exemption -------------------------------------------------------------


def test_google_sheets_rows_keep_their_own_sector_without_the_game_filter() -> None:
    """A Tech-sector sheet row is product content, not pollution: it stays."""
    csv_text = (
        "Company,City,Country,Job Title,Sector,Link\n"
        "Zscaler,Remote,US,Staff Site Reliability Engineer,Tech,https://example.com/jobs/1\n"
        "Pixel Forge,Amsterdam,NL,Gameplay Programmer,Game,https://example.com/jobs/2\n"
    )
    rows = jf.parse_google_sheets_csv(csv_text)
    assert len(rows) == 2
    assert {row["sector"] for row in rows} == {"Tech", "Game"}
