from __future__ import annotations

from src.jobs.adapters.parsers.structured_listing import parse_bamboohr_jobs_html


def test_bamboohr_parser_does_not_count_board_root_as_job() -> None:
    html = """
    <a href="https://beamdog.bamboohr.com/careers">View Current Openings</a>
    <a href="/jobs">Jobs</a>
    """

    jobs, next_pages = parse_bamboohr_jobs_html(
        html,
        "https://beamdog.bamboohr.com/careers",
        fallback_company="Beamdog",
    )

    assert jobs == []
    assert next_pages == []


def test_bamboohr_parser_keeps_real_job_links() -> None:
    html = '<a href="/jobs/view/gameplay-programmer">Gameplay Programmer</a>'

    jobs, _next_pages = parse_bamboohr_jobs_html(
        html,
        "https://beamdog.bamboohr.com/careers",
        fallback_company="Beamdog",
    )

    assert len(jobs) == 1
    assert jobs[0]["title"] == "Gameplay Programmer"
    assert jobs[0]["jobLink"] == "https://beamdog.bamboohr.com/jobs/view/gameplay-programmer"
