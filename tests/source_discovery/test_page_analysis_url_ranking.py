from __future__ import annotations

from src.source_discovery.page_analysis import extract_explicit_careers_url_from_page


def test_extract_explicit_careers_url_from_page_prefers_jobs_listing_over_broad_careers() -> None:
    html = """
    <a href="/careers/">Careers</a>
    <a href="/careers/people/">People</a>
    <a href="/careers/life/">Life</a>
    <a href="/careers/jobs/">Jobs</a>
    """

    careers_url = extract_explicit_careers_url_from_page(
        "https://studio.example.com/",
        html,
        studio="Example Studio",
        nl_priority=False,
        discovery_method="gamesmap",
    )

    assert careers_url == "https://studio.example.com/careers/jobs/"
