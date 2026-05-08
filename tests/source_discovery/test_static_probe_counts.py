from __future__ import annotations

from src.source_discovery import probe


def test_static_probe_ignores_career_section_and_generic_application_links() -> None:
    candidate = {"adapter": "static", "listing_url": "https://azragames.com/careers/"}
    html = """
    <a href="/careers/">Careers</a>
    <a href="/careers/#opening">Openings</a>
    <a href="https://azragames.com/careers/#jobs">Jobs</a>
    <a href="https://job-boards.greenhouse.io/azragames/jobs/4978306007">
      Senior Unity Gameplay Capture Artist
    </a>
    <a href="https://boards.greenhouse.io/azragamesoa/jobs/4345814007">
      Submit Your Application
    </a>
    """

    ok, count, error = probe.probe_candidate(candidate, timeout_s=5, fetcher=lambda *_: html)

    assert ok
    assert count == 1
    assert error == ""
