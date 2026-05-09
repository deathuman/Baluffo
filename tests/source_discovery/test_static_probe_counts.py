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
    assert candidate["lastProbeCountConfidence"] == "high"
    assert candidate["lastReliableJobsFound"] == 1


def test_static_probe_does_not_count_stale_jsonld_when_page_says_no_openings() -> None:
    candidate = {"adapter": "static", "listing_url": "https://studio.example/careers/"}
    html = """
    <main>
      <h1>Careers</h1>
      <p>There are currently no open positions.</p>
      <script type="application/ld+json">
      {
        "@context": "https://schema.org",
        "@type": "JobPosting",
        "title": "Gameplay Engineer",
        "url": "https://studio.example/jobs/gameplay-engineer"
      }
      </script>
    </main>
    """

    ok, count, error = probe.probe_candidate(candidate, timeout_s=5, fetcher=lambda *_: html)

    assert ok
    assert count == 0
    assert error == ""
    assert candidate["lastProbeWeakSignal"] is True
    assert candidate["lastProbeCountReason"] == "no_openings_overrides_jsonld"
    assert candidate["lastReliableJobsFound"] == 0


def test_static_probe_ignores_hidden_template_job_links() -> None:
    candidate = {"adapter": "static", "listing_url": "https://studio.example/careers/"}
    html = """
    <section hidden>
      <a href="/jobs/one">Gameplay Engineer</a>
      <a href="/jobs/two">Tools Engineer</a>
    </section>
    <div style="display:none">
      <a href="/jobs/three">Producer</a>
    </div>
    <a href="/jobs/live">Live Operations Engineer</a>
    """

    ok, count, error = probe.probe_candidate(candidate, timeout_s=5, fetcher=lambda *_: html)

    assert ok
    assert count == 1
    assert error == ""


def test_static_probe_counts_same_listing_detail_pages() -> None:
    candidate = {
        "adapter": "static",
        "listing_url": "https://studio.example/work-with-us/index.html",
    }
    html = """
    <a href="/work-with-us/4023614009/">Systems Engineer</a>
    <a href="/work-with-us/4023591009/">Lead Environment Artist</a>
    <a href="/work-with-us/94b98a86-d14e-49e5-b117-5b40bce17c9d/">HR Business Partner</a>
    <a href="/work-with-us/#benefits">Work settings</a>
    """

    ok, count, error = probe.probe_candidate(candidate, timeout_s=5, fetcher=lambda *_: html)

    assert ok
    assert count == 3
    assert error == ""
    assert candidate["lastProbeCountReason"] == "detail_links"
    assert candidate["lastReliableJobsFound"] == 3
