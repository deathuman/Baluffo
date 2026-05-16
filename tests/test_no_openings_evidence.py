from __future__ import annotations

from src.jobs.adapters.plugins.static import _heuristics
from src.jobs.page_gating import classify_job_page
from src.source_discovery import probe


def test_visible_no_openings_marker_is_shared_across_callers() -> None:
    html = "<main><h1>Careers</h1><p>There are currently no open positions.</p></main>"

    assert _heuristics.detect_no_openings(html)
    assert classify_job_page(html, "https://studio.example/careers") == (
        False,
        "no_openings",
    )

    evidence = probe.static_probe_evidence(html, "https://studio.example/careers")
    assert evidence.count == 0
    assert evidence.confidence == "high"
    assert evidence.reason == "no_openings"


def test_no_openings_marker_ignores_script_template_and_hidden_text() -> None:
    html = """
    <main><h1>Careers</h1><p>Open applications are reviewed monthly.</p></main>
    <script>window.copy = "There are currently no open positions";</script>
    <template>No jobs found</template>
    <div style="display:none">0 jobs</div>
    """

    assert not _heuristics.detect_no_openings(html)

    evidence = probe.static_probe_evidence(html, "https://studio.example/careers")
    assert evidence.reason == "no_jobs"


def test_weak_zero_result_marker_requires_job_context() -> None:
    assert not _heuristics.detect_no_openings("<main><p>0 results</p></main>")
    assert _heuristics.detect_no_openings("<main><h1>Careers</h1><p>0 jobs</p></main>")


def test_filter_empty_copy_does_not_override_real_job_links() -> None:
    html = """
    <main>
      <a href="https://jobs.ashbyhq.com/studio/8615ea53-9992-489f-b2cd-38ede3434679">Principal Rendering Engineer</a>
      <a href="https://jobs.ashbyhq.com/studio/393927f5-29cd-492c-b091-7a5eaeab7284">Lighting Artist</a>
      <div>Sorry no jobs match your search...</div>
    </main>
    """

    assert not _heuristics.detect_no_openings(html)
    assert classify_job_page(html, "https://studio.example/careers") == (
        True,
        "job_listing_anchors",
    )


def test_page_gating_rejects_visible_no_openings_before_stale_jsonld() -> None:
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

    assert classify_job_page(html, "https://studio.example/careers") == (
        False,
        "no_openings",
    )
