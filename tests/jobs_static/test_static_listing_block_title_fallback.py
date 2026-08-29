"""Tests for the generic block-title list-only fallback in the static listing runner.

When a block-structured listing parses job-title-looking headings but exposes no per-role
links (and the generic JSON-LD / rendered-card / detail-link paths find nothing), the runner
emits query-anchored rows so list-only boards recover without a per-host plugin.
"""

from __future__ import annotations

from typing import Any

import pytest

from src.jobs.adapters.plugins.static._runner import static_listing_anchor_link
from src.jobs.adapters.static_listing_rows import _job_like_heading_titles
from src.jobs.adapters.static_sources import run_static_studio_pages_source


def _studio_row(name: str, page_url: str) -> dict[str, Any]:
    return {
        "name": name,
        "studio": name,
        "adapter": "static",
        "company": name,
        "pages": [page_url],
        "enabledByDefault": True,
    }


# --- pure heading-scan unit tests -----------------------------------------------------


def test_heading_scan_extracts_job_like_titles_only() -> None:
    html = """
    <html><body>
    <h1>Careers at Example</h1>
    <h2>Open Roles</h2>
    <h3>Senior Game Designer</h3>
    <h3>3D Environment Artist</h3>
    <h3>We're Hiring Creative Designers</h3>
    <h3>Join Our Team of Programmers</h3>
    <h4>Technical Animator</h4>
    <h3>About Us</h3>
    <script>var t = '<h3>Fake Role In Script</h3>';</script>
    <style>.x h3 { color: red; }</style>
    </body></html>
    """
    assert _job_like_heading_titles(html) == [
        "Senior Game Designer",
        "3D Environment Artist",
        "Technical Animator",
    ]


def test_heading_scan_dedupes_identical_titles() -> None:
    html = (
        "<h3>Game Programmer</h3><h4>Game Programmer</h4><h3>Game Programmer</h3>"
        "<h3>Level Designer</h3>"
    )
    assert _job_like_heading_titles(html) == ["Game Programmer", "Level Designer"]


def test_heading_scan_skips_headers_without_role_tokens() -> None:
    html = "<h2>About</h2><h2>Our Team</h2><h2>Contact</h2><h2>FAQ</h2>"
    assert _job_like_heading_titles(html) == []


def test_heading_scan_skips_section_header_phrases() -> None:
    html = "<h2>Open Roles</h2><h2>Current Openings</h2><h2>Now Hiring</h2><h2>Join Our Team</h2>"
    assert _job_like_heading_titles(html) == []


# --- end-to-end runner tests ----------------------------------------------------------


def test_fallback_recovers_heading_only_listing() -> None:
    listing = """
    <html><body>
    <h1>Careers at Fallback Studio</h1>
    <h2>Open Positions</h2>
    <h3>Senior Game Designer</h3>
    <h3>3D Artist</h3>
    <h3>Technical Programmer</h3>
    </body></html>
    """

    def fake_fetch(url: str, _: int) -> str:
        assert url == "https://example.net/careers"
        return listing

    rows = run_static_studio_pages_source(
        fetch_text=fake_fetch,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        sources=[_studio_row("Fallback Studio", "https://example.net/careers")],
    )
    assert [r["title"] for r in rows] == [
        "Senior Game Designer",
        "3D Artist",
        "Technical Programmer",
    ]
    assert [r["jobLink"] for r in rows] == [
        "https://example.net/careers?static-role=senior-game-designer",
        "https://example.net/careers?static-role=3d-artist",
        "https://example.net/careers?static-role=technical-programmer",
    ]
    assert len({r["sourceJobId"] for r in rows}) == 3
    assert all(r["adapter"] == "static" for r in rows)
    assert all(r["studio"] == "Fallback Studio" for r in rows)


def test_fallback_single_heading_yields_no_rows() -> None:
    # A single job-like heading is more likely a page/hero header than a listing, so the
    # fallback must not fire; the empty generic source surfaces as a fetch error.
    from src.exceptions import AdapterValidationError

    listing = "<html><body><h2>We Are Hiring Game Designers</h2></body></html>"

    def fake_fetch(url: str, _: int) -> str:
        return listing

    with pytest.raises(AdapterValidationError, match="no jobs extracted"):
        run_static_studio_pages_source(
            fetch_text=fake_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            sources=[_studio_row("Single Studio", "https://example.net/careers")],
        )


def test_fallback_not_used_when_detail_links_exist() -> None:
    listing = """
    <html><body>
    <h2>Open Positions</h2>
    <h3>Engine Programmer</h3>
    <div class="job-listing-item"><a href="/job/engine-programmer">Engine Programmer</a></div>
    </body></html>
    """
    detail = "<html><body><h1>Engine Programmer</h1></body></html>"

    def fake_fetch(url: str, _: int) -> str:
        if url == "https://example.net/careers":
            return listing
        if url == "https://example.net/job/engine-programmer":
            return detail
        raise AssertionError(f"Unexpected URL: {url}")

    rows = run_static_studio_pages_source(
        fetch_text=fake_fetch,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        sources=[_studio_row("Detail Studio", "https://example.net/careers")],
    )
    # The real detail link wins; no query-anchored fallback rows may be emitted.
    assert len(rows) == 1
    assert rows[0]["jobLink"] == "https://example.net/job/engine-programmer"
    assert "static-role=" not in rows[0]["jobLink"]


def test_fallback_rows_use_query_anchors_that_survive_normalization() -> None:
    from src.jobs.text_utils import normalize_url

    link = static_listing_anchor_link("https://example.net/careers/", "Senior Game Designer")
    assert normalize_url(link) == "https://example.net/careers?static-role=senior-game-designer"
