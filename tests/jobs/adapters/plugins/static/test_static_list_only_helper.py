"""Tests for the shared list-only static-plugin helper (query-anchored roles)."""

from __future__ import annotations

import re
from typing import Any, cast

from src.jobs.adapters.plugins.static._runner import (
    SimpleStaticContext,
    static_list_only_job_rows,
    static_listing_anchor_link,
)


def _ctx(page_url: str, html: str) -> SimpleStaticContext:
    return SimpleStaticContext(
        page_url=page_url,
        html=html,
        source_row={"id": "example", "name": "Example Careers"},
        company="Example Studio",
        source_id="example",
    )


def test_static_listing_anchor_link_is_on_domain_and_distinct() -> None:
    link_a = static_listing_anchor_link("https://example.com/careers/", "Senior Game Designer")
    link_b = static_listing_anchor_link("https://example.com/careers/", "Character Artist")
    assert link_a == "https://example.com/careers/?static-role=senior-game-designer"
    assert link_b == "https://example.com/careers/?static-role=character-artist"
    assert link_a != link_b


def test_static_listing_anchor_link_survives_normalize_url() -> None:
    # The whole point of the query anchor: the pipeline normalizes URLs at several
    # stages (plugin-row dedup, canonicalization, dedup fingerprinting) and strips
    # fragments. A query parameter must survive so list-only roles stay distinct.
    from src.jobs.text_utils import normalize_url

    link = static_listing_anchor_link("https://example.com/careers/", "Senior Game Designer")
    assert normalize_url(link) == "https://example.com/careers?static-role=senior-game-designer"
    links = {
        static_listing_anchor_link("https://example.com/careers/", title)
        for title in ("Animators", "Rigging Artists", "Character Artists")
    }
    assert len({normalize_url(link) for link in links}) == 3


def test_static_listing_anchor_link_handles_ampersand_and_whitespace() -> None:
    assert static_listing_anchor_link("https://example.com/careers", "R&D Artists") == (
        "https://example.com/careers?static-role=randd-artists"
    )
    # Internal whitespace is preserved in the slug (same as the original upsurge helper).
    assert static_listing_anchor_link("https://example.com/careers/", "  Tech  Director ") == (
        "https://example.com/careers/?static-role=tech--director"
    )


def test_list_only_rows_extract_titles_and_anchor_queries() -> None:
    html = """
    <section class="CareerList">
      <section class="CareerSummary"><h3 class="CareerSummary__Title">Animators</h3>
        <table><tr><th>Job Description</th><td>Maya.</td></tr></table></section>
      <section class="CareerSummary"><h3 class="CareerSummary__Title">Rigging Artists</h3>
        <table><tr><th>Job Description</th><td>Skeletons.</td></tr></table></section>
    </section>
    """
    block_sep = re.compile(r'(?is)(?=class\s*=\s*["\']*CareerSummary\b)')
    title_re = re.compile(
        r'(?is)<h3[^>]*class\s*=\s*["\']*[^\s"\'<>]*CareerSummary__Title[^\s"\'<>]*["\']*[^>]*>(.*?)</h3>'
    )
    rows = static_list_only_job_rows(
        _ctx("https://upsurgestudios.com/careers/", html),
        block_sep=block_sep,
        title_re=title_re,
    )
    assert [r["title"] for r in rows] == ["Animators", "Rigging Artists"]
    assert [r["jobLink"] for r in rows] == [
        "https://upsurgestudios.com/careers/?static-role=animators",
        "https://upsurgestudios.com/careers/?static-role=rigging-artists",
    ]
    # Distinct sourceJobIds despite the same base URL (query-anchored rows).
    assert len({r["sourceJobId"] for r in rows}) == 2


def test_list_only_rows_work_with_unquoted_attributes() -> None:
    # Mirrors the live Upsurge markup (unquoted class attributes).
    html = (
        "<section class=CareerSummary><h3 class=CareerSummary__Title>FX Artists</h3></section>"
        "<section class=CareerSummary><h3 class=CareerSummary__Title>Hard Surface Artists</h3></section>"
    )
    block_sep = re.compile(r'(?is)(?=class\s*=\s*["\']*CareerSummary\b)')
    title_re = re.compile(
        r'(?is)<h3[^>]*class\s*=\s*["\']*[^\s"\'<>]*CareerSummary__Title[^\s"\'<>]*["\']*[^>]*>(.*?)</h3>'
    )
    rows = static_list_only_job_rows(
        _ctx("https://upsurgestudios.com/careers/", html),
        block_sep=block_sep,
        title_re=title_re,
    )
    assert [r["title"] for r in rows] == ["FX Artists", "Hard Surface Artists"]


def test_list_only_rows_dedupe_identical_titles() -> None:
    # Block marker `card` must not collide with the title class (mirrors the
    # CareerSummary / CareerSummary__Title separation in real boards).
    html = (
        "<div class=card><h3 class=title>Duplicate Role</h3></div>"
        "<div class=card><h3 class=title>Duplicate Role</h3></div>"
    )
    rows = static_list_only_job_rows(
        _ctx("https://example.com/careers/", html),
        block_sep=re.compile(r'(?is)(?=class\s*=\s*["\']*card\b)'),
        title_re=re.compile(r'(?is)<h3[^>]*class\s*=\s*["\']*title["\']*[^>]*>(.*?)</h3>'),
    )
    assert len(rows) == 1
    assert rows[0]["title"] == "Duplicate Role"


def test_list_only_rows_skip_blocks_without_titles_and_empty_titles() -> None:
    html = (
        "<div class=card><h3 class=title>Real Role</h3></div>"
        "<div class=card><h3 class=title>  </h3></div>"
        "<div class=card><p>no title here</p></div>"
    )
    rows = static_list_only_job_rows(
        _ctx("https://example.com/careers/", html),
        block_sep=re.compile(r'(?is)(?=class\s*=\s*["\']*card\b)'),
        title_re=re.compile(r'(?is)<h3[^>]*class\s*=\s*["\']*title["\']*[^>]*>(.*?)</h3>'),
    )
    assert [r["title"] for r in rows] == ["Real Role"]


def test_list_only_rows_empty_html_yields_no_rows() -> None:
    rows = static_list_only_job_rows(
        _ctx("https://example.com/careers/", ""),
        block_sep=re.compile(r'(?is)(?=class="role")'),
        title_re=re.compile(r"(?is)<h3[^>]*>(.*?)</h3>"),
    )
    assert rows == []


def test_upsurge_plugin_still_recovers_list_only_roles() -> None:
    # End-to-end through the refactored plugin: the WP5 plugin must still extract rows.
    from src.jobs.adapters.plugins.static import upsurge

    html = """
    <section class=CareerSummary><h3 class=CareerSummary__Title>Real Time Effects Artists</h3>
      <table class=CareerSummary__Data><tr><th>Job Description</th><td>FX.</td></tr></table></section>
    <section class=CareerSummary><h3 class=CareerSummary__Title>Character Artists</h3>
      <table class=CareerSummary__Data><tr><th>Job Description</th><td>Chars.</td></tr></table></section>
    """

    def fetch_text(url: str, timeout_s: int) -> str:
        assert url == "https://upsurgestudios.com/careers/"
        assert timeout_s == 10
        return html

    rows = cast(
        list[dict[str, Any]],
        upsurge.run(
            fetch_text=fetch_text,
            timeout_s=10,
            retries=0,
            backoff_s=0.0,
            pages=["https://upsurgestudios.com/careers/"],
            source_row={"id": "upsurge", "name": "Upsurge Careers", "company": "Upsurge Studio"},
        ),
    )
    assert [r["title"] for r in rows] == [
        "Real Time Effects Artists",
        "Character Artists",
    ]
    assert rows[0]["adapter"] == "static"
    assert rows[0]["studio"] == "Upsurge Studio"
