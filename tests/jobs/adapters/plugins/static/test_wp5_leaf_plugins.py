"""Focused tests for the WP5 leaf static plugins (Upsurge, Sandsoft)."""

from __future__ import annotations

from typing import Any, cast

import pytest

from src.jobs.adapters.plugins.static import sandsoft, upsurge


def _source_row(plugin_name: str) -> dict[str, Any]:
    return {
        "id": plugin_name,
        "name": f"{plugin_name.title()} Careers",
        "studio": f"{plugin_name.title()} Studio",
        "company": f"{plugin_name.title()} Studio",
    }


def _fetch_once(page_url: str, html: str) -> Any:
    def fetch_text(url: str, timeout_s: int) -> str:
        assert timeout_s == 10
        return html if url == page_url else ""

    return fetch_text


@pytest.mark.parametrize(
    ("plugin", "host"),
    [
        (upsurge, "upsurgestudios.com"),
        (upsurge, "www.upsurgestudios.com"),
        (sandsoft, "sandsoft.com"),
        (sandsoft, "www.sandsoft.com"),
    ],
)
def test_can_handle_own_hosts(plugin: Any, host: str) -> None:
    ctx = cast(Any, type("Ctx", (), {"source_identity": host})())
    assert plugin.can_handle(ctx) is True


@pytest.mark.parametrize(
    ("plugin", "host"),
    [
        (upsurge, "outerdawn.com"),
        (sandsoft, "astridentertainment.com"),
        (upsurge, "sandsoft.com"),
    ],
)
def test_plugin_rejects_unrelated_hosts(plugin: Any, host: str) -> None:
    ctx = cast(Any, type("Ctx", (), {"source_identity": host})())
    assert plugin.can_handle(ctx) is False


def test_upsurge_extracts_all_roles_as_distinct_rows() -> None:
    # Mirrors the live careers page: unquoted class attributes, no per-role links.
    html = """
    <section class=CareerList><h2 class=CareerList__Title>Open Positions</h2>
      <section class=CareerSummary>
        <h3 class=CareerSummary__Title>Real Time Effects Artists</h3>
        <table class=CareerSummary__Data><tr><th>Job Description</th><td>FX work.</td></tr><tr><th>Requirements</th><td>Houdini.</td></tr></table>
      </section>
      <section class=CareerSummary>
        <h3 class=CareerSummary__Title>Character Artists</h3>
        <table class=CareerSummary__Data><tr><th>Job Description</th><td>Characters.</td></tr></table>
      </section>
    </section>
    """
    rows = cast(
        list[dict[str, Any]],
        upsurge.run(
            fetch_text=_fetch_once("https://upsurgestudios.com/careers/", html),
            timeout_s=10,
            retries=0,
            backoff_s=0.0,
            pages=["https://upsurgestudios.com/careers/"],
            source_row=_source_row("upsurge"),
        ),
    )
    assert [r["title"] for r in rows] == ["Real Time Effects Artists", "Character Artists"]
    assert [r["jobLink"] for r in rows] == [
        "https://upsurgestudios.com/careers/#real-time-effects-artists",
        "https://upsurgestudios.com/careers/#character-artists",
    ]
    # Distinct sourceJobIds despite same base URL (fragment-linked rows).
    assert len({r["sourceJobId"] for r in rows}) == 2
    assert rows[0]["adapter"] == "static"
    assert rows[0]["studio"] == "Upsurge Studio"


def test_upsurge_ignores_non_career_blocks() -> None:
    html = """
    <h3 class=PageTitle>Recruit</h3>
    <section class=CareerList><h2 class=CareerList__Title>Open Positions</h2>
      <section class=CareerSummary><h3 class=CareerSummary__Title>Animators</h3>
        <table class=CareerSummary__Data><tr><th>Job Description</th><td>Maya.</td></tr></table>
      </section>
    </section>
    """
    rows = cast(
        list[dict[str, Any]],
        upsurge.run(
            fetch_text=_fetch_once("https://upsurgestudios.com/careers/", html),
            timeout_s=10,
            retries=0,
            backoff_s=0.0,
            pages=["https://upsurgestudios.com/careers/"],
            source_row=_source_row("upsurge"),
        ),
    )
    assert [r["title"] for r in rows] == ["Animators"]


def test_sandsoft_fetches_feed_and_recovers_postings() -> None:
    feed = """<?xml version="1.0"?><rss version="2.0"><channel>
      <item><title>Senior Game Designer</title><link>https://sandsoft.com/careers/senior-game-designer/</link><guid isPermaLink="false">x</guid></item>
      <item><title>3D Marketing Animator</title><link>https://sandsoft.com/careers/3d-marketing-animator/</link><guid isPermaLink="false">y</guid></item>
    </channel></rss>"""

    def fetch_text(url: str, timeout_s: int) -> str:
        assert timeout_s == 10
        assert url == "https://sandsoft.com/careers/feed/"
        return feed

    rows = cast(
        list[dict[str, Any]],
        sandsoft.run(
            fetch_text=fetch_text,
            timeout_s=10,
            retries=0,
            backoff_s=0.0,
            pages=["https://sandsoft.com/careers/"],
            source_row=_source_row("sandsoft"),
        ),
    )
    assert [r["title"] for r in rows] == ["Senior Game Designer", "3D Marketing Animator"]
    assert [r["jobLink"] for r in rows] == [
        "https://sandsoft.com/careers/senior-game-designer/",
        "https://sandsoft.com/careers/3d-marketing-animator/",
    ]
    assert rows[0]["studio"] == "Sandsoft Studio"


def test_sandsoft_feed_url_handles_both_slash_forms() -> None:
    assert (
        sandsoft._feed_url("https://sandsoft.com/careers") == "https://sandsoft.com/careers/feed/"
    )
    assert (
        sandsoft._feed_url("https://sandsoft.com/careers/") == "https://sandsoft.com/careers/feed/"
    )
    assert (
        sandsoft._feed_url("https://sandsoft.com/careers/feed")
        == "https://sandsoft.com/careers/feed/"
    )


def test_sandsoft_empty_feed_yields_no_rows() -> None:
    rows = cast(
        list[dict[str, Any]],
        sandsoft.run(
            fetch_text=_fetch_once("https://sandsoft.com/careers/feed/", "not xml at all"),
            timeout_s=10,
            retries=0,
            backoff_s=0.0,
            pages=["https://sandsoft.com/careers/"],
            source_row=_source_row("sandsoft"),
        ),
    )
    assert rows == []
