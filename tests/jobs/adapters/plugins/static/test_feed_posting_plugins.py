"""Tests for the conservative WordPress site-feed job-posting plugins.

arsanesia and petprojectgames expose their only recoverable job signal as a single
blog post inside the site's news feed. These tests pin down the conservative
role-keyword + negative-news title filter (against the exact live feed titles) and
the leaf-plugin extraction behavior.
"""

from __future__ import annotations

from typing import Any, cast

import pytest

from src.jobs.adapters.plugins.static import arsanesia, petprojectgames
from src.jobs.adapters.plugins.static._feed_postings import (
    looks_like_feed_role_posting,
    site_feed_url,
)

_PLUGINS = (arsanesia, petprojectgames)


@pytest.mark.parametrize(
    ("title", "expected"),
    [
        # arsanesia site feed
        ("Game Programmer: Full-Time & Intern", True),
        ("Development Log: July 2026", False),
        ("Development Log: June 2026", False),
        # petprojectgames site feed
        ("Pet Project Games Is Looking for a 3D Animator", True),
        ("Pet Project Games Releases the Official Trailer for Ripout", False),
        ("Ripout: Teaser Trailer and Steam Page Launched", False),
        ("Introducing Ripout: A Fresh Co-Op Horror Sci-Fi FPS Experience", False),
        ("Introducing Pet Project Games: An Innovative Video Game Company", False),
        ("Welcome to Our Blog!", False),
        ("Top 5 Sci-Fi Movies of All Time You Don't Want to Miss", False),
        ("30 Space Facts That Will Blow Your Mind", False),
    ],
)
def test_title_filter_matches_live_feed_postings(title: str, expected: bool) -> None:
    assert looks_like_feed_role_posting(title) is expected


@pytest.mark.parametrize(
    ("title", "expected"),
    [
        # conservative guards: a role noun alone is not enough (needs a hiring signal)
        ("Senior Gameplay Programmer (Paris)", False),
        ("Spotlight on our 3D Artist", False),
        ("Meet the Art Director", False),
        # genuine postings with hiring signals are kept
        ("We're Hiring a 3D Artist", True),
        ("Game Programmer: Full-Time", True),
        ("Now Hiring: Sound Designer", True),
        ("We Are Looking for a Level Designer", True),
        ("Junior Game Tester (Internship)", True),
        # empty / non-title noise
        ("", False),
    ],
)
def test_title_filter_is_conservative(title: str, expected: bool) -> None:
    assert looks_like_feed_role_posting(title) is expected


def test_site_feed_url_targets_wordpress_feed() -> None:
    assert site_feed_url("https://arsanesia.com/career/") == "https://arsanesia.com/feed/"
    assert site_feed_url("https://www.petprojectgames.com/careers/") == (
        "https://www.petprojectgames.com/feed/"
    )
    assert site_feed_url("") == ""
    assert site_feed_url("ftp://arsanesia.com/") == ""


def _source_row(plugin_name: str) -> dict[str, Any]:
    return {
        "id": plugin_name,
        "name": f"{plugin_name.title()} Careers",
        "studio": f"{plugin_name.title()} Studio",
        "company": f"{plugin_name.title()} Studio",
    }


def _run_plugin(plugin: Any, *, page_url: str, feed_html: str) -> list[dict[str, Any]]:
    def fetch_text(url: str, timeout_s: int) -> str:
        assert timeout_s == 10
        assert url == site_feed_url(page_url)
        return feed_html

    return cast(
        list[dict[str, Any]],
        plugin.run(
            fetch_text=fetch_text,
            timeout_s=10,
            retries=0,
            backoff_s=0.0,
            pages=[page_url],
            source_row=_source_row(plugin.__name__.split(".")[-1]),
        ),
    )


_ARSANESIA_FEED = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel>
<item><title>Development Log: July 2026</title><link>https://arsanesia.com/news/development/development-log-july-2026/</link></item>
<item><title>Game Programmer: Full-Time &amp; Intern</title><link>https://arsanesia.com/career/game-programmer-full-time-intern/</link></item>
<item><title>Development Log: June 2026</title><link>https://arsanesia.com/games/smiths-chronicles/development-log-june-2026/</link></item>
</channel></rss>
"""

_PETPROJECT_FEED = """<?xml version="1.0"?>
<rss version="2.0"><channel>
<item><title>Pet Project Games Is Looking for a 3D Animator</title><link>https://www.petprojectgames.com/pet-project-games-is-looking-for-a-3d-animator/</link></item>
<item><title>Pet Project Games Releases the Official Trailer for Ripout</title><link>https://www.petprojectgames.com/pet-project-games-releases-the-official-trailer-for-ripout/</link></item>
<item><title>Ripout: Teaser Trailer and Steam Page Launched</title><link>https://www.petprojectgames.com/ripout-teaser-trailer-and-steam-page-launched/</link></item>
<item><title>Welcome to Our Blog!</title><link>https://www.petprojectgames.com/welcome-to-our-blog/</link></item>
</channel></rss>
"""


def test_arsanesia_plugin_recovers_only_the_job_post() -> None:
    rows = _run_plugin(
        arsanesia,
        page_url="https://arsanesia.com/career/",
        feed_html=_ARSANESIA_FEED,
    )
    assert [r["title"] for r in rows] == ["Game Programmer: Full-Time & Intern"]
    assert rows[0]["jobLink"] == "https://arsanesia.com/career/game-programmer-full-time-intern/"
    assert rows[0]["sourceJobId"] == (
        "static:arsanesia:https://arsanesia.com/career/game-programmer-full-time-intern/"
    )


def test_petproject_plugin_recovers_only_the_job_post() -> None:
    rows = _run_plugin(
        petprojectgames,
        page_url="https://www.petprojectgames.com/careers/",
        feed_html=_PETPROJECT_FEED,
    )
    assert [r["title"] for r in rows] == ["Pet Project Games Is Looking for a 3D Animator"]
    assert rows[0]["jobLink"] == (
        "https://www.petprojectgames.com/pet-project-games-is-looking-for-a-3d-animator/"
    )


def test_feed_plugin_empty_feed_or_no_jobs_yields_no_rows() -> None:
    assert (
        _run_plugin(arsanesia, page_url="https://arsanesia.com/career/", feed_html="<html></html>")
        == []
    )
    news_only = """<?xml version="1.0"?><rss><channel>
    <item><title>Development Log: July 2026</title><link>https://arsanesia.com/dev/</link></item>
    </channel></rss>"""
    assert (
        _run_plugin(arsanesia, page_url="https://arsanesia.com/career/", feed_html=news_only) == []
    )


@pytest.mark.parametrize(
    ("plugin", "host"),
    [
        (arsanesia, "arsanesia.com"),
        (arsanesia, "www.arsanesia.com"),
        (petprojectgames, "petprojectgames.com"),
        (petprojectgames, "www.petprojectgames.com"),
    ],
)
def test_can_handle_own_hosts(plugin: Any, host: str) -> None:
    ctx = cast(Any, type("Ctx", (), {"source_identity": host})())
    assert plugin.can_handle(ctx) is True


@pytest.mark.parametrize(
    ("plugin", "host"),
    [
        (arsanesia, "petprojectgames.com"),
        (petprojectgames, "arsanesia.com"),
        (arsanesia, "upsurgestudios.com"),
    ],
)
def test_plugin_rejects_unrelated_hosts(plugin: Any, host: str) -> None:
    ctx = cast(Any, type("Ctx", (), {"source_identity": host})())
    assert plugin.can_handle(ctx) is False
