from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from src.jobs.adapters.plugins.static import (
    _heuristics,
    climax,
    embark,
    globalstep,
)

_PLUGIN_CASES = [
    pytest.param(
        climax,
        "https://www.climaxstudios.com/careers/",
        """
        <a href="/join-our-team/jobs/gameplay-programmer">
          <article>
            <h2>Gameplay Programmer</h2>
            <span>Location Portsmouth, United Kingdom</span>
            <span>Permanent, Full-time</span>
          </article>
        </a>
        """,
        "Gameplay Programmer",
        "https://www.climaxstudios.com/join-our-team/jobs/gameplay-programmer",
        "climax_listing_present_but_plugin_empty",
        id="climax",
    ),
    pytest.param(
        embark,
        "https://careers.embark-studios.com/jobs/",
        """
        <a href="/jobs/creative-director">
          <div>Creative Director</div>
          <span>Stockholm, Sweden</span>
        </a>
        """,
        "Creative Director",
        "https://careers.embark-studios.com/jobs/creative-director",
        "embark_listing_present_but_plugin_empty",
        id="embark",
    ),
    pytest.param(
        globalstep,
        "https://globalstep.com/careers/",
        """
        <a href="/jobs/qa-tester">
          <h2>QA Tester</h2>
          <span>Montreal, Canada</span>
          <span>Full-time</span>
          <span>More details</span>
        </a>
        """,
        "QA Tester",
        "https://globalstep.com/jobs/qa-tester",
        "globalstep_listing_present_but_plugin_empty",
        id="globalstep",
    ),
]


def _source_row(plugin_name: str) -> dict[str, Any]:
    return {
        "id": plugin_name,
        "name": f"{plugin_name.title()} Careers",
        "studio": f"{plugin_name.title()} Studio",
        "company": f"{plugin_name.title()} Studio",
    }


def _run_plugin(
    plugin: Any,
    *,
    page_url: str,
    html_or_fetch: str | Callable[[str, int], str],
    source_row: dict[str, Any],
) -> list[dict[str, Any]]:
    def fetch_text(url: str, timeout_s: int) -> str:
        assert url == page_url
        assert timeout_s == 10
        if callable(html_or_fetch):
            return html_or_fetch(url, timeout_s)
        return html_or_fetch

    return plugin.run(
        fetch_text=fetch_text,
        timeout_s=10,
        retries=0,
        backoff_s=0.0,
        pages=[page_url],
        source_row=source_row,
    )


@pytest.mark.parametrize(
    ("plugin", "page_url", "html", "expected_title", "expected_link", "_empty_hint"),
    _PLUGIN_CASES,
)
def test_standard_static_plugin_success_tags_rows(
    plugin: Any,
    page_url: str,
    html: str,
    expected_title: str,
    expected_link: str,
    _empty_hint: str,
) -> None:
    source_row = _source_row(plugin.__name__.rsplit(".", 1)[-1])

    rows = _run_plugin(plugin, page_url=page_url, html_or_fetch=html, source_row=source_row)

    assert len(rows) == 1
    assert rows[0]["title"] == expected_title
    assert rows[0]["jobLink"] == expected_link
    assert rows[0]["adapter"] == "static"
    assert rows[0]["studio"] == source_row["company"]
    assert rows[0]["source"] == source_row["name"]
    assert source_row["_staticPluginMeta"]["classification"] == (
        _heuristics.CLASSIFICATION_OK_WITH_JOBS
    )
    assert source_row["_staticPluginMeta"]["detailTraversalMode"] == "listing_only"


@pytest.mark.parametrize(
    ("plugin", "page_url", "_html", "_title", "_link", "_empty_hint"),
    _PLUGIN_CASES,
)
def test_standard_static_plugin_fetch_exception_sets_meta(
    plugin: Any,
    page_url: str,
    _html: str,
    _title: str,
    _link: str,
    _empty_hint: str,
) -> None:
    source_row = _source_row(plugin.__name__.rsplit(".", 1)[-1])

    def fail_fetch(_url: str, _timeout_s: int) -> str:
        raise RuntimeError("HTTP 403 Forbidden")

    rows = _run_plugin(plugin, page_url=page_url, html_or_fetch=fail_fetch, source_row=source_row)

    assert rows == []
    assert source_row["_staticPluginMeta"]["classification"] == (
        _heuristics.CLASSIFICATION_BLOCKED_OR_CHALLENGE
    )
    assert source_row["_staticPluginMeta"]["browserFallbackRecommended"] is True
    assert source_row["_staticPluginMeta"]["extractorHint"] == "fetch_failed"
    assert "HTTP 403" in source_row["_staticPluginMeta"]["error"]


@pytest.mark.parametrize(
    ("plugin", "page_url", "_html", "_title", "_link", "empty_hint"),
    _PLUGIN_CASES,
)
def test_standard_static_plugin_empty_listing_sets_parser_stale_meta(
    plugin: Any,
    page_url: str,
    _html: str,
    _title: str,
    _link: str,
    empty_hint: str,
) -> None:
    source_row = _source_row(plugin.__name__.rsplit(".", 1)[-1])

    rows = _run_plugin(
        plugin,
        page_url=page_url,
        html_or_fetch="<html><body><p>Careers page without matching links.</p></body></html>",
        source_row=source_row,
    )

    assert rows == []
    assert source_row["_staticPluginMeta"] == {
        "classification": _heuristics.CLASSIFICATION_PARSER_STALE,
        "browserFallbackRecommended": False,
        "extractorHint": empty_hint,
        "detailFetchRequired": False,
        "detailTraversalMode": "listing_only",
    }
