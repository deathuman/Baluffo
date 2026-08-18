from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast

import pytest

from src.jobs.adapters.plugins.static import (
    _heuristics,
    _runner,
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

    return cast(
        list[dict[str, Any]],
        plugin.run(
            fetch_text=fetch_text,
            timeout_s=10,
            retries=0,
            backoff_s=0.0,
            pages=[page_url],
            source_row=source_row,
        ),
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


def test_static_plugin_fetch_helper_does_not_swallow_unexpected_bug() -> None:
    source_row = _source_row("generic")

    def broken_fetch(_url: str, _timeout_s: int) -> str:
        raise RuntimeError("unexpected static plugin fetch bug")

    with pytest.raises(RuntimeError, match="unexpected static plugin fetch bug"):
        _runner.fetch_static_plugin_html(
            fetch_text=broken_fetch,
            page_url="https://example.com/careers",
            timeout_s=10,
            source_row=source_row,
        )

    assert "_staticPluginMeta" not in source_row


def test_simple_static_plugin_fetch_retry_does_not_swallow_unexpected_bug() -> None:
    source_row = _source_row("simple")
    browser_calls: list[str] = []

    def broken_fetch(_url: str, _timeout_s: int) -> str:
        raise RuntimeError("unexpected simple plugin fetch bug")

    def fake_browser(url: str, _timeout_s: int) -> tuple[str, str]:
        browser_calls.append(url)
        return "<html><body>Rendered</body></html>", ""

    spec = _runner.SimpleStaticPlugin(
        source_id="simple",
        default_company="Simple Studio",
        playwright_on_fetch_error=True,
        parser_stale_hint="simple_empty",
    )

    with pytest.raises(RuntimeError, match="unexpected simple plugin fetch bug"):
        _runner.run_simple_static_plugin(
            fetch_text=broken_fetch,
            timeout_s=10,
            retries=0,
            backoff_s=0,
            pages=["https://example.com/careers"],
            source_row=source_row,
            spec=spec,
            parse_html=lambda _ctx: [],
            try_playwright=fake_browser,
        )

    assert browser_calls == []


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
