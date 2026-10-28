from __future__ import annotations

import pytest

from src.source_discovery import page_diagnostics
from src.source_discovery.page_diagnostics import looks_like_js_shell, no_candidate_reason_detail


def test_looks_like_js_shell_default_preserves_web_search_behavior() -> None:
    html = "<noscript>Enable JS</noscript><script></script>" * 20

    assert looks_like_js_shell('<div id="root"></div><script src="/app.js"></script>') is True
    assert looks_like_js_shell(html) is False


def test_looks_like_js_shell_opt_in_noscript_matches_gamedevmap_behavior() -> None:
    html = "<noscript>Enable JS</noscript><script></script>" * 20

    assert looks_like_js_shell(html, include_noscript_script_shell=True) is True


def test_no_candidate_reason_detail_preserves_gamedevmap_buckets() -> None:
    def no_jobish(_url: str, _html: str) -> list[str]:
        return []

    def has_jobish(_url: str, _html: str) -> list[str]:
        return ["https://studio.example/jobs"]

    assert (
        no_candidate_reason_detail(
            "https://linktr.ee/studio",
            "<html></html>",
            social_profile_hosts={"linktr.ee"},
            jobish_url_fn=has_jobish,
        )
        == "social_profile_host"
    )
    assert (
        no_candidate_reason_detail(
            "https://sites.google.com/studio",
            "<html></html>",
            third_party_profile_hosts={"sites.google.com"},
            jobish_url_fn=has_jobish,
        )
        == "third_party_profile_host"
    )
    assert (
        no_candidate_reason_detail(
            "https://studio.example",
            '<div id="app"></div><script></script>',
            jobish_url_fn=has_jobish,
        )
        == "js_shell"
    )
    assert (
        no_candidate_reason_detail(
            "https://studio.example",
            "<html></html>",
            jobish_url_fn=no_jobish,
        )
        == "no_jobish_links"
    )
    assert (
        no_candidate_reason_detail(
            "https://studio.example",
            "<a href='/jobs'>Jobs</a>",
            jobish_url_fn=has_jobish,
        )
        == "homepage_links_no_candidate"
    )


JS_SHELL_FIXTURES = (
    ('<div id="app"></div><script src="/app.js"></script>', True, True),
    ('<div id="__next"></div><script></script>' + "x" * 1000, True, True),
    ('<DIV ID="ROOT"></DIV><SCRIPT SRC="/APP.JS"></SCRIPT>' + "x" * 1000, True, True),
    ("<html><body><p>" + "plain page content " * 400 + "</p></body></html>", False, False),
    ("<noscript>Enable JS</noscript><script></script>" * 20, False, True),
)


@pytest.mark.parametrize(("html", "default", "noscript"), JS_SHELL_FIXTURES)
def test_looks_like_js_shell_reuses_a_lowered_body_without_changing_the_verdict(
    html: str, default: bool, noscript: bool
) -> None:
    """The caller's lowered body is the one the test would have computed itself."""
    assert looks_like_js_shell(html) is default
    assert looks_like_js_shell(html, lowered=html.lower()) is default
    assert looks_like_js_shell(html, include_noscript_script_shell=True) is noscript
    assert (
        looks_like_js_shell(html, lowered=html.lower(), include_noscript_script_shell=True)
        is noscript
    )


def test_no_candidate_reason_detail_forwards_the_lowered_body(monkeypatch) -> None:
    """The detail hands the js-shell test the lowered body it was given, not a second copy."""
    seen: list[str | None] = []
    real = page_diagnostics.looks_like_js_shell

    def _capture(html: str, *, lowered=None, **_kwargs) -> bool:
        seen.append(lowered)
        return real(html, lowered=lowered, **_kwargs)

    monkeypatch.setattr(page_diagnostics, "looks_like_js_shell", _capture)
    body = '<div id="app"></div><script></script>'

    assert (
        no_candidate_reason_detail(
            "https://studio.example",
            body,
            jobish_url_fn=lambda _url, _html: [],
            lowered=body.lower(),
        )
        == "js_shell"
    )
    assert seen == [body.lower()]


def test_no_candidate_reason_detail_supports_generic_profile_host_bucket() -> None:
    assert (
        no_candidate_reason_detail(
            "https://facebook.com/studio",
            "<html></html>",
            profile_hosts={"facebook.com"},
            jobish_url_fn=lambda _url, _html: ["https://facebook.com/studio/jobs"],
        )
        == "profile_host"
    )
