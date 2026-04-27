from __future__ import annotations

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
