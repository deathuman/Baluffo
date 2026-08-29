"""Tests for the shared WordPress-feed probe and its discovery-sweep integration."""

from __future__ import annotations

from src.source_discovery import wordpress_feed_probe as wfp
from src.source_discovery.directory_page_recovery import (
    DirectoryRecoveryRequest,
    run_directory_page_recovery,
)


def _request(
    key: str = "https://studio.example.com/",
    *,
    html: str = "<html><body>No openings here</body></html>",
) -> DirectoryRecoveryRequest:
    return DirectoryRecoveryRequest(
        key=key,
        adapter="gameprog",
        discovery_method="gameprog",
        name="Studio",
        studio="Studio",
        page_url=key,
        html=html,
        payload={"studio": "Studio"},
    )


# ── extract_advertised_feed_urls ──────────────────────────────────────────────


def test_advertised_feed_urls_reads_rss_alternate_link_and_joins_against_base_url() -> None:
    html = """
    <link rel="alternate" type="application/rss+xml" title="Studio" href="/feed/" />
    <link rel="alternate" type="text/html" href="/about" />
    """
    assert wfp.extract_advertised_feed_urls(html, "https://studio.example.com/careers") == [
        "https://studio.example.com/feed/"
    ]


def test_advertised_feed_urls_handles_atom_and_absolute_hrefs_and_dedups() -> None:
    html = """
    <link rel="alternate" type="application/atom+xml" href="https://blog.example.com/feed.xml" />
    <link rel="alternate" type="application/rss+xml" href="/feed/" />
    <link rel="alternate" type="application/rss+xml" href="/feed/" />
    """
    assert wfp.extract_advertised_feed_urls(html, "https://studio.example.com/") == [
        "https://blog.example.com/feed.xml",
        "https://studio.example.com/feed/",
    ]


def test_advertised_feed_urls_ignores_non_feed_alternates_and_empty_hrefs() -> None:
    html = """
    <link rel="alternate" type="text/html" href="/about" />
    <link rel="alternate" type="application/rss+xml" />
    """
    assert wfp.extract_advertised_feed_urls(html, "https://studio.example.com/") == []


# ── wordpress_feed_candidate_urls ─────────────────────────────────────────────


def test_wordpress_feed_candidate_urls_builds_canonical_paths() -> None:
    assert wfp.wordpress_feed_candidate_urls("https://studio.example.com/careers") == [
        "https://studio.example.com/feed/",
        "https://studio.example.com/careers/feed/",
        "https://studio.example.com/feed",
    ]


def test_wordpress_feed_candidate_urls_root_page_drops_page_path_variant() -> None:
    assert wfp.wordpress_feed_candidate_urls("https://studio.example.com/") == [
        "https://studio.example.com/feed/",
        "https://studio.example.com/feed",
    ]


def test_wordpress_feed_candidate_urls_rejects_non_http() -> None:
    assert wfp.wordpress_feed_candidate_urls("ftp://studio.example.com/") == []
    assert wfp.wordpress_feed_candidate_urls("") == []


# ── looks_like_feed_document / feed_item_count ────────────────────────────────


def test_looks_like_feed_document_accepts_rss_and_atom() -> None:
    assert (
        wfp.looks_like_feed_document(
            '<?xml version="1.0"?><rss version="2.0"><channel><item><title>Job</title></item></channel></rss>'
        )
        is True
    )
    assert (
        wfp.looks_like_feed_document(
            '<?xml version="1.0"?><feed xmlns="http://www.w3.org/2005/Atom"><entry><title>Job</title></entry></feed>'
        )
        is True
    )


def test_looks_like_feed_document_rejects_html_and_featureless_xml() -> None:
    assert wfp.looks_like_feed_document("<html><body>no feed</body></html>") is False
    assert (
        wfp.looks_like_feed_document('<?xml version="1.0"?><rss version="2.0"><channel/></rss>')
        is False
    )
    assert wfp.looks_like_feed_document("") is False


def test_feed_item_count_counts_items() -> None:
    body = '<?xml version="1.0"?><rss><channel><item/><item/><item/><entry/></channel></rss>'
    assert wfp.feed_item_count(body) == 4


# ── probe_wordpress_feed ──────────────────────────────────────────────────────


def test_probe_prefers_advertised_feed_without_network_request() -> None:
    html = (
        '<link rel="alternate" type="application/rss+xml" href="/feed/" />\n<div id="root"></div>'
    )
    calls: list[str] = []

    def fetch_text(url: str, _timeout: int) -> str:
        calls.append(url)
        return "<html></html>"

    result = wfp.probe_wordpress_feed(
        "https://studio.example.com/careers",
        html,
        fetch_text=fetch_text,
        timeout_s=5,
    )

    assert result == {
        "feedUrl": "https://studio.example.com/feed/",
        "source": "advertised",
        "reason": "",
        "itemCount": 0,
    }
    assert calls == []


def test_probe_falls_back_to_wordpress_feed_paths() -> None:
    feed_body = (
        '<?xml version="1.0"?><rss version="2.0"><channel>'
        "<item><title>Job A</title></item>"
        "<item><title>Job B</title></item>"
        "</channel></rss>"
    )

    def fetch_text(url: str, _timeout: int) -> str:
        if url == "https://studio.example.com/feed/":
            return feed_body
        return "<html></html>"

    result = wfp.probe_wordpress_feed(
        "https://studio.example.com/careers",
        '<div id="root"></div>',
        fetch_text=fetch_text,
        timeout_s=5,
    )

    assert result == {
        "feedUrl": "https://studio.example.com/feed/",
        "source": "wordpress_fallback",
        "reason": "",
        "itemCount": 2,
    }


def test_probe_tolerates_fetch_errors_and_returns_empty_when_no_feed() -> None:
    def fetch_text(url: str, _timeout: int) -> str:
        if url.endswith("/feed/"):
            raise OSError("timed out")
        raise RuntimeError("http error 404")

    result = wfp.probe_wordpress_feed(
        "https://studio.example.com/careers",
        "<html></html>",
        fetch_text=fetch_text,
        timeout_s=5,
    )

    assert result == {"feedUrl": "", "source": "", "reason": "no_feed", "itemCount": 0}


# ── discovery-sweep integration ───────────────────────────────────────────────


def _run_recovery(requests, inject_fetch):
    def analyze(result, request):
        return [], []

    return run_directory_page_recovery(
        5,
        requests,
        fetcher=inject_fetch,
        total_concurrency=2,
        per_host_concurrency=1,
        analyze_result=analyze,
        progress_label="Test",
    )


def test_js_shell_with_feed_becomes_feed_candidate_not_browser_candidate() -> None:
    request = _request(
        html=(
            '<div id="root"></div><script src="/app.js"></script>\n'
            '<link rel="alternate" type="application/rss+xml" href="/feed/" />'
        )
    )

    def fetch_text(url: str, _timeout: int) -> str:
        return "<html></html>"

    output = _run_recovery([request], fetch_text)

    assert output.static_candidates == [
        {
            "name": "Studio",
            "studio": "Studio",
            "company": "Studio",
            "adapter": "static",
            "pages": ["https://studio.example.com/feed/"],
            "listing_url": "https://studio.example.com/feed/",
            "careersUrl": "https://studio.example.com/",
            "discoveryMethod": "gameprog",
            "discoveryStage": "wordpress_feed",
            "evidenceSource": "wordpress_feed",
            "evidenceTypes": ["server_rendered_feed"],
            "feedUrl": "https://studio.example.com/feed/",
            "feedSource": "advertised",
            "feedItemCount": 0,
            "enabledByDefault": False,
            "weakSignal": True,
        }
    ]
    assert output.browser_recovery_candidates == []
    assert output.summary["feedRecoveryCandidates"] == 1


def test_js_shell_without_feed_still_lands_in_browser_pool() -> None:
    request = _request(html='<div id="root"></div><script src="/app.js"></script>')

    def fetch_text(url: str, _timeout: int) -> str:
        raise OSError("timed out")  # feed probes all fail

    output = _run_recovery([request], fetch_text)

    assert output.browser_recovery_candidates == [
        {
            "adapter": "gameprog",
            "discoveryMethod": "gameprog",
            "name": "Studio",
            "studio": "Studio",
            "url": request.page_url,
            "sourceDirectoryEntryUrl": request.page_url,
            "reason": "no_careers_evidence",
            "reasonDetail": "js_shell",
        }
    ]
    assert output.static_candidates == []
    assert output.summary["feedRecoveryCandidates"] == 0


def test_non_js_shell_requests_are_untouched_by_feed_probe() -> None:
    request = _request(html="<html><body>No openings here</body></html>")
    fetched: list[str] = []

    def fetch_text(url: str, _timeout: int) -> str:
        fetched.append(url)
        return "<html></html>"

    output = _run_recovery([request], fetch_text)

    assert output.browser_recovery_candidates == []
    assert output.static_candidates == []
    assert all(not url.startswith("https://studio.example.com/feed") for url in fetched)


def test_freestanding_probe_result_is_json_serializable() -> None:
    html = '<link rel="alternate" type="application/rss+xml" href="/blog/feed/" />'
    result = wfp.probe_wordpress_feed(
        "https://studio.example.com/jobs",
        html,
        fetch_text=lambda _url, _t: "",
        timeout_s=5,
    )
    import json

    assert json.loads(json.dumps(result)) == result
