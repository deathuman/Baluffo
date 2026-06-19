import pytest

from src.bridge import html_extractor
from src.bridge.html_extractor import (
    extract_external_job_links_from_scripts,
    extract_job_like_links,
    extract_text_job_signals,
)


def test_extract_text_job_signals_strips_malformed_script_and_style_end_tags() -> None:
    noisy_html = """
        <script>
            apply now engineer apply now artist apply now designer apply now producer
        </script
            ignored>
        <style>
            apply now engineer apply now artist apply now designer apply now producer
        </style
            ignored>
        <main>Careers</main>
    """

    assert extract_text_job_signals(noisy_html, "https://example.com/careers") == []


def test_extract_job_like_links_falls_back_for_expected_urljoin_failure(monkeypatch) -> None:
    def fail_urljoin(_base_url: str, raw_href: str) -> str:
        if raw_href == "https://example.com/jobs/designer":
            raise ValueError("bad url")
        return raw_href

    monkeypatch.setattr(html_extractor, "urljoin", fail_urljoin)

    links = extract_job_like_links(
        '<a href="https://example.com/jobs/designer">Designer</a>',
        "https://example.com/careers",
    )

    assert links == ["https://example.com/jobs/designer"]


def test_extract_job_like_links_does_not_hide_unexpected_urljoin_failure(monkeypatch) -> None:
    monkeypatch.setattr(
        html_extractor,
        "urljoin",
        lambda _base_url, _raw_href: (_ for _ in ()).throw(RuntimeError("urljoin bug")),
    )

    with pytest.raises(RuntimeError, match="urljoin bug"):
        extract_job_like_links(
            '<a href="/jobs/designer">Designer</a>',
            "https://example.com/careers",
        )


def test_extract_external_job_links_from_scripts_records_expected_fetch_failure() -> None:
    html = '<script src="https://example.com/jobs-feed.js"></script>'

    links, errors = extract_external_job_links_from_scripts(
        html,
        "https://example.com/careers",
        3,
        lambda _url, _timeout_s: (_ for _ in ()).throw(RuntimeError("network failed")),
    )

    assert links == []
    assert errors == ["https://example.com/jobs-feed.js: network failed"]


def test_extract_external_job_links_from_scripts_records_intervieweb_fetch_failure() -> None:
    html = (
        '<script src="https://cezanneondemand.intervieweb.it/integration/'
        "announces_js.php?lang=en&utype=0&k=abc123&LAC=studio&d=example.com"
        "&annType=published&view=list&defgroup=name&gnavenable=1&desc=1"
        '&typeView=large"></script>'
    )

    links, errors = extract_external_job_links_from_scripts(
        html,
        "https://example.com/careers",
        3,
        lambda _url, _timeout_s: (_ for _ in ()).throw(OSError("iframe unavailable")),
    )

    assert links == []
    assert errors == [
        "https://cezanneondemand.intervieweb.it/app.php?module=iframeAnnunci&lang=en"
        "&k=abc123&d=example.com&LAC=studio&utype=0&act1=23&defgroup=name"
        "&gnavenable=1&desc=1&annType=published&h=&typeView=large: iframe unavailable"
    ]


def test_extract_external_job_links_from_scripts_does_not_hide_unexpected_fetch_failure() -> None:
    html = '<script src="https://example.com/jobs-feed.js"></script>'

    def broken_fetch(_url: str, _timeout_s: int) -> str:
        raise AttributeError("fetch bug")

    with pytest.raises(AttributeError, match="fetch bug"):
        extract_external_job_links_from_scripts(
            html,
            "https://example.com/careers",
            3,
            broken_fetch,
        )
