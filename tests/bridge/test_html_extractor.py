import pytest

from src.bridge import html_extractor
from src.bridge.html_extractor import extract_job_like_links, extract_text_job_signals


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
