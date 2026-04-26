import pytest

from src.jobs.adapters.static_runtime_support import StaticHtmlFetcher
from src.jobs.common.http import HttpStatusError


def test_static_html_fetcher_follows_one_safe_same_host_redirect() -> None:
    calls: list[str] = []

    def fetch_text(url: str, _: int) -> str:
        calls.append(url)
        if url == "http://example.com/careers":
            raise HttpStatusError(301, url, location="https://example.com/jobs")
        if url == "https://example.com/jobs":
            return "<html>jobs</html>"
        raise AssertionError(f"unexpected url: {url}")

    fetcher = StaticHtmlFetcher(fetch_text=fetch_text, timeout_s=5, retries=0, backoff_s=0)

    html, cache_hit = fetcher.fetch_html_cached("http://example.com/careers")

    assert html == "<html>jobs</html>"
    assert cache_hit is False
    assert calls == ["http://example.com/careers", "https://example.com/jobs"]


def test_static_html_fetcher_follows_www_alias_redirect() -> None:
    calls: list[str] = []

    def fetch_text(url: str, _: int) -> str:
        calls.append(url)
        if url == "https://www.example.com/careers":
            raise HttpStatusError(301, url, location="https://example.com/jobs")
        if url == "https://example.com/jobs":
            return "<html>jobs</html>"
        raise AssertionError(f"unexpected url: {url}")

    fetcher = StaticHtmlFetcher(fetch_text=fetch_text, timeout_s=5, retries=0, backoff_s=0)

    html, cache_hit = fetcher.fetch_html_cached("https://www.example.com/careers")

    assert html == "<html>jobs</html>"
    assert cache_hit is False
    assert calls == ["https://www.example.com/careers", "https://example.com/jobs"]


def test_static_html_fetcher_allows_http_to_https_same_site_upgrade() -> None:
    calls: list[str] = []

    def fetch_text(url: str, _: int) -> str:
        calls.append(url)
        if url == "http://www.example.com/careers":
            raise HttpStatusError(301, url, location="https://example.com/careers")
        if url == "https://example.com/careers":
            return "<html>jobs</html>"
        raise AssertionError(f"unexpected url: {url}")

    fetcher = StaticHtmlFetcher(fetch_text=fetch_text, timeout_s=5, retries=0, backoff_s=0)

    html, cache_hit = fetcher.fetch_html_cached("http://www.example.com/careers")

    assert html == "<html>jobs</html>"
    assert cache_hit is False
    assert calls == ["http://www.example.com/careers", "https://example.com/careers"]


def test_static_html_fetcher_rejects_unsafe_cross_host_redirect() -> None:
    def fetch_text(url: str, _: int) -> str:
        raise HttpStatusError(302, url, location="https://jobs.example.net/careers")

    fetcher = StaticHtmlFetcher(fetch_text=fetch_text, timeout_s=5, retries=0, backoff_s=0)

    with pytest.raises(RuntimeError, match="Unsafe static redirect"):
        fetcher.fetch_html_cached("https://example.com/careers")


def test_static_html_fetcher_rejects_https_downgrade_on_www_alias() -> None:
    def fetch_text(url: str, _: int) -> str:
        raise HttpStatusError(302, url, location="http://example.com/careers")

    fetcher = StaticHtmlFetcher(fetch_text=fetch_text, timeout_s=5, retries=0, backoff_s=0)

    with pytest.raises(RuntimeError, match="Unsafe static redirect"):
        fetcher.fetch_html_cached("https://www.example.com/careers")


@pytest.mark.parametrize(
    ("location", "message"),
    [
        ("", "missing Location"),
        ("https://example.com/careers", "redirect loop"),
    ],
)
def test_static_html_fetcher_rejects_missing_location_and_redirect_loop(
    location: str, message: str
) -> None:
    def fetch_text(url: str, _: int) -> str:
        raise HttpStatusError(301, url, location=location)

    fetcher = StaticHtmlFetcher(fetch_text=fetch_text, timeout_s=5, retries=0, backoff_s=0)

    with pytest.raises(RuntimeError, match=message):
        fetcher.fetch_html_cached("https://example.com/careers")
