import pytest

from src.jobs.adapters.plugins.static import ncsoft
from src.jobs.adapters.plugins.static._rendered_cards import (
    can_handle_rendered_cards,
    run_rendered_cards_plugin,
)
from src.jobs.adapters.plugins.types import AdapterPluginContext


def test_ncsoft_plugin_uses_detail_page_title_and_location() -> None:
    listing_url = "https://nca.ncsoft.com/en-US/careers"
    detail_url = "https://nca.ncsoft.com/en-US/careers/16408"
    listing_html = """
        <a href="/en-US/careers/16408">
          Customer Service Specialist Irvine, CA · Full time
        </a>
    """
    detail_html = """
        <html>
          <body>
            <h1>Customer Service Specialist</h1>
            <section>Irvine, CA · Full time</section>
          </body>
        </html>
    """

    def fake_fetch(url: str, _timeout: int) -> str:
        if url == listing_url:
            return listing_html
        if url == detail_url:
            return detail_html
        raise AssertionError(f"unexpected URL {url}")

    rows = ncsoft.run(
        fetch_text=fake_fetch,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        pages=[listing_url],
        source_row={
            "id": "static:listing_url:https://nca.ncsoft.com/en-us/careers",
            "name": "NCSoft (Sheet)",
            "company": "NCSoft",
        },
    )

    assert len(rows) == 1
    assert rows[0]["title"] == "Customer Service Specialist"
    assert rows[0]["city"] == "Irvine"
    assert rows[0]["country"] == "US"
    assert rows[0]["jobLink"] == detail_url


def test_ncsoft_plugin_uses_browser_listing_before_detail_fetch() -> None:
    listing_url = "https://nca.ncsoft.com/en-US/careers"
    detail_url = "https://nca.ncsoft.com/en-US/careers/16407"
    browser_calls: list[str] = []

    def fake_fetch(url: str, _timeout: int) -> str:
        if url == listing_url:
            return "<main>Careers</main>"
        if url == detail_url:
            return "<h1>Public Relations Assistant</h1><div>Irvine, CA</div>"
        raise AssertionError(f"unexpected URL {url}")

    def fake_browser(url: str, _timeout: int) -> tuple[str, str]:
        browser_calls.append(url)
        return ('<a href="/en-US/careers/16407">Public Relations Assistant</a>', "")

    rows = ncsoft.run(
        fetch_text=fake_fetch,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        pages=[listing_url],
        source_row={
            "id": "static:listing_url:https://nca.ncsoft.com/en-us/careers",
            "name": "NCSoft (Sheet)",
            "company": "NCSoft",
        },
        try_playwright=fake_browser,
    )

    assert browser_calls == [listing_url]
    assert [row["title"] for row in rows] == ["Public Relations Assistant"]


def test_ncsoft_plugin_does_not_swallow_unexpected_listing_fetch_bug() -> None:
    listing_url = "https://nca.ncsoft.com/en-US/careers"

    def broken_fetch(_url: str, _timeout: int) -> str:
        raise AssertionError("broken ncsoft listing fetch")

    with pytest.raises(AssertionError, match="broken ncsoft listing fetch"):
        ncsoft.run(
            fetch_text=broken_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            pages=[listing_url],
            source_row={
                "id": "static:listing_url:https://nca.ncsoft.com/en-us/careers",
                "name": "NCSoft (Sheet)",
                "company": "NCSoft",
            },
        )


def test_ncsoft_plugin_does_not_swallow_unexpected_detail_fetch_bug() -> None:
    listing_url = "https://nca.ncsoft.com/en-US/careers"
    detail_url = "https://nca.ncsoft.com/en-US/careers/16408"
    listing_html = '<a href="/en-US/careers/16408">Customer Service Specialist</a>'

    def broken_detail_fetch(url: str, _timeout: int) -> str:
        if url == listing_url:
            return listing_html
        if url == detail_url:
            raise AssertionError("broken ncsoft detail fetch")
        raise AssertionError(f"unexpected URL {url}")

    with pytest.raises(AssertionError, match="broken ncsoft detail fetch"):
        ncsoft.run(
            fetch_text=broken_detail_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            pages=[listing_url],
            source_row={
                "id": "static:listing_url:https://nca.ncsoft.com/en-us/careers",
                "name": "NCSoft (Sheet)",
                "company": "NCSoft",
            },
        )


def test_rollic_rendered_cards_plugin_uses_browser_after_blocked_http() -> None:
    source_row = {
        "id": "static:listing_url:https://www.rollicgames.com/jobs",
        "name": "Rollic Games (Sheet)",
        "company": "Rollic Games",
    }
    browser_calls: list[str] = []

    def blocked_fetch(_url: str, _timeout: int) -> str:
        raise RuntimeError("HTTP 403 for https://www.rollicgames.com/jobs")

    def fake_browser(url: str, _timeout: int) -> tuple[str, str]:
        browser_calls.append(url)
        return (
            """
            <article>
              <h3>Game Designer</h3>
              <p>Istanbul, Turkiye</p>
              <a href="/jobs/game-designer">Apply</a>
            </article>
            """,
            "",
        )

    assert can_handle_rendered_cards(
        AdapterPluginContext(
            family="static", adapter_key="static", source_identity="www.rollicgames.com"
        )
    )

    rows = run_rendered_cards_plugin(
        fetch_text=blocked_fetch,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        pages=["https://www.rollicgames.com/jobs"],
        source_row=source_row,
        try_playwright=fake_browser,
    )

    assert browser_calls == ["https://www.rollicgames.com/jobs"]
    assert len(rows) == 1
    assert rows[0]["title"] == "Game Designer"
    assert rows[0]["source"] == "Rollic Games (Sheet)"


def test_rendered_cards_plugin_fetch_fallback_does_not_swallow_unexpected_runtime_bug() -> None:
    source_row = {
        "id": "static:listing_url:https://www.rollicgames.com/jobs",
        "name": "Rollic Games (Sheet)",
        "company": "Rollic Games",
    }

    def broken_fetch(_url: str, _timeout: int) -> str:
        raise RuntimeError("unexpected rendered-card fetch bug")

    with pytest.raises(RuntimeError, match="unexpected rendered-card fetch bug"):
        run_rendered_cards_plugin(
            fetch_text=broken_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            pages=["https://www.rollicgames.com/jobs"],
            source_row=source_row,
            try_playwright=lambda _url, _timeout: ("<article></article>", ""),
        )

    assert "_staticPluginMeta" not in source_row
