from __future__ import annotations

import pytest

from tests.helpers import jobs_reporting


def _site_changed_report() -> dict[str, object]:
    return {
        "name": "scrapy_static_sources",
        "studio": "Site Changed Studio",
        "adapter": "scrapy_static",
        "status": "ok",
        "failureBucket": "site_changed",
        "listingUrl": "https://example.com/careers",
        "sourceId": "static:site-changed",
    }


def test_parser_regression_queue_suppresses_expected_redirect_failure() -> None:
    def fail_redirect(_url: str) -> str:
        raise OSError("redirect resolver unavailable")

    rows = jobs_reporting.build_parser_regression_queue(
        [_site_changed_report()],
        generated_at="2026-03-28T12:00:00+00:00",
        resolve_redirect_url=fail_redirect,
    )

    assert len(rows) == 1
    assert str(rows[0].get("oldUrl") or "") == "https://example.com/careers"
    assert "currentUrl" not in rows[0]


def test_parser_regression_queue_does_not_swallow_unexpected_redirect_failure() -> None:
    def fail_redirect(_url: str) -> str:
        raise AssertionError("unexpected redirect resolver bug")

    with pytest.raises(AssertionError, match="unexpected redirect resolver bug"):
        jobs_reporting.build_parser_regression_queue(
            [_site_changed_report()],
            generated_at="2026-03-28T12:00:00+00:00",
            resolve_redirect_url=fail_redirect,
        )
