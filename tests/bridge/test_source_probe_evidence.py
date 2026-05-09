from __future__ import annotations

from urllib.error import HTTPError

from src.bridge.source_probe_evidence import (
    ProbeFetchResponse,
    probe_source_evidence,
)


def test_static_probe_uses_browser_like_headers_for_krafton_style_403() -> None:
    seen_headers: list[dict[str, str]] = []
    html = """
    <a href="/careers/jobs/producer-123/">Producer</a>
    <a href="/careers/jobs/designer-456/">Designer</a>
    """

    def fake_fetch(url: str, _timeout_s: int, *, headers: dict[str, str]):
        seen_headers.append(headers)
        if "Mozilla/5.0" not in headers.get("User-Agent", ""):
            raise HTTPError(url, 403, "Forbidden", {}, None)
        return ProbeFetchResponse(200, url, html)

    evidence = probe_source_evidence(
        {
            "id": "static:listing_url:https://www.krafton.com/careers/jobs/",
            "adapter": "static",
            "listing_url": "https://www.krafton.com/careers/jobs/",
        },
        5,
        fetcher=fake_fetch,
    )

    assert seen_headers
    assert evidence.ok is True
    assert evidence.jobs_found == 2
    assert evidence.error == ""


def test_static_probe_evidence_reports_high_confidence_no_openings() -> None:
    evidence = probe_source_evidence(
        {
            "id": "static:listing_url:https://studio.example/jobs",
            "adapter": "static",
            "listing_url": "https://studio.example/jobs",
        },
        5,
        fetcher=lambda url, _timeout_s, **_kwargs: ProbeFetchResponse(
            200, url, "<html><body>Currently no open positions</body></html>"
        ),
    )

    assert evidence.ok is True
    assert evidence.jobs_found == 0
    assert evidence.count_confidence == "high"
    assert evidence.count_reason == "no_openings"


def test_static_probe_evidence_keeps_jsonld_only_weak() -> None:
    html = """
    <script type="application/ld+json">
      {"@type":"JobPosting","title":"Gameplay Programmer","url":"https://studio.example/jobs/1"}
    </script>
    """

    evidence = probe_source_evidence(
        {
            "id": "static:listing_url:https://studio.example/jobs",
            "adapter": "static",
            "listing_url": "https://studio.example/jobs",
        },
        5,
        fetcher=lambda url, _timeout_s, **_kwargs: ProbeFetchResponse(200, url, html),
    )

    assert evidence.ok is True
    assert evidence.jobs_found == 0
    assert evidence.count_confidence == "weak"
    assert evidence.count_reason == "jsonld_only"


def test_static_probe_403_without_browser_returns_fallback_recommended() -> None:
    def blocked(url: str, _timeout_s: int, **_kwargs):
        raise HTTPError(url, 403, "Forbidden", {}, None)

    evidence = probe_source_evidence(
        {
            "id": "static:listing_url:https://www.krafton.com/careers/jobs/",
            "adapter": "static",
            "listing_url": "https://www.krafton.com/careers/jobs/",
        },
        5,
        fetcher=blocked,
    )

    assert evidence.ok is False
    assert evidence.http_status == 403
    assert evidence.browser_fallback_recommended is True


def test_static_probe_403_can_use_playwright_fallback() -> None:
    def blocked(url: str, _timeout_s: int, **_kwargs):
        raise HTTPError(url, 403, "Forbidden", {}, None)

    evidence = probe_source_evidence(
        {
            "id": "static:listing_url:https://www.krafton.com/careers/jobs/",
            "adapter": "static",
            "listing_url": "https://www.krafton.com/careers/jobs/",
        },
        5,
        fetcher=blocked,
        try_playwright=lambda _url, _timeout_s: (
            '<a href="/careers/jobs/designer-456/">Designer</a>',
            "",
        ),
    )

    assert evidence.ok is True
    assert evidence.jobs_found == 1
    assert evidence.browser_fallback_used is True


def test_provider_compact_id_reconstructs_api_and_skips_browser_fallback() -> None:
    seen_urls: list[str] = []

    def fake_fetch(url: str, _timeout_s: int, **_kwargs):
        seen_urls.append(url)
        return ProbeFetchResponse(200, url, '{"content":[{"id":"1","name":"Designer"}]}')

    def fail_playwright(_url: str, _timeout_s: int):
        raise AssertionError("provider probes should not use browser fallback")

    row = {
        "id": "smartrecruiters:company_id:cdprojektred",
        "adapter": "smartrecruiters",
    }
    original = dict(row)
    evidence = probe_source_evidence(
        row,
        5,
        fetcher=fake_fetch,
        try_playwright=fail_playwright,
    )

    assert row == original
    assert seen_urls == ["https://api.smartrecruiters.com/v1/companies/cdprojektred/postings"]
    assert evidence.ok is True
    assert evidence.jobs_found == 1
