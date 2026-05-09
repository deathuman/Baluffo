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


def test_static_probe_counts_embedded_lever_board_before_no_jobs() -> None:
    page_html = """
    <h1>Open Positions</h1>
    <ul class="list"></ul>
    <div id="lever-no-results" style="display: none;">No results</div>
    <script>window.leverJobsOptions = {accountName: 'skyboxlabs'};</script>
    """
    lever_payload = """
    [
      {
        "id": "abc123",
        "text": "Senior Gameplay Programmer",
        "hostedUrl": "https://jobs.lever.co/skyboxlabs/abc123",
        "categories": {"team": "Engineering", "location": "Remote"}
      }
    ]
    """
    seen_urls: list[str] = []

    def fake_fetch(url: str, _timeout_s: int, **_kwargs):
        seen_urls.append(url)
        if "api.lever.co" in url:
            return ProbeFetchResponse(200, url, lever_payload)
        return ProbeFetchResponse(200, url, page_html)

    evidence = probe_source_evidence(
        {
            "id": "static:listing_url:https://skyboxlabs.com/jobs/",
            "adapter": "static",
            "listing_url": "https://skyboxlabs.com/jobs/",
        },
        5,
        fetcher=fake_fetch,
    )

    assert seen_urls == [
        "https://skyboxlabs.com/jobs/",
        "https://api.lever.co/v0/postings/skyboxlabs?mode=json",
    ]
    assert evidence.ok is True
    assert evidence.jobs_found == 1
    assert evidence.count_reason == "provider_embed:lever"
    assert evidence.payload_adapter == "lever"
    assert evidence.payload_fields == {
        "adapter": "lever",
        "account": "skyboxlabs",
        "api_url": "https://api.lever.co/v0/postings/skyboxlabs?mode=json",
    }


def test_static_probe_counts_ubisoft_algolia_search_page() -> None:
    page_html = """
    <section id="jobsSearch"></section>
    <script>
      window.__config = {
        "AlgoliaAppId": "AVCVYSEJS1",
        "AlgoliaApiKey": "d2ec5782c4eb549092cfa4ed5062599a"
      };
    </script>
    """
    algolia_payload = """
    {
      "nbHits": 136,
      "hits": [
        {
          "objectID": "job-1",
          "title": "Gameplay Programmer",
          "link": "https://www.ubisoft.com/en-us/company/careers/search/744-job"
        }
      ]
    }
    """
    seen_urls: list[str] = []
    seen_headers: list[dict[str, str]] = []

    def fake_fetch(url: str, _timeout_s: int, *, headers: dict[str, str]):
        seen_urls.append(url)
        seen_headers.append(headers)
        if "algolia.net" in url:
            return ProbeFetchResponse(200, url, algolia_payload)
        return ProbeFetchResponse(200, url, page_html)

    evidence = probe_source_evidence(
        {
            "id": "static:listing_url:https://www.ubisoft.com/en-us/company/careers/search",
            "adapter": "static",
            "listing_url": "https://www.ubisoft.com/en-us/company/careers/search",
        },
        5,
        fetcher=fake_fetch,
    )

    assert seen_urls[0] == "https://www.ubisoft.com/en-us/company/careers/search"
    assert seen_urls[1].startswith(
        "https://AVCVYSEJS1-dsn.algolia.net/1/indexes/jobs_en-us_default?"
    )
    assert seen_headers[1]["X-Algolia-Application-Id"] == "AVCVYSEJS1"
    assert evidence.ok is True
    assert evidence.jobs_found == 136
    assert evidence.count_reason == "provider_embed:ubisoft_algolia"
    assert evidence.sample_urls == ("https://www.ubisoft.com/en-us/company/careers/search/744-job",)


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
