from __future__ import annotations

import pytest

import src.source_discovery.directory_fetch as directory_fetch
import src.source_discovery.web_search_candidates as web_candidates
from src.source_discovery.directory_page_recovery import http_recovery_request_from_context
from src.source_discovery.page_outcomes import FetchedPageContext

from ._helpers import web_audit_rows


@pytest.mark.parametrize(
    ("url", "adapter", "expected_key", "expected_value"),
    [
        (
            "https://api.smartrecruiters.com/v1/companies/Ubisoft/postings",
            "smartrecruiters",
            "company_id",
            "Ubisoft",
        ),
        ("https://apply.workable.com/supergiant-games/", "workable", "account", "supergiantgames"),
        ("https://example.recruitee.com/o/designer", "recruitee", "subdomain", "example"),
        (
            "https://example.pinpointhq.com/postings/123",
            "pinpoint",
            "api_url",
            "https://example.pinpointhq.com/postings.json",
        ),
        (
            "https://example.teamtailor.com/jobs/123-gameplay",
            "teamtailor",
            "listing_url",
            "https://example.teamtailor.com/jobs",
        ),
        (
            "https://jobs.ashbyhq.com/example-studio/job/123",
            "ashby",
            "board_url",
            "https://jobs.ashbyhq.com/examplestudio",
        ),
        (
            "https://example.jobs.personio.de/job/123",
            "personio",
            "feed_url",
            "https://example.jobs.personio.de/xml",
        ),
    ],
)
def test_infer_web_candidate_covers_provider_url_shapes(
    url: str, adapter: str, expected_key: str, expected_value: str
) -> None:
    row = web_candidates.infer_web_candidate(
        url,
        "Example Studio",
        nl_priority=True,
        discovery_method="web_search",
    )

    assert row is not None
    assert row["adapter"] == adapter
    assert row[expected_key] == expected_value
    assert row["nlPriority"] is True
    assert row["discoveryStage"] == "web_provider"


def test_infer_web_candidate_rejects_invalid_unknown_and_empty_provider_tokens() -> None:
    assert (
        web_candidates.infer_web_candidate("https://example.com/jobs", "Example", nl_priority=False)
        is None
    )
    assert web_candidates.infer_web_candidate("not a url", "Example", nl_priority=False) is None
    assert (
        web_candidates.infer_web_candidate(
            "https://jobs.ashbyhq.com/", "Example", nl_priority=False
        )
        is None
    )


def test_http_recovery_request_preserves_web_shape_and_validation() -> None:
    request = http_recovery_request_from_context(
        FetchedPageContext(
            page_url=" https://studio.example/jobs ",
            html="<html></html>",
            studio="Studio",
            nl_priority=True,
            discovery_method="web_search",
            payload={"nlPriority": True},
            recovery_key="seed-key",
        )
    )

    assert request is not None
    assert request.key == "seed-key"
    assert request.adapter == "web_search"
    assert request.discovery_method == "web_search"
    assert request.name == "Studio"
    assert request.studio == "Studio"
    assert request.page_url == "https://studio.example/jobs"
    assert request.html == "<html></html>"
    assert request.payload == {"nlPriority": True}
    assert (
        http_recovery_request_from_context(
            FetchedPageContext(
                page_url="not a url",
                html="",
                studio="Studio",
                nl_priority=False,
                discovery_method="web_search",
            )
        )
        is None
    )


def test_web_browser_recovery_append_preserves_shape_and_skips_incomplete_rows() -> None:
    rows: list[dict[str, object]] = []

    web_candidates._append_browser_recovery_candidate(
        rows,
        url=" https://studio.example/jobs ",
        studio="Studio",
        nl_priority=True,
        discovery_method="web_search",
        reason_detail="js_shell",
        error="",
    )
    web_candidates._append_browser_recovery_candidate(
        rows,
        url="",
        studio="Studio",
        nl_priority=True,
        discovery_method="web_search",
        reason_detail="js_shell",
    )

    assert rows == [
        {
            "name": "Studio (Browser Recovery)",
            "studio": "Studio",
            "company": "Studio",
            "url": "https://studio.example/jobs",
            "sourceDirectoryEntryUrl": "https://studio.example/jobs",
            "nlPriority": True,
            "discoveryMethod": "web_search",
            "adapter": "web_search",
            "reasonDetail": "js_shell",
            "error": "",
        }
    ]


def test_build_web_search_queries_skips_blank_seeds_adds_site_queries_and_caps_results() -> None:
    seeds = [
        {"studio": ""},
        {
            "studio": "Example Studio",
            "careersUrl": "https://jobs.example.com/careers",
        },
        {"studio": "Second Studio"},
    ]

    queries = web_candidates.build_web_search_queries(seeds, max_queries=20)
    capped = web_candidates.build_web_search_queries(seeds, max_queries=2)

    assert all(query for query, _seed in queries)
    assert queries[0][0] == "Example Studio site:jobs.example.com jobs"
    assert any(query == "Example Studio careers game studio" for query, _seed in queries)
    assert len(capped) == 2
    assert {seed["studio"] for _query, seed in capped} == {"Example Studio"}


def test_infer_provider_candidates_from_html_combines_page_url_and_embedded_urls() -> None:
    rows = web_candidates.infer_provider_candidates_from_html(
        "https://example.teamtailor.com/jobs",
        "<script>const api = 'https://api.lever.co/v0/postings/example?mode=json';</script>",
        studio="Example Studio",
        nl_priority=False,
        discovery_method="seed_careers_page",
    )

    adapters = {row["adapter"] for row in rows}
    assert "teamtailor" in adapters
    assert "lever" in adapters
    assert all("careers_page" in row["evidenceTypes"] for row in rows)


def test_web_audit_seed_careers_records_fetch_failures(monkeypatch) -> None:
    def fake_fetch_directory_pages(*_args, **_kwargs):
        return [
            {
                "ok": False,
                "failure": {
                    "name": "https://example.com/careers",
                    "adapter": "seed_careers_page",
                    "error": "timeout",
                },
            }
        ]

    monkeypatch.setattr(directory_fetch, "fetch_directory_pages", fake_fetch_directory_pages)

    providers, static_rows, failures = web_audit_rows(
        name="web-audit-seed-fetch-failure",
        studio_seeds=[
            {
                "studio": "Example Studio",
                "careersUrl": "https://example.com/careers",
            }
        ],
        fetcher=lambda *_args: "",
        include_seed_careers=True,
        include_web_search=False,
    )

    assert providers == []
    assert static_rows == []
    assert failures == [
        {
            "name": "https://example.com/careers",
            "adapter": "seed_careers_page",
            "error": "timeout",
        }
    ]


def test_seed_careers_page_readiness_preserves_fetch_jobs_and_provenance(monkeypatch) -> None:
    seen_page_jobs = []

    def fake_fetch_directory_pages(_timeout_s, page_jobs, **_kwargs):
        seen_page_jobs.extend(page_jobs)
        job = page_jobs[0]
        return [
            {
                "ok": True,
                "url": job["url"],
                "payload": job["payload"],
                "text": "<script>const board='https://jobs.lever.co/example-studio';</script>",
            }
        ]

    monkeypatch.setattr(directory_fetch, "fetch_directory_pages", fake_fetch_directory_pages)

    providers, static_rows, failures = web_audit_rows(
        name="web-audit-seed-readiness",
        studio_seeds=[
            {
                "studio": "Example Studio",
                "careersUrl": "https://example.com/careers",
                "nlPriority": True,
            }
        ],
        fetcher=lambda *_args: "",
        include_seed_careers=True,
        include_web_search=False,
    )

    assert seen_page_jobs == [
        {
            "url": "https://example.com/careers",
            "payload": {"studio": "Example Studio", "nlPriority": True},
            "name": "https://example.com/careers",
            "adapter": "seed_careers_page",
            "failureStage": "page_fetch",
        }
    ]
    assert static_rows == []
    assert failures == []
    assert len(providers) == 1
    assert providers[0]["adapter"] == "lever"
    assert providers[0]["discoveryMethod"] == "seed_careers_page"
    assert providers[0]["discoveryStage"] == "web_provider"


def test_web_audit_web_search_records_search_and_page_fetch_failures(
    monkeypatch,
) -> None:
    providers, static_rows, failures = web_audit_rows(
        name="web-audit-search-failure",
        studio_seeds=[{"studio": "Example Studio"}],
        fetcher=lambda *_args: (_ for _ in ()).throw(RuntimeError("search blocked")),
        include_seed_careers=False,
        include_web_search=True,
        max_queries=1,
    )

    assert providers == []
    assert static_rows == []
    assert failures == [
        {
            "name": "Example Studio careers game studio",
            "adapter": "web_search",
            "error": "search blocked",
            "stage": "search",
        }
    ]

    def fake_fetch(url: str, _timeout: int) -> str:
        if "duckduckgo.com" in url:
            return '<a href="https://example.com/careers">Careers</a>'
        raise AssertionError(f"unexpected direct fetch: {url}")

    def fake_fetch_directory_pages(*_args, **_kwargs):
        return [
            {
                "ok": False,
                "failure": {
                    "name": "https://example.com/careers",
                    "adapter": "web_search",
                    "error": "page failed",
                },
            }
        ]

    monkeypatch.setattr(directory_fetch, "fetch_directory_pages", fake_fetch_directory_pages)

    providers, static_rows, failures = web_audit_rows(
        name="web-audit-page-fetch-failure",
        studio_seeds=[{"studio": "Example Studio"}],
        fetcher=fake_fetch,
        include_seed_careers=False,
        include_web_search=True,
        max_queries=1,
    )

    assert providers == []
    assert static_rows == []
    assert failures == [
        {
            "name": "https://example.com/careers",
            "adapter": "web_search",
            "error": "page failed",
        }
    ]


def test_web_search_readiness_preserves_page_jobs_failures_and_provenance(monkeypatch) -> None:
    seen_page_jobs = []

    def fake_fetch(url: str, _timeout: int) -> str:
        if "duckduckgo.com" in url:
            return '<a href="https://example.com/careers">Careers</a>'
        raise AssertionError(f"unexpected direct fetch: {url}")

    def fake_fetch_directory_pages(_timeout_s, page_jobs, **_kwargs):
        seen_page_jobs.extend(page_jobs)
        job = page_jobs[0]
        return [
            {
                "ok": True,
                "url": job["url"],
                "payload": job["payload"],
                "text": "<script>const board='https://apply.workable.com/example-studio/';</script>",
            },
            {
                "ok": False,
                "failure": {
                    "name": "https://example.com/extra-careers",
                    "adapter": "web_search",
                    "error": "timeout",
                    "stage": "page_fetch",
                },
            },
        ]

    monkeypatch.setattr(directory_fetch, "fetch_directory_pages", fake_fetch_directory_pages)

    providers, static_rows, failures = web_audit_rows(
        name="web-audit-search-readiness",
        studio_seeds=[{"studio": "Example Studio", "nlPriority": True}],
        fetcher=fake_fetch,
        include_seed_careers=False,
        include_web_search=True,
        max_queries=1,
    )

    assert seen_page_jobs == [
        {
            "url": "https://example.com/careers",
            "payload": {"studio": "Example Studio", "nlPriority": True},
            "name": "https://example.com/careers",
            "adapter": "web_search",
            "failureStage": "page_fetch",
        }
    ]
    assert static_rows == []
    assert failures == [
        {
            "name": "https://example.com/extra-careers",
            "adapter": "web_search",
            "error": "timeout",
            "stage": "page_fetch",
        }
    ]
    assert len(providers) == 1
    assert providers[0]["adapter"] == "workable"
    assert providers[0]["discoveryMethod"] == "web_search"
    assert providers[0]["discoveryStage"] == "web_provider"


def test_web_audit_web_search_uses_direct_provider_links_without_page_fetch(
    monkeypatch,
) -> None:
    seen_page_jobs = []

    def fake_fetch(url: str, _timeout: int) -> str:
        if "duckduckgo.com" in url:
            return '<a href="https://jobs.smartrecruiters.com/ExampleStudio/123">Role</a>'
        raise AssertionError(f"unexpected page fetch: {url}")

    def fake_fetch_directory_pages(_timeout_s, page_jobs, **_kwargs):
        seen_page_jobs.extend(page_jobs)
        return []

    monkeypatch.setattr(directory_fetch, "fetch_directory_pages", fake_fetch_directory_pages)

    providers, static_rows, failures = web_audit_rows(
        name="web-audit-direct-provider-link",
        studio_seeds=[{"studio": "Example Studio", "nlPriority": True}],
        fetcher=fake_fetch,
        include_seed_careers=False,
        include_web_search=True,
        max_queries=1,
    )

    assert seen_page_jobs == []
    assert static_rows == []
    assert failures == []
    assert len(providers) == 1
    assert providers[0]["adapter"] == "smartrecruiters"
    assert providers[0]["company_id"] == "ExampleStudio"
