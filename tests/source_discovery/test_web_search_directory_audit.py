from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import src.source_discovery.web_search_candidates as web_candidates
from src.source_discovery import directory_audit

from ._helpers import workspace_tmpdir


def _audit_config(
    audit_path: str,
    *,
    max_queries: int | None = None,
    max_links_per_query: int | None = None,
) -> dict[str, object]:
    web_search: dict[str, object] = {
        "activeAuditEnabled": True,
        "activeAuditPath": audit_path,
        "activeAuditTtlMinutes": 60,
    }
    if max_queries is not None:
        web_search["maxQueries"] = max_queries
    if max_links_per_query is not None:
        web_search["maxLinksPerQuery"] = max_links_per_query
    return {
        "webSearch": {
            **web_search,
        }
    }


def _seeds() -> list[dict[str, object]]:
    return [
        {
            "studio": "Seed Studio",
            "careersUrl": "https://seed.example/careers",
            "nlPriority": True,
        },
        {
            "studio": "Search Studio",
            "nlPriority": False,
        },
    ]


def _fetcher(url: str, _timeout_s: int) -> str:
    if url == "https://seed.example/careers":
        return '<a href="https://boards.greenhouse.io/seedstudio/jobs/1">Role</a>'
    if "duckduckgo.com" in url:
        return '<a href="https://search.example/careers">Careers</a>'
    if url == "https://search.example/careers":
        return '<a href="https://boards.greenhouse.io/searchstudio/jobs/1">Role</a>'
    raise RuntimeError(f"unexpected URL: {url}")


def test_web_search_directory_audit_missing_artifact_runs_both_substages() -> None:
    with workspace_tmpdir("web-search-audit-missing") as root:
        audit_path = root / "web-audit.json"

        artifact, cache_hit = web_candidates.run_web_search_directory_audit(
            5,
            studio_seeds=_seeds(),
            include_seed_careers=True,
            include_web_search=True,
            config=_audit_config(str(audit_path)),
            fetcher=_fetcher,
            max_queries=1,
        )

        assert cache_hit is False
        assert audit_path.exists()
        assert artifact["adapter"] == "web_search"
        assert artifact["schemaVersion"] == 1
        assert artifact["progress"]["complete"] is True
        assert artifact["progress"]["cursor"] == 2
        assert artifact["summary"]["seedCareersEnabled"] is True
        assert artifact["summary"]["webSearchEnabled"] is True
        assert artifact["summary"]["seedRows"] == 2
        assert artifact["summary"]["seedPageFetchJobs"] == 1
        assert artifact["summary"]["webQueriesPlanned"] == 1
        assert artifact["summary"]["maxQueries"] == 1
        assert artifact["summary"]["maxLinksPerQuery"] == 8
        assert artifact["summary"]["webPageFetchJobs"] == 1
        assert artifact["summary"]["webLinksExtracted"] == 1
        assert artifact["summary"]["webLinksConsidered"] == 1
        assert artifact["summary"]["webJobishLinks"] == 1
        assert artifact["summary"]["webNonJobishLinksSkipped"] == 0
        assert artifact["summary"]["webDuplicatePageFetchUrls"] == 0
        assert artifact["summary"]["webPageFetchFailures"] == 0
        assert artifact["summary"]["webQuerySamples"] == [
            {"query": "Seed Studio site:seed.example jobs", "studio": "Seed Studio"}
        ]
        assert artifact["summary"]["webFailureSamples"] == []
        assert artifact["summary"]["providerCandidates"] == 2
        assert artifact["summary"]["staticCandidates"] == 0
        assert artifact["summary"]["failures"] == 0
        assert artifact["timings"]["totalsMs"]["seedPageFetchMs"] >= 0
        assert artifact["timings"]["totalsMs"]["webSearchFetchMs"] >= 0
        methods = {row["discoveryMethod"] for row in artifact["providerCandidates"]}
        assert methods == {"seed_careers_page", "web_search"}


def test_web_search_directory_audit_reuses_fresh_artifact_without_fetch() -> None:
    with workspace_tmpdir("web-search-audit-reuse") as root:
        audit_path = root / "web-audit.json"
        config = _audit_config(str(audit_path))

        first_artifact, first_cache_hit = web_candidates.run_web_search_directory_audit(
            5,
            studio_seeds=_seeds(),
            include_seed_careers=True,
            include_web_search=True,
            config=config,
            fetcher=_fetcher,
            max_queries=1,
        )
        second_artifact, second_cache_hit = web_candidates.run_web_search_directory_audit(
            5,
            studio_seeds=_seeds(),
            include_seed_careers=True,
            include_web_search=True,
            config=config,
            fetcher=lambda *_args: (_ for _ in ()).throw(
                AssertionError("fresh web-search audit artifact should bypass fetch")
            ),
            max_queries=1,
        )

        assert first_cache_hit is False
        assert second_cache_hit is True
        assert second_artifact == first_artifact


def test_web_search_directory_audit_tuning_config_changes_signature() -> None:
    with workspace_tmpdir("web-search-audit-tuning-signature") as root:
        audit_path = root / "web-audit.json"
        calls = {"count": 0}

        def counting_fetch(url: str, _timeout_s: int) -> str:
            calls["count"] += 1
            if "duckduckgo.com" in url:
                return '<a href="https://search.example/careers">Careers</a>'
            if url == "https://search.example/careers":
                return '<a href="https://boards.greenhouse.io/searchstudio/jobs/1">Role</a>'
            raise RuntimeError(f"unexpected URL: {url}")

        first_artifact, first_cache_hit = web_candidates.run_web_search_directory_audit(
            5,
            studio_seeds=[{"studio": "Search Studio"}],
            include_seed_careers=False,
            include_web_search=True,
            config=_audit_config(str(audit_path), max_queries=1, max_links_per_query=1),
            fetcher=counting_fetch,
        )
        calls_after_first = calls["count"]
        second_artifact, second_cache_hit = web_candidates.run_web_search_directory_audit(
            5,
            studio_seeds=[{"studio": "Search Studio"}],
            include_seed_careers=False,
            include_web_search=True,
            config=_audit_config(str(audit_path), max_queries=1, max_links_per_query=1),
            fetcher=lambda *_args: (_ for _ in ()).throw(
                AssertionError("matching web-search tuning should reuse the audit artifact")
            ),
        )
        third_artifact, third_cache_hit = web_candidates.run_web_search_directory_audit(
            5,
            studio_seeds=[{"studio": "Search Studio"}],
            include_seed_careers=False,
            include_web_search=True,
            config=_audit_config(str(audit_path), max_queries=1, max_links_per_query=2),
            fetcher=counting_fetch,
        )

        assert first_cache_hit is False
        assert second_cache_hit is True
        assert third_cache_hit is False
        assert second_artifact == first_artifact
        assert third_artifact["runtime"]["configSignature"]["maxLinksPerQuery"] == 2
        assert calls["count"] > calls_after_first


def test_web_search_directory_audit_records_link_diagnostics_and_caps_samples() -> None:
    with workspace_tmpdir("web-search-audit-link-diagnostics") as root:
        audit_path = root / "web-audit.json"

        def diagnostic_fetch(url: str, _timeout_s: int) -> str:
            if "duckduckgo.com" in url:
                return "".join(
                    [
                        '<a href="https://noise.example/about">About</a>',
                        '<a href="https://search.example/careers">Careers</a>',
                        '<a href="https://search.example/careers">Careers Duplicate</a>',
                        '<a href="https://jobs.smartrecruiters.com/SearchStudio/123">Role</a>',
                    ]
                )
            if url == "https://search.example/careers":
                return '<a href="https://boards.greenhouse.io/searchstudio/jobs/1">Role</a>'
            raise RuntimeError(f"unexpected URL: {url}")

        artifact, _cache_hit = web_candidates.run_web_search_directory_audit(
            5,
            studio_seeds=[{"studio": f"Search Studio {index}"} for index in range(30)],
            include_seed_careers=False,
            include_web_search=True,
            config=_audit_config(str(audit_path), max_queries=30, max_links_per_query=4),
            fetcher=diagnostic_fetch,
        )

        summary = artifact["summary"]
        assert summary["webQueriesPlanned"] == 30
        assert summary["webSearchSuccesses"] == 30
        assert summary["webLinksExtracted"] == 120
        assert summary["webLinksConsidered"] == 120
        assert summary["webDirectProviderLinks"] == 30
        assert summary["webJobishLinks"] == 60
        assert summary["webNonJobishLinksSkipped"] == 30
        assert summary["webDuplicatePageFetchUrls"] == 59
        assert summary["webPageFetchJobs"] == 1
        assert len(summary["webQuerySamples"]) == 25
        assert summary["webFailureSamples"] == []


def test_web_search_directory_audit_reruns_stale_wrong_schema_incomplete_or_signature_mismatch() -> (
    None
):
    cases = [
        {"schemaVersion": 0},
        {"schemaVersion": 1, "progress": {"complete": False}},
        {"schemaVersion": 1, "runtime": {"configSignature": {"maxQueries": 99}}},
        {
            "schemaVersion": 1,
            "updatedAt": (datetime.now(UTC) - timedelta(minutes=90)).isoformat(),
        },
    ]

    for index, existing in enumerate(cases):
        with workspace_tmpdir(f"web-search-audit-rerun-{index}") as root:
            audit_path = root / "web-audit.json"
            payload = {
                "schemaVersion": 1,
                "updatedAt": datetime.now(UTC).isoformat(),
                "progress": {"complete": True},
                "runtime": {"configSignature": {}},
                **existing,
            }
            audit_path.write_text(json.dumps(payload), encoding="utf-8")

            artifact, cache_hit = web_candidates.run_web_search_directory_audit(
                5,
                studio_seeds=_seeds(),
                include_seed_careers=True,
                include_web_search=True,
                config=_audit_config(str(audit_path)),
                fetcher=_fetcher,
                max_queries=1,
            )

            assert cache_hit is False
            assert artifact["summary"]["providerCandidates"] == 2


def test_web_search_directory_audit_output_matches_legacy_scans() -> None:
    with workspace_tmpdir("web-search-audit-equivalence") as root:
        audit_path = root / "web-audit.json"

        legacy_seed = web_candidates.discover_seed_careers_page_candidates(
            5,
            studio_seeds=_seeds(),
            fetcher=_fetcher,
        )
        legacy_web = web_candidates.discover_web_search_candidates(
            5,
            studio_seeds=_seeds(),
            fetcher=_fetcher,
            max_queries=1,
        )
        artifact, _cache_hit = web_candidates.run_web_search_directory_audit(
            5,
            studio_seeds=_seeds(),
            include_seed_careers=True,
            include_web_search=True,
            config=_audit_config(str(audit_path)),
            fetcher=_fetcher,
            max_queries=1,
        )
        audit_rows = directory_audit.directory_audit_rows(artifact)

        assert audit_rows == (
            [*legacy_seed[0], *legacy_web[0]],
            [*legacy_seed[1], *legacy_web[1]],
            [*legacy_seed[2], *legacy_web[2]],
        )


def test_web_search_directory_audit_records_search_and_page_fetch_failures() -> None:
    with workspace_tmpdir("web-search-audit-failures") as root:
        audit_path = root / "web-audit.json"

        def failing_fetch(url: str, _timeout_s: int) -> str:
            if "duckduckgo.com" in url:
                raise RuntimeError("search blocked")
            raise RuntimeError("page blocked")

        artifact, _cache_hit = web_candidates.run_web_search_directory_audit(
            5,
            studio_seeds=_seeds(),
            include_seed_careers=True,
            include_web_search=True,
            config=_audit_config(str(audit_path)),
            fetcher=failing_fetch,
            max_queries=1,
        )

        assert artifact["summary"]["failures"] == 2
        assert artifact["failureCounts"] == {"page_fetch": 1, "search": 1}
        assert artifact["summary"]["seedFailures"] == 1
        assert artifact["summary"]["webFailures"] == 1
        assert artifact["summary"]["webSearchFailures"] == 1
        assert artifact["summary"]["webPageFetchFailures"] == 0
        assert artifact["summary"]["webFailureSamples"][0]["stage"] == "search"


def test_web_search_directory_audit_supports_seed_only_and_web_only() -> None:
    with workspace_tmpdir("web-search-audit-seed-only") as root:
        artifact, _cache_hit = web_candidates.run_web_search_directory_audit(
            5,
            studio_seeds=_seeds(),
            include_seed_careers=True,
            include_web_search=False,
            config=_audit_config(str(root / "seed-audit.json")),
            fetcher=_fetcher,
            max_queries=1,
        )
        assert artifact["summary"]["seedCareersEnabled"] is True
        assert artifact["summary"]["webSearchEnabled"] is False
        assert {row["discoveryMethod"] for row in artifact["providerCandidates"]} == {
            "seed_careers_page"
        }

    with workspace_tmpdir("web-search-audit-web-only") as root:
        artifact, _cache_hit = web_candidates.run_web_search_directory_audit(
            5,
            studio_seeds=_seeds(),
            include_seed_careers=False,
            include_web_search=True,
            config=_audit_config(str(root / "web-audit.json")),
            fetcher=_fetcher,
            max_queries=1,
        )
        assert artifact["summary"]["seedCareersEnabled"] is False
        assert artifact["summary"]["webSearchEnabled"] is True
        assert {row["discoveryMethod"] for row in artifact["providerCandidates"]} == {"web_search"}
