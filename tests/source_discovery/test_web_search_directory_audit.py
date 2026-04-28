from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import src.source_discovery.web_search_candidates as web_candidates
from src.source_discovery import directory_audit

from ._helpers import workspace_tmpdir, write_web_search_browser_recovery_artifact


def _audit_config(
    audit_path: str,
    *,
    max_queries: int | None = None,
    max_links_per_query: int | None = None,
    recovery_enabled: bool | None = None,
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
    if recovery_enabled is not None:
        web_search["activeAuditRecoveryEnabled"] = bool(recovery_enabled)
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
        assert artifact["schemaVersion"] == web_candidates.WEB_SEARCH_AUDIT_SCHEMA_VERSION
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
        assert artifact["summary"]["browserRecoveryCandidates"] == 0
        assert "recoveryFetchAttempts" not in artifact["summary"]
        assert artifact["browserRecoveryCandidates"] == []
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
        {
            "schemaVersion": web_candidates.WEB_SEARCH_AUDIT_SCHEMA_VERSION,
            "progress": {"complete": False},
        },
        {
            "schemaVersion": web_candidates.WEB_SEARCH_AUDIT_SCHEMA_VERSION,
            "runtime": {"configSignature": {"maxQueries": 99}},
        },
        {
            "schemaVersion": web_candidates.WEB_SEARCH_AUDIT_SCHEMA_VERSION,
            "updatedAt": (datetime.now(UTC) - timedelta(minutes=90)).isoformat(),
        },
    ]

    for index, existing in enumerate(cases):
        with workspace_tmpdir(f"web-search-audit-rerun-{index}") as root:
            audit_path = root / "web-audit.json"
            payload = {
                "schemaVersion": web_candidates.WEB_SEARCH_AUDIT_SCHEMA_VERSION,
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


def test_web_search_directory_audit_records_browser_recovery_candidates() -> None:
    with workspace_tmpdir("web-search-audit-browser-candidates") as root:
        audit_path = root / "web-audit.json"

        def browser_candidate_fetch(url: str, _timeout_s: int) -> str:
            if url == "https://seed.example/careers":
                return '<html><div id="root"></div><script src="/app.js"></script></html>'
            if "duckduckgo.com" in url:
                return '<a href="https://search.example/careers">Careers</a>'
            raise RuntimeError("403 forbidden")

        artifact, _cache_hit = web_candidates.run_web_search_directory_audit(
            5,
            studio_seeds=_seeds(),
            include_seed_careers=True,
            include_web_search=True,
            config=_audit_config(str(audit_path)),
            fetcher=browser_candidate_fetch,
            max_queries=1,
        )

        assert artifact["summary"]["browserRecoveryCandidates"] == 2
        assert artifact["summary"]["browserRecoveryJsShellCandidates"] == 1
        assert artifact["summary"]["browserRecoveryFetchFailureCandidates"] == 1
        assert {row["reasonDetail"] for row in artifact["browserRecoveryCandidates"]} == {
            "js_shell",
            "browser_recovery_fetch_failed",
        }


def test_web_search_directory_audit_default_seed_recovery_finds_static_candidate() -> None:
    with workspace_tmpdir("web-search-audit-seed-http-recovery-default") as root:
        audit_path = root / "web-audit.json"

        def fetcher(url: str, _timeout_s: int) -> str:
            if url == "https://seed-recover.example/":
                return "<html><body>Seed Recover Studio</body></html>"
            if url == "https://seed-recover.example/careers":
                return '<a href="/jobs/designer">Designer</a><a href="/jobs/engineer">Engineer</a>'
            if url == "https://seed-recover.example/jobs":
                return "<html><body>No roles</body></html>"
            raise RuntimeError(f"unexpected URL: {url}")

        config = _audit_config(str(audit_path))
        artifact, cache_hit = web_candidates.run_web_search_directory_audit(
            5,
            studio_seeds=[
                {
                    "studio": "Seed Recover Studio",
                    "careersUrl": "https://seed-recover.example/",
                    "nlPriority": True,
                }
            ],
            include_seed_careers=True,
            include_web_search=False,
            config=config,
            fetcher=fetcher,
        )
        second_artifact, second_cache_hit = web_candidates.run_web_search_directory_audit(
            5,
            studio_seeds=[
                {
                    "studio": "Seed Recover Studio",
                    "careersUrl": "https://seed-recover.example/",
                    "nlPriority": True,
                }
            ],
            include_seed_careers=True,
            include_web_search=False,
            config=config,
            fetcher=lambda *_args: (_ for _ in ()).throw(
                AssertionError("fresh default recovery web artifact should bypass fetch")
            ),
        )

        assert cache_hit is False
        assert second_cache_hit is True
        assert second_artifact == artifact
        assert artifact["summary"]["recoveryFetchAttempts"] == 2
        assert artifact["summary"]["recoveredStaticCandidates"] == 1
        assert artifact["summary"]["seedStaticCandidates"] == 1
        assert artifact["summary"]["failures"] == 0
        assert artifact["staticCandidates"][0]["listing_url"] == (
            "https://seed-recover.example/careers"
        )
        assert artifact["staticCandidates"][0]["discoveryMethod"] == "seed_careers_page"
        assert artifact["timings"]["totalsMs"]["seedRecoveryFetchMs"] >= 0


def test_web_search_directory_audit_default_web_recovery_finds_static_candidate() -> None:
    with workspace_tmpdir("web-search-audit-web-http-recovery-default") as root:
        audit_path = root / "web-audit.json"

        def fetcher(url: str, _timeout_s: int) -> str:
            if "duckduckgo.com" in url:
                return '<a href="https://web-recover.example/careers">Careers</a>'
            if url == "https://web-recover.example/careers":
                return "<html><body>Web Recover Studio</body></html>"
            if url == "https://web-recover.example/jobs":
                return '<a href="/jobs/designer">Designer</a><a href="/jobs/engineer">Engineer</a>'
            raise RuntimeError(f"unexpected URL: {url}")

        artifact, _cache_hit = web_candidates.run_web_search_directory_audit(
            5,
            studio_seeds=[{"studio": "Web Recover Studio"}],
            include_seed_careers=False,
            include_web_search=True,
            config=_audit_config(str(audit_path)),
            fetcher=fetcher,
            max_queries=1,
        )

        assert artifact["summary"]["recoveryFetchAttempts"] == 2
        assert artifact["summary"]["recoveredStaticCandidates"] == 1
        assert artifact["summary"]["webStaticCandidates"] == 1
        assert artifact["staticCandidates"][0]["listing_url"] == "https://web-recover.example/jobs"
        assert artifact["staticCandidates"][0]["discoveryMethod"] == "web_search"
        assert artifact["timings"]["totalsMs"]["webRecoveryFetchMs"] >= 0


def test_web_search_directory_audit_explicit_recovery_false_preserves_no_recovery_output() -> None:
    with workspace_tmpdir("web-search-audit-http-recovery-explicit-false") as root:
        audit_path = root / "web-audit.json"

        def fetcher(url: str, _timeout_s: int) -> str:
            if url == "https://no-recovery.example/":
                return "<html><body>No roles</body></html>"
            raise RuntimeError(f"unexpected recovery fetch: {url}")

        artifact, _cache_hit = web_candidates.run_web_search_directory_audit(
            5,
            studio_seeds=[
                {
                    "studio": "No Recovery Studio",
                    "careersUrl": "https://no-recovery.example/",
                }
            ],
            include_seed_careers=True,
            include_web_search=False,
            config=_audit_config(str(audit_path), recovery_enabled=False),
            fetcher=fetcher,
        )

        assert "recoveryFetchAttempts" not in artifact["summary"]
        assert artifact["summary"]["providerCandidates"] == 0
        assert artifact["summary"]["staticCandidates"] == 0
        assert artifact["summary"]["failures"] == 0


def test_web_search_directory_audit_opt_in_recovery_miss_and_failure_are_diagnostic_only() -> None:
    with workspace_tmpdir("web-search-audit-http-recovery-miss-failure") as root:
        audit_path = root / "web-audit.json"

        def fetcher(url: str, _timeout_s: int) -> str:
            if url == "https://seed-miss.example/":
                return "<html><body>No roles</body></html>"
            if url.startswith("https://seed-miss.example/"):
                return "<html><body>Still no roles</body></html>"
            if "duckduckgo.com" in url:
                return '<a href="https://web-fail.example/careers">Careers</a>'
            if url == "https://web-fail.example/careers":
                return "<html><body>No roles</body></html>"
            if url.startswith("https://web-fail.example/"):
                raise RuntimeError("recovery timeout")
            raise RuntimeError(f"unexpected URL: {url}")

        artifact, _cache_hit = web_candidates.run_web_search_directory_audit(
            5,
            studio_seeds=[
                {
                    "studio": "Seed Miss Studio",
                    "careersUrl": "https://seed-miss.example/",
                },
                {"studio": "Web Fail Studio"},
            ],
            include_seed_careers=True,
            include_web_search=True,
            config=_audit_config(str(audit_path), recovery_enabled=True),
            fetcher=fetcher,
            max_queries=1,
        )

        assert artifact["summary"]["recoveryFetchAttempts"] == 12
        assert artifact["summary"]["recoveredStaticCandidates"] == 0
        assert artifact["summary"]["recoveryFailures"] == 5
        assert artifact["summary"]["failures"] == 0
        assert artifact["providerCandidates"] == []
        assert artifact["staticCandidates"] == []


def test_web_search_directory_audit_opt_in_recovery_skips_fetch_failures_and_js_shells() -> None:
    with workspace_tmpdir("web-search-audit-http-recovery-skip-diagnostics") as root:
        audit_path = root / "web-audit.json"
        shell = '<html><div id="root"></div><script src="/app.js"></script></html>'

        def fetcher(url: str, _timeout_s: int) -> str:
            if url == "https://seed-shell.example/careers":
                return shell
            if "duckduckgo.com" in url:
                return '<a href="https://web-fail.example/careers">Careers</a>'
            if url == "https://web-fail.example/careers":
                raise RuntimeError("429 web page")
            raise RuntimeError(f"unexpected URL: {url}")

        artifact, _cache_hit = web_candidates.run_web_search_directory_audit(
            5,
            studio_seeds=[
                {
                    "studio": "Seed Shell Studio",
                    "careersUrl": "https://seed-shell.example/careers",
                },
                {"studio": "Web Fetch Fail Studio"},
            ],
            include_seed_careers=True,
            include_web_search=True,
            config=_audit_config(str(audit_path), recovery_enabled=True),
            fetcher=fetcher,
            max_queries=1,
        )

        assert "recoveryFetchAttempts" not in artifact["summary"]
        assert artifact["summary"]["browserRecoveryCandidates"] == 2
        assert artifact["summary"]["browserRecoveryJsShellCandidates"] == 1
        assert artifact["summary"]["browserRecoveryFetchFailureCandidates"] == 1
        assert artifact["summary"]["failures"] == 1
        assert artifact["failureCounts"] == {"page_fetch": 1}


def test_web_search_directory_audit_recovery_toggle_changes_signature() -> None:
    with workspace_tmpdir("web-search-audit-http-recovery-signature") as root:
        audit_path = root / "web-audit.json"

        def disabled_fetcher(url: str, _timeout_s: int) -> str:
            if url == "https://signature.example/":
                return "<html><body>No roles</body></html>"
            raise RuntimeError(f"unexpected URL: {url}")

        first_artifact, first_cache_hit = web_candidates.run_web_search_directory_audit(
            5,
            studio_seeds=[
                {
                    "studio": "Signature Studio",
                    "careersUrl": "https://signature.example/",
                }
            ],
            include_seed_careers=True,
            include_web_search=False,
            config=_audit_config(str(audit_path), recovery_enabled=False),
            fetcher=disabled_fetcher,
        )

        def enabled_fetcher(url: str, _timeout_s: int) -> str:
            if url == "https://signature.example/":
                return "<html><body>No roles</body></html>"
            if url == "https://signature.example/careers":
                return '<a href="/jobs/engineer">Engineer</a>'
            if url == "https://signature.example/jobs":
                return "<html><body>No roles</body></html>"
            raise RuntimeError(f"unexpected URL: {url}")

        second_artifact, second_cache_hit = web_candidates.run_web_search_directory_audit(
            5,
            studio_seeds=[
                {
                    "studio": "Signature Studio",
                    "careersUrl": "https://signature.example/",
                }
            ],
            include_seed_careers=True,
            include_web_search=False,
            config=_audit_config(str(audit_path), recovery_enabled=True),
            fetcher=enabled_fetcher,
        )

        assert first_cache_hit is False
        assert second_cache_hit is False
        assert "recoveryFetchAttempts" not in first_artifact["summary"]
        assert second_artifact["summary"]["recoveryFetchAttempts"] == 2


def test_web_search_browser_recovery_merges_only_validated_rendered_sources() -> None:
    with workspace_tmpdir("web-search-browser-recovery") as root:
        audit_path = root / "web-audit.json"

        def shell_fetch(url: str, _timeout_s: int) -> str:
            if url == "https://seed.example/careers":
                return '<html><div id="root"></div><script src="/app.js"></script></html>'
            raise RuntimeError(f"unexpected URL: {url}")

        artifact, _cache_hit = web_candidates.run_web_search_directory_audit(
            5,
            studio_seeds=[_seeds()[0]],
            include_seed_careers=True,
            include_web_search=False,
            config=_audit_config(str(audit_path)),
            fetcher=shell_fetch,
        )
        assert artifact["summary"]["browserRecoveryCandidates"] == 1

        calls: list[str] = []

        def fake_browser(url: str, _timeout_s: int) -> tuple[str, str]:
            calls.append(url)
            return '<a href="/jobs/rendering-engineer">Rendering Engineer</a>', ""

        recovered = web_candidates.run_web_search_browser_recovery(
            5,
            config=_audit_config(str(audit_path)),
            output_path=audit_path,
            browser_fetcher=fake_browser,
            fetcher=lambda *_args: "",
        )

        assert calls == ["https://seed.example/careers"]
        assert recovered["browserRecovery"]["processedCount"] == 1
        assert recovered["browserRecovery"]["activeCandidates"] == 1
        assert recovered["summary"]["browserRecoveredActiveCandidates"] == 1
        assert recovered["staticCandidates"][0]["prevalidatedDiscovery"] is True
        assert recovered["staticCandidates"][0]["probeStatus"] == "ok"
        assert recovered["staticCandidates"][0]["queueAdapterCapOverride"] == 500
        assert recovered["staticCandidates"][0]["queueDomainCapOverride"] == 8


def test_web_search_browser_recovery_keeps_zero_and_failed_rows_as_diagnostics() -> None:
    with workspace_tmpdir("web-search-browser-recovery-diagnostics") as root:
        audit_path = root / "web-audit.json"
        write_web_search_browser_recovery_artifact(
            audit_path,
            schema_version=web_candidates.WEB_SEARCH_AUDIT_SCHEMA_VERSION,
            browser_candidates=[
                {
                    "name": "Zero Studio",
                    "studio": "Zero Studio",
                    "url": "https://zero.example/jobs",
                    "discoveryMethod": "web_search",
                },
                {
                    "name": "Failed Studio",
                    "studio": "Failed Studio",
                    "url": "https://failed.example/jobs",
                    "discoveryMethod": "web_search",
                },
            ],
        )

        def fake_browser(url: str, _timeout_s: int) -> tuple[str, str]:
            if "failed" in url:
                return "", "browser failed"
            return "<html>No jobs here</html>", ""

        recovered = web_candidates.run_web_search_browser_recovery(
            5,
            config=_audit_config(str(audit_path)),
            output_path=audit_path,
            browser_fetcher=fake_browser,
            fetcher=lambda *_args: "",
        )

        assert recovered["providerCandidates"] == []
        assert recovered["staticCandidates"] == []
        assert recovered["browserRecovery"]["processedCount"] == 2
        assert recovered["browserRecovery"]["activeCandidates"] == 0
        assert recovered["browserRecovery"]["fetchFailures"] == 1
        assert recovered["browserRecovery"]["failureSamples"][0]["stage"] == "browser_fetch"


def test_web_search_browser_recovery_respects_batch_size_and_max_batches() -> None:
    with workspace_tmpdir("web-search-browser-recovery-batch") as root:
        audit_path = root / "web-audit.json"
        config = _audit_config(str(audit_path))
        config["webSearch"]["browserRecoveryBatchSize"] = 1
        config["webSearch"]["browserRecoveryMaxBatchesPerRun"] = 1
        write_web_search_browser_recovery_artifact(
            audit_path,
            schema_version=web_candidates.WEB_SEARCH_AUDIT_SCHEMA_VERSION,
            browser_candidates=[
                {"name": "One", "studio": "One", "url": "https://one.example/jobs"},
                {"name": "Two", "studio": "Two", "url": "https://two.example/jobs"},
            ],
        )
        calls: list[str] = []

        def fake_browser(url: str, _timeout_s: int) -> tuple[str, str]:
            calls.append(url)
            return "<html>No jobs</html>", ""

        recovered = web_candidates.run_web_search_browser_recovery(
            5,
            config=config,
            output_path=audit_path,
            browser_fetcher=fake_browser,
            fetcher=lambda *_args: "",
        )

        assert calls == ["https://one.example/jobs"]
        assert recovered["browserRecovery"]["processedCount"] == 1


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
