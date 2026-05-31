from __future__ import annotations

import pytest

import src.source_discovery.web_search_candidates as web_candidates
from src.url_hosts import url_host_matches_domain

from ._helpers import workspace_tmpdir


def _audit_config(audit_path: str) -> dict[str, object]:
    return {
        "webSearch": {
            "activeAuditPath": audit_path,
            "activeAuditTtlMinutes": 60,
        }
    }


def _seed() -> list[dict[str, object]]:
    return [
        {
            "studio": "Seed Studio",
            "careersUrl": "https://seed.example/careers",
            "nlPriority": True,
        }
    ]


@pytest.mark.parametrize(
    (
        "scenario",
        "expected_summary",
        "expected_recovery_methods",
        "expected_recovery_reasons",
        "expected_recovery_source_urls",
        "expected_provider_methods",
        "expected_web_failure_stage",
    ),
    [
        pytest.param(
            "fetch-failures",
            {
                "seedFailures": 1,
                "webPageFetchFailures": 1,
                "webFailures": 1,
                "browserRecoveryFetchFailureCandidates": 2,
                "browserRecoveryCandidates": 2,
            },
            {"seed_careers_page", "web_search"},
            {"browser_recovery_fetch_failed"},
            {"https://seed.example/careers", "https://search.example/careers"},
            None,
            "page_fetch",
            id="recoverable-fetch-failures",
        ),
        pytest.param(
            "js-shells",
            {
                "seedPagesFetched": 1,
                "webPagesFetched": 1,
                "providerCandidates": 0,
                "staticCandidates": 0,
                "browserRecoveryJsShellCandidates": 2,
            },
            {"seed_careers_page", "web_search"},
            {"js_shell"},
            None,
            None,
            None,
            id="js-shell-recovery-candidates",
        ),
        pytest.param(
            "success",
            {
                "seedPagesFetched": 1,
                "webPagesFetched": 1,
                "providerCandidates": 2,
                "staticCandidates": 0,
                "browserRecoveryCandidates": 0,
            },
            None,
            None,
            None,
            {"seed_careers_page", "web_search"},
            None,
            id="successful-provider-extraction",
        ),
    ],
)
def test_web_page_result_seed_and_web_scenarios(
    scenario: str,
    expected_summary: dict[str, int],
    expected_recovery_methods: set[str] | None,
    expected_recovery_reasons: set[str] | None,
    expected_recovery_source_urls: set[str] | None,
    expected_provider_methods: set[str] | None,
    expected_web_failure_stage: str | None,
) -> None:
    with workspace_tmpdir(f"web-page-result-{scenario}") as root:
        audit_path = root / "web-audit.json"
        shell = '<html><div id="root"></div><script src="/app.js"></script></html>'

        def fetcher(url: str, _timeout_s: int) -> str:
            if scenario == "fetch-failures":
                if url == "https://seed.example/careers":
                    raise RuntimeError("timeout seed page")
                if url_host_matches_domain(url, "duckduckgo.com"):
                    return '<a href="https://search.example/careers">Careers</a>'
                if url == "https://search.example/careers":
                    raise RuntimeError("429 web page")
                raise RuntimeError(f"unexpected URL: {url}")
            if scenario == "js-shells":
                if url == "https://seed.example/careers":
                    return shell
                if url_host_matches_domain(url, "duckduckgo.com"):
                    return '<a href="https://search.example/careers">Careers</a>'
                if url == "https://search.example/careers":
                    return shell
                raise RuntimeError(f"unexpected URL: {url}")
            if url == "https://seed.example/careers":
                return '<a href="https://boards.greenhouse.io/seedstudio/jobs/1">Role</a>'
            if url_host_matches_domain(url, "duckduckgo.com"):
                return '<a href="https://search.example/careers">Careers</a>'
            if url == "https://search.example/careers":
                return '<a href="https://boards.greenhouse.io/searchstudio/jobs/1">Role</a>'
            raise RuntimeError(f"unexpected URL: {url}")

        artifact, _cache_hit = web_candidates.run_web_search_directory_audit(
            5,
            studio_seeds=_seed(),
            include_seed_careers=True,
            include_web_search=True,
            config=_audit_config(str(audit_path)),
            fetcher=fetcher,
            max_queries=1,
        )

        for key, value in expected_summary.items():
            assert artifact["summary"][key] == value
        if expected_web_failure_stage is not None:
            assert (
                artifact["summary"]["webFailureSamples"][0]["stage"] == expected_web_failure_stage
            )
        if expected_recovery_methods is not None:
            assert {row["discoveryMethod"] for row in artifact["browserRecoveryCandidates"]} == (
                expected_recovery_methods
            )
        if expected_recovery_reasons is not None:
            assert {row["reasonDetail"] for row in artifact["browserRecoveryCandidates"]} == (
                expected_recovery_reasons
            )
        if expected_recovery_source_urls is not None:
            assert {
                row["sourceDirectoryEntryUrl"] for row in artifact["browserRecoveryCandidates"]
            } == expected_recovery_source_urls
        if expected_provider_methods is not None:
            assert {row["discoveryMethod"] for row in artifact["providerCandidates"]} == (
                expected_provider_methods
            )
