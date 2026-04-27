from __future__ import annotations

import src.source_discovery.web_search_candidates as web_candidates

from ._helpers import workspace_tmpdir


def _audit_config(audit_path: str) -> dict[str, object]:
    return {
        "webSearch": {
            "activeAuditEnabled": True,
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


def test_web_page_result_records_seed_and_web_recoverable_fetch_failures() -> None:
    with workspace_tmpdir("web-page-result-fetch-failures") as root:
        audit_path = root / "web-audit.json"

        def fetcher(url: str, _timeout_s: int) -> str:
            if url == "https://seed.example/careers":
                raise RuntimeError("timeout seed page")
            if "duckduckgo.com" in url:
                return '<a href="https://search.example/careers">Careers</a>'
            if url == "https://search.example/careers":
                raise RuntimeError("429 web page")
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

        assert artifact["summary"]["seedFailures"] == 1
        assert artifact["summary"]["webPageFetchFailures"] == 1
        assert artifact["summary"]["webFailures"] == 1
        assert artifact["summary"]["browserRecoveryFetchFailureCandidates"] == 2
        assert artifact["summary"]["browserRecoveryCandidates"] == 2
        assert artifact["summary"]["webFailureSamples"][0]["stage"] == "page_fetch"
        assert {row["discoveryMethod"] for row in artifact["browserRecoveryCandidates"]} == {
            "seed_careers_page",
            "web_search",
        }
        assert {row["reasonDetail"] for row in artifact["browserRecoveryCandidates"]} == {
            "browser_recovery_fetch_failed"
        }


def test_web_page_result_records_seed_and_web_js_shell_candidates() -> None:
    with workspace_tmpdir("web-page-result-js-shells") as root:
        audit_path = root / "web-audit.json"
        shell = '<html><div id="root"></div><script src="/app.js"></script></html>'

        def fetcher(url: str, _timeout_s: int) -> str:
            if url == "https://seed.example/careers":
                return shell
            if "duckduckgo.com" in url:
                return '<a href="https://search.example/careers">Careers</a>'
            if url == "https://search.example/careers":
                return shell
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

        assert artifact["summary"]["seedPagesFetched"] == 1
        assert artifact["summary"]["webPagesFetched"] == 1
        assert artifact["summary"]["providerCandidates"] == 0
        assert artifact["summary"]["staticCandidates"] == 0
        assert artifact["summary"]["browserRecoveryJsShellCandidates"] == 2
        assert {row["discoveryMethod"] for row in artifact["browserRecoveryCandidates"]} == {
            "seed_careers_page",
            "web_search",
        }
        assert {row["reasonDetail"] for row in artifact["browserRecoveryCandidates"]} == {
            "js_shell"
        }


def test_web_page_result_preserves_seed_and_web_successful_page_analysis() -> None:
    with workspace_tmpdir("web-page-result-success") as root:
        audit_path = root / "web-audit.json"

        def fetcher(url: str, _timeout_s: int) -> str:
            if url == "https://seed.example/careers":
                return '<a href="https://boards.greenhouse.io/seedstudio/jobs/1">Role</a>'
            if "duckduckgo.com" in url:
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

        assert artifact["summary"]["seedPagesFetched"] == 1
        assert artifact["summary"]["webPagesFetched"] == 1
        assert artifact["summary"]["providerCandidates"] == 2
        assert artifact["summary"]["staticCandidates"] == 0
        assert artifact["summary"]["browserRecoveryCandidates"] == 0
        assert {row["discoveryMethod"] for row in artifact["providerCandidates"]} == {
            "seed_careers_page",
            "web_search",
        }
