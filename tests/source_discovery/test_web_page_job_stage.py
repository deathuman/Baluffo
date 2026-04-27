from __future__ import annotations

import src.source_discovery.web_search_candidates as web_candidates


def _fetch_from(payloads: dict[str, str]):
    def fake_fetch(url: str, _timeout_s: int) -> str:
        if url not in payloads:
            raise RuntimeError(f"unexpected URL: {url}")
        return payloads[url]

    return fake_fetch


def test_web_page_job_stage_records_fetch_failure_browser_candidate_and_sample() -> None:
    failure_samples: list[dict[str, object]] = []

    row = web_candidates._run_web_page_job_stage(
        5,
        page_jobs=[
            web_candidates._page_job(
                url="https://blocked.example/jobs",
                studio="Blocked Studio",
                nl_priority=True,
                adapter="web_search",
            )
        ],
        discovery_method="web_search",
        fetcher=lambda *_args: (_ for _ in ()).throw(RuntimeError("timeout")),
        page_fetch_progress_label="Web search page fetch",
        recovery_progress_label="Web search page recovery",
        recovery_timing_key="webRecoveryFetchMs",
        failure_samples=failure_samples,
    )

    assert row["summary"]["pageFetchJobs"] == 1
    assert row["summary"]["pagesFetched"] == 0
    assert row["summary"]["pageFetchFailures"] == 1
    assert row["summary"]["browserRecoveryFetchFailureCandidates"] == 1
    assert row["browserRecoveryCandidates"][0]["reasonDetail"] == "browser_recovery_fetch_failed"
    assert row["failures"][0]["stage"] == "page_fetch"
    assert failure_samples == [
        {"stage": "page_fetch", "name": "https://blocked.example/jobs", "error": "timeout"}
    ]


def test_web_page_job_stage_records_js_shell_browser_candidate_without_recovery() -> None:
    row = web_candidates._run_web_page_job_stage(
        5,
        page_jobs=[
            web_candidates._page_job(
                url="https://shell.example/careers",
                studio="Shell Studio",
                nl_priority=False,
                adapter="seed_careers_page",
            )
        ],
        discovery_method="seed_careers_page",
        fetcher=_fetch_from(
            {
                "https://shell.example/careers": (
                    '<html><div id="root"></div><script src="/app.js"></script></html>'
                )
            }
        ),
        page_fetch_progress_label="Seed careers page fetch",
        recovery_progress_label="Seed careers page recovery",
        recovery_timing_key="seedRecoveryFetchMs",
        enable_recovery=True,
    )

    assert row["summary"]["pagesFetched"] == 1
    assert row["summary"]["browserRecoveryJsShellCandidates"] == 1
    assert row["summary"].get("recoveryFetchAttempts") is None
    assert row["providerCandidates"] == []
    assert row["staticCandidates"] == []


def test_web_page_job_stage_runs_http_recovery_and_remaps_timing() -> None:
    row = web_candidates._run_web_page_job_stage(
        5,
        page_jobs=[
            web_candidates._page_job(
                url="https://recover.example/",
                studio="Recover Studio",
                nl_priority=False,
                adapter="seed_careers_page",
            )
        ],
        discovery_method="seed_careers_page",
        fetcher=_fetch_from(
            {
                "https://recover.example/": "<html><body>Recover Studio</body></html>",
                "https://recover.example/careers": '<a href="/jobs/engineer">Engineer</a>',
                "https://recover.example/jobs": "<html><body>No roles</body></html>",
            }
        ),
        page_fetch_progress_label="Seed careers page fetch",
        recovery_progress_label="Seed careers page recovery",
        recovery_timing_key="seedRecoveryFetchMs",
        enable_recovery=True,
        recovery_url_limit=2,
    )

    assert row["summary"]["pagesFetched"] == 1
    assert row["summary"]["recoveryFetchAttempts"] == 2
    assert row["summary"]["recoveredStaticCandidates"] == 1
    assert row["staticCandidates"][0]["listing_url"] == "https://recover.example/careers"
    assert "seedRecoveryFetchMs" in row["batchTiming"]
    assert row["completedUrlIdentities"] == ["https://recover.example/"]
