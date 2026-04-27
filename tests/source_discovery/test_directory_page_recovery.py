from __future__ import annotations

from src.source_discovery.directory_page_recovery import (
    DirectoryRecoveryRequest,
    DirectoryRecoveryResult,
    apply_recovery_fetch_results,
    apply_recovery_to_scan_result,
    browser_recovery_candidate,
    build_recovery_fetch_job,
    dedupe_recovery_fetch_jobs,
    default_recovery_summary,
    fetch_recovery_jobs,
    looks_like_js_shell,
    plan_recovery_fetch_job_waves,
    plan_recovery_urls,
    recovery_cache_result,
    resolve_recovery_url_limit,
    run_directory_page_recovery,
)
from src.source_discovery.provider_inference_filters import split_bad_provider_inferences


def _request(
    key: str = "https://studio.example.com/",
    *,
    html: str = "<html><body>No openings here</body></html>",
) -> DirectoryRecoveryRequest:
    return DirectoryRecoveryRequest(
        key=key,
        adapter="gameprog",
        discovery_method="gameprog",
        name="Studio",
        studio="Studio",
        page_url=key,
        html=html,
        payload={"studio": "Studio"},
    )


def test_directory_recovery_plans_bounded_deduped_same_site_urls() -> None:
    request = _request(
        html="""
            <a href="/jobs">Jobs</a>
            <script>{"url":"https://studio.example.com/careers#team"}</script>
        """,
    )

    urls = plan_recovery_urls(request, paths=("/careers", "/jobs"), limit=2)

    assert urls == ["https://studio.example.com/jobs", "https://studio.example.com/careers"]


def test_directory_recovery_skips_common_paths_for_profile_hosts() -> None:
    request = _request("https://linktr.ee/studio")

    assert plan_recovery_urls(request, paths=("/careers", "/jobs"), limit=4) == []


def test_directory_recovery_fans_out_shared_fetched_url_to_multiple_rows() -> None:
    requests = [
        _request("https://studio.example.com/"),
        _request("https://studio.example.com/"),
    ]
    calls: list[str] = []

    def fake_fetch(url: str, _timeout: int) -> str:
        calls.append(url)
        if url.endswith("/careers"):
            return '<a href="/jobs/engineer">Engineer</a><a href="/jobs/designer">Designer</a>'
        return "<html></html>"

    def analyze(result, request):
        if str(result.get("url") or "").endswith("/careers"):
            return [], [
                {
                    "name": request.name,
                    "studio": request.studio,
                    "adapter": "static",
                    "listing_url": str(result.get("url")),
                    "discoveryMethod": request.discovery_method,
                }
            ]
        return [], []

    output = run_directory_page_recovery(
        5,
        requests,
        fetcher=fake_fetch,
        total_concurrency=2,
        per_host_concurrency=1,
        analyze_result=analyze,
        progress_label="Test",
    )

    assert calls.count("https://studio.example.com/careers") == 1
    assert len(output.static_candidates) == 2
    assert output.recovered_keys == {"https://studio.example.com/"}
    assert output.summary["recoveryFetchAttempts"] == 2


def test_directory_recovery_marks_js_shell_browser_candidate() -> None:
    request = _request(html='<div id="root"></div><script src="/app.js"></script>')

    assert looks_like_js_shell(request.html) is True
    row = browser_recovery_candidate(request, reason_detail="js_shell")

    assert row == {
        "adapter": "gameprog",
        "discoveryMethod": "gameprog",
        "name": "Studio",
        "studio": "Studio",
        "url": request.page_url,
        "sourceDirectoryEntryUrl": request.page_url,
        "reason": "no_careers_evidence",
        "reasonDetail": "js_shell",
    }
    assert row["reasonDetail"] == "js_shell"
    assert row["url"] == request.page_url


def test_recovery_fetch_job_builder_preserves_payload_and_failure_stage() -> None:
    payload = {"row": {"studio": "Studio"}, "homepageUrl": "https://studio.example"}

    job = build_recovery_fetch_job(
        recovery_url="https://studio.example/careers",
        payload=payload,
        name="Studio recovery",
        adapter="gamedevmap",
        failure_stage="gamedevmap_recovery_fetch",
    )

    assert job == {
        "url": "https://studio.example/careers",
        "payload": payload,
        "name": "Studio recovery",
        "adapter": "gamedevmap",
        "failureStage": "gamedevmap_recovery_fetch",
    }


def test_plan_recovery_fetch_job_waves_preserves_payloads_and_custom_url_extraction() -> None:
    primary_jobs, secondary_jobs = plan_recovery_fetch_job_waves(
        page_url="https://studio.example/",
        html='<a href="/jobs">Jobs</a><script>window.url="/hidden-careers"</script>',
        primary_paths=("/careers",),
        secondary_paths=("/join-us",),
        payload_factory=lambda recovery_url, wave: {
            "homepageUrl": "https://studio.example/",
            "recoveryUrl": recovery_url,
            "reasonDetail": "no_jobish_links",
            "recoverySource": "same_party_recovery_url",
            "recoveryWave": wave,
        },
        name_factory=lambda recovery_url, wave: f"Studio wave {wave} {recovery_url}",
        adapter="gamedevmap",
        failure_stage="gamedevmap_recovery_fetch",
        blocked_hosts=set(),
        html_url_candidate_fn=lambda _html: ["https://studio.example/hidden-careers"],
    )

    assert [job["url"] for job in primary_jobs] == [
        "https://studio.example/jobs",
        "https://studio.example/hidden-careers",
        "https://studio.example/careers",
    ]
    assert [job["url"] for job in secondary_jobs] == ["https://studio.example/join-us"]
    assert primary_jobs[0]["payload"] == {
        "homepageUrl": "https://studio.example/",
        "recoveryUrl": "https://studio.example/jobs",
        "reasonDetail": "no_jobish_links",
        "recoverySource": "same_party_recovery_url",
        "recoveryWave": 1,
    }
    assert primary_jobs[0]["name"] == "Studio wave 1 https://studio.example/jobs"
    assert primary_jobs[0]["adapter"] == "gamedevmap"
    assert primary_jobs[0]["failureStage"] == "gamedevmap_recovery_fetch"
    assert secondary_jobs[0]["payload"]["recoveryWave"] == 2


def test_dedupe_recovery_fetch_jobs_fans_out_payloads() -> None:
    jobs = [
        build_recovery_fetch_job(
            recovery_url="https://shared.example/jobs",
            payload={"row": {"studio": "One"}},
            name="One recovery",
            adapter="gamedevmap",
            failure_stage="gamedevmap_recovery_fetch",
        ),
        build_recovery_fetch_job(
            recovery_url="https://shared.example/jobs",
            payload={"row": {"studio": "Two"}},
            name="Two recovery",
            adapter="gamedevmap",
            failure_stage="gamedevmap_recovery_fetch",
        ),
    ]

    deduped = dedupe_recovery_fetch_jobs(jobs)

    assert len(deduped) == 1
    assert deduped[0]["url"] == "https://shared.example/jobs"
    assert deduped[0]["payload"] == {
        "requests": [{"row": {"studio": "One"}}, {"row": {"studio": "Two"}}],
        "dedupeCount": 2,
    }


def test_recovery_cache_result_reconstructs_success_and_failure_shape() -> None:
    job = build_recovery_fetch_job(
        recovery_url="https://studio.example/jobs",
        payload={"row": {"studio": "Studio"}},
        name="Studio recovery",
        adapter="gamedevmap",
        failure_stage="gamedevmap_recovery_fetch",
    )

    assert recovery_cache_result({"ok": True, "text": "<html></html>"}, job) == {
        "job": job,
        "payload": job["payload"],
        "url": "https://studio.example/jobs",
        "ok": True,
        "text": "<html></html>",
        "error": "",
        "failure": None,
    }
    assert recovery_cache_result({"ok": False, "error": "timeout"}, job) == {
        "job": job,
        "payload": job["payload"],
        "url": "https://studio.example/jobs",
        "ok": False,
        "text": "",
        "error": "timeout",
        "failure": {
            "name": "Studio recovery",
            "adapter": "gamedevmap",
            "error": "timeout",
            "stage": "gamedevmap_recovery_fetch",
        },
    }


def test_fetch_recovery_jobs_uses_cache_and_updates_uncached_results() -> None:
    cached_job = build_recovery_fetch_job(
        recovery_url="https://cached.example/jobs",
        payload={"row": {"studio": "Cached"}},
        name="Cached recovery",
        adapter="gamedevmap",
        failure_stage="gamedevmap_recovery_fetch",
    )
    uncached_job = build_recovery_fetch_job(
        recovery_url="https://fresh.example/jobs",
        payload={"row": {"studio": "Fresh"}},
        name="Fresh recovery",
        adapter="gamedevmap",
        failure_stage="gamedevmap_recovery_fetch",
    )
    cache = {
        "https://cached.example/jobs": {
            "url": "https://cached.example/jobs",
            "ok": True,
            "text": "cached text",
            "error": "",
        }
    }
    fetched_jobs: list[dict[str, object]] = []

    def fake_fetch_pages(_timeout_s, jobs, **_kwargs):
        fetched_jobs.extend(jobs)
        return [
            {
                "job": jobs[0],
                "payload": jobs[0]["payload"],
                "url": jobs[0]["url"],
                "ok": False,
                "text": "",
                "error": "down",
                "failure": {
                    "name": jobs[0]["name"],
                    "adapter": jobs[0]["adapter"],
                    "stage": jobs[0]["failureStage"],
                    "error": "down",
                },
            }
        ]

    results, unique_count, network_count = fetch_recovery_jobs(
        5,
        [cached_job, uncached_job],
        fetcher=lambda *_args: "",
        total_concurrency=2,
        per_host_concurrency=1,
        progress_label="Test recovery",
        recovery_cache=cache,
        fetch_pages=fake_fetch_pages,
    )

    assert unique_count == 2
    assert network_count == 1
    assert [job["url"] for job in fetched_jobs] == ["https://fresh.example/jobs"]
    assert [row["url"] for row in results] == [
        "https://cached.example/jobs",
        "https://fresh.example/jobs",
    ]
    assert cache["https://fresh.example/jobs"] == {
        "url": "https://fresh.example/jobs",
        "ok": False,
        "text": "",
        "error": "down",
    }


def test_apply_recovery_fetch_results_processes_fanout_failures_and_finalizers() -> None:
    def apply_payload(payload, result, grouped, provider_candidates, _static_candidates):
        key = str(payload["key"])
        group = grouped.setdefault(key, {"attempts": 0, "candidates": 0, "row": payload})
        group["attempts"] += 1
        if not bool(result.get("ok")):
            group["failures"] = int(group.get("failures") or 0) + 1
            return ""
        group["fetched"] = int(group.get("fetched") or 0) + 1
        if not bool(payload.get("found")):
            return ""
        provider_candidates.append({"adapter": "lever", "source": key})
        group["candidates"] += 1
        return key

    def finalize_group(group):
        if int(group.get("candidates") or 0) > 0:
            return []
        return [{"reason": "no_careers_evidence", "row": group["row"]}]

    output = apply_recovery_fetch_results(
        [
            {
                "ok": True,
                "payload": {
                    "requests": [
                        {"key": "one", "found": True},
                        {"key": "two", "found": False},
                    ]
                },
            },
            {
                "ok": False,
                "payload": {"requests": [{"key": "two"}]},
                "failure": {"stage": "recovery_fetch", "error": "timeout"},
            },
        ],
        apply_payload=apply_payload,
        finalize_group=finalize_group,
    )

    assert output.pages_fetched == 1
    assert output.provider_candidates == [{"adapter": "lever", "source": "one"}]
    assert output.failures == [{"stage": "recovery_fetch", "error": "timeout"}]
    assert output.recovered_homepages == {"one"}
    assert output.rejected_rows == [
        {"reason": "no_careers_evidence", "row": {"key": "two", "found": False}}
    ]
    assert output.grouped["two"]["attempts"] == 2


def test_default_recovery_summary_preserves_report_keys() -> None:
    assert default_recovery_summary() == {
        "recoveryFetchAttempts": 0,
        "recoveryPagesFetched": 0,
        "recoveredProviderCandidates": 0,
        "recoveredStaticCandidates": 0,
        "recoveryFailures": 0,
        "browserRecoveryCandidates": 0,
    }


def test_resolve_recovery_url_limit_defaults_and_rejects_invalid_values() -> None:
    assert resolve_recovery_url_limit({}) == 6
    assert resolve_recovery_url_limit({"activeAuditRecoveryUrlLimit": 4}) == 4
    assert resolve_recovery_url_limit({"activeAuditRecoveryUrlLimit": 0}) == 6
    assert resolve_recovery_url_limit({"activeAuditRecoveryUrlLimit": "bad"}) == 6


def test_apply_recovery_to_scan_result_suppresses_recovered_fallbacks() -> None:
    recovery = DirectoryRecoveryResult(
        provider_candidates=[{"adapter": "greenhouse", "studio": "Recovered"}],
        static_candidates=[{"adapter": "static", "listing_url": "https://recovered.example/jobs"}],
        browser_recovery_candidates=[{"url": "https://js.example", "reasonDetail": "js_shell"}],
        recovered_keys={"recovered"},
        summary={**default_recovery_summary(), "recoveredStaticCandidates": 1},
        batch_timing={"recoveryFetchMs": 12},
    )

    row = apply_recovery_to_scan_result(
        {
            "providerCandidates": [{"adapter": "lever", "studio": "Existing"}],
            "staticCandidates": [
                {"adapter": "static", "listing_url": "https://direct.example/jobs"}
            ],
            "browserRecoveryCandidates": [],
            "summary": {"existing": 1},
            "batchTiming": {"candidateAnalysisMs": 3},
        },
        recovery,
        provider_dedupe=lambda rows: rows,
        static_dedupe=lambda rows: rows,
        fallback_static_candidates=[
            {"key": "recovered", "candidate": {"listing_url": "skip"}},
            {"key": "miss", "candidate": {"listing_url": "keep"}},
        ],
        fallback_key=lambda entry: entry["key"],
        fallback_candidate=lambda entry: entry["candidate"],
    )

    assert row["providerCandidates"] == [
        {"adapter": "lever", "studio": "Existing"},
        {"adapter": "greenhouse", "studio": "Recovered"},
    ]
    assert row["staticCandidates"] == [
        {"adapter": "static", "listing_url": "https://direct.example/jobs"},
        {"adapter": "static", "listing_url": "https://recovered.example/jobs"},
        {"listing_url": "keep"},
    ]
    assert row["browserRecoveryCandidates"] == [
        {"url": "https://js.example", "reasonDetail": "js_shell"}
    ]
    assert row["summary"]["existing"] == 1
    assert row["summary"]["recoveredStaticCandidates"] == 1
    assert row["batchTiming"] == {"candidateAnalysisMs": 3, "recoveryFetchMs": 12}


def test_apply_recovery_to_scan_result_can_remap_timing_key() -> None:
    recovery = DirectoryRecoveryResult(batch_timing={"recoveryFetchMs": 7})

    row = apply_recovery_to_scan_result(
        {
            "providerCandidates": [],
            "staticCandidates": [],
            "summary": {},
            "batchTiming": {},
        },
        recovery,
        timing_key="webRecoveryFetchMs",
    )

    assert row["batchTiming"] == {"webRecoveryFetchMs": 7}


def test_bad_provider_inference_filter_rejects_generic_hosts_and_slugs() -> None:
    good, bad = split_bad_provider_inferences(
        [
            {"adapter": "greenhouse", "slug": "embed", "url": "https://boards.greenhouse.io/embed"},
            {"adapter": "teamtailor", "url": "https://www.teamtailor.com/"},
            {"adapter": "greenhouse", "slug": "realstudio"},
        ]
    )

    assert [row["reasonDetail"] for row in bad] == [
        "bad_greenhouse_slug",
        "bad_teamtailor_host",
    ]
    assert good == [{"adapter": "greenhouse", "slug": "realstudio"}]
