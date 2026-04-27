from __future__ import annotations

from src.source_discovery.directory_page_recovery import (
    DirectoryRecoveryRequest,
    browser_recovery_candidate,
    build_recovery_fetch_job,
    dedupe_recovery_fetch_jobs,
    fetch_recovery_jobs,
    looks_like_js_shell,
    plan_recovery_urls,
    recovery_cache_result,
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
