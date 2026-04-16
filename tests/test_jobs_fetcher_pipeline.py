import json
import threading
import time
from pathlib import Path
from unittest import mock

import pytest

from src.jobs.adapters import static as static_adapter
from src.jobs.pipeline_runtime import PipelineTaskRuntime, make_task_state_writer
from tests.jobs_fetcher_helpers import (
    _fixture,
    _fixture_json,
    jf,
    patch_jobs_fetcher_aliases,
    workspace_tmpdir,
)

patch_jobs_fetcher_aliases()


def test_run_pipeline_social_sources_report_and_output() -> None:
    social_cfg = {
        "enabled": True,
        "minConfidence": 20,
        "rejectForHirePosts": True,
        "reddit": {
            "enabled": True,
            "subreddits": [
                "gamedev",
                "gameDevClassifieds",
                "gamedevjobs",
                "INAT",
                "gamejobs",
                "indiegaming",
            ],
            "maxPostsPerSubreddit": 5,
            "rssFallback": True,
            "htmlFallback": False,
        },
        "x": {
            "enabled": False,
            "queries": ["#gamedevjobs"],
            "maxPostsPerQuery": 5,
            "api": {"enabled": False, "endpoint": "", "bearerTokenEnv": "BALUFFO_X_BEARER_TOKEN"},
            "scraperFallback": {"enabled": True, "endpoint": "https://example.local/x-search"},
        },
        "mastodon": {
            "enabled": True,
            "instances": ["https://mastodon.gamedev.place"],
            "hashtags": ["gamedevjobs"],
            "maxPostsPerTag": 5,
        },
    }

    def social_reddit_loader(**kwargs):
        return jf.run_social_reddit_source(**kwargs, social_config=social_cfg)

    def social_mastodon_loader(**kwargs):
        return jf.run_social_mastodon_source(**kwargs, social_config=social_cfg)

    reddit_payload = {
        "data": {
            "children": [
                {
                    "data": {
                        "id": "abc123",
                        "title": "We're hiring a Technical Artist at Nebula Games",
                        "selftext": "Apply https://jobs.nebula.dev/ta",
                        "link_flair_text": "Hiring",
                        "permalink": "/r/gamedev/comments/abc123/test/",
                        "url": "https://www.reddit.com/r/gamedev/comments/abc123/test/",
                        "created_utc": 1700000000,
                        "author": "nebula_hr",
                    }
                }
            ]
        }
    }
    mastodon_payload = [
        {
            "id": "m1",
            "content": "<p>Hiring gameplay programmer at Aurora Games https://careers.aurora.dev/gp</p>",
            "created_at": "2026-03-09T11:05:00Z",
            "url": "https://mastodon.gamedev.place/@aurora/111",
            "account": {"display_name": "Aurora Games"},
        }
    ]

    def fake_fetch(url: str, _: int) -> str:
        if "reddit.com/r/gamedev/new.json" in url:
            return json.dumps(reddit_payload)
        if "mastodon.gamedev.place/api/v1/timelines/tag/gamedevjobs" in url:
            return json.dumps(mastodon_payload)
        raise RuntimeError(f"Unhandled URL in fake fetch: {url}")

    with workspace_tmpdir("jobs-fetcher-social") as tmp:
        report = jf.run_pipeline(
            output_dir=Path(tmp),
            fetch_text=fake_fetch,
            source_loaders=[
                ("social_reddit", social_reddit_loader),
                ("social_mastodon", social_mastodon_loader),
            ],
            timeout_s=5,
            retries=0,
            backoff_s=0,
        )
        sources = {row["name"]: row for row in report["sources"]}
        assert sources["social_reddit"]["status"] == "ok"
        assert sources["social_mastodon"]["status"] == "ok"
        assert sources["social_reddit"]["keptCount"] == 1
        social_summary = report.get("socialSummary") or {}
        expected_kept = sum(
            int(row.get("keptCount") or 0)
            for row in sources.values()
            if str(row.get("name") or "").startswith("social_")
        )
        assert int(social_summary.get("keptCount") or 0) == expected_kept
        assert int(social_summary.get("uniqueKeptCount") or 0) == expected_kept
        assert int(social_summary.get("officialBoardOverlapCount") or 0) == 0
        assert int(social_summary.get("duplicateCount") or 0) == 0
        assert float(social_summary.get("duplicateRate") or 0) == 0.0
        assert int(social_summary.get("sampleSize") or 0) == 0
        assert int(social_summary.get("reviewedCount") or 0) == 0
        assert int(social_summary.get("falsePositiveCount") or 0) == 0
        assert float(social_summary.get("falsePositiveRate") or 0) == 0.0
        channels = social_summary.get("channels") or {}
        assert int((channels.get("reddit") or {}).get("keptCount") or 0) == 1
        assert int((channels.get("mastodon") or {}).get("keptCount") or 0) == 0
        review_path = Path(tmp) / "social-experiment-review.json"
        assert review_path.exists()
        review_payload = json.loads(review_path.read_text(encoding="utf-8"))
        assert int(review_payload.get("candidateCount") or 0) == expected_kept
        assert int(review_payload.get("sampleSize") or 0) == 0
        rows = json.loads((Path(tmp) / "jobs-unified.json").read_text(encoding="utf-8"))
        assert any(str(row.get("source") or "").startswith("social_") for row in rows)


def test_task_state_writer_serializes_concurrent_writes() -> None:
    runtime = PipelineTaskRuntime(
        task_rows={
            "static_source::static:listing_url:https://example.com/careers": {
                "name": "static_source::static:listing_url:https://example.com/careers",
                "status": "running",
                "startedAt": "2026-03-28T21:45:26+00:00",
                "finishedAt": "",
                "durationMs": 0,
                "heartbeatAt": "2026-03-28T21:45:26+00:00",
                "error": "",
            }
        },
        task_lock=threading.Lock(),
        last_task_write_monotonic=0.0,
        last_heartbeat_write={},
        thread_local=threading.local(),
        domain_lock=threading.Lock(),
        domain_gates={},
        show_progress=False,
    )
    write_calls = 0
    active_writes = 0
    max_active_writes = 0
    write_guard = threading.Lock()

    def normalize_task_state_payload(payload, **_kwargs):
        return payload

    def fake_write_text_if_changed(_path, _text):
        nonlocal write_calls, active_writes, max_active_writes
        with write_guard:
            write_calls += 1
            active_writes += 1
            max_active_writes = max(max_active_writes, active_writes)
        time.sleep(0.02)
        with write_guard:
            active_writes -= 1
        return True

    write_task_state = make_task_state_writer(
        runtime=runtime,
        run_id="fetch_test",
        started_at="2026-03-28T21:45:26+00:00",
        report_path="C:/tmp/jobs-fetch-report.json",
        task_state_path="C:/tmp/jobs-fetch-tasks.json",
        normalize_task_state_payload=normalize_task_state_payload,
        write_text_if_changed=fake_write_text_if_changed,
    )

    threads = [threading.Thread(target=write_task_state, kwargs={"force": True}) for _ in range(6)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=2)

    assert all(not thread.is_alive() for thread in threads)
    assert write_calls == 6
    assert max_active_writes == 1


def test_normalize_source_report_row_preserves_static_stage_timings() -> None:
    row = jf.normalize_source_report_row(
        {
            "name": "static_source::test",
            "status": "ok",
            "adapter": "static",
            "stageTimingsMs": {
                "listingFetch": 120,
                "candidateExtraction": 45,
                "detailFetch": 310,
                "canonicalization": 12,
            },
            "details": [
                {
                    "adapter": "static",
                    "studio": "Test Studio",
                    "name": "Test Studio",
                    "status": "ok",
                    "stats": {
                        "candidate_links_found": 8,
                        "detail_pages_visited": 4,
                        "jobs_emitted": 3,
                        "fetch_cache_hits": 2,
                        "detail_yield_percent": 75,
                        "listing_fetch_ms": 120,
                        "candidate_extraction_ms": 45,
                        "detail_fetch_ms": 310,
                    },
                }
            ],
        }
    )
    assert (row.get("stageTimingsMs") or {}).get("detailFetch") == 310
    detail_stats = (row.get("details") or [{}])[0].get("stats") or {}
    assert int(detail_stats.get("fetch_cache_hits") or 0) == 2
    assert int(detail_stats.get("detail_yield_percent") or 0) == 75


def test_pipeline_partial_success_when_one_source_fails() -> None:
    def failing_loader(**_: object):
        raise RuntimeError("timeout")

    def ok_loader(**_: object):
        return [
            {
                "title": "Gameplay Programmer",
                "company": "Nebula Games",
                "city": "Remote",
                "country": "Remote",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": "https://example.com/jobs/1",
                "sector": "Game",
                "sourceJobId": "ok-1",
                "postedAt": "2026-02-10",
            }
        ]

    with workspace_tmpdir("jobs-fetcher") as tmp:
        report = jf.run_pipeline(
            output_dir=Path(tmp),
            source_loaders=[("failing", failing_loader), ("ok", ok_loader)],
        )

        assert report["summary"]["failedSources"] == 1
        assert report["summary"]["outputCount"] == 1

        output = json.loads((Path(tmp) / "jobs-unified.json").read_text(encoding="utf-8"))
        assert len(output) == 1
        assert output[0]["source"] == "ok"


def test_pipeline_preserves_previous_output_when_current_is_empty() -> None:
    existing = [
        {
            "id": 1,
            "title": "Engine Programmer",
            "company": "Archive Studio",
            "city": "Remote",
            "country": "Remote",
            "workType": "Remote",
            "contractType": "Full-time",
            "jobLink": "https://archive.example/jobs/1",
            "sector": "Game",
            "profession": "engine",
            "companyType": "Game",
            "description": "Engine Programmer at Archive Studio",
            "source": "archive",
            "sourceJobId": "archive-1",
            "fetchedAt": "2026-02-01T00:00:00+00:00",
            "postedAt": "2026-01-30T00:00:00+00:00",
            "dedupKey": "url:archive",
            "qualityScore": 100,
        }
    ]

    def empty_loader(**_: object):
        return []

    with workspace_tmpdir("jobs-fetcher") as tmp:
        out = Path(tmp)
        (out / "jobs-unified.json").write_text(json.dumps(existing), encoding="utf-8")
        report = jf.run_pipeline(output_dir=out, source_loaders=[("empty", empty_loader)])

        output = json.loads((out / "jobs-unified.json").read_text(encoding="utf-8"))
        assert len(output) == 1
        assert int(report["summary"].get("outputCount") or 0) == 1


def test_pipeline_tracks_likely_removed_jobs_in_lifecycle_state() -> None:
    def one_job_loader(**_: object):
        return [
            {
                "title": "Engine Programmer",
                "company": "Lifecycle Studio",
                "city": "Remote",
                "country": "Remote",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": "https://example.com/lifecycle/engine-programmer",
                "sector": "Game",
                "sourceJobId": "life-1",
                "postedAt": "2026-03-01",
            }
        ]

    def empty_loader(**_: object):
        return []

    previous_default_loaders = jf.default_source_loaders
    try:
        with workspace_tmpdir("jobs-fetcher") as tmp:
            out = Path(tmp)
            jf.default_source_loaders = lambda: [("only_source", one_job_loader)]
            first = jf.run_pipeline(
                output_dir=out, preserve_previous_on_empty=False, force_refresh_all=True
            )
            assert int(first["summary"].get("outputCount") or 0) == 1
            assert int(first["summary"].get("lifecycleActiveCount") or 0) == 1

            jf.default_source_loaders = lambda: [("only_source", empty_loader)]
            second = jf.run_pipeline(
                output_dir=out, preserve_previous_on_empty=False, force_refresh_all=True
            )
            assert int(second["summary"].get("outputCount") or 0) == 0
            assert int(second["summary"].get("lifecycleLikelyRemovedCount") or 0) == 1

            lifecycle_payload = json.loads(
                (out / "jobs-lifecycle-state.json").read_text(encoding="utf-8")
            )
            jobs_map = lifecycle_payload.get("jobs") or {}
            assert len(jobs_map) == 1
            entry = list(jobs_map.values())[0]
            assert str(entry.get("status") or "") == "likely_removed"
            assert str(entry.get("removedAt") or "")
    finally:
        jf.default_source_loaders = previous_default_loaders


def test_pipeline_marks_missing_for_successful_sources_even_when_other_sources_fail() -> None:
    def one_job_loader(**_: object):
        return [
            {
                "title": "Engine Programmer",
                "company": "Lifecycle Studio",
                "city": "Remote",
                "country": "Remote",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": "https://example.com/lifecycle/engine-programmer",
                "sector": "Game",
                "sourceJobId": "life-1",
                "postedAt": "2026-03-01",
            }
        ]

    def empty_loader(**_: object):
        return []

    def failing_loader(**_: object):
        raise RuntimeError("timeout")

    previous_default_loaders = jf.default_source_loaders
    try:
        with workspace_tmpdir("jobs-fetcher") as tmp:
            out = Path(tmp)
            jf.default_source_loaders = lambda: [
                ("ok_source", one_job_loader),
                ("failing_source", failing_loader),
            ]
            first = jf.run_pipeline(
                output_dir=out, preserve_previous_on_empty=False, force_refresh_all=True
            )
            assert int(first["summary"].get("outputCount") or 0) == 1
            assert int(first["summary"].get("failedSources") or 0) == 1

            jf.default_source_loaders = lambda: [
                ("ok_source", empty_loader),
                ("failing_source", failing_loader),
            ]
            second = jf.run_pipeline(
                output_dir=out, preserve_previous_on_empty=False, force_refresh_all=True
            )
            assert int(second["summary"].get("failedSources") or 0) == 1
            assert int(second["summary"].get("lifecycleLikelyRemovedCount") or 0) == 1

            lifecycle_payload = json.loads(
                (out / "jobs-lifecycle-state.json").read_text(encoding="utf-8")
            )
            jobs_map = lifecycle_payload.get("jobs") or {}
            assert len(jobs_map) == 1
            entry = list(jobs_map.values())[0]
            assert str(entry.get("status") or "") == "likely_removed"
            assert str(entry.get("removedAt") or "")
    finally:
        jf.default_source_loaders = previous_default_loaders


def test_pipeline_output_contract_matches_frontend() -> None:
    def ok_loader(**_: object):
        return [
            {
                "title": "Technical Artist",
                "company": "Orion Labs",
                "city": "Remote",
                "country": "Remote",
                "workType": "remote",
                "contractType": "contract",
                "jobLink": "https://example.com/jobs/ta",
                "sector": "gaming",
                "sourceJobId": "ta-1",
                "postedAt": "2026-02-01",
            }
        ]

    with workspace_tmpdir("jobs-fetcher") as tmp:
        jf.run_pipeline(output_dir=Path(tmp), source_loaders=[("ok", ok_loader)])
        rows = json.loads((Path(tmp) / "jobs-unified.json").read_text(encoding="utf-8"))
        assert len(rows) == 1
        row = rows[0]
        for field in jf.REQUIRED_FIELDS:
            assert field in row
        for field in jf.OPTIONAL_FIELDS:
            assert field in row
        assert row["workType"] == "Remote"
        assert isinstance(row["focusScore"], int)


def test_pipeline_default_source_loader_contract_excludes_wellfound_and_keeps_core_families() -> (
    None
):
    loader_names = [name for name, _ in jf.default_source_loaders()]

    assert "wellfound" not in loader_names
    assert "google_sheets" in loader_names
    assert "google_sheets_1er2oaxo" in loader_names
    assert "google_sheets_1mvqhxat" in loader_names
    assert "remote_ok" in loader_names
    assert "gamesindustry" in loader_names
    assert "gamejobs" in loader_names
    assert "workwithindies" in loader_names
    assert "greenhouse_boards" in loader_names
    assert "teamtailor_sources" in loader_names
    assert "lever_sources" in loader_names
    assert "smartrecruiters_sources" in loader_names
    assert "workable_sources" in loader_names
    assert "recruitee_sources" in loader_names
    assert "pinpoint_sources" in loader_names
    assert "ashby_sources" in loader_names
    assert "breezy_sources" in loader_names
    assert "jazzhr_sources" in loader_names
    assert "personio_sources" in loader_names
    assert "scrapy_static_sources" in loader_names
    assert any(name.startswith("static_source::") for name in loader_names)


@pytest.mark.slow
@pytest.mark.integration
def test_pipeline_default_source_mix_smoke_excludes_wellfound_and_includes_guerrilla() -> None:
    google_csv = _fixture("google_sheets.csv")
    remote_json = _fixture_json("remoteok.json")
    gamesindustry_html = _fixture("gamesindustry_jobs.html")
    gamejobs_html = _fixture("gamejobs.html")
    workwithindies_html = _fixture("workwithindies.html")
    eightbitplay_html = _fixture("8bitplay_jobs.html")
    gracklehq_html = _fixture("gracklehq_jobs.html")
    greenhouse_json = _fixture_json("greenhouse_guerrilla_jobs.json")
    greenhouse_playstation_json = _fixture_json("greenhouse_playstation_jobs.json")
    teamtailor_listing = _fixture("teamtailor_listing.html")
    littlechicken_listing = _fixture("littlechicken_jobs_page.html")
    littlechicken_detail = _fixture("littlechicken_job_detail.html")
    lever_json = _fixture_json("lever_jobs.json")
    smart_json = _fixture_json("smartrecruiters_jobs.json")
    workable_json = _fixture_json("workable_jobs.json")
    ashby_html = _fixture("ashby_jobs.html")
    recruitee_json = _fixture_json("recruitee_jobs.json")
    pinpoint_json = _fixture_json("pinpoint_jobs.json")
    breezy_html = _fixture("breezy_jobs.html")
    jazzhr_html = _fixture("jazzhr_jobs.html")
    personio_xml = _fixture("personio_feed.xml")
    littlechicken_source = {
        "name": "Little Chicken (Manual Website)",
        "studio": "Little Chicken",
        "adapter": "static",
        "company": "Little Chicken",
        "pages": ["https://www.littlechicken.nl/jobs/"],
        "enabledByDefault": True,
        "id": "static:listing_url:https://www.littlechicken.nl/jobs/",
    }
    littlechicken_loader_name = static_adapter.static_source_name_for_registry_row(
        littlechicken_source
    )
    paradox_links = jf.parse_teamtailor_listing_links(
        teamtailor_listing,
        base_url="https://career.paradoxplaza.com/jobs",
    )

    def fake_fetch(url: str, _: int) -> str:
        if url == "https://www.littlechicken.nl/jobs/":
            return littlechicken_listing
        if "littlechicken.nl/job/" in url:
            return littlechicken_detail
        if "jobs.gamesindustry.biz" in url:
            return gamesindustry_html
        raise RuntimeError(f"Unhandled URL in fake fetch: {url}")

    class _FakeRedirectResolver:
        def resolve(self, url: str) -> str:
            return url

        def seed_cache(self, cache: dict[str, str]) -> None:
            _ = cache

        def close(self) -> None:
            return None

    with (
        workspace_tmpdir("jobs-fetcher") as tmp,
        mock.patch.object(jf, "build_redirect_resolver", return_value=_FakeRedirectResolver()),
        mock.patch.object(
            jf,
            "default_source_loaders",
            return_value=[
                ("google_sheets", lambda **_: jf.parse_google_sheets_csv(google_csv)),
                ("google_sheets_1er2oaxo", lambda **_: jf.parse_google_sheets_csv(google_csv)),
                ("google_sheets_1mvqhxat", lambda **_: jf.parse_google_sheets_csv(google_csv)),
                ("remote_ok", lambda **_: jf.parse_remote_ok_payload(remote_json)),
                (
                    "gamesindustry",
                    lambda **_: jf.parse_gamesindustry_html(
                        gamesindustry_html,
                        base_url="https://jobs.gamesindustry.biz/jobs/",
                    ),
                ),
                (
                    "gamejobs",
                    lambda **_: jf.parse_gamejobs_html(
                        gamejobs_html,
                        base_url="https://gamejobs.co/",
                    ),
                ),
                (
                    "workwithindies",
                    lambda **_: jf.parse_workwithindies_html(
                        workwithindies_html,
                        base_url="https://www.workwithindies.com/",
                    ),
                ),
                (
                    "8bitplay",
                    lambda **_: jf.parse_8bitplay_html(
                        eightbitplay_html,
                        base_url="https://8bitplay.com/jobs/",
                    ),
                ),
                (
                    "gracklehq",
                    lambda **_: jf.parse_gracklehq_html(
                        gracklehq_html,
                        base_url="https://gracklehq.com/jobs",
                    ),
                ),
                (
                    "greenhouse_boards",
                    lambda **_: jf.parse_greenhouse_jobs_payload(
                        greenhouse_json,
                        "guerrilla-games",
                        fallback_company="Guerrilla Games",
                    )
                    + jf.parse_greenhouse_jobs_payload(
                        greenhouse_playstation_json,
                        "sonyinteractiveentertainmentglobal",
                        fallback_company="PlayStation Global",
                    ),
                ),
                (
                    "teamtailor_sources",
                    lambda **_: [
                        {
                            "sourceJobId": "teamtailor:paradox:1",
                            "title": "Senior Rendering Programmer",
                            "company": "Paradox Interactive",
                            "city": "Stockholm",
                            "country": "SE",
                            "workType": "Hybrid",
                            "contractType": "Full Time",
                            "jobLink": paradox_links[0],
                            "sector": "Game",
                            "postedAt": "",
                        }
                    ],
                ),
                (
                    "lever_sources",
                    lambda **_: jf.parse_lever_jobs_payload(
                        lever_json,
                        "pixelforge",
                        fallback_company="Pixel Forge",
                    ),
                ),
                (
                    "smartrecruiters_sources",
                    lambda **_: jf.parse_smartrecruiters_jobs_payload(
                        smart_json,
                        "ubisoft2",
                        fallback_company="Ubisoft",
                    ),
                ),
                (
                    "workable_sources",
                    lambda **_: jf.parse_workable_jobs_payload(
                        workable_json,
                        "pixeldominion",
                        fallback_company="Pixel Dominion",
                    ),
                ),
                (
                    "recruitee_sources",
                    lambda **_: jf.parse_recruitee_jobs_payload(
                        recruitee_json,
                        "jobs.crazygames.com",
                        fallback_company="CrazyGames",
                    ),
                ),
                (
                    "pinpoint_sources",
                    lambda **_: jf.parse_pinpoint_jobs_payload(
                        pinpoint_json,
                        "gameplaygalaxy",
                        fallback_company="Gameplay Galaxy",
                    ),
                ),
                (
                    "ashby_sources",
                    lambda **_: jf.parse_ashby_jobs_from_html(
                        ashby_html,
                        "https://jobs.ashbyhq.com/jagex/jobs",
                        "Jagex",
                    ),
                ),
                (
                    "breezy_sources",
                    lambda **_: jf.parse_breezy_jobs_html(
                        breezy_html,
                        "https://yallaplay.breezy.hr/",
                        "YallaPlay",
                    ),
                ),
                (
                    "jazzhr_sources",
                    lambda **_: jf.parse_jazzhr_jobs_html(
                        jazzhr_html,
                        "https://lostboysinteractive.applytojob.com/apply",
                        "Lost Boys Interactive",
                    ),
                ),
                (
                    "personio_sources",
                    lambda **_: jf.parse_personio_feed_xml(
                        personio_xml,
                        source_name="InnoGames",
                    ),
                ),
                (
                    littlechicken_loader_name,
                    lambda **kwargs: static_adapter.run_static_source_entry_source(
                        source_row=littlechicken_source,
                        diagnostics_name=littlechicken_loader_name,
                        **kwargs,
                    ),
                ),
            ],
        ),
    ):
        report = jf.run_pipeline(
            output_dir=Path(tmp),
            fetch_text=fake_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
        )

        sources = {row["name"]: row for row in report["sources"]}
        assert sources["google_sheets"]["status"] == "ok"
        assert sources["google_sheets_1er2oaxo"]["status"] == "ok"
        assert sources["google_sheets_1mvqhxat"]["status"] == "ok"
        assert sources["remote_ok"]["status"] == "ok"
        assert sources["gamesindustry"]["status"] == "ok"
        assert sources["gamejobs"]["status"] == "ok"
        assert sources["workwithindies"]["status"] == "ok"
        assert sources["8bitplay"]["status"] == "ok"
        assert sources["gracklehq"]["status"] == "ok"
        assert sources["greenhouse_boards"]["status"] == "ok"
        assert sources["teamtailor_sources"]["status"] == "ok"
        assert sources["lever_sources"]["status"] == "ok"
        assert sources["smartrecruiters_sources"]["status"] == "ok"
        assert sources["workable_sources"]["status"] == "ok"
        assert sources["recruitee_sources"]["status"] == "ok"
        assert sources["pinpoint_sources"]["status"] == "ok"
        assert sources["ashby_sources"]["status"] == "ok"
        assert sources["breezy_sources"]["status"] == "ok"
        assert sources["jazzhr_sources"]["status"] == "ok"
        assert sources["personio_sources"]["status"] == "ok"
        static_rows = [
            row for row in report["sources"] if str(row.get("adapter") or "").lower() == "static"
        ]
        assert static_rows
        assert any(str(row.get("status") or "").lower() == "ok" for row in static_rows)
        assert sources["wellfound"]["status"] == "excluded"
        assert "disabled_by_default" in sources["wellfound"]["error"]
        assert sources["greenhouse_boards"]["adapter"] == "greenhouse"
        assert sources["teamtailor_sources"]["adapter"] == "teamtailor"
        assert sources["lever_sources"]["adapter"] == "lever"
        assert sources["smartrecruiters_sources"]["adapter"] == "smartrecruiters"
        assert sources["workable_sources"]["adapter"] == "workable"
        assert sources["recruitee_sources"]["adapter"] == "recruitee"
        assert sources["pinpoint_sources"]["adapter"] == "pinpoint"
        assert sources["8bitplay"]["adapter"] == "html"
        assert sources["gracklehq"]["adapter"] == "html"
        assert sources["ashby_sources"]["adapter"] == "ashby"
        assert sources["breezy_sources"]["adapter"] == "breezy"
        assert sources["jazzhr_sources"]["adapter"] == "jazzhr"
        assert sources["personio_sources"]["adapter"] == "personio"
        assert "failedSources" in report["summary"]
        assert report["summary"]["excludedSources"] == 1
        assert "targetRoleCount" in report["summary"]
        assert "netherlandsCount" in report["summary"]
        assert "remoteCount" in report["summary"]
        assert "rawFetchedCount" in report["summary"]
        assert "uniqueOutputCount" in report["summary"]
        assert "sourceBundleCollisions" in report["summary"]

        rows = json.loads((Path(tmp) / "jobs-unified.json").read_text(encoding="utf-8"))
        assert any("guerrilla" in row.get("company", "").lower() for row in rows)
        assert any("playstation" in row.get("company", "").lower() for row in rows)
        assert any("paradox" in row.get("company", "").lower() for row in rows)
        assert any("pixel forge" in row.get("company", "").lower() for row in rows)
        assert any("moonshot games" in row.get("company", "").lower() for row in rows)
        assert any("pixel dominion" in row.get("company", "").lower() for row in rows)
        assert any("ubisoft" in row.get("company", "").lower() for row in rows)
        assert any("crazygames" in row.get("company", "").lower() for row in rows)
        assert any("gameplay galaxy" in row.get("company", "").lower() for row in rows)
        assert any("little chicken" in row.get("company", "").lower() for row in rows)
        assert all("focusScore" in row for row in rows)
        assert all("sourceBundleCount" in row for row in rows)
        assert all("sourceBundle" in row for row in rows)
        all_errors = " ".join(row.get("error", "") for row in report["sources"])
        assert "403" not in all_errors


def test_run_pipeline_writes_normalized_report_task_and_source_state_contracts() -> None:
    def ok_loader(**_: object):
        return [
            {
                "title": "Engine Programmer",
                "company": "Contract Studio",
                "city": "Remote",
                "country": "Remote",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": "https://example.com/contract/engine-programmer",
                "sector": "Game",
                "sourceJobId": "contract-1",
                "postedAt": "2026-03-01",
            }
        ]

    with workspace_tmpdir("jobs-fetcher") as tmp:
        out = Path(tmp)
        report = jf.run_pipeline(
            output_dir=out,
            source_loaders=[("ok_source", ok_loader)],
            max_workers=2,
            max_per_domain=2,
        )
        assert str(report.get("schemaVersion") or "") == str(jf.SCHEMA_VERSION)
        runtime = report.get("runtime") or {}
        assert int(runtime.get("maxWorkers") or 0) == 2
        assert int(runtime.get("maxPerDomain") or 0) == 2
        assert str(runtime.get("fetchStrategy") or "") == "auto"
        assert str(runtime.get("fetchClient") or "") in {"urllib", "httpx_async"}
        assert (
            int(runtime.get("adapterHttpConcurrency") or 0) == jf.DEFAULT_ADAPTER_HTTP_CONCURRENCY
        )
        assert (
            int(runtime.get("staticDetailConcurrency") or 0) == jf.DEFAULT_STATIC_DETAIL_CONCURRENCY
        )
        assert (
            int(runtime.get("googleSheetsRedirectConcurrency") or 0)
            == jf.DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY
        )
        assert int(runtime.get("selectedSourceCount") or 0) == 1
        assert isinstance(runtime.get("slowestSources"), list)
        timing = runtime.get("timingSummary") or {}
        assert "medianSourceDurationMs" in timing
        assert "p95SourceDurationMs" in timing
        assert "stageTotalsMs" in timing
        assert "adapterTimings" in timing
        assert "summary" in report
        assert "sources" in report
        assert str(report["sources"][0].get("fetchStrategy") or "") == "auto"
        assert "loss" in report["sources"][0]
        assert "canonicalDropReasons" in (report["sources"][0].get("loss") or {})
        stage_timings = report["sources"][0].get("stageTimingsMs") or {}
        if stage_timings:
            assert "fetchAndParse" in stage_timings
        adapter_timings = timing.get("adapterTimings") or []
        if adapter_timings:
            assert str(adapter_timings[0].get("adapter") or "") == "custom"

        task_payload = json.loads((out / "jobs-fetch-tasks.json").read_text(encoding="utf-8"))
        assert str(task_payload.get("schemaVersion") or "") == str(jf.SCHEMA_VERSION)
        assert "heartbeatAt" in task_payload
        assert "summary" in task_payload
        assert "tasks" in task_payload
        assert "outputs" in task_payload
        assert str((task_payload.get("outputs") or {}).get("report") or "") == str(
            out / "jobs-fetch-report.json"
        )
        assert str((task_payload.get("tasks") or [])[0].get("status") or "") == "ok"
        assert (
            str(((report.get("runtime") or {}).get("lifecycle") or {}).get("owner") or "")
            == "fetch_report"
        )

        state_payload = json.loads((out / "jobs-source-state.json").read_text(encoding="utf-8"))
        assert str(state_payload.get("schemaVersion") or "") == str(jf.SCHEMA_VERSION)
        sources_state = state_payload.get("sources") or {}
        assert "ok_source" in sources_state
        assert int((sources_state["ok_source"]).get("consecutiveFailures") or 0) == 0


def test_run_pipeline_includes_selection_exclusions() -> None:
    def ok_loader(**_: object):
        return [
            {
                "title": "Technical Artist",
                "company": "Incl Studio",
                "city": "Remote",
                "country": "Remote",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": "https://example.com/included",
                "sector": "Game",
                "sourceJobId": "incl-1",
                "postedAt": "2026-03-01",
            }
        ]

    with workspace_tmpdir("jobs-fetcher") as tmp:
        out = Path(tmp)
        report = jf.run_pipeline(
            output_dir=out,
            source_loaders=[("included_source", ok_loader)],
            selection_exclusions=[
                {
                    "name": "excluded_source",
                    "status": "excluded",
                    "adapter": "custom",
                    "fetchStrategy": "auto",
                    "studio": "",
                    "fetchedCount": 0,
                    "keptCount": 0,
                    "error": "only_sources_filter",
                    "exclusionReason": "only_sources_filter",
                    "durationMs": 0,
                }
            ],
        )
        excluded_rows = [
            row for row in (report.get("sources") or []) if row.get("name") == "excluded_source"
        ]
        assert len(excluded_rows) == 1
        assert str(excluded_rows[0].get("status") or "") == "excluded"
        assert str(excluded_rows[0].get("exclusionReason") or "") == "only_sources_filter"


def test_should_skip_source_by_ttl_honors_recent_success_and_failure_state() -> None:
    now = jf.now_iso()
    rows = {"source_a": {"lastSuccessAt": now, "consecutiveFailures": 0}}
    assert jf.should_skip_source_by_ttl("source_a", rows, ttl_minutes=360)

    rows["source_a"]["consecutiveFailures"] = 2
    assert not jf.should_skip_source_by_ttl("source_a", rows, ttl_minutes=360)


def test_should_skip_source_by_cadence_uses_hot_and_cold_windows() -> None:
    now = jf.datetime.now(jf.timezone.utc)
    rows = {
        "hot_source": {
            "lastSuccessAt": (now - jf.timedelta(minutes=10)).isoformat(),
            "lastChangedAt": (now - jf.timedelta(minutes=30)).isoformat(),
            "consecutiveFailures": 0,
        },
        "cold_source": {
            "lastSuccessAt": (now - jf.timedelta(minutes=20)).isoformat(),
            "lastChangedAt": (now - jf.timedelta(days=2)).isoformat(),
            "consecutiveFailures": 0,
        },
    }
    assert jf.should_skip_source_by_cadence("hot_source", rows, hot_minutes=15, cold_minutes=60)
    assert jf.should_skip_source_by_cadence("cold_source", rows, hot_minutes=15, cold_minutes=60)

    rows["hot_source"]["lastSuccessAt"] = (now - jf.timedelta(minutes=20)).isoformat()
    rows["cold_source"]["lastSuccessAt"] = (now - jf.timedelta(minutes=70)).isoformat()
    assert not jf.should_skip_source_by_cadence("hot_source", rows, hot_minutes=15, cold_minutes=60)
    assert not jf.should_skip_source_by_cadence(
        "cold_source", rows, hot_minutes=15, cold_minutes=60
    )


def test_get_incremental_cache_decision_prefers_skip_and_listing_modes() -> None:
    from src.jobs import state as state_pkg

    now = jf.datetime.now(jf.timezone.utc)
    rows = {
        "provider_source": {
            "lastAdapter": "greenhouse",
            "lastStatus": "ok",
            "lastSuccessAt": (now - jf.timedelta(minutes=10)).isoformat(),
            "lastChangedAt": (now - jf.timedelta(minutes=20)).isoformat(),
            "lastKeptCount": 3,
        },
        "static_source::example": {
            "lastAdapter": "static",
            "lastStatus": "ok",
            "lastSuccessAt": (now - jf.timedelta(minutes=20)).isoformat(),
            "lastKeptCount": 2,
            "lastListingFingerprint": "abc123",
        },
    }
    provider_decision = state_pkg.get_incremental_cache_decision(
        "provider_source", rows, adapter="greenhouse"
    )
    static_decision = state_pkg.get_incremental_cache_decision(
        "static_source::example", rows, adapter="static"
    )
    assert provider_decision["cacheDecision"] == "skip_fresh"
    assert static_decision["cacheDecision"] == "listing_only"


def test_get_incremental_cache_decision_treats_future_next_eligible_after_run_as_skip_fresh() -> (
    None
):
    from src.jobs import state as state_pkg

    now = jf.datetime.now(jf.timezone.utc)
    rows = {
        "provider_board": {
            "lastAdapter": "lever",
            "lastStatus": "ok",
            "lastSuccessAt": (now - jf.timedelta(minutes=1)).isoformat(),
            "lastKeptCount": 5,
            "nextEligibleCheckAt": (now + jf.timedelta(minutes=30)).isoformat(),
            "cacheDecision": "run_now",
            "cacheDecisionReason": "no_cache_state",
        }
    }
    decision = state_pkg.get_incremental_cache_decision("provider_board", rows, adapter="lever")
    assert decision["cacheDecision"] == "skip_fresh"
    assert decision["cacheDecisionReason"] == "within_freshness_window"


def test_run_pipeline_incremental_second_run_skips_fresh_source_and_preserves_output() -> None:
    calls = {"count": 0}

    def ok_loader(**_: object):
        calls["count"] += 1
        return [
            {
                "title": "Gameplay Engineer",
                "company": "Incremental Studio",
                "city": "Remote",
                "country": "Remote",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": "https://example.com/incremental/gameplay-engineer",
                "sector": "Game",
                "sourceJobId": "incremental-1",
                "postedAt": "2026-03-01",
            }
        ]

    with workspace_tmpdir("jobs-fetcher-incremental") as tmp:
        out = Path(tmp)
        first = jf.run_pipeline(
            output_dir=out, source_loaders=[("incremental_source", ok_loader)], show_progress=False
        )
        second = jf.run_pipeline(
            output_dir=out, source_loaders=[("incremental_source", ok_loader)], show_progress=False
        )
        assert calls["count"] == 1
        assert int(first["summary"].get("outputCount") or 0) == 1
        assert int(second["summary"].get("outputCount") or 0) == 1
        excluded = [
            row for row in (second.get("sources") or []) if row.get("name") == "incremental_source"
        ]
        assert len(excluded) == 1
        assert str(excluded[0].get("status") or "") == "excluded"
        assert str(excluded[0].get("cacheDecision") or "") == "skip_fresh"
        assert "cache_" in str(excluded[0].get("exclusionReason") or "")


def test_run_pipeline_force_refresh_all_bypasses_incremental_skip() -> None:
    calls = {"count": 0}

    def ok_loader(**_: object):
        calls["count"] += 1
        return [
            {
                "title": "Engine Programmer",
                "company": "Refresh Studio",
                "city": "Remote",
                "country": "Remote",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": "https://example.com/refresh/engine-programmer",
                "sector": "Game",
                "sourceJobId": "refresh-1",
                "postedAt": "2026-03-01",
            }
        ]

    with workspace_tmpdir("jobs-fetcher-force-refresh") as tmp:
        out = Path(tmp)
        jf.run_pipeline(
            output_dir=out, source_loaders=[("refresh_source", ok_loader)], show_progress=False
        )
        jf.run_pipeline(
            output_dir=out,
            source_loaders=[("refresh_source", ok_loader)],
            show_progress=False,
            force_refresh_all=True,
        )
        assert calls["count"] == 2


def test_apply_incremental_cache_exclusions_keeps_provider_family_loader_for_board_level_refresh() -> (
    None
):
    from src.jobs import pipeline_loader_selection as selection_pkg
    from src.jobs import state as state_pkg

    now = jf.datetime.now(jf.timezone.utc)
    future = (now + jf.timedelta(minutes=10)).isoformat()
    selected = [
        ("greenhouse_boards", lambda **_: []),
        ("incremental_source", lambda **_: []),
    ]
    source_state_rows = {
        "greenhouse_boards": {
            "lastAdapter": "greenhouse",
            "lastStatus": "ok",
            "lastSuccessAt": (now - jf.timedelta(minutes=5)).isoformat(),
            "lastKeptCount": 2,
            "nextEligibleCheckAt": future,
            "cacheDecision": "skip_fresh",
            "cacheDecisionReason": "within_freshness_window",
        },
        "incremental_source": {
            "lastAdapter": "custom",
            "lastStatus": "ok",
            "lastSuccessAt": (now - jf.timedelta(minutes=5)).isoformat(),
            "lastKeptCount": 1,
            "nextEligibleCheckAt": future,
            "cacheDecision": "skip_fresh",
            "cacheDecisionReason": "within_freshness_window",
        },
    }
    filtered, skipped = selection_pkg.apply_incremental_cache_exclusions(
        selected,
        incremental_cache_enabled=True,
        force_refresh_all=False,
        source_state_rows=source_state_rows,
        get_incremental_cache_decision=state_pkg.get_incremental_cache_decision,
        build_excluded_source_report=lambda name, reason: {"name": name, "exclusionReason": reason},
        source_report_meta={
            "greenhouse_boards": {"adapter": "greenhouse"},
            "incremental_source": {"adapter": "custom"},
        },
    )
    assert [name for name, _ in filtered] == ["greenhouse_boards"]
    assert [row["name"] for row in skipped] == ["incremental_source"]


def test_provider_family_json_sources_refresh_only_stale_boards() -> None:
    from src.jobs.adapters.plugins.provider_api import json_feed as json_feed_module

    calls = []
    captured = {}

    def _registry_entries(adapter: str):
        assert adapter == "greenhouse"
        return [
            {
                "name": "Fresh Board",
                "studio": "Fresh Board",
                "endpoint": "https://example.com/fresh.json",
            },
            {
                "name": "Stale Board",
                "studio": "Stale Board",
                "endpoint": "https://example.com/stale.json",
            },
        ]

    def _fetch_with_retries(
        url: str, fetch_text, timeout_s: int, retries: int, backoff_s: float
    ) -> str:
        calls.append(url)
        return json.dumps({"jobs": [{"id": url}]})

    def _set_source_diagnostics(source_name: str, **kwargs) -> None:
        captured["source_name"] = source_name
        captured["kwargs"] = kwargs

    now = jf.datetime.now(jf.timezone.utc)
    state_rows = {
        "Fresh Board": {
            "lastAdapter": "greenhouse",
            "lastStatus": "ok",
            "lastSuccessAt": (now - jf.timedelta(minutes=5)).isoformat(),
            "lastKeptCount": 2,
            "nextEligibleCheckAt": (now + jf.timedelta(minutes=10)).isoformat(),
            "cacheDecision": "skip_fresh",
            "cacheDecisionReason": "within_freshness_window",
        },
        "Stale Board": {
            "lastAdapter": "greenhouse",
            "lastStatus": "ok",
            "lastSuccessAt": (now - jf.timedelta(hours=3)).isoformat(),
            "lastChangedAt": (now - jf.timedelta(days=2)).isoformat(),
            "lastKeptCount": 2,
        },
    }

    with (
        mock.patch.object(json_feed_module, "registry_entries", side_effect=_registry_entries),
        mock.patch.object(json_feed_module, "fetch_with_retries", side_effect=_fetch_with_retries),
        mock.patch.object(
            json_feed_module,
            "set_source_diagnostics",
            side_effect=_set_source_diagnostics,
        ),
    ):
        rows = json_feed_module._run_json_feed_sources(
            adapter_name="greenhouse",
            registry_adapter="greenhouse",
            default_error="missing endpoint",
            parse_payload=lambda source, payload, studio: [
                {
                    "title": f"{studio} Engineer",
                    "company": studio,
                    "city": "",
                    "country": "Unknown",
                    "workType": "",
                    "contractType": "",
                    "jobLink": f"https://example.com/{str(source.get('name') or '').lower().replace(' ', '-')}",
                    "sector": "Game",
                    "postedAt": "",
                    "sourceJobId": f"greenhouse:{str(source.get('name') or '')}",
                }
            ],
            build_url=lambda source: str(source.get("endpoint") or ""),
            payload_count=lambda payload, parsed: len(parsed),
            fetch_text=lambda url, timeout: "",
            timeout_s=5,
            retries=0,
            backoff_s=0,
            source_state_rows=state_rows,
            force_refresh_all=False,
        )
    assert calls == ["https://example.com/stale.json"]
    assert len(rows) == 1
    details = captured["kwargs"]["details"]
    fresh_detail = next(row for row in details if row["name"] == "Fresh Board")
    stale_detail = next(row for row in details if row["name"] == "Stale Board")
    assert fresh_detail["status"] == "excluded"
    assert fresh_detail["cacheDecision"] == "skip_fresh"
    assert stale_detail["status"] == "ok"
    assert stale_detail["cacheDecision"] == "run_now"


def test_provider_family_revalidate_only_board_skips_fetch_on_not_modified() -> None:
    from src.jobs.adapters.plugins.provider_api import json_feed as json_feed_module

    calls = []
    captured = {}

    def _registry_entries(adapter: str):
        assert adapter == "lever"
        return [
            {
                "name": "Revalidate Board",
                "studio": "Revalidate Board",
                "endpoint": "https://example.com/revalidate.json",
            }
        ]

    def _fetch_with_retries(
        url: str, fetch_text, timeout_s: int, retries: int, backoff_s: float
    ) -> str:
        calls.append(url)
        return "[]"

    def _set_source_diagnostics(source_name: str, **kwargs) -> None:
        captured["kwargs"] = kwargs

    now = jf.datetime.now(jf.timezone.utc)
    state_rows = {
        "Revalidate Board": {
            "lastAdapter": "lever",
            "lastStatus": "ok",
            "lastSuccessAt": (now - jf.timedelta(minutes=30)).isoformat(),
            "lastChangedAt": (now - jf.timedelta(days=2)).isoformat(),
            "lastKeptCount": 1,
            "lastHttpEtag": "etag-1",
        }
    }

    with (
        mock.patch.object(json_feed_module, "registry_entries", side_effect=_registry_entries),
        mock.patch.object(json_feed_module, "fetch_with_retries", side_effect=_fetch_with_retries),
        mock.patch.object(
            json_feed_module,
            "set_source_diagnostics",
            side_effect=_set_source_diagnostics,
        ),
        mock.patch.object(
            json_feed_module,
            "conditional_revalidate_url",
            return_value={
                "supported": True,
                "notModified": True,
                "statusCode": 304,
                "etag": "etag-1",
                "lastModified": "",
            },
        ),
    ):
        rows = json_feed_module._run_json_feed_sources(
            adapter_name="lever",
            registry_adapter="lever",
            default_error="missing endpoint",
            parse_payload=lambda source, payload, studio: [],
            build_url=lambda source: str(source.get("endpoint") or ""),
            payload_count=lambda payload, parsed: len(parsed),
            fetch_text=lambda url, timeout: "",
            timeout_s=5,
            retries=0,
            backoff_s=0,
            source_state_rows=state_rows,
            force_refresh_all=False,
        )
    assert rows == []
    assert calls == []
    details = captured["kwargs"]["details"]
    assert len(details) == 1
    assert details[0]["status"] == "excluded"
    assert details[0]["cacheDecision"] == "revalidate_only"
    assert details[0]["cacheDecisionReason"] == "not_modified_304"
    assert details[0]["httpStatus"] == 304


def test_teamtailor_sources_skip_fresh_listing_without_fetching() -> None:
    from src.jobs.adapters.plugins.provider_api import teamtailor_runner as teamtailor_module

    calls = []
    captured = {}

    def _registry_entries(adapter: str):
        assert adapter == "teamtailor"
        return [
            {
                "name": "Paradox Teamtailor",
                "studio": "Paradox Interactive",
                "listing_url": "https://career.paradoxplaza.com/jobs",
                "base_url": "https://career.paradoxplaza.com",
                "company": "Paradox Interactive",
            }
        ]

    def _fetch_with_retries(
        url: str, fetch_text, timeout_s: int, retries: int, backoff_s: float
    ) -> str:
        calls.append(url)
        return ""

    def _set_source_diagnostics(source_name: str, **kwargs) -> None:
        captured["kwargs"] = kwargs

    now = jf.datetime.now(jf.timezone.utc)
    state_rows = {
        "Paradox Teamtailor": {
            "lastAdapter": "teamtailor",
            "lastStatus": "ok",
            "lastSuccessAt": (now - jf.timedelta(minutes=5)).isoformat(),
            "lastKeptCount": 3,
            "nextEligibleCheckAt": (now + jf.timedelta(minutes=20)).isoformat(),
            "cacheDecision": "skip_fresh",
            "cacheDecisionReason": "within_freshness_window",
        }
    }

    with (
        mock.patch.object(teamtailor_module, "registry_entries", side_effect=_registry_entries),
        mock.patch.object(teamtailor_module, "fetch_with_retries", side_effect=_fetch_with_retries),
        mock.patch.object(
            teamtailor_module,
            "set_source_diagnostics",
            side_effect=_set_source_diagnostics,
        ),
    ):
        rows = teamtailor_module._run_teamtailor_sources(
            fetch_text=lambda url, timeout: "",
            timeout_s=5,
            retries=0,
            backoff_s=0,
            source_state_rows=state_rows,
            force_refresh_all=False,
        )
    assert rows == []
    assert calls == []
    details = captured["kwargs"]["details"]
    assert len(details) == 1
    assert details[0]["status"] == "excluded"
    assert details[0]["cacheDecision"] == "skip_fresh"
    assert details[0]["cacheDecisionReason"] == "within_freshness_window"


def test_apply_incremental_cache_exclusions_keeps_social_multi_feed_loaders_for_detail_level_refresh() -> (
    None
):
    from src.jobs import pipeline_loader_selection as selection_pkg
    from src.jobs import state as state_pkg

    now = jf.datetime.now(jf.timezone.utc)
    future = (now + jf.timedelta(minutes=10)).isoformat()
    selected = [
        ("social_x", lambda **_: []),
        ("social_mastodon", lambda **_: []),
        ("social_reddit", lambda **_: []),
    ]
    source_state_rows = {
        "social_x": {
            "lastAdapter": "social",
            "nextEligibleCheckAt": future,
            "cacheDecision": "skip_fresh",
            "cacheDecisionReason": "within_freshness_window",
        },
        "social_mastodon": {
            "lastAdapter": "social",
            "nextEligibleCheckAt": future,
            "cacheDecision": "skip_fresh",
            "cacheDecisionReason": "within_freshness_window",
        },
        "social_reddit": {
            "lastAdapter": "social",
            "nextEligibleCheckAt": future,
            "cacheDecision": "skip_fresh",
            "cacheDecisionReason": "within_freshness_window",
        },
    }
    filtered, skipped = selection_pkg.apply_incremental_cache_exclusions(
        selected,
        incremental_cache_enabled=True,
        force_refresh_all=False,
        source_state_rows=source_state_rows,
        get_incremental_cache_decision=state_pkg.get_incremental_cache_decision,
        build_excluded_source_report=lambda name, reason: {"name": name, "exclusionReason": reason},
        source_report_meta={
            "social_x": {"adapter": "social"},
            "social_mastodon": {"adapter": "social"},
            "social_reddit": {"adapter": "social"},
        },
    )
    assert [name for name, _ in filtered] == ["social_x", "social_mastodon"]
    assert [row["name"] for row in skipped] == ["social_reddit"]


def test_social_x_skips_fresh_query_without_fetching() -> None:
    cfg = {
        "enabled": True,
        "minConfidence": 40,
        "rejectForHirePosts": True,
        "x": {
            "enabled": True,
            "queries": ["game jobs"],
            "rssFallback": {"enabled": True, "instances": ["https://nitter.example"]},
        },
    }
    now = jf.datetime.now(jf.timezone.utc)
    state_rows = {
        "x:game jobs": {
            "lastAdapter": "social",
            "lastStatus": "ok",
            "lastSuccessAt": (now - jf.timedelta(minutes=5)).isoformat(),
            "lastKeptCount": 2,
            "nextEligibleCheckAt": (now + jf.timedelta(minutes=20)).isoformat(),
            "cacheDecision": "skip_fresh",
            "cacheDecisionReason": "within_freshness_window",
        }
    }

    def failing_fetch(url: str, timeout: int) -> str:
        raise AssertionError("social_x fetch should be skipped for fresh query")

    rows = jf.run_social_x_source(
        fetch_text=failing_fetch,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        social_config=cfg,
        source_state_rows=state_rows,
        force_refresh_all=False,
    )
    assert rows == []
    diag = jf.SOURCE_DIAGNOSTICS.get("social_x") or {}
    details = diag.get("details") or []
    assert len(details) == 1
    assert details[0]["status"] == "excluded"
    assert details[0]["cacheDecision"] == "skip_fresh"


def test_social_mastodon_skips_fresh_instance_tag_without_fetching() -> None:
    cfg = {
        "enabled": True,
        "minConfidence": 40,
        "rejectForHirePosts": True,
        "mastodon": {
            "enabled": True,
            "instances": ["https://mastodon.example"],
            "hashtags": ["gamedevjobs"],
        },
    }
    now = jf.datetime.now(jf.timezone.utc)
    state_rows = {
        "mastodon:mastodon.example:#gamedevjobs": {
            "lastAdapter": "social",
            "lastStatus": "ok",
            "lastSuccessAt": (now - jf.timedelta(minutes=5)).isoformat(),
            "lastKeptCount": 2,
            "nextEligibleCheckAt": (now + jf.timedelta(minutes=20)).isoformat(),
            "cacheDecision": "skip_fresh",
            "cacheDecisionReason": "within_freshness_window",
        }
    }

    def failing_fetch(url: str, timeout: int) -> str:
        raise AssertionError("social_mastodon fetch should be skipped for fresh instance/tag")

    rows = jf.run_social_mastodon_source(
        fetch_text=failing_fetch,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        social_config=cfg,
        source_state_rows=state_rows,
        force_refresh_all=False,
    )
    assert rows == []
    diag = jf.SOURCE_DIAGNOSTICS.get("social_mastodon") or {}
    details = diag.get("details") or []
    assert len(details) == 1
    assert details[0]["status"] == "excluded"
    assert details[0]["cacheDecision"] == "skip_fresh"


def test_social_reddit_skips_fresh_subreddit_without_fetching() -> None:
    cfg = {
        "enabled": True,
        "minConfidence": 40,
        "rejectForHirePosts": True,
        "reddit": {
            "enabled": True,
            "subreddits": ["gamedev"],
            "maxPostsPerSubreddit": 5,
            "rssFallback": True,
            "htmlFallback": True,
            "rateLimitDelay": 0,
        },
    }
    future = "2099-01-01T00:00:00+00:00"
    state_rows = {
        "reddit:r/gamedev": {
            "lastAdapter": "social",
            "nextEligibleCheckAt": future,
            "cacheDecision": "skip_fresh",
            "cacheDecisionReason": "within_freshness_window",
        }
    }

    def failing_fetch(_: str, __: int) -> str:
        raise AssertionError("social_reddit fetch should be skipped for fresh subreddit")

    rows = jf.run_social_reddit_source(
        fetch_text=failing_fetch,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        social_config=cfg,
        source_state_rows=state_rows,
        force_refresh_all=False,
    )
    assert rows == []
    diag = jf.SOURCE_DIAGNOSTICS.get("social_reddit") or {}
    details = diag.get("details") or []
    assert len(details) == 1
    assert details[0]["status"] == "excluded"
    assert details[0]["cacheDecision"] == "skip_fresh"


def test_run_social_reddit_source_keeps_successful_rss_fallback_out_of_error_state() -> None:
    cfg = {
        "enabled": True,
        "minConfidence": 20,
        "rejectForHirePosts": True,
        "reddit": {
            "enabled": True,
            "subreddits": ["gamedev"],
            "maxPostsPerSubreddit": 5,
            "rssFallback": True,
            "htmlFallback": False,
            "rateLimitDelay": 0,
        },
    }
    calls = []

    def fake_fetch(url: str, _: int) -> str:
        calls.append(url)
        if url.endswith("/new.json?limit=5"):
            raise RuntimeError("json api blocked")
        if url.endswith("/new.rss"):
            return "<feed />"
        raise AssertionError(f"unexpected reddit url: {url}")

    with mock.patch(
        "src.jobs.adapters.plugins.social.register._social_parsers.parse_reddit_rss_payload",
        return_value=(
            [
                {
                    "title": "Technical Artist",
                    "company": "Nebula Games",
                    "jobLink": "https://jobs.nebula.dev/ta",
                    "sourceJobId": "reddit:gamedev:abc123",
                    "source": "social_reddit",
                }
            ],
            0,
        ),
    ):
        rows = jf.run_social_reddit_source(
            fetch_text=fake_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            social_config=cfg,
        )
    assert len(rows) >= 1
    diag = jf.SOURCE_DIAGNOSTICS.get("social_reddit") or {}
    details = diag.get("details") or []
    assert len(details) == 1
    assert details[0]["status"] == "ok"
    assert details[0]["error"] == ""


def test_run_social_reddit_source_forwards_heartbeat_to_plugin_run() -> None:
    cfg = {
        "enabled": True,
        "minConfidence": 20,
        "rejectForHirePosts": True,
        "reddit": {
            "enabled": True,
            "subreddits": ["gamedev"],
            "maxPostsPerSubreddit": 5,
            "rssFallback": False,
            "htmlFallback": False,
            "rateLimitDelay": 0,
        },
    }
    heartbeat_calls: list[str] = []

    class _Plugin:
        def run(self, **kwargs):  # noqa: ANN001, ANN202
            heartbeat = kwargs.get("heartbeat_callback")
            assert callable(heartbeat)
            heartbeat()
            return []

    with (
        mock.patch("src.jobs.adapters.social.ensure_social_plugins"),
        mock.patch(
            "src.jobs.adapters.social.default_registry.select", return_value=(_Plugin(), None)
        ),
    ):
        rows = jf.run_social_reddit_source(
            fetch_text=lambda url, _: "{}",
            timeout_s=5,
            retries=0,
            backoff_s=0,
            social_config=cfg,
            heartbeat_callback=lambda: heartbeat_calls.append("tick"),
        )

    assert rows == []
    assert len(heartbeat_calls) >= 2


def test_run_social_reddit_source_keeps_successful_old_reddit_html_fallback_out_of_error_state() -> (
    None
):
    cfg = {
        "enabled": True,
        "minConfidence": 20,
        "rejectForHirePosts": True,
        "reddit": {
            "enabled": True,
            "subreddits": ["gamedev"],
            "maxPostsPerSubreddit": 5,
            "rssFallback": False,
            "htmlFallback": True,
            "rateLimitDelay": 0,
        },
    }
    calls = []

    def fake_fetch(url: str, _: int) -> str:
        calls.append(url)
        if url.endswith("/new.json?limit=5"):
            raise RuntimeError("json api blocked")
        if url.startswith("https://old.reddit.com/r/gamedev/new/"):
            return "<html />"
        raise AssertionError(f"unexpected reddit url: {url}")

    with mock.patch(
        "src.jobs.adapters.plugins.social.register._social_parsers.parse_reddit_html_payload",
        return_value=(
            [
                {
                    "title": "Technical Artist",
                    "company": "Nebula Games",
                    "jobLink": "https://jobs.nebula.dev/ta",
                    "sourceJobId": "reddit:gamedev:abc123",
                    "source": "social_reddit",
                }
            ],
            0,
        ),
    ):
        rows = jf.run_social_reddit_source(
            fetch_text=fake_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            social_config=cfg,
        )
    assert len(rows) >= 1
    assert any(url.startswith("https://old.reddit.com/r/gamedev/new/") for url in calls)
    diag = jf.SOURCE_DIAGNOSTICS.get("social_reddit") or {}
    details = diag.get("details") or []
    assert len(details) == 1
    assert details[0]["status"] == "ok"
    assert details[0]["error"] == ""


def test_run_pipeline_reports_social_subsource_cache_rollup() -> None:
    def social_loader(**_: object):
        jf.SOURCE_DIAGNOSTICS["social_x"] = {
            "adapter": "social",
            "studio": "x",
            "details": [
                {
                    "name": "x:game jobs",
                    "studio": "x",
                    "status": "excluded",
                    "cacheDecision": "skip_fresh",
                    "cacheDecisionReason": "within_freshness_window",
                },
                {
                    "name": "x:unity jobs",
                    "studio": "x",
                    "status": "ok",
                    "cacheDecision": "run_now",
                    "cacheDecisionReason": "provider_refresh_due",
                    "fetchedCount": 1,
                    "keptCount": 1,
                },
            ],
        }
        return [
            {
                "title": "Gameplay Engineer",
                "company": "Studio Social",
                "city": "",
                "country": "Unknown",
                "workType": "",
                "contractType": "",
                "jobLink": "https://example.com/social/gameplay-engineer",
                "sector": "Game",
                "sourceJobId": "social-x-1",
                "postedAt": "2026-03-01",
            }
        ]

    with workspace_tmpdir("jobs-fetcher-social-subsource-rollup") as tmp:
        report = jf.run_pipeline(
            output_dir=Path(tmp),
            source_loaders=[("social_x", social_loader)],
            show_progress=False,
            force_refresh_all=True,
        )
        row = next(item for item in report["sources"] if item["name"] == "social_x")
        assert row["subsourceCount"] == 2
        assert row["subsourceCacheDecisionCounts"] == {"skip_fresh": 1, "run_now": 1}
        assert row["subsourceSkippedCount"] == 1
        assert row["subsourceRefreshedCount"] == 1


def test_run_pipeline_reports_board_level_provider_cache_rollup() -> None:
    def provider_family_loader(**_: object):
        jf.SOURCE_DIAGNOSTICS["greenhouse_boards"] = {
            "adapter": "greenhouse",
            "studio": "multiple",
            "details": [
                {
                    "name": "Board A",
                    "studio": "Board A",
                    "status": "excluded",
                    "cacheDecision": "skip_fresh",
                    "cacheDecisionReason": "within_freshness_window",
                },
                {
                    "name": "Board B",
                    "studio": "Board B",
                    "status": "excluded",
                    "cacheDecision": "revalidate_only",
                    "cacheDecisionReason": "not_modified_304",
                    "httpStatus": 304,
                },
                {
                    "name": "Board C",
                    "studio": "Board C",
                    "status": "ok",
                    "cacheDecision": "run_now",
                    "cacheDecisionReason": "provider_refresh_due",
                    "fetchedCount": 1,
                    "keptCount": 1,
                },
            ],
        }
        return [
            {
                "title": "Gameplay Engineer",
                "company": "Board C",
                "city": "",
                "country": "Unknown",
                "workType": "",
                "contractType": "",
                "jobLink": "https://example.com/board-c/gameplay-engineer",
                "sector": "Game",
                "sourceJobId": "greenhouse:board-c:1",
                "postedAt": "2026-03-01",
            }
        ]

    with workspace_tmpdir("jobs-fetcher-provider-board-rollup") as tmp:
        report = jf.run_pipeline(
            output_dir=Path(tmp),
            source_loaders=[("greenhouse_boards", provider_family_loader)],
            show_progress=False,
            force_refresh_all=True,
        )
        row = next(item for item in report["sources"] if item["name"] == "greenhouse_boards")
        assert row["boardCount"] == 3
        assert row["boardCacheDecisionCounts"] == {
            "skip_fresh": 1,
            "revalidate_only": 1,
            "run_now": 1,
        }
        assert row["boardSkippedCount"] == 1
        assert row["boardRevalidatedCount"] == 1
        assert row["boardNotModifiedCount"] == 1
        assert row["boardRefreshedCount"] == 1


def test_run_pipeline_excludes_quarantined_source_unless_ignored() -> None:
    calls = {"count": 0}

    def ok_loader(**_: object):
        calls["count"] += 1
        return [
            {
                "title": "Gameplay Engineer",
                "company": "Circuit Studio",
                "city": "Remote",
                "country": "Remote",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": "https://example.com/circuit/gameplay-engineer",
                "sector": "Game",
                "sourceJobId": "circuit-1",
                "postedAt": "2026-03-01",
            }
        ]

    with workspace_tmpdir("jobs-fetcher") as tmp:
        out = Path(tmp)
        blocked_until = (jf.datetime.now(jf.timezone.utc) + jf.timedelta(hours=2)).isoformat()
        state_payload = {
            "updatedAt": jf.now_iso(),
            "sources": {
                "blocked_source": {
                    "consecutiveFailures": 3,
                    "quarantinedUntilAt": blocked_until,
                }
            },
        }
        (out / "jobs-source-state.json").write_text(json.dumps(state_payload), encoding="utf-8")

        blocked_report = jf.run_pipeline(
            output_dir=out,
            source_loaders=[("blocked_source", ok_loader)],
            circuit_breaker_failures=3,
            circuit_breaker_cooldown_minutes=180,
            ignore_circuit_breaker=False,
        )
        blocked_rows = [
            row for row in blocked_report.get("sources", []) if row.get("name") == "blocked_source"
        ]
        assert calls["count"] == 0
        assert len(blocked_rows) == 1
        assert str(blocked_rows[0].get("status") or "") == "excluded"
        assert "circuit_breaker_active_until" in str(blocked_rows[0].get("error") or "")

        unblocked_report = jf.run_pipeline(
            output_dir=out,
            source_loaders=[("blocked_source", ok_loader)],
            circuit_breaker_failures=3,
            circuit_breaker_cooldown_minutes=180,
            ignore_circuit_breaker=True,
        )
        unblocked_rows = [
            row
            for row in unblocked_report.get("sources", [])
            if row.get("name") == "blocked_source"
        ]
        assert calls["count"] == 1
        assert len(unblocked_rows) == 1
        assert str(unblocked_rows[0].get("status") or "") == "ok"


def test_pipeline_report_snapshot_contract() -> None:
    def ok_loader(**_: object):
        return [
            {
                "title": "Technical Artist",
                "company": "Snapshot Studio",
                "city": "Remote",
                "country": "Remote",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": "https://example.com/snapshot/ta",
                "sector": "Game",
                "sourceJobId": "snap-1",
                "postedAt": "2026-03-01",
            }
        ]

    with workspace_tmpdir("jobs-fetcher") as tmp:
        report = jf.run_pipeline(output_dir=Path(tmp), source_loaders=[("ok", ok_loader)])
        snapshot = {
            "schemaVersion": report.get("schemaVersion"),
            "summary": {
                "inputCount": int(report["summary"].get("inputCount") or 0),
                "mergedCount": int(report["summary"].get("mergedCount") or 0),
                "outputCount": int(report["summary"].get("outputCount") or 0),
                "rawFetchedCount": int(report["summary"].get("rawFetchedCount") or 0),
                "uniqueOutputCount": int(report["summary"].get("uniqueOutputCount") or 0),
                "sourceCount": int(report["summary"].get("sourceCount") or 0),
                "successfulSources": int(report["summary"].get("successfulSources") or 0),
                "failedSources": int(report["summary"].get("failedSources") or 0),
                "excludedSources": int(report["summary"].get("excludedSources") or 0),
            },
            "outputs": {
                "hasJson": bool(report.get("outputs", {}).get("json")),
                "hasCsv": bool(report.get("outputs", {}).get("csv")),
                "hasLightJson": bool(report.get("outputs", {}).get("lightJson")),
                "hasChangedFlags": isinstance(report.get("outputs", {}).get("changed"), dict),
            },
            "sources": [
                {
                    "name": str(report["sources"][0].get("name")),
                    "status": str(report["sources"][0].get("status")),
                    "fetchedCount": int(report["sources"][0].get("fetchedCount") or 0),
                    "keptCount": int(report["sources"][0].get("keptCount") or 0),
                }
            ],
        }
        assert snapshot == _fixture_json("jobs_fetch_report_snapshot.json")


def test_run_pipeline_records_wall_clock_timing_summary() -> None:
    def ok_loader(**_: object):
        return [
            {
                "title": "Gameplay Engineer",
                "company": "Timing Studio",
                "city": "Remote",
                "country": "Remote",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": "https://example.com/timing/gameplay-engineer",
                "sector": "Game",
                "sourceJobId": "timing-1",
                "postedAt": "2026-03-01",
            }
        ]

    with workspace_tmpdir("jobs-fetcher-wall-clock") as tmp:
        report = jf.run_pipeline(
            output_dir=Path(tmp), source_loaders=[("timing_source", ok_loader)], show_progress=False
        )
        timing = ((report.get("runtime") or {}).get("timingSummary")) or {}
        assert int(timing.get("wallClockDurationMs") or 0) >= 0
