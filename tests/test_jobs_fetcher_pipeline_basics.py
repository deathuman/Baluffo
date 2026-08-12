"""Tests for jobs fetcher pipeline basics."""

import json
import threading
from pathlib import Path

import src.jobs.text_utils as jobs_text_utils
from src import jobs_fetcher as jf
from src.jobs.pipeline_runtime_summary import PipelineTaskRuntime
from src.jobs.pipeline_runtime_writers import make_task_state_writer
from src.jobs.pipeline_source_results import _classify_report_outcome
from src.jobs.pipeline_stage_source_execution import _failure_bucket_from_zero_extract_context
from src.pipeline_io import write_pipeline_rows_sidecar
from src.shared.json_io import read_json
from tests.helpers.concurrency import BlockingActiveCounter
from tests.helpers.job_fixtures import _fixture_json
from tests.helpers.temp_paths import workspace_tmpdir


def _social_source_config() -> dict[str, object]:
    return {
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


def test_run_pipeline_social_sources_report_and_output() -> None:
    social_cfg = _social_source_config()

    def social_reddit_loader(**kwargs):
        return jf.run_social_reddit_source(**kwargs, social_config=social_cfg)

    def social_mastodon_loader(**kwargs):
        return jf.run_social_mastodon_source(**kwargs, social_config=social_cfg)

    reddit_payload = _fixture_json("payloads/social_reddit_listing.json")
    mastodon_payload = _fixture_json("payloads/social_mastodon_timeline.json")

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
        source_families = {row["name"]: row for row in (report.get("sourceFamilies") or [])}
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
        rows = read_json(Path(tmp) / "jobs-unified.json", [])
        assert any(str(row.get("source") or "").startswith("social_") for row in rows)


def test_run_pipeline_passes_max_workers_to_scrapy_static_loader() -> None:
    seen: dict[str, object] = {}

    def scrapy_loader(**kwargs):  # noqa: ANN001, ANN202
        seen["max_workers"] = kwargs.get("max_workers")
        return []

    with workspace_tmpdir("jobs-fetcher-scrapy-static-max-workers") as tmp:
        report = jf.run_pipeline(
            output_dir=Path(tmp),
            source_loaders=[("scrapy_static_sources", scrapy_loader)],
            max_workers=3,
            max_per_domain=1,
        )

    assert seen["max_workers"] == 3
    assert str((report.get("sources") or [{}])[0].get("name") or "") == "scrapy_static_sources"


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
    write_paths: list[str] = []
    write_guard = threading.Lock()
    writes = BlockingActiveCounter()

    def normalize_task_state_payload(payload, **_kwargs):
        return payload

    def fake_write_text_if_changed(path, _text):
        with write_guard:
            write_paths.append(Path(path).name)
        writes.enter()
        try:
            writes.wait_released()
        finally:
            writes.exit()
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
    writes.wait_until_peak(1)
    writes.release()
    for thread in threads:
        thread.join(timeout=2)

    assert all(not thread.is_alive() for thread in threads)
    assert len(write_paths) == 12
    assert write_paths.count("jobs-fetch-tasks.json") == 6
    assert write_paths.count("jobs-fetch-report-summary.json") == 6
    assert writes.peak == 1


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


class _ReportClassificationRoot:
    _failure_bucket_from_zero_extract_context = staticmethod(
        _failure_bucket_from_zero_extract_context
    )


def test_classify_report_outcome_marks_canonical_drop_all_as_needs_review() -> None:
    report = {
        "name": "personio_sources",
        "status": "ok",
        "adapter": "personio",
        "fetchedCount": 27,
        "keptCount": 0,
        "error": "",
        "loss": {
            "rawFetched": 27,
            "canonicalDropped": 27,
            "canonicalKept": 0,
            "canonicalDropReasons": {"missing_job_link": 27},
        },
    }

    _classify_report_outcome(report=report, root_module=_ReportClassificationRoot())

    assert report["failureBucket"] == "needs_review"
    assert report["zeroKeptClassification"] == "needs_review"


def test_classify_report_outcome_accepts_explicit_empty_evidence() -> None:
    report = {
        "name": "static_source::empty",
        "status": "ok",
        "adapter": "static",
        "classification": "empty_confirmed",
        "emptyConfirmed": True,
        "extractorHint": "explicit_no_openings_marker",
        "fetchedCount": 0,
        "keptCount": 0,
        "error": "",
    }

    _classify_report_outcome(report=report, root_module=_ReportClassificationRoot())

    assert report["failureBucket"] == "no_openings"
    assert report["zeroKeptClassification"] == "legit_empty"


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

        output = read_json(Path(tmp) / "jobs-unified.json", [])
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
        write_pipeline_rows_sidecar(out / "jobs-unified.json", existing)
        report = jf.run_pipeline(output_dir=out, source_loaders=[("empty", empty_loader)])

        output = read_json(out / "jobs-unified.json", [])
        assert len(output) == 1
        assert int(report["summary"].get("outputCount") or 0) == 1


def test_pipeline_reads_previous_output_in_packaged_layout_with_shared_contract_fallback(
    monkeypatch,
) -> None:
    existing = [
        {
            "id": 1,
            "title": "Engine Programmer",
            "company": "Archive Studio",
            "city": "",
            "country": "",
            "locationSummary": "Content & Editorial",
            "locations": ["Content & Editorial"],
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
        ship_root = Path(tmp) / "ship"
        output_dir = ship_root / "data"
        shared_contract_dir = output_dir / "contracts"
        shared_contract_dir.mkdir(parents=True, exist_ok=True)
        (shared_contract_dir / jobs_text_utils.CITY_NOISE_CONTRACT_NAME).write_text(
            json.dumps(
                {
                    "version": 1,
                    "knownJunkTokens": ["Content & Editorial"],
                }
            ),
            encoding="utf-8",
        )
        (shared_contract_dir / jobs_text_utils.COUNTRY_ACCEPTANCE_CONTRACT_NAME).write_text(
            json.dumps(
                {
                    "version": 1,
                    "acceptedExactLabels": [],
                    "normalizeAliasesToValue": {},
                }
            ),
            encoding="utf-8",
        )
        versioned_module_path = (
            ship_root / "app" / "versions" / "1.2.3" / "src" / "jobs" / "text_utils.py"
        )
        versioned_module_path.parent.mkdir(parents=True, exist_ok=True)
        versioned_module_path.write_text("# test stub\n", encoding="utf-8")
        (output_dir / "jobs-unified.json").write_text(json.dumps(existing), encoding="utf-8")
        write_pipeline_rows_sidecar(output_dir / "jobs-unified.json", existing)

        monkeypatch.setattr(jobs_text_utils, "__file__", str(versioned_module_path))
        jobs_text_utils.load_city_noise_contract.cache_clear()
        jobs_text_utils.load_country_acceptance_contract.cache_clear()
        try:
            report = jf.run_pipeline(
                output_dir=output_dir, source_loaders=[("empty", empty_loader)]
            )
        finally:
            jobs_text_utils.load_city_noise_contract.cache_clear()
            jobs_text_utils.load_country_acceptance_contract.cache_clear()

        output = read_json(output_dir / "jobs-unified.json", [])
        assert len(output) == 1
        assert output[0]["city"] == ""
        assert output[0]["country"] == "Unknown"
        assert output[0]["locations"] == []
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

            lifecycle_payload = read_json(out / "jobs-lifecycle-state.json", {})
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

            lifecycle_payload = read_json(out / "jobs-lifecycle-state.json", {})
            jobs_map = lifecycle_payload.get("jobs") or {}
            assert len(jobs_map) == 1
            entry = list(jobs_map.values())[0]
            assert str(entry.get("status") or "") == "likely_removed"
            assert str(entry.get("removedAt") or "")
    finally:
        jf.default_source_loaders = previous_default_loaders
