from __future__ import annotations

from types import SimpleNamespace

from src.source_discovery.directory_adapter_templates import (
    apply_directory_provenance,
    build_directory_static_candidate,
    build_known_directory_entry_candidate,
    empty_directory_scan_result,
    empty_scan_result_payload,
    run_directory_website_scan,
)


def test_build_directory_static_candidate_preserves_careers_url_shape() -> None:
    row = build_directory_static_candidate(
        studio="Example Studio",
        target_url="https://example.com/careers",
        nl_priority=True,
        website_only=False,
        name_suffix="Gameprog",
        discovery_method="gameprog",
        evidence_source="gameprog",
        evidence_types=["gameprog_directory"],
        source_directory="gameprog",
        source_directory_url="https://gameprog.it/",
        source_directory_entry_url="https://gameprog.it/studio/example",
        source_directory_location="Rome",
        careers_evidence_type="gameprog_careers_url",
        location_evidence_type="gameprog_location",
    )

    assert row["name"] == "Example Studio (Gameprog)"
    assert row["adapter"] == "static"
    assert row["listing_url"] == "https://example.com/careers"
    assert row["nlPriority"] is True
    assert row["enabledByDefault"] is False
    assert row["discoveryMethod"] == "gameprog"
    assert row["evidenceTypes"] == [
        "gameprog_directory",
        "gameprog_careers_url",
        "gameprog_location",
    ]
    assert row["sourceDirectory"] == "gameprog"
    assert row["sourceDirectoryLocation"] == "Rome"


def test_build_directory_static_candidate_preserves_website_only_shape() -> None:
    row = build_directory_static_candidate(
        studio="Example Studio",
        target_url="https://example.com/",
        nl_priority=False,
        website_only=True,
        name_suffix="Gamesmap",
        discovery_method="gamesmap",
        evidence_source="gamesmap",
        evidence_types=["gamesmap_directory", "gamesmap_category_match"],
        source_directory="gamesmap",
        source_directory_url="https://www.gamesmap.de/",
        source_directory_entry_url="https://www.gamesmap.de/en/company/example",
        source_directory_location="Hamburg",
        source_directory_categories=["Developer", "Developer"],
        manual_only=True,
        website_evidence_types=["gamesmap_website", "gamesmap_website_only"],
        location_evidence_type="gamesmap_location",
    )

    assert row["name"] == "Example Studio (Gamesmap)"
    assert row["adapter"] == "static"
    assert row["careersUrl"] == ""
    assert row["weakSignal"] is True
    assert row["manualOnly"] is True
    assert row["evidenceScore"] == 24
    assert row["evidenceTypes"] == [
        "gamesmap_directory",
        "gamesmap_category_match",
        "gamesmap_website",
        "gamesmap_website_only",
        "gamesmap_location",
    ]
    assert row["sourceDirectoryCategories"] == ["Developer"]


def test_apply_directory_provenance_preserves_candidate_and_adds_metadata() -> None:
    row = apply_directory_provenance(
        {
            "studio": "Example Studio",
            "adapter": "greenhouse",
            "careersUrl": "",
            "evidenceTypes": ["greenhouse"],
            "evidenceScore": 30,
        },
        evidence_source="gamesmap",
        evidence_types=["gamesmap_directory", "gamesmap_website_fetch"],
        source_directory="gamesmap",
        source_directory_url="https://www.gamesmap.de/",
        source_directory_entry_url="https://www.gamesmap.de/en/company/example",
        source_directory_location="Hamburg",
        source_directory_categories=["Developer"],
        careers_url_fallback="https://example.com/",
        evidence_score_floor=44,
    )

    assert row["adapter"] == "greenhouse"
    assert row["evidenceSource"] == "gamesmap"
    assert row["evidenceTypes"] == [
        "greenhouse",
        "gamesmap_directory",
        "gamesmap_website_fetch",
    ]
    assert row["evidenceScore"] == 44
    assert row["careersUrl"] == "https://example.com/"
    assert row["sourceDirectoryCategories"] == ["Developer"]


def test_build_known_directory_entry_candidate_prefers_provider_and_adds_metadata() -> None:
    def infer_provider(_url, studio, *, nl_priority, discovery_method):
        return {
            "name": studio,
            "studio": studio,
            "adapter": "greenhouse",
            "nlPriority": nl_priority,
            "discoveryMethod": discovery_method,
            "evidenceTypes": ["provider_url"],
            "evidenceScore": 28,
        }

    row = build_known_directory_entry_candidate(
        target_url="https://boards.greenhouse.io/example",
        studio="Example Studio",
        nl_priority=False,
        discovery_method="sheet_directory",
        discovery_stage="sheet_directory",
        evidence_source="game_studios_sheet",
        evidence_types=["sheet_directory", "sheet_row"],
        evidence_score=46,
        name_suffix="Sheet",
        enabled_by_default=None,
        weak_signal=False,
        extra_fields={
            "sourceDirectory": "game_studios_sheet",
            "sourceDirectoryEntryUrl": "https://boards.greenhouse.io/example",
        },
        infer_provider=infer_provider,
    )

    assert row["adapter"] == "greenhouse"
    assert row["discoveryStage"] == "sheet_directory"
    assert row["evidenceTypes"] == ["provider_url", "sheet_directory", "sheet_row"]
    assert row["evidenceScore"] == 46
    assert row["careersUrl"] == "https://boards.greenhouse.io/example"
    assert row["sourceDirectory"] == "game_studios_sheet"


def test_build_known_directory_entry_candidate_falls_back_to_static_shape() -> None:
    row = build_known_directory_entry_candidate(
        target_url="https://example.com/careers",
        studio="Example Studio",
        nl_priority=False,
        discovery_method="sheet_directory",
        discovery_stage="sheet_directory",
        evidence_source="game_studios_sheet",
        evidence_types=["sheet_directory", "sheet_roles_open_speculative"],
        evidence_score=18,
        name_suffix="Sheet",
        enabled_by_default=None,
        weak_signal=True,
        extra_fields={
            "sourceDirectory": "game_studios_sheet",
            "sourceDirectoryEntryUrl": "https://example.com/careers",
        },
        infer_provider=lambda *_args, **_kwargs: None,
    )

    assert row["name"] == "Example Studio (Sheet)"
    assert row["adapter"] == "static"
    assert "enabledByDefault" not in row
    assert row["weakSignal"] is True
    assert row["discoveryStage"] == "sheet_directory"
    assert row["sourceDirectory"] == "game_studios_sheet"


def test_empty_directory_scan_result_preserves_template_fields() -> None:
    failures = [{"adapter": "gameprog", "stage": "teams_fetch"}]
    summary = {"parsedRows": 0, "websiteFetchJobs": 0}
    batch_timing = {"teamsFetchMs": 2}

    row = empty_directory_scan_result(
        failures=failures,
        summary=summary,
        batch_timing=batch_timing,
        write_cache=False,
    )

    assert row == {
        "providerCandidates": [],
        "staticCandidates": [],
        "failures": failures,
        "summary": summary,
        "websiteFetchJobs": [],
        "browserRecoveryCandidates": [],
        "batchTiming": batch_timing,
        "writeCache": False,
    }


def test_empty_scan_result_payload_preserves_minimal_shape() -> None:
    failures = [{"adapter": "sheet_directory", "stage": "directory_index_fetch"}]
    batch_timing = {"csvFetchMs": 2}
    progress = {"complete": True, "cursor": 0, "completedUrlIdentities": []}

    row = empty_scan_result_payload(
        failures=failures,
        summary={"csvUrlAttempts": 3},
        progress=progress,
        batch_timing=batch_timing,
    )

    assert row == {
        "providerCandidates": [],
        "staticCandidates": [],
        "failures": failures,
        "summary": {"csvUrlAttempts": 3},
        "progress": progress,
        "batchTiming": batch_timing,
    }


def test_run_directory_website_scan_builds_fetch_jobs_and_merges_rows() -> None:
    captured_jobs: list[dict[str, object]] = []

    def fake_fetch_pages(_timeout_s, jobs, **_kwargs):
        captured_jobs.extend(jobs)
        return [
            {"url": "https://first.example.com", "payload": {"studio": "First"}},
            {"url": "https://second.example.com", "payload": {"studio": "Second"}},
        ]

    def analyze_result(result):
        if str(result["url"]) == "https://first.example.com":
            return {"providerCandidates": [{"adapter": "greenhouse", "studio": "First"}]}
        return {
            "failures": [{"adapter": "gameprog", "stage": "website_fetch"}],
            "fetchFailed": True,
        }

    batch_timing = {"parseMs": 1}
    row = run_directory_website_scan(
        5,
        entries=[
            {"studio": "First", "url": "https://first.example.com"},
            {"studio": "Second", "url": "https://second.example.com"},
        ],
        url_field="url",
        adapter="gameprog",
        failure_stage="website_fetch",
        fetcher=lambda *_args: "",
        fetch_pages=fake_fetch_pages,
        fetch_concurrency=3,
        per_host_concurrency=2,
        progress_label="Gameprog website fetch",
        analyze_result=analyze_result,
        enable_recovery=False,
        recovery_analyze_result=lambda _result, _request: {"providerCandidates": []},
        recovery_progress_label="Gameprog",
        unique_sources_fn=lambda rows: rows,
        batch_timing=batch_timing,
        summary={"parsedRows": 2, "eligibleRows": 2},
        progress_cursor=2,
        required_fields=("studio",),
    )

    assert [job["url"] for job in captured_jobs] == [
        "https://first.example.com",
        "https://second.example.com",
    ]
    assert all(job["adapter"] == "gameprog" for job in captured_jobs)
    assert row["providerCandidates"] == [{"adapter": "greenhouse", "studio": "First"}]
    assert row["failures"] == [{"adapter": "gameprog", "stage": "website_fetch"}]
    assert row["summary"]["websiteFetchJobs"] == 2
    assert row["summary"]["websiteFetchFailures"] == 1
    assert row["progress"]["completedUrlIdentities"] == [
        "https://first.example.com",
        "https://second.example.com",
    ]
    assert "websiteFetchMs" in batch_timing
    assert "candidateAnalysisMs" in batch_timing


def test_run_directory_website_scan_runs_recovery_and_skips_recovered_fallback() -> None:
    recovery_calls: list[list[object]] = []

    def fake_fetch_pages(_timeout_s, jobs, **_kwargs):
        return [{"url": job["url"], "payload": job["payload"]} for job in jobs]

    def analyze_result(_result):
        return {
            "recoveryRequests": [SimpleNamespace(key="recovered")],
            "fallbackStaticCandidates": [
                {"key": "recovered", "candidate": {"adapter": "static", "listing_url": "skip"}},
                {"key": "fallback", "candidate": {"adapter": "static", "listing_url": "keep"}},
            ],
            "badProviderInferences": 1,
        }

    def recovery_runner(_timeout_s, requests, **_kwargs):
        recovery_calls.append(list(requests))
        return SimpleNamespace(
            provider_candidates=[],
            static_candidates=[{"adapter": "static", "listing_url": "recovered"}],
            browser_recovery_candidates=[{"url": "https://example.com"}],
            recovered_keys={"recovered"},
            summary={
                "recoveryFetchAttempts": 1,
                "recoveryPagesFetched": 1,
                "recoveredProviderCandidates": 0,
                "recoveredStaticCandidates": 1,
                "recoveryFailures": 0,
                "browserRecoveryCandidates": 1,
            },
            batch_timing={"recoveryFetchMs": 4},
        )

    batch_timing: dict[str, object] = {}
    row = run_directory_website_scan(
        5,
        entries=[{"websiteUrl": "https://example.com", "studio": "Example"}],
        url_field="websiteUrl",
        adapter="gamesmap",
        failure_stage="website_fetch",
        fetcher=lambda *_args: "",
        fetch_pages=fake_fetch_pages,
        fetch_concurrency=3,
        per_host_concurrency=2,
        progress_label="Gamesmap website fetch",
        analyze_result=analyze_result,
        enable_recovery=True,
        recovery_analyze_result=lambda _result, _request: {"staticCandidates": []},
        recovery_progress_label="Gamesmap",
        unique_sources_fn=lambda rows: rows,
        batch_timing=batch_timing,
        summary={"eligibleRows": 1},
        progress_cursor=1,
        recovery_runner=recovery_runner,
    )

    assert len(recovery_calls) == 1
    assert row["staticCandidates"] == [
        {"adapter": "static", "listing_url": "recovered"},
        {"adapter": "static", "listing_url": "keep"},
    ]
    assert row["browserRecoveryCandidates"] == [{"url": "https://example.com"}]
    assert row["summary"]["recoveredStaticCandidates"] == 1
    assert row["summary"]["browserRecoveryCandidates"] == 1
    assert row["summary"]["badProviderInferences"] == 1
    assert batch_timing["recoveryFetchMs"] == 4
