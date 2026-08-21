"""Tests for source discovery runtime and generator behavior."""

# ruff: noqa: F401
from typing import Any

from src.url_hosts import url_host

from ._helpers import (
    FIXTURES_DIR,
    GENERATOR_DISABLED_DISCOVERY_CONFIG,
    DiscoveryReportSummarySchema,
    Path,
    _directory_audit_result,
    _fixture_json,
    _fixture_text,
    _gamesmap_next_payload_html,
    discovery_config_module,
    discovery_config_without_generator_stages,
    discovery_orchestrator,
    discovery_url_patches,
    json,
    mock,
    override_discovery_config,
    override_discovery_runtime,
    patch_empty_generator_stages,
    sd,
    sr,
    workspace_tmpdir,
)


def test_run_discovery_default_and_uncapped_report_runtime_cap_bypass_flags() -> None:
    dynamic_candidates = [
        {
            "name": f"Sheet Static {index}",
            "studio": "Sheet Static",
            "adapter": "static",
            "score": 90 - index,
            "evidenceScore": 80,
            "pages": [f"https://sheet.example/jobs/{index}"],
            "careersUrl": f"https://sheet.example/jobs/{index}",
            "sourceDirectoryEntryUrl": f"https://sheet.example/jobs/{index}",
            "discoveryStage": "sheet_directory",
            "sourceDirectory": "game_studios_sheet",
            "discoveryMethod": "static",
            "evidenceTypes": ["sheet_directory"],
        }
        for index in range(12)
    ]

    def fake_probe(row, timeout_s, fetcher=None, try_playwright=None, playwright_semaphore=None):
        return True, 2, ""

    config = discovery_config_without_generator_stages()

    def run_preset(preset: str) -> dict:
        with workspace_tmpdir(f"source-discovery-{preset}") as tmp:
            root = Path(tmp)
            with override_discovery_runtime(root) as paths:
                for path in (paths.active_path, paths.pending_path, paths.rejected_path):
                    path.write_text("[]", encoding="utf-8")
                with (
                    mock.patch.object(
                        discovery_orchestrator,
                        "run_sheet_directory_audit",
                        return_value=_directory_audit_result(static=list(dynamic_candidates)),
                    ),
                    mock.patch.object(
                        discovery_orchestrator, "stage_curated_seed_candidates", return_value=[]
                    ),
                    mock.patch.object(
                        discovery_orchestrator, "build_pattern_candidates", return_value=[]
                    ),
                    mock.patch.object(
                        discovery_orchestrator,
                        "run_web_search_directory_audit",
                        return_value=_directory_audit_result(),
                    ),
                    mock.patch.object(
                        discovery_orchestrator,
                        "discover_gamesmap_candidates",
                        return_value=([], [], []),
                    ),
                    mock.patch.object(
                        discovery_orchestrator,
                        "discover_gameprog_candidates",
                        return_value=([], [], []),
                    ),
                    mock.patch.object(
                        discovery_orchestrator, "async_probe_candidate", side_effect=fake_probe
                    ),
                    mock.patch.object(discovery_orchestrator, "load_url_patches", return_value={}),
                    mock.patch.object(
                        discovery_orchestrator, "save_url_patch_manifest", return_value=None
                    ),
                    mock.patch.object(discovery_orchestrator, "read_source_state", return_value={}),
                ):
                    return discovery_orchestrator.run_discovery(
                        timeout_s=1,
                        top_n=0,
                        preset=preset,
                        mode="dynamic",
                        include_web_search=False,
                        discovery_config=config,
                    )

    default_report = run_preset("default")
    uncapped_report = run_preset("uncapped")

    default_runtime = default_report.get("runtime") or {}
    default_summary = default_report.get("summary") or {}
    uncapped_runtime = uncapped_report.get("runtime") or {}
    uncapped_summary = uncapped_report.get("summary") or {}

    assert str(default_runtime.get("preset") or "") == "default"
    assert bool(default_runtime.get("topCapBypassed")) is True
    assert bool(default_runtime.get("sheetStaticProbeCapBypassed")) is True
    assert int(default_summary.get("queuedCandidateCount") or 0) == 2
    assert int(default_summary.get("discoverableButDeferredCount") or 0) == 10
    assert int((default_summary.get("deferredReasons") or {}).get("domain_cap") or 0) == 10
    assert int(default_summary.get("suppressedStaticCount") or 0) == 0
    assert all(
        str(entry.get("key") or "") != "static" for entry in default_report.get("topFailures") or []
    )
    assert int((default_report.get("suppressionSummary") or {}).get("dedupeSkippedCount") or 0) == 0
    assert (
        int((default_report.get("suppressionSummary") or {}).get("suppressedStaticCount") or 0) == 0
    )

    assert str(uncapped_runtime.get("preset") or "") == "uncapped"
    assert bool(uncapped_runtime.get("topCapBypassed")) is True
    assert bool(uncapped_runtime.get("sheetStaticProbeCapBypassed")) is True
    assert int(uncapped_summary.get("queuedCandidateCount") or 0) == 8
    assert int(uncapped_summary.get("discoverableButDeferredCount") or 0) == 4
    assert int((uncapped_summary.get("deferredReasons") or {}).get("domain_cap") or 0) == 4
    assert int(uncapped_summary.get("suppressedStaticCount") or 0) == 0
    assert all(
        str(entry.get("key") or "") != "static"
        for entry in uncapped_report.get("topFailures") or []
    )
    assert (
        int((uncapped_report.get("suppressionSummary") or {}).get("dedupeSkippedCount") or 0) == 0
    )
    assert (
        int((uncapped_report.get("suppressionSummary") or {}).get("suppressedStaticCount") or 0)
        == 0
    )


def test_run_discovery_deduplicates_duplicate_endpoints_and_stale_pending_rows() -> None:
    cases: list[dict[str, Any]] = [
        {
            "name": "duplicate endpoint fingerprints",
            "kind": "run_discovery",
            "setup": {
                "static": [
                    {
                        "name": "Demo Lever A",
                        "studio": "Demo",
                        "adapter": "lever",
                        "account": "demo",
                        "api_url": "https://api.lever.co/v0/postings/demo?mode=json",
                    },
                    {
                        "name": "Demo Lever A Duplicate",
                        "studio": "Demo",
                        "adapter": "lever",
                        "account": "demo2",
                        "api_url": "https://api.lever.co/v0/postings/demo?mode=json",
                        "discoveryMethod": "pattern",
                    },
                ],
                "fetcher": lambda *_: json.dumps([{"id": 1}]),
            },
            "expected_queued": 1,
            "expected_skipped": 1,
            "expected_duplicate_reasons": True,
        },
        {
            "name": "stale pending duplicate",
            "kind": "unique_sources",
            "rows": [
                {
                    "name": "Fresh Board",
                    "studio": "Fresh Board",
                    "adapter": "greenhouse",
                    "slug": "fresh-board",
                    "jobsFound": 3,
                    "sampleCount": 3,
                    "lastProbedAt": "2026-03-23T00:00:00Z",
                },
                {
                    "name": "Fresh Board",
                    "studio": "Fresh Board",
                    "adapter": "greenhouse",
                    "slug": "fresh-board",
                    "jobsFound": 0,
                    "sampleCount": 0,
                    "lastProbedAt": "2026-03-20T00:00:00Z",
                },
            ],
            "expected_len": 1,
            "expected_id": "greenhouse:slug:fresh-board",
            "expected_jobs_found": 3,
            "expected_sample_count": 3,
        },
    ]

    for case in cases:
        if case["kind"] == "run_discovery":
            with workspace_tmpdir("source-discovery") as root:
                with override_discovery_runtime(
                    root,
                    studio_seeds=[],
                    static_candidates=case["setup"]["static"],
                ):
                    report = sd.run_discovery(
                        timeout_s=5,
                        top_n=0,
                        mode="dynamic",
                        include_web_search=False,
                        fetcher=case["setup"]["fetcher"],
                    )
                    assert (
                        int(report["summary"].get("queuedCandidateCount") or 0)
                        == case["expected_queued"]
                    ), case["name"]
                    assert (
                        int(report["summary"].get("skippedDuplicateCount") or 0)
                        == case["expected_skipped"]
                    ), case["name"]
                    assert ("duplicateReasons" in report["summary"]) == case[
                        "expected_duplicate_reasons"
                    ], case["name"]
        else:
            merged = sr.unique_sources(case["rows"])
            assert len(merged) == case["expected_len"], case["name"]
            assert merged[0]["id"] == case["expected_id"], case["name"]
            assert int(merged[0]["jobsFound"] or 0) == case["expected_jobs_found"], case["name"]
            assert int(merged[0]["sampleCount"] or 0) == case["expected_sample_count"], case["name"]


def test_run_discovery_dynamic_tracks_stage_metrics_and_queue_contract() -> None:
    with workspace_tmpdir("source-discovery") as root:
        with override_discovery_runtime(
            root,
            studio_seeds=[],
            static_candidates=[
                {
                    "name": "Demo Lever",
                    "studio": "Demo",
                    "adapter": "lever",
                    "account": "demo",
                    "api_url": "https://api.lever.co/v0/postings/demo?mode=json",
                    "nlPriority": True,
                },
                {
                    "name": "Demo Greenhouse",
                    "studio": "Demo",
                    "adapter": "greenhouse",
                    "slug": "demo",
                    "api_url": "https://boards-api.greenhouse.io/v1/boards/demo/jobs?content=true",
                    "nlPriority": True,
                },
            ],
        ) as paths:

            def fake_fetch(url: str, _: int) -> str:
                if url_host(url) == "api.lever.co":
                    return json.dumps([{"id": 1}, {"id": 2}, {"id": 3}])
                if url_host(url) == "boards-api.greenhouse.io":
                    return json.dumps({"jobs": [{}, {}]})
                raise RuntimeError(f"unexpected URL: {url}")

            report = sd.run_discovery(
                timeout_s=5,
                top_n=0,
                mode="dynamic",
                include_web_search=False,
                discovery_config=GENERATOR_DISABLED_DISCOVERY_CONFIG,
                fetcher=fake_fetch,
            )
            summary = report["summary"]
            assert int(summary.get("foundEndpointCount") or 0) == 2
            assert int(summary.get("probedCandidateCount") or 0) == 2
            assert int(summary.get("queuedCandidateCount") or 0) == 2
            assert "generatedCountByStage" in summary
            assert "queuedCountByStage" in summary
            assert "lossAccounting" in summary
            assert int((summary.get("lossAccounting") or {}).get("generated") or 0) == 2
            assert int((summary.get("lossAccounting") or {}).get("queued") or 0) == 2
            assert int((summary.get("queuedCountByStage") or {}).get("curated_seed") or 0) == 2

            queued = json.loads(paths.discovery_candidates_path.read_text(encoding="utf-8"))
            assert len(queued) == 2
            for row in queued:
                assert "evidenceScore" in row
                assert "evidenceTypes" in row
                assert "discoveryStage" in row
                assert not (bool(row.get("deferred")))


def test_run_discovery_emits_phase_logs_for_candidate_generation() -> None:
    with workspace_tmpdir("source-discovery") as root:
        with override_discovery_runtime(root, studio_seeds=[], static_candidates=[]):
            with mock.patch.object(discovery_orchestrator, "emit_log") as emit_log_mock:
                report = sd.run_discovery(
                    timeout_s=5,
                    top_n=0,
                    mode="dynamic",
                    include_web_search=False,
                    discovery_config=GENERATOR_DISABLED_DISCOVERY_CONFIG,
                    fetcher=lambda *_: "",
                )

            messages = [str(call.args[0]) for call in emit_log_mock.call_args_list if call.args]
            assert any("Generating curated seed candidates" in message for message in messages)
            assert any("Generating provider-pattern candidates" in message for message in messages)
            assert any("Scanning known careers pages" in message for message in messages)
            assert any("Starting probe phase" in message for message in messages)
            assert str((report.get("summary") or {}).get("phase") or "") == "completed"


def test_run_discovery_gamesmap_candidates_flow_into_report_and_queue() -> None:
    with workspace_tmpdir("source-discovery") as root:
        with override_discovery_runtime(root, studio_seeds=[], static_candidates=[]) as paths:
            config = {
                "gamesmap": {
                    "enabled": True,
                    "activeAuditPath": str(root / "gamesmap-audit.json"),
                    "baseUrl": "https://www.gamesmap.de",
                    "indexUrls": ["https://www.gamesmap.de/en"],
                    "websiteOnlyFallback": False,
                    "maxDetailPages": 10,
                    "allowedCategoryTokens": ["developer", "publisher", "pc", "console"],
                    "blockedCategoryTokens": ["association", "education"],
                },
                "gameprog": {"enabled": False},
                "gamedevmap": {"enabled": False},
            }
            payloads = {
                "https://www.gamesmap.de/en": _fixture_text("gamesmap_index_next_payload.html"),
                "https://boards-api.greenhouse.io/v1/boards/examplestudio/jobs?content=true": json.dumps(
                    {"jobs": [{}, {}]}
                ),
            }

            def fake_fetch(url: str, _: int) -> str:
                if url not in payloads:
                    raise RuntimeError(f"unexpected URL: {url}")
                return payloads[url]

            report = sd.run_discovery(
                timeout_s=5,
                top_n=0,
                mode="dynamic",
                include_web_search=False,
                discovery_config=config,
                fetcher=fake_fetch,
            )
            assert int(report["summary"].get("queuedCandidateCount") or 0) == 1
            assert (
                int((report["summary"].get("generatedCountByStage") or {}).get("web_provider") or 0)
                == 1
            )
            queued = json.loads(paths.discovery_candidates_path.read_text(encoding="utf-8"))
            assert len(queued) == 1
            assert str(queued[0].get("discoveryMethod") or "") == "gamesmap"
            assert str(queued[0].get("sourceDirectory") or "") == "gamesmap"


def test_run_discovery_gamedevmap_candidates_flow_into_report_and_queue() -> None:
    with workspace_tmpdir("source-discovery-gamedevmap") as root:
        with override_discovery_runtime(root, studio_seeds=[], static_candidates=[]) as paths:
            config = discovery_config_without_generator_stages(
                gamedevmap={
                    "enabled": True,
                    "csvUrl": "https://www.gamedevmap.com/cmsdata/gamedevmapdata.csv",
                    "indexUrl": "https://www.gamedevmap.com/index.php",
                    "allowedCategories": ["Developer"],
                    "blockedCategories": ["Organization"],
                    "maxRows": 0,
                    "maxHomepageFetches": 0,
                },
            )
            payloads = {
                "https://www.gamedevmap.com/cmsdata/gamedevmapdata.csv": _fixture_text(
                    "gamedevmap_data.csv"
                ),
                "https://boards-api.greenhouse.io/v1/boards/providerfeedstudio/jobs?content=true": json.dumps(
                    {
                        "jobs": [
                            {
                                "id": 1,
                                "title": "Gameplay Engineer",
                                "absolute_url": "https://job-boards.greenhouse.io/providerfeedstudio/jobs/1",
                            },
                            {
                                "id": 2,
                                "title": "Technical Artist",
                                "absolute_url": "https://job-boards.greenhouse.io/providerfeedstudio/jobs/2",
                            },
                        ]
                    }
                ),
            }

            def fake_fetch(url: str, _: int) -> str:
                if url not in payloads:
                    raise RuntimeError(f"unexpected URL: {url}")
                return payloads[url]

            report = sd.run_discovery(
                timeout_s=5,
                top_n=0,
                mode="dynamic",
                include_web_search=False,
                discovery_config=config,
                fetcher=fake_fetch,
            )
            assert int(report["summary"].get("queuedCandidateCount") or 0) == 1
            assert (
                int((report["summary"].get("generatedCountByStage") or {}).get("web_provider") or 0)
                == 1
            )
            queued = json.loads(paths.discovery_candidates_path.read_text(encoding="utf-8"))
            assert len(queued) == 1
            assert str(queued[0].get("discoveryMethod") or "") == "gamedevmap"
            assert str(queued[0].get("sourceDirectory") or "") == "gamedevmap"
