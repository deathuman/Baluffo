"""Tests for source discovery probe persistence behavior."""

# ruff: noqa: F401
from typing import Any

from src.url_hosts import url_host
from tests.helpers.mutation import append_and_return

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


def test_run_discovery_refreshes_url_patches_and_reprobes_candidate() -> None:
    with workspace_tmpdir("source-discovery") as root:
        with override_discovery_runtime(
            root,
            studio_seeds=[],
            static_candidates=[
                {
                    "name": "Recoverable Static",
                    "studio": "Recoverable Static",
                    "adapter": "static",
                    "listing_url": "https://old.example/jobs",
                    "pages": ["https://old.example/jobs"],
                    "evidenceScore": 52,
                    "evidenceTypes": ["seed_curated"],
                }
            ],
        ) as paths:

            def fake_fetch(url: str, _timeout: int) -> str:
                if url == "https://old.example/jobs":
                    raise RuntimeError(
                        "Client error '404 Not Found' for url 'https://old.example/jobs'"
                    )
                if url == "https://new.example/jobs":
                    return '<a href="https://new.example/jobs/role-1">Role</a>'
                raise RuntimeError(f"unexpected URL: {url}")

            with mock.patch.object(
                discovery_orchestrator,
                "resolve_patch_target",
                return_value="https://new.example/jobs",
            ):
                report = sd.run_discovery(
                    timeout_s=5,
                    top_n=0,
                    mode="dynamic",
                    include_web_search=False,
                    discovery_config=GENERATOR_DISABLED_DISCOVERY_CONFIG,
                    fetcher=fake_fetch,
                )

            manifest = json.loads(paths.url_patch_manifest_path.read_text(encoding="utf-8"))
            assert manifest["patches"]["https://old.example/jobs"] == "https://new.example/jobs"
            assert report["summary"]["queuedCandidateCount"] == 1
            assert report["summary"]["failedProbeCount"] == 0
            assert report["runtime"]["urlPatchStats"]["added"] == 1
            assert report["runtime"]["urlPatchStats"]["reprobed"] == 1
            assert report["runtime"]["urlPatchRecoveredCount"] == 1


def test_run_discovery_sheet_directory_candidates_flow_into_queue() -> None:
    with workspace_tmpdir("source-discovery") as root:
        with override_discovery_runtime(
            root,
            studio_seeds=[],
            static_candidates=[],
            extra_config_overrides={
                "GAME_STUDIOS_SHEET_ID": "sheet_test",
                "GAME_STUDIOS_SHEET_GID": "1",
            },
        ) as paths:
            sheet_url = sd.game_studios_sheet_candidate_urls(
                discovery_config_module.GAME_STUDIOS_SHEET_ID,
                discovery_config_module.GAME_STUDIOS_SHEET_GID,
            )[0]
            csv_text = """x,x,x,x
x,Studio,Hiring Location,Roles open,Link
x,Example Studio,Remote,yes,https://boards.greenhouse.io/examplestudio
"""

            payloads = {
                sheet_url: csv_text,
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
                discovery_config=GENERATOR_DISABLED_DISCOVERY_CONFIG,
                fetcher=fake_fetch,
            )
            assert int(report["summary"].get("queuedCandidateCount") or 0) == 1
            assert (
                int(
                    (report["summary"].get("generatedCountByStage") or {}).get("sheet_directory")
                    or 0
                )
                >= 1
            )
            queued = json.loads(paths.discovery_candidates_path.read_text(encoding="utf-8"))
            assert len(queued) == 1
            assert str(queued[0].get("discoveryMethod") or "") == "sheet_directory"
            assert str(queued[0].get("sourceDirectory") or "") == "game_studios_sheet"
            assert str(queued[0].get("adapter") or "") == "greenhouse"
            runtime = report.get("runtime") or {}
            assert int(runtime.get("totalDurationMs") or 0) >= 0
            assert "stageTimingsMs" in runtime
            assert "adapterTimings" in runtime
            assert any(
                str(row.get("adapter") or "") == "greenhouse"
                for row in (runtime.get("adapterTimings") or [])
            )


def test_run_discovery_suppresses_blocked_static_domains_before_probe() -> None:
    with workspace_tmpdir("source-discovery") as root:
        with override_discovery_runtime(
            root,
            studio_seeds=[],
            static_candidates=[
                {
                    "name": "Blocked Static",
                    "studio": "Blocked Static",
                    "adapter": "static",
                    "listing_url": "https://www.linkedin.com/company/example/jobs/",
                    "pages": ["https://www.linkedin.com/company/example/jobs/"],
                    "evidenceScore": 52,
                    "evidenceTypes": ["seed_curated"],
                }
            ],
        ):
            calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
            report = sd.run_discovery(
                timeout_s=5,
                top_n=0,
                mode="dynamic",
                include_web_search=False,
                discovery_config=GENERATOR_DISABLED_DISCOVERY_CONFIG,
                fetcher=lambda *args, **kwargs: append_and_return(calls, (args, kwargs), ""),
            )
            assert not any(
                args and args[0] == "https://www.linkedin.com/company/example/jobs/"
                for args, _kwargs in calls
            )
            assert report["summary"]["suppressedStaticCount"] == 1
            assert report["summary"]["failedProbeCount"] == 0
            assert any(
                str(row.get("dropReason") or "") == "blocked_domain"
                for row in (report.get("failures") or [])
            )


def test_run_discovery_tracks_probe_miss_separately_from_failures() -> None:
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
                }
            ],
        ):
            report = sd.run_discovery(
                timeout_s=5,
                top_n=0,
                mode="dynamic",
                include_web_search=False,
                discovery_config=GENERATOR_DISABLED_DISCOVERY_CONFIG,
                fetcher=lambda *_a, **_k: (_ for _ in ()).throw(
                    RuntimeError("HTTP Error 404: Not Found")
                ),
            )
            assert int(report["summary"].get("probedCandidateCount") or 0) == 1
            assert int(report["summary"].get("failedProbeCount") or 0) == 0
            assert int(report["summary"].get("probeMissCount") or 0) == 1
            assert str((report.get("failures") or [])[0].get("stage") or "") == "probe_miss"


def test_run_discovery_uses_previous_deferred_review_history_in_ranking() -> None:
    with workspace_tmpdir("source-discovery") as root:
        with override_discovery_runtime(root, studio_seeds=[], static_candidates=[]) as paths:
            paths.discovery_candidates_path.write_text(
                json.dumps(
                    [
                        {
                            "id": "greenhouse:slug:demo-deferred",
                            "name": "Demo Deferred",
                            "studio": "Demo Deferred",
                            "adapter": "greenhouse",
                            "slug": "demo-deferred",
                            "api_url": "https://boards-api.greenhouse.io/v1/boards/demo-deferred/jobs?content=true",
                            "deferred": True,
                            "deferReason": "domain_cap",
                            "deferCount": 2,
                            "firstDeferredAt": "2026-03-20T00:00:00Z",
                            "lastDeferredAt": "2026-03-22T00:00:00Z",
                        }
                    ]
                ),
                encoding="utf-8",
            )
            payloads = {
                "https://boards-api.greenhouse.io/v1/boards/demo-deferred/jobs?content=true": json.dumps(
                    {"jobs": [{}, {}]}
                ),
            }

            def fake_fetch(url: str, _: int) -> str:
                if url not in payloads:
                    raise RuntimeError(f"unexpected URL: {url}")
                return payloads[url]

            with (
                mock.patch.object(
                    discovery_orchestrator, "stage_curated_seed_candidates", return_value=[]
                ),
                mock.patch.object(
                    discovery_orchestrator,
                    "run_sheet_directory_audit",
                    return_value=_directory_audit_result(
                        provider=[
                            {
                                "name": "Demo Deferred",
                                "studio": "Demo Deferred",
                                "adapter": "greenhouse",
                                "slug": "demo-deferred",
                                "api_url": "https://boards-api.greenhouse.io/v1/boards/demo-deferred/jobs?content=true",
                                "discoveryMethod": "sheet_directory",
                                "discoveryStage": "sheet_directory",
                                "evidenceScore": 46,
                                "evidenceTypes": ["sheet_directory"],
                            }
                        ]
                    ),
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
                mock.patch.object(discovery_orchestrator, "load_url_patches", return_value={}),
                mock.patch.object(
                    discovery_orchestrator, "save_url_patch_manifest", return_value=None
                ),
                mock.patch.object(discovery_orchestrator, "read_source_state", return_value={}),
            ):
                report = sd.run_discovery(
                    timeout_s=5,
                    top_n=0,
                    mode="dynamic",
                    include_web_search=False,
                    discovery_config=GENERATOR_DISABLED_DISCOVERY_CONFIG,
                    fetcher=fake_fetch,
                )

            row = report["candidates"][0]
            assert row["rankScore"] > row["score"]
            assert "deferred_backlog_age" in row["rankReasons"]


def test_run_discovery_uses_seed_careers_pages_without_web_search() -> None:
    with workspace_tmpdir("source-discovery") as root:
        with override_discovery_runtime(
            root,
            studio_seeds=[
                {
                    "studio": "Example Studio",
                    "aliases": ["example-studio"],
                    "nlPriority": False,
                    "likelyProviders": ["teamtailor"],
                    "careersUrl": "https://example.com/careers",
                }
            ],
            static_candidates=[],
        ) as paths:

            def fake_fetch(url: str, _: int) -> str:
                if url == "https://example.com/careers":
                    return '<a href="https://boards.greenhouse.io/example-studio/jobs/123">Job</a>'
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
            assert int(report["summary"].get("queuedCandidateCount") or 0) == 1
            assert (
                int((report["summary"].get("queuedCountByStage") or {}).get("web_provider") or 0)
                == 1
            )
            assert (
                int((report["summary"].get("generatedCountByStage") or {}).get("web_provider") or 0)
                == 1
            )
            assert (
                int(
                    (report["summary"].get("generatedCountByStage") or {}).get("generic_static")
                    or 0
                )
                == 0
            )
            queued = json.loads(paths.discovery_candidates_path.read_text(encoding="utf-8"))
            assert len(queued) == 1
            assert str(queued[0].get("discoveryMethod") or "") == "seed_careers_page"


def test_run_discovery_writes_m5_backlog_snapshot() -> None:
    with workspace_tmpdir("source-discovery") as root:
        prev_sheet = discovery_orchestrator.run_sheet_directory_audit
        prev_gamesmap = discovery_orchestrator.discover_gamesmap_candidates
        prev_gameprog = discovery_orchestrator.discover_gameprog_candidates
        prev_web_audit = discovery_orchestrator.run_web_search_directory_audit
        prev_probe = discovery_orchestrator.async_probe_candidate
        try:
            with override_discovery_runtime(
                root,
                studio_seeds=[],
                static_candidates=[
                    {
                        "name": "Asia Studio",
                        "studio": "Asia Studio",
                        "adapter": "static",
                        "listing_url": "https://asia.example/jobs",
                        "evidenceScore": 88,
                        "jobsFound": 4,
                        "hqRegion": "Asia",
                        "discoveryMethod": "seed",
                        "discoveryStage": "curated_seed",
                    }
                ],
                include_m5_backlog=True,
            ) as paths:

                async def fake_probe(
                    candidate, timeout_s, *, fetcher, try_playwright=None, playwright_semaphore=None
                ):
                    return True, 4, ""

                discovery_orchestrator.run_sheet_directory_audit = lambda *_a, **_k: (
                    _directory_audit_result()
                )
                discovery_orchestrator.discover_gamesmap_candidates = lambda *args, **kwargs: (
                    [],
                    [],
                    [],
                )
                discovery_orchestrator.discover_gameprog_candidates = lambda *args, **kwargs: (
                    [],
                    [],
                    [],
                )
                discovery_orchestrator.run_web_search_directory_audit = lambda *_a, **_k: (
                    _directory_audit_result()
                )
                discovery_orchestrator.async_probe_candidate = fake_probe

                report = discovery_orchestrator.run_discovery(
                    timeout_s=1,
                    top_n=0,
                    preset="uncapped",
                    mode="static",
                    include_web_search=False,
                    discovery_config=discovery_config_without_generator_stages(),
                    fetcher=lambda *args, **kwargs: "",
                )

                assert report["summary"]["queuedCandidateCount"] == 1
                assert paths.discovery_candidates_path.exists()
                assert paths.m5_strategic_backlog_path.exists()

                backlog = json.loads(paths.m5_strategic_backlog_path.read_text(encoding="utf-8"))
                assert len(backlog) == 1
                assert backlog[0]["candidateIdentityKey"] == sr.source_identity(
                    {
                        "name": "Asia Studio",
                        "studio": "Asia Studio",
                        "adapter": "static",
                        "listing_url": "https://asia.example/jobs",
                        "evidenceScore": 88,
                        "jobsFound": 4,
                        "hqRegion": "Asia",
                        "discoveryMethod": "seed",
                        "discoveryStage": "curated_seed",
                    }
                )
                assert backlog[0]["coverageLane"] == "lane_c_asia_custom"
                assert backlog[0]["ownerMilestone"] == "M5"
        finally:
            discovery_orchestrator.run_sheet_directory_audit = prev_sheet
            discovery_orchestrator.discover_gamesmap_candidates = prev_gamesmap
            discovery_orchestrator.discover_gameprog_candidates = prev_gameprog
            discovery_orchestrator.run_web_search_directory_audit = prev_web_audit
            discovery_orchestrator.async_probe_candidate = prev_probe


def test_run_discovery_writes_phase_progress_before_probe() -> None:
    with workspace_tmpdir("source-discovery") as root:
        saved_reports = []
        original_save_json_atomic = discovery_orchestrator.save_json_atomic

        def capture_save(path, payload):
            if Path(path) == paths.discovery_report_path and isinstance(payload, dict):
                saved_reports.append(payload)
            original_save_json_atomic(path, payload)

        with override_discovery_runtime(root, studio_seeds=[], static_candidates=[]) as paths:
            with mock.patch.object(
                discovery_orchestrator, "save_json_atomic", side_effect=capture_save
            ):
                report = sd.run_discovery(
                    timeout_s=5,
                    top_n=0,
                    mode="dynamic",
                    include_web_search=False,
                    discovery_config=GENERATOR_DISABLED_DISCOVERY_CONFIG,
                    fetcher=lambda *_: json.dumps([{"id": 1}]),
                )

            phase_labels = [
                str(((payload.get("taskProgress") or {}).get("phaseLabel")) or "")
                for payload in saved_reports
            ]
            assert "Generating seed candidates" in phase_labels
            assert "Scanning game studios sheet directory" in phase_labels
            assert "Generating provider-pattern candidates" in phase_labels
            assert "Scanning known careers pages" in phase_labels
            assert "Discovery completed" == str(
                (report.get("taskProgress") or {}).get("phaseLabel") or ""
            )
