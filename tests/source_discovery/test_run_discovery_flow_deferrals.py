"""Tests for source discovery deferral and ranking behavior."""

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


def test_run_discovery_does_not_auto_approve_weak_pending_only_rows() -> None:
    with workspace_tmpdir("source-discovery-auto-approval") as root:
        prev_approval_state_path = discovery_orchestrator.DEFAULT_APPROVAL_STATE_PATH
        prev_sheet = discovery_orchestrator.run_sheet_directory_audit
        prev_gamesmap = discovery_orchestrator.discover_gamesmap_candidates
        prev_gameprog = discovery_orchestrator.discover_gameprog_candidates
        prev_web_audit = discovery_orchestrator.run_web_search_directory_audit
        prev_probe = discovery_orchestrator.async_probe_candidate
        try:
            with override_discovery_runtime(
                root,
                studio_seeds=[],
                static_candidates=[],
                include_m5_backlog=True,
            ) as paths:
                discovery_orchestrator.DEFAULT_APPROVAL_STATE_PATH = (
                    root / "source-approval-state.json"
                )
                sr.save_json_atomic(paths.active_path, [])
                sr.save_json_atomic(
                    paths.pending_path,
                    [
                        {
                            "id": "pending-ok",
                            "adapter": "static",
                            "name": "Healthy Pending",
                            "jobsFound": 3,
                            "weakSignal": True,
                            "status": "healthy",
                        }
                    ],
                )
                sr.save_json_atomic(paths.rejected_path, [])

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

                async def _fake_probe_candidate(*args: Any, **kwargs: Any) -> tuple[bool, int, str]:
                    return False, 0, ""

                discovery_orchestrator.async_probe_candidate = _fake_probe_candidate

                report = discovery_orchestrator.run_discovery(
                    timeout_s=1,
                    top_n=0,
                    preset="uncapped",
                    mode="dynamic",
                    include_web_search=False,
                    discovery_config=discovery_config_without_generator_stages(
                        autoApproveHealthyPendingOnComplete=True
                    ),
                    fetcher=lambda *args, **kwargs: "",
                )

                assert int((report.get("summary") or {}).get("approvedCandidateCount") or 0) == 0
                assert int((report.get("summary") or {}).get("liveCandidateCount") or 0) == 0
                assert (
                    int(
                        (
                            ((report.get("runtime") or {}).get("autoApproval") or {}).get(
                                "approvedCount"
                            )
                        )
                        or 0
                    )
                    == 0
                )
                active = json.loads(paths.active_path.read_text(encoding="utf-8"))
                pending = json.loads(paths.pending_path.read_text(encoding="utf-8"))
                assert active == []
                assert [row["id"] for row in pending] == ["pending-ok"]
                assert not (root / "source-approval-state.json").exists()
        finally:
            discovery_orchestrator.DEFAULT_APPROVAL_STATE_PATH = prev_approval_state_path
            discovery_orchestrator.run_sheet_directory_audit = prev_sheet
            discovery_orchestrator.discover_gamesmap_candidates = prev_gamesmap
            discovery_orchestrator.discover_gameprog_candidates = prev_gameprog
            discovery_orchestrator.run_web_search_directory_audit = prev_web_audit
            discovery_orchestrator.async_probe_candidate = prev_probe


def test_run_discovery_only_gamedevmap_skips_other_generator_stages() -> None:
    with workspace_tmpdir("source-discovery-only-gamedevmap") as root:
        with override_discovery_runtime(root, studio_seeds=[], static_candidates=[]) as paths:
            cli_args = discovery_orchestrator.parse_args(["--only-gamedevmap"])

            with (
                mock.patch.object(
                    discovery_orchestrator,
                    "stage_curated_seed_candidates",
                    side_effect=AssertionError("curated seed stage should be disabled"),
                ),
                mock.patch.object(
                    discovery_orchestrator,
                    "run_sheet_directory_audit",
                    side_effect=AssertionError("sheet directory stage should be disabled"),
                ),
                mock.patch.object(
                    discovery_orchestrator,
                    "build_pattern_candidates",
                    side_effect=AssertionError("provider-pattern stage should be disabled"),
                ),
                mock.patch.object(
                    discovery_orchestrator,
                    "discover_gamesmap_candidates",
                    side_effect=AssertionError("gamesmap stage should be disabled"),
                ),
                mock.patch.object(
                    discovery_orchestrator,
                    "discover_gameprog_candidates",
                    side_effect=AssertionError("gameprog stage should be disabled"),
                ),
                mock.patch.object(
                    discovery_orchestrator,
                    "discover_gamedevmap_candidates",
                    return_value=(
                        [
                            {
                                "name": "GameDevMap Greenhouse",
                                "studio": "GameDevMap Studio",
                                "adapter": "greenhouse",
                                "slug": "gamedevmap-studio",
                                "api_url": "https://boards-api.greenhouse.io/v1/boards/gamedevmap-studio/jobs?content=true",
                                "discoveryMethod": "gamedevmap",
                                "discoveryStage": "web_provider",
                                "evidenceScore": 46,
                                "evidenceTypes": ["gamedevmap_directory"],
                                "sourceDirectory": "gamedevmap",
                            }
                        ],
                        [],
                        [],
                    ),
                ) as gamedevmap_mock,
            ):
                report = discovery_orchestrator.run_discovery(
                    timeout_s=5,
                    top_n=0,
                    mode="dynamic",
                    include_web_search=True,
                    discovery_config={"gamedevmap": {"enabled": False}},
                    cli_args=cli_args,
                    fetcher=lambda *args, **kwargs: "",
                )

            assert gamedevmap_mock.call_count == 1
            assert int(report["summary"].get("queuedCandidateCount") or 0) == 1
            assert (
                int((report["summary"].get("generatedCountByStage") or {}).get("web_provider") or 0)
                == 1
            )
            queued = json.loads(paths.discovery_candidates_path.read_text(encoding="utf-8"))
            assert len(queued) == 1
            assert str(queued[0].get("discoveryMethod") or "") == "gamedevmap"
            assert str(queued[0].get("sourceDirectory") or "") == "gamedevmap"


def test_run_discovery_pattern_candidates_below_reinforced_threshold_are_skipped() -> None:
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
        ):
            report = sd.run_discovery(
                timeout_s=5,
                top_n=0,
                mode="dynamic",
                include_web_search=False,
                discovery_config={
                    "stageToggles": {
                        "curatedSeed": True,
                        "sheetDirectory": False,
                        "providerPatterns": True,
                        "seedCareersScan": False,
                        "gamesmap": False,
                        "gameprog": False,
                        "gamedevmap": False,
                        "webSearch": False,
                    },
                    "gamesmap": {"enabled": False},
                    "gameprog": {"enabled": False},
                    "gamedevmap": {"enabled": False},
                    "thresholds": {"patternProviderProbeThreshold": 32},
                },
                fetcher=lambda *_: json.dumps({"jobs": [{}]}),
            )
            assert int(report["summary"].get("probedCandidateCount") or 0) == 0
            assert int(report["summary"].get("queuedCandidateCount") or 0) == 0
            assert (
                int((report["summary"].get("lossAccounting") or {}).get("lowEvidenceSkipped") or 0)
                == 1
            )
            stages = [str(row.get("stage") or "") for row in (report.get("failures") or [])]
            assert "probe_skipped" in stages
            dropped = [
                row
                for row in (report.get("failures") or [])
                if str(row.get("dropStage") or "") == "low_evidence_skipped"
            ]
            assert dropped


def test_run_discovery_persists_deferred_candidates_in_candidates_file() -> None:
    with workspace_tmpdir("source-discovery") as root:
        with override_discovery_runtime(root, studio_seeds=[], static_candidates=[]) as paths:
            payloads = {
                "https://boards-api.greenhouse.io/v1/boards/demo/jobs?content=true": json.dumps(
                    {"jobs": [{}, {}]}
                ),
                "https://boards-api.greenhouse.io/v1/boards/demo-alt/jobs?content=true": json.dumps(
                    {"jobs": [{}, {}]}
                ),
                "https://boards-api.greenhouse.io/v1/boards/demo-third/jobs?content=true": json.dumps(
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
                                "name": "Demo Greenhouse",
                                "studio": "Demo",
                                "adapter": "greenhouse",
                                "slug": "demo",
                                "api_url": "https://boards-api.greenhouse.io/v1/boards/demo/jobs?content=true",
                                "discoveryMethod": "sheet_directory",
                                "discoveryStage": "sheet_directory",
                                "evidenceScore": 46,
                                "evidenceTypes": ["sheet_directory"],
                            },
                            {
                                "name": "Demo Greenhouse Alt",
                                "studio": "Demo Alt",
                                "adapter": "greenhouse",
                                "slug": "demo-alt",
                                "api_url": "https://boards-api.greenhouse.io/v1/boards/demo-alt/jobs?content=true",
                                "discoveryMethod": "sheet_directory",
                                "discoveryStage": "sheet_directory",
                                "evidenceScore": 46,
                                "evidenceTypes": ["sheet_directory"],
                            },
                            {
                                "name": "Demo Greenhouse Third",
                                "studio": "Demo Third",
                                "adapter": "greenhouse",
                                "slug": "demo-third",
                                "api_url": "https://boards-api.greenhouse.io/v1/boards/demo-third/jobs?content=true",
                                "discoveryMethod": "sheet_directory",
                                "discoveryStage": "sheet_directory",
                                "evidenceScore": 46,
                                "evidenceTypes": ["sheet_directory"],
                            },
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

            persisted_candidates = json.loads(
                paths.discovery_candidates_path.read_text(encoding="utf-8")
            )
            assert report["summary"]["queuedCandidateCount"] == 2
            assert report["summary"]["discoverableButDeferredCount"] == 1
            assert len(persisted_candidates) == 3
            assert len([row for row in persisted_candidates if not bool(row.get("deferred"))]) == 2
            deferred_row = next(row for row in persisted_candidates if bool(row.get("deferred")))
            assert deferred_row["deferReason"] == "domain_cap"
            assert deferred_row["promotionLane"] == "domain_cap_review"
            assert deferred_row["candidateState"] == "validated"
            assert int(deferred_row["deferCount"]) == 1
            assert deferred_row["firstDeferredAt"]
            assert deferred_row["lastDeferredAt"]
