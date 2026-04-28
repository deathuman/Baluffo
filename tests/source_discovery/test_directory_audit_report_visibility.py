from __future__ import annotations

import json

from ._helpers import (
    _gamesmap_next_payload_html,
    discovery_orchestrator,
    mock,
    override_discovery_runtime,
    sd,
    workspace_tmpdir,
)


def _stage_config(target: str, section: dict[str, object]) -> dict[str, object]:
    return {
        "stageToggles": {
            "curatedSeed": False,
            "sheetDirectory": False,
            "providerPatterns": False,
            "seedCareersScan": False,
            "gamesmap": target == "gamesmap",
            "gameprog": target == "gameprog",
            "gamedevmap": False,
            "webSearch": False,
        },
        "gamesmap": {"enabled": target == "gamesmap"},
        "gameprog": {"enabled": target == "gameprog"},
        target: section,
    }


def test_run_discovery_reports_opt_in_gameprog_directory_audit_summary() -> None:
    with workspace_tmpdir("directory-audit-report-gameprog") as root:
        with override_discovery_runtime(root, studio_seeds=[], static_candidates=[]) as paths:
            audit_path = root / "gameprog-audit.json"
            config = _stage_config(
                "gameprog",
                {
                    "enabled": True,
                    "activeAuditPath": str(audit_path),
                    "activeAuditTtlMinutes": 60,
                    "teamsUrl": "https://gameprog.it/teams.json",
                    "websiteOnlyFallback": False,
                    "maxStudios": 1,
                },
            )
            payloads = {
                "https://gameprog.it/teams.json": json.dumps(
                    [{"name": "Quiet Studio", "url": "https://quiet.example.com/", "place": "Rome"}]
                ),
                "https://quiet.example.com/": "<!doctype html><html><body>Studio</body></html>",
            }

            report = sd.run_discovery(
                timeout_s=5,
                top_n=0,
                mode="dynamic",
                include_web_search=False,
                discovery_config=config,
                fetcher=lambda url, _timeout: payloads[url],
            )

            summary = report["directoryAuditSummaries"]["gameprog"]
            assert report["summary"]["directoryAudits"]["gameprog"] == summary
            assert summary["cacheHit"] is False
            assert summary["complete"] is True
            assert summary["teamsRows"] == 1
            assert summary["websiteFetchJobs"] == 1
            assert summary["failures"] == 0
            assert summary["artifactSizeBytes"] > 0
            assert summary["timingTotalsMs"]["teamsFetchMs"] >= 0
            assert report["candidates"] == []
            assert json.loads(paths.pending_path.read_text(encoding="utf-8")) == []


def test_run_discovery_reuses_default_gameprog_directory_audit_artifact() -> None:
    with workspace_tmpdir("directory-audit-report-gameprog-cache") as root:
        with override_discovery_runtime(root, studio_seeds=[], static_candidates=[]):
            audit_path = root / "gameprog-audit.json"
            config = _stage_config(
                "gameprog",
                {
                    "enabled": True,
                    "activeAuditPath": str(audit_path),
                    "activeAuditTtlMinutes": 60,
                    "teamsUrl": "https://gameprog.it/teams.json",
                    "maxStudios": 0,
                },
            )

            sd.run_discovery(
                timeout_s=5,
                top_n=0,
                mode="dynamic",
                include_web_search=False,
                discovery_config=config,
                fetcher=lambda url, _timeout: "[]" if url.endswith("teams.json") else "",
            )
            report = sd.run_discovery(
                timeout_s=5,
                top_n=0,
                mode="dynamic",
                include_web_search=False,
                discovery_config=config,
                fetcher=lambda *_args: (_ for _ in ()).throw(
                    AssertionError("fresh audit artifact should bypass Gameprog network work")
                ),
            )

            summary = report["directoryAuditSummaries"]["gameprog"]
            assert summary["cacheHit"] is True
            assert summary["complete"] is True
            assert summary["artifactSizeBytes"] > 0


def test_run_discovery_reports_gamesmap_directory_audit_cache_hit() -> None:
    with workspace_tmpdir("directory-audit-report-gamesmap") as root:
        with override_discovery_runtime(root, studio_seeds=[], static_candidates=[]):
            audit_path = root / "gamesmap-audit.json"
            config = _stage_config(
                "gamesmap",
                {
                    "enabled": True,
                    "activeAuditPath": str(audit_path),
                    "activeAuditTtlMinutes": 60,
                    "baseUrl": "https://www.gamesmap.de",
                    "indexUrls": ["https://www.gamesmap.de/en"],
                    "websiteOnlyFallback": True,
                    "maxDetailPages": 0,
                    "allowedCategoryTokens": ["developer"],
                    "blockedCategoryTokens": [],
                },
            )
            payloads = {"https://www.gamesmap.de/en": _gamesmap_next_payload_html([])}

            def fake_fetch(url: str, _timeout: int) -> str:
                if url not in payloads:
                    raise RuntimeError(f"unexpected URL: {url}")
                return payloads[url]

            sd.run_discovery(
                timeout_s=5,
                top_n=0,
                mode="dynamic",
                include_web_search=False,
                discovery_config=config,
                fetcher=fake_fetch,
            )
            report = sd.run_discovery(
                timeout_s=5,
                top_n=0,
                mode="dynamic",
                include_web_search=False,
                discovery_config=config,
                fetcher=lambda *_args: (_ for _ in ()).throw(
                    AssertionError("fresh audit artifact should bypass network work")
                ),
            )

            summary = report["directoryAuditSummaries"]["gamesmap"]
            assert report["summary"]["directoryAudits"]["gamesmap"] == summary
            assert summary["cacheHit"] is True
            assert summary["complete"] is True
            assert summary["indexUrls"] == 1
            assert summary["parsedRows"] == 0
            assert summary["artifactSizeBytes"] > 0
            assert "indexFetchParseMs" in summary["timingTotalsMs"]


def test_run_discovery_keeps_gamesmap_adapter_disabled_by_default() -> None:
    with workspace_tmpdir("directory-audit-report-gamesmap-disabled-default") as root:
        with override_discovery_runtime(root, studio_seeds=[], static_candidates=[]):
            config = {
                "stageToggles": {
                    "curatedSeed": False,
                    "sheetDirectory": False,
                    "providerPatterns": False,
                    "seedCareersScan": False,
                    "gamesmap": True,
                    "gameprog": False,
                    "gamedevmap": False,
                    "webSearch": False,
                },
                "gameprog": {"enabled": False},
                "gamedevmap": {"enabled": False},
            }

            report = sd.run_discovery(
                timeout_s=5,
                top_n=0,
                mode="dynamic",
                include_web_search=False,
                discovery_config=config,
                fetcher=lambda *_args: (_ for _ in ()).throw(
                    AssertionError("disabled Gamesmap adapter should not fetch")
                ),
            )

            assert report["directoryAuditSummaries"] == {}
            assert report["summary"]["directoryAudits"] == {}


def test_run_discovery_gameprog_uses_audit_metadata_even_when_legacy_flag_disabled() -> None:
    with workspace_tmpdir("directory-audit-report-gameprog-legacy-flag") as root:
        with override_discovery_runtime(root, studio_seeds=[], static_candidates=[]):
            config = _stage_config(
                "gameprog",
                {
                    "enabled": True,
                    "activeAuditEnabled": False,
                    "activeAuditPath": str(root / "gameprog-audit.json"),
                    "activeAuditTtlMinutes": 60,
                    "teamsUrl": "https://gameprog.it/teams.json",
                    "maxStudios": 0,
                },
            )

            report = sd.run_discovery(
                timeout_s=5,
                top_n=0,
                mode="dynamic",
                include_web_search=False,
                discovery_config=config,
                fetcher=lambda url, _timeout: "[]" if url.endswith("teams.json") else "",
            )

            summary = report["directoryAuditSummaries"]["gameprog"]
            assert report["summary"]["directoryAudits"]["gameprog"] == summary
            assert summary["cacheHit"] is False
            assert summary["complete"] is True


def test_run_discovery_default_sheet_directory_reports_audit_summary_and_reuses_cache() -> None:
    with workspace_tmpdir("directory-audit-report-sheet-default") as root:
        with override_discovery_runtime(
            root,
            studio_seeds=[],
            static_candidates=[],
            extra_config_overrides={
                "GAME_STUDIOS_SHEET_ID": "sheet_test",
                "GAME_STUDIOS_SHEET_GID": "1",
            },
        ) as paths:
            sheet_url = sd.game_studios_sheet_candidate_urls("sheet_test", "1")[0]
            audit_path = root / "sheet-audit.json"
            config = {
                "stageToggles": {
                    "curatedSeed": False,
                    "sheetDirectory": True,
                    "providerPatterns": False,
                    "seedCareersScan": False,
                    "gamesmap": False,
                    "gameprog": False,
                    "gamedevmap": False,
                    "webSearch": False,
                },
                "sheetDirectory": {
                    "activeAuditPath": str(audit_path),
                    "activeAuditTtlMinutes": 60,
                },
                "gamesmap": {"enabled": False},
                "gameprog": {"enabled": False},
                "gamedevmap": {"enabled": False},
            }
            provider_api = (
                "https://boards-api.greenhouse.io/v1/boards/examplestudio/jobs?content=true"
            )
            payloads = {
                sheet_url: """x,x,x,x
x,Studio,Hiring Location,Roles open,Link
x,Example Studio,Remote,yes,https://boards.greenhouse.io/examplestudio
""",
                provider_api: json.dumps({"jobs": [{}, {}]}),
            }

            first_report = sd.run_discovery(
                timeout_s=5,
                top_n=0,
                mode="dynamic",
                include_web_search=False,
                discovery_config=config,
                fetcher=lambda url, _timeout: payloads[url],
            )

            first_summary = first_report["directoryAuditSummaries"]["sheet_directory"]
            assert first_report["summary"]["directoryAudits"]["sheet_directory"] == first_summary
            assert first_summary["cacheHit"] is False
            assert first_summary["complete"] is True
            assert first_summary["rawRows"] == 1
            assert first_summary["providerCandidates"] == 1
            assert audit_path.exists()
            assert int(first_report["summary"].get("queuedCandidateCount") or 0) == 1

            def fetch_without_sheet(url: str, _timeout: int) -> str:
                if url == provider_api:
                    return payloads[provider_api]
                raise AssertionError("fresh sheet audit artifact should bypass CSV fetch")

            second_report = sd.run_discovery(
                timeout_s=5,
                top_n=0,
                mode="dynamic",
                include_web_search=False,
                discovery_config=config,
                fetcher=fetch_without_sheet,
            )

            second_summary = second_report["directoryAuditSummaries"]["sheet_directory"]
            assert second_summary["cacheHit"] is True
            assert second_summary["complete"] is True
            queued = json.loads(paths.discovery_candidates_path.read_text(encoding="utf-8"))
            assert all(str(row.get("discoveryMethod") or "") == "sheet_directory" for row in queued)


def test_run_discovery_explicit_sheet_directory_audit_disabled_uses_legacy_path() -> None:
    with workspace_tmpdir("directory-audit-report-sheet-disabled") as root:
        with override_discovery_runtime(
            root,
            studio_seeds=[],
            static_candidates=[],
            extra_config_overrides={
                "GAME_STUDIOS_SHEET_ID": "sheet_test",
                "GAME_STUDIOS_SHEET_GID": "1",
            },
        ) as paths:
            config = {
                "stageToggles": {
                    "curatedSeed": False,
                    "sheetDirectory": True,
                    "providerPatterns": False,
                    "seedCareersScan": False,
                    "gamesmap": False,
                    "gameprog": False,
                    "gamedevmap": False,
                    "webSearch": False,
                },
                "sheetDirectory": {
                    "activeAuditEnabled": False,
                },
                "gamesmap": {"enabled": False},
                "gameprog": {"enabled": False},
                "gamedevmap": {"enabled": False},
            }

            with mock.patch.object(
                discovery_orchestrator,
                "discover_game_studio_sheet_candidates",
                return_value=(
                    [
                        {
                            "adapter": "greenhouse",
                            "slug": "examplestudio",
                            "api_url": "https://boards-api.greenhouse.io/v1/boards/examplestudio/jobs?content=true",
                            "name": "Example Studio",
                            "discoveryMethod": "sheet_directory",
                            "discoveryStage": "sheet_directory",
                            "evidenceScore": 46,
                        }
                    ],
                    [],
                    [],
                ),
            ) as legacy_scan:
                report = sd.run_discovery(
                    timeout_s=5,
                    top_n=0,
                    mode="dynamic",
                    include_web_search=False,
                    discovery_config=config,
                    fetcher=lambda *_args: json.dumps({"jobs": [{}, {}]}),
                )

            legacy_scan.assert_called_once()
            assert "sheet_directory" not in report["directoryAuditSummaries"]
            assert "sheet_directory" not in report["summary"]["directoryAudits"]
            queued = json.loads(paths.discovery_candidates_path.read_text(encoding="utf-8"))
            assert len(queued) == 1
            assert str(queued[0].get("discoveryMethod") or "") == "sheet_directory"
