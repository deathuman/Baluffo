from __future__ import annotations

import json

from ._helpers import discovery_orchestrator, mock, override_discovery_runtime, sd, workspace_tmpdir


def _stage_config(
    *,
    audit_path: str | None = None,
    active_audit_enabled: bool | None = None,
) -> dict[str, object]:
    config: dict[str, object] = {
        "stageToggles": {
            "curatedSeed": False,
            "sheetDirectory": False,
            "providerPatterns": False,
            "seedCareersScan": True,
            "gamesmap": False,
            "gameprog": False,
            "gamedevmap": False,
            "webSearch": True,
        },
        "gamesmap": {"enabled": False},
        "gameprog": {"enabled": False},
        "gamedevmap": {"enabled": False},
    }
    if audit_path is not None:
        web_search_config: dict[str, object] = {
            "activeAuditPath": audit_path,
            "activeAuditTtlMinutes": 60,
        }
        if active_audit_enabled is not None:
            web_search_config["activeAuditEnabled"] = active_audit_enabled
        config["webSearch"] = web_search_config
    return config


def test_run_discovery_audit_disabled_web_search_uses_direct_paths_without_metadata() -> None:
    with workspace_tmpdir("web-search-audit-flow-disabled") as root:
        with override_discovery_runtime(
            root,
            studio_seeds=[{"studio": "Example Studio", "careersUrl": "https://example.com/jobs"}],
            static_candidates=[],
        ):
            config = _stage_config(
                audit_path=str(root / "disabled-web-audit.json"),
                active_audit_enabled=False,
            )
            with (
                mock.patch.object(
                    discovery_orchestrator,
                    "discover_seed_careers_page_candidates",
                    return_value=([], [], []),
                ) as seed_scan,
                mock.patch.object(
                    discovery_orchestrator,
                    "discover_web_search_candidates",
                    return_value=([], [], []),
                ) as web_scan,
            ):
                report = sd.run_discovery(
                    timeout_s=5,
                    top_n=0,
                    mode="dynamic",
                    include_web_search=True,
                    discovery_config=config,
                    fetcher=lambda *_args: "",
                )

            seed_scan.assert_called_once()
            web_scan.assert_called_once()
            assert "web_search" not in report["directoryAuditSummaries"]
            assert "web_search" not in report["summary"]["directoryAudits"]


def test_run_discovery_default_web_search_audit_reuses_artifact() -> None:
    with workspace_tmpdir("web-search-audit-flow-default") as root:
        with override_discovery_runtime(
            root,
            studio_seeds=[
                {
                    "studio": "Seed Studio",
                    "careersUrl": "https://seed.example/careers",
                }
            ],
            static_candidates=[],
        ) as paths:
            audit_path = root / "web-audit.json"
            config = _stage_config(audit_path=str(audit_path))

            def fake_fetch(url: str, _timeout_s: int) -> str:
                if url == "https://seed.example/careers":
                    return '<a href="https://boards.greenhouse.io/seedstudio/jobs/1">Role</a>'
                if "duckduckgo.com" in url:
                    return '<a href="https://search.example/careers">Careers</a>'
                if url == "https://search.example/careers":
                    return '<a href="https://boards.greenhouse.io/searchstudio/jobs/1">Role</a>'
                if "boards-api.greenhouse.io" in url:
                    return json.dumps({"jobs": [{}, {}]})
                raise RuntimeError(f"unexpected URL: {url}")

            first_report = sd.run_discovery(
                timeout_s=5,
                top_n=0,
                mode="dynamic",
                include_web_search=True,
                discovery_config=config,
                fetcher=fake_fetch,
            )

            first_summary = first_report["directoryAuditSummaries"]["web_search"]
            assert first_report["summary"]["directoryAudits"]["web_search"] == first_summary
            assert first_summary["cacheHit"] is False
            assert first_summary["complete"] is True
            assert first_summary["seedCareersEnabled"] is True
            assert first_summary["webSearchEnabled"] is True
            assert first_summary["maxQueries"] == 24
            assert first_summary["maxLinksPerQuery"] == 8
            assert first_summary["webQueriesPlanned"] == 3
            assert first_summary["webSearchSuccesses"] == 3
            assert first_summary["webLinksExtracted"] == 3
            assert first_summary["webLinksConsidered"] == 3
            assert first_summary["webJobishLinks"] == 3
            assert first_summary["webDuplicatePageFetchUrls"] == 2
            assert len(first_summary["webQuerySamples"]) == 3
            assert first_summary["providerCandidates"] >= 2
            assert audit_path.exists()
            queued = json.loads(paths.discovery_candidates_path.read_text(encoding="utf-8"))
            assert {row["discoveryMethod"] for row in queued} == {
                "seed_careers_page",
                "web_search",
            }

            def cache_fetch(url: str, _timeout_s: int) -> str:
                if "boards-api.greenhouse.io" in url:
                    return json.dumps({"jobs": [{}, {}]})
                raise AssertionError(
                    "fresh web-search audit artifact should bypass discovery fetch"
                )

            second_report = sd.run_discovery(
                timeout_s=5,
                top_n=0,
                mode="dynamic",
                include_web_search=True,
                discovery_config=config,
                fetcher=cache_fetch,
            )

            assert second_report["directoryAuditSummaries"]["web_search"]["cacheHit"] is True

            config_with_changed_tuning = _stage_config(audit_path=str(audit_path))
            config_with_changed_tuning["webSearch"]["maxLinksPerQuery"] = 4
            third_report = sd.run_discovery(
                timeout_s=5,
                top_n=0,
                mode="dynamic",
                include_web_search=True,
                discovery_config=config_with_changed_tuning,
                fetcher=fake_fetch,
            )

            assert third_report["directoryAuditSummaries"]["web_search"]["cacheHit"] is False
            assert third_report["directoryAuditSummaries"]["web_search"]["maxLinksPerQuery"] == 4


def test_run_discovery_default_web_search_audit_respects_no_web_search() -> None:
    with workspace_tmpdir("web-search-audit-flow-seed-only") as root:
        with override_discovery_runtime(
            root,
            studio_seeds=[
                {
                    "studio": "Seed Studio",
                    "careersUrl": "https://seed.example/careers",
                }
            ],
            static_candidates=[],
        ):
            audit_path = root / "seed-only-web-audit.json"
            config = _stage_config(audit_path=str(audit_path))

            def fake_fetch(url: str, _timeout_s: int) -> str:
                if url == "https://seed.example/careers":
                    return '<a href="https://boards.greenhouse.io/seedstudio/jobs/1">Role</a>'
                if "boards-api.greenhouse.io" in url:
                    return json.dumps({"jobs": [{}]})
                raise AssertionError(
                    "web-search fetch should not run when include_web_search is false"
                )

            report = sd.run_discovery(
                timeout_s=5,
                top_n=0,
                mode="dynamic",
                include_web_search=False,
                discovery_config=config,
                fetcher=fake_fetch,
            )

            summary = report["directoryAuditSummaries"]["web_search"]
            assert summary["cacheHit"] is False
            assert summary["complete"] is True
            assert summary["seedCareersEnabled"] is True
            assert summary["webSearchEnabled"] is False
            assert summary["seedProviderCandidates"] == 1
            assert int(summary.get("webProviderCandidates") or 0) == 0
            assert int(summary.get("webQueriesPlanned") or 0) == 0
            assert int(summary.get("webSearchSuccesses") or 0) == 0
