import asyncio
import importlib
import json
import os
import sys
import threading
import time
from pathlib import Path
from unittest import mock

from src import source_discovery as sd
from src import source_registry as sr
from src.source_discovery import gamesmap as gamesmap_adapter
from src.source_discovery import orchestrator as discovery_orchestrator
from src.source_discovery import url_patches as discovery_url_patches
from src.source_discovery.core import classify_probe_failure_stage
from src.source_discovery.schemas import DiscoveryReportSummarySchema
from src.source_discovery.web_search import async_fetch_text_httpx
from tests.helpers.temp_paths import workspace_tmpdir

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


def _fixture_json(name: str):
    return json.loads((FIXTURES_DIR / name).read_text(encoding="utf-8"))


def _fixture_text(name: str) -> str:
    return (FIXTURES_DIR / name).read_text(encoding="utf-8")


def _gamesmap_next_payload_html(companies: list[dict[str, object]]) -> str:
    payload = f'payload-start "companies":{json.dumps(companies, ensure_ascii=False)},"regions":[] payload-end'
    return (
        '<!DOCTYPE html><html lang="en"><body><script>'
        f"self.__next_f.push([1,{json.dumps(payload, ensure_ascii=False)}]);"
        "</script></body></html>"
    )


def test_resolve_directory_fetch_limits_uses_env_defaults_and_adapter_overrides() -> None:
    with mock.patch.dict(
        os.environ,
        {
            "BALUFFO_DISCOVERY_DIRECTORY_FETCH_CONCURRENCY_TOTAL": "9",
            "BALUFFO_DISCOVERY_DIRECTORY_FETCH_CONCURRENCY_PER_HOST": "3",
        },
        clear=False,
    ):
        assert sd.directory_fetch_concurrency_defaults() == {"total": 9, "perHost": 3}
        assert sd.resolve_directory_fetch_limits({}) == (9, 3)
        assert sd.resolve_directory_fetch_limits(
            {"fetchConcurrency": 4, "perHostConcurrency": 1}
        ) == (4, 1)
        assert sd.resolve_directory_fetch_limits(
            {"fetchConcurrency": 0, "perHostConcurrency": 0}
        ) == (9, 3)


def test_default_directory_fetch_profiles_use_24x3_for_live_adapters() -> None:
    for adapter in ("gameprog", "gamesmap", "gamedevmap"):
        cfg = sd.DEFAULT_DISCOVERY_CONFIG[adapter]
        assert int(cfg.get("fetchConcurrency") or 0) == 24
        assert int(cfg.get("perHostConcurrency") or 0) == 3


def test_fetch_directory_pages_preserves_order_and_respects_concurrency_limits() -> None:
    jobs = [
        {
            "url": "https://a.example/slow",
            "payload": {"id": "a-slow"},
            "name": "a-slow",
            "adapter": "gamedevmap",
            "failureStage": "homepage_fetch",
        },
        {
            "url": "https://a.example/fast",
            "payload": {"id": "a-fast"},
            "name": "a-fast",
            "adapter": "gamedevmap",
            "failureStage": "homepage_fetch",
        },
        {
            "url": "https://b.example/fast",
            "payload": {"id": "b-fast"},
            "name": "b-fast",
            "adapter": "gamedevmap",
            "failureStage": "homepage_fetch",
        },
        {
            "url": "https://c.example/fail",
            "payload": {"id": "c-fail"},
            "name": "c-fail",
            "adapter": "gamedevmap",
            "failureStage": "homepage_fetch",
        },
    ]
    delays = {
        "https://a.example/slow": 0.08,
        "https://a.example/fast": 0.01,
        "https://b.example/fast": 0.02,
        "https://c.example/fail": 0.02,
    }
    lock = threading.Lock()
    active = 0
    max_active = 0
    host_active: dict[str, int] = {}
    host_max: dict[str, int] = {}

    def fake_fetch(url: str, _: int) -> str:
        nonlocal active, max_active
        host = url.split("/")[2]
        with lock:
            active += 1
            max_active = max(max_active, active)
            host_active[host] = host_active.get(host, 0) + 1
            host_max[host] = max(host_max.get(host, 0), host_active[host])
        try:
            time.sleep(delays[url])
            if url.endswith("/fail"):
                raise RuntimeError("boom")
            return f"<html>{url}</html>"
        finally:
            with lock:
                active -= 1
                host_active[host] = max(0, host_active.get(host, 1) - 1)

    results = sd.fetch_directory_pages(
        5,
        jobs,
        fetcher=fake_fetch,
        total_concurrency=3,
        per_host_concurrency=1,
        progress_label="Test directory fetch",
        progress_every=2,
    )

    assert [str((row.get("payload") or {}).get("id") or "") for row in results] == [
        "a-slow",
        "a-fast",
        "b-fast",
        "c-fail",
    ]
    assert [bool(row.get("ok")) for row in results] == [True, True, True, False]
    assert str(results[0].get("text") or "").startswith("<html>")
    assert "boom" in str(results[3].get("error") or "")
    assert str(((results[3].get("failure") or {}).get("stage")) or "") == "homepage_fetch"
    assert max_active <= 3
    assert all(count <= 1 for count in host_max.values())


def test_build_pattern_candidates_respects_likely_providers() -> None:
    previous = list(sd.STUDIO_SEEDS)
    sd.STUDIO_SEEDS = [
        {
            "studio": "Example Studio",
            "aliases": ["example-studio"],
            "nlPriority": True,
            "likelyProviders": ["greenhouse", "teamtailor"],
        }
    ]
    try:
        rows = sd.build_pattern_candidates()
    finally:
        sd.STUDIO_SEEDS = previous

    adapters = {str(row.get("adapter")) for row in rows}
    assert adapters == {"greenhouse", "teamtailor"}


def test_build_pattern_candidates_adds_reinforcement_for_provider_matching_careers_url() -> None:
    previous = list(sd.STUDIO_SEEDS)
    sd.STUDIO_SEEDS = [
        {
            "studio": "Example Studio",
            "aliases": ["example-studio"],
            "nlPriority": False,
            "likelyProviders": ["greenhouse"],
            "careersUrl": "https://boards.greenhouse.io/example-studio",
        }
    ]
    try:
        rows = sd.build_pattern_candidates()
    finally:
        sd.STUDIO_SEEDS = previous
    assert len(rows) >= 1
    assert all(int(row.get("evidenceScore") or 0) >= 42 for row in rows)
    assert all("seed_provider_reinforced" in (row.get("evidenceTypes") or []) for row in rows)


def test_build_pattern_candidates_supports_recruitee_and_pinpoint_providers() -> None:
    previous = list(sd.STUDIO_SEEDS)
    sd.STUDIO_SEEDS = [
        {
            "studio": "Example Studio",
            "aliases": ["example-studio"],
            "nlPriority": False,
            "likelyProviders": ["recruitee", "pinpoint"],
        }
    ]
    try:
        rows = sd.build_pattern_candidates()
    finally:
        sd.STUDIO_SEEDS = previous

    adapters = {str(row.get("adapter")) for row in rows}
    assert adapters == {"recruitee", "pinpoint"}
    assert any(str(row.get("api_url") or "").endswith("/api/offers/") for row in rows)
    assert any(str(row.get("api_url") or "").endswith("/postings.json") for row in rows)


def test_build_pattern_candidates_generates_root_ashby_board_urls() -> None:
    previous = list(sd.STUDIO_SEEDS)
    sd.STUDIO_SEEDS = [
        {
            "studio": "Example Studio",
            "aliases": ["example-studio"],
            "nlPriority": False,
            "likelyProviders": ["ashby"],
        }
    ]
    try:
        rows = sd.build_pattern_candidates()
    finally:
        sd.STUDIO_SEEDS = previous
    assert len(rows) == 1
    assert rows[0]["adapter"] == "ashby"
    assert rows[0]["board_url"] == "https://jobs.ashbyhq.com/example-studio"


def test_seed_catalog_path_points_to_repo_src_catalog() -> None:
    assert sd.SEED_CATALOG_PATH.name == "discovery_seed_catalog.json"
    assert sd.SEED_CATALOG_PATH.parts[-2] == "src"
    assert sd.SEED_CATALOG_PATH.exists()


def test_probe_concurrency_defaults_use_updated_fallbacks() -> None:
    previous = {
        key: os.environ.get(key)
        for key in (
            "BALUFFO_DISCOVERY_PROBE_CONCURRENCY_TOTAL",
            "BALUFFO_DISCOVERY_PROBE_CONCURRENCY_STATIC",
            "BALUFFO_DISCOVERY_PROBE_CONCURRENCY_PROVIDER",
            "BALUFFO_DISCOVERY_PROBE_CONCURRENCY_TEAMTAILOR",
        )
    }
    try:
        for key in previous:
            os.environ.pop(key, None)
        defaults = sd.probe_concurrency_defaults()
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    assert defaults == {
        "total": 40,
        "static": 16,
        "provider": 40,
        "teamtailor": 15,
    }


def test_adapter_queue_caps_use_updated_provider_growth_defaults() -> None:
    assert sd.ADAPTER_QUEUE_CAPS == {
        "greenhouse": 12,
        "lever": 10,
        "smartrecruiters": 8,
        "workable": 8,
        "teamtailor": 8,
        "ashby": 10,
        "recruitee": 6,
        "pinpoint": 6,
        "personio": 3,
        "static": 8,
    }
    assert sd.UNCAPPED_DISCOVERY_DOMAIN_QUEUE_CAP == 8
    assert sd.UNCAPPED_DISCOVERY_ADAPTER_QUEUE_CAPS == {
        "greenhouse": 24,
        "lever": 20,
        "smartrecruiters": 16,
        "workable": 16,
        "teamtailor": 16,
        "ashby": 20,
        "recruitee": 12,
        "pinpoint": 12,
        "personio": 6,
        "static": 16,
    }


def test_apply_queue_balancing_covers_provider_bias_and_google_sheet_cap_bypass() -> None:
    cases = [
        {
            "name": "provider bias in bounded runs",
            "candidates": [
                {
                    "name": "Static A",
                    "studio": "Static A",
                    "adapter": "static",
                    "score": 99,
                    "evidenceScore": 99,
                    "jobsFound": 5,
                    "pages": ["https://static-a.example/jobs"],
                },
                {
                    "name": "Static B",
                    "studio": "Static B",
                    "adapter": "static",
                    "score": 98,
                    "evidenceScore": 98,
                    "jobsFound": 5,
                    "pages": ["https://static-b.example/jobs"],
                },
                {
                    "name": "Greenhouse A",
                    "studio": "Greenhouse A",
                    "adapter": "greenhouse",
                    "score": 80,
                    "evidenceScore": 80,
                    "jobsFound": 4,
                    "api_url": "https://boards-api.greenhouse.io/v1/boards/a/jobs?content=true",
                },
                {
                    "name": "Lever A",
                    "studio": "Lever A",
                    "adapter": "lever",
                    "score": 79,
                    "evidenceScore": 79,
                    "jobsFound": 4,
                    "api_url": "https://api.lever.co/v0/postings/a?mode=json",
                },
                {
                    "name": "Ashby A",
                    "studio": "Ashby A",
                    "adapter": "ashby",
                    "score": 78,
                    "evidenceScore": 78,
                    "jobsFound": 4,
                    "board_url": "https://jobs.ashbyhq.com/a",
                },
                {
                    "name": "SmartRecruiters A",
                    "studio": "SmartRecruiters A",
                    "adapter": "smartrecruiters",
                    "score": 77,
                    "evidenceScore": 77,
                    "jobsFound": 4,
                    "api_url": "https://api.smartrecruiters.com/v1/companies/A/postings",
                },
            ],
            "top_n": 4,
            "expected_queued": ["greenhouse", "lever", "ashby", "smartrecruiters"],
            "expected_static_queued": 0,
            "expected_static_deferred": 2,
            "expected_static_healthy_deferred": 2,
            "expected_deferred_count": 2,
            "expected_provider_target": 2,
        },
        {
            "name": "google sheet family cap under base balancing",
            "candidates": [
                {
                    "name": f"Sheet Static {index}",
                    "studio": f"Sheet Static {index}",
                    "adapter": "static",
                    "score": 90 - index,
                    "evidenceScore": 70,
                    "jobsFound": 2,
                    "pages": [f"https://sheet.example/jobs/{index}"],
                    "discoveryStage": "sheet_directory",
                    "sourceDirectory": "game_studios_sheet",
                    "careersUrl": f"https://sheet.example/jobs/{index}",
                    "sourceDirectoryEntryUrl": f"https://sheet.example/jobs/{index}",
                }
                for index in range(10)
            ],
            "top_n": 0,
            "expected_len": 2,
            "expected_static_queued": 2,
            "expected_static_deferred": 8,
            "expected_static_healthy_deferred": 8,
            "expected_deferred_count": 8,
            "expected_deferred_reason": "domain_cap",
            "expected_provider_target": 0,
        },
        {
            "name": "uncapped exploration raises family cap for repeated sheet families",
            "candidates": [
                {
                    "name": f"Sheet Static {index}",
                    "studio": "Sheet Static",
                    "adapter": "static",
                    "score": 90 - index,
                    "evidenceScore": 70,
                    "jobsFound": 2,
                    "pages": [f"https://sheet.example/jobs/{index}"],
                    "discoveryStage": "sheet_directory",
                    "sourceDirectory": "game_studios_sheet",
                    "careersUrl": f"https://sheet.example/jobs/{index}",
                    "sourceDirectoryEntryUrl": f"https://sheet.example/jobs/{index}",
                }
                for index in range(10)
            ],
            "top_n": 0,
            "queue_kwargs": {
                "domain_cap": sd.UNCAPPED_DISCOVERY_DOMAIN_QUEUE_CAP,
                "adapter_caps": sd.UNCAPPED_DISCOVERY_ADAPTER_QUEUE_CAPS,
            },
            "expected_len": 8,
            "expected_static_queued": 8,
            "expected_static_deferred": 2,
            "expected_static_healthy_deferred": 2,
            "expected_deferred_count": 2,
            "expected_deferred_reason": "domain_cap",
            "expected_provider_target": 0,
        },
    ]

    for case in cases:
        queued, report_rows, stats = sd.apply_queue_balancing(
            case["candidates"],
            top_n=case["top_n"],
            **dict(case.get("queue_kwargs") or {}),
        )
        if "expected_queued" in case:
            assert [str(row.get("adapter") or "") for row in queued] == case["expected_queued"], (
                case["name"]
            )
        if "expected_len" in case:
            assert len(queued) == case["expected_len"], case["name"]
        assert (
            int((stats.get("queuedByAdapter") or {}).get("static") or 0)
            == case["expected_static_queued"]
        ), case["name"]
        assert (
            int((stats.get("deferredByAdapter") or {}).get("static") or 0)
            == case["expected_static_deferred"]
        ), case["name"]
        assert (
            int((stats.get("healthyButDeferredByAdapter") or {}).get("static") or 0)
            == case["expected_static_healthy_deferred"]
        ), case["name"]
        if "expected_deferred_count" in case:
            assert (
                len([row for row in report_rows if bool(row.get("deferred"))])
                == case["expected_deferred_count"]
            ), case["name"]
            assert int(stats.get("providerTarget") or 0) == case["expected_provider_target"], case[
                "name"
            ]
        else:
            assert len([row for row in report_rows if bool(row.get("deferred"))]) == 0, case["name"]
            assert "adapter_cap" not in (stats.get("deferredReasons") or {}), case["name"]
        if "expected_deferred_reason" in case:
            assert (
                int((stats.get("deferredReasons") or {}).get(case["expected_deferred_reason"]) or 0)
                == case["expected_deferred_count"]
            ), case["name"]


def test_apply_sheet_directory_static_probe_cap_bypasses_cap_for_uncapped_mode() -> None:
    candidates = [
        {
            "name": f"Sheet Static {index}",
            "studio": f"Sheet Static {index}",
            "adapter": "static",
            "score": 90 - index,
            "evidenceScore": 70,
            "jobsFound": 2,
            "pages": [f"https://sheet-{index}.example/jobs"],
            "discoveryStage": "sheet_directory",
            "sourceDirectory": "game_studios_sheet",
        }
        for index in range(12)
    ]

    kept, suppressed = discovery_orchestrator.apply_sheet_directory_static_probe_cap(
        candidates,
        top_n=6,
        bypass_cap=True,
        source_state_rows={},
    )

    assert len(kept) == 12
    assert suppressed == []


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

    config = sd.load_discovery_config()

    def run_preset(preset: str) -> dict:
        with workspace_tmpdir(f"source-discovery-{preset}") as tmp:
            root = Path(tmp)
            previous_paths = {
                "ACTIVE_PATH": sd.ACTIVE_PATH,
                "PENDING_PATH": sd.PENDING_PATH,
                "REJECTED_PATH": sd.REJECTED_PATH,
                "DISCOVERY_REPORT_PATH": sd.DISCOVERY_REPORT_PATH,
                "DISCOVERY_CANDIDATES_PATH": sd.DISCOVERY_CANDIDATES_PATH,
                "URL_PATCH_MANIFEST_PATH": getattr(sd, "URL_PATCH_MANIFEST_PATH", None),
            }
            sd.ACTIVE_PATH = root / "source-registry-active.json"
            sd.PENDING_PATH = root / "source-registry-pending.json"
            sd.REJECTED_PATH = root / "source-registry-rejected.json"
            sd.DISCOVERY_REPORT_PATH = root / "source-discovery-report.json"
            sd.DISCOVERY_CANDIDATES_PATH = root / "source-discovery-candidates.json"
            if previous_paths["URL_PATCH_MANIFEST_PATH"] is not None:
                sd.URL_PATCH_MANIFEST_PATH = root / "url-patch-manifest.json"
            for path in (sd.ACTIVE_PATH, sd.PENDING_PATH, sd.REJECTED_PATH):
                path.write_text("[]", encoding="utf-8")
            try:
                with (
                    mock.patch.object(
                        discovery_orchestrator,
                        "discover_game_studio_sheet_candidates",
                        return_value=([], list(dynamic_candidates), []),
                    ),
                    mock.patch.object(
                        discovery_orchestrator, "stage_curated_seed_candidates", return_value=[]
                    ),
                    mock.patch.object(
                        discovery_orchestrator.sd, "build_pattern_candidates", return_value=[]
                    ),
                    mock.patch.object(
                        discovery_orchestrator.sd,
                        "discover_seed_careers_page_candidates",
                        return_value=([], [], []),
                    ),
                    mock.patch.object(
                        discovery_orchestrator,
                        "discover_web_search_candidates",
                        return_value=([], []),
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
            finally:
                sd.ACTIVE_PATH = previous_paths["ACTIVE_PATH"]
                sd.PENDING_PATH = previous_paths["PENDING_PATH"]
                sd.REJECTED_PATH = previous_paths["REJECTED_PATH"]
                sd.DISCOVERY_REPORT_PATH = previous_paths["DISCOVERY_REPORT_PATH"]
                sd.DISCOVERY_CANDIDATES_PATH = previous_paths["DISCOVERY_CANDIDATES_PATH"]
                if previous_paths["URL_PATCH_MANIFEST_PATH"] is not None:
                    sd.URL_PATCH_MANIFEST_PATH = previous_paths["URL_PATCH_MANIFEST_PATH"]

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


def test_classify_static_suppression_suppresses_weak_repeat_low_yield_static_candidate() -> None:
    reason = sd.classify_static_suppression(
        {
            "name": "Weak Static (Manual Website)",
            "studio": "Weak Static",
            "adapter": "static",
            "discoveryStage": "generic_static",
            "weakSignal": True,
            "manualOnly": True,
            "evidenceScore": 26,
            "evidenceTypes": ["careers_keyword"],
        },
        source_state_rows={
            "Weak Static (Manual Website)": {
                "lastDurationMs": 32000,
                "lastKeptCount": 0,
                "lastDetailPagesVisited": 14,
                "lastDetailYieldPct": 0,
                "lastCandidateLinksFound": 12,
            }
        },
        thresholds=sd.DEFAULT_DISCOVERY_THRESHOLDS,
    )
    assert reason == "manual_only_repeat_low_yield"


def test_classify_static_suppression_preserves_strong_or_previously_productive_static_candidate() -> (
    None
):
    strong_reason = sd.classify_static_suppression(
        {
            "name": "Strong Static (Manual Website)",
            "studio": "Strong Static",
            "adapter": "static",
            "discoveryStage": "generic_static",
            "weakSignal": True,
            "manualOnly": True,
            "evidenceScore": 26,
            "evidenceTypes": ["careers_keyword", "structured_job_links"],
        },
        source_state_rows={},
        thresholds=sd.DEFAULT_DISCOVERY_THRESHOLDS,
    )
    productive_reason = sd.classify_static_suppression(
        {
            "name": "Previously Productive (Manual Website)",
            "studio": "Previously Productive",
            "adapter": "static",
            "discoveryStage": "generic_static",
            "weakSignal": True,
            "manualOnly": True,
            "evidenceScore": 24,
            "evidenceTypes": ["careers_keyword"],
        },
        source_state_rows={
            "Previously Productive (Manual Website)": {
                "lastDurationMs": 22000,
                "lastKeptCount": 3,
                "lastDetailPagesVisited": 6,
                "lastDetailYieldPct": 25,
            }
        },
        thresholds=sd.DEFAULT_DISCOVERY_THRESHOLDS,
    )
    assert strong_reason == ""
    assert productive_reason == ""


def test_sheet_directory_static_probe_cap_scales_from_bounded_top_n() -> None:
    assert sd.sheet_directory_static_probe_cap(0) == 0
    assert sd.sheet_directory_static_probe_cap(4) == 4
    assert sd.sheet_directory_static_probe_cap(20) == 6


def test_apply_sheet_directory_static_probe_cap_limits_overproducing_sheet_static_rows() -> None:
    candidates = [
        {
            "name": "Sheet Static Productive",
            "studio": "Productive",
            "adapter": "static",
            "discoveryStage": "sheet_directory",
            "evidenceScore": 46,
            "jobsFound": 0,
            "pages": ["https://productive.example/jobs"],
        },
        {
            "name": "Sheet Static B",
            "studio": "B",
            "adapter": "static",
            "discoveryStage": "sheet_directory",
            "evidenceScore": 46,
            "jobsFound": 0,
            "pages": ["https://b.example/jobs"],
        },
        {
            "name": "Sheet Static C",
            "studio": "C",
            "adapter": "static",
            "discoveryStage": "sheet_directory",
            "evidenceScore": 46,
            "jobsFound": 0,
            "pages": ["https://c.example/jobs"],
        },
        {
            "name": "Sheet Static D",
            "studio": "D",
            "adapter": "static",
            "discoveryStage": "sheet_directory",
            "evidenceScore": 46,
            "jobsFound": 0,
            "pages": ["https://d.example/jobs"],
        },
        {
            "name": "Sheet Static E",
            "studio": "E",
            "adapter": "static",
            "discoveryStage": "sheet_directory",
            "evidenceScore": 46,
            "jobsFound": 0,
            "pages": ["https://e.example/jobs"],
        },
        {
            "name": "Greenhouse A",
            "studio": "Greenhouse A",
            "adapter": "greenhouse",
            "discoveryStage": "provider_pattern",
            "evidenceScore": 70,
            "jobsFound": 0,
            "api_url": "https://boards-api.greenhouse.io/v1/boards/a/jobs?content=true",
        },
    ]
    kept, suppressed = sd.apply_sheet_directory_static_probe_cap(
        candidates,
        top_n=4,
        source_state_rows={
            "Sheet Static Productive": {
                "lastKeptCount": 3,
                "lastJobsFound": 5,
                "lastDurationMs": 1200,
            }
        },
    )
    assert len([row for row in kept if str(row.get("adapter")) == "static"]) == 4
    assert len(suppressed) == 1
    assert any(str(row.get("name")) == "Sheet Static Productive" for row in kept)


def test_source_registry_paths_honor_baluffo_data_dir_override() -> None:
    previous = os.environ.get("BALUFFO_DATA_DIR")
    override_root = str((Path.cwd() / "_out" / "test-source-registry-override").resolve())
    try:
        os.environ["BALUFFO_DATA_DIR"] = override_root
        import src.source_registry as source_registry

        source_registry = importlib.reload(source_registry)
        assert source_registry.DATA_DIR == Path(override_root)
        assert source_registry.ACTIVE_PATH == Path(override_root) / "source-registry-active.json"
        assert source_registry.PENDING_PATH == Path(override_root) / "source-registry-pending.json"
        assert (
            source_registry.REJECTED_PATH == Path(override_root) / "source-registry-rejected.json"
        )
        assert (
            source_registry.DISCOVERY_REPORT_PATH
            == Path(override_root) / "source-discovery-report.json"
        )
        assert (
            source_registry.DISCOVERY_CANDIDATES_PATH
            == Path(override_root) / "source-discovery-candidates.json"
        )
        assert (
            source_registry.URL_PATCH_MANIFEST_PATH
            == Path(override_root) / "url-patch-manifest.json"
        )
    finally:
        if previous is None:
            os.environ.pop("BALUFFO_DATA_DIR", None)
        else:
            os.environ["BALUFFO_DATA_DIR"] = previous


def test_async_fetch_text_httpx_enables_redirect_following() -> None:
    calls = []

    class _Response:
        def __init__(self):
            self.encoding = None
            self.text = "ok"

        def raise_for_status(self) -> None:
            return None

    class _Client:
        async def get(self, url: str, **kwargs):
            calls.append((url, kwargs))
            return _Response()

    result = asyncio.run(async_fetch_text_httpx(_Client(), "https://example.com/jobs", timeout_s=5))
    assert result == "ok"
    assert calls == [
        (
            "https://example.com/jobs",
            {
                "headers": mock.ANY,
                "follow_redirects": True,
            },
        )
    ]


def test_classify_probe_failure_stage_treats_httpx_404_as_probe_miss() -> None:
    assert (
        classify_probe_failure_stage(
            "https://example.com/jobs: Client error '404 Not Found' for url 'https://example.com/jobs'"
        )
        == "probe_miss"
    )


def test_probe_candidate_maps_jobs_found_for_greenhouse_and_teamtailor() -> None:
    greenhouse = {
        "adapter": "greenhouse",
        "slug": "example",
        "api_url": "https://boards-api.greenhouse.io/v1/boards/example/jobs?content=true",
    }
    ok, count, error = sd.probe_candidate(
        greenhouse, timeout_s=5, fetcher=lambda *_: json.dumps({"jobs": [{}, {}]})
    )
    assert ok
    assert count == 2
    assert error == ""

    teamtailor = {"adapter": "teamtailor", "listing_url": "https://example.teamtailor.com/jobs"}
    html = """
    <a href="https://example.teamtailor.com/jobs/123-role-a">A</a>
    <a href="https://example.teamtailor.com/jobs/456-role-b">B</a>
    """
    ok, count, error = sd.probe_candidate(teamtailor, timeout_s=5, fetcher=lambda *_: html)
    assert ok
    assert count == 2
    assert error == ""


def test_probe_candidate_uses_fallback_when_primary_fails() -> None:
    greenhouse = {
        "adapter": "greenhouse",
        "slug": "example",
        "api_url": "https://boards-api.greenhouse.io/v1/boards/example/jobs?content=true",
    }

    def fake_fetch(url: str, _: int) -> str:
        if "boards-api.greenhouse.io" in url:
            raise RuntimeError("HTTP Error 404: Not Found")
        if "boards.greenhouse.io/example" in url:
            return '<a href="https://boards.greenhouse.io/example/jobs/123">Role</a>'
        raise RuntimeError(f"unexpected URL: {url}")

    ok, count, error = sd.probe_candidate(greenhouse, timeout_s=5, fetcher=fake_fetch)
    assert ok
    assert count == 1
    assert error == ""


def test_probe_candidate_maps_jobs_found_for_recruitee_and_pinpoint() -> None:
    recruitee = {
        "adapter": "recruitee",
        "subdomain": "example",
        "api_url": "https://example.recruitee.com/api/offers/",
    }
    ok, count, error = sd.probe_candidate(
        recruitee,
        timeout_s=5,
        fetcher=lambda *_: json.dumps({"offers": [{}, {}]}),
    )
    assert ok
    assert count == 2
    assert error == ""

    pinpoint = {
        "adapter": "pinpoint",
        "subdomain": "gameplaygalaxy",
        "api_url": "https://gameplaygalaxy.pinpointhq.com/postings.json",
    }
    ok, count, error = sd.probe_candidate(
        pinpoint,
        timeout_s=5,
        fetcher=lambda *_: json.dumps({"data": [{}, {}, {}]}),
    )
    assert ok
    assert count == 3
    assert error == ""


def test_async_probe_candidate_mirrors_sync_probe_count() -> None:
    greenhouse = {
        "adapter": "greenhouse",
        "slug": "example",
        "api_url": "https://boards-api.greenhouse.io/v1/boards/example/jobs?content=true",
    }

    async def fake_async_fetch(url: str, _timeout: int) -> str:
        assert "boards-api.greenhouse.io" in url
        return json.dumps({"jobs": [{}, {}, {}]})

    ok, count, error = asyncio.run(
        sd.async_probe_candidate(greenhouse, timeout_s=5, fetcher=fake_async_fetch)
    )
    assert ok
    assert count == 3
    assert error == ""


def test_validate_candidate_for_probe_rejects_invalid_identity() -> None:
    valid, reason = sd.validate_candidate_for_probe({"adapter": "lever", "account": "12"})
    assert not valid
    assert "invalid" in reason


def test_infer_provider_candidates_from_html_detects_embedded_urls() -> None:
    html = """
    <a href="https://boards.greenhouse.io/example/jobs/123">Job</a>
    <script>const api='https://api.lever.co/v0/postings/example?mode=json';</script>
    """
    rows = sd.infer_provider_candidates_from_html(
        "https://example.com/careers",
        html,
        studio="Example Studio",
        nl_priority=False,
    )
    adapters = {str(row.get("adapter") or "") for row in rows}
    assert "greenhouse" in adapters
    assert "lever" in adapters


def test_infer_provider_candidates_from_html_detects_provider_from_page_url() -> None:
    rows = sd.infer_provider_candidates_from_html(
        "https://example.jobs.personio.de/",
        "<html><body>Careers</body></html>",
        studio="Example Studio",
        nl_priority=False,
        discovery_method="seed_careers_page",
    )
    assert len(rows) == 1
    assert str(rows[0].get("adapter") or "") == "personio"
    assert str(rows[0].get("evidenceSource") or "") == "page_url"


def test_infer_provider_candidates_from_html_detects_pinpoint_provider_from_page_url() -> None:
    rows = sd.infer_provider_candidates_from_html(
        "https://example.pinpointhq.com/",
        "<html><body>Careers</body></html>",
        studio="Example Studio",
        nl_priority=False,
        discovery_method="seed_careers_page",
    )
    assert len(rows) == 1
    assert str(rows[0].get("adapter") or "") == "pinpoint"
    assert str(rows[0].get("evidenceSource") or "") == "page_url"


def test_infer_provider_candidates_from_html_collapses_competing_seed_page_variants() -> None:
    html = """
    <a href="https://boards.greenhouse.io/first-board/jobs/123">Job A</a>
    <a href="https://boards.greenhouse.io/second-board/jobs/456">Job B</a>
    """
    rows = sd.infer_provider_candidates_from_html(
        "https://example.com/careers",
        html,
        studio="Example Studio",
        nl_priority=False,
        discovery_method="seed_careers_page",
    )
    assert len(rows) == 1
    assert str(rows[0].get("adapter") or "") == "greenhouse"


def test_discover_seed_careers_page_candidates_infers_provider_without_web_search() -> None:
    previous = list(sd.STUDIO_SEEDS)
    sd.STUDIO_SEEDS = [
        {
            "studio": "Example Studio",
            "aliases": ["example-studio"],
            "nlPriority": False,
            "careersUrl": "https://example.com/careers",
        }
    ]
    try:
        providers, static_rows, failures = sd.discover_seed_careers_page_candidates(
            5,
            fetcher=lambda *_: (
                '<a href="https://boards.greenhouse.io/example-studio/jobs/123">Job</a>'
            ),
        )
    finally:
        sd.STUDIO_SEEDS = previous

    assert len(failures) == 0
    assert len(static_rows) == 0
    assert len(providers) == 1
    assert str(providers[0].get("adapter") or "") == "greenhouse"
    assert str(providers[0].get("discoveryMethod") or "") == "seed_careers_page"


def test_discover_seed_careers_page_candidates_prefers_personio_provider_over_static() -> None:
    previous = list(sd.STUDIO_SEEDS)
    sd.STUDIO_SEEDS = [
        {
            "studio": "Example Studio",
            "aliases": ["example-studio"],
            "nlPriority": False,
            "careersUrl": "https://example.jobs.personio.de/",
        }
    ]
    try:

        def should_not_fetch(*_: object) -> str:
            raise AssertionError("direct provider seed URLs should not be fetched")

        providers, static_rows, failures = sd.discover_seed_careers_page_candidates(
            5,
            fetcher=should_not_fetch,
        )
    finally:
        sd.STUDIO_SEEDS = previous

    assert len(failures) == 0
    assert len(providers) == 1
    assert len(static_rows) == 0
    assert str(providers[0].get("adapter") or "") == "personio"


def test_discover_seed_careers_page_candidates_prefers_explicit_careers_links() -> None:
    previous = list(sd.STUDIO_SEEDS)
    sd.STUDIO_SEEDS = [
        {
            "studio": "Example Studio",
            "aliases": ["example-studio"],
            "nlPriority": False,
            "careersUrl": "https://example.com/",
        }
    ]
    try:
        providers, static_rows, failures = sd.discover_seed_careers_page_candidates(
            5,
            fetcher=lambda *_: (
                """
            <a href="/careers">Careers</a>
            <a href="/jobs/rendering-engineer">Rendering Engineer</a>
            """
            ),
        )
    finally:
        sd.STUDIO_SEEDS = previous

    assert len(failures) == 0
    assert providers == []
    assert len(static_rows) == 1
    assert str(static_rows[0].get("careersUrl") or "") == "https://example.com/careers"
    assert str(static_rows[0].get("name") or "") == "Example Studio (Manual Website)"


def test_discover_seed_careers_page_candidates_builds_static_candidate_without_web_search() -> None:
    previous = list(sd.STUDIO_SEEDS)
    sd.STUDIO_SEEDS = [
        {
            "studio": "Example Studio",
            "aliases": ["example-studio"],
            "nlPriority": False,
            "careersUrl": "https://example.com/careers",
        }
    ]
    try:
        providers, static_rows, failures = sd.discover_seed_careers_page_candidates(
            5,
            fetcher=lambda *_: (
                """
            <a href="/jobs/rendering-engineer">Rendering Engineer</a>
            <a href="/jobs/gameplay-engineer">Gameplay Engineer</a>
            """
            ),
        )
    finally:
        sd.STUDIO_SEEDS = previous

    assert len(failures) == 0
    assert len(providers) == 0
    assert len(static_rows) == 1
    assert str(static_rows[0].get("adapter") or "") == "static"
    assert str(static_rows[0].get("discoveryMethod") or "") == "seed_careers_page"


def test_discover_web_search_candidates_prefers_explicit_careers_links_from_result_pages() -> None:
    studio_seeds = [
        {
            "studio": "Example Studio",
            "aliases": ["example-studio"],
            "nlPriority": False,
        }
    ]

    def fake_fetch(url: str, _: int) -> str:
        if "duckduckgo.com" in url:
            return '<a href="https://example.com/jobs">Example Studio</a>'
        if url == "https://example.com/jobs":
            return """
            <a href="/careers">Careers</a>
            <a href="/jobs/rendering-engineer">Rendering Engineer</a>
            """
        raise RuntimeError(f"unexpected URL: {url}")

    providers, static_rows, failures = sd.discover_web_search_candidates(
        5,
        studio_seeds=studio_seeds,
        fetcher=fake_fetch,
        max_queries=1,
    )
    assert failures == []
    assert providers == []
    assert len(static_rows) == 1
    assert str(static_rows[0].get("careersUrl") or "") == "https://example.com/careers"
    assert str(static_rows[0].get("discoveryMethod") or "") == "web_search"


def test_build_static_candidate_from_page_records_evidence() -> None:
    html = """
    <a href="/jobs/rendering-engineer">Rendering Engineer</a>
    <script type="application/ld+json">{"@type":"JobPosting","title":"Gameplay Engineer"}</script>
    """
    row = sd.build_static_candidate_from_page(
        "https://example.com/careers",
        html,
        studio="Example Studio",
        nl_priority=False,
        discovery_method="web_search",
    )
    assert row is not None
    assert str(row.get("adapter") or "") == "static"
    assert int(row.get("evidenceScore") or 0) >= sd.MIN_STATIC_EVIDENCE_TO_QUEUE
    assert "jobposting_jsonld" in (row.get("evidenceTypes") or [])


def test_build_static_candidate_from_page_blocks_linkedin_like_domains() -> None:
    row = sd.build_static_candidate_from_page(
        "https://www.linkedin.com/company/example/jobs/",
        '<a href="/jobs/test">Test</a>',
        studio="Example Studio",
        nl_priority=False,
        discovery_method="web_search",
    )
    assert row is None


def test_build_known_careers_url_candidate_preserves_requested_fields() -> None:
    row = sd.build_known_careers_url_candidate(
        "https://example.com/careers",
        studio="Example Studio",
        name_suffix="Gameprog",
        nl_priority=False,
        discovery_method="gameprog",
        evidence_source="gameprog",
        evidence_types=["gameprog_directory", "gameprog_careers_url"],
        evidence_score=40,
        enabled_by_default=False,
        extra_fields={
            "sourceDirectory": "gameprog",
            "sourceDirectoryEntryUrl": "https://example.com/",
            "manualOnly": False,
        },
    )
    assert str(row.get("name") or "") == "Example Studio (Gameprog)"
    assert str(row.get("careersUrl") or "") == "https://example.com/careers"
    assert int(row.get("evidenceScore") or 0) == 40
    assert str(row.get("sourceDirectory") or "") == "gameprog"
    assert "gameprog_careers_url" in (row.get("evidenceTypes") or [])


def test_extract_explicit_careers_url_from_page_skips_provider_and_offsite_links() -> None:
    html = """
    <a href="https://boards.greenhouse.io/example-studio">Greenhouse</a>
    <a href="https://external.example.net/careers">External Careers</a>
    <a href="/careers">Careers</a>
    """
    careers_url = sd.extract_explicit_careers_url_from_page(
        "https://studio.example.com/",
        html,
        studio="Example Studio",
        nl_priority=False,
        discovery_method="gamesmap",
    )
    assert careers_url == "https://studio.example.com/careers"


def test_extract_explicit_careers_url_from_page_prefers_landing_page_over_job_detail_links() -> (
    None
):
    html = """
    <a href="/jobs/rendering-engineer">Rendering Engineer</a>
    <a href="/careers">Careers</a>
    """
    careers_url = sd.extract_explicit_careers_url_from_page(
        "https://studio.example.com/",
        html,
        studio="Example Studio",
        nl_priority=False,
        discovery_method="gamesmap",
    )
    assert careers_url == "https://studio.example.com/careers"


def test_analyze_fetched_page_prefers_provider_candidates_over_other_outcomes() -> None:
    html = """
    <a href="/careers">Careers</a>
    <a href="https://boards.greenhouse.io/example-studio/jobs/123">Rendering Engineer</a>
    <script type="application/ld+json">{"@type":"JobPosting","title":"Gameplay Engineer"}</script>
    """
    analyzed = sd.analyze_fetched_page(
        "https://studio.example.com/",
        html,
        studio="Example Studio",
        nl_priority=False,
        discovery_method="gamedevmap",
    )
    assert len(analyzed["provider_candidates"]) == 1
    assert str(analyzed.get("explicit_careers_url") or "") == ""
    assert analyzed["generic_static_candidate"] is None


def test_analyze_fetched_page_falls_back_to_generic_static_without_explicit_links() -> None:
    html = """
    <script type="application/ld+json">{"@type":"JobPosting","title":"Gameplay Engineer"}</script>
    """
    analyzed = sd.analyze_fetched_page(
        "https://studio.example.com/careers",
        html,
        studio="Example Studio",
        nl_priority=False,
        discovery_method="web_search",
    )
    assert analyzed["provider_candidates"] == []
    assert str(analyzed.get("explicit_careers_url") or "") == ""
    assert analyzed["generic_static_candidate"] is not None


def test_parse_gamedevmap_csv_returns_normalized_rows() -> None:
    rows = sd.parse_gamedevmap_csv(_fixture_text("gamedevmap_data.csv"))
    assert len(rows) == 6
    assert rows[0]["studio"] == "Provider Feed Studio"
    assert rows[0]["url"] == "https://boards.greenhouse.io/provider-feed-studio"
    assert rows[0]["country"] == "Sweden"


def test_select_gamedevmap_representative_rows_filters_and_dedupes() -> None:
    rows = sd.parse_gamedevmap_csv(_fixture_text("gamedevmap_data.csv"))
    selected = sd.select_gamedevmap_representative_rows(
        rows,
        allowed_categories=[
            "Developer",
            "Developer and Publisher",
            "Publisher",
            "Mobile",
        ],
        blocked_categories=["Organization"],
        index_url="https://www.gamedevmap.com/index.php",
    )
    assert len(selected) == 4
    duplicate = next(
        row for row in selected if str(row.get("url") or "") == "https://duplicate.example.com"
    )
    assert str(duplicate.get("studio") or "") == "Duplicate Direct A"
    assert int(duplicate.get("duplicateCount") or 0) == 2
    assert duplicate.get("categories") == ["Developer", "Mobile"]
    assert "query=Duplicate+Direct+A" in str(duplicate.get("sourceDirectoryEntryUrl") or "")
    assert "exact=1" in str(duplicate.get("sourceDirectoryEntryUrl") or "")
    assert "type=Developer" in str(duplicate.get("sourceDirectoryEntryUrl") or "")


def test_discover_gamedevmap_candidates_emits_direct_provider_homepage_provider_and_static() -> (
    None
):
    config = {
        "gamedevmap": {
            "enabled": True,
            "csvUrl": "https://www.gamedevmap.com/cmsdata/gamedevmapdata.csv",
            "indexUrl": "https://www.gamedevmap.com/index.php",
            "allowedCategories": [
                "Developer",
                "Developer and Publisher",
                "Publisher",
                "Mobile",
            ],
            "blockedCategories": ["Organization"],
            "maxHomepageFetches": 2,
        }
    }
    payloads = {
        "https://www.gamedevmap.com/cmsdata/gamedevmapdata.csv": _fixture_text(
            "gamedevmap_data.csv"
        ),
        "https://homepage-provider.example.com": _fixture_text("gamedevmap_homepage_provider.html"),
        "https://homepage-static.example.com": _fixture_text("gamedevmap_homepage_static.html"),
    }

    def fake_fetch(url: str, _: int) -> str:
        if url not in payloads:
            raise RuntimeError(f"unexpected URL: {url}")
        return payloads[url]

    provider_rows, static_rows, failures = sd.discover_gamedevmap_candidates(
        5, config=config, fetcher=fake_fetch
    )
    assert len(failures) == 0
    assert len(provider_rows) == 2
    assert len(static_rows) == 1
    direct_provider = next(
        row
        for row in provider_rows
        if str(row.get("careersUrl") or "") == "https://boards.greenhouse.io/provider-feed-studio"
    )
    homepage_provider = next(
        row
        for row in provider_rows
        if str(row.get("careersUrl") or "") == "https://homepage-provider.example.com"
    )
    assert "gamedevmap_direct_url" in (direct_provider.get("evidenceTypes") or [])
    assert "gamedevmap_homepage_fetch" in (homepage_provider.get("evidenceTypes") or [])
    assert str(homepage_provider.get("sourceDirectory") or "") == "gamedevmap"
    assert int(homepage_provider.get("evidenceScore") or 0) >= 44
    assert str(static_rows[0].get("adapter") or "") == "static"
    assert str(static_rows[0].get("sourceDirectory") or "") == "gamedevmap"
    assert "gamedevmap_homepage_fetch" in (static_rows[0].get("evidenceTypes") or [])
    assert "gamedevmap_careers_url" not in (static_rows[0].get("evidenceTypes") or [])
    assert str(static_rows[0].get("careersUrl") or "") == "https://homepage-static.example.com"
    assert not bool(static_rows[0].get("weakSignal"))


def test_discover_gamedevmap_candidates_skips_homepages_without_job_evidence() -> None:
    csv_text = """Organization,URL,City,State/Province,Country/Region,Map Def,Category,Comments,Updated By,Bluesky,AI Response
No Jobs Studio,https://homepage-no-jobs.example.com,Rome,Lazio,Italy,Rome,Developer,Verified gaming studio.,,,Correct (Gaming)
"""
    config = {
        "gamedevmap": {
            "enabled": True,
            "csvUrl": "https://www.gamedevmap.com/cmsdata/gamedevmapdata.csv",
            "indexUrl": "https://www.gamedevmap.com/index.php",
            "allowedCategories": ["Developer"],
            "blockedCategories": [],
            "maxHomepageFetches": 1,
        }
    }
    payloads = {
        "https://www.gamedevmap.com/cmsdata/gamedevmapdata.csv": csv_text,
        "https://homepage-no-jobs.example.com": _fixture_text("gamedevmap_homepage_no_jobs.html"),
    }

    def fake_fetch(url: str, _: int) -> str:
        if url not in payloads:
            raise RuntimeError(f"unexpected URL: {url}")
        return payloads[url]

    provider_rows, static_rows, failures = sd.discover_gamedevmap_candidates(
        5, config=config, fetcher=fake_fetch
    )
    assert len(failures) == 0
    assert provider_rows == []
    assert static_rows == []


def test_discover_gamedevmap_candidates_reuses_fresh_cache() -> None:
    with workspace_tmpdir("gamedevmap-cache") as root:
        cache_path = root / "gamedevmap-cache.json"
        config = {
            "gamedevmap": {
                "enabled": True,
                "csvUrl": "https://www.gamedevmap.com/cmsdata/gamedevmapdata.csv",
                "indexUrl": "https://www.gamedevmap.com/index.php",
                "allowedCategories": [
                    "Developer",
                    "Developer and Publisher",
                    "Publisher",
                    "Mobile",
                ],
                "blockedCategories": ["Organization"],
                "maxHomepageFetches": 2,
                "cachePath": str(cache_path),
                "cacheTtlMinutes": 60,
            }
        }
        payloads = {
            "https://www.gamedevmap.com/cmsdata/gamedevmapdata.csv": _fixture_text(
                "gamedevmap_data.csv"
            ),
            "https://homepage-provider.example.com": _fixture_text(
                "gamedevmap_homepage_provider.html"
            ),
            "https://homepage-static.example.com": _fixture_text("gamedevmap_homepage_static.html"),
        }
        calls: list[str] = []

        def fake_fetch(url: str, _: int) -> str:
            calls.append(url)
            if url not in payloads:
                raise RuntimeError(f"unexpected URL: {url}")
            return payloads[url]

        provider_rows_1, static_rows_1, failures_1 = sd.discover_gamedevmap_candidates(
            5, config=config, fetcher=fake_fetch
        )
        assert len(calls) > 0
        first_call_count = len(calls)

        with mock.patch(
            "src.source_discovery.gamedevmap.fetch_directory_pages",
            side_effect=AssertionError("directory fetch helper should be bypassed on cache hit"),
        ):
            provider_rows_2, static_rows_2, failures_2 = sd.discover_gamedevmap_candidates(
                5, config=config, fetcher=fake_fetch
            )
        assert len(calls) == first_call_count
        assert provider_rows_1 == provider_rows_2
        assert static_rows_1 == static_rows_2
        assert failures_1 == failures_2


def test_parse_gamesmap_detail_page_extracts_careers_and_provenance() -> None:
    row = sd.parse_gamesmap_detail_page(
        "https://www.gamesmap.de/en/detail/industry/example-studio-gmbh",
        _fixture_text("gamesmap_detail_careers.html"),
    )
    assert row is not None
    assert str(row.get("studio") or "") == "Example Studio GmbH"
    assert str(row.get("careersUrl") or "") == "https://boards.greenhouse.io/examplestudio"
    assert str(row.get("websiteUrl") or "") == "https://www.example-studio.com"
    assert "Developer and Publisher" in (row.get("categories") or [])


def test_parse_gamesmap_detail_page_supports_website_only_entries() -> None:
    row = sd.parse_gamesmap_detail_page(
        "https://www.gamesmap.de/en/detail/industry/example-publisher",
        _fixture_text("gamesmap_detail_website_only.html"),
    )
    assert row is not None
    assert str(row.get("careersUrl") or "") == ""
    assert str(row.get("websiteUrl") or "") == "https://www.example-publisher.com"
    assert "Publisher" in (row.get("categories") or [])


def test_parse_gamesmap_detail_page_ignores_directory_and_social_links_for_website_fallback() -> (
    None
):
    html = """
    <html><body>
      <h1>Example Studio</h1>
      <h3>Categories</h3>
      <div><span class="view-detail-category">Developer</span></div>
      <a href="https://www.game.de/datenschutz/">Data protection</a>
      <a href="https://www.facebook.com/example">Facebook</a>
      <a href="https://example-studio.com/">https://example-studio.com/</a>
    </body></html>
    """
    row = sd.parse_gamesmap_detail_page(
        "https://www.gamesmap.de/en/detail/industry/example-studio",
        html,
    )
    assert row is not None
    assert str(row.get("websiteUrl") or "") == "https://example-studio.com/"
    assert str(row.get("careersUrl") or "") == ""


def test_parse_gamesmap_index_entries_extracts_company_rows_from_next_payload() -> None:
    rows = sd.parse_gamesmap_index_entries(
        _fixture_text("gamesmap_index_next_payload.html"),
        "https://www.gamesmap.de",
        prefer_english=True,
    )
    assert len(rows) == 5
    direct_provider = next(
        row for row in rows if str(row.get("studio") or "") == "Provider Direct Studio"
    )
    assert (
        str(direct_provider.get("detailUrl") or "")
        == "https://www.gamesmap.de/en/company/provider-direct-studio"
    )
    assert (
        str(direct_provider.get("websiteUrl") or "") == "https://boards.greenhouse.io/examplestudio"
    )
    assert direct_provider.get("categories") == ["Developer"]
    assert str(direct_provider.get("location") or "") == "Hamburg"
    homepage_provider = next(
        row for row in rows if str(row.get("studio") or "") == "Homepage Provider Studio"
    )
    assert homepage_provider.get("categories") == ["Console / PC", "Developer"]
    missing_website = next(
        row for row in rows if str(row.get("studio") or "") == "Missing Website Studio"
    )
    assert str(missing_website.get("websiteUrl") or "") == ""
    assert missing_website.get("categories") == ["Developer", "Publisher"]


def test_parse_gamesmap_index_entries_resolves_category_references_and_drops_bad_ones() -> None:
    category_ref = "$1b:props:children:props:children:props:children:props:companies:{company}:categories:{category}"
    companies = [
        {
            "id": "1",
            "name": "Anchor Developer",
            "slug": "anchor-developer",
            "categories": [{"name": "Developer"}],
            "address": {"city": "Berlin", "state": "Berlin", "country": "DE"},
            "websites": ["https://anchor.example.com"],
        },
        {
            "id": "2",
            "name": "Recursive Reference",
            "slug": "recursive-reference",
            "categories": [category_ref.format(company=0, category=0)],
            "address": {"city": "Berlin", "state": "Berlin", "country": "DE"},
            "websites": ["https://recursive.example.com"],
        },
        {
            "id": "3",
            "name": "Bad Reference",
            "slug": "bad-reference",
            "categories": [
                category_ref.format(company=99, category=0),
                {"name": "Publisher"},
            ],
            "address": {"city": "Berlin", "state": "Berlin", "country": "DE"},
            "websites": ["https://bad.example.com"],
        },
        {
            "id": "4",
            "name": "Cyclic Reference",
            "slug": "cyclic-reference",
            "categories": [category_ref.format(company=3, category=0)],
            "address": {"city": "Berlin", "state": "Berlin", "country": "DE"},
            "websites": ["https://cycle.example.com"],
        },
    ]

    rows, diagnostics = gamesmap_adapter._parse_gamesmap_index_entries_with_diagnostics(
        _gamesmap_next_payload_html(companies),
        "https://www.gamesmap.de",
        prefer_english=True,
    )

    by_studio = {str(row.get("studio") or ""): row for row in rows}
    assert by_studio["Recursive Reference"]["categories"] == ["Developer"]
    assert by_studio["Bad Reference"]["categories"] == ["Publisher"]
    assert by_studio["Cyclic Reference"]["categories"] == []
    assert int(diagnostics.get("unresolvedReferenceCount") or 0) == 2
    assert all(
        "$1b:" not in category for row in rows for category in list(row.get("categories") or [])
    )


def test_gamesmap_category_filter_rejects_blocked_entries() -> None:
    row = sd.parse_gamesmap_detail_page(
        "https://www.gamesmap.de/detail/industry/tooling-association",
        _fixture_text("gamesmap_detail_blocked.html"),
    )
    assert row is not None
    config = {
        "gamesmap": {
            "allowedCategoryTokens": ["developer", "publisher"],
            "blockedCategoryTokens": ["association", "education"],
        }
    }
    assert not sd.gamesmap_matches_category(
        row.get("categories") or [],
        config["gamesmap"]["allowedCategoryTokens"],
        config["gamesmap"]["blockedCategoryTokens"],
    )


def test_gamesmap_matches_category_uses_token_aware_rules() -> None:
    allowed = [
        "developer",
        "developer and publisher",
        "publisher",
        "console",
        "pc",
        "mobile",
        "browser",
        "online",
        "vr",
        "ar",
        "serious games",
    ]
    blocked = ["public institution", "service provider"]

    assert sd.gamesmap_matches_category(["Developer"], allowed, blocked)
    assert sd.gamesmap_matches_category(
        ["Developer", "Publisher"], ["developer and publisher"], blocked
    )
    assert sd.gamesmap_matches_category(["Console / PC"], allowed, blocked)
    assert sd.gamesmap_matches_category(["VR / AR"], allowed, blocked)
    assert sd.gamesmap_matches_category(["Serious games"], allowed, blocked)
    assert not sd.gamesmap_matches_category(["Research"], ["ar"], [])
    assert not sd.gamesmap_matches_category(["Market research"], ["ar"], [])
    assert not sd.gamesmap_matches_category(["PR/marketing agency"], ["ar"], [])
    assert not sd.gamesmap_matches_category(["Public institutions"], allowed, blocked)
    assert not sd.gamesmap_matches_category(["Service provider"], allowed, blocked)


def test_parse_gamesmap_live_style_reference_payload_preserves_many_eligible_rows() -> None:
    category_ref = "$1b:props:children:props:children:props:children:props:companies:{company}:categories:{category}"
    companies = [
        {
            "id": "1",
            "name": "Direct Developer",
            "slug": "direct-developer",
            "categories": [{"name": "Developer"}],
            "address": {"city": "Berlin", "state": "Berlin", "country": "DE"},
            "websites": ["https://direct.example.com"],
        },
        {
            "id": "2",
            "name": "Resolved Developer",
            "slug": "resolved-developer",
            "categories": [category_ref.format(company=0, category=0)],
            "address": {"city": "Berlin", "state": "Berlin", "country": "DE"},
            "websites": ["https://resolved.example.com"],
        },
        {
            "id": "3",
            "name": "Recursive Developer",
            "slug": "recursive-developer",
            "categories": [category_ref.format(company=1, category=0)],
            "address": {"city": "Berlin", "state": "Berlin", "country": "DE"},
            "websites": ["https://recursive.example.com"],
        },
        {
            "id": "4",
            "name": "Developer Publisher",
            "slug": "developer-publisher",
            "categories": [
                category_ref.format(company=0, category=0),
                {"name": "Publisher"},
            ],
            "address": {"city": "Berlin", "state": "Berlin", "country": "DE"},
            "websites": ["https://publisher.example.com"],
        },
        {
            "id": "5",
            "name": "Research Agency",
            "slug": "research-agency",
            "categories": [{"name": "Market research"}],
            "address": {"city": "Berlin", "state": "Berlin", "country": "DE"},
            "websites": ["https://research.example.com"],
        },
    ]

    rows = sd.parse_gamesmap_index_entries(
        _gamesmap_next_payload_html(companies),
        "https://www.gamesmap.de",
        prefer_english=True,
    )
    eligible_rows = [
        row
        for row in rows
        if str(row.get("websiteUrl") or "").strip()
        and sd.gamesmap_matches_category(
            list(row.get("categories") or []),
            ["developer", "publisher", "console", "pc", "mobile", "vr", "ar"],
            ["association", "education", "service provider"],
        )
    ]
    assert len(eligible_rows) == 4
    assert {str(row.get("studio") or "") for row in eligible_rows} == {
        "Direct Developer",
        "Resolved Developer",
        "Recursive Developer",
        "Developer Publisher",
    }


def test_discover_gamesmap_candidates_emits_direct_provider_homepage_provider_and_static_rows() -> (
    None
):
    config = {
        "gamesmap": {
            "enabled": True,
            "baseUrl": "https://www.gamesmap.de",
            "indexUrls": ["https://www.gamesmap.de/en"],
            "websiteOnlyFallback": True,
            "maxDetailPages": 10,
            "allowedCategoryTokens": ["developer", "publisher", "mobile", "pc", "console"],
            "blockedCategoryTokens": ["association", "education"],
        }
    }

    payloads = {
        "https://www.gamesmap.de/en": _fixture_text("gamesmap_index_next_payload.html"),
        "https://homepage-provider.example.com": _fixture_text("gamedevmap_homepage_provider.html"),
        "https://homepage-website-only.example.com": _fixture_text(
            "gamedevmap_homepage_no_jobs.html"
        ),
    }

    def fake_fetch(url: str, _: int) -> str:
        if url not in payloads:
            raise RuntimeError(f"unexpected URL: {url}")
        return payloads[url]

    provider_rows, static_rows, failures = sd.discover_gamesmap_candidates(
        5, config=config, fetcher=fake_fetch
    )
    assert len(failures) == 0
    assert len(provider_rows) == 2
    assert len(static_rows) == 1
    direct_provider = next(
        row
        for row in provider_rows
        if str(row.get("careersUrl") or "") == "https://boards.greenhouse.io/examplestudio"
    )
    homepage_provider = next(
        row
        for row in provider_rows
        if str(row.get("careersUrl") or "") == "https://homepage-provider.example.com"
    )
    assert str(direct_provider.get("adapter") or "") == "greenhouse"
    assert str(direct_provider.get("sourceDirectory") or "") == "gamesmap"
    assert "gamesmap_website" in (direct_provider.get("evidenceTypes") or [])
    assert "gamesmap_website_fetch" in (homepage_provider.get("evidenceTypes") or [])
    assert str(static_rows[0].get("adapter") or "") == "static"
    assert bool(static_rows[0].get("weakSignal"))
    assert (
        str(static_rows[0].get("sourceDirectoryEntryUrl") or "")
        == "https://www.gamesmap.de/en/company/website-only-publisher"
    )
    assert not (bool(static_rows[0].get("manualOnly")))
    assert "gamesmap_website_fetch" in (static_rows[0].get("evidenceTypes") or [])


def test_discover_gamesmap_candidates_emits_explicit_careers_links_without_website_only_fallback() -> (
    None
):
    config = {
        "gamesmap": {
            "enabled": True,
            "baseUrl": "https://www.gamesmap.de",
            "indexUrls": ["https://www.gamesmap.de/en"],
            "websiteOnlyFallback": False,
            "maxDetailPages": 10,
            "allowedCategoryTokens": ["developer", "publisher", "mobile", "pc", "console"],
            "blockedCategoryTokens": ["association", "education"],
        }
    }
    index_html = """
    <!DOCTYPE html>
    <html lang="en">
      <body>
        <script>
          self.__next_f.push([1,"payload-start \\"companies\\":[{\\"id\\":\\"1\\",\\"name\\":\\"Explicit Careers Studio\\",\\"slug\\":\\"explicit-careers-studio\\",\\"categories\\":[{\\"name\\":\\"Developer\\"}],\\"address\\":{\\"city\\":\\"Berlin\\",\\"state\\":\\"Berlin\\",\\"country\\":\\"DE\\"},\\"websites\\":[\\"https://homepage-careers.example.com\\"]}],\\"regions\\":[] payload-end"]);
        </script>
      </body>
    </html>
    """
    payloads = {
        "https://www.gamesmap.de/en": index_html,
        "https://homepage-careers.example.com": """
        <!doctype html>
        <html><body><a href="/careers">Careers</a></body></html>
        """,
    }

    def fake_fetch(url: str, _: int) -> str:
        if url not in payloads:
            raise RuntimeError(f"unexpected URL: {url}")
        return payloads[url]

    provider_rows, static_rows, failures = sd.discover_gamesmap_candidates(
        5, config=config, fetcher=fake_fetch
    )
    assert failures == []
    assert provider_rows == []
    assert len(static_rows) == 1
    assert (
        str(static_rows[0].get("careersUrl") or "")
        == "https://homepage-careers.example.com/careers"
    )
    assert "gamesmap_careers_url" in (static_rows[0].get("evidenceTypes") or [])
    assert "gamesmap_website_fetch" in (static_rows[0].get("evidenceTypes") or [])
    assert not bool(static_rows[0].get("weakSignal"))


def test_discover_gamesmap_candidates_marks_manual_website_only_rows() -> None:
    config = {
        "gamesmap": {
            "enabled": True,
            "baseUrl": "https://www.gamesmap.de",
            "indexUrls": ["https://www.gamesmap.de/en"],
            "websiteOnlyFallback": True,
            "websiteOnlyManualOnly": True,
            "maxDetailPages": 10,
            "allowedCategoryTokens": ["publisher"],
            "blockedCategoryTokens": ["association", "education"],
        }
    }
    payloads = {
        "https://www.gamesmap.de/en": _fixture_text("gamesmap_index_next_payload.html"),
        "https://homepage-website-only.example.com": _fixture_text(
            "gamedevmap_homepage_no_jobs.html"
        ),
    }

    def fake_fetch(url: str, _: int) -> str:
        if url not in payloads:
            raise RuntimeError(f"unexpected URL: {url}")
        return payloads[url]

    _provider_rows, static_rows, failures = sd.discover_gamesmap_candidates(
        5, config=config, fetcher=fake_fetch
    )
    assert len(failures) == 0
    assert len(static_rows) == 1
    assert bool(static_rows[0].get("weakSignal"))
    assert bool(static_rows[0].get("manualOnly"))
    assert "gamesmap_manual_website_only" in (static_rows[0].get("evidenceTypes") or [])
    assert "gamesmap_website_fetch" in (static_rows[0].get("evidenceTypes") or [])


def test_discover_gamesmap_candidates_reports_parse_failure_when_index_shape_is_unknown() -> None:
    config = {
        "gamesmap": {
            "enabled": True,
            "baseUrl": "https://www.gamesmap.de",
            "indexUrls": ["https://www.gamesmap.de/en"],
            "websiteOnlyFallback": True,
            "maxDetailPages": 10,
            "allowedCategoryTokens": ["developer"],
            "blockedCategoryTokens": [],
        }
    }

    def fake_fetch(url: str, _: int) -> str:
        if url != "https://www.gamesmap.de/en":
            raise RuntimeError(f"unexpected URL: {url}")
        return "<html><body><h1>No embedded company payload</h1></body></html>"

    provider_rows, static_rows, failures = sd.discover_gamesmap_candidates(
        5, config=config, fetcher=fake_fetch
    )
    assert len(provider_rows) == 0
    assert len(static_rows) == 0
    assert len(failures) == 1
    assert str(failures[0].get("stage") or "") == "directory_index_parse"


def test_discover_gamesmap_candidates_reuses_fresh_cache() -> None:
    with workspace_tmpdir("gamesmap-cache") as root:
        cache_path = root / "gamesmap-cache.json"
        config = {
            "gamesmap": {
                "enabled": True,
                "baseUrl": "https://www.gamesmap.de",
                "indexUrls": ["https://www.gamesmap.de/en"],
                "websiteOnlyFallback": True,
                "maxDetailPages": 10,
                "allowedCategoryTokens": ["developer", "publisher", "mobile", "pc", "console"],
                "blockedCategoryTokens": ["association", "education"],
                "cachePath": str(cache_path),
                "cacheTtlMinutes": 60,
            }
        }
        payloads = {
            "https://www.gamesmap.de/en": _fixture_text("gamesmap_index_next_payload.html"),
            "https://homepage-provider.example.com": _fixture_text(
                "gamedevmap_homepage_provider.html"
            ),
            "https://homepage-website-only.example.com": _fixture_text(
                "gamedevmap_homepage_no_jobs.html"
            ),
        }
        calls: list[str] = []

        def fake_fetch(url: str, _: int) -> str:
            calls.append(url)
            if url not in payloads:
                raise RuntimeError(f"unexpected URL: {url}")
            return payloads[url]

        provider_rows_1, static_rows_1, failures_1 = sd.discover_gamesmap_candidates(
            5, config=config, fetcher=fake_fetch
        )
        assert len(calls) > 0
        first_call_count = len(calls)

        with mock.patch(
            "src.source_discovery.gamesmap.fetch_directory_pages",
            side_effect=AssertionError("directory fetch helper should be bypassed on cache hit"),
        ):
            provider_rows_2, static_rows_2, failures_2 = sd.discover_gamesmap_candidates(
                5, config=config, fetcher=fake_fetch
            )
        assert len(calls) == first_call_count
        assert provider_rows_1 == provider_rows_2
        assert static_rows_1 == static_rows_2
        assert failures_1 == failures_2


def test_run_discovery_gamesmap_candidates_flow_into_report_and_queue() -> None:
    with workspace_tmpdir("source-discovery") as root:
        prev_paths = (
            sd.ACTIVE_PATH,
            sd.PENDING_PATH,
            sd.REJECTED_PATH,
            sd.DISCOVERY_CANDIDATES_PATH,
            sd.DISCOVERY_REPORT_PATH,
            sd.URL_PATCH_MANIFEST_PATH,
        )
        prev_static = list(sd.STATIC_DISCOVERY_CANDIDATES)
        prev_seeds = list(sd.STUDIO_SEEDS)
        try:
            sd.ACTIVE_PATH = root / "active.json"
            sd.PENDING_PATH = root / "pending.json"
            sd.REJECTED_PATH = root / "rejected.json"
            sd.DISCOVERY_CANDIDATES_PATH = root / "candidates.json"
            sd.DISCOVERY_REPORT_PATH = root / "report.json"
            sd.URL_PATCH_MANIFEST_PATH = root / "url-patch-manifest.json"
            sd.STUDIO_SEEDS = []
            sd.STATIC_DISCOVERY_CANDIDATES = []

            config = {
                "gamesmap": {
                    "enabled": True,
                    "baseUrl": "https://www.gamesmap.de",
                    "indexUrls": ["https://www.gamesmap.de/en"],
                    "websiteOnlyFallback": False,
                    "maxDetailPages": 10,
                    "allowedCategoryTokens": ["developer", "publisher", "pc", "console"],
                    "blockedCategoryTokens": ["association", "education"],
                },
                "gameprog": {
                    "enabled": False,
                },
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
            queued = json.loads(sd.DISCOVERY_CANDIDATES_PATH.read_text(encoding="utf-8"))
            assert len(queued) == 1
            assert str(queued[0].get("discoveryMethod") or "") == "gamesmap"
            assert str(queued[0].get("sourceDirectory") or "") == "gamesmap"
        finally:
            (
                sd.ACTIVE_PATH,
                sd.PENDING_PATH,
                sd.REJECTED_PATH,
                sd.DISCOVERY_CANDIDATES_PATH,
                sd.DISCOVERY_REPORT_PATH,
                sd.URL_PATCH_MANIFEST_PATH,
            ) = prev_paths
            sd.STATIC_DISCOVERY_CANDIDATES = prev_static
            sd.STUDIO_SEEDS = prev_seeds


def test_run_discovery_gamedevmap_candidates_flow_into_report_and_queue() -> None:
    with workspace_tmpdir("source-discovery-gamedevmap") as root:
        prev_paths = (
            sd.ACTIVE_PATH,
            sd.PENDING_PATH,
            sd.REJECTED_PATH,
            sd.DISCOVERY_CANDIDATES_PATH,
            sd.DISCOVERY_REPORT_PATH,
            sd.URL_PATCH_MANIFEST_PATH,
        )
        prev_static = list(sd.STATIC_DISCOVERY_CANDIDATES)
        prev_seeds = list(sd.STUDIO_SEEDS)
        try:
            sd.ACTIVE_PATH = root / "active.json"
            sd.PENDING_PATH = root / "pending.json"
            sd.REJECTED_PATH = root / "rejected.json"
            sd.DISCOVERY_CANDIDATES_PATH = root / "candidates.json"
            sd.DISCOVERY_REPORT_PATH = root / "report.json"
            sd.URL_PATCH_MANIFEST_PATH = root / "url-patch-manifest.json"
            sd.STUDIO_SEEDS = []
            sd.STATIC_DISCOVERY_CANDIDATES = []

            config = {
                "gamesmap": {"enabled": False},
                "gameprog": {"enabled": False},
                "gamedevmap": {
                    "enabled": True,
                    "csvUrl": "https://www.gamedevmap.com/cmsdata/gamedevmapdata.csv",
                    "indexUrl": "https://www.gamedevmap.com/index.php",
                    "allowedCategories": ["Developer"],
                    "blockedCategories": ["Organization"],
                    "maxRows": 0,
                    "maxHomepageFetches": 0,
                },
            }
            payloads = {
                "https://www.gamedevmap.com/cmsdata/gamedevmapdata.csv": _fixture_text(
                    "gamedevmap_data.csv"
                ),
                "https://boards-api.greenhouse.io/v1/boards/providerfeedstudio/jobs?content=true": json.dumps(
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
            queued = json.loads(sd.DISCOVERY_CANDIDATES_PATH.read_text(encoding="utf-8"))
            assert len(queued) == 1
            assert str(queued[0].get("discoveryMethod") or "") == "gamedevmap"
            assert str(queued[0].get("sourceDirectory") or "") == "gamedevmap"
        finally:
            (
                sd.ACTIVE_PATH,
                sd.PENDING_PATH,
                sd.REJECTED_PATH,
                sd.DISCOVERY_CANDIDATES_PATH,
                sd.DISCOVERY_REPORT_PATH,
                sd.URL_PATCH_MANIFEST_PATH,
            ) = prev_paths
            sd.STATIC_DISCOVERY_CANDIDATES = prev_static
            sd.STUDIO_SEEDS = prev_seeds


def test_run_discovery_dynamic_tracks_stage_metrics_and_queue_contract() -> None:
    with workspace_tmpdir("source-discovery") as root:
        prev_paths = (
            sd.ACTIVE_PATH,
            sd.PENDING_PATH,
            sd.REJECTED_PATH,
            sd.DISCOVERY_CANDIDATES_PATH,
            sd.DISCOVERY_REPORT_PATH,
            sd.URL_PATCH_MANIFEST_PATH,
        )
        prev_static = list(sd.STATIC_DISCOVERY_CANDIDATES)
        prev_seeds = list(sd.STUDIO_SEEDS)
        try:
            sd.ACTIVE_PATH = root / "active.json"
            sd.PENDING_PATH = root / "pending.json"
            sd.REJECTED_PATH = root / "rejected.json"
            sd.DISCOVERY_CANDIDATES_PATH = root / "candidates.json"
            sd.DISCOVERY_REPORT_PATH = root / "report.json"
            sd.URL_PATCH_MANIFEST_PATH = root / "url-patch-manifest.json"
            sd.STUDIO_SEEDS = []
            sd.STATIC_DISCOVERY_CANDIDATES = [
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
            ]

            def fake_fetch(url: str, _: int) -> str:
                if "api.lever.co" in url:
                    return json.dumps([{"id": 1}, {"id": 2}, {"id": 3}])
                if "boards-api.greenhouse.io" in url:
                    return json.dumps({"jobs": [{}, {}]})
                raise RuntimeError(f"unexpected URL: {url}")

            report = sd.run_discovery(
                timeout_s=5,
                top_n=0,
                mode="dynamic",
                include_web_search=False,
                discovery_config={"gamesmap": {"enabled": False}, "gameprog": {"enabled": False}},
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

            queued = json.loads(sd.DISCOVERY_CANDIDATES_PATH.read_text(encoding="utf-8"))
            assert len(queued) == 2
            for row in queued:
                assert "evidenceScore" in row
                assert "evidenceTypes" in row
                assert "discoveryStage" in row
                assert not (bool(row.get("deferred")))
        finally:
            (
                sd.ACTIVE_PATH,
                sd.PENDING_PATH,
                sd.REJECTED_PATH,
                sd.DISCOVERY_CANDIDATES_PATH,
                sd.DISCOVERY_REPORT_PATH,
                sd.URL_PATCH_MANIFEST_PATH,
            ) = prev_paths
            sd.STATIC_DISCOVERY_CANDIDATES = prev_static
            sd.STUDIO_SEEDS = prev_seeds


def test_run_discovery_emits_phase_logs_for_candidate_generation() -> None:
    with workspace_tmpdir("source-discovery") as root:
        prev_paths = (
            sd.ACTIVE_PATH,
            sd.PENDING_PATH,
            sd.REJECTED_PATH,
            sd.DISCOVERY_CANDIDATES_PATH,
            sd.DISCOVERY_REPORT_PATH,
            sd.URL_PATCH_MANIFEST_PATH,
        )
        prev_static = list(sd.STATIC_DISCOVERY_CANDIDATES)
        prev_seeds = list(sd.STUDIO_SEEDS)
        try:
            sd.ACTIVE_PATH = root / "active.json"
            sd.PENDING_PATH = root / "pending.json"
            sd.REJECTED_PATH = root / "rejected.json"
            sd.DISCOVERY_CANDIDATES_PATH = root / "candidates.json"
            sd.DISCOVERY_REPORT_PATH = root / "report.json"
            sd.URL_PATCH_MANIFEST_PATH = root / "url-patch-manifest.json"
            sd.STUDIO_SEEDS = []
            sd.STATIC_DISCOVERY_CANDIDATES = []

            with mock.patch.object(sd, "emit_log") as emit_log_mock:
                report = sd.run_discovery(
                    timeout_s=5,
                    top_n=0,
                    mode="dynamic",
                    include_web_search=False,
                    discovery_config={
                        "gamesmap": {"enabled": False},
                        "gameprog": {"enabled": False},
                    },
                    fetcher=lambda *_: "",
                )

            messages = [str(call.args[0]) for call in emit_log_mock.call_args_list if call.args]
            assert any("Generating curated seed candidates" in message for message in messages)
            assert any("Generating provider-pattern candidates" in message for message in messages)
            assert any("Scanning known careers pages" in message for message in messages)
            assert any("Starting probe phase" in message for message in messages)
            assert str((report.get("summary") or {}).get("phase") or "") == "completed"
        finally:
            (
                sd.ACTIVE_PATH,
                sd.PENDING_PATH,
                sd.REJECTED_PATH,
                sd.DISCOVERY_CANDIDATES_PATH,
                sd.DISCOVERY_REPORT_PATH,
                sd.URL_PATCH_MANIFEST_PATH,
            ) = prev_paths
            sd.STATIC_DISCOVERY_CANDIDATES = prev_static
            sd.STUDIO_SEEDS = prev_seeds


def test_run_discovery_deduplicates_duplicate_endpoints_and_stale_pending_rows() -> None:
    cases = [
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
                prev_paths = (
                    sd.ACTIVE_PATH,
                    sd.PENDING_PATH,
                    sd.REJECTED_PATH,
                    sd.DISCOVERY_CANDIDATES_PATH,
                    sd.DISCOVERY_REPORT_PATH,
                    sd.URL_PATCH_MANIFEST_PATH,
                )
                prev_static = list(sd.STATIC_DISCOVERY_CANDIDATES)
                prev_seeds = list(sd.STUDIO_SEEDS)
                try:
                    sd.ACTIVE_PATH = root / "active.json"
                    sd.PENDING_PATH = root / "pending.json"
                    sd.REJECTED_PATH = root / "rejected.json"
                    sd.DISCOVERY_CANDIDATES_PATH = root / "candidates.json"
                    sd.DISCOVERY_REPORT_PATH = root / "report.json"
                    sd.STUDIO_SEEDS = []
                    sd.STATIC_DISCOVERY_CANDIDATES = case["setup"]["static"]
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
                finally:
                    (
                        sd.ACTIVE_PATH,
                        sd.PENDING_PATH,
                        sd.REJECTED_PATH,
                        sd.DISCOVERY_CANDIDATES_PATH,
                        sd.DISCOVERY_REPORT_PATH,
                        sd.URL_PATCH_MANIFEST_PATH,
                    ) = prev_paths
                    sd.STATIC_DISCOVERY_CANDIDATES = prev_static
                    sd.STUDIO_SEEDS = prev_seeds
        else:
            merged = sr.unique_sources(case["rows"])
            assert len(merged) == case["expected_len"], case["name"]
            assert merged[0]["id"] == case["expected_id"], case["name"]
            assert int(merged[0]["jobsFound"] or 0) == case["expected_jobs_found"], case["name"]
            assert int(merged[0]["sampleCount"] or 0) == case["expected_sample_count"], case["name"]


def test_run_discovery_balances_queue_with_deferrals() -> None:
    with workspace_tmpdir("source-discovery") as root:
        prev_paths = (
            sd.ACTIVE_PATH,
            sd.PENDING_PATH,
            sd.REJECTED_PATH,
            sd.DISCOVERY_CANDIDATES_PATH,
            sd.DISCOVERY_REPORT_PATH,
            sd.URL_PATCH_MANIFEST_PATH,
        )
        prev_static = list(sd.STATIC_DISCOVERY_CANDIDATES)
        prev_seeds = list(sd.STUDIO_SEEDS)
        prev_caps = dict(sd.ADAPTER_QUEUE_CAPS)
        try:
            sd.ACTIVE_PATH = root / "active.json"
            sd.PENDING_PATH = root / "pending.json"
            sd.REJECTED_PATH = root / "rejected.json"
            sd.DISCOVERY_CANDIDATES_PATH = root / "candidates.json"
            sd.DISCOVERY_REPORT_PATH = root / "report.json"
            sd.URL_PATCH_MANIFEST_PATH = root / "url-patch-manifest.json"
            sd.STUDIO_SEEDS = []
            sd.ADAPTER_QUEUE_CAPS["lever"] = 1
            sd.STATIC_DISCOVERY_CANDIDATES = [
                {
                    "name": "Demo Lever A",
                    "studio": "Demo A",
                    "adapter": "lever",
                    "account": "demoa",
                    "api_url": "https://api.lever.co/v0/postings/demoa?mode=json",
                },
                {
                    "name": "Demo Lever B",
                    "studio": "Demo B",
                    "adapter": "lever",
                    "account": "demob",
                    "api_url": "https://api.lever.co/v0/postings/demob?mode=json",
                },
            ]

            report = sd.run_discovery(
                timeout_s=5,
                top_n=0,
                mode="dynamic",
                include_web_search=False,
                fetcher=lambda *_: json.dumps([{"id": 1}, {"id": 2}]),
            )
            assert int(report["summary"].get("queuedCandidateCount") or 0) == 1
            assert int(report["summary"].get("discoverableButDeferredCount") or 0) == 1
            assert (
                int((report["summary"].get("lossAccounting") or {}).get("deferredByCap") or 0) == 1
            )
            deferred = [
                row for row in (report.get("candidates") or []) if bool(row.get("deferred"))
            ]
            assert len(deferred) == 1
            assert str(deferred[0].get("deferReason") or "") == "adapter_cap"
            assert str(deferred[0].get("dropStage") or "") == "deferred_by_cap"
            assert str(deferred[0].get("dropReason") or "") == "adapter_cap"
        finally:
            (
                sd.ACTIVE_PATH,
                sd.PENDING_PATH,
                sd.REJECTED_PATH,
                sd.DISCOVERY_CANDIDATES_PATH,
                sd.DISCOVERY_REPORT_PATH,
                sd.URL_PATCH_MANIFEST_PATH,
            ) = prev_paths
            sd.STATIC_DISCOVERY_CANDIDATES = prev_static
            sd.STUDIO_SEEDS = prev_seeds
            sd.ADAPTER_QUEUE_CAPS.clear()
            sd.ADAPTER_QUEUE_CAPS.update(prev_caps)


def test_run_discovery_pattern_candidates_below_reinforced_threshold_are_skipped() -> None:
    with workspace_tmpdir("source-discovery") as root:
        prev_paths = (
            sd.ACTIVE_PATH,
            sd.PENDING_PATH,
            sd.REJECTED_PATH,
            sd.DISCOVERY_CANDIDATES_PATH,
            sd.DISCOVERY_REPORT_PATH,
            sd.URL_PATCH_MANIFEST_PATH,
        )
        prev_static = list(sd.STATIC_DISCOVERY_CANDIDATES)
        prev_seeds = list(sd.STUDIO_SEEDS)
        try:
            sd.ACTIVE_PATH = root / "active.json"
            sd.PENDING_PATH = root / "pending.json"
            sd.REJECTED_PATH = root / "rejected.json"
            sd.DISCOVERY_CANDIDATES_PATH = root / "candidates.json"
            sd.DISCOVERY_REPORT_PATH = root / "report.json"
            sd.STATIC_DISCOVERY_CANDIDATES = []
            sd.STUDIO_SEEDS = [
                {
                    "studio": "Example Studio",
                    "aliases": ["example-studio"],
                    "nlPriority": False,
                    "likelyProviders": ["teamtailor"],
                    "careersUrl": "https://example.com/careers",
                }
            ]
            report = sd.run_discovery(
                timeout_s=5,
                top_n=0,
                mode="dynamic",
                include_web_search=False,
                discovery_config={"thresholds": {"patternProviderProbeThreshold": 32}},
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
        finally:
            (
                sd.ACTIVE_PATH,
                sd.PENDING_PATH,
                sd.REJECTED_PATH,
                sd.DISCOVERY_CANDIDATES_PATH,
                sd.DISCOVERY_REPORT_PATH,
                sd.URL_PATCH_MANIFEST_PATH,
            ) = prev_paths
            sd.STATIC_DISCOVERY_CANDIDATES = prev_static
            sd.STUDIO_SEEDS = prev_seeds


def test_run_discovery_tracks_probe_miss_separately_from_failures() -> None:
    with workspace_tmpdir("source-discovery") as root:
        prev_paths = (
            sd.ACTIVE_PATH,
            sd.PENDING_PATH,
            sd.REJECTED_PATH,
            sd.DISCOVERY_CANDIDATES_PATH,
            sd.DISCOVERY_REPORT_PATH,
            sd.URL_PATCH_MANIFEST_PATH,
        )
        prev_static = list(sd.STATIC_DISCOVERY_CANDIDATES)
        prev_seeds = list(sd.STUDIO_SEEDS)
        try:
            sd.ACTIVE_PATH = root / "active.json"
            sd.PENDING_PATH = root / "pending.json"
            sd.REJECTED_PATH = root / "rejected.json"
            sd.DISCOVERY_CANDIDATES_PATH = root / "candidates.json"
            sd.DISCOVERY_REPORT_PATH = root / "report.json"
            sd.URL_PATCH_MANIFEST_PATH = root / "url-patch-manifest.json"
            sd.STUDIO_SEEDS = []
            sd.STATIC_DISCOVERY_CANDIDATES = [
                {
                    "name": "Demo Lever",
                    "studio": "Demo",
                    "adapter": "lever",
                    "account": "demo",
                    "api_url": "https://api.lever.co/v0/postings/demo?mode=json",
                }
            ]
            report = sd.run_discovery(
                timeout_s=5,
                top_n=0,
                mode="dynamic",
                include_web_search=False,
                discovery_config={"gamesmap": {"enabled": False}, "gameprog": {"enabled": False}},
                fetcher=lambda *_a, **_k: (_ for _ in ()).throw(
                    RuntimeError("HTTP Error 404: Not Found")
                ),
            )
            assert int(report["summary"].get("probedCandidateCount") or 0) == 1
            assert int(report["summary"].get("failedProbeCount") or 0) == 0
            assert int(report["summary"].get("probeMissCount") or 0) == 1
            assert str((report.get("failures") or [])[0].get("stage") or "") == "probe_miss"
        finally:
            (
                sd.ACTIVE_PATH,
                sd.PENDING_PATH,
                sd.REJECTED_PATH,
                sd.DISCOVERY_CANDIDATES_PATH,
                sd.DISCOVERY_REPORT_PATH,
                sd.URL_PATCH_MANIFEST_PATH,
            ) = prev_paths
            sd.STATIC_DISCOVERY_CANDIDATES = prev_static
            sd.STUDIO_SEEDS = prev_seeds


def test_run_discovery_uses_seed_careers_pages_without_web_search() -> None:
    with workspace_tmpdir("source-discovery") as root:
        prev_paths = (
            sd.ACTIVE_PATH,
            sd.PENDING_PATH,
            sd.REJECTED_PATH,
            sd.DISCOVERY_CANDIDATES_PATH,
            sd.DISCOVERY_REPORT_PATH,
            sd.URL_PATCH_MANIFEST_PATH,
        )
        prev_static = list(sd.STATIC_DISCOVERY_CANDIDATES)
        prev_seeds = list(sd.STUDIO_SEEDS)
        try:
            sd.ACTIVE_PATH = root / "active.json"
            sd.PENDING_PATH = root / "pending.json"
            sd.REJECTED_PATH = root / "rejected.json"
            sd.DISCOVERY_CANDIDATES_PATH = root / "candidates.json"
            sd.DISCOVERY_REPORT_PATH = root / "report.json"
            sd.STATIC_DISCOVERY_CANDIDATES = []
            sd.STUDIO_SEEDS = [
                {
                    "studio": "Example Studio",
                    "aliases": ["example-studio"],
                    "nlPriority": False,
                    "likelyProviders": ["teamtailor"],
                    "careersUrl": "https://example.com/careers",
                }
            ]

            def fake_fetch(url: str, _: int) -> str:
                if url == "https://example.com/careers":
                    return '<a href="https://boards.greenhouse.io/example-studio/jobs/123">Job</a>'
                if "boards-api.greenhouse.io" in url:
                    return json.dumps({"jobs": [{}, {}]})
                raise RuntimeError(f"unexpected URL: {url}")

            report = sd.run_discovery(
                timeout_s=5,
                top_n=0,
                mode="dynamic",
                include_web_search=False,
                discovery_config={"gamesmap": {"enabled": False}, "gameprog": {"enabled": False}},
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
            queued = json.loads(sd.DISCOVERY_CANDIDATES_PATH.read_text(encoding="utf-8"))
            assert len(queued) == 1
            assert str(queued[0].get("discoveryMethod") or "") == "seed_careers_page"
        finally:
            (
                sd.ACTIVE_PATH,
                sd.PENDING_PATH,
                sd.REJECTED_PATH,
                sd.DISCOVERY_CANDIDATES_PATH,
                sd.DISCOVERY_REPORT_PATH,
                sd.URL_PATCH_MANIFEST_PATH,
            ) = prev_paths
            sd.STATIC_DISCOVERY_CANDIDATES = prev_static
            sd.STUDIO_SEEDS = prev_seeds


def test_discovery_report_snapshot_contract() -> None:
    with workspace_tmpdir("source-discovery") as root:
        prev_paths = (
            sd.ACTIVE_PATH,
            sd.PENDING_PATH,
            sd.REJECTED_PATH,
            sd.DISCOVERY_CANDIDATES_PATH,
            sd.DISCOVERY_REPORT_PATH,
            sd.URL_PATCH_MANIFEST_PATH,
        )
        prev_static = list(sd.STATIC_DISCOVERY_CANDIDATES)
        prev_seeds = list(sd.STUDIO_SEEDS)
        try:
            sd.ACTIVE_PATH = root / "active.json"
            sd.PENDING_PATH = root / "pending.json"
            sd.REJECTED_PATH = root / "rejected.json"
            sd.DISCOVERY_CANDIDATES_PATH = root / "candidates.json"
            sd.DISCOVERY_REPORT_PATH = root / "report.json"
            sd.URL_PATCH_MANIFEST_PATH = root / "url-patch-manifest.json"
            sd.STUDIO_SEEDS = []
            sd.STATIC_DISCOVERY_CANDIDATES = [
                {
                    "name": "Demo Lever",
                    "studio": "Demo",
                    "adapter": "lever",
                    "account": "demo",
                    "api_url": "https://api.lever.co/v0/postings/demo?mode=json",
                },
                {
                    "name": "Demo Greenhouse",
                    "studio": "Demo",
                    "adapter": "greenhouse",
                    "slug": "demo",
                    "api_url": "https://boards-api.greenhouse.io/v1/boards/demo/jobs?content=true",
                },
            ]

            def fake_fetch(url: str, _: int) -> str:
                if "api.lever.co" in url:
                    return json.dumps([{"id": 1}, {"id": 2}])
                if "boards-api.greenhouse.io" in url:
                    return json.dumps({"jobs": [{}]})
                raise RuntimeError(f"unexpected URL: {url}")

            report = sd.run_discovery(
                timeout_s=5,
                top_n=0,
                mode="dynamic",
                include_web_search=False,
                discovery_config={"gamesmap": {"enabled": False}, "gameprog": {"enabled": False}},
                fetcher=fake_fetch,
            )
            DiscoveryReportSummarySchema.model_validate(report["summary"])
            snapshot = {
                "schemaVersion": report.get("schemaVersion"),
                "mode": str(report.get("mode")),
                "summary": {
                    "foundEndpointCount": int(report["summary"].get("foundEndpointCount") or 0),
                    "probedCandidateCount": int(report["summary"].get("probedCandidateCount") or 0),
                    "queuedCandidateCount": int(report["summary"].get("queuedCandidateCount") or 0),
                    "discoverableButDeferredCount": int(
                        report["summary"].get("discoverableButDeferredCount") or 0
                    ),
                    "failedProbeCount": int(report["summary"].get("failedProbeCount") or 0),
                },
                "counts": {
                    "candidates": len(report.get("candidates") or []),
                    "failures": len(report.get("failures") or []),
                },
                "adapterCounts": report["summary"].get("adapterCounts") or {},
                "methodCounts": report["summary"].get("methodCounts") or {},
                "generatedCountByStage": report["summary"].get("generatedCountByStage") or {},
            }
            assert snapshot == _fixture_json("source_discovery_report_snapshot.json")
        finally:
            (
                sd.ACTIVE_PATH,
                sd.PENDING_PATH,
                sd.REJECTED_PATH,
                sd.DISCOVERY_CANDIDATES_PATH,
                sd.DISCOVERY_REPORT_PATH,
                sd.URL_PATCH_MANIFEST_PATH,
            ) = prev_paths
            sd.STATIC_DISCOVERY_CANDIDATES = prev_static
            sd.STUDIO_SEEDS = prev_seeds


def test_run_discovery_writes_phase_progress_before_probe() -> None:
    with workspace_tmpdir("source-discovery") as root:
        prev_paths = (
            sd.ACTIVE_PATH,
            sd.PENDING_PATH,
            sd.REJECTED_PATH,
            sd.DISCOVERY_CANDIDATES_PATH,
            sd.DISCOVERY_REPORT_PATH,
            sd.URL_PATCH_MANIFEST_PATH,
        )
        prev_static = list(sd.STATIC_DISCOVERY_CANDIDATES)
        prev_seeds = list(sd.STUDIO_SEEDS)
        saved_reports = []
        original_save_json_atomic = discovery_orchestrator.save_json_atomic

        def capture_save(path, payload):
            if Path(path) == sd.DISCOVERY_REPORT_PATH and isinstance(payload, dict):
                saved_reports.append(payload)
            original_save_json_atomic(path, payload)

        try:
            sd.ACTIVE_PATH = root / "active.json"
            sd.PENDING_PATH = root / "pending.json"
            sd.REJECTED_PATH = root / "rejected.json"
            sd.DISCOVERY_CANDIDATES_PATH = root / "candidates.json"
            sd.DISCOVERY_REPORT_PATH = root / "report.json"
            sd.URL_PATCH_MANIFEST_PATH = root / "url-patch-manifest.json"
            sd.STUDIO_SEEDS = []
            sd.STATIC_DISCOVERY_CANDIDATES = []

            with mock.patch.object(
                discovery_orchestrator, "save_json_atomic", side_effect=capture_save
            ):
                report = sd.run_discovery(
                    timeout_s=5,
                    top_n=0,
                    mode="dynamic",
                    include_web_search=False,
                    discovery_config={
                        "gamesmap": {"enabled": False},
                        "gameprog": {"enabled": False},
                    },
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
        finally:
            (
                sd.ACTIVE_PATH,
                sd.PENDING_PATH,
                sd.REJECTED_PATH,
                sd.DISCOVERY_CANDIDATES_PATH,
                sd.DISCOVERY_REPORT_PATH,
                sd.URL_PATCH_MANIFEST_PATH,
            ) = prev_paths
            sd.STATIC_DISCOVERY_CANDIDATES = prev_static
            sd.STUDIO_SEEDS = prev_seeds


def test_discovery_report_write_path_prefers_baluffo_data_dir(monkeypatch, tmp_path: Path) -> None:
    data_dir = tmp_path / "desktop-data"
    data_dir.mkdir()
    monkeypatch.setenv("BALUFFO_DATA_DIR", str(data_dir))
    assert (
        discovery_orchestrator._discovery_report_write_path()
        == data_dir / "source-discovery-report.json"
    )


def test_discovery_report_write_path_prefers_bridge_spawn_env(monkeypatch, tmp_path: Path) -> None:
    """Bridge sets BALUFFO_DISCOVERY_REPORT_PATH so the worker updates the seeded file exactly."""
    explicit = tmp_path / "source-discovery-report.json"
    wrong_dir = tmp_path / "other-data"
    wrong_dir.mkdir()
    monkeypatch.setenv("BALUFFO_DISCOVERY_REPORT_PATH", str(explicit))
    monkeypatch.setenv("BALUFFO_DATA_DIR", str(wrong_dir))
    assert discovery_orchestrator._discovery_report_write_path() == explicit.resolve()


def test_parse_args_supports_manual_gamesmap_mode() -> None:
    prev_argv = list(sys.argv)
    try:
        sys.argv = [
            "source_discovery.py",
            "--gamesmap-website-only-fallback",
            "--gamesmap-max-detail-pages",
            "25",
        ]
        args = sd.parse_args()
    finally:
        sys.argv = prev_argv
    assert bool(args.gamesmap_website_only_fallback)
    assert int(args.gamesmap_max_detail_pages or 0) == 25


def test_parse_args_supports_gamedevmap_mode() -> None:
    prev_argv = list(sys.argv)
    try:
        sys.argv = [
            "source_discovery.py",
            "--gamedevmap-enabled",
            "--gamedevmap-max-rows",
            "40",
            "--gamedevmap-max-homepage-fetches",
            "12",
        ]
        args = sd.parse_args()
    finally:
        sys.argv = prev_argv
    assert bool(args.gamedevmap_enabled)
    assert int(args.gamedevmap_max_rows or 0) == 40
    assert int(args.gamedevmap_max_homepage_fetches or 0) == 12


def test_parse_args_supports_only_gamedevmap_mode() -> None:
    prev_argv = list(sys.argv)
    try:
        sys.argv = ["source_discovery.py", "--only-gamedevmap"]
        args = sd.parse_args()
    finally:
        sys.argv = prev_argv
    assert bool(args.only_gamedevmap)


def test_run_discovery_leaves_pending_only_rows_unapproved() -> None:
    with workspace_tmpdir("source-discovery-auto-approval") as root:
        prev_paths = (
            sd.ACTIVE_PATH,
            sd.PENDING_PATH,
            sd.REJECTED_PATH,
            sd.DISCOVERY_CANDIDATES_PATH,
            sd.DISCOVERY_REPORT_PATH,
            sd.M5_STRATEGIC_BACKLOG_PATH,
            sd.URL_PATCH_MANIFEST_PATH,
        )
        prev_static = list(sd.STATIC_DISCOVERY_CANDIDATES)
        prev_seeds = list(sd.STUDIO_SEEDS)
        prev_approval_state_path = discovery_orchestrator.DEFAULT_APPROVAL_STATE_PATH
        prev_sheet = discovery_orchestrator.discover_game_studio_sheet_candidates
        prev_gamesmap = discovery_orchestrator.discover_gamesmap_candidates
        prev_gameprog = discovery_orchestrator.discover_gameprog_candidates
        prev_web = discovery_orchestrator.discover_web_search_candidates
        prev_seed_scan = discovery_orchestrator.sd.discover_seed_careers_page_candidates
        prev_probe = discovery_orchestrator.async_probe_candidate
        try:
            sd.ACTIVE_PATH = root / "active.json"
            sd.PENDING_PATH = root / "pending.json"
            sd.REJECTED_PATH = root / "rejected.json"
            sd.DISCOVERY_CANDIDATES_PATH = root / "candidates.json"
            sd.DISCOVERY_REPORT_PATH = root / "report.json"
            sd.M5_STRATEGIC_BACKLOG_PATH = root / "m5.json"
            sd.URL_PATCH_MANIFEST_PATH = root / "url-patch-manifest.json"
            discovery_orchestrator.DEFAULT_APPROVAL_STATE_PATH = root / "source-approval-state.json"
            sd.STUDIO_SEEDS = []
            sd.STATIC_DISCOVERY_CANDIDATES = []
            sr.save_json_atomic(sd.ACTIVE_PATH, [])
            sr.save_json_atomic(
                sd.PENDING_PATH,
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
            sr.save_json_atomic(sd.REJECTED_PATH, [])

            discovery_orchestrator.discover_game_studio_sheet_candidates = lambda *args, **kwargs: (
                [],
                [],
                [],
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
            discovery_orchestrator.discover_web_search_candidates = lambda *args, **kwargs: (
                [],
                [],
                [],
            )
            discovery_orchestrator.sd.discover_seed_careers_page_candidates = (
                lambda *args, **kwargs: ([], [], [])
            )
            discovery_orchestrator.async_probe_candidate = lambda *args, **kwargs: (
                False,
                0,
                "",
            )

            report = discovery_orchestrator.run_discovery(
                timeout_s=1,
                top_n=0,
                preset="uncapped",
                mode="dynamic",
                include_web_search=False,
                discovery_config={
                    "autoApproveHealthyPendingOnComplete": True,
                    "gamesmap": {"enabled": False},
                    "gameprog": {"enabled": False},
                },
                fetcher=lambda *args, **kwargs: "",
            )

            assert int((report.get("summary") or {}).get("approvedCandidateCount") or 0) == 1
            assert int((report.get("summary") or {}).get("liveCandidateCount") or 0) == 1
            assert (
                int(
                    (((report.get("runtime") or {}).get("autoApproval") or {}).get("approvedCount"))
                    or 0
                )
                == 1
            )
            active = json.loads(sd.ACTIVE_PATH.read_text(encoding="utf-8"))
            pending = json.loads(sd.PENDING_PATH.read_text(encoding="utf-8"))
            assert [row["id"] for row in active] == ["pending-ok"]
            assert bool(active[0].get("weakSignal"))
            assert pending == []
            assert json.loads(
                (root / "source-approval-state.json").read_text(encoding="utf-8")
            ) == {"approvedSinceLastRun": 1}
        finally:
            (
                sd.ACTIVE_PATH,
                sd.PENDING_PATH,
                sd.REJECTED_PATH,
                sd.DISCOVERY_CANDIDATES_PATH,
                sd.DISCOVERY_REPORT_PATH,
                sd.M5_STRATEGIC_BACKLOG_PATH,
                sd.URL_PATCH_MANIFEST_PATH,
            ) = prev_paths
            discovery_orchestrator.DEFAULT_APPROVAL_STATE_PATH = prev_approval_state_path
            sd.STATIC_DISCOVERY_CANDIDATES = prev_static
            sd.STUDIO_SEEDS = prev_seeds
            discovery_orchestrator.discover_game_studio_sheet_candidates = prev_sheet
            discovery_orchestrator.discover_gamesmap_candidates = prev_gamesmap
            discovery_orchestrator.discover_gameprog_candidates = prev_gameprog
            discovery_orchestrator.discover_web_search_candidates = prev_web
            discovery_orchestrator.sd.discover_seed_careers_page_candidates = prev_seed_scan
            discovery_orchestrator.async_probe_candidate = prev_probe


def test_parse_game_studio_sheet_csv_returns_expected_keys() -> None:
    """Health check: parsed rows must have studio, careersUrl, openingsFlag (game-studios-sheet contract)."""
    csv_text = """,,,,
,Studio,Hiring Location,Roles open,Link
,Acme Games,Remote,yes,https://example.com/careers
"""
    rows = sd.parse_game_studio_sheet_csv(csv_text)
    assert len(rows) >= 1
    for row in rows:
        assert "studio" in row
        assert "careersUrl" in row
        assert "openingsFlag" in row
        assert row["careersUrl"].startswith("http")


def test_parse_game_studio_sheet_csv_handles_metadata_rows_and_openings_flag() -> None:
    csv_text = """,,,,
,Studios Hiring now,,,Last update: 18 Feb 2026
,, ,,
,Studio,Hiring Location,Roles open (as of 18 Feb),Link
,Example Studio,Remote,yes,https://boards.greenhouse.io/example
,Example Studio 2,Remote,no,https://jobs.lever.co/example2
"""
    rows = sd.parse_game_studio_sheet_csv(csv_text)
    assert len(rows) == 2
    assert rows[0]["studio"] == "Example Studio"
    assert rows[0]["careersUrl"] == "https://boards.greenhouse.io/example"
    assert rows[0]["openingsFlag"] == "yes"
    assert rows[1]["openingsFlag"] == "no"


def test_discover_game_studio_sheet_candidates_reports_parse_failure_when_csv_empty_parse() -> None:
    """When CSV is non-empty but no rows are parsed, discovery returns a directory_parse failure."""
    csv_with_wrong_header = "Column A,Column B,Column C\nx,y,z\n"
    payloads = {
        sd.game_studios_sheet_candidate_urls(sd.GAME_STUDIOS_SHEET_ID, sd.GAME_STUDIOS_SHEET_GID)[
            0
        ]: csv_with_wrong_header,
    }

    def fake_fetch(url: str, _: int) -> str:
        if url not in payloads:
            raise RuntimeError(f"unexpected URL: {url}")
        return payloads[url]

    provider, static, failures = sd.discover_game_studio_sheet_candidates(5, fetcher=fake_fetch)
    assert provider == []
    assert static == []
    assert len(failures) == 1
    assert str(failures[0].get("adapter")) == "sheet_directory"
    assert str(failures[0].get("stage")) == "directory_parse"
    assert "no rows parsed" in str(failures[0].get("error"))


def test_run_discovery_sheet_directory_candidates_flow_into_queue() -> None:
    with workspace_tmpdir("source-discovery") as root:
        prev_paths = (
            sd.ACTIVE_PATH,
            sd.PENDING_PATH,
            sd.REJECTED_PATH,
            sd.DISCOVERY_CANDIDATES_PATH,
            sd.DISCOVERY_REPORT_PATH,
            sd.URL_PATCH_MANIFEST_PATH,
        )
        prev_static = list(sd.STATIC_DISCOVERY_CANDIDATES)
        prev_seeds = list(sd.STUDIO_SEEDS)
        prev_sheet = (sd.GAME_STUDIOS_SHEET_ID, sd.GAME_STUDIOS_SHEET_GID)
        try:
            sd.ACTIVE_PATH = root / "active.json"
            sd.PENDING_PATH = root / "pending.json"
            sd.REJECTED_PATH = root / "rejected.json"
            sd.DISCOVERY_CANDIDATES_PATH = root / "candidates.json"
            sd.DISCOVERY_REPORT_PATH = root / "report.json"
            sd.URL_PATCH_MANIFEST_PATH = root / "url-patch-manifest.json"
            sd.STUDIO_SEEDS = []
            sd.STATIC_DISCOVERY_CANDIDATES = []
            sd.GAME_STUDIOS_SHEET_ID = "sheet_test"
            sd.GAME_STUDIOS_SHEET_GID = "1"

            sheet_url = sd.game_studios_sheet_candidate_urls(
                sd.GAME_STUDIOS_SHEET_ID, sd.GAME_STUDIOS_SHEET_GID
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
                discovery_config={"gamesmap": {"enabled": False}, "gameprog": {"enabled": False}},
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
            queued = json.loads(sd.DISCOVERY_CANDIDATES_PATH.read_text(encoding="utf-8"))
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
        finally:
            (
                sd.ACTIVE_PATH,
                sd.PENDING_PATH,
                sd.REJECTED_PATH,
                sd.DISCOVERY_CANDIDATES_PATH,
                sd.DISCOVERY_REPORT_PATH,
                sd.URL_PATCH_MANIFEST_PATH,
            ) = prev_paths
            sd.STATIC_DISCOVERY_CANDIDATES = prev_static
            sd.STUDIO_SEEDS = prev_seeds
            sd.GAME_STUDIOS_SHEET_ID, sd.GAME_STUDIOS_SHEET_GID = prev_sheet


def test_run_discovery_applies_existing_url_patches_before_probe() -> None:
    with workspace_tmpdir("source-discovery") as root:
        prev_paths = (
            sd.ACTIVE_PATH,
            sd.PENDING_PATH,
            sd.REJECTED_PATH,
            sd.DISCOVERY_CANDIDATES_PATH,
            sd.DISCOVERY_REPORT_PATH,
            sd.URL_PATCH_MANIFEST_PATH,
        )
        prev_static = list(sd.STATIC_DISCOVERY_CANDIDATES)
        prev_seeds = list(sd.STUDIO_SEEDS)
        try:
            sd.ACTIVE_PATH = root / "active.json"
            sd.PENDING_PATH = root / "pending.json"
            sd.REJECTED_PATH = root / "rejected.json"
            sd.DISCOVERY_CANDIDATES_PATH = root / "candidates.json"
            sd.DISCOVERY_REPORT_PATH = root / "report.json"
            sd.URL_PATCH_MANIFEST_PATH = root / "url-patch-manifest.json"
            discovery_url_patches.save_url_patch_manifest(
                {"https://old.example/jobs": "https://new.example/jobs"},
                path=sd.URL_PATCH_MANIFEST_PATH,
                added=1,
                updated=0,
                reprobed=0,
            )
            sd.STUDIO_SEEDS = []
            sd.STATIC_DISCOVERY_CANDIDATES = [
                {
                    "name": "Patched Static",
                    "studio": "Patched Static",
                    "adapter": "static",
                    "listing_url": "https://old.example/jobs",
                    "pages": ["https://old.example/jobs"],
                    "evidenceScore": 52,
                    "evidenceTypes": ["seed_curated"],
                }
            ]

            seen_urls = []

            def fake_fetch(url: str, _timeout: int) -> str:
                seen_urls.append(url)
                if url == "https://new.example/jobs":
                    return '<a href="https://new.example/jobs/role-1">Role</a>'
                raise RuntimeError(f"unexpected URL: {url}")

            report = sd.run_discovery(
                timeout_s=5,
                top_n=0,
                mode="dynamic",
                include_web_search=False,
                discovery_config={"gamesmap": {"enabled": False}, "gameprog": {"enabled": False}},
                fetcher=fake_fetch,
            )
            assert "https://old.example/jobs" not in seen_urls
            assert report["summary"]["queuedCandidateCount"] == 1
            assert report["runtime"]["urlPatchStats"]["loaded"] == 1
            queued = json.loads(sd.DISCOVERY_CANDIDATES_PATH.read_text(encoding="utf-8"))
            assert queued[0]["listing_url"] == "https://new.example/jobs"
        finally:
            (
                sd.ACTIVE_PATH,
                sd.PENDING_PATH,
                sd.REJECTED_PATH,
                sd.DISCOVERY_CANDIDATES_PATH,
                sd.DISCOVERY_REPORT_PATH,
                sd.URL_PATCH_MANIFEST_PATH,
            ) = prev_paths
            sd.STATIC_DISCOVERY_CANDIDATES = prev_static
            sd.STUDIO_SEEDS = prev_seeds


def test_run_discovery_suppresses_blocked_static_domains_before_probe() -> None:
    with workspace_tmpdir("source-discovery") as root:
        prev_paths = (
            sd.ACTIVE_PATH,
            sd.PENDING_PATH,
            sd.REJECTED_PATH,
            sd.DISCOVERY_CANDIDATES_PATH,
            sd.DISCOVERY_REPORT_PATH,
            sd.URL_PATCH_MANIFEST_PATH,
        )
        prev_static = list(sd.STATIC_DISCOVERY_CANDIDATES)
        prev_seeds = list(sd.STUDIO_SEEDS)
        try:
            sd.ACTIVE_PATH = root / "active.json"
            sd.PENDING_PATH = root / "pending.json"
            sd.REJECTED_PATH = root / "rejected.json"
            sd.DISCOVERY_CANDIDATES_PATH = root / "candidates.json"
            sd.DISCOVERY_REPORT_PATH = root / "report.json"
            sd.URL_PATCH_MANIFEST_PATH = root / "url-patch-manifest.json"
            sd.STUDIO_SEEDS = []
            sd.STATIC_DISCOVERY_CANDIDATES = [
                {
                    "name": "Blocked Static",
                    "studio": "Blocked Static",
                    "adapter": "static",
                    "listing_url": "https://www.linkedin.com/company/example/jobs/",
                    "pages": ["https://www.linkedin.com/company/example/jobs/"],
                    "evidenceScore": 52,
                    "evidenceTypes": ["seed_curated"],
                }
            ]
            calls = []
            report = sd.run_discovery(
                timeout_s=5,
                top_n=0,
                mode="dynamic",
                include_web_search=False,
                discovery_config={"gamesmap": {"enabled": False}, "gameprog": {"enabled": False}},
                fetcher=lambda *args, **kwargs: calls.append((args, kwargs)) or "",
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
        finally:
            (
                sd.ACTIVE_PATH,
                sd.PENDING_PATH,
                sd.REJECTED_PATH,
                sd.DISCOVERY_CANDIDATES_PATH,
                sd.DISCOVERY_REPORT_PATH,
                sd.URL_PATCH_MANIFEST_PATH,
            ) = prev_paths
            sd.STATIC_DISCOVERY_CANDIDATES = prev_static
            sd.STUDIO_SEEDS = prev_seeds


def test_run_discovery_refreshes_url_patches_and_reprobes_candidate() -> None:
    with workspace_tmpdir("source-discovery") as root:
        prev_paths = (
            sd.ACTIVE_PATH,
            sd.PENDING_PATH,
            sd.REJECTED_PATH,
            sd.DISCOVERY_CANDIDATES_PATH,
            sd.DISCOVERY_REPORT_PATH,
            sd.URL_PATCH_MANIFEST_PATH,
        )
        prev_static = list(sd.STATIC_DISCOVERY_CANDIDATES)
        prev_seeds = list(sd.STUDIO_SEEDS)
        try:
            sd.ACTIVE_PATH = root / "active.json"
            sd.PENDING_PATH = root / "pending.json"
            sd.REJECTED_PATH = root / "rejected.json"
            sd.DISCOVERY_CANDIDATES_PATH = root / "candidates.json"
            sd.DISCOVERY_REPORT_PATH = root / "report.json"
            sd.URL_PATCH_MANIFEST_PATH = root / "url-patch-manifest.json"
            sd.STUDIO_SEEDS = []
            sd.STATIC_DISCOVERY_CANDIDATES = [
                {
                    "name": "Recoverable Static",
                    "studio": "Recoverable Static",
                    "adapter": "static",
                    "listing_url": "https://old.example/jobs",
                    "pages": ["https://old.example/jobs"],
                    "evidenceScore": 52,
                    "evidenceTypes": ["seed_curated"],
                }
            ]

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
                    discovery_config={
                        "gamesmap": {"enabled": False},
                        "gameprog": {"enabled": False},
                    },
                    fetcher=fake_fetch,
                )

            manifest = json.loads(sd.URL_PATCH_MANIFEST_PATH.read_text(encoding="utf-8"))
            assert manifest["patches"]["https://old.example/jobs"] == "https://new.example/jobs"
            assert report["summary"]["queuedCandidateCount"] == 1
            assert report["summary"]["failedProbeCount"] == 0
            assert report["runtime"]["urlPatchStats"]["added"] == 1
            assert report["runtime"]["urlPatchStats"]["reprobed"] == 1
            assert report["runtime"]["urlPatchRecoveredCount"] == 1
        finally:
            (
                sd.ACTIVE_PATH,
                sd.PENDING_PATH,
                sd.REJECTED_PATH,
                sd.DISCOVERY_CANDIDATES_PATH,
                sd.DISCOVERY_REPORT_PATH,
                sd.URL_PATCH_MANIFEST_PATH,
            ) = prev_paths
            sd.STATIC_DISCOVERY_CANDIDATES = prev_static
            sd.STUDIO_SEEDS = prev_seeds


def test_run_discovery_persists_deferred_candidates_in_candidates_file() -> None:
    with workspace_tmpdir("source-discovery") as root:
        prev_paths = (
            sd.ACTIVE_PATH,
            sd.PENDING_PATH,
            sd.REJECTED_PATH,
            sd.DISCOVERY_CANDIDATES_PATH,
            sd.DISCOVERY_REPORT_PATH,
            sd.URL_PATCH_MANIFEST_PATH,
        )
        prev_static = list(sd.STATIC_DISCOVERY_CANDIDATES)
        prev_seeds = list(sd.STUDIO_SEEDS)
        try:
            sd.ACTIVE_PATH = root / "active.json"
            sd.PENDING_PATH = root / "pending.json"
            sd.REJECTED_PATH = root / "rejected.json"
            sd.DISCOVERY_CANDIDATES_PATH = root / "candidates.json"
            sd.DISCOVERY_REPORT_PATH = root / "report.json"
            sd.URL_PATCH_MANIFEST_PATH = root / "url-patch-manifest.json"
            sd.STUDIO_SEEDS = []
            sd.STATIC_DISCOVERY_CANDIDATES = []
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
                    "discover_game_studio_sheet_candidates",
                    return_value=(
                        [
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
                        ],
                        [],
                        [],
                    ),
                ),
                mock.patch.object(
                    discovery_orchestrator.sd, "build_pattern_candidates", return_value=[]
                ),
                mock.patch.object(
                    discovery_orchestrator.sd,
                    "discover_seed_careers_page_candidates",
                    return_value=([], [], []),
                ),
                mock.patch.object(
                    discovery_orchestrator, "discover_web_search_candidates", return_value=([], [])
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
                    discovery_config={
                        "gamesmap": {"enabled": False},
                        "gameprog": {"enabled": False},
                    },
                    fetcher=fake_fetch,
                )

            persisted_candidates = json.loads(
                sd.DISCOVERY_CANDIDATES_PATH.read_text(encoding="utf-8")
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
        finally:
            (
                sd.ACTIVE_PATH,
                sd.PENDING_PATH,
                sd.REJECTED_PATH,
                sd.DISCOVERY_CANDIDATES_PATH,
                sd.DISCOVERY_REPORT_PATH,
                sd.URL_PATCH_MANIFEST_PATH,
            ) = prev_paths
            sd.STATIC_DISCOVERY_CANDIDATES = prev_static
            sd.STUDIO_SEEDS = prev_seeds


def test_run_discovery_uses_previous_deferred_review_history_in_ranking() -> None:
    with workspace_tmpdir("source-discovery") as root:
        prev_paths = (
            sd.ACTIVE_PATH,
            sd.PENDING_PATH,
            sd.REJECTED_PATH,
            sd.DISCOVERY_CANDIDATES_PATH,
            sd.DISCOVERY_REPORT_PATH,
            sd.URL_PATCH_MANIFEST_PATH,
        )
        prev_static = list(sd.STATIC_DISCOVERY_CANDIDATES)
        prev_seeds = list(sd.STUDIO_SEEDS)
        try:
            sd.ACTIVE_PATH = root / "active.json"
            sd.PENDING_PATH = root / "pending.json"
            sd.REJECTED_PATH = root / "rejected.json"
            sd.DISCOVERY_CANDIDATES_PATH = root / "candidates.json"
            sd.DISCOVERY_REPORT_PATH = root / "report.json"
            sd.URL_PATCH_MANIFEST_PATH = root / "url-patch-manifest.json"
            sd.STUDIO_SEEDS = []
            sd.STATIC_DISCOVERY_CANDIDATES = []
            sd.DISCOVERY_CANDIDATES_PATH.write_text(
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
                    "discover_game_studio_sheet_candidates",
                    return_value=(
                        [
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
                        ],
                        [],
                        [],
                    ),
                ),
                mock.patch.object(
                    discovery_orchestrator.sd, "build_pattern_candidates", return_value=[]
                ),
                mock.patch.object(
                    discovery_orchestrator.sd,
                    "discover_seed_careers_page_candidates",
                    return_value=([], [], []),
                ),
                mock.patch.object(
                    discovery_orchestrator, "discover_web_search_candidates", return_value=([], [])
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
                    discovery_config={
                        "gamesmap": {"enabled": False},
                        "gameprog": {"enabled": False},
                    },
                    fetcher=fake_fetch,
                )

            row = report["candidates"][0]
            assert row["rankScore"] > row["score"]
            assert "deferred_backlog_age" in row["rankReasons"]
        finally:
            (
                sd.ACTIVE_PATH,
                sd.PENDING_PATH,
                sd.REJECTED_PATH,
                sd.DISCOVERY_CANDIDATES_PATH,
                sd.DISCOVERY_REPORT_PATH,
                sd.URL_PATCH_MANIFEST_PATH,
            ) = prev_paths
            sd.STATIC_DISCOVERY_CANDIDATES = prev_static
            sd.STUDIO_SEEDS = prev_seeds


def test_build_m5_strategic_backlog_applies_frozen_lanes_and_identity_rules() -> None:
    backlog = sd.build_m5_strategic_backlog(
        report_candidates=[
            {
                "sourceId": "source-1",
                "name": "TiMi Studio Group",
                "studio": "TiMi Studio Group",
                "adapter": "static",
                "rankScore": 88,
                "rankReasons": ["live_jobs_detected"],
                "jobsFound": 4,
                "hqRegion": "Asia",
            },
            {
                "id": "custom-row",
                "name": "Custom Studio",
                "studio": "Custom Studio",
                "adapter": "static",
                "score": 55,
                "rankReasons": [],
                "jobsFound": 0,
                "region": "North America",
            },
            {
                "id": "workday-row",
                "name": "Wolcen Studios",
                "studio": "Wolcen Studios",
                "adapter": "workday",
                "rankScore": 80,
                "rankReasons": ["live_jobs_detected"],
                "jobsFound": 2,
            },
        ],
        failures=[
            {
                "name": "Blocked Static",
                "adapter": "static",
                "dropReason": "blocked_domain",
                "dropStage": "suppressed_static",
            },
            {
                "name": "Existing Source",
                "adapter": "static",
                "dropReason": "existing_id",
            },
        ],
        active_rows=[],
        source_state_rows={
            "Custom Studio": {
                "lastStatus": "ok",
                "lastKeptCount": 4,
            },
            "Wolcen Studios": {
                "structuredMigrationBaselineCapturedAt": "2026-03-26T09:00:00Z",
                "structuredMigrationBaselineDurationMs": 9100,
                "structuredMigrationBaselineStatus": "error",
                "structuredMigrationBaselineError": "static timeout",
                "structuredMigrationBaselineFailureBucket": "static_listing",
                "structuredMigrationBaselineKeptCount": 1,
                "lastDurationMs": 5400,
                "lastStatus": "ok",
                "lastError": "",
                "lastFailureBucket": "structured_listing",
                "lastKeptCount": 2,
                "structuredMigrationShadowRunCount": 4,
                "structuredMigrationHealthyRunCount": 3,
                "structuredMigrationPromotedAt": "2026-03-26T10:00:00Z",
            },
        },
    )

    assert [row["coverageLane"] for row in backlog] == [
        "lane_c_asia_custom",
        "lane_b_custom",
        "lane_a_m4_followup",
        "lane_d_defer",
        "lane_d_defer",
    ]
    assert {row["coverageLane"] for row in backlog}.issubset(
        {
            "lane_a_m4_followup",
            "lane_b_custom",
            "lane_c_asia_custom",
            "lane_d_defer",
        }
    )

    asia_row = backlog[0]
    assert asia_row["candidateIdentityKey"] == "source-1"
    assert asia_row["coveragePriority"] > backlog[1]["coveragePriority"]
    assert "asia_hq" in asia_row["rankReasons"]
    assert "open_role_evidence" in asia_row["rankReasons"]
    assert "weak_regional_coverage" in asia_row["rankReasons"]

    custom_row = backlog[1]
    assert custom_row["candidateIdentityKey"] == sr.source_identity(
        {"id": "custom-row", "name": "Custom Studio", "adapter": "static"}
    )
    assert custom_row["firstRunOutcome"] == "healthy_keep"
    assert custom_row["firstRunKeptCount"] == 4

    workday_row = backlog[2]
    assert workday_row["coverageLane"] == "lane_a_m4_followup"
    assert workday_row["exclusionStatus"] == "excluded"
    assert workday_row["exclusionReason"] == "m4_family_followup"
    assert workday_row["migrationComparison"] == {
        "before": {
            "durationMs": 9100,
            "status": "error",
            "error": "static timeout",
            "failureBucket": "static_listing",
            "keptCount": 1,
        },
        "after": {
            "durationMs": 5400,
            "status": "ok",
            "error": "",
            "failureBucket": "structured_listing",
            "keptCount": 2,
        },
        "runtimeDeltaMs": -3700,
        "keptCountDelta": 1,
        "shadowRunCount": 4,
        "healthyRunCount": 3,
        "promotedAt": "2026-03-26T10:00:00Z",
        "demotedAt": "",
        "rollbackChecklist": [
            "Re-enable the static twin in the registry.",
            "Keep structured shadow mode until 3 consecutive healthy runs complete.",
            "Demote the structured source if kept count drops to zero or duplicate rate regresses.",
        ],
    }

    blocked_row = backlog[3]
    assert blocked_row["exclusionStatus"] == "excluded"
    assert blocked_row["exclusionReason"] == "blocked_domain"

    existing_row = backlog[4]
    assert existing_row["exclusionStatus"] == "excluded"
    assert existing_row["exclusionReason"] == "existing_id"


def test_run_discovery_writes_m5_backlog_snapshot() -> None:
    with workspace_tmpdir("source-discovery") as root:
        prev_paths = (
            sd.ACTIVE_PATH,
            sd.PENDING_PATH,
            sd.REJECTED_PATH,
            sd.DISCOVERY_CANDIDATES_PATH,
            sd.DISCOVERY_REPORT_PATH,
            sd.M5_STRATEGIC_BACKLOG_PATH,
            sd.URL_PATCH_MANIFEST_PATH,
        )
        prev_static = list(sd.STATIC_DISCOVERY_CANDIDATES)
        prev_seeds = list(sd.STUDIO_SEEDS)
        prev_sheet = discovery_orchestrator.discover_game_studio_sheet_candidates
        prev_gamesmap = discovery_orchestrator.discover_gamesmap_candidates
        prev_gameprog = discovery_orchestrator.discover_gameprog_candidates
        prev_web = discovery_orchestrator.discover_web_search_candidates
        prev_seed_scan = discovery_orchestrator.sd.discover_seed_careers_page_candidates
        prev_probe = discovery_orchestrator.async_probe_candidate
        try:
            sd.ACTIVE_PATH = root / "active.json"
            sd.PENDING_PATH = root / "pending.json"
            sd.REJECTED_PATH = root / "rejected.json"
            sd.DISCOVERY_CANDIDATES_PATH = root / "candidates.json"
            sd.DISCOVERY_REPORT_PATH = root / "report.json"
            sd.M5_STRATEGIC_BACKLOG_PATH = root / "m5-strategic-backlog.json"
            sd.URL_PATCH_MANIFEST_PATH = root / "url-patch-manifest.json"
            sd.STUDIO_SEEDS = []
            sd.STATIC_DISCOVERY_CANDIDATES = [
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
            ]

            async def fake_probe(
                candidate, timeout_s, *, fetcher, try_playwright=None, playwright_semaphore=None
            ):
                return True, 4, ""

            discovery_orchestrator.discover_game_studio_sheet_candidates = lambda *args, **kwargs: (
                [],
                [],
                [],
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
            discovery_orchestrator.discover_web_search_candidates = lambda *args, **kwargs: (
                [],
                [],
                [],
            )
            discovery_orchestrator.sd.discover_seed_careers_page_candidates = (
                lambda *args, **kwargs: ([], [], [])
            )
            discovery_orchestrator.async_probe_candidate = fake_probe

            report = discovery_orchestrator.run_discovery(
                timeout_s=1,
                top_n=0,
                preset="uncapped",
                mode="static",
                include_web_search=False,
                discovery_config=sd.load_discovery_config(),
                fetcher=lambda *args, **kwargs: "",
            )

            assert report["summary"]["queuedCandidateCount"] == 1
            assert sd.DISCOVERY_CANDIDATES_PATH.exists()
            assert sd.M5_STRATEGIC_BACKLOG_PATH.exists()

            backlog = json.loads(sd.M5_STRATEGIC_BACKLOG_PATH.read_text(encoding="utf-8"))
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
            (
                sd.ACTIVE_PATH,
                sd.PENDING_PATH,
                sd.REJECTED_PATH,
                sd.DISCOVERY_CANDIDATES_PATH,
                sd.DISCOVERY_REPORT_PATH,
                sd.M5_STRATEGIC_BACKLOG_PATH,
                sd.URL_PATCH_MANIFEST_PATH,
            ) = prev_paths
            sd.STATIC_DISCOVERY_CANDIDATES = prev_static
            sd.STUDIO_SEEDS = prev_seeds
            discovery_orchestrator.discover_game_studio_sheet_candidates = prev_sheet
            discovery_orchestrator.discover_gamesmap_candidates = prev_gamesmap
            discovery_orchestrator.discover_gameprog_candidates = prev_gameprog
            discovery_orchestrator.discover_web_search_candidates = prev_web
            discovery_orchestrator.sd.discover_seed_careers_page_candidates = prev_seed_scan
            discovery_orchestrator.async_probe_candidate = prev_probe


def test_run_discovery_only_gamedevmap_skips_other_generator_stages() -> None:
    with workspace_tmpdir("source-discovery-only-gamedevmap") as root:
        prev_paths = (
            sd.ACTIVE_PATH,
            sd.PENDING_PATH,
            sd.REJECTED_PATH,
            sd.DISCOVERY_CANDIDATES_PATH,
            sd.DISCOVERY_REPORT_PATH,
            sd.URL_PATCH_MANIFEST_PATH,
        )
        prev_static = list(sd.STATIC_DISCOVERY_CANDIDATES)
        prev_seeds = list(sd.STUDIO_SEEDS)
        try:
            sd.ACTIVE_PATH = root / "active.json"
            sd.PENDING_PATH = root / "pending.json"
            sd.REJECTED_PATH = root / "rejected.json"
            sd.DISCOVERY_CANDIDATES_PATH = root / "candidates.json"
            sd.DISCOVERY_REPORT_PATH = root / "report.json"
            sd.URL_PATCH_MANIFEST_PATH = root / "url-patch-manifest.json"
            sd.STUDIO_SEEDS = []
            sd.STATIC_DISCOVERY_CANDIDATES = []

            cli_args = discovery_orchestrator.parse_args(["--only-gamedevmap"])

            with (
                mock.patch.object(
                    discovery_orchestrator,
                    "stage_curated_seed_candidates",
                    side_effect=AssertionError("curated seed stage should be disabled"),
                ),
                mock.patch.object(
                    discovery_orchestrator,
                    "discover_game_studio_sheet_candidates",
                    side_effect=AssertionError("sheet directory stage should be disabled"),
                ),
                mock.patch.object(
                    discovery_orchestrator.sd,
                    "build_pattern_candidates",
                    side_effect=AssertionError("provider-pattern stage should be disabled"),
                ),
                mock.patch.object(
                    discovery_orchestrator.sd,
                    "discover_seed_careers_page_candidates",
                    side_effect=AssertionError("seed careers stage should be disabled"),
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
                    "discover_web_search_candidates",
                    side_effect=AssertionError("web search stage should be disabled"),
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
            queued = json.loads(sd.DISCOVERY_CANDIDATES_PATH.read_text(encoding="utf-8"))
            assert len(queued) == 1
            assert str(queued[0].get("discoveryMethod") or "") == "gamedevmap"
            assert str(queued[0].get("sourceDirectory") or "") == "gamedevmap"
        finally:
            (
                sd.ACTIVE_PATH,
                sd.PENDING_PATH,
                sd.REJECTED_PATH,
                sd.DISCOVERY_CANDIDATES_PATH,
                sd.DISCOVERY_REPORT_PATH,
                sd.URL_PATCH_MANIFEST_PATH,
            ) = prev_paths
            sd.STATIC_DISCOVERY_CANDIDATES = prev_static
            sd.STUDIO_SEEDS = prev_seeds


def test_load_discovery_config_uses_configured_path() -> None:
    with workspace_tmpdir("source-discovery") as root:
        previous_path = sd.DISCOVERY_CONFIG_PATH
        try:
            sd.DISCOVERY_CONFIG_PATH = root / "nested" / "discovery.json"
            sd.DISCOVERY_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
            sd.DISCOVERY_CONFIG_PATH.write_text(
                json.dumps({"gamesmap": {"enabled": True, "maxDetailPages": 25}}),
                encoding="utf-8",
            )
            cfg = sd.load_discovery_config()
        finally:
            sd.DISCOVERY_CONFIG_PATH = previous_path
        assert bool((cfg.get("gamesmap") or {}).get("enabled"))
        assert int((cfg.get("gamesmap") or {}).get("maxDetailPages") or 0) == 25


def test_load_discovery_config_merges_directory_adapter_sections() -> None:
    with workspace_tmpdir("source-discovery-merge") as root:
        previous_path = sd.DISCOVERY_CONFIG_PATH
        try:
            sd.DISCOVERY_CONFIG_PATH = root / "nested" / "discovery.json"
            sd.DISCOVERY_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
            sd.DISCOVERY_CONFIG_PATH.write_text(
                json.dumps(
                    {
                        "gamesmap": {"enabled": True, "maxDetailPages": 25},
                        "gameprog": {"enabled": False, "maxStudios": 15},
                        "gamedevmap": {
                            "enabled": True,
                            "maxRows": 30,
                            "maxHomepageFetches": 8,
                        },
                    }
                ),
                encoding="utf-8",
            )
            cfg = sd.load_discovery_config()
        finally:
            sd.DISCOVERY_CONFIG_PATH = previous_path
        assert bool((cfg.get("gamesmap") or {}).get("enabled"))
        assert int((cfg.get("gamesmap") or {}).get("maxDetailPages") or 0) == 25
        assert not bool((cfg.get("gameprog") or {}).get("enabled"))
        assert int((cfg.get("gameprog") or {}).get("maxStudios") or 0) == 15
        assert bool((cfg.get("gamedevmap") or {}).get("enabled"))
        assert int((cfg.get("gamedevmap") or {}).get("maxRows") or 0) == 30
        assert int((cfg.get("gamedevmap") or {}).get("maxHomepageFetches") or 0) == 8


def test_resolve_discovery_thresholds_overrides_defaults() -> None:
    thresholds = sd.resolve_discovery_thresholds(
        {
            "thresholds": {
                "minProviderEvidenceToProbe": 7,
                "patternProviderQueueThreshold": 55,
            }
        }
    )
    assert int(thresholds.get("minProviderEvidenceToProbe") or 0) == 7
    assert int(thresholds.get("patternProviderQueueThreshold") or 0) == 55
    assert int(thresholds.get("minStaticEvidenceToQueue") or 0) == int(
        sd.DEFAULT_DISCOVERY_THRESHOLDS["minStaticEvidenceToQueue"]
    )


def test_parse_gameprog_teams_json_returns_studios() -> None:
    json_text = """[
        {"name": "Test Studio", "url": "https://test-studio.com/", "place": "Rome"},
        {"name": "Another Studio", "url": "https://another.it/", "place": "Milan"}
    ]"""
    rows = sd.parse_gameprog_teams_json(json_text)
    assert len(rows) == 2
    assert rows[0]["studio"] == "Test Studio"
    assert rows[0]["url"] == "https://test-studio.com/"
    assert rows[0]["place"] == "Rome"


def test_parse_gameprog_teams_json_handles_missing_fields() -> None:
    json_text = """[
        {"name": "Valid Studio", "url": "https://valid.com/", "place": "Turin"},
        {"name": "No URL"},
        {"url": "https://no-name.com/"},
        {"name": "", "url": ""}
    ]"""
    rows = sd.parse_gameprog_teams_json(json_text)
    assert len(rows) == 1
    assert rows[0]["studio"] == "Valid Studio"


def test_discover_gameprog_candidates_emits_provider_and_static() -> None:
    config = {
        "gameprog": {
            "enabled": True,
            "teamsUrl": "https://gameprog.it/teams.json",
            "websiteOnlyFallback": True,
            "maxStudios": 10,
        }
    }

    teams_json = """[
        {"name": "Studio With Careers", "url": "https://example-studio.com/", "place": "Rome"},
        {"name": "Studio Website Only", "url": "https://website-only.it/", "place": "Milan"}
    ]"""

    careers_html = """<!DOCTYPE html>
    <html><body>
    <a href="https://boards.greenhouse.io/example">Jobs</a>
    </body></html>"""

    website_html = """<!DOCTYPE html>
    <html><body><a href="/careers">Careers</a></body></html>"""

    payloads = {
        "https://gameprog.it/teams.json": teams_json,
        "https://example-studio.com/": careers_html,
        "https://website-only.it/": website_html,
    }

    def fake_fetch(url: str, _: int) -> str:
        if url not in payloads:
            raise RuntimeError(f"unexpected URL: {url}")
        return payloads[url]

    provider_rows, static_rows, failures = sd.discover_gameprog_candidates(
        5, config=config, fetcher=fake_fetch
    )
    assert len(failures) == 0
    assert len(provider_rows) >= 1
    assert str(provider_rows[0].get("adapter") or "") == "greenhouse"
    assert str(provider_rows[0].get("discoveryMethod") or "") == "gameprog"
    assert str(provider_rows[0].get("sourceDirectory") or "") == "gameprog"
    assert len(static_rows) >= 1
    assert str(static_rows[0].get("adapter") or "") == "static"
    assert str(static_rows[0].get("careersUrl") or "") == "https://website-only.it/careers"
    assert "gameprog_careers_url" in (static_rows[0].get("evidenceTypes") or [])


def test_discover_gameprog_candidates_keeps_guessed_careers_path_fallback_after_helper_misses() -> (
    None
):
    config = {
        "gameprog": {
            "enabled": True,
            "teamsUrl": "https://gameprog.it/teams.json",
            "websiteOnlyFallback": True,
            "maxStudios": 10,
        }
    }
    teams_json = """[
        {"name": "Studio Website Only", "url": "https://website-only.it/", "place": "Milan"}
    ]"""
    payloads = {
        "https://gameprog.it/teams.json": teams_json,
        "https://website-only.it/": "<!DOCTYPE html><html><body><h1>Welcome</h1></body></html>",
    }

    def fake_fetch(url: str, _: int) -> str:
        if url not in payloads:
            raise RuntimeError(f"unexpected URL: {url}")
        return payloads[url]

    provider_rows, static_rows, failures = sd.discover_gameprog_candidates(
        5, config=config, fetcher=fake_fetch
    )
    assert failures == []
    assert provider_rows == []
    assert len(static_rows) == 1
    assert str(static_rows[0].get("careersUrl") or "") == "https://website-only.it/careers"
    assert "gameprog_careers_url" in (static_rows[0].get("evidenceTypes") or [])


def test_discover_gameprog_candidates_handles_fetch_failure() -> None:
    config = {
        "gameprog": {
            "enabled": True,
            "teamsUrl": "https://gameprog.it/teams.json",
            "websiteOnlyFallback": True,
            "maxStudios": 10,
        }
    }

    teams_json = """[{"name": "Test Studio", "url": "https://example.com/", "place": "Rome"}]"""

    payloads = {
        "https://gameprog.it/teams.json": teams_json,
    }

    def fake_fetch(url: str, _: int) -> str:
        if url not in payloads:
            raise RuntimeError(f"unexpected URL: {url}")
        if url == "https://example.com/":
            raise RuntimeError("fetch failed")
        return payloads[url]

    provider_rows, static_rows, failures = sd.discover_gameprog_candidates(
        5, config=config, fetcher=fake_fetch
    )
    assert len(failures) >= 1
    assert len(static_rows) >= 1
    assert bool(static_rows[0].get("manualOnly"))


def test_discover_gameprog_candidates_reuses_fresh_cache() -> None:
    with workspace_tmpdir("gameprog-cache") as root:
        cache_path = root / "gameprog-cache.json"
        config = {
            "gameprog": {
                "enabled": True,
                "teamsUrl": "https://gameprog.it/teams.json",
                "websiteOnlyFallback": True,
                "maxStudios": 10,
                "cachePath": str(cache_path),
                "cacheTtlMinutes": 60,
            }
        }
        teams_json = """[
            {"name": "Studio With Careers", "url": "https://example-studio.com/", "place": "Rome"},
            {"name": "Studio Website Only", "url": "https://website-only.it/", "place": "Milan"}
        ]"""
        payloads = {
            "https://gameprog.it/teams.json": teams_json,
            "https://example-studio.com/": """<!DOCTYPE html><html><body><a href="https://boards.greenhouse.io/example">Jobs</a></body></html>""",
            "https://website-only.it/": """<!DOCTYPE html><html><body><h1>Welcome</h1></body></html>""",
        }
        calls: list[str] = []

        def fake_fetch(url: str, _: int) -> str:
            calls.append(url)
            if url not in payloads:
                raise RuntimeError(f"unexpected URL: {url}")
            return payloads[url]

        provider_rows_1, static_rows_1, failures_1 = sd.discover_gameprog_candidates(
            5, config=config, fetcher=fake_fetch
        )
        assert len(calls) > 0
        first_call_count = len(calls)

        with mock.patch(
            "src.source_discovery.gameprog.fetch_directory_pages",
            side_effect=AssertionError("directory fetch helper should be bypassed on cache hit"),
        ):
            provider_rows_2, static_rows_2, failures_2 = sd.discover_gameprog_candidates(
                5, config=config, fetcher=fake_fetch
            )

        assert len(calls) == first_call_count
        assert provider_rows_1 == provider_rows_2
        assert static_rows_1 == static_rows_2
        assert failures_1 == failures_2
