import asyncio
import importlib
import json
import os
import sys
from pathlib import Path
from unittest import mock

from src import source_discovery as sd
from src import source_registry as sr
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


def test_apply_queue_balancing_prefers_provider_candidates_over_static_in_bounded_runs() -> None:
    candidates = [
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
    ]

    queued, report_rows, stats = sd.apply_queue_balancing(candidates, top_n=4)
    assert [str(row.get("adapter") or "") for row in queued] == [
        "greenhouse",
        "lever",
        "ashby",
        "smartrecruiters",
    ]
    assert int((stats.get("queuedByAdapter") or {}).get("static") or 0) == 0
    assert int((stats.get("deferredByAdapter") or {}).get("static") or 0) == 2
    assert int((stats.get("healthyButDeferredByAdapter") or {}).get("static") or 0) == 2
    assert len([row for row in report_rows if bool(row.get("deferred"))]) == 2
    assert int(stats.get("providerTarget") or 0) == 2


def test_apply_queue_balancing_does_not_adapter_cap_google_sheet_candidates() -> None:
    candidates = []
    for index in range(10):
        candidates.append(
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
                "careersUrl": f"https://sheet-{index}.example/jobs",
                "sourceDirectoryEntryUrl": f"https://sheet-{index}.example/jobs",
            }
        )

    queued, report_rows, stats = sd.apply_queue_balancing(candidates, top_n=0)

    assert len(queued) == 10
    assert len([row for row in report_rows if bool(row.get("deferred"))]) == 0
    assert int((stats.get("queuedByAdapter") or {}).get("static") or 0) == 10
    assert int((stats.get("deferredByAdapter") or {}).get("static") or 0) == 0
    assert int((stats.get("healthyButDeferredByAdapter") or {}).get("static") or 0) == 0
    assert "adapter_cap" not in (stats.get("deferredReasons") or {})


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


def test_run_discovery_uncapped_reports_runtime_cap_bypass_flags() -> None:
    dynamic_candidates = [
        {
            "name": f"Sheet Static {index}",
            "studio": f"Sheet Static {index}",
            "adapter": "static",
            "score": 90 - index,
            "evidenceScore": 80,
            "pages": [f"https://sheet-{index}.example/jobs"],
            "careersUrl": f"https://sheet-{index}.example/jobs",
            "sourceDirectoryEntryUrl": f"https://sheet-{index}.example/jobs",
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
    with workspace_tmpdir("source-discovery") as tmp:
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
                mock.patch.object(
                    discovery_orchestrator, "async_probe_candidate", side_effect=fake_probe
                ),
                mock.patch.object(discovery_orchestrator, "load_url_patches", return_value={}),
                mock.patch.object(
                    discovery_orchestrator, "save_url_patch_manifest", return_value=None
                ),
                mock.patch.object(discovery_orchestrator, "read_source_state", return_value={}),
            ):
                report = discovery_orchestrator.run_discovery(
                    timeout_s=1,
                    top_n=0,
                    preset="uncapped",
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

    runtime = report.get("runtime") or {}
    assert str(runtime.get("preset") or "") == "uncapped"
    assert bool(runtime.get("topCapBypassed")) is True
    assert bool(runtime.get("sheetStaticProbeCapBypassed")) is True
    assert int((report.get("summary") or {}).get("probedCandidateCount") or 0) == 12
    assert int((report.get("summary") or {}).get("suppressedStaticCount") or 0) == 0


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
        providers, static_rows, failures = sd.discover_seed_careers_page_candidates(
            5,
            fetcher=lambda *_: '<a href="/position/artist">Artist</a>',
        )
    finally:
        sd.STUDIO_SEEDS = previous

    assert len(failures) == 0
    assert len(providers) == 1
    assert len(static_rows) == 0
    assert str(providers[0].get("adapter") or "") == "personio"


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


def test_parse_gamesmap_index_entries_extracts_industry_rows_from_js_payload() -> None:
    rows = sd.parse_gamesmap_index_entries(
        _fixture_text("gamesmap_index.html"),
        "https://www.gamesmap.de",
        prefer_english=True,
    )
    assert len(rows) == 3
    assert (
        str(rows[0].get("detailUrl") or "")
        == "https://www.gamesmap.de/en/detail/industry/example-studio-gmbh"
    )
    assert str(rows[0].get("studio") or "") == "Example Studio GmbH"
    assert str(rows[0].get("location") or "") == "Hamburg"


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


def test_discover_gamesmap_candidates_emits_provider_and_static_rows() -> None:
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
        "https://www.gamesmap.de/en": _fixture_text("gamesmap_index.html"),
        "https://www.gamesmap.de/en/detail/industry/example-studio-gmbh": _fixture_text(
            "gamesmap_detail_careers.html"
        ),
        "https://www.gamesmap.de/en/detail/industry/tooling-association": _fixture_text(
            "gamesmap_detail_blocked.html"
        ),
        "https://www.gamesmap.de/en/detail/industry/example-publisher": _fixture_text(
            "gamesmap_detail_website_only.html"
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
    assert len(provider_rows) == 1
    assert str(provider_rows[0].get("adapter") or "") == "greenhouse"
    assert str(provider_rows[0].get("discoveryMethod") or "") == "gamesmap"
    assert str(provider_rows[0].get("sourceDirectory") or "") == "gamesmap"
    assert len(static_rows) == 1
    assert str(static_rows[0].get("adapter") or "") == "static"
    assert bool(static_rows[0].get("weakSignal"))
    assert (
        str(static_rows[0].get("sourceDirectoryEntryUrl") or "")
        == "https://www.gamesmap.de/en/detail/industry/example-publisher"
    )
    assert not (bool(static_rows[0].get("manualOnly")))


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
        "https://www.gamesmap.de/en": _fixture_text("gamesmap_index.html"),
        "https://www.gamesmap.de/en/detail/industry/example-studio-gmbh": _fixture_text(
            "gamesmap_detail_careers.html"
        ),
        "https://www.gamesmap.de/en/detail/industry/tooling-association": _fixture_text(
            "gamesmap_detail_blocked.html"
        ),
        "https://www.gamesmap.de/en/detail/industry/example-publisher": _fixture_text(
            "gamesmap_detail_website_only.html"
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


def test_discover_gamesmap_candidates_dedupes_repeated_directory_entries() -> None:
    html = """
    <a href="/en/detail/industry/example-studio-gmbh">Example Studio</a>
    <a href="/detail/industry/example-studio-gmbh">Example Studio duplicate</a>
    """
    config = {
        "gamesmap": {
            "enabled": True,
            "baseUrl": "https://www.gamesmap.de",
            "indexUrls": ["https://www.gamesmap.de/en"],
            "websiteOnlyFallback": False,
            "maxDetailPages": 10,
            "allowedCategoryTokens": ["developer"],
            "blockedCategoryTokens": [],
        }
    }

    def fake_fetch(url: str, _: int) -> str:
        if url == "https://www.gamesmap.de/en":
            return html
        return _fixture_text("gamesmap_detail_careers.html")

    provider_rows, static_rows, _failures = sd.discover_gamesmap_candidates(
        5, config=config, fetcher=fake_fetch
    )
    assert len(provider_rows) == 1
    assert len(static_rows) == 0


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
            "https://www.gamesmap.de/en": _fixture_text("gamesmap_index.html"),
            "https://www.gamesmap.de/en/detail/industry/example-studio-gmbh": _fixture_text(
                "gamesmap_detail_careers.html"
            ),
            "https://www.gamesmap.de/en/detail/industry/tooling-association": _fixture_text(
                "gamesmap_detail_blocked.html"
            ),
            "https://www.gamesmap.de/en/detail/industry/example-publisher": _fixture_text(
                "gamesmap_detail_website_only.html"
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
                "https://www.gamesmap.de/en": _fixture_text("gamesmap_index.html"),
                "https://www.gamesmap.de/en/detail/industry/example-studio-gmbh": _fixture_text(
                    "gamesmap_detail_careers.html"
                ),
                "https://www.gamesmap.de/en/detail/industry/tooling-association": _fixture_text(
                    "gamesmap_detail_blocked.html"
                ),
                "https://www.gamesmap.de/en/detail/industry/example-publisher": _fixture_text(
                    "gamesmap_detail_website_only.html"
                ),
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


def test_run_discovery_skips_duplicate_endpoint_fingerprints() -> None:
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
            sd.STATIC_DISCOVERY_CANDIDATES = [
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
            ]
            report = sd.run_discovery(
                timeout_s=5,
                top_n=0,
                mode="dynamic",
                include_web_search=False,
                fetcher=lambda *_: json.dumps([{"id": 1}]),
            )
            assert int(report["summary"].get("queuedCandidateCount") or 0) == 1
            assert int(report["summary"].get("skippedDuplicateCount") or 0) == 1
            assert "duplicateReasons" in report["summary"]
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


def test_run_discovery_auto_approves_healthy_pending_rows() -> None:
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
            approval_state = json.loads(
                (root / "source-approval-state.json").read_text(encoding="utf-8")
            )
            assert [row["id"] for row in active] == ["pending-ok"]
            assert pending == []
            assert int(approval_state["approvedSinceLastRun"]) == 1
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


def test_run_discovery_prefers_fresh_queued_candidate_over_stale_pending_duplicate() -> None:
    stale_pending = {
        "name": "Fresh Board",
        "studio": "Fresh Board",
        "adapter": "greenhouse",
        "slug": "fresh-board",
        "jobsFound": 0,
        "sampleCount": 0,
        "lastProbedAt": "2026-03-20T00:00:00Z",
    }
    fresh_queued = {
        "name": "Fresh Board",
        "studio": "Fresh Board",
        "adapter": "greenhouse",
        "slug": "fresh-board",
        "jobsFound": 3,
        "sampleCount": 3,
        "lastProbedAt": "2026-03-23T00:00:00Z",
    }

    merged = sr.unique_sources([fresh_queued, stale_pending])

    assert len(merged) == 1
    assert merged[0]["id"] == "greenhouse:slug:fresh-board"
    assert int(merged[0]["jobsFound"] or 0) == 3
    assert int(merged[0]["sampleCount"] or 0) == 3


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
    <html><body><h1>Welcome</h1></body></html>"""

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
    assert str(static_rows[0].get("sourceDirectoryEntryUrl") or "").startswith("https://")


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
