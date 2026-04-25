# ruff: noqa: F401
from ._helpers import (
    FIXTURES_DIR,
    DiscoveryReportSummarySchema,
    Path,
    _fixture_json,
    _fixture_text,
    _gamesmap_next_payload_html,
    async_fetch_text_httpx,
    asyncio,
    classify_probe_failure_stage,
    discovery_config_module,
    discovery_orchestrator,
    discovery_url_patches,
    gamesmap_adapter,
    importlib,
    json,
    mock,
    os,
    override_discovery_config,
    override_discovery_runtime,
    sd,
    sr,
    sys,
    threading,
    time,
    workspace_tmpdir,
)


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


def test_discovery_report_snapshot_contract() -> None:
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
                },
                {
                    "name": "Demo Greenhouse",
                    "studio": "Demo",
                    "adapter": "greenhouse",
                    "slug": "demo",
                    "api_url": "https://boards-api.greenhouse.io/v1/boards/demo/jobs?content=true",
                },
            ],
        ):

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


def test_run_discovery_applies_existing_url_patches_before_probe() -> None:
    with workspace_tmpdir("source-discovery") as root:
        with override_discovery_runtime(root, studio_seeds=[], static_candidates=[]) as paths:
            discovery_url_patches.save_url_patch_manifest(
                {"https://old.example/jobs": "https://new.example/jobs"},
                path=paths.url_patch_manifest_path,
                added=1,
                updated=0,
                reprobed=0,
            )
            with override_discovery_config(
                studio_seeds=[],
                static_candidates=[
                    {
                        "name": "Patched Static",
                        "studio": "Patched Static",
                        "adapter": "static",
                        "listing_url": "https://old.example/jobs",
                        "pages": ["https://old.example/jobs"],
                        "evidenceScore": 52,
                        "evidenceTypes": ["seed_curated"],
                    }
                ],
            ):
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
                    discovery_config={
                        "gamesmap": {"enabled": False},
                        "gameprog": {"enabled": False},
                    },
                    fetcher=fake_fetch,
                )
            assert "https://old.example/jobs" not in seen_urls
            assert report["summary"]["queuedCandidateCount"] == 1
            assert report["runtime"]["urlPatchStats"]["loaded"] == 1
            queued = json.loads(paths.discovery_candidates_path.read_text(encoding="utf-8"))
            assert queued[0]["listing_url"] == "https://new.example/jobs"


def test_run_discovery_balances_queue_with_deferrals() -> None:
    with workspace_tmpdir("source-discovery") as root:
        prev_caps = dict(discovery_config_module.ADAPTER_QUEUE_CAPS)
        try:
            with override_discovery_runtime(
                root,
                studio_seeds=[],
                static_candidates=[
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
                ],
            ):
                discovery_config_module.ADAPTER_QUEUE_CAPS["lever"] = 1
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
                    int((report["summary"].get("lossAccounting") or {}).get("deferredByCap") or 0)
                    == 1
                )
                deferred = [
                    row for row in (report.get("candidates") or []) if bool(row.get("deferred"))
                ]
                assert len(deferred) == 1
                assert str(deferred[0].get("deferReason") or "") == "adapter_cap"
                assert str(deferred[0].get("dropStage") or "") == "deferred_by_cap"
                assert str(deferred[0].get("dropReason") or "") == "adapter_cap"
        finally:
            discovery_config_module.ADAPTER_QUEUE_CAPS.clear()
            discovery_config_module.ADAPTER_QUEUE_CAPS.update(prev_caps)


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
            with override_discovery_runtime(root) as paths:
                for path in (paths.active_path, paths.pending_path, paths.rejected_path):
                    path.write_text("[]", encoding="utf-8")
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
                        discovery_orchestrator, "build_pattern_candidates", return_value=[]
                    ),
                    mock.patch.object(
                        discovery_orchestrator,
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


def test_run_discovery_gamesmap_candidates_flow_into_report_and_queue() -> None:
    with workspace_tmpdir("source-discovery") as root:
        with override_discovery_runtime(root, studio_seeds=[], static_candidates=[]) as paths:
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
            queued = json.loads(paths.discovery_candidates_path.read_text(encoding="utf-8"))
            assert len(queued) == 1
            assert str(queued[0].get("discoveryMethod") or "") == "gamesmap"
            assert str(queued[0].get("sourceDirectory") or "") == "gamesmap"


def test_run_discovery_gamedevmap_candidates_flow_into_report_and_queue() -> None:
    with workspace_tmpdir("source-discovery-gamedevmap") as root:
        with override_discovery_runtime(root, studio_seeds=[], static_candidates=[]) as paths:
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
            queued = json.loads(paths.discovery_candidates_path.read_text(encoding="utf-8"))
            assert len(queued) == 1
            assert str(queued[0].get("discoveryMethod") or "") == "gamedevmap"
            assert str(queued[0].get("sourceDirectory") or "") == "gamedevmap"


def test_run_discovery_does_not_auto_approve_weak_pending_only_rows() -> None:
    with workspace_tmpdir("source-discovery-auto-approval") as root:
        prev_approval_state_path = discovery_orchestrator.DEFAULT_APPROVAL_STATE_PATH
        prev_sheet = discovery_orchestrator.discover_game_studio_sheet_candidates
        prev_gamesmap = discovery_orchestrator.discover_gamesmap_candidates
        prev_gameprog = discovery_orchestrator.discover_gameprog_candidates
        prev_web = discovery_orchestrator.discover_web_search_candidates
        prev_seed_scan = discovery_orchestrator.discover_seed_careers_page_candidates
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

                discovery_orchestrator.discover_game_studio_sheet_candidates = (
                    lambda *args, **kwargs: ([], [], [])
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
                discovery_orchestrator.discover_seed_careers_page_candidates = (
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
            discovery_orchestrator.discover_game_studio_sheet_candidates = prev_sheet
            discovery_orchestrator.discover_gamesmap_candidates = prev_gamesmap
            discovery_orchestrator.discover_gameprog_candidates = prev_gameprog
            discovery_orchestrator.discover_web_search_candidates = prev_web
            discovery_orchestrator.discover_seed_careers_page_candidates = prev_seed_scan
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
                    "discover_game_studio_sheet_candidates",
                    side_effect=AssertionError("sheet directory stage should be disabled"),
                ),
                mock.patch.object(
                    discovery_orchestrator,
                    "build_pattern_candidates",
                    side_effect=AssertionError("provider-pattern stage should be disabled"),
                ),
                mock.patch.object(
                    discovery_orchestrator,
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
                    discovery_orchestrator, "build_pattern_candidates", return_value=[]
                ),
                mock.patch.object(
                    discovery_orchestrator,
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
                    discovery_config={
                        "gamesmap": {"enabled": False},
                        "gameprog": {"enabled": False},
                    },
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
                discovery_config={"gamesmap": {"enabled": False}, "gameprog": {"enabled": False}},
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
                    discovery_orchestrator, "build_pattern_candidates", return_value=[]
                ),
                mock.patch.object(
                    discovery_orchestrator,
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
            queued = json.loads(paths.discovery_candidates_path.read_text(encoding="utf-8"))
            assert len(queued) == 1
            assert str(queued[0].get("discoveryMethod") or "") == "seed_careers_page"


def test_run_discovery_writes_m5_backlog_snapshot() -> None:
    with workspace_tmpdir("source-discovery") as root:
        prev_sheet = discovery_orchestrator.discover_game_studio_sheet_candidates
        prev_gamesmap = discovery_orchestrator.discover_gamesmap_candidates
        prev_gameprog = discovery_orchestrator.discover_gameprog_candidates
        prev_web = discovery_orchestrator.discover_web_search_candidates
        prev_seed_scan = discovery_orchestrator.discover_seed_careers_page_candidates
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

                discovery_orchestrator.discover_game_studio_sheet_candidates = (
                    lambda *args, **kwargs: ([], [], [])
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
                discovery_orchestrator.discover_seed_careers_page_candidates = (
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
            discovery_orchestrator.discover_game_studio_sheet_candidates = prev_sheet
            discovery_orchestrator.discover_gamesmap_candidates = prev_gamesmap
            discovery_orchestrator.discover_gameprog_candidates = prev_gameprog
            discovery_orchestrator.discover_web_search_candidates = prev_web
            discovery_orchestrator.discover_seed_careers_page_candidates = prev_seed_scan
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
