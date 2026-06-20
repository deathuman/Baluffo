# ruff: noqa: F401
from tests.helpers.concurrency import BlockingActiveCounter

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
    } | {"oracle_hcm": 4}
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
    } | {"oracle_hcm": 8}


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


def test_default_directory_fetch_profiles_use_24x3_for_live_adapters() -> None:
    for adapter in ("gameprog", "gamesmap", "gamedevmap"):
        cfg = sd.DEFAULT_DISCOVERY_CONFIG[adapter]
        assert int(cfg.get("fetchConcurrency") or 0) == 24
        assert int(cfg.get("perHostConcurrency") or 0) == 3


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
    lock = threading.Lock()
    active = 0
    max_active = 0
    host_active: dict[str, int] = {}
    host_max: dict[str, int] = {}
    fetches = BlockingActiveCounter(auto_release_at=2)

    def fake_fetch(url: str, _: int) -> str:
        nonlocal active, max_active
        host = url.split("/")[2]
        with lock:
            active += 1
            max_active = max(max_active, active)
            host_active[host] = host_active.get(host, 0) + 1
            host_max[host] = max(host_max.get(host, 0), host_active[host])
        fetches.enter()
        try:
            fetches.wait_released()
            if url.endswith("/fail"):
                raise RuntimeError("boom")
            return f"<html>{url}</html>"
        finally:
            fetches.exit()
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


def test_load_discovery_config_merges_directory_adapter_sections() -> None:
    with workspace_tmpdir("source-discovery-merge") as root:
        config_path = root / "nested" / "discovery.json"
        with override_discovery_config(discovery_config_path=config_path):
            config_path.parent.mkdir(parents=True, exist_ok=True)
            config_path.write_text(
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
        assert bool((cfg.get("gamesmap") or {}).get("enabled"))
        assert int((cfg.get("gamesmap") or {}).get("maxDetailPages") or 0) == 25
        assert not bool((cfg.get("gameprog") or {}).get("enabled"))
        assert int((cfg.get("gameprog") or {}).get("maxStudios") or 0) == 15
        assert bool((cfg.get("gamedevmap") or {}).get("enabled"))
        assert int((cfg.get("gamedevmap") or {}).get("maxRows") or 0) == 30
        assert int((cfg.get("gamedevmap") or {}).get("maxHomepageFetches") or 0) == 8


def test_load_discovery_config_uses_configured_path() -> None:
    with workspace_tmpdir("source-discovery") as root:
        config_path = root / "nested" / "discovery.json"
        with override_discovery_config(discovery_config_path=config_path):
            config_path.parent.mkdir(parents=True, exist_ok=True)
            config_path.write_text(
                json.dumps({"gamesmap": {"enabled": True, "maxDetailPages": 25}}),
                encoding="utf-8",
            )
            cfg = sd.load_discovery_config()
        assert bool((cfg.get("gamesmap") or {}).get("enabled"))
        assert int((cfg.get("gamesmap") or {}).get("maxDetailPages") or 0) == 25


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


def test_parse_args_supports_only_gamedevmap_mode() -> None:
    prev_argv = list(sys.argv)
    try:
        sys.argv = ["source_discovery.py", "--only-gamedevmap"]
        args = sd.parse_args()
    finally:
        sys.argv = prev_argv
    assert bool(args.only_gamedevmap)


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


def test_seed_catalog_path_points_to_repo_src_catalog() -> None:
    assert sd.SEED_CATALOG_PATH.name == "discovery_seed_catalog.json"
    assert sd.SEED_CATALOG_PATH.parts[-2] == "src"
    assert sd.SEED_CATALOG_PATH.exists()


def test_sheet_directory_static_probe_cap_scales_from_bounded_top_n() -> None:
    assert sd.sheet_directory_static_probe_cap(0) == 0
    assert sd.sheet_directory_static_probe_cap(4) == 4
    assert sd.sheet_directory_static_probe_cap(20) == 6


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
