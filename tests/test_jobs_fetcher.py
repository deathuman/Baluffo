# ruff: noqa: F403,F405
from tests.jobs_fetcher_helpers import *

patch_jobs_fetcher_aliases()


def test_runtime_facade_falls_back_to_main_module_for_jobs_fetcher_runs() -> None:
    prev_jf = runtime_resolver.sys.modules.get("src.jobs_fetcher")
    prev_main = runtime_resolver.sys.modules.get("__main__")
    try:
        runtime_resolver.sys.modules.pop("src.jobs_fetcher", None)
        # When run as `python -m src.jobs_fetcher`, __main__ is the jobs_fetcher module.
        # Simulate that so facade() returns a module that has the parser attributes.
        runtime_resolver.sys.modules["__main__"] = jf
        main_mod = runtime_resolver.sys.modules.get("__main__")

        class _Spec:
            name = "src.jobs_fetcher"

        if main_mod is None:
            raise RuntimeError("__main__ module missing")
        prev_spec = getattr(main_mod, "__spec__", None)
        main_mod.__spec__ = _Spec()  # type: ignore[attr-defined]
        resolved = runtime_resolver.facade()
        assert resolved is main_mod
        assert callable(getattr(resolved, "parse_ashby_jobs_from_html", None))
        assert callable(getattr(resolved, "parse_breezy_jobs_html", None))
        assert callable(getattr(resolved, "parse_bamboohr_jobs_html", None))
        assert callable(getattr(resolved, "parse_jazzhr_jobs_html", None))
        assert callable(getattr(resolved, "parse_recruitee_jobs_payload", None))
        assert callable(getattr(resolved, "parse_pinpoint_jobs_payload", None))
        assert callable(getattr(resolved, "parse_8bitplay_html", None))
        assert callable(getattr(resolved, "parse_gracklehq_html", None))
        assert callable(getattr(resolved, "parse_personio_feed_xml", None))
        assert callable(getattr(resolved, "parse_epic_games_jobs_payload", None))
        assert callable(getattr(resolved, "parse_workday_jobs_html", None))
        main_mod.__spec__ = prev_spec  # type: ignore[attr-defined]
    finally:
        if prev_main is not None:
            runtime_resolver.sys.modules["__main__"] = prev_main
        if prev_jf is not None:
            runtime_resolver.sys.modules["src.jobs_fetcher"] = prev_jf


def test_static_plugin_registry_selects_supercell_plugin() -> None:
    ctx = AdapterPluginContext(
        family="static", adapter_key="static", source_identity="supercell.com"
    )
    plugin, selection = default_registry.select(ctx)
    assert selection.plugin_name in {"supercell"}


def test_provider_plugin_registry_selects_ashby_sources_plugin() -> None:
    ensure_provider_plugins()
    ctx = AdapterPluginContext(family="provider_api", adapter_key="ashby_sources")
    plugin, selection = default_registry.select(ctx)
    assert plugin.name == "ashby_sources"
    assert selection.plugin_name == "ashby_sources"


def test_registry_entries_static_filters_redundant_when_provider_present() -> None:
    """When the registry has both a provider source (e.g. SmartRecruiters CD PROJEKT RED) and a static source for the same careers host, static entries for that host are excluded."""
    provider_entry = {
        "name": "CD PROJEKT RED (SmartRecruiters)",
        "studio": "CD PROJEKT RED",
        "adapter": "smartrecruiters",
        "company_id": "CDPROJEKTRED",
        "api_url": "https://api.smartrecruiters.com/v1/companies/CDPROJEKTRED/postings",
        "enabledByDefault": True,
    }
    static_cdprojekt = {
        "name": "Cdprojektred (Manual Website)",
        "studio": "Cdprojektred",
        "adapter": "static",
        "pages": ["https://www.cdprojektred.com/en/jobs"],
        "enabledByDefault": True,
    }
    static_other = {
        "name": "Other Studio (Manual Website)",
        "studio": "Other",
        "adapter": "static",
        "pages": ["https://other.com/careers"],
        "enabledByDefault": True,
    }
    patched_registry = [provider_entry, static_cdprojekt, static_other]
    with mock.patch.object(jobs_common, "STUDIO_SOURCE_REGISTRY", patched_registry):
        static_entries = jobs_common.registry_entries("static")
    # Redundant static (cdprojektred) must be filtered out; other static must remain.
    names = [e.get("name") for e in static_entries]
    assert "Other Studio (Manual Website)" in names
    assert "Cdprojektred (Manual Website)" not in names


def test_registry_entries_bamboohr_derives_static_rows_and_suppresses_static_when_provider_exists() -> (
    None
):
    static_bamboohr = {
        "name": "Wolcen Studios (Manual Website)",
        "studio": "Wolcen Studios",
        "adapter": "static",
        "pages": ["https://wolcenstudios.bamboohr.com/jobs/"],
        "enabledByDefault": True,
    }
    bamboohr_provider = {
        "name": "Wolcen Studios BambooHR",
        "studio": "Wolcen Studios",
        "adapter": "bamboohr",
        "listing_url": "https://wolcenstudios.bamboohr.com/careers",
        "enabledByDefault": True,
    }
    with mock.patch.object(
        jobs_common, "STUDIO_SOURCE_REGISTRY", [static_bamboohr, bamboohr_provider]
    ):
        bamboohr_entries = jobs_common.registry_entries("bamboohr")
        static_entries = jobs_common.registry_entries("static")

    assert any(
        row.get("name") == "Wolcen Studios (Manual Website)"
        and row.get("adapter") == "bamboohr"
        and row.get("migrationSourceAdapter") == "static"
        for row in bamboohr_entries
    )
    assert any(row.get("name") == "Wolcen Studios BambooHR" for row in bamboohr_entries)
    assert all(row.get("name") != "Wolcen Studios (Manual Website)" for row in static_entries)


def test_parse_args_uses_config_backed_output_and_social_defaults() -> None:
    prev_argv = list(sys.argv)
    try:
        sys.argv = ["jobs_fetcher.py"]
        args = jf.parse_args()
    finally:
        sys.argv = prev_argv
    assert Path(args.output_dir) == jf.DEFAULT_OUTPUT_DIR
    assert Path(args.social_config_path) == jf.DEFAULT_SOCIAL_CONFIG_PATH


def test_parse_args_uses_updated_pipeline_concurrency_defaults() -> None:
    prev_argv = list(sys.argv)
    try:
        sys.argv = ["jobs_fetcher.py"]
        args = jf.parse_args()
    finally:
        sys.argv = prev_argv
    assert int(args.max_workers or 0) == 12
    assert int(args.max_per_domain or 0) == 3
    assert int(args.timeout or 0) == 15
    assert float(args.backoff or 0) == 1.2
    assert int(args.adapter_http_concurrency or 0) == 48
    assert int(args.static_detail_concurrency or 0) == 10


def test_default_source_loaders_includes_all_registry_sources() -> None:
    """All DEFAULT_SOURCE_LOADER_NAMES (except static_studio_pages*) are attempted via loaders or static shards."""
    base_expected = {
        n
        for n in jfr.DEFAULT_SOURCE_LOADER_NAMES
        if n
        not in {
            "static_studio_pages",
            "static_studio_pages_a_i",
            "static_studio_pages_j_r",
            "static_studio_pages_s_z",
        }
    }
    loaders_with_social = jf.default_source_loaders(social_enabled=True)
    loader_names = {name for name, _ in loaders_with_social}
    for name in base_expected:
        assert name in loader_names, (
            f"Registry source {name} should be in default loaders when social_enabled=True"
        )
    assert len(loaders_with_social) >= len(base_expected), (
        "Loaders should include all base sources plus static shards"
    )


def test_source_detail_limit_for_caps_chronic_low_yield_sources() -> None:
    limit = source_detail_limit_for(
        "Climax (Manual Website)",
        source_state_rows={
            "Climax (Manual Website)": {
                "lastDetailPagesVisited": 42,
                "lastKeptCount": 1,
                "lastDurationMs": 52000,
                "lastDetailYieldPct": 2,
            }
        },
        discovered_links=28,
        listing_jobs_found=0,
        low_yield_detail_cap=12,
        very_low_yield_detail_cap=6,
    )
    assert limit == 12


def test_source_detail_limit_for_uses_tighter_cap_when_listing_jobs_already_found() -> None:
    limit = source_detail_limit_for(
        "Nintendo (Manual Website)",
        source_state_rows={
            "Nintendo (Manual Website)": {
                "lastDetailPagesVisited": 18,
                "lastKeptCount": 2,
                "lastDurationMs": 26000,
                "lastDetailYieldPct": 4,
            }
        },
        discovered_links=20,
        listing_jobs_found=5,
        low_yield_detail_cap=12,
        very_low_yield_detail_cap=6,
    )
    assert limit == 6
