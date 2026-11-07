# ruff: noqa: F401
import sys
from pathlib import Path
from unittest import mock

from src import jobs_fetcher as jf
from src import jobs_fetcher_registry as jfr
from src.jobs import registry as jobs_registry
from src.jobs.adapters import static_runtime_support
from src.jobs.adapters.plugins import default_registry
from src.jobs.adapters.plugins.provider_api import ensure_registered as ensure_provider_plugins
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.adapters.static_detail_heuristics import (
    source_detail_limit_for,
    source_detail_retries_for,
)
from src.jobs.common import config as jobs_common_config


def test_jobs_fetcher_keeps_parser_compatibility_exports() -> None:
    assert callable(getattr(jf, "parse_ashby_jobs_from_html", None))
    assert callable(getattr(jf, "parse_breezy_jobs_html", None))
    assert callable(getattr(jf, "parse_bamboohr_jobs_html", None))
    assert callable(getattr(jf, "parse_jazzhr_jobs_html", None))
    assert callable(getattr(jf, "parse_recruitee_jobs_payload", None))
    assert callable(getattr(jf, "parse_pinpoint_jobs_payload", None))
    assert callable(getattr(jf, "parse_8bitplay_html", None))
    assert callable(getattr(jf, "parse_gracklehq_html", None))
    assert callable(getattr(jf, "parse_personio_feed_xml", None))
    assert callable(getattr(jf, "parse_epic_games_jobs_payload", None))
    assert callable(getattr(jf, "parse_workday_jobs_html", None))


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
    with mock.patch.object(jobs_registry, "STUDIO_SOURCE_REGISTRY", patched_registry):
        static_entries = jobs_registry.registry_entries("static")
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
        jobs_registry, "STUDIO_SOURCE_REGISTRY", [static_bamboohr, bamboohr_provider]
    ):
        bamboohr_entries = jobs_registry.registry_entries("bamboohr")
        static_entries = jobs_registry.registry_entries("static")

    assert any(
        row.get("name") == "Wolcen Studios (Manual Website)"
        and row.get("adapter") == "bamboohr"
        and row.get("migrationSourceAdapter") == "static"
        for row in bamboohr_entries
    )
    assert any(row.get("name") == "Wolcen Studios BambooHR" for row in bamboohr_entries)
    assert all(row.get("name") != "Wolcen Studios (Manual Website)" for row in static_entries)


def test_registry_entries_suppresses_nextlevelgames_static_when_jazzhr_provider_exists() -> None:
    static_nextlevel = {
        "name": "Next Level Games (Manual Website)",
        "studio": "Next Level Games",
        "adapter": "static",
        "pages": [
            "https://nextlevelgames.com/jobs-at-next-level-games-subsidiary-of-nintendo-co-ltd/"
        ],
        "enabledByDefault": True,
    }
    jazzhr_provider = {
        "name": "Next Level Games (JazzHR)",
        "studio": "Next Level Games",
        "adapter": "jazzhr",
        "board_url": "https://nextlevelgames.applytojob.com/apply",
        "enabledByDefault": True,
    }
    with mock.patch.object(
        jobs_registry, "STUDIO_SOURCE_REGISTRY", [static_nextlevel, jazzhr_provider]
    ):
        static_entries = jobs_registry.registry_entries("static")

    assert all(row.get("name") != "Next Level Games (Manual Website)" for row in static_entries)


def test_registry_entries_keeps_nextlevelgames_static_for_other_jazzhr_provider() -> None:
    static_nextlevel = {
        "name": "Next Level Games (Manual Website)",
        "studio": "Next Level Games",
        "adapter": "static",
        "pages": [
            "https://nextlevelgames.com/jobs-at-next-level-games-subsidiary-of-nintendo-co-ltd/"
        ],
        "enabledByDefault": True,
    }
    jazzhr_provider = {
        "name": "Lost Boys Interactive (JazzHR)",
        "studio": "Lost Boys Interactive",
        "adapter": "jazzhr",
        "board_url": "https://lostboysinteractive.applytojob.com/apply",
        "enabledByDefault": True,
    }
    with mock.patch.object(
        jobs_registry, "STUDIO_SOURCE_REGISTRY", [static_nextlevel, jazzhr_provider]
    ):
        static_entries = jobs_registry.registry_entries("static")

    assert any(row.get("name") == "Next Level Games (Manual Website)" for row in static_entries)


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
    assert int(args.max_workers or 0) == 16
    assert int(args.max_per_domain or 0) == 3
    assert int(args.timeout or 0) == 15
    assert float(args.backoff or 0) == 1.2
    assert int(args.adapter_http_concurrency or 0) == 48
    assert int(args.static_detail_concurrency or 0) == 10
    assert int(args.source_ttl_minutes or 0) == jobs_common_config.DEFAULT_SOURCE_TTL_MINUTES
    assert int(args.circuit_breaker_failures or 0) == (
        jobs_common_config.DEFAULT_CIRCUIT_BREAKER_FAILURES
    )
    assert int(args.circuit_breaker_zero_kept or 0) == (
        jobs_common_config.DEFAULT_CIRCUIT_BREAKER_ZERO_KEPT
    )
    assert int(args.circuit_breaker_cooldown_minutes or 0) == (
        jobs_common_config.DEFAULT_CIRCUIT_BREAKER_COOLDOWN_MINUTES
    )


def test_shared_fetch_defaults_propagate_to_cli_and_bridge(monkeypatch) -> None:
    """Both consumers must read the shared constants, not a copied literal.

    Patching only the config module is what makes this meaningful: an
    import-time alias or a hand-copied default would keep answering the old
    numbers on one of the two paths.
    """
    from src.bridge.task_launch_fetcher_args import build_fetcher_args_from_payload
    from src.jobs import pipeline
    from src.shared.utils import int_or_default

    monkeypatch.setattr(jobs_common_config, "DEFAULT_SOURCE_TTL_MINUTES", 47)
    monkeypatch.setattr(jobs_common_config, "DEFAULT_CIRCUIT_BREAKER_FAILURES", 6)
    monkeypatch.setattr(jobs_common_config, "DEFAULT_CIRCUIT_BREAKER_ZERO_KEPT", 4)
    monkeypatch.setattr(jobs_common_config, "DEFAULT_CIRCUIT_BREAKER_COOLDOWN_MINUTES", 23)
    monkeypatch.setattr(jobs_common_config, "DEFAULT_HOT_SOURCE_CADENCE_MINUTES", 31)
    monkeypatch.setattr(jobs_common_config, "DEFAULT_COLD_SOURCE_CADENCE_MINUTES", 71)
    monkeypatch.setattr(jobs_common_config, "DEFAULT_BROWSER_FALLBACK_COOLDOWN_MINUTES", 19)
    monkeypatch.setattr(jobs_common_config, "DEFAULT_BROWSER_FALLBACK_MAX_WORKERS", -3)
    monkeypatch.setattr(sys, "argv", ["jobs_fetcher.py"])

    cli_args = pipeline.parse_args()
    assert cli_args.source_ttl_minutes == 47
    assert cli_args.circuit_breaker_failures == 6
    assert cli_args.circuit_breaker_zero_kept == 4
    assert cli_args.circuit_breaker_cooldown_minutes == 23
    assert cli_args.hot_source_cadence_minutes == 31
    assert cli_args.cold_source_cadence_minutes == 71
    assert cli_args.browser_fallback_cooldown_minutes == 19
    assert cli_args.browser_fallback_max_workers == -3

    bridge_args, _preset = build_fetcher_args_from_payload(
        {"skipSuccessfulSources": True, "socialEnabled": False},
        safe_int=lambda value, default, minimum, maximum: max(
            minimum, min(maximum, int_or_default(value, default))
        ),
        default_source_loaders=lambda: [],
        failed_source_names_from_latest_report=lambda _names: [],
    )

    def value_after(flag: str) -> str:
        return bridge_args[bridge_args.index(flag) + 1]

    assert value_after("--source-ttl-minutes") == "47"
    assert value_after("--circuit-breaker-failures") == "6"
    assert value_after("--circuit-breaker-cooldown-minutes") == "23"
    assert value_after("--hot-source-cadence-minutes") == "31"
    assert value_after("--cold-source-cadence-minutes") == "71"
    assert value_after("--browser-fallback-cooldown-minutes") == "19"


def test_run_pipeline_resolves_shared_defaults_at_call_time(tmp_path: Path, monkeypatch) -> None:
    """run_pipeline must read config when called, not when the module is imported."""
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    from src.jobs import pipeline

    prepared: dict[str, object] = {}
    finalized: dict[str, object] = {}

    def fake_prepare(**kwargs: object) -> object:
        prepared.update(kwargs)
        setup = MagicMock()
        setup.effective_seed_from_existing_output = False
        setup.fetched_rows_writer = None
        setup.seeded_row_count = 0
        setup.progress_phase = {}
        setup.runtime_payload = {}
        setup.paths = SimpleNamespace(output_dir=tmp_path, json_path=tmp_path / "jobs.json")
        return setup

    def fake_finalize(**kwargs: object) -> dict[str, object]:
        finalized.update(kwargs)
        return {}

    def no_execute(_setup: object) -> None:
        return None

    monkeypatch.setattr(pipeline.pipeline_run_setup_mod, "prepare_pipeline_run", fake_prepare)
    monkeypatch.setattr(
        pipeline.pipeline_execution_flow_mod, "execute_pipeline_sources", no_execute
    )
    monkeypatch.setattr(pipeline.pipeline_finalize_pkg, "finalize_pipeline_run", fake_finalize)

    monkeypatch.setattr(jobs_common_config, "DEFAULT_TIMEOUT_S", 41)
    monkeypatch.setattr(jobs_common_config, "DEFAULT_RETRIES", 7)
    monkeypatch.setattr(jobs_common_config, "DEFAULT_BACKOFF_S", 2.5)
    monkeypatch.setattr(jobs_common_config, "DEFAULT_FETCH_STRATEGY", "http")
    monkeypatch.setattr(jobs_common_config, "DEFAULT_ADAPTER_HTTP_CONCURRENCY", 43)
    monkeypatch.setattr(pipeline.community_adapter, "DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY", 9)
    monkeypatch.setattr(jobs_common_config, "DEFAULT_HOT_SOURCE_CADENCE_MINUTES", 31)
    monkeypatch.setattr(jobs_common_config, "DEFAULT_COLD_SOURCE_CADENCE_MINUTES", 71)
    monkeypatch.setattr(jobs_common_config, "DEFAULT_CIRCUIT_BREAKER_FAILURES", 6)
    monkeypatch.setattr(jobs_common_config, "DEFAULT_CIRCUIT_BREAKER_COOLDOWN_MINUTES", 23)
    monkeypatch.setattr(jobs_common_config, "DEFAULT_BROWSER_FALLBACK_COOLDOWN_MINUTES", 19)
    monkeypatch.setattr(jobs_common_config, "DEFAULT_BROWSER_FALLBACK_MAX_WORKERS", -3)
    monkeypatch.setattr(jobs_common_config, "DEFAULT_CIRCUIT_BREAKER_ZERO_KEPT", 4)
    monkeypatch.setattr(jobs_common_config, "DEFAULT_SOCIAL_LOOKBACK_MINUTES", 37)
    monkeypatch.setattr(jobs_common_config, "DEFAULT_STATIC_DETAIL_CONCURRENCY", 11)

    assert pipeline.run_pipeline(output_dir=tmp_path) == {}

    assert prepared["timeout_s"] == 41
    assert prepared["retries"] == 7
    assert prepared["backoff_s"] == 2.5
    assert prepared["fetch_strategy"] == "http"
    assert prepared["adapter_http_concurrency"] == 43
    assert prepared["google_sheets_redirect_concurrency"] == 9
    assert prepared["hot_source_cadence_minutes"] == 31
    assert prepared["cold_source_cadence_minutes"] == 71
    assert prepared["circuit_breaker_failures"] == 6
    assert prepared["circuit_breaker_cooldown_minutes"] == 23
    assert prepared["browser_fallback_cooldown_minutes"] == 19
    assert prepared["browser_fallback_max_workers"] == -3
    assert prepared["social_lookback_minutes"] == 37
    assert prepared["static_detail_concurrency"] == 11
    assert finalized["circuit_breaker_zero_kept"] == 4


def test_parse_args_accepts_pending_provider_migration_fetch_flag() -> None:
    prev_argv = list(sys.argv)
    try:
        sys.argv = ["jobs_fetcher.py", "--include-pending-provider-migration"]
        args = jf.parse_args()
    finally:
        sys.argv = prev_argv
    assert args.include_pending_provider_migration is True


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
    # Reddit is intentionally excluded from the default loader fan-out for now because it
    # currently burns a lot of time while yielding no usable jobs in uncapped runs.
    assert "social_reddit" not in base_expected
    loaders_with_social = jf.default_source_loaders(social_enabled=True)
    loader_names = {name for name, _ in loaders_with_social}
    assert "social_reddit" not in loader_names
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


def test_source_detail_limit_for_tails_off_from_detail_fetch_history() -> None:
    limit = source_detail_limit_for(
        "Stillfront (Sheet)",
        source_state_rows={
            "Stillfront (Sheet)": {
                "lastDetailPagesVisited": 54,
                "lastKeptCount": 21,
                "lastDurationMs": 145137,
                "lastDetailYieldPct": 39,
                "lastStageTimingsMs": {"detailFetch": 217029},
            }
        },
        discovered_links=54,
        listing_jobs_found=21,
        low_yield_detail_cap=12,
        very_low_yield_detail_cap=6,
    )
    assert limit == 6


def test_source_detail_limit_for_uncapped_zero_caps_disables_low_yield_truncation() -> None:
    source_state_rows = {
        "Climax (Manual Website)": {
            "lastDetailPagesVisited": 42,
            "lastKeptCount": 1,
            "lastDurationMs": 52000,
            "lastDetailYieldPct": 2,
        }
    }
    regular_limit = source_detail_limit_for(
        "Climax (Manual Website)",
        source_state_rows=source_state_rows,
        discovered_links=28,
        listing_jobs_found=0,
        low_yield_detail_cap=12,
        very_low_yield_detail_cap=6,
    )
    uncapped_limit = source_detail_limit_for(
        "Climax (Manual Website)",
        source_state_rows=source_state_rows,
        discovered_links=28,
        listing_jobs_found=0,
        low_yield_detail_cap=0,
        very_low_yield_detail_cap=0,
        uncapped_deep_static=True,
    )
    assert regular_limit == 12
    assert uncapped_limit == 28


def test_source_detail_retries_for_reduces_tail_retry_pressure() -> None:
    retries = source_detail_retries_for(
        "Stillfront (Sheet)",
        source_state_rows={
            "Stillfront (Sheet)": {
                "lastDetailPagesVisited": 54,
                "lastKeptCount": 21,
                "lastDurationMs": 145137,
                "lastDetailYieldPct": 39,
                "lastStageTimingsMs": {"detailFetch": 217029},
            }
        },
        base_retries=2,
    )
    assert retries == 0


def test_source_detail_retries_for_first_run_listing_rows_skips_detail_retries() -> None:
    retries = source_detail_retries_for(
        "Fresh Static Source (Sheet)",
        source_state_rows={},
        base_retries=2,
        listing_jobs_found=3,
    )
    assert retries == 0


def test_source_detail_retries_for_uncapped_deep_static_keeps_base_retries() -> None:
    retries = source_detail_retries_for(
        "Stillfront (Sheet)",
        source_state_rows={
            "Stillfront (Sheet)": {
                "lastDetailPagesVisited": 54,
                "lastKeptCount": 21,
                "lastDurationMs": 145137,
                "lastDetailYieldPct": 39,
                "lastStageTimingsMs": {"detailFetch": 217029},
            }
        },
        base_retries=2,
        uncapped_deep_static=True,
    )
    assert retries == 2


def test_build_static_source_runtime_config_reads_uncapped_deep_env_overrides() -> None:
    with mock.patch.dict(
        "os.environ",
        {
            "BALUFFO_STATIC_SOURCE_TIME_BUDGET_S": "180",
            "BALUFFO_STATIC_LOW_YIELD_DETAIL_CAP": "0",
            "BALUFFO_STATIC_VERY_LOW_YIELD_DETAIL_CAP": "0",
            "BALUFFO_STATIC_DETAIL_HEURISTICS_PROFILE": "broad",
            "BALUFFO_UNCAPPED_DEEP_STATIC": "1",
        },
        clear=False,
    ):
        runtime = static_runtime_support.build_static_source_runtime_config(10)

    assert runtime.static_source_time_budget_s == 180
    assert runtime.low_yield_detail_cap == 0
    assert runtime.very_low_yield_detail_cap == 0
    assert runtime.static_profile == "broad"
    assert runtime.uncapped_deep_static is True


def test_build_static_source_runtime_config_regular_still_clamps_zero_caps() -> None:
    with mock.patch.dict(
        "os.environ",
        {
            "BALUFFO_STATIC_LOW_YIELD_DETAIL_CAP": "0",
            "BALUFFO_STATIC_VERY_LOW_YIELD_DETAIL_CAP": "0",
        },
        clear=False,
    ):
        runtime = static_runtime_support.build_static_source_runtime_config(10)

    assert runtime.low_yield_detail_cap == 4
    assert runtime.very_low_yield_detail_cap == 2
    assert runtime.uncapped_deep_static is False
