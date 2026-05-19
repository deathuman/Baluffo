"""Compatibility export tables for the stable ``src.jobs_fetcher`` facade."""

from __future__ import annotations

import src.jobs.common.contracts_fetch_report as contracts_fetch_report_mod
import src.jobs.common.contracts_runtime as contracts_runtime_mod
import src.jobs.common.contracts_source_reports as contracts_source_reports_mod
from src.jobs import canonicalize as canonicalize_mod
from src.jobs import dedup as dedup_mod
from src.jobs import parsers as parsers_mod
from src.jobs import pipeline as pipeline_mod
from src.jobs import registry as registry_mod
from src.jobs import state as state_mod
from src.jobs import transport as transport_mod
from src.jobs.adapters import community as community_mod
from src.jobs.adapters import provider_api as provider_api_mod
from src.jobs.adapters import social as social_mod
from src.jobs.adapters import static as static_mod
from src.jobs.common import config as common_config_mod
from src.jobs.common import datetime_utils as common_datetime_utils_mod
from src.jobs.common import diagnostics as common_diagnostics_mod
from src.jobs.common import fetch as common_fetch_mod
from src.jobs.common import url as common_url_mod


def _module_attr_exports(module: object, names: tuple[str, ...]) -> dict[str, tuple[object, str]]:
    return {name: (module, name) for name in names}


COMPAT_MODULE_EXPORTS: dict[str, tuple[object, str]] = {}
COMPAT_MODULE_EXPORTS.update(
    _module_attr_exports(
        parsers_mod,
        (
            "parse_8bitplay_html",
            "parse_ashby_jobs_from_html",
            "parse_bamboohr_jobs_html",
            "parse_breezy_jobs_html",
            "parse_epic_games_jobs_payload",
            "parse_gamesindustry_html",
            "parse_gamejobs_html",
            "parse_google_sheets_csv",
            "parse_gracklehq_html",
            "parse_greenhouse_jobs_payload",
            "parse_jazzhr_jobs_html",
            "parse_jobpostings_from_html",
            "parse_lever_jobs_payload",
            "parse_mastodon_payload",
            "parse_oracle_hcm_requisitions_payload",
            "parse_personio_feed_xml",
            "parse_pinpoint_jobs_payload",
            "parse_recruitee_jobs_payload",
            "parse_reddit_json_payload",
            "parse_reddit_rss_payload",
            "parse_remote_ok_payload",
            "parse_smartrecruiters_jobs_payload",
            "parse_teamtailor_listing_links",
            "parse_wellfound_html",
            "parse_workable_jobs_payload",
            "parse_workday_jobs_html",
            "parse_workwithindies_html",
            "parse_x_payload",
            "parse_x_rss_payload",
        ),
    )
)
COMPAT_MODULE_EXPORTS.update(
    _module_attr_exports(
        canonicalize_mod,
        (
            "LIGHTWEIGHT_OUTPUT_FIELDS",
            "OPTIONAL_FIELDS",
            "OUTPUT_FIELDS",
            "REQUIRED_FIELDS",
            "GoogleSheetsProviderTitleResolver",
            "UNKNOWN_COMPANY_LABEL",
            "canonicalize_google_sheets_rows",
            "canonicalize_job",
            "canonicalize_job_with_reason",
            "map_profession",
        ),
    )
)
COMPAT_MODULE_EXPORTS.update(
    _module_attr_exports(
        transport_mod,
        (
            "AsyncHttpTextFetcher",
            "DEFAULT_REDIRECT_HEADERS",
            "PooledRedirectResolver",
            "default_fetch_text",
            "resolve_fetch_text_impl",
        ),
    )
)
COMPAT_MODULE_EXPORTS.update(
    {
        "normalize_fetch_report_payload": (
            contracts_fetch_report_mod,
            "normalize_fetch_report_payload",
        ),
        "normalize_runtime_payload": (contracts_runtime_mod, "normalize_runtime_payload"),
        "normalize_source_report_row": (
            contracts_source_reports_mod,
            "normalize_source_report_row",
        ),
    }
)
COMPAT_MODULE_EXPORTS.update(
    _module_attr_exports(
        state_mod,
        (
            "should_skip_source_by_cadence",
            "should_skip_source_by_ttl",
        ),
    )
)
COMPAT_MODULE_EXPORTS.update(
    _module_attr_exports(
        community_mod,
        (
            "DEFAULT_GOOGLE_SHEET_GID",
            "DEFAULT_GOOGLE_SHEET_ID",
            "DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY",
            "google_sheet_candidate_urls",
            "run_8bitplay_source",
            "run_epic_games_careers_source",
            "run_gamesindustry_source",
            "run_gamejobs_source",
            "run_google_sheets_source",
            "run_gracklehq_source",
            "run_remote_ok_source",
            "run_wellfound_source",
            "run_workwithindies_source",
        ),
    )
)
COMPAT_MODULE_EXPORTS.update(
    _module_attr_exports(
        provider_api_mod,
        (
            "run_ashby_sources_source",
            "run_bamboohr_sources_source",
            "run_breezy_sources_source",
            "run_greenhouse_boards_source",
            "run_jazzhr_sources_source",
            "run_lever_sources_source",
            "run_oracle_hcm_sources_source",
            "run_personio_sources_source",
            "run_pinpoint_sources_source",
            "run_recruitee_sources_source",
            "run_smartrecruiters_sources_source",
            "run_teamtailor_sources_source",
            "run_workable_sources_source",
            "run_workday_sources_source",
        ),
    )
)
COMPAT_MODULE_EXPORTS.update(
    _module_attr_exports(
        social_mod,
        (
            "run_social_mastodon_source",
            "run_social_reddit_source",
            "run_social_x_source",
        ),
    )
)
COMPAT_MODULE_EXPORTS.update(
    _module_attr_exports(
        static_mod,
        (
            "run_static_source_entry_source",
            "run_static_studio_pages_a_i_source",
            "run_static_studio_pages_j_r_source",
            "run_static_studio_pages_s_z_source",
            "run_static_studio_pages_source",
        ),
    )
)
COMPAT_MODULE_EXPORTS.update(
    _module_attr_exports(
        common_config_mod,
        (
            "DEFAULT_ADAPTER_HTTP_CONCURRENCY",
            "DEFAULT_BACKOFF_S",
            "DEFAULT_COLD_SOURCE_CADENCE_MINUTES",
            "DEFAULT_FETCH_STRATEGY",
            "DEFAULT_HOT_SOURCE_CADENCE_MINUTES",
            "DEFAULT_RETRIES",
            "DEFAULT_SCRAPY_VALIDATION_STRICT",
            "DEFAULT_STATIC_DETAIL_CONCURRENCY",
            "DEFAULT_STATIC_DETAIL_HEURISTICS_PROFILE",
            "DEFAULT_TIMEOUT_S",
            "EPIC_CAREERS_API_URL",
            "GAMES_INDUSTRY_URLS",
            "GREENHOUSE_JOBS_URL_TEMPLATE",
            "REMOTE_OK_URLS",
        ),
    )
)
COMPAT_MODULE_EXPORTS.update(_module_attr_exports(common_datetime_utils_mod, ("to_iso",)))
COMPAT_MODULE_EXPORTS.update(
    _module_attr_exports(common_diagnostics_mod, ("set_source_diagnostics",))
)
COMPAT_MODULE_EXPORTS.update(_module_attr_exports(common_fetch_mod, ("fetch_with_retries",)))
COMPAT_MODULE_EXPORTS.update(_module_attr_exports(common_url_mod, ("fingerprint_url",)))
COMPAT_MODULE_EXPORTS.update(
    _module_attr_exports(
        registry_mod,
        (
            "DEFAULT_OUTPUT_DIR",
            "DEFAULT_SOCIAL_CONFIG_PATH",
            "DEFAULT_SOCIAL_LOOKBACK_MINUTES",
            "DEFAULT_SOCIAL_MIN_CONFIDENCE",
            "GOOGLE_SHEETS_SOURCES",
            "SOURCE_REGISTRY_ACTIVE_PATH",
            "SOURCE_REGISTRY_PENDING_PATH",
            "load_registry_from_file",
            "load_social_config",
            "read_approved_since_last_run",
        ),
    )
)
COMPAT_MODULE_EXPORTS.update(_module_attr_exports(dedup_mod, ("deduplicate_jobs",)))
COMPAT_MODULE_EXPORTS.update(_module_attr_exports(pipeline_mod, ("default_source_loaders",)))
