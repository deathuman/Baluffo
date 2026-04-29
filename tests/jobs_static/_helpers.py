import hashlib
from collections import Counter
from pathlib import Path
from types import SimpleNamespace

from scrapy.http import HtmlResponse, Request

from src import jobs_fetcher as jf
from src import jobs_fetcher_registry as jfr
from src.jobs import canonicalize as jobs_canonicalize
from src.jobs import dedup as jobs_dedup
from src.jobs import registry as jobs_registry
from src.jobs.adapters import static_helpers, static_scrapy
from src.jobs.adapters.plugins import default_registry
from src.jobs.adapters.plugins.provider_api import ensure_registered as ensure_provider_plugins
from src.jobs.adapters.plugins.static import (
    ats_wrappers,
    frontier,
    kojima,
    sheet_studios,
)
from src.jobs.adapters.plugins.static._rendered_cards import (
    _looks_like_location_cell,
    _parse_structured_locations,
    can_handle_rendered_cards,
    extract_rendered_card_jobs,
    run_rendered_cards_plugin,
)
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.adapters.static_helpers import (
    process_detail_link,
    source_detail_limit_for,
    source_detail_retries_for,
)
from src.jobs.common import config as jobs_common_config
from src.jobs.common import registry as jobs_common_registry
from src.jobs.contamination_audit import (
    build_city_garbage_report,
    build_contamination_report,
    build_location_quality_report,
    build_public_text_quality_report,
)
from src.jobs.page_gating import classify_job_page
from src.scrapers import runner as scrapy_runner
from src.scrapers.spiders.generic_careers import GenericCareersSpider
from tests.helpers import jobs_reporting
from tests.helpers.job_fixtures import _fixture
from tests.helpers.temp_paths import workspace_tmpdir

FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures"
rendered_cards = SimpleNamespace(
    can_handle=can_handle_rendered_cards,
    run=run_rendered_cards_plugin,
)


def _read_fixture(name: str) -> str:
    return (FIXTURES_DIR / name).read_text(encoding="utf-8")


__all__ = [
    "AdapterPluginContext",
    "Counter",
    "FIXTURES_DIR",
    "GenericCareersSpider",
    "HtmlResponse",
    "Path",
    "Request",
    "_fixture",
    "_looks_like_location_cell",
    "_parse_structured_locations",
    "_read_fixture",
    "ats_wrappers",
    "build_city_garbage_report",
    "build_contamination_report",
    "build_location_quality_report",
    "build_public_text_quality_report",
    "classify_job_page",
    "default_registry",
    "ensure_provider_plugins",
    "extract_rendered_card_jobs",
    "frontier",
    "hashlib",
    "jf",
    "jobs_canonicalize",
    "jobs_common_config",
    "jobs_common_registry",
    "jobs_dedup",
    "jobs_registry",
    "jobs_reporting",
    "jfr",
    "kojima",
    "process_detail_link",
    "rendered_cards",
    "scrapy_runner",
    "sheet_studios",
    "source_detail_limit_for",
    "source_detail_retries_for",
    "static_helpers",
    "static_scrapy",
    "workspace_tmpdir",
]
