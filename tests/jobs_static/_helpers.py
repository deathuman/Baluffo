import hashlib
from collections import Counter
from pathlib import Path

from scrapy.http import HtmlResponse, Request

from src.jobs.adapters.plugins.static import (
    ats_wrappers,
    frontier,
    kojima,
    rendered_cards,
    sheet_studios,
)
from src.jobs.adapters.plugins.static._rendered_cards import (
    _looks_like_location_cell,
    _parse_structured_locations,
    extract_rendered_card_jobs,
)
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.adapters.static_helpers import process_detail_link
from src.jobs.common import config as jobs_common_config
from src.jobs.common import registry as jobs_common_registry
from src.jobs.page_gating import classify_job_page
from src.scrapers.spiders.generic_careers import GenericCareersSpider
from tests.jobs_fetcher_helpers import (
    _fixture,
    build_city_garbage_report,
    build_contamination_report,
    build_location_quality_report,
    build_public_text_quality_report,
    default_registry,
    ensure_provider_plugins,
    jf,
    jfr,
    jobs_canonicalize,
    jobs_dedup,
    jobs_registry,
    jobs_reporting,
    patch_jobs_fetcher_aliases,
    scrapy_runner,
    source_detail_limit_for,
    source_detail_retries_for,
    static_helpers,
    static_scrapy,
    workspace_tmpdir,
)

patch_jobs_fetcher_aliases()

FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures"


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
    "patch_jobs_fetcher_aliases",
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
