from __future__ import annotations

import json
from pathlib import Path

from src import jobs_fetcher as jf
from src import jobs_fetcher_registry as jfr
from src.exceptions import AdapterValidationError
from src.jobs import canonicalize as jobs_canonicalize
from src.jobs import dedup as jobs_dedup
from src.jobs import registry as jobs_registry
from src.jobs import reporting as jobs_reporting
from src.jobs.adapters import static_helpers, static_scrapy
from src.jobs.adapters.plugins import default_registry
from src.jobs.adapters.plugins.provider_api import ensure_registered as ensure_provider_plugins
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.adapters.static_helpers import source_detail_limit_for, source_detail_retries_for
from src.jobs.browser_fallback import BrowserFallbackCircuitBreaker
from src.jobs.contamination_audit import (
    build_city_garbage_report,
    build_contamination_report,
    build_location_quality_report,
    build_public_text_quality_report,
)
from src.scrapers import runner as scrapy_runner
from tests.helpers.temp_paths import workspace_tmpdir

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"

__all__ = [
    "AdapterPluginContext",
    "AdapterValidationError",
    "BrowserFallbackCircuitBreaker",
    "FIXTURES_DIR",
    "build_contamination_report",
    "build_city_garbage_report",
    "build_public_text_quality_report",
    "build_location_quality_report",
    "default_registry",
    "ensure_provider_plugins",
    "_fixture",
    "_fixture_json",
    "jobs_canonicalize",
    "jobs_registry",
    "jobs_dedup",
    "jobs_reporting",
    "jfr",
    "jf",
    "patch_jobs_fetcher_aliases",
    "scrapy_runner",
    "source_detail_limit_for",
    "source_detail_retries_for",
    "static_helpers",
    "static_scrapy",
    "workspace_tmpdir",
]


def _fixture(name: str) -> str:
    return (FIXTURES_DIR / name).read_text(encoding="utf-8")


def _fixture_json(name: str):
    return json.loads(_fixture(name))


def patch_jobs_fetcher_aliases() -> None:
    jf.canonicalize_job = jobs_canonicalize.canonicalize_job
    jf.canonicalize_job_with_reason = jobs_canonicalize.canonicalize_job_with_reason
    jf.canonicalize_google_sheets_rows = jobs_canonicalize.canonicalize_google_sheets_rows
    jf.deduplicate_jobs = jobs_dedup.deduplicate_jobs
