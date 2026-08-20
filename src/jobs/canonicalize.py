"""Canonicalization and typed boundary helpers.

AI boundary owns: raw job to CanonicalJob conversion, field cleanup, contract shaping, and quality drop reasons.
AI boundary implement in: canonicalize_google_sheets.py (title/category repair), canonicalize_locations.py
(location/sector audit and canonical job build), canonicalize_redirects.py (sheet redirect resolution);
this module is the thin coordinator that keeps the public entry points.
AI boundary search before contracts: DATA_CONTRACT.md, CanonicalJob models, adapter parsers, and jobs quality tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused canonicalization/quality tests.
"""

from __future__ import annotations

import time
from collections import Counter
from collections.abc import Callable
from typing import Any, cast

from src.jobs.common.datetime_utils import to_iso as to_iso
from src.jobs.common.heuristics import (
    compute_focus_score as compute_focus_score,
)
from src.jobs.common.heuristics import (
    compute_quality_score as compute_quality_score,
)
from src.jobs.common.heuristics import (
    map_profession as map_profession,
)
from src.jobs.interfaces import JobProcessor
from src.jobs.models import CanonicalJob, RawJob
from src.jobs.text_utils import (
    clean_text as clean_text,
)
from src.jobs.text_utils import (
    norm_text as norm_text,
)
from src.jobs.text_utils import (
    normalize_url as normalize_url,
)
from src.jobs.transport import PooledRedirectResolver

from .canonicalize_google_sheets import (
    GoogleSheetsCategoryLinkStatusResolver as GoogleSheetsCategoryLinkStatusResolver,
)
from .canonicalize_google_sheets import (
    GoogleSheetsProviderTitleResolver as GoogleSheetsProviderTitleResolver,
)
from .canonicalize_google_sheets import (
    _derive_google_sheets_title_from_url as _derive_google_sheets_title_from_url,
)
from .canonicalize_google_sheets import (
    _google_sheets_provider_title_target as _google_sheets_provider_title_target,
)
from .canonicalize_google_sheets import (
    _is_google_sheets_category_label as _is_google_sheets_category_label,
)
from .canonicalize_locations import (
    canonicalize_job as canonicalize_job,
)
from .canonicalize_locations import (
    canonicalize_job_with_reason as canonicalize_job_with_reason,
)
from .canonicalize_locations import (
    reset_location_quality_audit as reset_location_quality_audit,
)
from .canonicalize_locations import (
    reset_sector_quality_audit as reset_sector_quality_audit,
)
from .canonicalize_locations import (
    snapshot_sector_quality_audit as snapshot_sector_quality_audit,
)
from .canonicalize_redirects import (
    DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY as DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY,
)
from .canonicalize_redirects import (
    GOOGLE_SHEETS_CATEGORY_LINK_STATUS_CONCURRENCY as GOOGLE_SHEETS_CATEGORY_LINK_STATUS_CONCURRENCY,
)
from .canonicalize_redirects import (
    canonicalize_google_sheets_rows as canonicalize_google_sheets_rows,
)
from .common import config as common_config

UNKNOWN_COMPANY_LABEL = common_config.UNKNOWN_COMPANY_LABEL
UNTRUSTWORTHY_COMPANY_LABELS = common_config.UNTRUSTWORTHY_COMPANY_LABELS
REQUIRED_FIELDS = common_config.REQUIRED_FIELDS
OPTIONAL_FIELDS = common_config.OPTIONAL_FIELDS
OUTPUT_FIELDS = common_config.OUTPUT_FIELDS
LIGHTWEIGHT_OUTPUT_FIELDS = common_config.LIGHTWEIGHT_OUTPUT_FIELDS
TARGET_PROFESSIONS = common_config.TARGET_PROFESSIONS
DEFAULT_CANONICAL_STRICT_URL = common_config.DEFAULT_CANONICAL_STRICT_URL
REDIRECT_RESOLUTION_SKIP_SOURCES = {"gracklehq"}


class CanonicalNormalizer(JobProcessor):
    """Structural normalizer implementing the JobProcessor protocol."""

    def __init__(
        self,
        source: str,
        fetched_at: str,
        resolve_redirect_url: Callable[[str], str] | None = None,
        redirect_resolver: PooledRedirectResolver | None = None,
        redirect_concurrency: int = DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY,
        title_hydration_resolver: GoogleSheetsProviderTitleResolver | None = None,
        category_link_status_resolver: GoogleSheetsCategoryLinkStatusResolver | None = None,
        progress_callback: Callable[..., Any] | None = None,
    ) -> None:
        self.source = source
        self.fetched_at = fetched_at
        self.resolve_redirect_url = resolve_redirect_url
        self.redirect_resolver = redirect_resolver
        self.redirect_concurrency = redirect_concurrency
        self.title_hydration_resolver = title_hydration_resolver
        self.category_link_status_resolver = category_link_status_resolver
        self.progress_callback = progress_callback
        self.stats: dict[str, Any] = {}
        self.drop_reasons: Counter[str] = Counter()

    def process(self, jobs: list[CanonicalJob], **options: Any) -> list[CanonicalJob]:
        # Implementation accepts RawJob masquerading as CanonicalJob initially
        # during the adapter -> pipeline boundary transition.
        raw_rows = cast(list[RawJob], jobs)
        if self.source.startswith("google_sheets"):
            canonical_batch, self.drop_reasons, self.stats = canonicalize_google_sheets_rows(
                raw_rows,
                source=self.source,
                fetched_at=self.fetched_at,
                redirect_resolver=self.redirect_resolver,
                redirect_concurrency=self.redirect_concurrency,
                title_hydration_resolver=self.title_hydration_resolver,
                category_link_status_resolver=self.category_link_status_resolver,
                progress_callback=self.progress_callback,
            )
            return canonical_batch

        canonical_batch = []
        canonical_started = time.perf_counter()
        for raw in raw_rows:
            normalized, drop_reason = canonicalize_job_with_reason(
                raw,
                source=self.source,
                fetched_at=self.fetched_at,
                resolve_redirect_url=self.resolve_redirect_url,
            )
            if normalized:
                canonical_batch.append(normalized)
            elif drop_reason:
                self.drop_reasons[drop_reason] += 1

        self.stats["canonicalize_ms"] = int((time.perf_counter() - canonical_started) * 1000)
        return canonical_batch
