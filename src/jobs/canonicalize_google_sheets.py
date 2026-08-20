"""Google Sheets title/category repair for canonical job normalization.

AI boundary owns: Google Sheets title hydration, category label repair, provider title
lookup, and category link-status resolution used during canonical job normalization.
AI boundary implement in: this coordinator re-exports the implementation leaves
(slug, category, link, provider, title); public entrypoints stay here.
AI boundary search before contracts: DATA_CONTRACT.md, CanonicalJob models, adapter parsers, and jobs quality tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused canonicalization/quality tests.
"""

from __future__ import annotations

from src.jobs.canonicalize_google_sheets_category import (
    _is_google_sheets_category_label as _is_google_sheets_category_label,
)
from src.jobs.canonicalize_google_sheets_link import (
    UNKNOWN_COMPANY_LABEL as UNKNOWN_COMPANY_LABEL,
)
from src.jobs.canonicalize_google_sheets_link import (
    _looks_like_google_sheets_category_row_noise as _looks_like_google_sheets_category_row_noise,
)
from src.jobs.canonicalize_google_sheets_provider import (
    _GOOGLE_SHEETS_CATEGORY_LINK_STAT_KEYS as _GOOGLE_SHEETS_CATEGORY_LINK_STAT_KEYS,
)
from src.jobs.canonicalize_google_sheets_provider import (
    _GOOGLE_SHEETS_TITLE_HYDRATION_STAT_KEYS as _GOOGLE_SHEETS_TITLE_HYDRATION_STAT_KEYS,
)
from src.jobs.canonicalize_google_sheets_provider import (
    GoogleSheetsCategoryLinkStatusResolver as GoogleSheetsCategoryLinkStatusResolver,
)
from src.jobs.canonicalize_google_sheets_provider import (
    GoogleSheetsProviderTitleResolver as GoogleSheetsProviderTitleResolver,
)
from src.jobs.canonicalize_google_sheets_provider import (
    _google_sheets_provider_title_target as _google_sheets_provider_title_target,
)
from src.jobs.canonicalize_google_sheets_slug import (
    _is_google_sheets_repairable_broad_title as _is_google_sheets_repairable_broad_title,
)
from src.jobs.canonicalize_google_sheets_title import (
    _derive_google_sheets_title_from_url as _derive_google_sheets_title_from_url,
)
from src.jobs.canonicalize_google_sheets_title import (
    _google_sheets_repaired_title_or_reason as _google_sheets_repaired_title_or_reason,
)
