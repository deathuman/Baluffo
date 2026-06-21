"""Compat shim for provider parsers.

Canonical location: src.jobs.adapters.parsers
This module re-exports everything from the new package to preserve
existing import paths across the codebase.

AI boundary owns: compatibility exports for provider parser helpers.
AI boundary implement in: this file for stable import compatibility only; parser behavior belongs in adapters/parsers leaves.
AI boundary search before contracts: provider adapter callers, parser modules, and compatibility tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused provider parser tests.
"""

from __future__ import annotations

from src.jobs.adapters.parsers.json_payloads import (
    parse_epic_games_jobs_payload,
    parse_greenhouse_jobs_payload,
    parse_lever_jobs_payload,
    parse_oracle_hcm_requisitions_payload,
    parse_pinpoint_jobs_payload,
    parse_recruitee_jobs_payload,
    parse_smartrecruiters_jobs_payload,
    parse_workable_jobs_payload,
)
from src.jobs.adapters.parsers.location import (
    _looks_like_country_token,
    normalize_location_details,
    parse_generic_location_fields,
    parse_greenhouse_location,
)
from src.jobs.adapters.parsers.personio import parse_personio_feed_xml
from src.jobs.adapters.parsers.provider_html import (
    parse_ashby_jobs_from_html,
    parse_breezy_jobs_html,
    parse_jazzhr_jobs_html,
)
from src.jobs.adapters.parsers.structured_listing import (
    parse_bamboohr_jobs_html,
    parse_workday_jobs_html,
)

__all__ = [
    "_looks_like_country_token",
    "parse_ashby_jobs_from_html",
    "parse_bamboohr_jobs_html",
    "parse_breezy_jobs_html",
    "parse_epic_games_jobs_payload",
    "parse_generic_location_fields",
    "parse_greenhouse_jobs_payload",
    "parse_greenhouse_location",
    "parse_jazzhr_jobs_html",
    "parse_lever_jobs_payload",
    "parse_oracle_hcm_requisitions_payload",
    "parse_personio_feed_xml",
    "parse_pinpoint_jobs_payload",
    "parse_recruitee_jobs_payload",
    "parse_smartrecruiters_jobs_payload",
    "parse_workable_jobs_payload",
    "parse_workday_jobs_html",
    "normalize_location_details",
]
