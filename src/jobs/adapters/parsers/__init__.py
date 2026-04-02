"""Canonical provider parser functions.

This package is the canonical location for all provider-specific parsing logic.
The old module src.jobs.adapters.provider_parsers is now a compat shim that
re-exports everything from here.
"""

from __future__ import annotations

from .json_payloads import (
    parse_epic_games_jobs_payload,
    parse_greenhouse_jobs_payload,
    parse_lever_jobs_payload,
    parse_pinpoint_jobs_payload,
    parse_recruitee_jobs_payload,
    parse_smartrecruiters_jobs_payload,
    parse_workable_jobs_payload,
)
from .location import (
    _looks_like_country_token,
    parse_generic_location_fields,
    parse_greenhouse_location,
)
from .personio import parse_personio_feed_xml
from .provider_html import (
    parse_ashby_jobs_from_html,
    parse_breezy_jobs_html,
    parse_jazzhr_jobs_html,
)
from .structured_listing import (
    parse_bamboohr_jobs_html,
    parse_workday_jobs_html,
)

__all__ = [
    # Location utilities
    "_looks_like_country_token",
    "parse_greenhouse_location",
    "parse_generic_location_fields",
    # JSON payload parsers
    "parse_greenhouse_jobs_payload",
    "parse_lever_jobs_payload",
    "parse_smartrecruiters_jobs_payload",
    "parse_workable_jobs_payload",
    "parse_epic_games_jobs_payload",
    "parse_recruitee_jobs_payload",
    "parse_pinpoint_jobs_payload",
    # HTML parsers
    "parse_ashby_jobs_from_html",
    "parse_breezy_jobs_html",
    "parse_jazzhr_jobs_html",
    # Structured listing parsers
    "parse_bamboohr_jobs_html",
    "parse_workday_jobs_html",
    # XML parser
    "parse_personio_feed_xml",
]
