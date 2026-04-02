"""Compat shim for provider parsers.

Canonical location: src.jobs.adapters.parsers
This module re-exports everything from the new package to preserve
existing import paths across the codebase.
"""

from __future__ import annotations

from src.jobs.adapters.parsers import (
    _looks_like_country_token,
    parse_ashby_jobs_from_html,
    parse_bamboohr_jobs_html,
    parse_breezy_jobs_html,
    parse_epic_games_jobs_payload,
    parse_generic_location_fields,
    parse_greenhouse_jobs_payload,
    parse_greenhouse_location,
    parse_jazzhr_jobs_html,
    parse_lever_jobs_payload,
    parse_personio_feed_xml,
    parse_pinpoint_jobs_payload,
    parse_recruitee_jobs_payload,
    parse_smartrecruiters_jobs_payload,
    parse_workable_jobs_payload,
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
    "parse_personio_feed_xml",
    "parse_pinpoint_jobs_payload",
    "parse_recruitee_jobs_payload",
    "parse_smartrecruiters_jobs_payload",
    "parse_workable_jobs_payload",
    "parse_workday_jobs_html",
]
