#!/usr/bin/env python3
"""Registry/constants for jobs fetch source execution and reporting."""

from __future__ import annotations

from typing import Dict, List

DEFAULT_SOURCE_LOADER_NAMES: List[str] = [
    "google_sheets",
    "google_sheets_1er2oaxo",
    "google_sheets_1mvqhxat",
    "remote_ok",
    "gamesindustry",
    "epic_games_careers",
    "greenhouse_boards",
    "teamtailor_sources",
    "lever_sources",
    "smartrecruiters_sources",
    "workable_sources",
    "ashby_sources",
    "personio_sources",
    "scrapy_static_sources",
    "social_reddit",
    "social_x",
    "social_mastodon",
]

SOURCE_REPORT_META: Dict[str, Dict[str, str]] = {
    "google_sheets": {"adapter": "csv", "studio": "community_sheet", "fetchStrategy": "http"},
    "google_sheets_1er2oaxo": {"adapter": "csv", "studio": "community_sheet", "fetchStrategy": "http"},
    "google_sheets_1mvqhxat": {"adapter": "csv", "studio": "community_sheet", "fetchStrategy": "http"},
    "remote_ok": {"adapter": "api", "studio": "remote_ok", "fetchStrategy": "http"},
    "gamesindustry": {"adapter": "html", "studio": "gamesindustry", "fetchStrategy": "http"},
    "epic_games_careers": {"adapter": "api", "studio": "epic_games", "fetchStrategy": "http"},
    "greenhouse_boards": {"adapter": "greenhouse", "studio": "multiple", "fetchStrategy": "http"},
    "teamtailor_sources": {"adapter": "teamtailor", "studio": "multiple", "fetchStrategy": "http"},
    "lever_sources": {"adapter": "lever", "studio": "multiple", "fetchStrategy": "http"},
    "smartrecruiters_sources": {"adapter": "smartrecruiters", "studio": "multiple", "fetchStrategy": "http"},
    "workable_sources": {"adapter": "workable", "studio": "multiple", "fetchStrategy": "http"},
    "ashby_sources": {"adapter": "ashby", "studio": "multiple", "fetchStrategy": "http"},
    "personio_sources": {"adapter": "personio", "studio": "multiple", "fetchStrategy": "http"},
    "scrapy_static_sources": {"adapter": "scrapy_static", "studio": "multiple", "fetchStrategy": "http"},
    "social_reddit": {"adapter": "social", "studio": "reddit", "fetchStrategy": "http"},
    "social_x": {"adapter": "social", "studio": "x", "fetchStrategy": "http"},
    "social_mastodon": {"adapter": "social", "studio": "mastodon", "fetchStrategy": "http"},
    "static_studio_pages_a_i": {"adapter": "static", "studio": "multiple", "fetchStrategy": "auto"},
    "static_studio_pages_j_r": {"adapter": "static", "studio": "multiple", "fetchStrategy": "auto"},
    "static_studio_pages_s_z": {"adapter": "static", "studio": "multiple", "fetchStrategy": "auto"},
    "static_studio_pages": {"adapter": "static", "studio": "multiple", "fetchStrategy": "auto"},
    "wellfound": {"adapter": "html", "studio": "wellfound", "fetchStrategy": "browser"},
}

EXCLUDED_DEFAULT_SOURCES = {
    "wellfound": "disabled_by_default: blocked by anti-bot restrictions in non-browser fetch mode",
}
