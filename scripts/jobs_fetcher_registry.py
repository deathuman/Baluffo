#!/usr/bin/env python3
"""Registry/constants for jobs fetch source execution and reporting."""

from __future__ import annotations

from typing import Dict, List

DEFAULT_SOURCE_LOADER_NAMES: List[str] = [
    "google_sheets",
    "remote_ok",
    "gamesindustry",
    "greenhouse_boards",
    "teamtailor_sources",
    "lever_sources",
    "smartrecruiters_sources",
    "workable_sources",
    "ashby_sources",
    "personio_sources",
    "static_studio_pages",
]

SOURCE_REPORT_META: Dict[str, Dict[str, str]] = {
    "google_sheets": {"adapter": "csv", "studio": "community_sheet"},
    "remote_ok": {"adapter": "api", "studio": "remote_ok"},
    "gamesindustry": {"adapter": "html", "studio": "gamesindustry"},
    "greenhouse_boards": {"adapter": "greenhouse", "studio": "multiple"},
    "teamtailor_sources": {"adapter": "teamtailor", "studio": "multiple"},
    "lever_sources": {"adapter": "lever", "studio": "multiple"},
    "smartrecruiters_sources": {"adapter": "smartrecruiters", "studio": "multiple"},
    "workable_sources": {"adapter": "workable", "studio": "multiple"},
    "ashby_sources": {"adapter": "ashby", "studio": "multiple"},
    "personio_sources": {"adapter": "personio", "studio": "multiple"},
    "static_studio_pages": {"adapter": "static", "studio": "multiple"},
    "wellfound": {"adapter": "html", "studio": "wellfound"},
}

EXCLUDED_DEFAULT_SOURCES = {
    "wellfound": "disabled_by_default: blocked by anti-bot restrictions in non-browser fetch mode",
}
