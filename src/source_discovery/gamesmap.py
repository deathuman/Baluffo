from __future__ import annotations

"""Stable import surface for Gamesmap discovery helpers."""

import sys

from src.source_registry import unique_sources

from . import gamesmap_candidates as gamesmap_candidates_mod
from . import gamesmap_parsing as gamesmap_parsing_mod
from .directory_fetch import fetch_directory_pages, resolve_directory_fetch_limits
from .page_analysis import analyze_fetched_page
from .web_search import fetch_text, infer_web_candidate

gamesmap_candidates_mod.root = sys.modules[__name__]

discover_gamesmap_candidates = gamesmap_candidates_mod.discover_gamesmap_candidates
gamesmap_matches_category = gamesmap_candidates_mod.gamesmap_matches_category
normalize_gamesmap_category_token = gamesmap_candidates_mod.normalize_gamesmap_category_token

parse_gamesmap_detail_page = gamesmap_parsing_mod.parse_gamesmap_detail_page
parse_gamesmap_index_entries = gamesmap_parsing_mod.parse_gamesmap_index_entries
parse_gamesmap_index_links = gamesmap_parsing_mod.parse_gamesmap_index_links
_parse_gamesmap_index_entries_with_diagnostics = (
    gamesmap_parsing_mod._parse_gamesmap_index_entries_with_diagnostics
)
