from __future__ import annotations

"""Stable import surface for Gamesmap discovery helpers."""

import sys

from src.source_registry import unique_sources as _unique_sources

from . import gamesmap_candidates as gamesmap_candidates_mod
from . import gamesmap_parsing as gamesmap_parsing_mod
from .directory_fetch import (
    fetch_directory_pages as _fetch_directory_pages,
)
from .directory_fetch import (
    resolve_directory_fetch_limits as _resolve_directory_fetch_limits,
)
from .page_analysis import analyze_fetched_page as _analyze_fetched_page
from .web_search import fetch_text as _fetch_text
from .web_search import infer_web_candidate as _infer_web_candidate

gamesmap_candidates_mod.root = sys.modules[__name__]

# Preserve the helper surface that `gamesmap_candidates` resolves through the root module.
unique_sources = _unique_sources
fetch_directory_pages = _fetch_directory_pages
resolve_directory_fetch_limits = _resolve_directory_fetch_limits
analyze_fetched_page = _analyze_fetched_page
fetch_text = _fetch_text
infer_web_candidate = _infer_web_candidate

discover_gamesmap_candidates = gamesmap_candidates_mod.discover_gamesmap_candidates
gamesmap_matches_category = gamesmap_candidates_mod.gamesmap_matches_category
normalize_gamesmap_category_token = gamesmap_candidates_mod.normalize_gamesmap_category_token

parse_gamesmap_detail_page = gamesmap_parsing_mod.parse_gamesmap_detail_page
parse_gamesmap_index_entries = gamesmap_parsing_mod.parse_gamesmap_index_entries
parse_gamesmap_index_links = gamesmap_parsing_mod.parse_gamesmap_index_links
_parse_gamesmap_index_entries_with_diagnostics = (
    gamesmap_parsing_mod._parse_gamesmap_index_entries_with_diagnostics
)
