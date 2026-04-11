"""Canonical provider parser package.

The individual parser modules are the canonical source of truth. This package
keeps only the small location helper surface that is safe to import during
module bootstrap.
"""

from __future__ import annotations

from .location import (
    _looks_like_country_token,
    normalize_location_details,
    parse_generic_location_fields,
    parse_greenhouse_location,
)

__all__ = [
    "_looks_like_country_token",
    "normalize_location_details",
    "parse_greenhouse_location",
    "parse_generic_location_fields",
]
