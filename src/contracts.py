#!/usr/bin/env python3
"""Shared contracts/constants for fetch and discovery reports."""

from __future__ import annotations

from typing import Any, Dict, List, TypedDict

SCHEMA_VERSION = "1.0"


class LogEvent(TypedDict, total=False):
    timestamp: str
    level: str
    scope: str
    sourceId: str
    message: str


class SourceSummary(TypedDict, total=False):
    activeCount: int
    pendingCount: int
    rejectedCount: int


JsonObject = Dict[str, Any]
JsonArray = List[JsonObject]
