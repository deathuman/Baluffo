"""Plugin framework for jobs adapter implementations.

This module provides a small, deterministic plugin registry that can be used to
incrementally migrate existing adapter entrypoints (compatibility layer) into
plugin-backed dispatchers.
"""

from __future__ import annotations

from .errors import NoPluginFoundError, PluginError
from .registry import PluginRegistry, default_registry
from .types import AdapterPlugin, AdapterPluginContext
from .versioning import normalize_schema_version

__all__ = [
    "AdapterPlugin",
    "AdapterPluginContext",
    "NoPluginFoundError",
    "PluginError",
    "PluginRegistry",
    "default_registry",
    "normalize_schema_version",
]

