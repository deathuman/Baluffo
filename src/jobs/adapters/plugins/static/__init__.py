"""Static adapter plugins: site-specific parsers registered by source_identity (e.g. host)."""

from __future__ import annotations

from .register import register_static_plugins

__all__ = ["register_static_plugins"]
