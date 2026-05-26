#!/usr/bin/env python3
"""Stable thin CLI facade for the refactored jobs pipeline package.

AI boundary: this file owns CLI compatibility exports and root patch seams only.
AI boundary implement in: `src.jobs.*` pipeline, adapter, dedup, report leaves.
AI boundary search before contracts: bridge task launch and frontend fetcher callers.
AI boundary verify: `npm run test:refactor:changed` plus focused fetcher tests.
"""

from __future__ import annotations

import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.request import urlopen


def _ensure_repo_on_path() -> None:
    repo_root = Path(__file__).resolve().parent.parent
    repo_root_str = str(repo_root)
    if repo_root_str not in sys.path:
        sys.path.insert(0, repo_root_str)


_ensure_repo_on_path()

from src.contracts import SCHEMA_VERSION
from src.jobs import fetcher_compat_exports as fetcher_compat_exports_mod
from src.jobs import fetcher_compat_runtime as fetcher_compat_runtime_mod
from src.jobs import pipeline as _pipeline
from src.jobs import registry as _registry
from src.jobs import transport as _transport
from src.jobs.common import diagnostics as _common_diagnostics
from src.jobs.text_utils import clean_text, norm_text, normalize_url
from src.jobs_fetcher_registry import SOURCE_REPORT_META
from src.shared.utils import env_flag, now_iso

SOURCE_DIAGNOSTICS = _common_diagnostics.SOURCE_DIAGNOSTICS
STUDIO_SOURCE_REGISTRY = _registry.STUDIO_SOURCE_REGISTRY
httpx = _transport.httpx

_COMPAT_MODULE_EXPORTS = fetcher_compat_exports_mod.COMPAT_MODULE_EXPORTS
_COMPAT_VALUES: dict[str, object] = {
    "SCHEMA_VERSION": SCHEMA_VERSION,
    "SOURCE_DIAGNOSTICS": SOURCE_DIAGNOSTICS,
    "SOURCE_REPORT_META": SOURCE_REPORT_META,
    "STUDIO_SOURCE_REGISTRY": STUDIO_SOURCE_REGISTRY,
    "clean_text": clean_text,
    "datetime": datetime,
    "env_flag": env_flag,
    "httpx": httpx,
    "norm_text": norm_text,
    "normalize_url": normalize_url,
    "now_iso": now_iso,
    "re": re,
    "timedelta": timedelta,
    "timezone": timezone,
    "urlopen": urlopen,
}

fetcher_compat_runtime_mod.root = sys.modules[__name__]

__all__ = [
    "SCHEMA_VERSION",
    "SOURCE_DIAGNOSTICS",
    "SOURCE_REPORT_META",
    "STUDIO_SOURCE_REGISTRY",
    "build_redirect_resolver",
    "default_source_loaders",
    "main",
    "maybe_fetch_kojima_job_listing_html",
    "parse_args",
    "registry_entries",
    "run_pipeline",
    "run_scrapy_static_source",
]


def parse_args(*args, **kwargs):
    return _pipeline.parse_args(*args, **kwargs)


def main(*args, **kwargs):
    return _pipeline.main(*args, **kwargs)


def run_pipeline(*args, **kwargs):
    return fetcher_compat_runtime_mod.run_pipeline(*args, **kwargs)


def run_scrapy_static_source(*args, **kwargs):
    return fetcher_compat_runtime_mod.run_scrapy_static_source(*args, **kwargs)


def registry_entries(adapter: str, *, enabled_only: bool = True, **kwargs):
    return fetcher_compat_runtime_mod.registry_entries(adapter, enabled_only=enabled_only, **kwargs)


def build_redirect_resolver(*args, **kwargs):
    return fetcher_compat_runtime_mod.build_redirect_resolver(*args, **kwargs)


def maybe_fetch_kojima_job_listing_html(*args, **kwargs):
    return fetcher_compat_runtime_mod.maybe_fetch_kojima_job_listing_html(*args, **kwargs)


def __getattr__(name: str) -> object:
    if name in _COMPAT_VALUES:
        return _COMPAT_VALUES[name]
    module_attr = _COMPAT_MODULE_EXPORTS.get(name)
    if module_attr is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module, attr_name = module_attr
    value = getattr(module, attr_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(_COMPAT_VALUES) | set(_COMPAT_MODULE_EXPORTS))


if __name__ == "__main__":
    raise SystemExit(main())
