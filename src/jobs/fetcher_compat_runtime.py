"""Root-backed runtime wrappers for the stable ``src.jobs_fetcher`` facade."""

from __future__ import annotations

from typing import Any

from src.jobs import pipeline as pipeline_mod
from src.jobs import registry as registry_mod
from src.jobs import transport as transport_mod
from src.jobs.adapters import static as static_mod
from src.jobs.adapters import static_scrapy as static_scrapy_mod

root: Any | None = None


def _root_mod() -> Any:
    if root is None:
        raise RuntimeError("src.jobs_fetcher root module was not bound to fetcher_compat_runtime")
    return root


def run_pipeline(*args, **kwargs):
    root_mod = _root_mod()
    previous = getattr(pipeline_mod, "build_redirect_resolver", None)
    try:
        pipeline_mod.build_redirect_resolver = root_mod.build_redirect_resolver  # type: ignore[assignment]
        return pipeline_mod.run_pipeline(*args, **kwargs)
    finally:
        if previous is not None:
            pipeline_mod.build_redirect_resolver = previous  # type: ignore[assignment]


def run_scrapy_static_source(*args, **kwargs):
    root_mod = _root_mod()
    previous = getattr(static_scrapy_mod, "registry_entries", None)
    try:
        static_scrapy_mod.registry_entries = root_mod.registry_entries  # type: ignore[assignment]
        return static_mod.run_scrapy_static_source(*args, **kwargs)
    finally:
        if previous is not None:
            static_scrapy_mod.registry_entries = previous  # type: ignore[assignment]


def registry_entries(adapter: str, *, enabled_only: bool = True):
    root_mod = _root_mod()
    return registry_mod.registry_entries(
        adapter,
        enabled_only=enabled_only,
        registry_rows=root_mod.STUDIO_SOURCE_REGISTRY,
    )


def build_redirect_resolver(*args, **kwargs):
    root_mod = _root_mod()
    previous_httpx = transport_mod.httpx
    transport_mod.httpx = root_mod.httpx
    try:
        return transport_mod.build_redirect_resolver(*args, **kwargs)
    finally:
        transport_mod.httpx = previous_httpx


def maybe_fetch_kojima_job_listing_html(*args, **kwargs):
    root_mod = _root_mod()
    import src.jobs.adapters.html_parsers as html_parsers_mod

    html_parsers_mod.urlopen = root_mod.urlopen
    return html_parsers_mod.maybe_fetch_kojima_job_listing_html(*args, **kwargs)
