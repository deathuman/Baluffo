"""Jobs pipeline root-injection binding (compatibility-only seam).

Two entry points bind distinct roots and both are live in the same process during a
fetch: ``src.jobs_fetcher`` (thin CLI facade) for the compat runtime wrappers, and
``pipeline_stage_source_execution`` for the pipeline source leaves. Leaf modules read
their root through this module instead of owning their own slot.

Compatibility-only: do not expand this seam. ``src.jobs_fetcher`` and
``pipeline_stage_source_execution`` are the only bind sites; per-module accessors keep
their exact fallback and error behavior.
"""

from __future__ import annotations

from typing import Any

# Root bound by src.jobs_fetcher for the compat runtime wrappers.
_JOBS_FETCHER_ROOT: Any | None = None
# Root bound by pipeline_stage_source_execution for the pipeline source leaves.
_PIPELINE_ROOT: Any | None = None


def bind_jobs_fetcher(root: Any) -> None:
    """Bind the stable ``src.jobs_fetcher`` module as the compat runtime root."""
    global _JOBS_FETCHER_ROOT
    _JOBS_FETCHER_ROOT = root


def bind_pipeline(root: Any) -> None:
    """Bind the pipeline source-execution stage module as the pipeline root."""
    global _PIPELINE_ROOT
    _PIPELINE_ROOT = root


def require_jobs_fetcher_root() -> Any:
    """Return the jobs_fetcher root, raising when the compat runtime is unbound."""
    if _JOBS_FETCHER_ROOT is None:
        raise RuntimeError("src.jobs_fetcher root module was not bound to fetcher_compat_runtime")
    return _JOBS_FETCHER_ROOT


def pipeline_root_or_none() -> Any:
    """Return the bound pipeline root, or None when unbound."""
    return _PIPELINE_ROOT


def require_pipeline_root(owner: str) -> Any:
    """Return the pipeline root, raising the owner-specific unbound error."""
    if _PIPELINE_ROOT is None:
        raise RuntimeError(f"jobs.{owner} root is not bound")
    return _PIPELINE_ROOT
