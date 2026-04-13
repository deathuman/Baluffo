from __future__ import annotations

import pytest

from src import jobs_fetcher as jf


@pytest.fixture(autouse=True)
def reset_jobs_static_runtime_state():
    original_registry = list(jf.STUDIO_SOURCE_REGISTRY)
    jf.SOURCE_DIAGNOSTICS.clear()
    try:
        yield
    finally:
        jf.STUDIO_SOURCE_REGISTRY = original_registry
        jf.SOURCE_DIAGNOSTICS.clear()
