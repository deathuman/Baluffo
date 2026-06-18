from __future__ import annotations

import pytest

from src import jobs_fetcher
from src.jobs.adapters import static_sources


def test_static_source_registry_falls_back_when_jobs_fetcher_facade_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fallback_rows = [
        {
            "name": "Fallback Static",
            "studio": "Fallback Static",
            "adapter": "static",
            "pages": [],
            "enabledByDefault": True,
        }
    ]
    fallback_calls: list[str] = []

    def fallback_registry_entries(adapter: str) -> list[dict]:
        fallback_calls.append(adapter)
        return fallback_rows

    monkeypatch.delattr(jobs_fetcher, "registry_entries")
    monkeypatch.setattr(static_sources, "registry_entries", fallback_registry_entries)

    rows = static_sources.run_static_studio_pages_source(
        fetch_text=lambda _url, _timeout_s: "",
        timeout_s=5,
        retries=0,
        backoff_s=0,
    )

    assert rows == []
    assert fallback_calls == ["static"]


def test_static_source_registry_does_not_swallow_unexpected_facade_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_registry_entries(_adapter: str, *, enabled_only: bool = True) -> list[dict]:
        raise RuntimeError("unexpected registry facade bug")

    monkeypatch.setattr(jobs_fetcher, "registry_entries", fail_registry_entries)

    with pytest.raises(RuntimeError, match="unexpected registry facade bug"):
        static_sources.run_static_studio_pages_source(
            fetch_text=lambda _url, _timeout_s: "",
            timeout_s=5,
            retries=0,
            backoff_s=0,
        )
