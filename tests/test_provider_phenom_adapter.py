"""Phenom (Jobs2Web) provider adapter tests: parser + runner + registration."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, ".")

from src.jobs.adapters.parsers.phenom import (  # noqa: E402
    is_phenom_job_href,
    parse_phenom_jobs_html,
    phenom_detail_fields,
)
from src.jobs.adapters.plugins.provider_api import phenom as phenom_runner  # noqa: E402

FIXTURES = Path("tests/fixtures")
SEARCH_URL = "https://jobsearch.createyourowncareer.com/RTL/search/?searchby=location"


def _fixture(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


class _FakeDeps:
    def __init__(self, registry_rows: dict[str, list[dict]]) -> None:
        self._rows = registry_rows
        self.SOURCE_DIAGNOSTICS: dict[str, dict] = {}

    def registry_entries(self, key: str) -> list[dict]:
        return [dict(row) for row in self._rows.get(key, [])]

    def fetch_with_retries(self, url, fetch_text, timeout_s, retries, backoff_s):
        return fetch_text(url, timeout_s)

    def set_source_diagnostics(
        self, name, *, adapter, studio, provider_url="", details=None, partial_errors=None
    ):
        self.SOURCE_DIAGNOSTICS[name] = {
            "adapter": adapter,
            "studio": studio,
            "providerUrl": provider_url,
            "details": details or [],
            "partialErrors": partial_errors or [],
        }


@pytest.fixture
def _bind(monkeypatch):
    def _bind_deps(deps: _FakeDeps) -> None:
        monkeypatch.setattr(phenom_runner, "registry_entries", deps.registry_entries)
        monkeypatch.setattr(phenom_runner, "fetch_with_retries", deps.fetch_with_retries)
        monkeypatch.setattr(phenom_runner, "set_source_diagnostics", deps.set_source_diagnostics)

    return _bind_deps


def test_is_phenom_job_href_shapes() -> None:
    assert is_phenom_job_href("/RTL/job/Hamburg-Role-20459/1434341833/")
    assert is_phenom_job_href("/OtherTenant/job/some-slug/123")
    assert not is_phenom_job_href("/RTL/search/?startrow=25")
    assert not is_phenom_job_href("/RTL/job/not-a-job/")
    assert not is_phenom_job_href("/careers/")


def test_parse_phenom_search_page_extracts_jobs_and_pagination() -> None:
    html = _fixture("phenom_rtl_search.html")
    jobs, next_pages = parse_phenom_jobs_html(html, SEARCH_URL, fallback_company="RTL")
    assert len(jobs) == 25
    assert len(next_pages) == 2
    assert all("startrow=" in page for page in next_pages)
    first = jobs[0]
    assert first["sourceJobId"] == "phenom:1434341833"
    assert "Working Student" in first["title"]
    assert first["company"] == "RTL"
    assert first["city"] == "Hamburg"
    assert first["jobLink"].startswith("https://jobsearch.createyourowncareer.com/RTL/job/")
    assert first["sector"] == "Game"
    # all rows carry unique stable ids
    ids = {row["sourceJobId"] for row in jobs}
    assert len(ids) == len(jobs)


def test_parse_phenom_ignores_non_job_and_sort_anchors() -> None:
    html = _fixture("phenom_rtl_search.html")
    jobs, _ = parse_phenom_jobs_html(html, SEARCH_URL, fallback_company="RTL")
    # sort-column header anchors must not become job rows
    assert all("/search/" not in row["jobLink"] for row in jobs)


def test_phenom_detail_fields_extract_schema_org_meta() -> None:
    html = _fixture("phenom_rtl_detail.html")
    fields = phenom_detail_fields(html)
    assert fields.get("streetAddress") == "Kiel, Germany, 24103"
    assert "2026" in fields.get("datePosted", "")


def test_phenom_runner_pages_and_emits_rows(_bind, monkeypatch) -> None:
    search_html = _fixture("phenom_rtl_search.html")
    page2_html = search_html.replace("startrow=25", "startrow=50").replace(
        "startrow=50", "startrow=75", 1
    )
    fetched_urls: list[str] = []

    def fetch_text(url: str, _timeout: int) -> str:
        fetched_urls.append(url)
        if "startrow=25" in url:
            return page2_html
        return search_html

    deps = _FakeDeps(
        {
            "phenom": [
                {
                    "name": "RTL (Phenom)",
                    "studio": "RTL",
                    "adapter": "phenom",
                    "listing_url": "https://jobsearch.createyourowncareer.com/RTL/",
                    "enabledByDefault": True,
                }
            ]
        }
    )
    _bind(deps)
    rows = phenom_runner.run_phenom_sources_source(
        fetch_text=fetch_text, timeout_s=5, retries=0, backoff_s=0
    )
    assert rows, "expected job rows from phenom runner"
    assert all(row["adapter"] == "phenom" for row in rows)
    assert all(row["studio"] == "RTL" for row in rows)
    assert all(row["company"] == "RTL" for row in rows)
    # first fetch normalizes to the tenant search root
    assert fetched_urls[0] == "https://jobsearch.createyourowncareer.com/RTL/search/"
    # pagination followed at least once
    assert any("startrow=25" in url for url in fetched_urls[1:])


def test_phenom_runner_empty_board_is_legit_empty(_bind) -> None:
    empty_html = (
        "<html><body><table id='searchresults'></table>"
        "<a href='/RTL/search/?q=&startrow=25'>Next</a></body></html>"
    )

    def fetch_text(url: str, _timeout: int) -> str:
        return empty_html

    deps = _FakeDeps(
        {
            "phenom": [
                {
                    "name": "Empty Tenant (Phenom)",
                    "studio": "Empty Tenant",
                    "adapter": "phenom",
                    "listing_url": "https://jobsearch.example.com/EmptyTenant/",
                    "enabledByDefault": True,
                }
            ]
        }
    )
    _bind(deps)
    rows = phenom_runner.run_phenom_sources_source(
        fetch_text=fetch_text, timeout_s=5, retries=0, backoff_s=0
    )
    assert rows == []
    diag = deps.SOURCE_DIAGNOSTICS["phenom_sources"]
    assert diag["adapter"] == "phenom"
    details = diag["details"]
    assert details and details[0].get("zeroKeptClassification") == "legit_empty"


def test_phenom_runner_missing_listing_url_reports_error(_bind) -> None:
    from src.exceptions import AdapterValidationError

    deps = _FakeDeps(
        {
            "phenom": [
                {
                    "name": "Broken (Phenom)",
                    "studio": "Broken",
                    "adapter": "phenom",
                    "listing_url": "",
                    "enabledByDefault": True,
                }
            ]
        }
    )
    _bind(deps)
    with pytest.raises(AdapterValidationError) as excinfo:
        phenom_runner.run_phenom_sources_source(
            fetch_text=lambda url, t: "", timeout_s=5, retries=0, backoff_s=0
        )
    assert "missing listing_url" in str(excinfo.value)


def test_phenom_plugin_registered_and_dispatchable() -> None:
    from src.jobs.adapters.plugins import default_registry
    from src.jobs.adapters.plugins.provider_api import ensure_registered
    from src.jobs.adapters.plugins.types import AdapterPluginContext

    ensure_registered()
    plugin, _selection = default_registry.select(
        AdapterPluginContext(family="provider_api", adapter_key="phenom_sources")
    )
    assert plugin is not None
    assert plugin.name == "phenom_sources"
