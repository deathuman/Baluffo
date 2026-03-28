from __future__ import annotations

from pathlib import Path
from unittest import mock

from src.jobs import common as jobs_common
from src.jobs.adapters import _runtime as runtime_resolver
from src.jobs.adapters import provider_api
from src.jobs.adapters.plugins import default_registry
from src.jobs.adapters.plugins.provider_api import ensure_registered as ensure_provider_plugins
from src.jobs.adapters.plugins.types import AdapterPluginContext

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


def _fixture(name: str) -> str:
    return (FIXTURES_DIR / name).read_text(encoding="utf-8")


class _FakeDeps:
    def __init__(self, registry_rows: dict[str, list[dict[str, object]]]) -> None:
        self._registry_rows = {
            key: [dict(row) for row in rows] for key, rows in registry_rows.items()
        }
        self.SOURCE_DIAGNOSTICS: dict[str, dict[str, object]] = {}

    def registry_entries(self, key: str) -> list[dict[str, object]]:
        return [dict(row) for row in self._registry_rows.get(key, [])]

    def fetch_with_retries(
        self,
        url: str,
        fetch_text,
        timeout_s: int,
        retries: int,
        backoff_s: float,
    ) -> str:
        _ = retries, backoff_s
        return fetch_text(url, timeout_s)

    def set_source_diagnostics(
        self,
        name: str,
        *,
        adapter: str,
        studio: str,
        provider_url: str = "",
        details: list[dict[str, object]],
        partial_errors: list[str],
    ) -> None:
        self.SOURCE_DIAGNOSTICS[name] = {
            "adapter": adapter,
            "studio": studio,
            "providerUrl": provider_url,
            "details": details,
            "partialErrors": partial_errors,
        }


def test_provider_plugin_registry_selects_bamboohr_and_workday_plugins() -> None:
    ensure_provider_plugins()

    bamboohr_plugin, bamboohr_selection = default_registry.select(
        AdapterPluginContext(family="provider_api", adapter_key="bamboohr_sources")
    )
    workday_plugin, workday_selection = default_registry.select(
        AdapterPluginContext(family="provider_api", adapter_key="workday_sources")
    )

    assert bamboohr_plugin.name == "bamboohr_sources"
    assert bamboohr_selection.plugin_name == "bamboohr_sources"
    assert workday_plugin.name == "workday_sources"
    assert workday_selection.plugin_name == "workday_sources"


def test_provider_api_bamboohr_dispatch_extracts_registry_backed_jobs() -> None:
    deps = _FakeDeps(
        {
            "bamboohr": [
                {
                    "name": "Wolcen Studios (BambooHR)",
                    "studio": "Wolcen Studios",
                    "adapter": "bamboohr",
                    "pages": ["https://wolcenstudios.bamboohr.com/jobs/"],
                    "enabledByDefault": True,
                }
            ]
        }
    )
    listing_url = "https://wolcenstudios.bamboohr.com/jobs/"

    def fetch_text(url: str, _timeout: int) -> str:
        if url == listing_url:
            return _fixture("bamboohr_jobs.html")
        if url.endswith("/jobs/view/lead-character-artist"):
            return _fixture("bamboohr_job_detail_lead_character_artist.html")
        if url.endswith("/jobs/view/technical-animator"):
            return _fixture("bamboohr_job_detail_technical_animator.html")
        raise AssertionError(f"unexpected fetch for {url}")

    with mock.patch.object(runtime_resolver, "facade", lambda: deps):
        rows = provider_api.run_bamboohr_sources_source(
            fetch_text=fetch_text,
            timeout_s=5,
            retries=0,
            backoff_s=0.0,
        )

    assert len(rows) == 2
    assert {row["title"] for row in rows} == {"Lead Character Artist", "Technical Animator"}
    assert all(row["adapter"] == "bamboohr" for row in rows)
    assert all(row["studio"] == "Wolcen Studios" for row in rows)
    assert deps.SOURCE_DIAGNOSTICS["bamboohr_sources"]["details"][0]["keptCount"] == 2


def test_provider_api_workday_dispatch_extracts_registry_backed_jobs_and_pagination() -> None:
    deps = _FakeDeps(
        {
            "workday": [
                {
                    "name": "TiMi Studio Group (Workday)",
                    "studio": "TiMi Studio Group",
                    "adapter": "workday",
                    "pages": ["https://example.wd5.myworkdayjobs.com/en-US/Company_Careers"],
                    "enabledByDefault": True,
                }
            ]
        }
    )
    listing_url = "https://example.wd5.myworkdayjobs.com/en-US/Company_Careers"
    page2_url = "https://example.wd5.myworkdayjobs.com/en-US/Company_Careers?page=2"

    def fetch_text(url: str, _timeout: int) -> str:
        if url == listing_url:
            return _fixture("workday_jobs.html")
        if url == page2_url:
            return _fixture("workday_jobs_page2.html")
        if url.endswith("/job/Gameplay-Programmer_JR100"):
            return _fixture("workday_job_detail_gameplay_programmer.html")
        if url.endswith("/job/Lead-Animator_JR200"):
            return _fixture("workday_job_detail_lead_animator.html")
        raise AssertionError(f"unexpected fetch for {url}")

    with mock.patch.object(runtime_resolver, "facade", lambda: deps):
        rows = provider_api.run_workday_sources_source(
            fetch_text=fetch_text,
            timeout_s=5,
            retries=0,
            backoff_s=0.0,
        )

    assert len(rows) == 2
    assert {row["title"] for row in rows} == {"Gameplay Programmer", "Lead Animator"}
    assert all(row["adapter"] == "workday" for row in rows)
    assert all(row["studio"] == "TiMi Studio Group" for row in rows)
    assert deps.SOURCE_DIAGNOSTICS["workday_sources"]["details"][0]["fetchedCount"] == 2


def test_registry_entries_bamboohr_derives_from_static_and_suppresses_redundant_static() -> None:
    static_row = {
        "name": "Wolcen Studios (Manual Website)",
        "studio": "Wolcen Studios",
        "adapter": "static",
        "pages": ["https://wolcenstudios.bamboohr.com/jobs/"],
        "enabledByDefault": True,
    }
    provider_row = {
        "name": "Wolcen Studios BambooHR",
        "studio": "Wolcen Studios",
        "adapter": "bamboohr",
        "listing_url": "https://wolcenstudios.bamboohr.com/careers",
        "enabledByDefault": True,
    }

    with mock.patch.object(jobs_common, "STUDIO_SOURCE_REGISTRY", [static_row, provider_row]):
        bamboohr_entries = jobs_common.registry_entries("bamboohr")
        static_entries = jobs_common.registry_entries("static")

    assert any(
        row.get("name") == "Wolcen Studios (Manual Website)"
        and row.get("adapter") == "bamboohr"
        and row.get("migrationSourceAdapter") == "static"
        for row in bamboohr_entries
    )
    assert any(row.get("name") == "Wolcen Studios BambooHR" for row in bamboohr_entries)
    assert all(row.get("name") != "Wolcen Studios (Manual Website)" for row in static_entries)
