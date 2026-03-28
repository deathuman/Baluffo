from __future__ import annotations

from pathlib import Path
from unittest import mock

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


def test_workable_provider_adapter_fixture_extracts_jobs() -> None:
    deps = _FakeDeps(
        {
            "workable": [
                {
                    "name": "Hutch (Workable)",
                    "studio": "Hutch",
                    "adapter": "workable",
                    "account": "hutch",
                    "api_url": "https://apply.workable.com/api/v1/widget/accounts/hutch?details=true",
                    "enabledByDefault": True,
                }
            ]
        }
    )

    def fetch_text(url: str, _timeout: int) -> str:
        assert url.endswith("/hutch?details=true")
        return _fixture("workable_jobs.json")

    with mock.patch.object(runtime_resolver, "facade", lambda: deps):
        rows = provider_api.run_workable_sources_source(
            fetch_text=fetch_text,
            timeout_s=5,
            retries=0,
            backoff_s=0,
        )

    assert len(rows) >= 1
    assert rows[0]["adapter"] == "workable"
    assert rows[0]["title"]
    assert rows[0]["company"]
    assert rows[0]["jobLink"]


def test_breezy_provider_adapter_fixture_extracts_jobs() -> None:
    deps = _FakeDeps(
        {
            "breezy": [
                {
                    "name": "YallaPlay (Breezy)",
                    "studio": "YallaPlay",
                    "adapter": "breezy",
                    "board_url": "https://yallaplay.breezy.hr/",
                    "enabledByDefault": True,
                }
            ]
        }
    )

    def fetch_text(url: str, _timeout: int) -> str:
        assert url == "https://yallaplay.breezy.hr/"
        return _fixture("breezy_jobs.html")

    with mock.patch.object(runtime_resolver, "facade", lambda: deps):
        rows = provider_api.run_breezy_sources_source(
            fetch_text=fetch_text,
            timeout_s=5,
            retries=0,
            backoff_s=0,
        )

    assert len(rows) >= 1
    assert rows[0]["adapter"] == "breezy"
    assert rows[0]["title"]
    assert rows[0]["company"]
    assert rows[0]["jobLink"]


def test_jazzhr_provider_adapter_fixture_extracts_jobs() -> None:
    deps = _FakeDeps(
        {
            "jazzhr": [
                {
                    "name": "Lost Boys Interactive (JazzHR)",
                    "studio": "Lost Boys Interactive",
                    "adapter": "jazzhr",
                    "board_url": "https://lostboysinteractive.applytojob.com/apply",
                    "enabledByDefault": True,
                }
            ]
        }
    )

    def fetch_text(url: str, _timeout: int) -> str:
        assert url == "https://lostboysinteractive.applytojob.com/apply"
        return _fixture("jazzhr_jobs.html")

    with mock.patch.object(runtime_resolver, "facade", lambda: deps):
        rows = provider_api.run_jazzhr_sources_source(
            fetch_text=fetch_text,
            timeout_s=5,
            retries=0,
            backoff_s=0,
        )

    assert len(rows) >= 1
    assert rows[0]["adapter"] == "jazzhr"
    assert rows[0]["title"]
    assert rows[0]["company"]
    assert rows[0]["jobLink"]


def test_personio_plugin_dispatch_uses_shared_helper_and_fixture() -> None:
    ensure_provider_plugins()
    deps = _FakeDeps(
        {
            "personio": [
                {
                    "name": "InnoGames (Personio)",
                    "studio": "InnoGames",
                    "adapter": "personio",
                    "feed_url": "https://innogames.jobs.personio.de/xml",
                    "enabledByDefault": True,
                }
            ]
        }
    )

    def fetch_text(url: str, _timeout: int) -> str:
        assert url == "https://innogames.jobs.personio.de/xml"
        return _fixture("personio_feed.xml")

    with mock.patch.object(runtime_resolver, "facade", lambda: deps):
        plugin, selection = default_registry.select(
            AdapterPluginContext(family="provider_api", adapter_key="personio_sources")
        )
        rows = plugin.run(
            fetch_text=fetch_text,
            timeout_s=5,
            retries=0,
            backoff_s=0,
        )

    assert selection.plugin_name == "personio_sources"
    assert len(rows) >= 1
    assert rows[0]["adapter"] == "personio"
    assert rows[0]["title"]
    assert rows[0]["company"]
    assert rows[0]["jobLink"]
