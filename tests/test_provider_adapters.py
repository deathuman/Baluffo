from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import pytest

from src.jobs.adapters import provider_api
from src.jobs.adapters import provider_personio as provider_personio_runner
from src.jobs.adapters.plugins import default_registry
from src.jobs.adapters.plugins.provider_api import ensure_registered as ensure_provider_plugins
from src.jobs.adapters.plugins.provider_api import html_board as html_board_runner
from src.jobs.adapters.plugins.provider_api import json_feed as json_feed_runner
from src.jobs.adapters.plugins.types import AdapterPluginContext
from tests.helpers.job_fixtures import _fixture


class _FakeDeps:
    def __init__(self, registry_rows: dict[str, list[dict[str, object]]]) -> None:
        self._registry_rows = {
            key: [dict(row) for row in rows] for key, rows in registry_rows.items()
        }
        self.SOURCE_DIAGNOSTICS: dict[str, dict[str, object]] = {}

    def registry_entries(self, key: str) -> list[dict[str, object]]:
        return [dict(row) for row in self._registry_rows.get(key, [])]

    def set_registry_entries(self, key: str, rows: list[dict[str, object]]) -> None:
        self._registry_rows[key] = [dict(row) for row in rows]

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


@dataclass(frozen=True)
class _FixtureCase:
    name: str
    setup: Callable[[_FakeDeps], None]
    run: Callable[[], list[dict[str, object]]]
    expected_adapter: str
    expected_studio: str
    extra_check: Callable[[list[dict[str, object]]], None] = lambda rows: None


def _assert_basic_job_fields(rows: list[dict[str, object]]) -> None:
    assert rows[0]["title"]
    assert rows[0]["company"]
    assert rows[0]["jobLink"]


def _setup_workable(deps: _FakeDeps) -> None:
    deps.set_registry_entries(
        "workable",
        [
            {
                "name": "Hutch (Workable)",
                "studio": "Hutch",
                "adapter": "workable",
                "account": "hutch",
                "api_url": "https://apply.workable.com/api/v1/widget/accounts/hutch?details=true",
                "enabledByDefault": True,
            }
        ],
    )


def _setup_breezy(deps: _FakeDeps) -> None:
    deps.set_registry_entries(
        "breezy",
        [
            {
                "name": "YallaPlay (Breezy)",
                "studio": "YallaPlay",
                "adapter": "breezy",
                "board_url": "https://yallaplay.breezy.hr/",
                "enabledByDefault": True,
            }
        ],
    )


def _setup_jazzhr(deps: _FakeDeps) -> None:
    deps.set_registry_entries(
        "jazzhr",
        [
            {
                "name": "Lost Boys Interactive (JazzHR)",
                "studio": "Lost Boys Interactive",
                "adapter": "jazzhr",
                "board_url": "https://lostboysinteractive.applytojob.com/apply",
                "enabledByDefault": True,
            }
        ],
    )


FIXTURE_CASES = [
    pytest.param(
        _FixtureCase(
            name="workable",
            setup=lambda deps: (
                _setup_workable(deps),
                deps.set_source_diagnostics(
                    "workable_sources",
                    adapter="workable",
                    studio="Hutch",
                    details=[],
                    partial_errors=[],
                ),
            ),
            run=lambda: provider_api.run_workable_sources_source(
                fetch_text=lambda url, timeout: (
                    _fixture("workable_jobs.json") if url.endswith("/hutch?details=true") else ""
                ),
                timeout_s=5,
                retries=0,
                backoff_s=0,
            ),
            expected_adapter="workable",
            expected_studio="Hutch",
            extra_check=_assert_basic_job_fields,
        ),
        id="workable",
    ),
    pytest.param(
        _FixtureCase(
            name="breezy",
            setup=lambda deps: (
                _setup_breezy(deps),
                deps.set_source_diagnostics(
                    "breezy_sources",
                    adapter="breezy",
                    studio="YallaPlay",
                    details=[],
                    partial_errors=[],
                ),
            ),
            run=lambda: provider_api.run_breezy_sources_source(
                fetch_text=lambda url, timeout: (
                    _fixture("breezy_jobs.html") if url == "https://yallaplay.breezy.hr/" else ""
                ),
                timeout_s=5,
                retries=0,
                backoff_s=0,
            ),
            expected_adapter="breezy",
            expected_studio="YallaPlay",
            extra_check=_assert_basic_job_fields,
        ),
        id="breezy",
    ),
    pytest.param(
        _FixtureCase(
            name="jazzhr",
            setup=lambda deps: (
                _setup_jazzhr(deps),
                deps.set_source_diagnostics(
                    "jazzhr_sources",
                    adapter="jazzhr",
                    studio="Lost Boys Interactive",
                    details=[],
                    partial_errors=[],
                ),
            ),
            run=lambda: provider_api.run_jazzhr_sources_source(
                fetch_text=lambda url, timeout: (
                    _fixture("jazzhr_jobs.html")
                    if url == "https://lostboysinteractive.applytojob.com/apply"
                    else ""
                ),
                timeout_s=5,
                retries=0,
                backoff_s=0,
            ),
            expected_adapter="jazzhr",
            expected_studio="Lost Boys Interactive",
            extra_check=_assert_basic_job_fields,
        ),
        id="jazzhr",
    ),
]


def _bind_fake_deps(monkeypatch: pytest.MonkeyPatch, deps: _FakeDeps) -> None:
    for module in (html_board_runner, json_feed_runner):
        monkeypatch.setattr(module, "registry_entries", deps.registry_entries)
        monkeypatch.setattr(module, "fetch_with_retries", deps.fetch_with_retries)
        monkeypatch.setattr(module, "set_source_diagnostics", deps.set_source_diagnostics)
    monkeypatch.setattr(provider_personio_runner, "jobs_registry_entries", deps.registry_entries)
    monkeypatch.setattr(provider_personio_runner, "fetch_with_retries", deps.fetch_with_retries)
    monkeypatch.setattr(
        provider_personio_runner, "set_source_diagnostics", deps.set_source_diagnostics
    )


@pytest.mark.parametrize("case", FIXTURE_CASES, ids=lambda case: case.name)
def test_provider_fixture_parsers_extract_jobs(
    case: _FixtureCase, monkeypatch: pytest.MonkeyPatch
) -> None:
    deps = _FakeDeps({})
    case.setup(deps)
    _bind_fake_deps(monkeypatch, deps)
    rows = case.run()

    assert len(rows) >= 1
    assert all(row["adapter"] == case.expected_adapter for row in rows)
    assert all(row["studio"] == case.expected_studio for row in rows)
    case.extra_check(rows)


def test_personio_plugin_dispatch_uses_shared_helper_and_fixture(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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

    _bind_fake_deps(monkeypatch, deps)
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
