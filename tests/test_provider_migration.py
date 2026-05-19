from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from unittest import mock

import pytest

from src.jobs import registry as jobs_registry
from src.jobs.adapters import provider_api
from src.jobs.adapters import provider_structured_listing as provider_structured_listing_runner
from src.jobs.adapters.plugins import default_registry
from src.jobs.adapters.plugins.provider_api import ensure_registered as ensure_provider_plugins
from src.jobs.adapters.plugins.types import AdapterPluginContext
from tests.helpers.job_fixtures import _fixture


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


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
class _DispatchCase:
    name: str
    setup: Callable[[_FakeDeps], None]
    run: Callable[[], list[dict[str, object]]]
    expected_len: int
    expected_adapter: str
    expected_studio: str
    extra_check: Callable[[list[dict[str, object]], _FakeDeps], None] = lambda rows, deps: None


def _setup_bamboohr(deps: _FakeDeps) -> None:
    deps.set_registry_entries(
        "bamboohr",
        [
            {
                "name": "Wolcen Studios (BambooHR)",
                "studio": "Wolcen Studios",
                "adapter": "bamboohr",
                "pages": ["https://wolcenstudios.bamboohr.com/jobs/"],
                "enabledByDefault": True,
            }
        ],
    )


def _setup_workday(deps: _FakeDeps) -> None:
    deps.set_registry_entries(
        "workday",
        [
            {
                "name": "TiMi Studio Group (Workday)",
                "studio": "TiMi Studio Group",
                "adapter": "workday",
                "pages": ["https://example.wd5.myworkdayjobs.com/en-US/Company_Careers"],
                "enabledByDefault": True,
            }
        ],
    )


DISPATCH_CASES = [
    pytest.param(
        _DispatchCase(
            name="bamboohr",
            setup=_setup_bamboohr,
            run=lambda: provider_api.run_bamboohr_sources_source(
                fetch_text=lambda url, timeout: _fixture_for_bamboohr(url, timeout),
                timeout_s=5,
                retries=0,
                backoff_s=0.0,
            ),
            expected_len=2,
            expected_adapter="bamboohr",
            expected_studio="Wolcen Studios",
            extra_check=lambda rows, deps: assert_bamboohr_details(deps),
        ),
        id="bamboohr",
    ),
    pytest.param(
        _DispatchCase(
            name="workday",
            setup=_setup_workday,
            run=lambda: provider_api.run_workday_sources_source(
                fetch_text=lambda url, timeout: _fixture_for_workday(url, timeout),
                timeout_s=5,
                retries=0,
                backoff_s=0.0,
            ),
            expected_len=2,
            expected_adapter="workday",
            expected_studio="TiMi Studio Group",
            extra_check=lambda rows, deps: assert_workday_details(deps),
        ),
        id="workday",
    ),
]


def _bind_fake_deps(monkeypatch: pytest.MonkeyPatch, deps: _FakeDeps) -> None:
    monkeypatch.setattr(
        provider_structured_listing_runner, "registry_entries", deps.registry_entries
    )
    monkeypatch.setattr(
        provider_structured_listing_runner,
        "set_source_diagnostics",
        deps.set_source_diagnostics,
    )


def _fixture_for_bamboohr(url: str, _timeout: int) -> str:
    if url.endswith("/jobs/"):
        return _fixture("bamboohr_jobs.html")
    if url.endswith("/jobs/view/lead-character-artist"):
        return _fixture("bamboohr_job_detail_lead_character_artist.html")
    if url.endswith("/jobs/view/technical-animator"):
        return _fixture("bamboohr_job_detail_technical_animator.html")
    raise AssertionError(f"unexpected fetch for {url}")


def _fixture_for_workday(url: str, _timeout: int) -> str:
    if url.endswith("/Company_Careers"):
        return _fixture("workday_jobs.html")
    if url.endswith("/Company_Careers?page=2"):
        return _fixture("workday_jobs_page2.html")
    if url.endswith("/job/Gameplay-Programmer_JR100"):
        return _fixture("workday_job_detail_gameplay_programmer.html")
    if url.endswith("/job/Lead-Animator_JR200"):
        return _fixture("workday_job_detail_lead_animator.html")
    raise AssertionError(f"unexpected fetch for {url}")


def assert_bamboohr_details(deps: _FakeDeps) -> None:
    assert deps.SOURCE_DIAGNOSTICS["bamboohr_sources"]["details"][0]["keptCount"] == 2


def assert_workday_details(deps: _FakeDeps) -> None:
    assert deps.SOURCE_DIAGNOSTICS["workday_sources"]["details"][0]["fetchedCount"] == 2


@pytest.mark.parametrize("case", DISPATCH_CASES, ids=lambda case: case.name)
def test_provider_api_dispatch_extracts_registry_backed_jobs(
    case: _DispatchCase, monkeypatch: pytest.MonkeyPatch
) -> None:
    deps = _FakeDeps({})
    case.setup(deps)
    _bind_fake_deps(monkeypatch, deps)
    rows = case.run()

    assert len(rows) == case.expected_len
    assert all(row["adapter"] == case.expected_adapter for row in rows)
    assert all(row["studio"] == case.expected_studio for row in rows)
    case.extra_check(rows, deps)


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

    with mock.patch.object(jobs_registry, "STUDIO_SOURCE_REGISTRY", [static_row, provider_row]):
        bamboohr_entries = jobs_registry.registry_entries("bamboohr")
        static_entries = jobs_registry.registry_entries("static")

    assert any(
        row.get("name") == "Wolcen Studios (Manual Website)"
        and row.get("adapter") == "bamboohr"
        and row.get("migrationSourceAdapter") == "static"
        for row in bamboohr_entries
    )
    assert any(row.get("name") == "Wolcen Studios BambooHR" for row in bamboohr_entries)
    assert all(row.get("name") != "Wolcen Studios (Manual Website)" for row in static_entries)


def test_registry_entries_excludes_pending_provider_migration_by_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pending_path = tmp_path / "source-registry-pending.json"
    _write_json(
        pending_path,
        [
            {
                "id": "bamboohr:listing_url:https://pending.bamboohr.com/careers",
                "name": "Pending Studio (BambooHR)",
                "studio": "Pending Studio",
                "adapter": "bamboohr",
                "listing_url": "https://pending.bamboohr.com/careers",
                "registryState": "pending",
                "pendingReason": "provider_migration_candidate",
                "enabledByDefault": False,
                "migrationSourceIdentity": "static:pending",
            }
        ],
    )
    monkeypatch.setattr(jobs_registry, "SOURCE_REGISTRY_PENDING_PATH", pending_path)
    monkeypatch.setattr(jobs_registry, "STUDIO_SOURCE_REGISTRY", [])

    assert jobs_registry.registry_entries("bamboohr") == []

    rows = jobs_registry.registry_entries("bamboohr", include_pending_provider_migration=True)

    assert len(rows) == 1
    assert rows[0]["name"] == "Pending Studio (BambooHR)"
    assert rows[0]["pendingReason"] == "provider_migration_candidate"
    assert rows[0]["migrationSourceIdentity"] == "static:pending"
    assert rows[0]["enabledByDefault"] is True
    assert rows[0]["fetchOnlyPendingProviderMigration"] is True


def test_registry_entries_pending_provider_migration_filters_unsafe_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pending_path = tmp_path / "source-registry-pending.json"
    _write_json(
        pending_path,
        [
            {
                "id": "bamboohr:listing_url:https://valid.bamboohr.com/careers",
                "name": "Valid Pending (BambooHR)",
                "studio": "Valid Pending",
                "adapter": "bamboohr",
                "listing_url": "https://valid.bamboohr.com/careers",
                "registryState": "pending",
                "pendingReason": "provider_migration_candidate",
                "migrationSourceIdentity": "static:valid",
                "enabledByDefault": False,
            },
            {
                "id": "bamboohr:listing_url:https://dupe.bamboohr.com/careers",
                "name": "Duplicate Pending (BambooHR)",
                "adapter": "bamboohr",
                "listing_url": "https://dupe.bamboohr.com/careers",
                "registryState": "pending",
                "pendingReason": "provider_migration_candidate",
                "migrationSourceIdentity": "static:dupe",
            },
            {
                "id": "bamboohr:listing_url:https://manual.bamboohr.com/careers",
                "name": "Manual Pending (BambooHR)",
                "adapter": "bamboohr",
                "listing_url": "https://manual.bamboohr.com/careers",
                "registryState": "pending",
                "pendingReason": "manual_source",
                "migrationSourceIdentity": "static:manual",
            },
            {
                "id": "static:pending",
                "name": "Static Pending",
                "adapter": "static",
                "listing_url": "https://static.example/jobs",
                "registryState": "pending",
                "pendingReason": "provider_migration_candidate",
                "migrationSourceIdentity": "static:source",
            },
            {
                "id": "oracle_hcm:pending",
                "name": "Unsupported Pending",
                "adapter": "oracle_hcm",
                "registryState": "pending",
                "pendingReason": "provider_migration_candidate",
                "migrationSourceIdentity": "static:oracle",
            },
            {
                "id": "bamboohr:listing_url:https://hidden.bamboohr.com/careers",
                "name": "Hidden Pending (BambooHR)",
                "adapter": "bamboohr",
                "listing_url": "https://hidden.bamboohr.com/careers",
                "registryState": "pending",
                "pendingReason": "provider_migration_candidate",
                "migrationSourceIdentity": "static:hidden",
                "candidateState": "hidden",
            },
            {
                "id": "bamboohr:listing_url:https://rejected.bamboohr.com/careers",
                "name": "Rejected Pending (BambooHR)",
                "adapter": "bamboohr",
                "listing_url": "https://rejected.bamboohr.com/careers",
                "registryState": "rejected",
                "pendingReason": "provider_migration_candidate",
                "migrationSourceIdentity": "static:rejected",
            },
        ],
    )
    monkeypatch.setattr(jobs_registry, "SOURCE_REGISTRY_PENDING_PATH", pending_path)
    monkeypatch.setattr(
        jobs_registry,
        "STUDIO_SOURCE_REGISTRY",
        [
            {
                "id": "bamboohr:listing_url:https://dupe.bamboohr.com/careers",
                "name": "Active Dupe (BambooHR)",
                "adapter": "bamboohr",
                "listing_url": "https://dupe.bamboohr.com/careers",
                "enabledByDefault": True,
            }
        ],
    )

    rows = jobs_registry.registry_entries("bamboohr", include_pending_provider_migration=True)
    names = {row.get("name") for row in rows}

    assert "Valid Pending (BambooHR)" in names
    assert "Duplicate Pending (BambooHR)" not in names
    assert "Manual Pending (BambooHR)" not in names
    assert "Static Pending" not in names
    assert "Unsupported Pending" not in names
    assert "Hidden Pending (BambooHR)" not in names
    assert "Rejected Pending (BambooHR)" not in names
    assert jobs_registry.registry_entries("static", include_pending_provider_migration=True) == []
