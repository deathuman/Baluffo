"""Source execution flow helpers for the package-owned pipeline entrypoint."""

from __future__ import annotations

from typing import Any

from src.jobs.adapters import static_sources as static_sources_mod
from src.jobs.pipeline_stage_source_execution import run_source_execution_stage
from src.jobs.registry import registry_entries
from src.jobs.state_source_state import append_excluded_default_sources
from src.jobs.text_utils import clean_text

from .pipeline_run_setup import PipelineRunSetup

_PROVIDER_METADATA_FIELDS = (
    "createdFromAdvisory",
    "migrationSourceIdentity",
    "detectedProviderFamily",
    "detectedProviderUrl",
    "detectedProviderId",
    "migrationConfidence",
    "migrationReasons",
)
_PROVIDER_REGISTRY_ADAPTERS = (
    "greenhouse",
    "lever",
    "ashby",
    "smartrecruiters",
    "workable",
    "recruitee",
    "pinpoint",
    "teamtailor",
    "bamboohr",
    "workday",
    "personio",
    "breezy",
    "jazzhr",
    "oracle_hcm",
)


def _attach_static_source_provenance(source_reports: list[dict[str, Any]]) -> None:
    static_name_to_row: dict[str, dict[str, Any]] = {}
    for row in registry_entries("static"):
        name = static_sources_mod.static_source_name_for_registry_row(row)
        static_name_to_row[name] = row
    for report in source_reports:
        if not isinstance(report, dict):
            continue
        source_name = clean_text(report.get("name"))
        registry_row = static_name_to_row.get(source_name)
        if registry_row is None:
            continue
        if clean_text(registry_row.get("sourceDirectory")):
            report["sourceDirectory"] = clean_text(registry_row.get("sourceDirectory"))
        if clean_text(registry_row.get("sourceDirectoryUrl")):
            report["sourceDirectoryUrl"] = clean_text(registry_row.get("sourceDirectoryUrl"))
        if clean_text(registry_row.get("listing_url")):
            report["listingUrl"] = clean_text(registry_row.get("listing_url"))


def _registry_source_name(row: dict[str, Any], adapter: str) -> str:
    return (
        clean_text(row.get("name"))
        or clean_text(row.get("studio"))
        or clean_text(row.get("slug"))
        or clean_text(row.get("account"))
        or clean_text(row.get("company_id"))
        or clean_text(row.get("subdomain"))
        or f"{adapter}_source"
    )


def _provider_migration_registry_rows() -> dict[str, dict[str, Any]]:
    rows_by_name: dict[str, dict[str, Any]] = {}
    for adapter in _PROVIDER_REGISTRY_ADAPTERS:
        for row in registry_entries(adapter):
            if not isinstance(row, dict) or not clean_text(row.get("migrationSourceIdentity")):
                continue
            name = _registry_source_name(row, adapter)
            if name:
                rows_by_name[name] = row
    return rows_by_name


def _copy_provider_migration_metadata(report: dict[str, Any], registry_row: dict[str, Any]) -> None:
    for key in _PROVIDER_METADATA_FIELDS:
        value = registry_row.get(key)
        if value not in (None, ""):
            report[key] = value


def _attach_provider_migration_metadata(source_reports: list[dict[str, Any]]) -> None:
    rows_by_name = _provider_migration_registry_rows()
    if not rows_by_name:
        return
    for report in source_reports:
        if not isinstance(report, dict):
            continue
        details = report.get("details")
        for item in details if isinstance(details, list) else []:
            if not isinstance(item, dict):
                continue
            source_name = clean_text(item.get("name"))
            registry_row = rows_by_name.get(source_name)
            if registry_row:
                _copy_provider_migration_metadata(item, registry_row)


def _close_runtime_resources(async_fetcher: Any, redirect_resolver: Any) -> None:
    if async_fetcher is not None:
        async_fetcher.close()
    close_redirect_resolver = getattr(redirect_resolver, "close", None)
    if callable(close_redirect_resolver):
        close_redirect_resolver()


def execute_pipeline_sources(setup: PipelineRunSetup) -> None:
    try:
        run_source_execution_stage(
            config=setup.stage_config,
            selected_loaders=setup.selected_loaders,
            fetch_text_limited=setup.fetch_text_limited,
            fetch_text_static_limited=setup.fetch_text_static_limited,
            static_listing_async_fetch=setup.static_listing_async_fetch,
            source_state_rows=setup.source_state_rows,
            redirect_resolver=setup.redirect_resolver,
            task_runtime=setup.task_runtime,
            task_rows=setup.task_runtime.task_rows,
            task_lock=setup.task_runtime.task_lock,
            thread_local=setup.task_runtime.thread_local,
            write_task_state=setup.write_task_state,
            write_progress_report=setup.write_progress_report,
            canonical_rows=setup.canonical_rows,
            source_reports=setup.source_reports,
        )
    finally:
        _close_runtime_resources(setup.async_fetcher, setup.redirect_resolver)

    if setup.using_default_loaders:
        append_excluded_default_sources(setup.source_reports)
    _attach_static_source_provenance(setup.source_reports)
    _attach_provider_migration_metadata(setup.source_reports)
