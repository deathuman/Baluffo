import copy
import json
from pathlib import Path

from src import jobs_fetcher as jf
from src.jobs import registry as jobs_registry
from src.jobs.adapters import static_sources as static_sources_mod
from src.jobs.common.registry_defaults import REDUNDANT_STATIC_IF_PROVIDER
from tests.helpers.temp_paths import workspace_tmpdir

BANDAI_STATIC_ID = "static:listing_url:https://www.bandainamcoent.com/careers#join"
BANDAI_STATIC_SOURCE_NAME = f"static_source::{BANDAI_STATIC_ID}"
BANDAI_PROVIDER_LOADER_NAME = "greenhouse_boards"


def _bandai_validation_registry_rows():
    return [
        {
            "id": "greenhouse:slug:bandainamco",
            "name": "Bandai Namco Greenhouse",
            "adapter": "greenhouse",
            "slug": "bandainamco",
            "enabledByDefault": True,
        },
        {
            "id": BANDAI_STATIC_ID,
            "name": "Bandai Namco Static",
            "adapter": "static",
            "listing_url": "https://www.bandainamcoent.com/careers#join",
            "pages": ["https://www.bandainamcoent.com/careers#join"],
            "enabledByDefault": True,
        },
        {
            "id": "static:listing_url:https://www.bandainamcoent.com/careers#unlinked",
            "name": "Bandai Namco Unlinked Static",
            "adapter": "static",
            "listing_url": "https://www.bandainamcoent.com/careers#unlinked",
            "pages": ["https://www.bandainamcoent.com/careers#unlinked"],
            "enabledByDefault": True,
        },
    ]


def _bandai_ready_provider_state(**overrides):
    state = {
        "lastAdapter": "greenhouse",
        "providerCoverageStatus": "validated_provider",
        "providerCoverageConsecutiveSuccesses": 2,
        "providerCoverageLatestKeptCount": 7,
        "migrationSourceIdentity": BANDAI_STATIC_ID,
    }
    state.update(overrides)
    return {BANDAI_PROVIDER_LOADER_NAME: state}


def _provider_loader(**_: object):
    return [
        {
            "title": "Provider Engineer",
            "company": "Bandai Namco",
            "city": "Remote",
            "country": "Remote",
            "workType": "Remote",
            "contractType": "Full-time",
            "jobLink": "https://boards.greenhouse.io/bandainamco/jobs/provider-engineer",
            "sector": "Game",
            "sourceJobId": "provider-1",
            "postedAt": "2026-03-01",
        }
    ]


def _run_bandai_validation_pipeline(
    monkeypatch,
    *,
    source_state_rows,
    include_linked_static_validation: bool,
    source_loaders=None,
):
    registry_rows = _bandai_validation_registry_rows()
    original_registry_rows = copy.deepcopy(registry_rows)
    redundant_rules_before = copy.deepcopy(REDUNDANT_STATIC_IF_PROVIDER)
    monkeypatch.setattr(jobs_registry, "STUDIO_SOURCE_REGISTRY", registry_rows)
    monkeypatch.setattr(static_sources_mod, "STUDIO_SOURCE_REGISTRY", registry_rows)
    monkeypatch.setattr(
        jf,
        "default_source_loaders",
        lambda **_: [(BANDAI_PROVIDER_LOADER_NAME, _provider_loader)],
    )
    with workspace_tmpdir("jobs-fetcher-linked-static-validation") as tmp:
        out = Path(tmp)
        source_state = {
            "schemaVersion": jf.SCHEMA_VERSION,
            "sources": source_state_rows,
        }
        (out / "jobs-source-state.json").write_text(json.dumps(source_state), encoding="utf-8")
        report = jf.run_pipeline(
            output_dir=out,
            source_loaders=source_loaders,
            show_progress=False,
            force_refresh_all=True,
            include_linked_static_validation=include_linked_static_validation,
        )
    assert registry_rows == original_registry_rows
    assert REDUNDANT_STATIC_IF_PROVIDER == redundant_rules_before
    return report


def test_default_run_keeps_redundant_static_rule_filtered_rows_absent(monkeypatch):
    report = _run_bandai_validation_pipeline(
        monkeypatch,
        source_state_rows=_bandai_ready_provider_state(),
        include_linked_static_validation=False,
    )

    source_names = {row["name"] for row in report["sources"]}
    assert BANDAI_STATIC_SOURCE_NAME not in source_names
    assert report["staticSuppressionPolicy"]["suppressedCount"] == 0
    assert report["runtime"]["includeLinkedStaticValidation"] is False


def test_linked_static_validation_injects_filtered_static_for_dynamic_suppression(
    monkeypatch,
):
    report = _run_bandai_validation_pipeline(
        monkeypatch,
        source_state_rows=_bandai_ready_provider_state(),
        include_linked_static_validation=True,
    )

    suppressed = next(row for row in report["sources"] if row["name"] == BANDAI_STATIC_SOURCE_NAME)
    assert suppressed["status"] == "excluded"
    assert suppressed["exclusionReason"] == "dynamic_redundant_provider"
    assert suppressed["migrationSourceIdentity"] == BANDAI_STATIC_ID
    assert report["staticSuppressionPolicy"]["suppressedCount"] == 1
    assert report["providerStaticOverlap"]["suppressedStaticCount"] == 1
    assert report["runtime"]["includeLinkedStaticValidation"] is True
    assert all("unlinked" not in row["name"] for row in report["sources"])


def test_linked_static_validation_does_not_inject_below_threshold(monkeypatch):
    report = _run_bandai_validation_pipeline(
        monkeypatch,
        source_state_rows=_bandai_ready_provider_state(
            providerCoverageConsecutiveSuccesses=1,
        ),
        include_linked_static_validation=True,
    )

    source_names = {row["name"] for row in report["sources"]}
    assert BANDAI_STATIC_SOURCE_NAME not in source_names
    assert report["staticSuppressionPolicy"]["suppressedCount"] == 0


def test_linked_static_validation_does_not_affect_explicit_only_source_selection(monkeypatch):
    explicit_loader_name = "explicit_provider"
    report = _run_bandai_validation_pipeline(
        monkeypatch,
        source_state_rows=_bandai_ready_provider_state(),
        include_linked_static_validation=True,
        source_loaders=[(explicit_loader_name, _provider_loader)],
    )

    source_names = {row["name"] for row in report["sources"]}
    assert BANDAI_STATIC_SOURCE_NAME not in source_names
    assert explicit_loader_name in source_names
    assert report["staticSuppressionPolicy"]["suppressedCount"] == 0
    assert report["runtime"]["includeLinkedStaticValidation"] is True
