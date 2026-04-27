from __future__ import annotations

from src.source_discovery.directory_adapter_templates import (
    apply_directory_provenance,
    build_directory_static_candidate,
    empty_directory_scan_result,
)


def test_build_directory_static_candidate_preserves_careers_url_shape() -> None:
    row = build_directory_static_candidate(
        studio="Example Studio",
        target_url="https://example.com/careers",
        nl_priority=True,
        website_only=False,
        name_suffix="Gameprog",
        discovery_method="gameprog",
        evidence_source="gameprog",
        evidence_types=["gameprog_directory"],
        source_directory="gameprog",
        source_directory_url="https://gameprog.it/",
        source_directory_entry_url="https://gameprog.it/studio/example",
        source_directory_location="Rome",
        careers_evidence_type="gameprog_careers_url",
        location_evidence_type="gameprog_location",
    )

    assert row["name"] == "Example Studio (Gameprog)"
    assert row["adapter"] == "static"
    assert row["listing_url"] == "https://example.com/careers"
    assert row["nlPriority"] is True
    assert row["enabledByDefault"] is False
    assert row["discoveryMethod"] == "gameprog"
    assert row["evidenceTypes"] == [
        "gameprog_directory",
        "gameprog_careers_url",
        "gameprog_location",
    ]
    assert row["sourceDirectory"] == "gameprog"
    assert row["sourceDirectoryLocation"] == "Rome"


def test_build_directory_static_candidate_preserves_website_only_shape() -> None:
    row = build_directory_static_candidate(
        studio="Example Studio",
        target_url="https://example.com/",
        nl_priority=False,
        website_only=True,
        name_suffix="Gamesmap",
        discovery_method="gamesmap",
        evidence_source="gamesmap",
        evidence_types=["gamesmap_directory", "gamesmap_category_match"],
        source_directory="gamesmap",
        source_directory_url="https://www.gamesmap.de/",
        source_directory_entry_url="https://www.gamesmap.de/en/company/example",
        source_directory_location="Hamburg",
        source_directory_categories=["Developer", "Developer"],
        manual_only=True,
        website_evidence_types=["gamesmap_website", "gamesmap_website_only"],
        location_evidence_type="gamesmap_location",
    )

    assert row["name"] == "Example Studio (Gamesmap)"
    assert row["adapter"] == "static"
    assert row["careersUrl"] == ""
    assert row["weakSignal"] is True
    assert row["manualOnly"] is True
    assert row["evidenceScore"] == 24
    assert row["evidenceTypes"] == [
        "gamesmap_directory",
        "gamesmap_category_match",
        "gamesmap_website",
        "gamesmap_website_only",
        "gamesmap_location",
    ]
    assert row["sourceDirectoryCategories"] == ["Developer"]


def test_apply_directory_provenance_preserves_candidate_and_adds_metadata() -> None:
    row = apply_directory_provenance(
        {
            "studio": "Example Studio",
            "adapter": "greenhouse",
            "careersUrl": "",
            "evidenceTypes": ["greenhouse"],
            "evidenceScore": 30,
        },
        evidence_source="gamesmap",
        evidence_types=["gamesmap_directory", "gamesmap_website_fetch"],
        source_directory="gamesmap",
        source_directory_url="https://www.gamesmap.de/",
        source_directory_entry_url="https://www.gamesmap.de/en/company/example",
        source_directory_location="Hamburg",
        source_directory_categories=["Developer"],
        careers_url_fallback="https://example.com/",
        evidence_score_floor=44,
    )

    assert row["adapter"] == "greenhouse"
    assert row["evidenceSource"] == "gamesmap"
    assert row["evidenceTypes"] == [
        "greenhouse",
        "gamesmap_directory",
        "gamesmap_website_fetch",
    ]
    assert row["evidenceScore"] == 44
    assert row["careersUrl"] == "https://example.com/"
    assert row["sourceDirectoryCategories"] == ["Developer"]


def test_empty_directory_scan_result_preserves_template_fields() -> None:
    failures = [{"adapter": "gameprog", "stage": "teams_fetch"}]
    summary = {"parsedRows": 0, "websiteFetchJobs": 0}
    batch_timing = {"teamsFetchMs": 2}

    row = empty_directory_scan_result(
        failures=failures,
        summary=summary,
        batch_timing=batch_timing,
        write_cache=False,
    )

    assert row == {
        "providerCandidates": [],
        "staticCandidates": [],
        "failures": failures,
        "summary": summary,
        "websiteFetchJobs": [],
        "browserRecoveryCandidates": [],
        "batchTiming": batch_timing,
        "writeCache": False,
    }
