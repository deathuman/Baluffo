from __future__ import annotations

from src.bridge.registry_source_table import compact_registry_source_table_row


def test_compact_registry_source_table_row_keeps_allowlisted_values() -> None:
    row = {
        "id": "static:listing_url:https://example.com/jobs",
        "name": "Example",
        "jobsFound": 3,
        "weakSignal": True,
        "hiddenFromDefault": False,
        "lastProbeError": "",
        "candidateState": "pending",
        "detailPagesSample": ["https://example.com/jobs/1"],
        "sourceDirectory": {"raw": "large"},
    }

    compact = compact_registry_source_table_row(row)

    assert compact == {
        "id": "static:listing_url:https://example.com/jobs",
        "name": "Example",
        "jobsFound": 3,
        "weakSignal": True,
        "registryState": "pending",
    }


def test_compact_registry_source_table_row_applies_field_specific_truncation() -> None:
    compact = compact_registry_source_table_row(
        {
            "name": "n" * 300,
            "listing_url": "https://example.com/" + ("x" * 3000),
            "sourceDirectory": "s" * 3000,
        }
    )

    assert len(compact["name"]) == 256
    assert len(compact["listing_url"]) == 2048
    assert "sourceDirectory" not in compact


def test_compact_registry_source_table_row_filters_reasons_but_keeps_blockers() -> None:
    compact = compact_registry_source_table_row(
        {
            "id": "pending",
            "rankReasons": ["low_priority", "existing_family_match"],
            "reasons": ["existing_registry_match", "because"],
            "approvalBlockers": ["weak_signal", "zero_jobs"],
            "approvalBlockerLabels": ["Weak signal"],
        }
    )

    assert compact["rankReasons"] == ["existing_family_match"]
    assert compact["reasons"] == ["existing_registry_match"]
    assert compact["approvalBlockers"] == ["weak_signal", "zero_jobs"]
    assert compact["approvalBlockerLabels"] == ["Weak signal"]


def test_compact_registry_source_table_row_uses_pages_only_without_direct_url() -> None:
    with_direct_url = compact_registry_source_table_row(
        {
            "id": "active",
            "listing_url": "https://example.com/jobs",
            "pages": ["https://fallback.example/jobs"],
        }
    )
    without_direct_url = compact_registry_source_table_row(
        {
            "id": "pending",
            "pages": ["https://fallback.example/jobs", "https://fallback.example/other"],
        }
    )

    assert "pages" not in with_direct_url
    assert without_direct_url["pages"] == ["https://fallback.example/jobs"]
