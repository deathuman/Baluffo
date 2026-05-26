from __future__ import annotations

from typing import Any

from src.bridge.registry_conflicts import (
    derive_registry_conflict_queue,
)
from src.bridge.registry_conflicts_row import _merge_fetch_report_source_details

OLD_FOCUS_ID = "recruitee:api_url:https://focushomeinteractive.recruitee.com/api/offers/"
NEW_FOCUS_ID = "recruitee:api_url:https://focusentertainment.recruitee.com/api/offers/"


def _focus_state() -> dict[str, list[dict[str, Any]]]:
    return {
        "active": [
            {
                "id": NEW_FOCUS_ID,
                "name": "Focus Entertainment (Recruitee)",
                "studio": "Focus Entertainment",
                "adapter": "recruitee",
                "registryState": "active",
                "jobsFound": 7,
            },
            {
                "id": OLD_FOCUS_ID,
                "name": "Focus Entertainment (Recruitee)",
                "studio": "Focus Entertainment",
                "adapter": "recruitee",
                "registryState": "active",
                "jobsFound": 2,
            },
        ],
        "pending": [],
        "rejected": [],
    }


def _rows_by_id(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        row["id"]: row
        for row in [
            payload["conflicts"][0]["winner"],
            *payload["conflicts"][0]["losers"],
        ]
    }


def test_registry_conflicts_ignores_ambiguous_name_only_source_state() -> None:
    source_state = {
        "schemaVersion": 1,
        "sources": {
            "Focus Entertainment (Recruitee)": {
                "lastStatus": "ok",
                "lastKeptCount": 7,
                "lastJobsKept": 7,
                "lastSuccessAt": "2026-05-09T11:15:13Z",
            },
        },
    }

    rows = _rows_by_id(derive_registry_conflict_queue(_focus_state(), source_state))

    assert rows[NEW_FOCUS_ID].get("sourceStateName") is None
    assert rows[NEW_FOCUS_ID].get("lastJobsKept") is None
    assert rows[OLD_FOCUS_ID].get("sourceStateName") is None
    assert rows[OLD_FOCUS_ID].get("lastJobsKept") is None


def test_registry_conflicts_matches_fetch_report_recruitee_provider_url_exactly() -> None:
    fetch_report = {
        "finishedAt": "2026-05-09T11:15:13Z",
        "sources": [
            {
                "name": "recruitee_sources",
                "adapter": "recruitee",
                "status": "ok",
                "details": [
                    {
                        "name": "Focus Entertainment (Recruitee)",
                        "studio": "Focus Entertainment",
                        "adapter": "recruitee",
                        "status": "ok",
                        "fetchedCount": 7,
                        "keptCount": 7,
                        "providerUrl": "https://focushomeinteractive.recruitee.com/api/offers/",
                    }
                ],
            }
        ],
    }
    source_state = _merge_fetch_report_source_details({"sources": {}}, fetch_report)

    rows = _rows_by_id(derive_registry_conflict_queue(_focus_state(), source_state))

    assert rows[OLD_FOCUS_ID]["sourceStateName"] == OLD_FOCUS_ID
    assert rows[OLD_FOCUS_ID]["lastJobsKept"] == 7
    assert rows[NEW_FOCUS_ID].get("sourceStateName") is None
    assert rows[NEW_FOCUS_ID].get("lastJobsKept") is None
