import json
from pathlib import Path

from scripts import source_policy_soak_report as soak


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_runtime(data_dir: Path, *, include_static_registry: bool) -> str:
    static_id = "static:listing_url:https://studio.example/jobs"
    _write_json(
        data_dir / "jobs-fetch-report.json",
        {
            "providerCoverage": {
                "totalProviderCandidates": 1,
                "statusCounts": {"validated_provider": 1},
            },
            "sources": [],
        },
    )
    _write_json(
        data_dir / "jobs-source-state.json",
        {
            "sources": {
                "Studio Greenhouse": {
                    "lastAdapter": "greenhouse",
                    "providerCoverageStatus": "validated_provider",
                    "providerReplacementReadiness": "ready_later",
                    "providerCoverageConsecutiveSuccesses": 2,
                    "providerCoverageLatestKeptCount": 4,
                    "migrationSourceIdentity": static_id,
                    "migrationSourceName": "Studio Static",
                }
            }
        },
    )
    active_rows: list[dict[str, object]] = [
        {
            "id": "provider:studio",
            "name": "Studio Greenhouse",
            "adapter": "greenhouse",
            "registryState": "active",
            "migrationSourceIdentity": static_id,
            "migrationSourceName": "Provider Link Metadata",
        }
    ]
    if include_static_registry:
        active_rows.append(
            {
                "id": static_id,
                "name": "Studio Static",
                "adapter": "static",
                "registryState": "active",
                "listing_url": "https://studio.example/jobs",
            }
        )
    _write_json(data_dir / "source-registry-active.json", active_rows)
    _write_json(data_dir / "source-registry-pending.json", [])
    _write_json(data_dir / "source-registry-rejected.json", [])
    _write_json(data_dir / "source-sync.json", {"schemaVersion": 2, "active": [], "pending": []})
    return static_id


def test_linked_static_lookup_uses_static_registry_row_not_provider_metadata(
    tmp_path: Path,
) -> None:
    cases = [
        {
            "case_id": "missing-static-registry-row",
            "include_static_registry": False,
            "expected_fields": {
                "selectionReason": "linked_static_missing_from_registry",
                "linkedStaticFoundInRegistry": False,
                "linkedStaticAdapter": "",
                "registryId": "",
            },
        },
        {
            "case_id": "static-registry-row-wins",
            "include_static_registry": True,
            "expected_fields": {
                "selectionReason": "linked_static_not_in_default_loader_set",
                "registryId": "<static_id>",
                "linkedStaticAdapter": "static",
                "loaderNameMatchStatus": "exact_match",
            },
        },
    ]

    for case in cases:
        case_id = str(case["case_id"])
        data_dir = tmp_path / case_id
        static_id = _write_runtime(
            data_dir,
            include_static_registry=bool(case["include_static_registry"]),
        )

        report = soak.build_soak_report(data_dir)
        row = report["sections"]["suppressionEligibility"]["missingLinkedStaticRows"][0]

        expected_fields = dict(case["expected_fields"])
        if expected_fields.get("registryId") == "<static_id>":
            expected_fields["registryId"] = static_id
        for key, expected in expected_fields.items():
            assert row[key] == expected, f"{case_id}:{key}"
