import json
from pathlib import Path

from scripts import source_policy_soak_report as soak


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _gate_ids(report: dict[str, object]) -> set[str]:
    return {str(gate.get("id")) for gate in report["qualityGates"]}  # type: ignore[index, union-attr]


def _write_suppression_eligibility_runtime(
    data_dir: Path,
    *,
    source_rows: list[dict[str, object]] | None = None,
    include_static_registry: bool = True,
    static_bucket: str = "active",
    static_id: str = "static:studio",
    static_name: str = "Studio Static",
    static_adapter: str = "static",
    static_hidden: bool = False,
    pending_reason: str = "",
) -> None:
    _write_json(
        data_dir / "jobs-fetch-report.json",
        {
            "providerCoverage": {
                "totalProviderCandidates": 1,
                "statusCounts": {"validated_provider": 1},
            },
            "sources": source_rows or [],
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
                    "migrationSourceName": static_name,
                }
            }
        },
    )
    active_rows = [
        {
            "id": "provider:studio",
            "name": "Studio Greenhouse",
            "adapter": "greenhouse",
            "registryState": "active",
        }
    ]
    if include_static_registry:
        static_row = {
            "id": static_id,
            "name": static_name,
            "adapter": static_adapter,
            "registryState": static_bucket,
            "hiddenFromDefault": static_hidden,
            "pendingReason": pending_reason,
        }
        if static_bucket == "active":
            active_rows.append(static_row)
    _write_json(data_dir / "source-registry-active.json", active_rows)
    _write_json(
        data_dir / "source-registry-pending.json",
        [static_row] if include_static_registry and static_bucket == "pending" else [],
    )
    _write_json(data_dir / "source-registry-rejected.json", [])
    _write_json(data_dir / "source-sync.json", {"schemaVersion": 2, "active": [], "pending": []})


def test_ready_provider_missing_linked_static_source_row_is_reported(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    _write_suppression_eligibility_runtime(data_dir)

    report = soak.build_soak_report(data_dir)
    section = report["sections"]["suppressionEligibility"]

    assert section["readyLinkedProviderCount"] == 1
    assert section["selectedLinkedStaticCount"] == 0
    assert section["missingLinkedStaticCount"] == 1
    assert section["suppressedLinkedStaticCount"] == 0
    row = section["missingLinkedStaticRows"][0]
    assert row["reason"] == "linked_static_not_in_default_loader_set"
    assert row["selectionReason"] == "linked_static_not_in_default_loader_set"
    assert row["providerSourceId"] == "provider:studio"
    assert row["registryBucket"] == "active"
    assert row["expectedLoaderName"] == "static_source::static:studio"
    assert row["foundInActiveRegistry"] is True
    assert row["foundInSourceRows"] is False
    assert "ready_provider_linked_static_not_selected" in _gate_ids(report)
    assert report["sections"]["sourceSyncCleanliness"]["clean"] is True


def test_ready_provider_selected_suppressed_static_is_counted(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    _write_suppression_eligibility_runtime(
        data_dir,
        source_rows=[
            {
                "name": "static_source::static:studio",
                "status": "excluded",
                "exclusionReason": "dynamic_redundant_provider",
            }
        ],
    )

    report = soak.build_soak_report(data_dir)
    section = report["sections"]["suppressionEligibility"]

    assert section["readyLinkedProviderCount"] == 1
    assert section["selectedLinkedStaticCount"] == 1
    assert section["missingLinkedStaticCount"] == 0
    assert section["suppressedLinkedStaticCount"] == 1
    assert section["missingLinkedStaticRows"] == []


def test_ready_provider_selected_static_without_suppression_is_reported(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "data"
    _write_suppression_eligibility_runtime(
        data_dir,
        source_rows=[{"name": "static_source::static:studio", "status": "ok"}],
    )

    report = soak.build_soak_report(data_dir)
    section = report["sections"]["suppressionEligibility"]

    assert section["selectedLinkedStaticCount"] == 1
    assert section["suppressedLinkedStaticCount"] == 0
    row = section["missingLinkedStaticRows"][0]
    assert row["reason"] == "linked_static_selected_not_suppressed"
    assert row["selectionReason"] == "linked_static_selected_not_suppressed"
    assert row["foundInSourceRows"] is True


def test_ready_provider_static_missing_from_registry_warns(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    _write_suppression_eligibility_runtime(data_dir, include_static_registry=False)

    report = soak.build_soak_report(data_dir)
    section = report["sections"]["suppressionEligibility"]

    assert section["missingLinkedStaticRows"][0]["reason"] == "linked_static_missing_from_registry"
    assert section["missingLinkedStaticRows"][0]["linkedStaticFoundInRegistry"] is False
    assert section["missingLinkedStaticRows"][0]["registryBucket"] == ""
    assert "ready_provider_linked_static_missing_from_registry" in _gate_ids(report)


def test_ready_provider_pending_static_reports_pending_not_default(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    _write_suppression_eligibility_runtime(data_dir, static_bucket="pending")

    report = soak.build_soak_report(data_dir)
    row = report["sections"]["suppressionEligibility"]["missingLinkedStaticRows"][0]

    assert row["selectionReason"] == "linked_static_pending_not_default"
    assert row["registryBucket"] == "pending"
    assert row["foundInPendingRegistry"] is True


def test_ready_provider_hidden_pending_static_reports_hidden_pending(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    _write_suppression_eligibility_runtime(
        data_dir,
        static_bucket="pending",
        static_hidden=True,
        pending_reason="manual_review",
    )

    report = soak.build_soak_report(data_dir)
    row = report["sections"]["suppressionEligibility"]["missingLinkedStaticRows"][0]

    assert row["selectionReason"] == "linked_static_hidden_pending"
    assert row["hiddenFromDefault"] is True
    assert row["pendingReason"] == "manual_review"


def test_ready_provider_non_static_adapter_reports_adapter_mismatch(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    _write_suppression_eligibility_runtime(data_dir, static_adapter="greenhouse")

    report = soak.build_soak_report(data_dir)
    row = report["sections"]["suppressionEligibility"]["missingLinkedStaticRows"][0]

    assert row["selectionReason"] == "linked_static_adapter_not_static"
    assert row["adapter"] == "greenhouse"


def test_ready_provider_identity_mismatch_reports_likely_registry_row(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    _write_suppression_eligibility_runtime(
        data_dir,
        static_id="static:missing-identity",
        static_name="Studio Static",
        include_static_registry=False,
    )
    active = json.loads((data_dir / "source-registry-active.json").read_text(encoding="utf-8"))
    active.append(
        {
            "id": "static:actual-identity",
            "name": "Studio Static",
            "adapter": "static",
            "registryState": "active",
        }
    )
    _write_json(data_dir / "source-registry-active.json", active)

    report = soak.build_soak_report(data_dir)
    row = report["sections"]["suppressionEligibility"]["missingLinkedStaticRows"][0]

    assert row["selectionReason"] == "linked_static_registry_identity_mismatch"
    assert row["registryBucket"] == "active"
    assert row["sourceIdentity"] == "static:actual-identity"
