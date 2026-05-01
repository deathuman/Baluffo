import json
from pathlib import Path

from src.local_data_store import LocalDataPaths, LocalDataStore
from tests.helpers.temp_paths import workspace_tmpdir


def _review_artifact(*, review_state: str = "acknowledged") -> dict[str, object]:
    return {
        "schemaVersion": "1.0",
        "updatedAt": "2026-05-01T10:00:00Z",
        "pairs": {
            "static:studio||provider:studio": {
                "staticSourceId": "static:studio",
                "staticSourceName": "Static Studio",
                "providerSourceId": "provider:studio",
                "providerSourceName": "Studio Provider",
                "reviewState": review_state,
                "manualSuppressionOverride": "force_pause",
                "notes": "local review",
                "updatedAt": "2026-05-01T10:00:00Z",
                "updatedBy": "admin",
            }
        },
    }


def _recommendations_artifact() -> dict[str, object]:
    return {
        "schemaVersion": "1.0",
        "updatedAt": "2026-05-01T10:00:00Z",
        "summary": {},
        "pairs": [
            {
                "staticSourceId": "static:studio",
                "staticSourceName": "Static Studio",
                "providerSourceId": "provider:studio",
                "providerSourceName": "Studio Provider",
                "currentRecommendation": "stable_safe_redundant",
                "currentRecommendedAction": "keep_runtime_suppression",
                "confidence": 0.9,
                "firstSeenAt": "2026-04-30T10:00:00Z",
                "lastSeenAt": "2026-05-01T10:00:00Z",
                "safeRunCount": 3,
                "consecutiveSafeRunCount": 3,
                "lastProposal": "safe_redundant_static",
                "lastAuditStatus": "safe",
                "destructiveActionAllowed": False,
                "history": [],
            }
        ],
    }


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_backup_exports_source_policy_artifacts_and_counts() -> None:
    with workspace_tmpdir("local-data-source-policy-export") as tmp:
        data_dir = Path(tmp) / "data"
        store = LocalDataStore(LocalDataPaths.from_data_dir(data_dir))
        user = store.sign_in("SourcePolicyExport")
        uid = str(user["uid"])
        _write_json(data_dir / "source-policy-review-state.json", _review_artifact())
        _write_json(data_dir / "source-policy-recommendations.json", _recommendations_artifact())

        payload = store.export_profile_data(uid, include_files=False)

        assert payload["counts"]["sourcePolicyReviewPairs"] == 1
        assert payload["counts"]["sourcePolicyRecommendationPairs"] == 1
        assert payload["sourcePolicy"]["reviewState"]["summary"]["forcePausedCount"] == 1
        assert payload["sourcePolicy"]["recommendations"]["summary"]["stableSafeCount"] == 1
        assert payload["sourcePolicy"]["warnings"] == []


def test_backup_exports_empty_source_policy_artifacts_when_missing() -> None:
    with workspace_tmpdir("local-data-source-policy-missing") as tmp:
        store = LocalDataStore(LocalDataPaths.from_data_dir(Path(tmp) / "data"))
        user = store.sign_in("SourcePolicyMissing")
        uid = str(user["uid"])

        payload = store.export_profile_data(uid, include_files=False)

        assert payload["counts"]["sourcePolicyReviewPairs"] == 0
        assert payload["counts"]["sourcePolicyRecommendationPairs"] == 0
        assert payload["sourcePolicy"]["reviewState"]["pairs"] == {}
        assert payload["sourcePolicy"]["recommendations"]["pairs"] == []
        assert payload["sourcePolicy"]["warnings"] == []


def test_backup_exports_malformed_source_policy_artifacts_as_empty_with_warnings() -> None:
    with workspace_tmpdir("local-data-source-policy-malformed") as tmp:
        data_dir = Path(tmp) / "data"
        store = LocalDataStore(LocalDataPaths.from_data_dir(data_dir))
        user = store.sign_in("SourcePolicyMalformed")
        uid = str(user["uid"])
        review_path = data_dir / "source-policy-review-state.json"
        recommendations_path = data_dir / "source-policy-recommendations.json"
        review_path.write_text("{not-json", encoding="utf-8")
        recommendations_path.write_text(json.dumps(["not", "object"]), encoding="utf-8")

        payload = store.export_profile_data(uid, include_files=False)

        assert payload["sourcePolicy"]["reviewState"]["pairs"] == {}
        assert payload["sourcePolicy"]["recommendations"]["pairs"] == []
        assert payload["counts"]["sourcePolicyReviewPairs"] == 0
        assert payload["counts"]["sourcePolicyRecommendationPairs"] == 0
        assert len(payload["sourcePolicy"]["warnings"]) == 2
        assert review_path.read_text(encoding="utf-8") == "{not-json"
        assert json.loads(recommendations_path.read_text(encoding="utf-8")) == ["not", "object"]


def test_backup_import_restores_normalized_source_policy_artifacts() -> None:
    with workspace_tmpdir("local-data-source-policy-import") as tmp:
        data_dir = Path(tmp) / "data"
        store = LocalDataStore(LocalDataPaths.from_data_dir(data_dir))
        user = store.sign_in("SourcePolicyImport")
        uid = str(user["uid"])
        payload = {
            "schemaVersion": 2,
            "savedJobs": [],
            "attachments": [],
            "activityLog": [],
            "sourcePolicy": {
                "reviewState": {
                    "pairs": {
                        "raw": {
                            "staticSourceId": "static:studio",
                            "providerSourceId": "provider:studio",
                            "reviewState": "not-valid",
                            "manualSuppressionOverride": "force_suppress",
                            "notes": "x" * 550,
                        }
                    }
                },
                "recommendations": _recommendations_artifact(),
            },
        }

        result = store.import_profile_data(uid, payload)
        review_state = json.loads(
            (data_dir / "source-policy-review-state.json").read_text(encoding="utf-8")
        )
        recommendations = json.loads(
            (data_dir / "source-policy-recommendations.json").read_text(encoding="utf-8")
        )
        restored_pair = next(iter(review_state["pairs"].values()))

        assert result["sourcePolicyReviewRestored"] is True
        assert result["sourcePolicyRecommendationsRestored"] is True
        assert restored_pair["reviewState"] == "new"
        assert restored_pair["manualSuppressionOverride"] == "none"
        assert len(restored_pair["notes"]) == 500
        assert recommendations["summary"]["totalPairs"] == 1
        assert recommendations["pairs"][0]["currentRecommendation"] == "stable_safe_redundant"


def test_backup_import_skips_malformed_source_policy_without_clobbering_existing() -> None:
    with workspace_tmpdir("local-data-source-policy-import-malformed") as tmp:
        data_dir = Path(tmp) / "data"
        store = LocalDataStore(LocalDataPaths.from_data_dir(data_dir))
        user = store.sign_in("SourcePolicyImportMalformed")
        uid = str(user["uid"])
        review_path = data_dir / "source-policy-review-state.json"
        recommendations_path = data_dir / "source-policy-recommendations.json"
        _write_json(review_path, _review_artifact(review_state="reviewed"))
        _write_json(recommendations_path, _recommendations_artifact())
        review_before = review_path.read_text(encoding="utf-8")
        recommendations_before = recommendations_path.read_text(encoding="utf-8")

        result = store.import_profile_data(
            uid,
            {
                "schemaVersion": 2,
                "sourcePolicy": {
                    "reviewState": "bad-review-state",
                    "recommendations": ["bad-recommendations"],
                },
            },
        )

        assert result["sourcePolicyReviewRestored"] is False
        assert result["sourcePolicyRecommendationsRestored"] is False
        assert len([w for w in result["warnings"] if "sourcePolicy" in w]) == 2
        assert review_path.read_text(encoding="utf-8") == review_before
        assert recommendations_path.read_text(encoding="utf-8") == recommendations_before


def test_backup_export_import_do_not_mutate_registry_files() -> None:
    with workspace_tmpdir("local-data-source-policy-registry") as tmp:
        data_dir = Path(tmp) / "data"
        store = LocalDataStore(LocalDataPaths.from_data_dir(data_dir))
        user = store.sign_in("SourcePolicyRegistry")
        uid = str(user["uid"])
        registry_payloads = {
            "source-registry-active.json": [{"id": "active-1", "adapter": "static"}],
            "source-registry-pending.json": [{"id": "pending-1", "adapter": "teamtailor"}],
            "source-registry-rejected.json": [{"id": "rejected-1", "adapter": "lever"}],
            "source-registry-tombstones.json": {"source-x": {"reason": "local"}},
        }
        for filename, payload in registry_payloads.items():
            _write_json(data_dir / filename, payload)
        before = {
            filename: (data_dir / filename).read_text(encoding="utf-8")
            for filename in registry_payloads
        }

        payload = store.export_profile_data(uid, include_files=False)
        store.import_profile_data(uid, payload)

        after = {
            filename: (data_dir / filename).read_text(encoding="utf-8")
            for filename in registry_payloads
        }
        assert after == before
