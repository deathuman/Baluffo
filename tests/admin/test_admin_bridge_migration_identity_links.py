from __future__ import annotations

import gzip
import json
from pathlib import Path

from src import admin_bridge
from src.bridge.routes.post_routes import handle_post
from src.bridge.source_policy_migration_links import ADMIN_MIGRATION_LINK_ACTOR
from src.jobs.common.contracts_provider_coverage import build_provider_coverage_summary
from src.jobs.common.registry_defaults import REDUNDANT_STATIC_IF_PROVIDER
from tests.helpers.bridge_api import FakeHandler

STATIC_ID = "static:listing_url:https://studio.example/jobs"
OTHER_STATIC_ID = "static:listing_url:https://other.example/jobs"
PROVIDER_ID = "greenhouse:slug:studio"


def _api():
    api = admin_bridge.build_bridge_api(admin_bridge.RUNTIME_CONFIG)
    api.now_iso = lambda: "2026-05-01T10:00:00Z"
    return api


def _read_json(path: Path):
    compressed = path.with_name(path.name + ".gz")
    if compressed.exists():
        with gzip.open(compressed, mode="rt", encoding="utf-8") as handle:
            return json.loads(handle.read())
    return json.loads(path.read_text(encoding="utf-8"))


def _seed_state(
    *,
    active: list[dict],
    pending: list[dict] | None = None,
    rejected: list[dict] | None = None,
) -> None:
    admin_bridge.save_json_atomic(admin_bridge.ACTIVE_PATH, active)
    admin_bridge.save_json_atomic(admin_bridge.PENDING_PATH, pending or [])
    admin_bridge.save_json_atomic(admin_bridge.REJECTED_PATH, rejected or [])
    _api().load_state()


def _provider_row(**extra) -> dict:
    return {
        "id": PROVIDER_ID,
        "name": "Studio Provider",
        "adapter": "greenhouse",
        "slug": "studio",
        "registryState": "active",
        "candidateState": "live",
        "enabledByDefault": True,
        **extra,
    }


def _static_row(source_id: str = STATIC_ID, *, adapter: str = "static") -> dict:
    return {
        "id": source_id,
        "name": "Studio Static",
        "adapter": adapter,
        "listing_url": "https://studio.example/jobs",
        "registryState": "active",
        "candidateState": "live",
        "enabledByDefault": True,
    }


def _valid_apply_payload(**extra) -> dict:
    return {
        "action": "apply_migration_identity_link",
        "providerSourceId": PROVIDER_ID,
        "staticSourceId": STATIC_ID,
        "staticSourceName": "Studio Static",
        "confidence": 0.95,
        "reasons": ["redundant_static_rule_exact_match"],
        "recommendationSource": "provider_coverage_link_backfill",
        "recommendedAction": "backfill_migration_identity_candidate",
        **extra,
    }


def _post(payload: dict) -> dict:
    handler = FakeHandler()
    handled = handle_post(
        handler,
        api=_api(),
        path="/source-policy/migration-link-action",
        payload=payload,
    )
    assert handled is True
    return handler.sent[-1]


def _row_by_id(rows: list[dict], row_id: str) -> dict:
    return next(row for row in rows if row.get("id") == row_id)


def test_apply_migration_identity_link_updates_active_provider_only(
    admin_bridge_entrypoint_root,
) -> None:
    _seed_state(active=[_provider_row(), _static_row()])
    admin_bridge.save_json_atomic(
        admin_bridge.SOURCE_POLICY_REVIEW_STATE_PATH, {"schemaVersion": 1, "pairs": {}}
    )
    admin_bridge.save_json_atomic(
        admin_bridge.SOURCE_POLICY_RECOMMENDATIONS_PATH,
        {"schemaVersion": 1, "pairs": []},
    )
    static_before = _row_by_id(_read_json(admin_bridge.ACTIVE_PATH), STATIC_ID)
    rejected_before = _read_json(admin_bridge.REJECTED_PATH)
    tombstones_before = _read_json(admin_bridge.TOMBSTONES_PATH)
    review_state_before = _read_json(admin_bridge.SOURCE_POLICY_REVIEW_STATE_PATH)
    recommendations_before = _read_json(admin_bridge.SOURCE_POLICY_RECOMMENDATIONS_PATH)
    rules_before = json.dumps(REDUNDANT_STATIC_IF_PROVIDER, sort_keys=True)

    response = _post(_valid_apply_payload())

    assert response["status"] == 200
    payload = response["payload"]
    assert payload["ok"] is True
    assert payload["changed"] is True
    assert payload["providerBucket"] == "active"
    assert payload["providerSourceId"] == PROVIDER_ID
    assert payload["staticSourceId"] == STATIC_ID
    provider = _row_by_id(_read_json(admin_bridge.ACTIVE_PATH), PROVIDER_ID)
    assert provider["migrationSourceIdentity"] == STATIC_ID
    assert provider["migrationSourceName"] == "Studio Static"
    assert provider["migrationConfidence"] == 0.95
    assert provider["migrationReasons"] == ["redundant_static_rule_exact_match"]
    assert provider["migrationLinkedAt"] == "2026-05-01T10:00:00Z"
    assert provider["migrationLinkedBy"] == ADMIN_MIGRATION_LINK_ACTOR
    assert provider["migrationLinkSource"] == "provider_coverage_link_backfill"
    assert provider["adapter"] == "greenhouse"
    assert provider["slug"] == "studio"
    assert provider["registryState"] == "active"
    assert _row_by_id(_read_json(admin_bridge.ACTIVE_PATH), STATIC_ID) == static_before
    assert _read_json(admin_bridge.REJECTED_PATH) == rejected_before
    assert _read_json(admin_bridge.TOMBSTONES_PATH) == tombstones_before
    assert _read_json(admin_bridge.SOURCE_POLICY_REVIEW_STATE_PATH) == review_state_before
    assert _read_json(admin_bridge.SOURCE_POLICY_RECOMMENDATIONS_PATH) == recommendations_before
    assert json.dumps(REDUNDANT_STATIC_IF_PROVIDER, sort_keys=True) == rules_before


def test_apply_migration_identity_link_updates_pending_provider_bucket(
    admin_bridge_entrypoint_root,
) -> None:
    pending_provider = _provider_row(
        registryState="pending",
        candidateState="validated",
        enabledByDefault=False,
        pendingReason="provider_migration_candidate",
    )
    _seed_state(active=[_static_row()], pending=[pending_provider])

    response = _post(_valid_apply_payload())

    assert response["status"] == 200
    assert response["payload"]["providerBucket"] == "pending"
    assert (
        _row_by_id(_read_json(admin_bridge.PENDING_PATH), PROVIDER_ID)["migrationSourceIdentity"]
        == STATIC_ID
    )
    assert "migrationSourceIdentity" not in _row_by_id(
        _read_json(admin_bridge.ACTIVE_PATH), STATIC_ID
    )


def test_apply_migration_identity_link_rejects_invalid_inputs(
    admin_bridge_entrypoint_root,
) -> None:
    _seed_state(active=[_provider_row(), _static_row()])
    cases = [
        (_valid_apply_payload(providerSourceId="missing"), "provider_source_not_found"),
        (
            _valid_apply_payload(providerSourceId=STATIC_ID),
            "unsupported_provider_adapter",
        ),
        (_valid_apply_payload(staticSourceId="missing-static"), "static_source_not_found"),
        (
            _valid_apply_payload(recommendedAction="ambiguous_static_match"),
            "ambiguous_static_match",
        ),
        (_valid_apply_payload(confidence=0.74), "migration_identity_confidence_below_threshold"),
    ]

    for payload, expected_error in cases:
        response = _post(payload)
        assert response["status"] == 400
        assert response["payload"]["error"] == expected_error


def test_apply_migration_identity_link_rejects_already_linked_to_different_static(
    admin_bridge_entrypoint_root,
) -> None:
    _seed_state(
        active=[
            _provider_row(migrationSourceIdentity=OTHER_STATIC_ID),
            _static_row(),
            _static_row(OTHER_STATIC_ID),
        ]
    )

    response = _post(_valid_apply_payload())

    assert response["status"] == 400
    assert (
        response["payload"]["error"]
        == "migration_identity_already_linked_to_different_static_source"
    )


def test_clear_migration_identity_link_removes_only_backfill_metadata(
    admin_bridge_entrypoint_root,
) -> None:
    _seed_state(
        active=[
            _provider_row(
                migrationSourceIdentity=STATIC_ID,
                migrationSourceName="Studio Static",
                migrationConfidence=0.95,
                migrationReasons=["redundant_static_rule_exact_match"],
                migrationLinkedAt="2026-05-01T10:00:00Z",
                migrationLinkedBy=ADMIN_MIGRATION_LINK_ACTOR,
                migrationLinkSource="provider_coverage_link_backfill",
                customProviderField="keep",
            ),
            _static_row(),
        ]
    )
    static_before = _row_by_id(_read_json(admin_bridge.ACTIVE_PATH), STATIC_ID)

    response = _post(
        {
            "action": "clear_migration_identity_link",
            "providerSourceId": PROVIDER_ID,
            "staticSourceId": STATIC_ID,
        }
    )

    assert response["status"] == 200
    assert response["payload"]["changed"] is True
    provider = _row_by_id(_read_json(admin_bridge.ACTIVE_PATH), PROVIDER_ID)
    assert provider["customProviderField"] == "keep"
    for field in (
        "migrationSourceIdentity",
        "migrationSourceName",
        "migrationConfidence",
        "migrationReasons",
        "migrationLinkedAt",
        "migrationLinkedBy",
        "migrationLinkSource",
    ):
        assert field not in provider
    assert _row_by_id(_read_json(admin_bridge.ACTIVE_PATH), STATIC_ID) == static_before


def test_clear_migration_identity_link_rejects_non_owned_and_mismatched_links(
    admin_bridge_entrypoint_root,
) -> None:
    _seed_state(
        active=[
            _provider_row(
                migrationSourceIdentity=STATIC_ID,
                migrationLinkedBy="provider_migration_staging",
            ),
            _static_row(),
            _static_row(OTHER_STATIC_ID),
        ]
    )
    non_owned = _post(
        {
            "action": "clear_migration_identity_link",
            "providerSourceId": PROVIDER_ID,
            "staticSourceId": STATIC_ID,
        }
    )
    mismatched = _post(
        {
            "action": "clear_migration_identity_link",
            "providerSourceId": PROVIDER_ID,
            "staticSourceId": OTHER_STATIC_ID,
        }
    )

    assert non_owned["status"] == 400
    assert non_owned["payload"]["error"] == "migration_identity_not_owned_by_backfill_action"
    assert mismatched["status"] == 400
    assert mismatched["payload"]["error"] == "migration_identity_static_source_mismatch"


def test_migration_identity_link_is_visible_to_provider_coverage(
    admin_bridge_entrypoint_root,
) -> None:
    _seed_state(active=[_provider_row(), _static_row()])

    response = _post(_valid_apply_payload())
    provider = dict(response["payload"]["providerRow"])
    provider["providerCoverageStatus"] = "validated_provider"
    provider["providerCoverageConsecutiveSuccesses"] = 2
    provider["providerCoverageLatestKeptCount"] = 4
    coverage = build_provider_coverage_summary({PROVIDER_ID: provider})

    assert coverage["totalProviderCandidates"] == 1
    assert coverage["statusCounts"]["validated_provider"] == 1
    assert coverage["validatedProviders"][0]["migrationSourceIdentity"] == STATIC_ID
