import json

from src import admin_bridge
from src.bridge.routes.post_routes import handle_post
from src.jobs.common.registry_defaults import REDUNDANT_STATIC_IF_PROVIDER
from tests.helpers.bridge_api import FakeHandler

STATIC_SOURCE_ID = "static:listing_url:https://studio.example/jobs"
PROVIDER_SOURCE_ID = "Studio Greenhouse"


def _api():
    api = admin_bridge.build_bridge_api(admin_bridge.RUNTIME_CONFIG)
    api.now_iso = lambda: "2026-04-30T10:00:00Z"
    return api


def _post(action_payload):
    handler = FakeHandler()
    handled = handle_post(
        handler,
        api=_api(),
        path="/source-policy/review-action",
        payload=action_payload,
    )
    return handled, handler.sent[-1]


def test_source_policy_review_actions_update_review_artifact_only(
    admin_bridge_entrypoint_root,
) -> None:
    active_before = admin_bridge.load_json_object(admin_bridge.ACTIVE_PATH, [])
    pending_before = admin_bridge.load_json_object(admin_bridge.PENDING_PATH, [])
    rejected_before = admin_bridge.load_json_object(admin_bridge.REJECTED_PATH, [])
    tombstones_before = admin_bridge.load_json_object(admin_bridge.TOMBSTONES_PATH, {})
    redundant_rules_before = json.dumps(REDUNDANT_STATIC_IF_PROVIDER, sort_keys=True)

    handled, response = _post(
        {
            "action": "acknowledge",
            "staticSourceId": STATIC_SOURCE_ID,
            "staticSourceName": "static_source::studio",
            "providerSourceId": PROVIDER_SOURCE_ID,
            "providerSourceName": PROVIDER_SOURCE_ID,
            "notes": "local review",
        }
    )

    assert handled is True
    assert response["status"] == 200
    assert response["payload"]["pair"]["reviewState"] == "acknowledged"
    state = admin_bridge.load_json_object(admin_bridge.SOURCE_POLICY_REVIEW_STATE_PATH, {})
    assert state["summary"]["acknowledgedCount"] == 1
    assert admin_bridge.load_json_object(admin_bridge.ACTIVE_PATH, []) == active_before
    assert admin_bridge.load_json_object(admin_bridge.PENDING_PATH, []) == pending_before
    assert admin_bridge.load_json_object(admin_bridge.REJECTED_PATH, []) == rejected_before
    assert admin_bridge.load_json_object(admin_bridge.TOMBSTONES_PATH, {}) == tombstones_before
    assert json.dumps(REDUNDANT_STATIC_IF_PROVIDER, sort_keys=True) == redundant_rules_before


def test_source_policy_snooze_and_force_pause_are_local_review_state(
    admin_bridge_entrypoint_root,
) -> None:
    snoozed, snooze_response = _post(
        {
            "action": "snooze",
            "staticSourceId": STATIC_SOURCE_ID,
            "providerSourceId": PROVIDER_SOURCE_ID,
            "snoozedUntil": "2026-05-07T10:00:00Z",
        }
    )
    forced, force_response = _post(
        {
            "action": "force_pause",
            "staticSourceId": STATIC_SOURCE_ID,
            "providerSourceId": PROVIDER_SOURCE_ID,
        }
    )

    assert snoozed is True
    assert forced is True
    assert snooze_response["payload"]["pair"]["reviewState"] == "snoozed"
    assert force_response["payload"]["pair"]["manualSuppressionOverride"] == "force_pause"
    state = admin_bridge.load_json_object(admin_bridge.SOURCE_POLICY_REVIEW_STATE_PATH, {})
    assert state["summary"]["forcePausedCount"] == 1


def test_source_policy_clear_override_restores_none(admin_bridge_entrypoint_root) -> None:
    _post(
        {
            "action": "force_pause",
            "staticSourceId": STATIC_SOURCE_ID,
            "providerSourceId": PROVIDER_SOURCE_ID,
        }
    )
    _handled, response = _post(
        {
            "action": "clear_override",
            "staticSourceId": STATIC_SOURCE_ID,
            "providerSourceId": PROVIDER_SOURCE_ID,
        }
    )

    assert response["status"] == 200
    assert response["payload"]["pair"]["manualSuppressionOverride"] == "none"


def test_source_policy_invalid_actions_are_rejected(admin_bridge_entrypoint_root) -> None:
    _handled, invalid_response = _post(
        {
            "action": "force_suppress",
            "staticSourceId": STATIC_SOURCE_ID,
            "providerSourceId": PROVIDER_SOURCE_ID,
        }
    )
    _handled, missing_response = _post(
        {
            "action": "acknowledge",
            "staticSourceId": "",
            "providerSourceId": PROVIDER_SOURCE_ID,
        }
    )
    _handled, snooze_response = _post(
        {
            "action": "snooze",
            "staticSourceId": STATIC_SOURCE_ID,
            "providerSourceId": PROVIDER_SOURCE_ID,
            "snoozedUntil": "later",
        }
    )

    assert invalid_response["status"] == 400
    assert missing_response["status"] == 400
    assert snooze_response["status"] == 400
