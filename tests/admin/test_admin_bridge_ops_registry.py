from __future__ import annotations

from src.bridge import registry_tombstones
from src.source_registry import ensure_source_id, source_identity, source_url_fingerprint
from tests.helpers.bridge_api import build_admin_bridge_api


def test_add_manual_source_adds_and_deduplicates(admin_bridge_entrypoint_root):
    api = build_admin_bridge_api()

    added = api.add_manual_source("https://example.teamtailor.com/jobs/")
    assert added["status"] == "added"
    assert added.get("sourceId")

    duplicate = api.add_manual_source("https://example.teamtailor.com/jobs?utm=abc")
    assert duplicate["status"] == "duplicate"
    assert str(duplicate.get("sourceId") or "").lower() == str(added.get("sourceId") or "").lower()


def test_add_manual_source_rejects_invalid_url(admin_bridge_entrypoint_root):
    api = build_admin_bridge_api()
    invalid = api.add_manual_source("not-a-url")
    assert invalid["status"] == "invalid"


def test_add_manual_source_uses_static_fallback_for_unsupported_provider(
    admin_bridge_entrypoint_root,
):
    api = build_admin_bridge_api()
    added = api.add_manual_source("https://milestone.it/careers/")
    assert added["status"] == "added"
    source = added.get("source") or {}
    assert str(source.get("adapter") or "").lower() == "static"
    assert source.get("pages") == ["https://milestone.it/careers"]
    assert "generic website scraping fallback" in str(added.get("message") or "").lower()


def test_add_manual_source_static_fallback_deduplicates_by_normalized_url(
    admin_bridge_entrypoint_root,
):
    api = build_admin_bridge_api()
    first = api.add_manual_source("https://milestone.it/careers/")
    second = api.add_manual_source("https://milestone.it/careers?utm=x")
    assert first["status"] == "added"
    assert second["status"] == "duplicate"
    assert str(first.get("sourceId") or "").lower() == str(second.get("sourceId") or "").lower()


def test_add_manual_source_respects_local_tombstones(admin_bridge_entrypoint_root, monkeypatch):
    api = build_admin_bridge_api()
    tombstone_path = admin_bridge_entrypoint_root / "source-registry-tombstones.json"
    monkeypatch.setattr(registry_tombstones, "TOMBSTONES_PATH", tombstone_path)

    added = api.add_manual_source("https://example.teamtailor.com/jobs/")
    source_id = str(added.get("sourceId") or "")
    assert added["status"] == "added"
    assert source_id

    registry_tombstones.save_tombstones(
        {
            source_id: {
                "sourceId": source_id,
                "deletedAt": "2026-04-09T00:00:00Z",
                "deletedBy": "test",
                "reason": "registry_delete",
                "bucket": "pending",
                "source": added.get("source") or {},
            }
        }
    )

    blocked = api.add_manual_source("https://example.teamtailor.com/jobs/?utm=abc")
    assert blocked["status"] == "tombstoned"
    assert str(blocked.get("sourceId") or "").lower() == source_id.lower()


def test_trigger_source_check_returns_error_for_missing_source(admin_bridge_entrypoint_root):
    api = build_admin_bridge_api()
    result = api.trigger_source_check("missing-source-id")
    assert not result["started"]
    assert "not found" in str(result["error"]).lower()


def test_trigger_source_check_updates_pending_source_on_success(
    admin_bridge_entrypoint_root,
    monkeypatch,
):
    api = build_admin_bridge_api()
    added = api.add_manual_source("https://example.teamtailor.com/jobs")
    source_id = str(added.get("sourceId") or "")
    assert source_id

    monkeypatch.setattr(
        "src.admin_bridge.discovery.probe_candidate",
        lambda *_args, **_kwargs: (True, 4, ""),
    )
    result = api.trigger_source_check(source_id)
    assert result["started"]
    assert result["ok"]
    assert result["jobsFound"] == 4
    pending = api.load_state()["pending"]
    updated = next((row for row in pending if api.source_identity(row) == source_id.lower()), {})
    assert int(updated.get("jobsFound") or 0) == 4
    assert str(updated.get("lastProbeError") or "") == ""


def test_trigger_source_check_returns_failed_result_on_probe_error(
    admin_bridge_entrypoint_root,
    monkeypatch,
):
    api = build_admin_bridge_api()
    added = api.add_manual_source("https://another.teamtailor.com/jobs")
    source_id = str(added.get("sourceId") or "")
    assert source_id

    monkeypatch.setattr(
        "src.admin_bridge.discovery.probe_candidate",
        lambda *_args, **_kwargs: (False, 0, "timeout"),
    )
    result = api.trigger_source_check(source_id)
    assert result["started"]
    assert not result["ok"]
    assert "timeout" in str(result["error"]).lower()


def test_trigger_source_check_reconstructs_greenhouse_api_url_when_missing(
    admin_bridge_entrypoint_root,
    monkeypatch,
):
    api = build_admin_bridge_api()
    row = ensure_source_id(
        {
            "name": "Larian Studios",
            "studio": "Larian Studios",
            "adapter": "greenhouse",
            "slug": "larian-studios",
            "enabledByDefault": True,
        }
    )
    state = api.load_state()
    state["active"] = [row]
    state["pending"] = []
    state["rejected"] = []
    api.persist_state_and_auto_sync(state, reason="unit_test_seed")
    source_id = source_identity(row)

    calls = {"count": 0}

    def fake_probe(candidate, *_args, **_kwargs):  # noqa: ANN001
        calls["count"] += 1
        if calls["count"] == 1:
            return False, 0, "missing adapter or URL"
        assert (
            str(candidate.get("api_url") or "")
            == "https://boards-api.greenhouse.io/v1/boards/larian-studios/jobs"
        )
        return True, 9, ""

    monkeypatch.setattr("src.admin_bridge.discovery.probe_candidate", fake_probe)
    result = api.trigger_source_check(source_id)
    assert result["started"]
    assert result["ok"]
    assert int(result["jobsFound"]) == 9


def test_registry_delete_removes_selected_ids_from_all_buckets(admin_bridge_entrypoint_root):
    api = build_admin_bridge_api()
    active_row = ensure_source_id(
        {
            "name": "Active Source",
            "studio": "Active Studio",
            "adapter": "static",
            "pages": ["https://active.example.com/careers"],
            "listing_url": "https://active.example.com/careers",
            "enabledByDefault": True,
        }
    )
    pending_row = ensure_source_id(
        {
            "name": "Pending Source",
            "studio": "Pending Studio",
            "adapter": "static",
            "pages": ["https://pending.example.com/careers"],
            "listing_url": "https://pending.example.com/careers",
            "enabledByDefault": False,
        }
    )
    rejected_row = ensure_source_id(
        {
            "name": "Rejected Source",
            "studio": "Rejected Studio",
            "adapter": "static",
            "pages": ["https://rejected.example.com/careers"],
            "listing_url": "https://rejected.example.com/careers",
            "enabledByDefault": False,
        }
    )

    api.persist_state_and_auto_sync(
        {"active": [active_row], "pending": [pending_row], "rejected": [rejected_row]},
        reason="unit_test_seed",
    )
    state = api.load_state()
    selected = {api.source_identity(active_row), api.source_identity(rejected_row)}
    before = len(state["active"]) + len(state["pending"]) + len(state["rejected"])
    state["active"] = [row for row in state["active"] if api.source_identity(row) not in selected]
    state["pending"] = [row for row in state["pending"] if api.source_identity(row) not in selected]
    state["rejected"] = [
        row for row in state["rejected"] if api.source_identity(row) not in selected
    ]
    state = api.persist_state_and_auto_sync(state, reason="unit_test_delete")
    after = len(state["active"]) + len(state["pending"]) + len(state["rejected"])

    assert before - after == 2
    assert len(state["active"]) == 0
    assert len(state["pending"]) == 1
    assert len(state["rejected"]) == 0


def test_registry_delete_can_match_by_url_fingerprint(admin_bridge_entrypoint_root):
    api = build_admin_bridge_api()
    pending_row = ensure_source_id(
        {
            "name": "Pending URL Match",
            "studio": "Pending URL Match",
            "adapter": "static",
            "pages": ["https://url-delete.example.com/careers/"],
            "listing_url": "https://url-delete.example.com/careers/",
            "enabledByDefault": False,
        }
    )
    api.persist_state_and_auto_sync(
        {"active": [], "pending": [pending_row], "rejected": []},
        reason="unit_test_seed",
    )

    state = api.load_state()
    selected_urls = {"https://url-delete.example.com/careers"}

    def keep_row(row):
        row_url = source_url_fingerprint(row)
        if row_url and row_url in selected_urls:
            return False
        return True

    state["pending"] = [row for row in state["pending"] if keep_row(row)]
    state = api.persist_state_and_auto_sync(state, reason="unit_test_delete")
    assert len(state["pending"]) == 0


def test_load_state_normalizes_static_www_studio_placeholder(admin_bridge_entrypoint_root):
    api = build_admin_bridge_api()
    pending_row = {
        "name": "Www (Manual Website)",
        "studio": "Www",
        "company": "Www",
        "adapter": "static",
        "pages": ["https://www.nixxes.com/jobs"],
        "listing_url": "https://www.nixxes.com/jobs",
        "enabledByDefault": False,
    }
    api.persist_state_and_auto_sync(
        {"active": [], "pending": [pending_row], "rejected": []},
        reason="unit_test_seed",
    )
    state = api.load_state()
    row = state["pending"][0]
    assert str(row.get("studio") or "") == "Nixxes"
    assert str(row.get("name") or "") == "Nixxes (Manual Website)"
