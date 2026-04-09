from src import admin_bridge
from src.bridge import registry_tombstones


def test_add_manual_source_adds_and_deduplicates(admin_bridge_ops_root):
    added = admin_bridge.add_manual_source("https://example.teamtailor.com/jobs/")
    assert added["status"] == "added"
    assert added.get("sourceId")

    duplicate = admin_bridge.add_manual_source("https://example.teamtailor.com/jobs?utm=abc")
    assert duplicate["status"] == "duplicate"
    assert str(duplicate.get("sourceId") or "").lower() == str(added.get("sourceId") or "").lower()


def test_add_manual_source_rejects_invalid_url(admin_bridge_ops_root):
    invalid = admin_bridge.add_manual_source("not-a-url")
    assert invalid["status"] == "invalid"


def test_add_manual_source_uses_static_fallback_for_unsupported_provider(admin_bridge_ops_root):
    added = admin_bridge.add_manual_source("https://milestone.it/careers/")
    assert added["status"] == "added"
    source = added.get("source") or {}
    assert str(source.get("adapter") or "").lower() == "static"
    assert source.get("pages") == ["https://milestone.it/careers"]
    assert "generic website scraping fallback" in str(added.get("message") or "").lower()


def test_add_manual_source_static_fallback_deduplicates_by_normalized_url(admin_bridge_ops_root):
    first = admin_bridge.add_manual_source("https://milestone.it/careers/")
    second = admin_bridge.add_manual_source("https://milestone.it/careers?utm=x")
    assert first["status"] == "added"
    assert second["status"] == "duplicate"
    assert str(first.get("sourceId") or "").lower() == str(second.get("sourceId") or "").lower()


def test_add_manual_source_respects_local_tombstones(admin_bridge_ops_root, monkeypatch):
    tombstone_path = admin_bridge_ops_root / "source-registry-tombstones.json"
    monkeypatch.setattr(registry_tombstones, "TOMBSTONES_PATH", tombstone_path)
    monkeypatch.setattr(admin_bridge, "TOMBSTONES_PATH", tombstone_path)
    monkeypatch.setattr(admin_bridge.source_registry_module, "TOMBSTONES_PATH", tombstone_path)

    added = admin_bridge.add_manual_source("https://example.teamtailor.com/jobs/")
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

    blocked = admin_bridge.add_manual_source("https://example.teamtailor.com/jobs/?utm=abc")
    assert blocked["status"] == "tombstoned"
    assert str(blocked.get("sourceId") or "").lower() == source_id.lower()


def test_trigger_source_check_returns_error_for_missing_source(admin_bridge_ops_root):
    result = admin_bridge.trigger_source_check("missing-source-id")
    assert not result["started"]
    assert "not found" in str(result["error"]).lower()


def test_trigger_source_check_updates_pending_source_on_success(admin_bridge_ops_root):
    added = admin_bridge.add_manual_source("https://example.teamtailor.com/jobs")
    source_id = str(added.get("sourceId") or "")
    assert source_id

    original_probe = admin_bridge.discovery.probe_candidate
    try:
        admin_bridge.discovery.probe_candidate = lambda *_args, **_kwargs: (True, 4, "")
        result = admin_bridge.trigger_source_check(source_id)
        assert result["started"]
        assert result["ok"]
        assert result["jobsFound"] == 4
        pending = admin_bridge.load_json_array(admin_bridge.PENDING_PATH, [])
        updated = next(
            (row for row in pending if admin_bridge.source_identity(row) == source_id.lower()), {}
        )
        assert int(updated.get("jobsFound") or 0) == 4
        assert str(updated.get("lastProbeError") or "") == ""
    finally:
        admin_bridge.discovery.probe_candidate = original_probe


def test_trigger_source_check_returns_failed_result_on_probe_error(admin_bridge_ops_root):
    added = admin_bridge.add_manual_source("https://another.teamtailor.com/jobs")
    source_id = str(added.get("sourceId") or "")
    assert source_id

    original_probe = admin_bridge.discovery.probe_candidate
    try:
        admin_bridge.discovery.probe_candidate = lambda *_args, **_kwargs: (False, 0, "timeout")
        result = admin_bridge.trigger_source_check(source_id)
        assert result["started"]
        assert not result["ok"]
        assert "timeout" in str(result["error"]).lower()
    finally:
        admin_bridge.discovery.probe_candidate = original_probe


def test_trigger_source_check_reconstructs_greenhouse_api_url_when_missing(admin_bridge_ops_root):
    row = {
        "name": "Larian Studios",
        "studio": "Larian Studios",
        "adapter": "greenhouse",
        "slug": "larian-studios",
        "enabledByDefault": True,
    }
    row = admin_bridge.ensure_source_id(row)
    admin_bridge.save_json_atomic(admin_bridge.ACTIVE_PATH, [row])
    source_id = admin_bridge.source_identity(row)

    original_probe = admin_bridge.discovery.probe_candidate
    try:
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

        admin_bridge.discovery.probe_candidate = fake_probe
        result = admin_bridge.trigger_source_check(source_id)
        assert result["started"]
        assert result["ok"]
        assert int(result["jobsFound"]) == 9
    finally:
        admin_bridge.discovery.probe_candidate = original_probe


def test_registry_delete_removes_selected_ids_from_all_buckets(admin_bridge_ops_root):
    active_row = {
        "name": "Active Source",
        "studio": "Active Studio",
        "adapter": "static",
        "pages": ["https://active.example.com/careers"],
        "listing_url": "https://active.example.com/careers",
        "enabledByDefault": True,
    }
    pending_row = {
        "name": "Pending Source",
        "studio": "Pending Studio",
        "adapter": "static",
        "pages": ["https://pending.example.com/careers"],
        "listing_url": "https://pending.example.com/careers",
        "enabledByDefault": False,
    }
    rejected_row = {
        "name": "Rejected Source",
        "studio": "Rejected Studio",
        "adapter": "static",
        "pages": ["https://rejected.example.com/careers"],
        "listing_url": "https://rejected.example.com/careers",
        "enabledByDefault": False,
    }
    active_row = admin_bridge.ensure_source_id(active_row)
    pending_row = admin_bridge.ensure_source_id(pending_row)
    rejected_row = admin_bridge.ensure_source_id(rejected_row)
    admin_bridge.save_json_atomic(admin_bridge.ACTIVE_PATH, [active_row])
    admin_bridge.save_json_atomic(admin_bridge.PENDING_PATH, [pending_row])
    admin_bridge.save_json_atomic(admin_bridge.REJECTED_PATH, [rejected_row])

    state = admin_bridge.load_state()
    selected = {
        admin_bridge.source_identity(active_row),
        admin_bridge.source_identity(rejected_row),
    }
    before = len(state["active"]) + len(state["pending"]) + len(state["rejected"])
    state["active"] = [
        row for row in state["active"] if admin_bridge.source_identity(row) not in selected
    ]
    state["pending"] = [
        row for row in state["pending"] if admin_bridge.source_identity(row) not in selected
    ]
    state["rejected"] = [
        row for row in state["rejected"] if admin_bridge.source_identity(row) not in selected
    ]
    state = admin_bridge.persist_state(state)
    after = len(state["active"]) + len(state["pending"]) + len(state["rejected"])

    assert before - after == 2
    assert len(state["active"]) == 0
    assert len(state["pending"]) == 1
    assert len(state["rejected"]) == 0


def test_registry_delete_can_match_by_url_fingerprint(admin_bridge_ops_root):
    pending_row = {
        "name": "Pending URL Match",
        "studio": "Pending URL Match",
        "adapter": "static",
        "pages": ["https://url-delete.example.com/careers/"],
        "listing_url": "https://url-delete.example.com/careers/",
        "enabledByDefault": False,
    }
    pending_row = admin_bridge.ensure_source_id(pending_row)
    admin_bridge.save_json_atomic(admin_bridge.ACTIVE_PATH, [])
    admin_bridge.save_json_atomic(admin_bridge.PENDING_PATH, [pending_row])
    admin_bridge.save_json_atomic(admin_bridge.REJECTED_PATH, [])

    state = admin_bridge.load_state()
    selected_urls = {"https://url-delete.example.com/careers"}

    def keep_row(row):
        row_id = admin_bridge.source_identity(row)
        row_url = admin_bridge.source_url_fingerprint(row)
        if row_id in set():
            return False
        if row_url and row_url in selected_urls:
            return False
        return True

    state["pending"] = [row for row in state["pending"] if keep_row(row)]
    state = admin_bridge.persist_state(state)
    assert len(state["pending"]) == 0


def test_load_state_normalizes_static_www_studio_placeholder(admin_bridge_ops_root):
    pending_row = {
        "name": "Www (Manual Website)",
        "studio": "Www",
        "company": "Www",
        "adapter": "static",
        "pages": ["https://www.nixxes.com/jobs"],
        "listing_url": "https://www.nixxes.com/jobs",
        "enabledByDefault": False,
    }
    admin_bridge.save_json_atomic(admin_bridge.ACTIVE_PATH, [])
    admin_bridge.save_json_atomic(admin_bridge.PENDING_PATH, [pending_row])
    admin_bridge.save_json_atomic(admin_bridge.REJECTED_PATH, [])
    state = admin_bridge.load_state()
    row = state["pending"][0]
    assert str(row.get("studio") or "") == "Nixxes"
    assert str(row.get("name") or "") == "Nixxes (Manual Website)"
