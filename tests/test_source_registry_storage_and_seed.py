import json
from pathlib import Path

from src import source_registry as sr
from src.bridge.registry_conflicts import derive_registry_conflict_queue
from tests.helpers.temp_paths import workspace_tmpdir


def test_save_json_atomic_removes_stale_plain_registry_snapshot() -> None:
    with workspace_tmpdir("source-registry") as tmp:
        path = Path(tmp) / "source-registry-active.json"
        stale_row = {
            "id": "greenhouse:slug:stale",
            "name": "Stale Source",
            "adapter": "greenhouse",
            "registryState": "active",
        }
        payload = [
            {
                "id": "greenhouse:slug:fresh",
                "name": "Fresh Source",
                "adapter": "greenhouse",
                "studio": "Fresh Studio",
                "registryState": "active",
                "candidateState": "live",
            }
        ]
        path.write_text(json.dumps([stale_row]), encoding="utf-8")

        sr.save_json_atomic(path, payload)

        assert not path.exists()
        assert (Path(tmp) / "source-registry-active.json.gz").exists()
        assert sr.load_json_array(path, []) == payload

        path.write_text(json.dumps([stale_row]), encoding="utf-8")
        sr.save_json_atomic(path, payload)

        assert not path.exists()
        assert sr.load_json_array(path, []) == payload


def test_default_guerrilla_seed_uses_live_greenhouse_slug() -> None:
    active_rows = json.loads(
        Path("data/defaults/source-registry-active.seed.json").read_text(encoding="utf-8")
    )
    pending_rows = json.loads(
        Path("data/defaults/source-registry-pending.seed.json").read_text(encoding="utf-8")
    )

    active_ids = {str(row.get("id") or "") for row in active_rows}
    pending_by_id = {str(row.get("id") or ""): row for row in pending_rows}
    stale_pending = pending_by_id["greenhouse:slug:guerrillagames"]

    assert "greenhouse:slug:guerrilla-games" in active_ids
    assert "greenhouse:slug:guerrillagames" not in active_ids
    assert stale_pending["duplicateOfSourceId"] == "greenhouse:slug:guerrilla-games"


def test_default_bandai_seed_has_provider_count_for_static_replacement() -> None:
    active_rows = json.loads(
        Path("data/defaults/source-registry-active.seed.json").read_text(encoding="utf-8")
    )
    rows_by_id = {str(row.get("id") or ""): row for row in active_rows}
    provider = rows_by_id["greenhouse:slug:bandainamco"]
    static = rows_by_id["static:listing_url:https://www.bandainamcoent.com/careers#join"]

    assert provider["jobsFound"] == 7
    assert static["jobsFound"] == 7

    payload = derive_registry_conflict_queue(
        {"active": [provider, static], "pending": [], "rejected": []}
    )
    conflict = payload["conflicts"][0]

    assert conflict["winner"]["id"] == "greenhouse:slug:bandainamco"
    assert conflict["safeAutomation"]["eligible"] is True
    assert conflict["safeAutomation"]["targetIds"] == [
        "static:listing_url:https://www.bandainamcoent.com/careers#join"
    ]


def test_default_big_time_seed_lets_lever_provider_replace_static_board_link_page() -> None:
    active_rows = json.loads(
        Path("data/defaults/source-registry-active.seed.json").read_text(encoding="utf-8")
    )
    rows_by_id = {str(row.get("id") or ""): row for row in active_rows}
    provider = rows_by_id["lever:account:bigtime"]
    static = rows_by_id["static:listing_url:https://www.bigtime.gg/careers"]

    assert provider["jobsFound"] == 2
    assert static["jobsFound"] == 0

    payload = derive_registry_conflict_queue(
        {"active": [provider, static], "pending": [], "rejected": []}
    )
    conflict = payload["conflicts"][0]

    assert conflict["winner"]["id"] == "lever:account:bigtime"
    assert conflict["safeAutomation"]["eligible"] is True
    assert conflict["safeAutomation"]["targetIds"] == [
        "static:listing_url:https://www.bigtime.gg/careers"
    ]


def test_default_azra_seed_uses_current_static_count_for_provider_replacement() -> None:
    active_rows = json.loads(
        Path("data/defaults/source-registry-active.seed.json").read_text(encoding="utf-8")
    )
    rows_by_id = {str(row.get("id") or ""): row for row in active_rows}
    static = rows_by_id["static:listing_url:https://azragames.com/careers/#opening"]
    provider = {
        "id": "greenhouse:slug:azragames",
        "name": "Azra Games (Greenhouse)",
        "studio": "Azra Games",
        "adapter": "greenhouse",
        "registryState": "active",
        "jobsFound": 1,
    }

    assert static["jobsFound"] == 1
    assert static["sampleCount"] == 1

    payload = derive_registry_conflict_queue(
        {"active": [provider, static], "pending": [], "rejected": []}
    )
    conflict = payload["conflicts"][0]

    assert conflict["winner"]["id"] == "greenhouse:slug:azragames"
    assert conflict["safeAutomation"]["eligible"] is True
    assert conflict["safeAutomation"]["targetIds"] == [
        "static:listing_url:https://azragames.com/careers/#opening"
    ]
