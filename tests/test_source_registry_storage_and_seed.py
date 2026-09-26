import json
from pathlib import Path
from typing import Any

from src import source_registry as sr
from src.bridge.registry_conflicts import derive_registry_conflict_queue
from tests.helpers.temp_paths import workspace_tmpdir


def _seed_rows_by_id() -> dict[str, dict[str, Any]]:
    rows = json.loads(
        Path("data/defaults/source-registry-active.seed.json").read_text(encoding="utf-8")
    )
    return {str(row.get("id") or ""): row for row in rows}


def _superseded_static_row(
    source_id: str, name: str, jobs_found: int, rank_score: int
) -> dict[str, Any]:
    """Stand-in for a static row the 2026-09-26 seed prune removed as provider-superseded.

    These tests exercise ``derive_registry_conflict_queue``, so the static row is input, not a seed
    assertion -- built inline for the same reason ``stale_homepage`` is. Values are what the
    pruned rows carried.
    """
    return {
        "id": source_id,
        "name": name,
        "studio": name.rsplit(" (", 1)[0],
        "adapter": "static",
        "registryState": "active",
        "jobsFound": jobs_found,
        "sampleCount": jobs_found,
        "rankScore": rank_score,
    }


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
    rows_by_id = _seed_rows_by_id()
    provider = rows_by_id["greenhouse:slug:bandainamco"]
    static = _superseded_static_row(
        "static:listing_url:https://www.bandainamcoent.com/careers#join",
        "Bandai Namco Entertainment America Inc. (Sheet)",
        7,
        48,
    )

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
    rows_by_id = _seed_rows_by_id()
    provider = rows_by_id["lever:account:bigtime"]
    static = _superseded_static_row(
        "static:listing_url:https://www.bigtime.gg/careers", "Big Time Studios (GameDevMap)", 0, 47
    )

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
    static = _superseded_static_row(
        "static:listing_url:https://azragames.com/careers/#opening", "Azra Games (Sheet)", 1, 43
    )
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


def test_default_bonfire_seed_lets_provider_replace_static_aliases() -> None:
    rows_by_id = _seed_rows_by_id()
    provider = rows_by_id["greenhouse:slug:bonfirestudiosinc"]
    default_static = _superseded_static_row(
        "static:listing_url:https://bonfirestudios.com/work-with-us/index.html",
        "Bonfire Studios (Sheet)",
        6,
        46,
    )
    runtime_static_alias = {
        **default_static,
        "id": "static:listing_url:https://bonfirestudios.com/work-with-us",
        "listing_url": "https://bonfirestudios.com/work-with-us",
        "careersUrl": "https://bonfirestudios.com/work-with-us",
        "pages": ["https://bonfirestudios.com/work-with-us"],
        "rankScore": 58,
    }

    assert provider["jobsFound"] == 6
    assert default_static["jobsFound"] == 6
    assert default_static["sampleCount"] == 6

    payload = derive_registry_conflict_queue(
        {"active": [runtime_static_alias, default_static, provider], "pending": [], "rejected": []}
    )
    conflict = payload["conflicts"][0]

    assert conflict["winner"]["id"] == "greenhouse:slug:bonfirestudiosinc"
    assert conflict["safeAutomation"]["eligible"] is True
    assert conflict["safeAutomation"]["targetIds"] == [
        "static:listing_url:https://bonfirestudios.com/work-with-us",
        "static:listing_url:https://bonfirestudios.com/work-with-us/index.html",
    ]


def test_default_ten_chambers_seed_keeps_valid_empty_careers_source_over_homepage() -> None:
    rows_by_id = _seed_rows_by_id()
    careers_source_id = "teamtailor:listing_url:https://careers.10chambers.com/jobs"
    careers = rows_by_id[careers_source_id]
    stale_homepage = _superseded_static_row(
        "static:listing_url:https://10chambers.com", "10 Chambers (GameDevMap)", 2, 35
    )

    assert careers["jobsFound"] == 0
    assert careers["sampleCount"] == 0

    payload = derive_registry_conflict_queue(
        {"active": [careers, stale_homepage], "pending": [], "rejected": []}
    )
    conflict = payload["conflicts"][0]

    assert conflict["winner"]["id"] == careers_source_id
    assert conflict["safeAutomation"]["eligible"] is True
    assert conflict["safeAutomation"]["targetIds"] == ["static:listing_url:https://10chambers.com"]
