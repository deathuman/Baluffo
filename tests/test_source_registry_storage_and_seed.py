import json
from pathlib import Path

from src import source_registry as sr
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
