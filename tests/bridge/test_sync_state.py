from __future__ import annotations

import json

from src.bridge.sync_state import SYNC_STATUS, SyncState
from tests.helpers.temp_paths import workspace_tmpdir


def test_sync_state_persists_runtime_status_fields() -> None:
    with workspace_tmpdir("sync-state") as data_dir:
        state = SyncState(data_dir=data_dir)

        before = state.load_sync_runtime_state()
        assert before["lastPullAt"] == ""
        assert before["lastPushAt"] == ""
        assert before["lastPullRemoteSha"] == ""
        assert before["lastPullRemoteGeneratedAt"] == ""
        assert before["lastPullSnapshotFormat"] == ""
        assert before["lastDiscoverySyncFinishedAt"] == ""
        assert before["counters"]["totalPushes"] == 0
        assert before["counters"]["totalPulls"] == 0

        state.set_sync_status(
            action="pull",
            result="ok",
            pulled=True,
            error="",
            last_pull_remote_sha="manifest-sha",
            last_pull_remote_generated_at="2026-05-12T10:00:00+00:00",
            last_pull_snapshot_format="sharded-v3",
        )
        after = state.load_sync_runtime_state()
        assert after["lastAction"] == "pull"
        assert after["lastResult"] == "ok"
        assert after["lastError"] == ""
        assert after["lastPullAt"]
        assert after["lastPullRemoteSha"] == "manifest-sha"
        assert after["lastPullRemoteGeneratedAt"] == "2026-05-12T10:00:00+00:00"
        assert after["lastPullSnapshotFormat"] == "sharded-v3"
        assert after["counters"]["date"]

        assert str(SYNC_STATUS.get("lastAction") or "") == "pull"

        runtime_path = data_dir / "source-sync-runtime.json"
        payload = json.loads(runtime_path.read_text(encoding="utf-8"))
        assert str(payload.get("lastAction") or "") == "pull"
        assert str(payload.get("lastResult") or "") == "ok"
        assert int((payload.get("counters") or {}).get("totalPulls") or 0) == 0


def test_sync_state_resets_counters_on_date_boundary() -> None:
    with workspace_tmpdir("sync-state-counters") as data_dir:
        state = SyncState(data_dir=data_dir)
        runtime_path = data_dir / "source-sync-runtime.json"
        runtime_path.write_text(
            json.dumps(
                {
                    "counters": {
                        "date": "2026-05-03",
                        "totalPushes": 7,
                        "totalPulls": 2,
                        "noOpSkips": 1,
                        "conflictsDetected": 1,
                        "conflictsResolved": 1,
                        "tombstonesSuppressed": 3,
                        "sourcesAdded": 4,
                        "sourcesRemoved": 5,
                    }
                }
            ),
            encoding="utf-8",
        )

        payload = state.load_sync_runtime_state()
        counters = payload["counters"]
        assert counters["date"] != "2026-05-03"
        assert counters["totalPushes"] == 0
        assert counters["totalPulls"] == 0
        assert counters["noOpSkips"] == 0
