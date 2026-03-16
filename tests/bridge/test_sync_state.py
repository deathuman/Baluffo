from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory

from src.bridge.sync_state import SYNC_STATUS, SyncState


def test_sync_state_persists_runtime_status_fields() -> None:
    with TemporaryDirectory() as tmp:
        data_dir = Path(tmp)
        state = SyncState(data_dir=data_dir)

        before = state.load_sync_runtime_state()
        assert before["lastPullAt"] == ""
        assert before["lastPushAt"] == ""
        assert before["lastDiscoverySyncFinishedAt"] == ""

        state.set_sync_status(action="pull", result="ok", pulled=True, error="")
        after = state.load_sync_runtime_state()
        assert after["lastAction"] == "pull"
        assert after["lastResult"] == "ok"
        assert after["lastError"] == ""
        assert after["lastPullAt"]

        # Also updates module-level status for backward compatibility.
        assert str(SYNC_STATUS.get("lastAction") or "") == "pull"

        runtime_path = data_dir / "source-sync-runtime.json"
        payload = json.loads(runtime_path.read_text(encoding="utf-8"))
        assert str(payload.get("lastAction") or "") == "pull"
        assert str(payload.get("lastResult") or "") == "ok"

