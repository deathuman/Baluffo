from __future__ import annotations

from src.storage import BaluffoStore
from tests.helpers.temp_paths import workspace_tmpdir


def test_compaction_records_a_run_id() -> None:
    with workspace_tmpdir("compaction-run-id") as data_dir:
        with BaluffoStore(data_dir) as store:
            assert store.compaction_status().get("runId") == ""
            store.vacuum(reason="test")
            run_id = store.compaction_status()["runId"]

    assert run_id.startswith("compaction_")
