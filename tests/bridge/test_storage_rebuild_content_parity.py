from __future__ import annotations

import time
from typing import Any, cast

from src.bridge.storage_health import close_storage_stores, get_storage_store
from src.bridge.storage_rebuild import (
    close_storage_rebuild_jobs,
    get_storage_rebuild_payload,
    start_storage_rebuild,
)
from src.storage import JobRuntimeStore, current_feed_rebuild
from src.storage.current_feed_rebuild import read_rebuild_journal, rebuild_journal_path
from tests.helpers.temp_paths import workspace_tmpdir


def _row(index: int) -> dict[str, object]:
    return {
        "id": index,
        "title": f"Job {index}",
        "company": "Studio",
        "city": "Amsterdam",
        "country": "Netherlands",
        "workType": "Hybrid",
        "contractType": "Full-time",
        "jobLink": f"https://example.test/jobs/{index}",
        "source": "Example",
        "sourceJobId": f"job-{index}",
        "status": "active",
        "firstSeenAt": "2026-05-12T10:00:00Z",
        "lastSeenAt": "2026-05-12T10:00:00Z",
        "sourceBundle": [
            {"sourceName": "Example", "sourceId": "example", "sourceJobId": f"job-{index}"}
        ],
    }


def _wait_for_terminal(data_dir, *, timeout_s: float = 30.0) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        rebuild = cast(dict[str, Any], get_storage_rebuild_payload(data_dir)["storageRebuild"])
        if rebuild["status"] not in {"running", "cancelling"}:
            return rebuild
        time.sleep(0.05)
    raise AssertionError("current-feed rebuild did not reach a terminal status")


def _seed(store) -> None:
    runtime = JobRuntimeStore(store, now_iso=lambda: "2026-05-12T10:00:00Z")
    runtime.replace_feed(run_id="run_old", rows=[_row(1), _row(2)])
    runtime.replace_feed(run_id="run_new", rows=[_row(10)])
    store.write(
        lambda conn: conn.execute(
            "INSERT INTO sources("
            " id, canonical_key, status, name, adapter, url, payload_json,"
            " created_at, updated_at"
            ") VALUES ("
            " 'src-1', 'example.test/jobs', 'active', 'Example', 'static',"
            " 'https://example.test/jobs', '{}', '2026-01-01T00:00:00Z',"
            " '2026-01-01T00:00:00Z'"
            ")"
        )
    )


def test_a_write_to_the_registry_during_the_copy_settles_superseded(monkeypatch) -> None:
    real_copy_table = current_feed_rebuild._copy_table

    with workspace_tmpdir("bridge-storage-rebuild-copy-race") as data_dir:
        try:
            store = get_storage_store(data_dir)
            _seed(store)

            def copying_after_a_write(*args: Any, **kwargs: Any) -> int:
                if args[2] == "sources":
                    store.write(
                        lambda conn: conn.execute(
                            "UPDATE sources SET name = 'Approved mid-copy' WHERE id = 'src-1'"
                        )
                    )
                return real_copy_table(*args, **kwargs)

            monkeypatch.setattr(current_feed_rebuild, "_copy_table", copying_after_a_write)

            started = start_storage_rebuild(data_dir)
            assert started["started"] is True
            rebuild = _wait_for_terminal(data_dir)
            journal = read_rebuild_journal(rebuild_journal_path(store.db_path))

            # A write to the data of record is the install gate's "superseded",
            # not a rebuild defect, so the next idle window can retry.
            assert rebuild["status"] == "superseded", rebuild
            assert "run the rebuild again" in rebuild["error"]
            assert "non-feed-mismatch" not in rebuild["error"]
            assert journal["status"] == "superseded"
            assert rebuild["backupExists"] is False
            assert store.execute_scalar("SELECT COUNT(*) FROM jobs") == 3
        finally:
            close_storage_rebuild_jobs()
            close_storage_stores()
