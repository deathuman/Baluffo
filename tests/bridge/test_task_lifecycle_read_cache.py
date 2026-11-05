"""The lifecycle ledger is parsed once per artifact version, not once per read.

The Ops health projection reads the ledger twice per request (current + recent)
and the JSON authority re-read and re-normalized the whole artifact each time.
These tests pin the artifact-signature cache that keeps a steady-state Ops poll
off the parser, and keep the write path seeding it so heartbeats do not either.
"""

from __future__ import annotations

from pathlib import Path

from src.bridge.task_lifecycle import TaskLifecycleService
from tests.helpers.json_files import load_json_object
from tests.helpers.task_lifecycle import (
    NOW_ISO,
    lifecycle_file,
    save_json_atomic,
    task_lifecycle_service,
)


def _ledger(*run_ids: str) -> dict[str, object]:
    return {
        "schemaVersion": 1,
        "updatedAt": NOW_ISO,
        "rows": [
            {
                "runId": run_id,
                "taskType": "fetch",
                "status": "succeeded",
                "startedAt": f"2026-05-06T18:00:0{index}+00:00",
                "finishedAt": f"2026-05-06T18:01:0{index}+00:00",
            }
            for index, run_id in enumerate(run_ids)
        ],
    }


def _write_ledger(path: Path, *run_ids: str) -> None:
    save_json_atomic(path, _ledger(*run_ids))


def _counting_service(path: Path, loads: list[str]) -> TaskLifecycleService:
    service = task_lifecycle_service(path)

    def counting(load_path: Path, default: dict[str, object]) -> dict[str, object]:
        loads.append(Path(load_path).name)
        return load_json_object(load_path, default)

    service._load_json_object = counting  # noqa: SLF001
    return service


def test_current_recent_and_rows_share_one_ledger_parse(tmp_path: Path) -> None:
    path = lifecycle_file(tmp_path)
    _write_ledger(path, "fetch_done_1")
    loads: list[str] = []
    service = _counting_service(path, loads)

    assert service.get_current_runs() == []
    assert [row["runId"] for row in service.get_recent_runs()] == ["fetch_done_1"]
    assert [row["runId"] for row in service.rows()] == ["fetch_done_1"]

    assert loads == [path.name]


def test_external_ledger_rewrite_is_read_once_again(tmp_path: Path) -> None:
    path = lifecycle_file(tmp_path)
    _write_ledger(path, "fetch_done_1")
    loads: list[str] = []
    service = _counting_service(path, loads)

    assert [row["runId"] for row in service.rows()] == ["fetch_done_1"]
    _write_ledger(path, "fetch_done_1", "fetch_done_2")

    assert [row["runId"] for row in service.rows()] == ["fetch_done_1", "fetch_done_2"]
    assert loads == [path.name, path.name]


def test_transition_writes_seed_the_read_cache_without_a_reparse(tmp_path: Path) -> None:
    path = lifecycle_file(tmp_path)
    loads: list[str] = []
    service = _counting_service(path, loads)

    service.start_run(run_id="fetch_live", task_type="fetch")
    assert loads == [path.name]

    service.heartbeat_run("fetch_live", "fetch", stage="executing_sources")
    current = service.get_current_runs()
    assert [row["runId"] for row in current] == ["fetch_live"]
    assert current[0]["stage"] == "executing_sources"

    service.finish_run("fetch_live", "fetch", terminal_reason="completed")
    recent = service.get_recent_runs()
    assert [row["runId"] for row in recent] == ["fetch_live"]
    assert recent[0]["lifecycleStatus"] == "succeeded"

    assert loads == [path.name]


def test_returned_rows_cannot_mutate_the_cached_ledger(tmp_path: Path) -> None:
    path = lifecycle_file(tmp_path)
    _write_ledger(path, "fetch_done_1")
    service = task_lifecycle_service(path)

    rows = service.rows()
    rows[0]["status"] = "tampered"

    assert service.rows()[0]["status"] == "succeeded"
