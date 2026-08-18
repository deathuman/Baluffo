import threading
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from src.bridge.run_history_api import SyncHistoryDeps, _safe_parse_iso, project_run_history


def test_safe_parse_iso_returns_none_for_expected_parse_failures() -> None:
    def parse_iso(_value: object):
        raise ValueError("invalid iso timestamp")

    assert _safe_parse_iso(parse_iso, "not-a-date") is None


def test_safe_parse_iso_does_not_swallow_unexpected_failures() -> None:
    def parse_iso(_value: object):
        raise AssertionError("unexpected parser bug")

    with pytest.raises(AssertionError, match="unexpected parser bug"):
        _safe_parse_iso(parse_iso, "2026-06-18T00:00:00+00:00")


def _minimal_sync_history_deps(**overrides: Any) -> SyncHistoryDeps:
    values: dict[str, Any] = {
        "ops_state_lock": threading.RLock(),
        "load_run_history": lambda: [],
        "save_run_history": lambda _rows: None,
        "save_json_atomic": lambda _path, _payload: None,
        "prune_started_rows_for_type": lambda *_args, **_kwargs: None,
        "clear_task_state": lambda _task_type: None,
        "clear_task_state_locked": lambda _task_type: None,
        "upsert_run_history": lambda entry, **_kwargs: entry,
        "task_running_from_state": lambda _task_type: False,
        "report_is_stale_in_progress": lambda *_args, **_kwargs: False,
        "load_json_object": lambda _path, default: dict(default),
        "normalize_fetch_report_contract": lambda payload: payload,
        "normalize_discovery_report_contract": lambda payload: payload,
        "summarize_fetch_report": lambda _report: {},
        "summarize_discovery_report": lambda _report: ({}, "ok"),
        "jobs_fetch_report_path": Path("fetch-report"),
        "jobs_fetch_tasks_path": Path("fetch-tasks"),
        "discovery_report_path": Path("discovery-report"),
        "task_state_path": Path("task-state"),
        "get_active_sync_runs": lambda: set(),
        "parse_iso": lambda _value: None,
        "now_iso": lambda: "2026-06-18T00:00:00+00:00",
        "now_utc": lambda: datetime(2026, 6, 18, tzinfo=UTC),
    }
    values.update(overrides)
    return SyncHistoryDeps(**values)


def test_project_run_history_uses_empty_pipeline_status_for_expected_failures() -> None:
    projection = project_run_history(
        _minimal_sync_history_deps(
            get_jobs_pipeline_status_payload=lambda: (_ for _ in ()).throw(
                OSError("pipeline status unavailable")
            )
        )
    )

    assert projection.rows == []
    assert projection.child_tasks["fetch"].active is False
    assert projection.child_tasks["discovery"].active is False


def test_project_run_history_does_not_swallow_unexpected_pipeline_status_failure() -> None:
    with pytest.raises(AssertionError, match="unexpected pipeline status bug"):
        project_run_history(
            _minimal_sync_history_deps(
                get_jobs_pipeline_status_payload=lambda: (_ for _ in ()).throw(
                    AssertionError("unexpected pipeline status bug")
                )
            )
        )
