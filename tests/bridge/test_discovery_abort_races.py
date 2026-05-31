from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from src.bridge.discovery_service import DiscoveryDeps, DiscoveryPaths, DiscoveryService


def _parse_iso_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _load_json_object(path: Path, default: object) -> object:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def _save_json_atomic(path: Path, payload: object) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(payload, indent=2), encoding="utf-8")


def test_discovery_watch_abort_intent_repairs_finished_report_and_skips_sync(
    tmp_path: Path,
) -> None:
    report_path = tmp_path / "source-discovery-report.json"
    sync_calls: list[str] = []
    _save_json_atomic(
        report_path,
        {
            "runId": "discovery_1",
            "startedAt": "2026-05-06T18:00:00Z",
            "finishedAt": "2026-05-06T18:05:00Z",
            "status": "ok",
            "summary": {"status": "ok", "queuedCandidateCount": 2},
            "taskProgress": {"active": True, "phaseKey": "done"},
        },
    )
    service = DiscoveryService(
        paths=DiscoveryPaths(
            report=report_path,
            candidates=tmp_path / "source-discovery-candidates.json",
            pending=tmp_path / "source-registry-pending.json",
            log=tmp_path / "source-discovery.log",
            settings=tmp_path / "source-discovery-config.json",
            approval_state=tmp_path / "source-approval-state.json",
        ),
        deps=DiscoveryDeps(
            schema_version=1,
            now_iso=lambda: "2026-05-06T18:10:00Z",
            now_utc=lambda: None,
            parse_iso=_parse_iso_utc,
            pid_is_running=lambda _pid: False,
            bridge_log=lambda *_args, **_kwargs: None,
            load_json_object=_load_json_object,
            save_json_atomic=_save_json_atomic,
            run_background_script=lambda *_args, **_kwargs: 123,
            append_run_history=lambda payload: payload,
            upsert_run_history=lambda payload, **_kwargs: payload,
            prune_started_rows_for_type=lambda *_args, **_kwargs: None,
            clear_task_state=lambda _task_type: None,
            normalize_discovery_report_contract=lambda payload: payload,
            load_state=lambda: {"active": [], "pending": [], "rejected": []},
            persist_state_and_auto_sync=lambda state, **_kwargs: (
                sync_calls.append("persist") or state
            ),
            load_sync_runtime_state=lambda: {},
            maybe_trigger_auto_sync_push=lambda reason: sync_calls.append(reason) or True,
            mark_discovery_sync_finished=lambda finished_at: sync_calls.append(finished_at),
            get_lifecycle_row=lambda _run_id, _task_type: {
                "runId": "discovery_1",
                "taskType": "discovery",
                "status": "running",
                "summary": {
                    "abortRequestedAt": "2026-05-06T18:04:00Z",
                    "abortReason": "test_abort",
                },
            },
        ),
    )

    service.watch_discovery_run_for_auto_sync(
        "discovery_1",
        pid=123,
        started_at="2026-05-06T18:00:00Z",
    )

    report = _load_json_object(report_path, {})
    assert isinstance(report, dict)
    assert report["status"] == "canceled"
    assert report["summary"]["abortReason"] == "test_abort"
    assert report["taskProgress"]["active"] is False
    assert sync_calls == []
