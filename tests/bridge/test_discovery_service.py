from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from src.bridge.discovery_service import DiscoveryDeps, DiscoveryPaths, DiscoveryService


def _parse_iso_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def test_trigger_discovery_task_uncapped_uses_explicit_uncapped_args(tmp_path: Path) -> None:
    calls: list[tuple[str, list[str] | None, dict[str, str] | None]] = []

    def run_background_script(
        script_name: str,
        args: list[str] | None = None,
        *,
        extra_env: dict[str, str] | None = None,
    ) -> int:
        calls.append((script_name, list(args or []), dict(extra_env or {})))
        return 123

    service = DiscoveryService(
        paths=DiscoveryPaths(
            report=tmp_path / "source-discovery-report.json",
            candidates=tmp_path / "source-registry-pending.json",
            pending=tmp_path / "source-registry-pending.json",
            log=tmp_path / "source-discovery.log",
            settings=tmp_path / "source-discovery-config.json",
            approval_state=tmp_path / "source-approval-state.json",
        ),
        deps=DiscoveryDeps(
            schema_version=1,
            now_iso=lambda: "2026-03-20T12:00:00Z",
            now_utc=lambda: None,
            parse_iso=lambda value: None,
            pid_is_running=lambda pid: False,
            bridge_log=lambda *args, **kwargs: None,
            load_json_object=lambda path, default: default,
            save_json_atomic=lambda path, payload: Path(path).write_text("{}", encoding="utf-8"),
            run_background_script=run_background_script,
            append_run_history=lambda payload: payload,
            upsert_run_history=lambda payload, **_kwargs: payload,
            prune_started_rows_for_type=lambda *_args, **_kwargs: None,
            clear_task_state=lambda _task_type: None,
            normalize_discovery_report_contract=lambda payload: payload,
            load_state=lambda: {"active": [], "pending": [], "rejected": []},
            persist_state_and_auto_sync=lambda state, **_kwargs: state,
            load_sync_runtime_state=lambda: {},
            maybe_trigger_auto_sync_push=lambda reason: False,
            mark_discovery_sync_finished=lambda finished_at: None,
        ),
    )

    status_code, result = service.trigger_discovery_task(
        route_name="/tasks/run-discovery",
        payload={"preset": "uncapped"},
        enable_auto_sync_watch=False,
    )

    assert status_code == 200
    assert result["started"] is True
    assert result["preset"] == "uncapped"
    assert result["args"] == ["--mode", "dynamic", "--top", "0", "--preset", "uncapped"]
    assert calls == [
        (
            "source_discovery.py",
            ["--mode", "dynamic", "--top", "0", "--preset", "uncapped"],
            {
                "BALUFFO_DISCOVERY_RUN_ID": str(result.get("runId") or ""),
                "BALUFFO_DISCOVERY_STARTED_AT": "2026-03-20T12:00:00Z",
            },
        )
    ]


def test_trigger_discovery_task_logs_launch_start_and_persists_shell(tmp_path: Path) -> None:
    calls: list[tuple[str, dict]] = []

    def bridge_log(level: str, message: str, **fields: object) -> None:
        calls.append((message, {"level": level, **fields}))

    def save_json_atomic(path: Path, payload: object) -> None:
        Path(path).write_text(json.dumps(payload), encoding="utf-8")

    service = DiscoveryService(
        paths=DiscoveryPaths(
            report=tmp_path / "source-discovery-report.json",
            candidates=tmp_path / "source-registry-pending.json",
            pending=tmp_path / "source-registry-pending.json",
            log=tmp_path / "source-discovery.log",
            settings=tmp_path / "source-discovery-config.json",
            approval_state=tmp_path / "source-approval-state.json",
        ),
        deps=DiscoveryDeps(
            schema_version=1,
            now_iso=lambda: "2026-03-20T12:00:00Z",
            now_utc=lambda: None,
            parse_iso=lambda value: None,
            pid_is_running=lambda pid: False,
            bridge_log=bridge_log,
            load_json_object=lambda path, default: default,
            save_json_atomic=save_json_atomic,
            run_background_script=lambda script_name, args=None, **kwargs: 456,
            append_run_history=lambda payload: payload,
            upsert_run_history=lambda payload, **_kwargs: payload,
            prune_started_rows_for_type=lambda *_args, **_kwargs: None,
            clear_task_state=lambda _task_type: None,
            normalize_discovery_report_contract=lambda payload: payload,
            load_state=lambda: {"active": [], "pending": [], "rejected": []},
            persist_state_and_auto_sync=lambda state, **_kwargs: state,
            load_sync_runtime_state=lambda: {},
            maybe_trigger_auto_sync_push=lambda reason: False,
            mark_discovery_sync_finished=lambda finished_at: None,
        ),
    )

    status_code, result = service.trigger_discovery_task(
        route_name="/tasks/run-discovery",
        payload={"preset": "default"},
        enable_auto_sync_watch=False,
    )

    report = json.loads((tmp_path / "source-discovery-report.json").read_text(encoding="utf-8"))
    assert status_code == 200
    assert result["started"] is True
    assert report["runId"] == str(result.get("runId") or "")
    assert report["startedAt"] == "2026-03-20T12:00:00Z"
    assert report["finishedAt"] == ""
    assert report["runtime"]["lifecycle"]["owner"] == "discovery_report"
    assert any(message == "discovery_launch_started" for message, _fields in calls)


def test_trigger_discovery_task_returns_conflict_for_active_discovery(tmp_path: Path) -> None:
    task_state_path = tmp_path / "admin-task-state.json"
    task_state_path.write_text(
        json.dumps(
            {
                "discovery": {
                    "runId": "discovery_live_1",
                    "taskType": "discovery",
                    "pid": 321,
                    "startedAt": "2026-03-20T12:00:00Z",
                    "status": "running",
                }
            }
        ),
        encoding="utf-8",
    )
    bridge_events: list[tuple[str, dict[str, object]]] = []
    spawn_calls: list[tuple[str, list[str] | None]] = []
    history_rows: list[dict[str, object]] = []

    def bridge_log(level: str, message: str, **fields: object) -> None:
        bridge_events.append((message, {"level": level, **fields}))

    def run_background_script(
        script_name: str, args: list[str] | None = None, **_kwargs: object
    ) -> int:
        spawn_calls.append((script_name, args))
        return 123

    service = DiscoveryService(
        paths=DiscoveryPaths(
            report=tmp_path / "source-discovery-report.json",
            candidates=tmp_path / "source-registry-pending.json",
            pending=tmp_path / "source-registry-pending.json",
            log=tmp_path / "source-discovery.log",
            settings=tmp_path / "source-discovery-config.json",
            approval_state=tmp_path / "source-approval-state.json",
            task_state=task_state_path,
        ),
        deps=DiscoveryDeps(
            schema_version=1,
            now_iso=lambda: "2026-03-20T12:05:00Z",
            now_utc=lambda: None,
            parse_iso=lambda value: None,
            pid_is_running=lambda pid: int(pid) == 321,
            bridge_log=bridge_log,
            load_json_object=lambda path, default: (
                json.loads(Path(path).read_text(encoding="utf-8"))
                if Path(path).exists()
                else default
            ),
            save_json_atomic=lambda path, payload: Path(path).write_text(
                json.dumps(payload), encoding="utf-8"
            ),
            run_background_script=run_background_script,
            append_run_history=lambda payload: history_rows.append(payload) or payload,
            upsert_run_history=lambda payload, **_kwargs: payload,
            prune_started_rows_for_type=lambda *_args, **_kwargs: None,
            clear_task_state=lambda _task_type: None,
            normalize_discovery_report_contract=lambda payload: payload,
            load_state=lambda: {"active": [], "pending": [], "rejected": []},
            persist_state_and_auto_sync=lambda state, **_kwargs: state,
            load_sync_runtime_state=lambda: {},
            maybe_trigger_auto_sync_push=lambda reason: False,
            mark_discovery_sync_finished=lambda finished_at: None,
        ),
    )

    status_code, result = service.trigger_discovery_task(
        route_name="/tasks/run-discovery",
        payload={"preset": "default"},
        enable_auto_sync_watch=False,
    )

    assert status_code == 409
    assert result == {
        "started": False,
        "alreadyRunning": True,
        "task": "source_discovery",
        "taskType": "discovery",
        "runId": "discovery_live_1",
        "startedAt": "2026-03-20T12:00:00Z",
        "pid": 321,
        "status": "running",
    }
    assert not spawn_calls
    assert not history_rows
    assert not (tmp_path / "source-discovery-report.json").exists()
    assert any(message == "task_start_attached_existing" for message, _fields in bridge_events)


def test_discovery_settings_default_to_auto_approve_enabled(tmp_path: Path) -> None:
    service = DiscoveryService(
        paths=DiscoveryPaths(
            report=tmp_path / "source-discovery-report.json",
            candidates=tmp_path / "source-registry-pending.json",
            pending=tmp_path / "source-registry-pending.json",
            log=tmp_path / "source-discovery.log",
            settings=tmp_path / "source-discovery-config.json",
            approval_state=tmp_path / "source-approval-state.json",
        ),
        deps=DiscoveryDeps(
            schema_version=1,
            now_iso=lambda: "2026-03-20T12:00:00Z",
            now_utc=lambda: None,
            parse_iso=lambda value: None,
            pid_is_running=lambda pid: False,
            bridge_log=lambda *args, **kwargs: None,
            load_json_object=lambda path, default: default,
            save_json_atomic=lambda path, payload: None,
            run_background_script=lambda script_name, args=None, **kwargs: 1,
            append_run_history=lambda payload: payload,
            upsert_run_history=lambda payload, **_kwargs: payload,
            prune_started_rows_for_type=lambda *_args, **_kwargs: None,
            clear_task_state=lambda _task_type: None,
            normalize_discovery_report_contract=lambda payload: payload,
            load_state=lambda: {"active": [], "pending": [], "rejected": []},
            persist_state_and_auto_sync=lambda state, **_kwargs: state,
            load_sync_runtime_state=lambda: {},
            maybe_trigger_auto_sync_push=lambda reason: False,
            mark_discovery_sync_finished=lambda finished_at: None,
        ),
    )

    payload = service.get_discovery_config_payload()
    assert payload["ok"] is True
    assert payload["savedConfig"]["autoApproveHealthyPendingOnComplete"] is True


def test_update_discovery_settings_persists_normalized_bool(tmp_path: Path) -> None:
    settings_path = tmp_path / "source-discovery-config.json"

    def load_json_object(path: Path, default: dict[str, object]) -> dict[str, object]:
        if path == settings_path and path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
        return dict(default)

    def save_json_atomic(path: Path, payload: object) -> None:
        Path(path).write_text(json.dumps(payload), encoding="utf-8")

    service = DiscoveryService(
        paths=DiscoveryPaths(
            report=tmp_path / "source-discovery-report.json",
            candidates=tmp_path / "source-registry-pending.json",
            pending=tmp_path / "source-registry-pending.json",
            log=tmp_path / "source-discovery.log",
            settings=settings_path,
            approval_state=tmp_path / "source-approval-state.json",
        ),
        deps=DiscoveryDeps(
            schema_version=1,
            now_iso=lambda: "2026-03-20T12:00:00Z",
            now_utc=lambda: None,
            parse_iso=lambda value: None,
            pid_is_running=lambda pid: False,
            bridge_log=lambda *args, **kwargs: None,
            load_json_object=load_json_object,
            save_json_atomic=save_json_atomic,
            run_background_script=lambda script_name, args=None, **kwargs: 1,
            append_run_history=lambda payload: payload,
            upsert_run_history=lambda payload, **_kwargs: payload,
            prune_started_rows_for_type=lambda *_args, **_kwargs: None,
            clear_task_state=lambda _task_type: None,
            normalize_discovery_report_contract=lambda payload: payload,
            load_state=lambda: {"active": [], "pending": [], "rejected": []},
            persist_state_and_auto_sync=lambda state, **_kwargs: state,
            load_sync_runtime_state=lambda: {},
            maybe_trigger_auto_sync_push=lambda reason: False,
            mark_discovery_sync_finished=lambda finished_at: None,
        ),
    )

    saved = service.update_saved_discovery_settings({"autoApproveHealthyPendingOnComplete": 0})

    assert saved == {"autoApproveHealthyPendingOnComplete": False}
    persisted = json.loads(settings_path.read_text(encoding="utf-8"))
    assert persisted == {"autoApproveHealthyPendingOnComplete": False}


def test_watch_discovery_run_auto_approves_healthy_pending_before_sync(tmp_path: Path) -> None:
    report_path = tmp_path / "source-discovery-report.json"
    settings_path = tmp_path / "source-discovery-config.json"
    approval_state_path = tmp_path / "source-approval-state.json"
    report_path.write_text(
        json.dumps(
            {
                "startedAt": "2026-03-20T12:00:00Z",
                "finishedAt": "2026-03-20T12:05:00Z",
                "summary": {"queuedCandidateCount": 3},
                "runtime": {},
                "candidates": [
                    {
                        "id": "pending-ok",
                        "adapter": "greenhouse",
                        "name": "Healthy Pending",
                        "deferred": False,
                        "jobsFound": 3,
                        "sampleCount": 3,
                        "weakSignal": False,
                        "evidenceScore": 24,
                        "confidence": "medium",
                        "rankScore": 24,
                        "rankReasons": ["structured_batch_family", "jobs_found_bonus"],
                        "promotionLane": "structured_batch",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    settings_path.write_text(
        json.dumps({"autoApproveHealthyPendingOnComplete": True}), encoding="utf-8"
    )

    persisted_states: list[dict[str, object]] = []
    bridge_events: list[str] = []
    marked: list[str] = []
    cleared_tasks: list[str] = []
    pruned_runs: list[dict[str, object]] = []
    upserted_runs: list[dict[str, object]] = []
    sync_calls: list[str] = []
    state = {
        "active": [{"id": "active-1", "adapter": "static", "name": "Already Active"}],
        "pending": [
            {
                "id": "pending-ok",
                "adapter": "greenhouse",
                "name": "Healthy Pending",
                "jobsFound": 3,
                "sampleCount": 3,
                "weakSignal": False,
                "evidenceScore": 24,
                "confidence": "medium",
                "rankScore": 24,
                "rankReasons": ["structured_batch_family", "jobs_found_bonus"],
                "promotionLane": "structured_batch",
            },
            {
                "id": "pending-zero",
                "adapter": "static",
                "name": "Zero Pending",
                "jobsFound": 0,
                "status": "healthy",
            },
            {
                "id": "pending-error",
                "adapter": "greenhouse",
                "name": "Errored Pending",
                "jobsFound": 2,
                "sampleCount": 2,
                "evidenceScore": 24,
                "confidence": "medium",
                "rankScore": 24,
                "rankReasons": ["structured_batch_family", "jobs_found_bonus"],
                "promotionLane": "structured_batch",
                "status": "error",
                "lastProbeError": "timeout",
            },
        ],
        "rejected": [],
    }

    def load_json_object(path: Path, default: dict[str, object]) -> dict[str, object]:
        if path == report_path:
            return json.loads(report_path.read_text(encoding="utf-8"))
        if path == settings_path:
            return json.loads(settings_path.read_text(encoding="utf-8"))
        if path == approval_state_path:
            if path.exists():
                return json.loads(path.read_text(encoding="utf-8"))
            return dict(default)
        return dict(default)

    def save_json_atomic(path: Path, payload: object) -> None:
        Path(path).write_text(json.dumps(payload), encoding="utf-8")

    def persist_state_and_auto_sync(
        next_state: dict[str, list[dict[str, object]]], **_kwargs: object
    ) -> dict[str, list[dict[str, object]]]:
        state["active"] = list(next_state.get("active") or [])
        state["pending"] = list(next_state.get("pending") or [])
        state["rejected"] = list(next_state.get("rejected") or [])
        persisted_states.append(json.loads(json.dumps(state)))
        return state

    service = DiscoveryService(
        paths=DiscoveryPaths(
            report=report_path,
            candidates=tmp_path / "source-registry-pending.json",
            pending=tmp_path / "source-registry-pending.json",
            log=tmp_path / "source-discovery.log",
            settings=settings_path,
            approval_state=approval_state_path,
        ),
        deps=DiscoveryDeps(
            schema_version=1,
            now_iso=lambda: "2026-03-20T12:06:00Z",
            now_utc=lambda: None,
            parse_iso=_parse_iso_utc,
            pid_is_running=lambda pid: False,
            bridge_log=lambda _level, message, **_fields: bridge_events.append(message),
            load_json_object=load_json_object,
            save_json_atomic=save_json_atomic,
            run_background_script=lambda script_name, args=None, **kwargs: 1,
            append_run_history=lambda payload: payload,
            upsert_run_history=lambda payload, **_kwargs: (
                upserted_runs.append(dict(payload)) or payload
            ),
            prune_started_rows_for_type=lambda run_type, **kwargs: pruned_runs.append(
                {"runType": run_type, **kwargs}
            ),
            clear_task_state=lambda task_type: cleared_tasks.append(task_type),
            normalize_discovery_report_contract=lambda payload: payload,
            load_state=lambda: {
                "active": list(state["active"]),
                "pending": list(state["pending"]),
                "rejected": list(state["rejected"]),
            },
            persist_state_and_auto_sync=persist_state_and_auto_sync,
            load_sync_runtime_state=lambda: {},
            maybe_trigger_auto_sync_push=lambda reason: (sync_calls.append(reason), True)[1],
            mark_discovery_sync_finished=lambda finished_at: marked.append(finished_at),
        ),
    )

    service.watch_discovery_run_for_auto_sync("discovery_1", 123, "2026-03-20T12:00:00Z")

    assert len(persisted_states) == 1
    assert [row["id"] for row in state["active"]] == ["active-1", "pending-ok"]
    assert [row["id"] for row in state["pending"]] == ["pending-zero", "pending-error"]
    assert state["active"][1]["enabledByDefault"] is True
    assert state["active"][1]["candidateState"] == "live"
    assert state["active"][1]["approvedBy"] == "discovery_auto_approve"
    assert state["active"][1]["approvedAt"] == "2026-03-20T12:06:00Z"
    assert state["active"][1]["liveAt"] == "2026-03-20T12:06:00Z"
    assert state["active"][1]["weakSignal"] is False
    assert state["active"][1]["promotionReason"] == "structured_batch_family"
    approval_state = json.loads(approval_state_path.read_text(encoding="utf-8"))
    assert int(approval_state["approvedSinceLastRun"]) == 1
    saved_report = json.loads(report_path.read_text(encoding="utf-8"))
    assert (
        int(
            (((saved_report.get("runtime") or {}).get("autoApproval") or {}).get("approvedCount"))
            or 0
        )
        == 1
    )
    assert int((saved_report.get("summary") or {}).get("approvedCandidateCount") or 0) == 1
    assert int((saved_report.get("summary") or {}).get("liveCandidateCount") or 0) == 1
    assert (saved_report.get("candidates") or [])[0]["promotionReason"] == "structured_batch_family"
    assert sync_calls == ["discovery_completed"]
    assert marked == ["2026-03-20T12:05:00Z"]
    assert cleared_tasks == ["discovery"]
    assert pruned_runs == [{"runType": "discovery", "finished_at": "2026-03-20T12:05:00Z"}]
    assert upserted_runs == [
        {
            "id": "discovery_1",
            "runId": "discovery_1",
            "type": "discovery",
            "status": "ok",
            "startedAt": "2026-03-20T12:00:00Z",
            "finishedAt": "2026-03-20T12:05:00Z",
            "durationMs": 300000,
            "summary": {"queuedCandidateCount": 3},
        }
    ]
    assert "discovery_auto_approval_completed" in bridge_events


def test_watch_discovery_run_finalizes_when_report_is_terminal_even_if_pid_lingers(
    tmp_path: Path,
) -> None:
    report_path = tmp_path / "source-discovery-report.json"
    settings_path = tmp_path / "source-discovery-config.json"
    report_path.write_text(
        json.dumps(
            {
                "startedAt": "2026-03-20T12:00:00Z",
                "finishedAt": "2026-03-20T12:05:00Z",
                "summary": {"queuedCandidateCount": 0, "failedProbeCount": 0, "probeMissCount": 0},
                "runtime": {},
            }
        ),
        encoding="utf-8",
    )

    cleared_tasks: list[str] = []
    pruned_runs: list[dict[str, object]] = []
    upserted_runs: list[dict[str, object]] = []

    def load_json_object(path: Path, default: dict[str, object]) -> dict[str, object]:
        if path == report_path:
            return json.loads(report_path.read_text(encoding="utf-8"))
        if path == settings_path:
            return json.loads(settings_path.read_text(encoding="utf-8"))
        return dict(default)

    service = DiscoveryService(
        paths=DiscoveryPaths(
            report=report_path,
            candidates=tmp_path / "source-registry-pending.json",
            pending=tmp_path / "source-registry-pending.json",
            log=tmp_path / "source-discovery.log",
            settings=settings_path,
            approval_state=tmp_path / "source-approval-state.json",
        ),
        deps=DiscoveryDeps(
            schema_version=1,
            now_iso=lambda: "2026-03-20T12:06:00Z",
            now_utc=lambda: None,
            parse_iso=_parse_iso_utc,
            pid_is_running=lambda pid: True,
            bridge_log=lambda *args, **kwargs: None,
            load_json_object=load_json_object,
            save_json_atomic=lambda path, payload: Path(path).write_text(
                json.dumps(payload), encoding="utf-8"
            ),
            run_background_script=lambda script_name, args=None, **kwargs: 1,
            append_run_history=lambda payload: payload,
            upsert_run_history=lambda payload, **_kwargs: (
                upserted_runs.append(dict(payload)) or payload
            ),
            prune_started_rows_for_type=lambda run_type, **kwargs: pruned_runs.append(
                {"runType": run_type, **kwargs}
            ),
            clear_task_state=lambda task_type: cleared_tasks.append(task_type),
            normalize_discovery_report_contract=lambda payload: payload,
            load_state=lambda: {"active": [], "pending": [], "rejected": []},
            persist_state_and_auto_sync=lambda state, **_kwargs: state,
            load_sync_runtime_state=lambda: {},
            maybe_trigger_auto_sync_push=lambda reason: False,
            mark_discovery_sync_finished=lambda finished_at: None,
        ),
    )

    service.watch_discovery_run_for_auto_sync("discovery_1", 123, "2026-03-20T12:00:00Z")

    assert cleared_tasks == ["discovery"]
    assert pruned_runs == [{"runType": "discovery", "finished_at": "2026-03-20T12:05:00Z"}]
    assert upserted_runs == [
        {
            "id": "discovery_1",
            "runId": "discovery_1",
            "type": "discovery",
            "status": "ok",
            "startedAt": "2026-03-20T12:00:00Z",
            "finishedAt": "2026-03-20T12:05:00Z",
            "durationMs": 300000,
            "summary": {"queuedCandidateCount": 0, "failedProbeCount": 0, "probeMissCount": 0},
        }
    ]


def test_watch_discovery_run_respects_disabled_auto_approval(tmp_path: Path) -> None:
    report_path = tmp_path / "source-discovery-report.json"
    settings_path = tmp_path / "source-discovery-config.json"
    report_path.write_text(
        json.dumps(
            {
                "startedAt": "2026-03-20T12:00:00Z",
                "finishedAt": "2026-03-20T12:05:00Z",
                "summary": {"queuedCandidateCount": 0},
                "runtime": {},
            }
        ),
        encoding="utf-8",
    )
    settings_path.write_text(
        json.dumps({"autoApproveHealthyPendingOnComplete": False}),
        encoding="utf-8",
    )

    persisted_states: list[dict[str, object]] = []
    cleared_tasks: list[str] = []
    pruned_runs: list[dict[str, object]] = []
    upserted_runs: list[dict[str, object]] = []
    sync_calls: list[str] = []
    state = {
        "active": [],
        "pending": [
            {
                "id": "pending-ok",
                "adapter": "static",
                "name": "Healthy Pending",
                "jobsFound": 3,
                "status": "healthy",
            }
        ],
        "rejected": [],
    }

    def load_json_object(path: Path, default: dict[str, object]) -> dict[str, object]:
        if path == report_path:
            return json.loads(report_path.read_text(encoding="utf-8"))
        if path == settings_path:
            return json.loads(settings_path.read_text(encoding="utf-8"))
        return dict(default)

    service = DiscoveryService(
        paths=DiscoveryPaths(
            report=report_path,
            candidates=tmp_path / "source-registry-pending.json",
            pending=tmp_path / "source-registry-pending.json",
            log=tmp_path / "source-discovery.log",
            settings=settings_path,
            approval_state=tmp_path / "source-approval-state.json",
        ),
        deps=DiscoveryDeps(
            schema_version=1,
            now_iso=lambda: "2026-03-20T12:06:00Z",
            now_utc=lambda: None,
            parse_iso=_parse_iso_utc,
            pid_is_running=lambda pid: False,
            bridge_log=lambda *args, **kwargs: None,
            load_json_object=load_json_object,
            save_json_atomic=lambda path, payload: Path(path).write_text(
                json.dumps(payload), encoding="utf-8"
            ),
            run_background_script=lambda script_name, args=None, **kwargs: 1,
            append_run_history=lambda payload: payload,
            upsert_run_history=lambda payload, **_kwargs: (
                upserted_runs.append(dict(payload)) or payload
            ),
            prune_started_rows_for_type=lambda run_type, **kwargs: pruned_runs.append(
                {"runType": run_type, **kwargs}
            ),
            clear_task_state=lambda task_type: cleared_tasks.append(task_type),
            normalize_discovery_report_contract=lambda payload: payload,
            load_state=lambda: {
                "active": list(state["active"]),
                "pending": list(state["pending"]),
                "rejected": list(state["rejected"]),
            },
            persist_state_and_auto_sync=lambda next_state, **_kwargs: (
                persisted_states.append(json.loads(json.dumps(next_state))) or next_state
            ),
            load_sync_runtime_state=lambda: {},
            maybe_trigger_auto_sync_push=lambda reason: sync_calls.append(reason) or False,
            mark_discovery_sync_finished=lambda finished_at: None,
        ),
    )

    service.watch_discovery_run_for_auto_sync("discovery_1", 123, "2026-03-20T12:00:00Z")

    assert persisted_states == []
    assert state["pending"][0]["id"] == "pending-ok"
    saved_report = json.loads(report_path.read_text(encoding="utf-8"))
    assert ((saved_report.get("runtime") or {}).get("autoApproval") or {}).get("enabled") is False
    assert (
        int(
            (((saved_report.get("runtime") or {}).get("autoApproval") or {}).get("approvedCount"))
            or 0
        )
        == 0
    )
    assert cleared_tasks == ["discovery"]
    assert pruned_runs == [{"runType": "discovery", "finished_at": "2026-03-20T12:05:00Z"}]
    assert upserted_runs == [
        {
            "id": "discovery_1",
            "runId": "discovery_1",
            "type": "discovery",
            "status": "ok",
            "startedAt": "2026-03-20T12:00:00Z",
            "finishedAt": "2026-03-20T12:05:00Z",
            "durationMs": 300000,
            "summary": {"queuedCandidateCount": 0},
        }
    ]
    assert sync_calls == []
