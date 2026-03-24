from __future__ import annotations

import json
from pathlib import Path

from src.bridge.discovery_service import DiscoveryDeps, DiscoveryPaths, DiscoveryService


def test_trigger_discovery_task_uncapped_uses_explicit_uncapped_args(tmp_path: Path) -> None:
    calls: list[tuple[str, list[str] | None]] = []

    def run_background_script(script_name: str, args: list[str] | None = None) -> int:
        calls.append((script_name, list(args or [])))
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
        ("source_discovery.py", ["--mode", "dynamic", "--top", "0", "--preset", "uncapped"])
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
            run_background_script=lambda script_name, args=None: 456,
            append_run_history=lambda payload: payload,
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
    assert report["startedAt"] == "2026-03-20T12:00:00Z"
    assert report["finishedAt"] == ""
    assert any(message == "discovery_launch_started" for message, _fields in calls)


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
            run_background_script=lambda script_name, args=None: 1,
            append_run_history=lambda payload: payload,
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
            run_background_script=lambda script_name, args=None: 1,
            append_run_history=lambda payload: payload,
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
                "summary": {"queuedCandidateCount": 1},
                "runtime": {},
                "candidates": [
                    {
                        "id": "pending-ok",
                        "adapter": "static",
                        "name": "Healthy Pending",
                        "deferred": False,
                        "jobsFound": 0,
                        "sampleCount": 0,
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
    sync_calls: list[str] = []
    state = {
        "active": [{"id": "active-1", "adapter": "static", "name": "Already Active"}],
        "pending": [
            {
                "id": "pending-ok",
                "adapter": "static",
                "name": "Healthy Pending",
                "jobsFound": 0,
                "sampleCount": 0,
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
                "adapter": "static",
                "name": "Errored Pending",
                "sampleCount": 2,
                "status": "error",
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
            parse_iso=lambda value: value,
            pid_is_running=lambda pid: False,
            bridge_log=lambda _level, message, **_fields: bridge_events.append(message),
            load_json_object=load_json_object,
            save_json_atomic=save_json_atomic,
            run_background_script=lambda script_name, args=None: 1,
            append_run_history=lambda payload: payload,
            normalize_discovery_report_contract=lambda payload: payload,
            load_state=lambda: {
                "active": list(state["active"]),
                "pending": list(state["pending"]),
                "rejected": list(state["rejected"]),
            },
            persist_state_and_auto_sync=persist_state_and_auto_sync,
            load_sync_runtime_state=lambda: {},
            maybe_trigger_auto_sync_push=lambda reason: sync_calls.append(reason) or True,
            mark_discovery_sync_finished=lambda finished_at: marked.append(finished_at),
        ),
    )

    service.watch_discovery_run_for_auto_sync("discovery_1", 123, "2026-03-20T12:00:00Z")

    assert len(persisted_states) == 1
    assert [row["id"] for row in state["active"]] == ["active-1", "pending-ok"]
    assert [row["id"] for row in state["pending"]] == ["pending-zero", "pending-error"]
    assert state["active"][1]["enabledByDefault"] is True
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
    assert sync_calls == ["discovery_completed"]
    assert marked == ["2026-03-20T12:05:00Z"]
    assert "discovery_auto_approval_completed" in bridge_events


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
            parse_iso=lambda value: value,
            pid_is_running=lambda pid: False,
            bridge_log=lambda *args, **kwargs: None,
            load_json_object=load_json_object,
            save_json_atomic=lambda path, payload: Path(path).write_text(
                json.dumps(payload), encoding="utf-8"
            ),
            run_background_script=lambda script_name, args=None: 1,
            append_run_history=lambda payload: payload,
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
    assert sync_calls == []
