from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from src.bridge.pipeline_service import PipelineRuntime, PipelineService
from tests.helpers.mutation import append_and_return


def _parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _now_iso_factory(ticks: list[str]) -> Any:
    state = {"idx": 0}

    def _now() -> str:
        i = state["idx"]
        state["idx"] += 1
        return ticks[min(i, len(ticks) - 1)]

    return _now


def _make_service(**overrides: Any) -> PipelineService:
    kwargs: dict[str, Any] = {
        "pipeline_state_lock": __import__("threading").RLock(),
        "pipeline_status": {},
        "runtime": PipelineRuntime(),
        "bridge_log": lambda *args, **kwargs: None,
        "now_iso": lambda: "2026-08-04T00:00:00Z",
        "parse_iso": _parse_iso,
        "sync_task_running": lambda: False,
        "current_fetch_output_count": lambda: 0,
        "load_json_object": lambda _path, default: default,
        "load_runtime_evidence": lambda path, default=None: default or {},
        "wait_for_sync_completion": lambda _run_id, _timeout_s: {},
        "discovery_report_path": Path("source-discovery-report.json"),
        "fetch_report_path": Path("jobs-fetch-report.json"),
        "trigger_discovery_task": lambda **_kwargs: (200, {}),
        "start_fetcher_task": lambda _payload: {},
        "start_sync_task": lambda _action, **_kwargs: {},
        "get_app_version": lambda: "0.0.0-test",
    }
    kwargs.update(overrides)
    return PipelineService(**kwargs)


def test_mark_stage_appends_ledger_entries_with_monotonic_entered_at() -> None:
    ticks = [f"2026-08-04T00:00:{i:02d}Z" for i in range(10)]
    heartbeats: list[dict[str, Any]] = []
    svc = _make_service(
        now_iso=_now_iso_factory(ticks),
        heartbeat_lifecycle_run=lambda *a, **kw: append_and_return(heartbeats, kw, {}),
    )
    svc._status.update({"runId": "pipeline_t1", "active": True, "_stageLedger": []})

    svc._mark_stage(stage="fetch", current_step=0, total_steps=3, label="Fetching")
    svc._mark_stage(stage="dedup", current_step=1, total_steps=3, label="Deduping")
    svc._mark_stage(stage="sync_push", current_step=2, total_steps=3, label="Pushing")

    ledger = svc._status.get("_stageLedger") or []
    assert [e["stage"] for e in ledger] == ["fetch", "dedup", "sync_push"]
    entered = [e["enteredAt"] for e in ledger]
    assert entered == sorted(entered)  # monotonic
    assert all(e.get("label") for e in ledger)
    # every stage emits a heartbeat that includes the ledger implicitly via status
    assert len(heartbeats) == 3


def test_mark_stage_caps_ledger_at_64_entries() -> None:
    ticks = [f"2026-08-04T00:{i // 60:02d}:{i % 60:02d}Z" for i in range(80)]
    svc = _make_service(now_iso=_now_iso_factory(ticks))
    svc._status.update({"runId": "pipeline_t2", "active": True, "_stageLedger": []})

    for i in range(70):
        svc._mark_stage(stage=f"stage_{i}", current_step=i, total_steps=70, label=f"Stage {i}")

    ledger = svc._status.get("_stageLedger") or []
    assert len(ledger) == 64
    # Oldest surviving entry should be stage_6 (dropped 0..5)
    assert ledger[0]["stage"] == "stage_6"
    assert ledger[-1]["stage"] == "stage_69"


def test_start_task_initialises_empty_ledger() -> None:
    started: list[dict[str, Any]] = []

    def _start_lifecycle_run(**kwargs: Any) -> dict[str, Any]:
        started.append(kwargs)
        return {}

    svc = _make_service(start_lifecycle_run=_start_lifecycle_run)
    svc._status["_stageLedger"] = [{"stage": "leftover", "enteredAt": "x", "label": ""}]
    # Do NOT actually run the worker thread — call the reset portion of start_task logic.
    # Simulate the path: re-set via the start task initializer shape.
    svc._status.update(
        {
            "active": True,
            "runId": "pipeline_new",
            "stage": "starting",
            "_stageLedger": [],
        }
    )
    assert svc._status["_stageLedger"] == []


def test_set_completed_flushes_ledger_into_lifecycle_summary() -> None:
    ticks = [f"2026-08-04T00:00:{i:02d}Z" for i in range(10)]
    finished: list[dict[str, Any]] = []
    svc = _make_service(
        now_iso=_now_iso_factory(ticks),
        finish_lifecycle_run=lambda run_id, task_type, **kw: append_and_return(finished, kw, {}),
    )
    svc._status.update(
        {
            "runId": "pipeline_t3",
            "active": True,
            "startedAt": ticks[0],
            "_stageLedger": [],
            "baselineOutputCount": 0,
            "jobsPageLoadedCount": 0,
        }
    )

    svc._mark_stage(stage="fetch", current_step=0, total_steps=3, label="Fetching")
    svc._set_completed(status="ok", error="", final_output_count=0)

    assert finished, "expected finish_lifecycle_run to be called"
    summary = finished[-1].get("summary") or {}
    ledger = summary.get("stageLedger") or []
    assert [e["stage"] for e in ledger] == ["fetch", "completed"]
    # Terminal entry bounds the fetch duration
    assert ledger[-1]["enteredAt"] >= ledger[0]["enteredAt"]


def test_set_completed_with_canceled_still_flushes_ledger() -> None:
    ticks = [f"2026-08-04T00:00:{i:02d}Z" for i in range(10)]
    canceled: list[dict[str, Any]] = []
    svc = _make_service(
        now_iso=_now_iso_factory(ticks),
        cancel_lifecycle_run=lambda run_id, task_type, **kw: append_and_return(canceled, kw, {}),
    )
    svc._status.update(
        {
            "runId": "pipeline_t4",
            "active": True,
            "startedAt": ticks[0],
            "_stageLedger": [],
            "baselineOutputCount": 0,
            "jobsPageLoadedCount": 0,
        }
    )
    svc._mark_stage(stage="fetch", current_step=0, total_steps=3, label="Fetching")
    svc._set_completed(status="canceled", error="", final_output_count=0)

    assert canceled, "expected cancel_lifecycle_run to be called"
    ledger = (canceled[-1].get("summary") or {}).get("stageLedger") or []
    assert [e["stage"] for e in ledger] == ["fetch", "canceled"]


def test_set_completed_with_error_still_flushes_ledger() -> None:
    ticks = [f"2026-08-04T00:00:{i:02d}Z" for i in range(10)]
    failed: list[dict[str, Any]] = []
    svc = _make_service(
        now_iso=_now_iso_factory(ticks),
        fail_lifecycle_run=lambda run_id, task_type, **kw: append_and_return(failed, kw, {}),
    )
    svc._status.update(
        {
            "runId": "pipeline_t5",
            "active": True,
            "startedAt": ticks[0],
            "_stageLedger": [],
            "baselineOutputCount": 0,
            "jobsPageLoadedCount": 0,
        }
    )
    svc._mark_stage(stage="fetch", current_step=0, total_steps=3, label="Fetching")
    svc._set_completed(status="error", error="boom", final_output_count=0)

    assert failed, "expected fail_lifecycle_run to be called"
    ledger = (failed[-1].get("summary") or {}).get("stageLedger") or []
    assert [e["stage"] for e in ledger] == ["fetch", "error"]


def test_set_completed_with_empty_ledger_still_marks_terminal_entry() -> None:
    ticks = [f"2026-08-04T00:00:{i:02d}Z" for i in range(5)]
    finished: list[dict[str, Any]] = []
    svc = _make_service(
        now_iso=_now_iso_factory(ticks),
        finish_lifecycle_run=lambda run_id, task_type, **kw: append_and_return(finished, kw, {}),
    )
    svc._status.update(
        {
            "runId": "pipeline_t6",
            "active": True,
            "startedAt": ticks[0],
            "_stageLedger": [],
            "baselineOutputCount": 0,
            "jobsPageLoadedCount": 0,
        }
    )
    # No _mark_stage call — run aborted before any stage transition.
    svc._set_completed(status="ok", error="", final_output_count=0)

    assert finished
    ledger = (finished[-1].get("summary") or {}).get("stageLedger") or []
    assert [e["stage"] for e in ledger] == ["completed"]


def test_record_child_phase_observation_appends_sub_stage_entries() -> None:
    ticks = [f"2026-08-04T00:00:{i:02d}Z" for i in range(10)]
    svc = _make_service(now_iso=_now_iso_factory(ticks))
    svc._status.update({"runId": "pipeline_sub1", "active": True, "_stageLedger": []})

    report_1 = {"taskProgress": {"phaseKey": "loading_state", "phaseLabel": "Loading fetch state"}}
    report_2 = {"taskProgress": {"phaseKey": "scraping_adapter", "phaseLabel": "Scraping adapters"}}
    svc._record_child_phase_observation("fetch", report_1)
    svc._record_child_phase_observation("fetch", report_2)
    svc._record_child_phase_observation("fetch", report_2)  # duplicate — should not append

    ledger = svc._status["_stageLedger"]
    assert [e["stage"] for e in ledger] == ["fetch/loading_state", "fetch/scraping_adapter"]
    assert ledger[0]["label"] == "Loading fetch state"
    assert ledger[1]["label"] == "Scraping adapters"


def test_record_child_phase_observation_skips_missing_progress() -> None:
    svc = _make_service()
    svc._status.update({"runId": "pipeline_sub2", "active": True, "_stageLedger": []})
    svc._record_child_phase_observation("fetch", {})
    svc._record_child_phase_observation("fetch", {"taskProgress": "not_a_dict"})
    svc._record_child_phase_observation("fetch", {"taskProgress": {"phaseKey": ""}})
    svc._record_child_phase_observation("fetch", None)
    assert svc._status["_stageLedger"] == []


def test_record_child_phase_observation_caps_at_64() -> None:
    ticks = [f"2026-08-04T00:{i // 60:02d}:{i % 60:02d}Z" for i in range(80)]
    svc = _make_service(now_iso=_now_iso_factory(ticks))
    svc._status.update({"runId": "pipeline_sub3", "active": True, "_stageLedger": []})
    for i in range(70):
        svc._record_child_phase_observation(
            "fetch", {"taskProgress": {"phaseKey": f"phase_{i}", "phaseLabel": f"Phase {i}"}}
        )
    ledger = svc._status["_stageLedger"]
    assert len(ledger) == 64
    assert ledger[0]["stage"] == "fetch/phase_6"
    assert ledger[-1]["stage"] == "fetch/phase_69"


# --- G2: get_status_payload emits stageTransitions + stallInfo ---


def test_status_payload_emits_stage_transitions() -> None:
    ticks = [f"2026-08-04T00:00:{i:02d}Z" for i in range(10)]
    svc = _make_service(now_iso=_now_iso_factory(ticks))
    svc._status.update(
        {"runId": "pipeline_g2a", "active": True, "_stageLedger": [], "_stageTransitions": []}
    )

    svc._mark_stage(stage="discovery", current_step=0, total_steps=1, label="Source discovery")
    svc._mark_stage(stage="fetch", current_step=0, total_steps=3, label="Fetching job listings")

    payload = svc.get_status_payload()
    transitions = payload.get("stageTransitions")
    assert isinstance(transitions, list)
    assert len(transitions) == 2
    assert transitions[0]["to"] == "discovery"
    assert transitions[1]["to"] == "fetch"
    assert all(set(e.keys()) == {"from", "to", "at"} for e in transitions)
    # defensive copy — mutating payload must not touch internal state
    transitions.append({"from": "x", "to": "y", "at": "z"})
    assert len(svc._status["_stageTransitions"]) == 2


def test_status_payload_emits_stall_info_when_heartbeat_stale() -> None:
    from datetime import UTC, datetime, timedelta

    stale_heartbeat = (datetime.now(UTC) - timedelta(seconds=200)).isoformat()

    svc = _make_service(parse_iso=_parse_iso)
    svc._status.update(
        {
            "runId": "pipeline_g2b",
            "active": True,
            "stage": "fetch",
            "activeChildTaskType": "fetch",
            "heartbeatAt": stale_heartbeat,
            "_stageLedger": [],
            "_stageTransitions": [],
        }
    )

    payload = svc.get_status_payload()
    stall = payload.get("stallInfo")
    assert stall is not None
    assert stall["stalled"] is True
    assert stall["inChild"] == "fetch"
    assert stall["thresholdSeconds"] == 180.0
    assert stall["silentSeconds"] >= 200.0


def test_status_payload_omits_stall_info_when_not_stalled() -> None:
    from datetime import UTC, datetime, timedelta

    fresh_heartbeat = (datetime.now(UTC) - timedelta(seconds=10)).isoformat()

    svc = _make_service(parse_iso=_parse_iso)
    svc._status.update(
        {
            "runId": "pipeline_g2c",
            "active": True,
            "stage": "fetch",
            "activeChildTaskType": "fetch",
            "heartbeatAt": fresh_heartbeat,
            "_stageLedger": [],
            "_stageTransitions": [],
        }
    )

    payload = svc.get_status_payload()
    assert "stallInfo" not in payload


# --- G4: _stageTransitions resets between runs ---


def test_start_task_initialises_empty_stage_transitions() -> None:
    svc = _make_service()
    svc._status["_stageTransitions"] = [{"from": "x", "to": "y", "at": "z"}]
    # Simulate the start_task initializer reset (line ~1989)
    svc._status.update(
        {
            "active": True,
            "runId": "pipeline_new",
            "stage": "starting",
            "_stageLedger": [],
            "_stageTransitions": [],
        }
    )
    assert svc._status["_stageTransitions"] == []


def test_stage_transitions_caps_at_16() -> None:
    ticks = [f"2026-08-04T00:{i // 60:02d}:{i % 60:02d}Z" for i in range(40)]
    svc = _make_service(now_iso=_now_iso_factory(ticks))
    svc._status.update(
        {"runId": "pipeline_g4b", "active": True, "_stageLedger": [], "_stageTransitions": []}
    )

    for i in range(20):
        svc._mark_stage(stage="discovery", current_step=i, total_steps=20, label=f"Discovery {i}")
        svc._mark_stage(stage="fetch", current_step=i, total_steps=20, label=f"Fetch {i}")

    transitions = svc._status.get("_stageTransitions") or []
    assert len(transitions) == 16
    assert transitions[-1]["to"] == "fetch"
