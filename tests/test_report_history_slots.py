"""Per-run report history slots: shared invariant for fetch + discovery reports (no network)."""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import pytest

import src.report_history_slots as slots
from src.report_history_slots import (
    HISTORY_KEEP,
    looks_like_terminal_report_text,
    upsert_history_slot,
    upsert_terminal_report_history_payloads,
    upsert_terminal_report_history_slots,
)
from src.source_registry_io_save import save_json_atomic

FETCH_NAMES = {"jobs-fetch-report.json"}
DISCOVERY_NAMES = {"source-discovery-report.json"}


def _report(run_id: str, *, terminal: bool = True, marker: str = "2026-09-06T11:00:00Z") -> dict:
    return {
        "runId": run_id,
        "startedAt": "2026-09-06T10:00:00Z",
        "finishedAt": marker if terminal else "",
        "summary": {"keptCount": 1},
    }


def _slot_payloads(history_dir: Path, stem: str) -> dict[str, dict]:
    out = {}
    for entry in sorted(history_dir.glob(f"{stem}-*.json.gz")):
        with gzip.open(entry, mode="rt", encoding="utf-8") as handle:
            out[entry.name] = json.load(handle)
    return out


def _history(tmp_path: Path, name: str) -> Path:
    return tmp_path / name


# --- text-level API -------------------------------------------------------


def test_progress_shell_write_backs_up_existing_terminal(tmp_path: Path) -> None:
    report = tmp_path / "jobs-fetch-report.json"
    upsert_terminal_report_history_slots(
        path=report,
        existing_text=json.dumps(_report("run-full")),
        incoming_text=json.dumps(_report("run-full", terminal=False)),
        report_names=FETCH_NAMES,
        history_dir_name="fetch-report-history",
        file_stem="jobs-fetch-report",
    )
    slots_map = _slot_payloads(_history(tmp_path, "fetch-report-history"), "jobs-fetch-report")
    assert len(slots_map) == 1
    assert next(iter(slots_map.values()))["runId"] == "run-full"


def test_terminal_over_different_run_finalizes_both_slots(tmp_path: Path) -> None:
    report = tmp_path / "jobs-fetch-report.json"
    upsert_terminal_report_history_slots(
        path=report,
        existing_text=json.dumps(_report("run-first")),
        incoming_text=json.dumps(_report("run-second")),
        report_names=FETCH_NAMES,
        history_dir_name="fetch-report-history",
        file_stem="jobs-fetch-report",
    )
    slots_map = _slot_payloads(_history(tmp_path, "fetch-report-history"), "jobs-fetch-report")
    runs = {payload["runId"] for payload in slots_map.values()}
    assert runs == {"run-first", "run-second"}


def test_same_run_terminal_rewrite_dedups_to_one_slot(tmp_path: Path) -> None:
    report = tmp_path / "jobs-fetch-report.json"
    common = dict(
        report_names=FETCH_NAMES,
        history_dir_name="fetch-report-history",
        file_stem="jobs-fetch-report",
    )
    upsert_terminal_report_history_slots(
        path=report,
        existing_text="",
        incoming_text=json.dumps(_report("run-a", marker="T1")),
        **common,
    )
    upsert_terminal_report_history_slots(
        path=report,
        existing_text=json.dumps(_report("run-a", marker="T1")),
        incoming_text=json.dumps(_report("run-a", marker="T2")),
        **common,
    )
    slots_map = _slot_payloads(_history(tmp_path, "fetch-report-history"), "jobs-fetch-report")
    assert len(slots_map) == 1
    assert next(iter(slots_map.values()))["finishedAt"] == "T2"


def test_non_terminal_write_without_terminal_existing_writes_nothing(tmp_path: Path) -> None:
    report = tmp_path / "jobs-fetch-report.json"
    upsert_terminal_report_history_slots(
        path=report,
        existing_text=json.dumps(_report("run-x", terminal=False)),
        incoming_text=json.dumps(_report("run-x", terminal=False)),
        report_names=FETCH_NAMES,
        history_dir_name="fetch-report-history",
        file_stem="jobs-fetch-report",
    )
    assert not (_history(tmp_path, "fetch-report-history")).exists() or not list(
        (_history(tmp_path, "fetch-report-history")).glob("*.json.gz")
    )


def test_name_gate_ignores_unrelated_files(tmp_path: Path) -> None:
    report = tmp_path / "some-other-report.json"
    upsert_terminal_report_history_slots(
        path=report,
        existing_text=json.dumps(_report("run-a")),
        incoming_text=json.dumps(_report("run-b")),
        report_names=FETCH_NAMES,
        history_dir_name="fetch-report-history",
        file_stem="jobs-fetch-report",
    )
    assert not (_history(tmp_path, "fetch-report-history")).exists()


def test_empty_run_id_falls_back_to_timestamp_slug(tmp_path: Path) -> None:
    report = tmp_path / "jobs-fetch-report.json"
    upsert_history_slot(
        json.dumps({"runId": "", "finishedAt": "2026-09-06T11:00:00Z"}),
        history_dir=_history(tmp_path, "fetch-report-history"),
        file_stem="jobs-fetch-report",
    )
    slots_map = _slot_payloads(_history(tmp_path, "fetch-report-history"), "jobs-fetch-report")
    assert len(slots_map) == 1
    assert "run-2" in next(iter(slots_map))


def test_terminal_marker_detection() -> None:
    assert looks_like_terminal_report_text(json.dumps({"finishedAt": "2026-09-06T11:00:00Z"}))
    assert not looks_like_terminal_report_text(json.dumps({"finishedAt": ""}))
    assert not looks_like_terminal_report_text("not json")


# --- payload-level API + retention ----------------------------------------


def test_payload_level_twin_matches_text_semantics(tmp_path: Path) -> None:
    report = tmp_path / "source-discovery-report.json"
    upsert_terminal_report_history_payloads(
        path=report,
        existing_payload=_report("disc-1"),
        incoming_payload=_report("disc-1", terminal=False),
        report_names=DISCOVERY_NAMES,
        history_dir_name="discovery-report-history",
        file_stem="source-discovery-report",
    )
    slots_map = _slot_payloads(
        _history(tmp_path, "discovery-report-history"), "source-discovery-report"
    )
    assert len(slots_map) == 1
    assert next(iter(slots_map.values()))["runId"] == "disc-1"


def test_retention_keeps_newest_slots(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(slots, "HISTORY_KEEP", 2)
    report = tmp_path / "jobs-fetch-report.json"
    for index in range(4):
        upsert_history_slot(
            json.dumps(_report(f"run-{index}")),
            history_dir=_history(tmp_path, "fetch-report-history"),
            file_stem="jobs-fetch-report",
        )
    slots_map = _slot_payloads(_history(tmp_path, "fetch-report-history"), "jobs-fetch-report")
    assert len(slots_map) == 2
    runs = {payload["runId"] for payload in slots_map.values()}
    assert runs == {"run-2", "run-3"}


def test_default_keep_is_48() -> None:
    assert HISTORY_KEEP == 48


# --- integration: save_json_atomic routes the discovery report -------------


def test_save_json_atomic_snapshots_discovery_report_per_run(tmp_path: Path) -> None:
    report = tmp_path / "source-discovery-report.json"
    save_json_atomic(report, _report("disc-run-1", marker="T1"))
    save_json_atomic(report, _report("disc-run-2", marker="T2"))
    history = _history(tmp_path, "discovery-report-history")
    slots_map = _slot_payloads(history, "source-discovery-report")
    runs = {payload["runId"]: payload["finishedAt"] for payload in slots_map.values()}
    assert runs == {"disc-run-1": "T1", "disc-run-2": "T2"}
    # live file holds the newest run
    assert json.loads(report.read_text(encoding="utf-8"))["runId"] == "disc-run-2"


def test_save_json_atomic_discovery_progress_write_preserves_terminal(tmp_path: Path) -> None:
    report = tmp_path / "source-discovery-report.json"
    save_json_atomic(report, _report("disc-run-1", marker="T1"))
    save_json_atomic(report, _report("disc-run-1", terminal=False))  # progress shell of same run
    history = _history(tmp_path, "discovery-report-history")
    slots_map = _slot_payloads(history, "source-discovery-report")
    assert len(slots_map) == 1
    assert next(iter(slots_map.values()))["finishedAt"] == "T1"


def test_save_json_atomic_ignores_non_report_files(tmp_path: Path) -> None:
    other = tmp_path / "unrelated.json"
    save_json_atomic(other, _report("run-a"))
    assert not (_history(tmp_path, "discovery-report-history")).exists()
    assert not (_history(tmp_path, "fetch-report-history")).exists()
