"""Fetch-report history backups: a later run must never destroy the previous run's terminal evidence."""

from __future__ import annotations

import gzip
import json
import os
import time
from pathlib import Path

from src.pipeline_io import (
    _write_fetch_report_history_backup,
    write_hot_text_if_changed,
    write_text_if_changed,
)
from src.report_history_slots import HISTORY_KEEP as FETCH_REPORT_HISTORY_KEEP


def _terminal_report(run_id: str) -> str:
    return json.dumps(
        {
            "runId": run_id,
            "startedAt": "2026-09-06T10:00:00Z",
            "finishedAt": "2026-09-06T11:00:00Z",
            "summary": {"keptCount": 1},
            "sources": [{"name": "static_source::x", "status": "error"}],
        }
    )


def _progress_shell(run_id: str) -> str:
    return json.dumps(
        {
            "runId": run_id,
            "startedAt": "2026-09-06T12:00:00Z",
            "summary": {"queued": 5},
        }
    )


def _history_dir(report_path: Path) -> Path:
    return report_path.parent / "fetch-report-history"


def _read_backup(path: Path) -> dict:
    with gzip.open(path, mode="rt", encoding="utf-8") as handle:
        return json.load(handle)


def test_terminal_report_is_backed_up_before_progress_shell_overwrite(tmp_path) -> None:
    report = tmp_path / "jobs-fetch-report.json"
    report.write_text(_terminal_report("run-full-pass"), encoding="utf-8")

    write_text_if_changed(report, _progress_shell("run-targeted"))

    history = _history_dir(report)
    backups = list(history.glob("jobs-fetch-report-*.json.gz"))
    assert len(backups) == 1
    saved = _read_backup(backups[0])
    assert saved["runId"] == "run-full-pass"
    assert saved["finishedAt"] == "2026-09-06T11:00:00Z"
    assert saved["sources"][0]["status"] == "error"
    # The live path still serves the new run.
    assert json.loads(report.read_text(encoding="utf-8"))["runId"] == "run-targeted"


def test_terminal_report_is_backed_up_before_next_terminal_report(tmp_path) -> None:
    report = tmp_path / "jobs-fetch-report.json"
    report.write_text(_terminal_report("run-first"), encoding="utf-8")

    write_hot_text_if_changed(report, _terminal_report("run-second"))

    # Both runs keep their own per-run snapshot; the live file serves the newest.
    runs = {
        _read_backup(path)["runId"]
        for path in _history_dir(report).glob("jobs-fetch-report-*.json.gz")
    }
    assert runs == {"run-first", "run-second"}
    assert json.loads(report.read_text(encoding="utf-8"))["runId"] == "run-second"


def test_progress_shell_overwrite_creates_no_backup(tmp_path) -> None:
    report = tmp_path / "jobs-fetch-report.json"
    report.write_text(_progress_shell("run-active"), encoding="utf-8")

    write_text_if_changed(report, _progress_shell("run-active-next"))

    assert not _history_dir(report).exists()


def test_partial_shell_without_finished_at_creates_no_backup(tmp_path) -> None:
    report = tmp_path / "jobs-fetch-report.json"
    partial = json.dumps({"runId": "run-x", "startedAt": "2026-09-06T10:00:00Z"})
    report.write_text(partial, encoding="utf-8")

    write_text_if_changed(report, _progress_shell("run-y"))

    assert not _history_dir(report).exists()


def test_non_report_files_are_never_backed_up(tmp_path) -> None:
    tasks = tmp_path / "jobs-fetch-tasks.json"
    tasks.write_text(_terminal_report("run-a"), encoding="utf-8")

    write_hot_text_if_changed(tasks, _terminal_report("run-b"))

    assert not _history_dir(tasks).exists()


def test_unchanged_write_skips_backup(tmp_path) -> None:
    report = tmp_path / "jobs-fetch-report.json"
    payload = _terminal_report("run-a")
    report.write_text(payload, encoding="utf-8")

    assert write_text_if_changed(report, payload) is False
    assert not _history_dir(report).exists()


def test_same_second_backups_do_not_overwrite_each_other(tmp_path) -> None:
    report = tmp_path / "jobs-fetch-report.json"
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(_terminal_report("run-a"), encoding="utf-8")
    target = report

    _write_fetch_report_history_backup(target, _terminal_report("run-a"))
    _write_fetch_report_history_backup(target, _terminal_report("run-b"))

    backups = sorted(_history_dir(report).glob("jobs-fetch-report-*.json.gz"))
    assert len(backups) == 2
    runs = {_read_backup(path)["runId"] for path in backups}
    assert runs == {"run-a", "run-b"}


def test_history_rotates_to_keep_limit(tmp_path) -> None:
    report = tmp_path / "jobs-fetch-report.json"
    report.parent.mkdir(parents=True, exist_ok=True)
    history = _history_dir(report)
    history.mkdir(parents=True, exist_ok=True)
    now = time.time()
    for index in range(60):
        entry = history / f"jobs-fetch-report-20260101-0000{index:02d}.json.gz"
        entry.write_text("old", encoding="utf-8")
        os.utime(entry, (now - (600 - index), now - (600 - index)))

    _write_fetch_report_history_backup(report, _terminal_report("run-new"))

    backups = list(history.glob("jobs-fetch-report-*.json.gz"))
    assert len(backups) == FETCH_REPORT_HISTORY_KEEP
    # The new snapshot survives and the oldest seeded entries are gone.
    newest = max(backups, key=lambda path: path.stat().st_mtime)
    assert _read_backup(newest)["runId"] == "run-new"
    assert not (history / "jobs-fetch-report-20260101-000000.json.gz").exists()


def test_unreadable_existing_payload_still_overwrites_without_crash(tmp_path) -> None:
    report = tmp_path / "jobs-fetch-report.json"
    report.write_text("{not json", encoding="utf-8")

    assert write_text_if_changed(report, _terminal_report("run-a")) is True
    assert json.loads(report.read_text(encoding="utf-8"))["runId"] == "run-a"
    # The corrupt predecessor is not backed up, but the new terminal report is.
    runs = {
        _read_backup(path)["runId"]
        for path in _history_dir(report).glob("jobs-fetch-report-*.json.gz")
    }
    assert runs == {"run-a"}


def test_history_filename_carries_run_slug(tmp_path) -> None:
    report = tmp_path / "jobs-fetch-report.json"
    report.write_text(_terminal_report("run-full-pass"), encoding="utf-8")

    write_text_if_changed(report, _progress_shell("run-targeted"))

    names = [path.name for path in _history_dir(report).glob("jobs-fetch-report-*.json.gz")]
    assert len(names) == 1
    assert names[0].startswith("jobs-fetch-report-run-full-pass-")


def test_targeted_run_snapshot_cannot_displace_full_pass_snapshot(tmp_path) -> None:
    report = tmp_path / "jobs-fetch-report.json"
    report.write_text(_terminal_report("run-full-pass"), encoding="utf-8")
    write_text_if_changed(report, _progress_shell("run-targeted"))
    # Now the targeted run finishes and its terminal report overwrites the shell.
    write_text_if_changed(report, _terminal_report("run-targeted"))

    history = _history_dir(report)
    runs = set()
    for path in history.glob("jobs-fetch-report-*.json.gz"):
        runs.add(_read_backup(path)["runId"])
    assert runs == {"run-full-pass", "run-targeted"}


def test_same_run_terminal_rewrites_dedup_to_newest_copy(tmp_path) -> None:
    report = tmp_path / "jobs-fetch-report.json"
    first = json.dumps(
        {
            "runId": "run-x",
            "startedAt": "2026-09-06T10:00:00Z",
            "finishedAt": "2026-09-06T11:00:00Z",
            "summary": {"keptCount": 1},
        }
    )
    second = json.dumps(
        {
            "runId": "run-x",
            "startedAt": "2026-09-06T10:00:00Z",
            "finishedAt": "2026-09-06T11:05:00Z",
            "summary": {"keptCount": 2},
        }
    )
    report.write_text(first, encoding="utf-8")
    assert write_text_if_changed(report, second) is True

    backups = list(_history_dir(report).glob("jobs-fetch-report-run-x-*.json.gz"))
    assert len(backups) == 1
    assert _read_backup(backups[0])["summary"]["keptCount"] == 2
