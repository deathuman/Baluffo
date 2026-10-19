import json
from pathlib import Path

import pytest

from src.bridge.fetch_report_summary import write_fetch_report_summary_artifact
from src.ship import runtime_launcher as rl
from tests.helpers.json_files import write_text_file_plain
from tests.helpers.temp_paths import workspace_tmpdir

_write = write_text_file_plain


def _write_windows_migration_report(data_dir: Path, *, created_at: str) -> None:
    _write(
        data_dir / "migration-reports" / "windows-user-data-migration.json",
        json.dumps(
            {
                "completed": True,
                "status": "copied",
                "createdAt": created_at,
                "targetDataDir": str(data_dir),
            }
        ),
    )


def _set_mtime(path: Path, timestamp: float) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch(exist_ok=True)
    rl.os.utime(str(path), (timestamp, timestamp))


GATE_EVENT = "desktop_launch_jobs_report_gate"
GATE_SKIPPED_EVENT = "desktop_launch_jobs_report_gate_skipped"


def _write_gate_report(data_dir: Path, *, sidecar: bool) -> None:
    report = {
        "runId": "run-1",
        "status": "ok",
        "finishedAt": "2026-06-14T19:40:00+00:00",
        "summary": {"outputCount": 12, "sourceCount": 1},
        "sources": [{"name": "source-a", "status": "ok", "keptCount": 12}],
    }
    _write(data_dir / "jobs-fetch-report.json", json.dumps(report))
    _write(data_dir / "jobs-unified.json", '[{"id":"job-1"}]')
    _write(data_dir / "jobs-unified-light.json", '[{"id":"job-1"}]')
    if sidecar:
        write_fetch_report_summary_artifact(data_dir / "jobs-fetch-report.json", report)


def _capture_traces(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, dict[str, object]]]:
    rows: list[tuple[str, dict[str, object]]] = []
    monkeypatch.setattr(
        rl,
        "_append_runtime_startup_trace",
        lambda event, **fields: rows.append((event, fields)),
    )
    return rows


def _gate_rows(rows: list[tuple[str, dict[str, object]]]) -> list[dict[str, object]]:
    return [fields for event, fields in rows if event == GATE_EVENT]


@pytest.mark.parametrize(
    ("sidecar", "expected_reader"),
    [(True, "summary-sidecar"), (False, "full-report")],
)
def test_quarantine_traces_which_artifact_settled_the_gate(
    monkeypatch: pytest.MonkeyPatch, sidecar: bool, expected_reader: str
) -> None:
    with workspace_tmpdir("runtime-launcher-gate-trace") as tmp:
        data_dir = Path(tmp) / "data"
        _write_gate_report(data_dir, sidecar=sidecar)
        rows = _capture_traces(monkeypatch)

        result = rl.quarantine_stale_jobs_row_artifacts(data_dir, process="bridge")

        assert result["skipped"] == "successful_runtime_report"
        assert result["reportGate"]["reader"] == expected_reader
        traced = _gate_rows(rows)
        assert len(traced) == 1
        fields = traced[0]
        assert fields["process"] == "bridge"
        assert fields["reader"] == expected_reader
        assert fields["success"] is True
        assert fields["hasReport"] is True
        assert fields["reportRunId"] == "run-1"
        assert fields["reportFinishedAt"] == "2026-06-14T19:40:00+00:00"
        assert fields["feedArtifactsOk"] is True
        assert fields["dataDir"] == str(data_dir)
        assert isinstance(fields["durationMs"], float)
        assert float(fields["durationMs"]) >= 0


def test_safe_quarantine_forwards_the_process_to_the_gate_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def _capture(data_dir: str | Path, *, process: str = "") -> dict[str, object]:
        captured.update(dataDir=str(data_dir), process=process)
        return {"quarantined": [], "failed": []}

    monkeypatch.setattr(rl, "quarantine_stale_jobs_row_artifacts", _capture)

    rl.safe_quarantine_stale_jobs_row_artifacts(Path("data"), process="site")

    assert captured == {"dataDir": "data", "process": "site"}


def test_quarantine_traces_a_failed_gate_without_checking_feeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("runtime-launcher-gate-trace-failure") as tmp:
        data_dir = Path(tmp) / "data"
        _write_gate_report(data_dir, sidecar=False)
        report = json.loads((data_dir / "jobs-fetch-report.json").read_text(encoding="utf-8"))
        report["status"] = "error"
        _write(data_dir / "jobs-fetch-report.json", json.dumps(report))
        rows = _capture_traces(monkeypatch)

        rl.quarantine_stale_jobs_row_artifacts(data_dir)

        fields = _gate_rows(rows)[0]
        assert fields["reader"] == "full-report"
        assert fields["reportMetadataOk"] is False
        assert fields["feedArtifactsOk"] is None
        assert fields["success"] is False


def test_quarantine_traces_a_skip_when_there_are_no_row_artifacts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("runtime-launcher-gate-trace-skipped") as tmp:
        data_dir = Path(tmp) / "data"
        data_dir.mkdir(parents=True)
        rows = _capture_traces(monkeypatch)

        result = rl.quarantine_stale_jobs_row_artifacts(data_dir, process="site")

        assert result == {"quarantined": [], "failed": [], "skipped": "no_artifacts"}
        traced = _gate_rows(rows)
        assert traced == []
        skipped = [fields for event, fields in rows if event == GATE_SKIPPED_EVENT]
        assert len(skipped) == 1
        assert skipped[0]["process"] == "site"
        assert skipped[0]["reason"] == "no_artifacts"
        assert skipped[0]["restored"] == 0
        assert skipped[0]["dataDir"] == str(data_dir)


def test_safe_quarantine_journals_process_and_error_when_it_crashes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("runtime-launcher-gate-quarantine-crash") as tmp:
        data_dir = Path(tmp) / "data"
        _write_gate_report(data_dir, sidecar=True)
        rows = _capture_traces(monkeypatch)

        def _crash(_data_dir: Path) -> tuple[bool, dict[str, object]]:
            raise RuntimeError("boom")

        monkeypatch.setattr(rl, "evaluate_runtime_jobs_report", _crash)

        result = rl.safe_quarantine_stale_jobs_row_artifacts(data_dir, process="site")

        assert result == {"quarantined": [], "failed": [{"path": str(data_dir), "error": "boom"}]}
        failures = [
            fields for event, fields in rows if event == "jobs_row_artifact_quarantine_failed"
        ]
        assert len(failures) == 1
        assert failures[0]["process"] == "site"
        assert failures[0]["dataDir"] == str(data_dir)
        assert failures[0]["error"] == "boom"


def test_quarantine_preserves_runtime_rows_newer_than_migration_report() -> None:
    with workspace_tmpdir("runtime-launcher-preserve-migrated-runtime-feed") as tmp:
        data_dir = Path(tmp) / "data"
        _write_windows_migration_report(data_dir, created_at="2026-05-31T08:18:52+00:00")
        _write(
            data_dir / "jobs-fetch-report.json",
            json.dumps(
                {
                    "status": "error",
                    "finishedAt": "2026-06-14T19:39:35+00:00",
                    "summary": {"outputCount": 49528},
                }
            ),
        )
        _write(data_dir / "jobs-unified.json", '[{"id":"job-1"}]')
        _write(data_dir / "jobs-unified-light.json", '[{"id":"job-1"}]')
        _write(data_dir / "jobs-unified.csv", "id,title\njob-1,Role\n")
        runtime_timestamp = rl.datetime(2026, 6, 14, 19, 39, 35, tzinfo=rl.UTC).timestamp()
        for path in (
            data_dir / "jobs-unified.json",
            data_dir / "jobs-unified-light.json",
            data_dir / "jobs-unified.csv",
        ):
            _set_mtime(path, runtime_timestamp)

        result = rl.quarantine_stale_jobs_row_artifacts(data_dir)

        assert result["skipped"] == "runtime_artifacts_newer_than_migration"
        assert result["quarantined"] == []
        assert (data_dir / "jobs-unified.json").exists()
        assert (data_dir / "jobs-unified-light.json").exists()
        assert (data_dir / "jobs-unified.csv").exists()


def test_quarantine_ignores_newer_startup_artifact_when_runtime_feed_is_older() -> None:
    with workspace_tmpdir("runtime-launcher-quarantine-startup-only-newer") as tmp:
        data_dir = Path(tmp) / "data"
        _write_windows_migration_report(data_dir, created_at="2026-05-31T08:18:52+00:00")
        _write(
            data_dir / "jobs-fetch-report.json",
            json.dumps(
                {
                    "status": "error",
                    "finishedAt": "2026-06-14T19:39:35+00:00",
                    "summary": {"outputCount": 49528},
                }
            ),
        )
        _write(data_dir / "jobs-unified-startup.json", '[{"id":"runtime-startup"}]')
        _write(data_dir / "jobs-unified.json", '[{"id":"old-seed"}]')
        _write(data_dir / "jobs-unified-light.json", '[{"id":"old-seed"}]')
        _write(data_dir / "jobs-unified.csv", "id,title\nold-seed,Role\n")
        startup_timestamp = rl.datetime(2026, 6, 14, 19, 39, 35, tzinfo=rl.UTC).timestamp()
        seed_timestamp = rl.datetime(2026, 5, 1, 0, 0, 0, tzinfo=rl.UTC).timestamp()
        _set_mtime(data_dir / "jobs-unified-startup.json", startup_timestamp)
        for path in (
            data_dir / "jobs-unified.json",
            data_dir / "jobs-unified-light.json",
            data_dir / "jobs-unified.csv",
        ):
            _set_mtime(path, seed_timestamp)

        result = rl.quarantine_stale_jobs_row_artifacts(data_dir)

        assert result["reason"] == "no_successful_runtime_jobs_report"
        assert len(result["quarantined"]) == 3
        assert not (data_dir / "jobs-unified-startup.json").exists()
        assert not (data_dir / "jobs-unified.json").exists()
        assert not (data_dir / "jobs-unified-light.json").exists()
        assert (data_dir / "jobs-unified.csv").exists()


def test_quarantine_restores_false_quarantined_runtime_rows_from_backup() -> None:
    with workspace_tmpdir("runtime-launcher-restore-false-quarantine") as tmp:
        data_dir = Path(tmp) / "data"
        backup_dir = data_dir / "backups" / "stripped-packaged-jobs-20260614T193933Z"
        _write_windows_migration_report(data_dir, created_at="2026-05-31T08:18:52+00:00")
        _write(
            data_dir / "migration-reports" / "stripped-packaged-jobs-cleanup-20260614T193933Z.json",
            json.dumps(
                {
                    "schemaVersion": 1,
                    "createdAt": "2026-06-14T19:39:33Z",
                    "reason": "no_successful_runtime_jobs_report",
                    "backupDir": str(backup_dir),
                    "quarantined": [
                        str(data_dir / "jobs-unified-light.json"),
                        str(data_dir / "jobs-unified.json"),
                        str(data_dir / "jobs-unified.csv"),
                    ],
                    "failed": [],
                }
            ),
        )
        _write(
            data_dir / "jobs-fetch-report.json",
            json.dumps(
                {
                    "status": "error",
                    "finishedAt": "2026-06-14T19:39:35+00:00",
                    "summary": {"outputCount": 49528},
                }
            ),
        )
        _write(backup_dir / "jobs-unified.json", '[{"id":"job-1"}]')
        _write(backup_dir / "jobs-unified-light.json", '[{"id":"job-1"}]')
        _write(backup_dir / "jobs-unified.csv", "id,title\njob-1,Role\n")
        runtime_timestamp = rl.datetime(2026, 6, 14, 19, 39, 35, tzinfo=rl.UTC).timestamp()
        for path in (
            backup_dir / "jobs-unified.json",
            backup_dir / "jobs-unified-light.json",
            backup_dir / "jobs-unified.csv",
        ):
            _set_mtime(path, runtime_timestamp)

        result = rl.quarantine_stale_jobs_row_artifacts(data_dir)

        assert result["skipped"] == "restored_false_quarantined_runtime_artifacts"
        assert len(result["restored"]) == 2
        assert (data_dir / "jobs-unified.json").exists()
        assert (data_dir / "jobs-unified-light.json").exists()
        assert not (data_dir / "jobs-unified.csv").exists()
        assert (backup_dir / "jobs-unified.csv").exists()
        assert (backup_dir / "jobs-unified.json").exists()
        assert list((data_dir / "migration-reports").glob("stripped-packaged-jobs-restore-*.json"))


def test_quarantine_does_not_restore_when_only_startup_backup_is_newer() -> None:
    with workspace_tmpdir("runtime-launcher-no-restore-startup-only-newer") as tmp:
        data_dir = Path(tmp) / "data"
        backup_dir = data_dir / "backups" / "stripped-packaged-jobs-20260614T193933Z"
        _write_windows_migration_report(data_dir, created_at="2026-05-31T08:18:52+00:00")
        _write(
            data_dir / "migration-reports" / "stripped-packaged-jobs-cleanup-20260614T193933Z.json",
            json.dumps(
                {
                    "schemaVersion": 1,
                    "createdAt": "2026-06-14T19:39:33Z",
                    "reason": "no_successful_runtime_jobs_report",
                    "backupDir": str(backup_dir),
                    "quarantined": [
                        str(data_dir / "jobs-unified-startup.json"),
                        str(data_dir / "jobs-unified-light.json"),
                        str(data_dir / "jobs-unified.json"),
                        str(data_dir / "jobs-unified.csv"),
                    ],
                    "failed": [],
                }
            ),
        )
        _write(backup_dir / "jobs-unified-startup.json", '[{"id":"runtime-startup"}]')
        _write(backup_dir / "jobs-unified.json", '[{"id":"old-seed"}]')
        _write(backup_dir / "jobs-unified-light.json", '[{"id":"old-seed"}]')
        _write(backup_dir / "jobs-unified.csv", "id,title\nold-seed,Role\n")
        startup_timestamp = rl.datetime(2026, 6, 14, 19, 39, 35, tzinfo=rl.UTC).timestamp()
        seed_timestamp = rl.datetime(2026, 5, 1, 0, 0, 0, tzinfo=rl.UTC).timestamp()
        _set_mtime(backup_dir / "jobs-unified-startup.json", startup_timestamp)
        for path in (
            backup_dir / "jobs-unified.json",
            backup_dir / "jobs-unified-light.json",
            backup_dir / "jobs-unified.csv",
        ):
            _set_mtime(path, seed_timestamp)

        result = rl.quarantine_stale_jobs_row_artifacts(data_dir)

        assert result == {"quarantined": [], "failed": [], "skipped": "no_artifacts"}
        assert not (data_dir / "jobs-unified-startup.json").exists()
        assert not (data_dir / "jobs-unified.json").exists()
        assert not (data_dir / "jobs-unified-light.json").exists()
        assert not (data_dir / "jobs-unified.csv").exists()


def test_quarantine_does_not_restore_packaged_rows_older_than_migration_report() -> None:
    with workspace_tmpdir("runtime-launcher-no-restore-legacy-seed") as tmp:
        data_dir = Path(tmp) / "data"
        backup_dir = data_dir / "backups" / "stripped-packaged-jobs-20260614T193933Z"
        _write_windows_migration_report(data_dir, created_at="2026-05-31T08:18:52+00:00")
        _write(
            data_dir / "migration-reports" / "stripped-packaged-jobs-cleanup-20260614T193933Z.json",
            json.dumps(
                {
                    "schemaVersion": 1,
                    "createdAt": "2026-06-14T19:39:33Z",
                    "reason": "no_successful_runtime_jobs_report",
                    "backupDir": str(backup_dir),
                    "quarantined": [str(data_dir / "jobs-unified.csv")],
                    "failed": [],
                }
            ),
        )
        _write(backup_dir / "jobs-unified.json", '[{"id":"seed-job"}]')
        _write(backup_dir / "jobs-unified-light.json", '[{"id":"seed-job"}]')
        _write(backup_dir / "jobs-unified.csv", "id,title\nseed-job,Role\n")
        seed_timestamp = rl.datetime(2026, 5, 1, 0, 0, 0, tzinfo=rl.UTC).timestamp()
        for path in (
            backup_dir / "jobs-unified.json",
            backup_dir / "jobs-unified-light.json",
            backup_dir / "jobs-unified.csv",
        ):
            _set_mtime(path, seed_timestamp)

        result = rl.quarantine_stale_jobs_row_artifacts(data_dir)

        assert result == {"quarantined": [], "failed": [], "skipped": "no_artifacts"}
        assert not (data_dir / "jobs-unified.json").exists()
        assert not (data_dir / "jobs-unified-light.json").exists()
        assert not (data_dir / "jobs-unified.csv").exists()
