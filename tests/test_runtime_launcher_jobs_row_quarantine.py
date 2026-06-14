import json
from pathlib import Path

from src.ship import runtime_launcher as rl
from tests.helpers.temp_paths import workspace_tmpdir


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


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
        assert len(result["quarantined"]) == 4
        assert not (data_dir / "jobs-unified-startup.json").exists()
        assert not (data_dir / "jobs-unified.json").exists()
        assert not (data_dir / "jobs-unified-light.json").exists()
        assert not (data_dir / "jobs-unified.csv").exists()


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
        assert len(result["restored"]) == 3
        assert (data_dir / "jobs-unified.json").exists()
        assert (data_dir / "jobs-unified-light.json").exists()
        assert (data_dir / "jobs-unified.csv").exists()
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
