from pathlib import Path

from scripts import backup_e2e_validate as backup_validate
from tests.helpers.temp_paths import workspace_tmpdir


def test_run_validation_reports_desktop_file_store_scenarios() -> None:
    with workspace_tmpdir("backup-e2e-validate") as tmp:
        report = backup_validate.run_validation(Path(tmp) / "data")
    assert bool(report.get("ok"))
    assert int(report.get("schemaVersion") or 0) == 4
    assert int(report.get("backupSchemaVersion") or 0) == 4
    assert "dataDir" in report
    scenarios = report.get("scenarios") or []
    assert len(scenarios) == 3
    assert all(bool(row.get("ok")) for row in scenarios if isinstance(row, dict))
    assert [str(row.get("name") or "") for row in scenarios] == [
        "scenario_a_json_no_files",
        "scenario_b_with_files",
        "scenario_c_duplicates_and_malformed",
    ]
