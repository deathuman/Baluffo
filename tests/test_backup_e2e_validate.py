import unittest
from pathlib import Path

from scripts import backup_e2e_validate as backup_validate
from tests.helpers.temp_paths import workspace_tmpdir


class BackupE2EValidateTests(unittest.TestCase):
    def test_run_validation_reports_desktop_file_store_scenarios(self) -> None:
        with workspace_tmpdir("backup-e2e-validate") as tmp:
            report = backup_validate.run_validation(Path(tmp) / "data")
        self.assertTrue(bool(report.get("ok")))
        self.assertEqual(int(report.get("schemaVersion") or 0), 2)
        self.assertIn("dataDir", report)
        scenarios = report.get("scenarios") or []
        self.assertEqual(len(scenarios), 3)
        self.assertTrue(all(bool(row.get("ok")) for row in scenarios if isinstance(row, dict)))
        self.assertEqual(
            [str(row.get("name") or "") for row in scenarios],
            [
                "scenario_a_json_no_files",
                "scenario_b_with_files",
                "scenario_c_duplicates_and_malformed",
            ],
        )


if __name__ == "__main__":
    unittest.main()