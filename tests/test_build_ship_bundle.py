import json
import unittest
from pathlib import Path

from scripts.build_ship_bundle import build_bundle
from tests.temp_paths import workspace_tmpdir


class BuildShipBundleTests(unittest.TestCase):
    def test_bundle_contains_runtime_assets_and_seeded_data_only(self) -> None:
        with workspace_tmpdir("build-ship-bundle") as tmp:
            output = build_bundle(Path(tmp) / "dist" / "baluffo-ship", "1.2.3")
            version_root = output / "app" / "versions" / "1.2.3"

            self.assertTrue((output / "run-site.ps1").exists())
            self.assertTrue((output / "run-bridge.ps1").exists())
            self.assertTrue((output / "app" / "update-manifest.json").exists())
            self.assertTrue((version_root / "frontend" / "admin" / "app.js").exists())
            self.assertTrue((version_root / "scripts" / "admin_bridge.py").exists())
            self.assertTrue((version_root / "scripts" / "source_sync.py").exists())
            self.assertTrue((version_root / "scripts" / "discovery_seed_catalog.json").exists())
            self.assertTrue((version_root / "packaging" / "README.md").exists())
            self.assertTrue((version_root / "packaging" / "github-app-sync-config.template.json").exists())
            self.assertFalse((version_root / "package-lock.json").exists())
            self.assertFalse((version_root / "LOCAL_SETUP.md").exists())
            self.assertFalse((version_root / "scripts" / "run_py_tests.cmd").exists())
            self.assertFalse((version_root / "scripts" / "build_sync_app_config.py").exists())

            seeded_report = json.loads((output / "data" / "jobs-fetch-report.json").read_text(encoding="utf-8"))
            self.assertEqual(seeded_report, {"summary": {}, "sources": [], "runtime": {}, "outputs": {}})


if __name__ == "__main__":
    unittest.main()
