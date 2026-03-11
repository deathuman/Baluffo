import json
import unittest
from pathlib import Path
from unittest import mock

from scripts.build_ship_bundle import STARTUP_PREVIEW_LIMIT, build_bundle
from tests.temp_paths import workspace_tmpdir


class BuildShipBundleTests(unittest.TestCase):
    def _build_with_temp_packaged_config(
        self,
        tmp: str,
        *,
        env: dict[str, str] | None = None,
    ) -> Path:
        temp_root = Path(tmp)
        packaging_dir = temp_root / "packaging"
        packaging_dir.mkdir(parents=True, exist_ok=True)
        config_path = packaging_dir / "github-app-sync-config.json"
        template_path = Path(__file__).resolve().parents[1] / "packaging" / "github-app-sync-config.template.json"
        with mock.patch("scripts.build_ship_bundle.PACKAGED_SYNC_CONFIG_PATH", config_path), mock.patch(
            "scripts.build_ship_bundle.PACKAGED_SYNC_CONFIG_TEMPLATE_PATH", template_path
        ), mock.patch.dict("os.environ", env or {}, clear=False):
            return build_bundle(temp_root / "dist" / "baluffo-ship", "1.2.3")

    def test_bundle_contains_runtime_assets_and_seeded_data_only(self) -> None:
        with workspace_tmpdir("build-ship-bundle") as tmp:
            config_path = Path(tmp) / "packaging" / "github-app-sync-config.json"
            config_path.parent.mkdir(parents=True, exist_ok=True)
            config_path.write_text(
                json.dumps(
                    {
                        "schemaVersion": 1,
                        "appId": "123456",
                        "installationId": "999999",
                        "repo": "owner/repo",
                        "branch": "main",
                        "path": "baluffo/source-sync.json",
                        "privateKeyPem": "-----BEGIN RSA PRIVATE KEY-----\nTEST\n-----END RSA PRIVATE KEY-----",
                    }
                ),
                encoding="utf-8",
            )
            output = self._build_with_temp_packaged_config(tmp)
            version_root = output / "app" / "versions" / "1.2.3"

            self.assertTrue((output / "run-site.ps1").exists())
            self.assertTrue((output / "run-bridge.ps1").exists())
            self.assertTrue((output / "app" / "update-manifest.json").exists())
            self.assertTrue((version_root / "frontend" / "admin" / "app.js").exists())
            self.assertTrue((version_root / "scripts" / "admin_bridge.py").exists())
            self.assertTrue((version_root / "scripts" / "local_data_store.py").exists())
            self.assertTrue((version_root / "scripts" / "source_sync.py").exists())
            self.assertTrue((version_root / "scripts" / "discovery_seed_catalog.json").exists())
            self.assertTrue((version_root / "app-local-data-client.js").exists())
            self.assertTrue((version_root / "desktop-local-data-client.js").exists())
            self.assertTrue((version_root / "desktop-probe-css.html").exists())
            self.assertTrue((version_root / "desktop-probe.html").exists())
            self.assertTrue((version_root / "desktop-probe-head.html").exists())
            self.assertTrue((version_root / "desktop-probe-inline.html").exists())
            self.assertTrue((version_root / "startup-probe.js").exists())
            self.assertTrue((version_root / "packaging" / "README.md").exists())
            self.assertTrue((version_root / "packaging" / "github-app-sync-config.template.json").exists())
            self.assertFalse((version_root / "package-lock.json").exists())
            self.assertFalse((version_root / "LOCAL_SETUP.md").exists())
            self.assertFalse((version_root / "scripts" / "run_py_tests.cmd").exists())
            self.assertFalse((version_root / "scripts" / "build_sync_app_config.py").exists())
            self.assertTrue((version_root / "packaging" / "github-app-sync-config.json").exists())

            seeded_report = json.loads((output / "data" / "jobs-fetch-report.json").read_text(encoding="utf-8"))
            self.assertEqual(seeded_report, {"summary": {}, "sources": [], "runtime": {}, "outputs": {}})

    def test_bundle_generates_capped_startup_preview(self) -> None:
        with workspace_tmpdir("build-ship-bundle") as tmp:
            config_path = Path(tmp) / "packaging" / "github-app-sync-config.json"
            config_path.parent.mkdir(parents=True, exist_ok=True)
            config_path.write_text(
                json.dumps(
                    {
                        "schemaVersion": 1,
                        "appId": "123456",
                        "installationId": "999999",
                        "repo": "owner/repo",
                        "branch": "main",
                        "path": "baluffo/source-sync.json",
                        "privateKeyPem": "-----BEGIN RSA PRIVATE KEY-----\nTEST\n-----END RSA PRIVATE KEY-----",
                    }
                ),
                encoding="utf-8",
            )
            output = self._build_with_temp_packaged_config(tmp)
            startup_rows = json.loads((output / "app" / "versions" / "1.2.3" / "data" / "jobs-unified-startup.json").read_text(encoding="utf-8"))
            self.assertIsInstance(startup_rows, list)
            self.assertLessEqual(len(startup_rows), STARTUP_PREVIEW_LIMIT)
            self.assertGreater(len(startup_rows), 0)

    def test_bundle_generates_packaged_sync_config_from_build_env(self) -> None:
        with workspace_tmpdir("build-ship-bundle") as tmp:
            private_key_path = Path(tmp) / "packaging" / "github-app-private-key.pem"
            private_key_path.parent.mkdir(parents=True, exist_ok=True)
            private_key_path.write_text(
                "-----BEGIN RSA PRIVATE KEY-----\nTEST\n-----END RSA PRIVATE KEY-----\n",
                encoding="utf-8",
            )
            output = self._build_with_temp_packaged_config(
                tmp,
                env={
                    "BALUFFO_SYNC_BUILD_APP_ID": "123456",
                    "BALUFFO_SYNC_BUILD_INSTALLATION_ID": "999999",
                    "BALUFFO_SYNC_BUILD_REPO": "owner/repo",
                    "BALUFFO_SYNC_BUILD_PRIVATE_KEY_PATH": str(private_key_path),
                },
            )
            packaged_config = json.loads(
                (
                    output
                    / "app"
                    / "versions"
                    / "1.2.3"
                    / "packaging"
                    / "github-app-sync-config.json"
                ).read_text(encoding="utf-8")
            )
            self.assertEqual(packaged_config["appId"], "123456")
            self.assertEqual(packaged_config["installationId"], "999999")
            self.assertEqual(packaged_config["repo"], "owner/repo")
            self.assertEqual(packaged_config["allowedRepo"], "owner/repo")
            self.assertEqual(packaged_config["allowedBranch"], "main")
            self.assertEqual(packaged_config["allowedPathPrefix"], "baluffo/source-sync.json")
            self.assertEqual(packaged_config["keyDerivation"], "embedded")


if __name__ == "__main__":
    unittest.main()
