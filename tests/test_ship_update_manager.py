import json
import unittest
from pathlib import Path
from unittest import mock
from zipfile import ZIP_DEFLATED, ZipFile

from scripts.ship import update_manager as um
from tests.temp_paths import workspace_tmpdir


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _seed_root(root: Path, version: str = "1.0.0") -> None:
    _write(root / "app" / "current.txt", f"{version}\n")
    _write(
        root / "app" / "update-state.json",
        json.dumps(
            {
                "current_version": version,
                "previous_version": "",
                "last_update_status": "ready",
                "last_error_code": "",
                "updated_at": um.iso_now(),
            }
        ),
    )
    (root / "app" / "versions" / version / "scripts").mkdir(parents=True, exist_ok=True)
    _write(root / "app" / "versions" / version / "scripts" / "admin_bridge.py", "print('ok')\n")
    _write(root / "app" / "versions" / version / "index.html", "<html></html>\n")
    _write(root / "app" / "versions" / version / "jobs.html", "<html></html>\n")
    _write(root / "app" / "versions" / version / "saved.html", "<html></html>\n")
    (root / "app" / "staging").mkdir(parents=True, exist_ok=True)
    (root / "data" / "backups").mkdir(parents=True, exist_ok=True)
    (root / "data" / "migration-reports").mkdir(parents=True, exist_ok=True)
    _write(root / "data" / "user-settings.json", '{"theme":"dark"}\n')


def _build_update_zip(work: Path, version: str) -> Path:
    source = work / "payload" / "app" / "versions" / version
    _write(source / "scripts" / "admin_bridge.py", "print('updated')\n")
    _write(source / "index.html", "<html>new</html>\n")
    _write(source / "jobs.html", "<html>new</html>\n")
    _write(source / "saved.html", "<html>new</html>\n")
    bundle = work / f"baluffo-{version}.zip"
    with ZipFile(bundle, "w", compression=ZIP_DEFLATED) as archive:
        for path in source.rglob("*"):
            if path.is_file():
                archive.write(path, path.relative_to(work / "payload").as_posix())
    return bundle


class ShipUpdateManagerTests(unittest.TestCase):
    def test_write_json_atomic_retries_transient_permission_error(self) -> None:
        with workspace_tmpdir("ship-update") as tmp:
            target = Path(tmp) / "ship" / "app" / "update-state.json"
            calls = {"count": 0}
            original_replace = um.os.replace

            def flaky_replace(src, dst):  # noqa: ANN001
                calls["count"] += 1
                if calls["count"] == 1:
                    raise PermissionError(32, "sharing violation")
                return original_replace(src, dst)

            with mock.patch.object(um.os, "replace", side_effect=flaky_replace):
                um.write_json_atomic(target, {"ok": True})

            self.assertEqual(json.loads(target.read_text(encoding="utf-8"))["ok"], True)
            self.assertEqual(calls["count"], 2)

    def test_startup_check_rejects_data_dir_inside_versions(self) -> None:
        with workspace_tmpdir("ship-update") as tmp:
            root = Path(tmp) / "ship"
            _seed_root(root)
            bad_data_dir = root / "app" / "versions" / "1.0.0" / "data"
            bad_data_dir.mkdir(parents=True, exist_ok=True)
            with self.assertRaises(ValueError):
                um.startup_check(root, bad_data_dir)

    def test_apply_update_success_switches_current_version_and_keeps_data(self) -> None:
        with workspace_tmpdir("ship-update") as tmp:
            root = Path(tmp) / "ship"
            _seed_root(root, version="1.0.0")
            bundle = _build_update_zip(Path(tmp), "1.1.0")
            sha256 = um.compute_sha256(bundle)
            key = "test-key"
            manifest = {
                "version": "1.1.0",
                "artifact_url": "file://local",
                "sha256": sha256,
                "signature": um.sign_manifest("1.1.0", sha256, key),
                "min_updater_version": "1.0.0",
                "migration_plan": ["noop"],
                "rollback_allowed": False,
            }
            manifest_path = Path(tmp) / "update-manifest.json"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

            result = um.apply_update(root, bundle, manifest_path, key)
            self.assertTrue(result["ok"])
            self.assertEqual((root / "app" / "current.txt").read_text(encoding="utf-8").strip(), "1.1.0")
            self.assertEqual(json.loads((root / "data" / "user-settings.json").read_text(encoding="utf-8"))["theme"], "dark")

    def test_apply_update_rejects_checksum_mismatch(self) -> None:
        with workspace_tmpdir("ship-update") as tmp:
            root = Path(tmp) / "ship"
            _seed_root(root, version="1.0.0")
            bundle = _build_update_zip(Path(tmp), "1.1.0")
            key = "test-key"
            manifest = {
                "version": "1.1.0",
                "artifact_url": "file://local",
                "sha256": "0" * 64,
                "signature": um.sign_manifest("1.1.0", "0" * 64, key),
                "min_updater_version": "1.0.0",
                "migration_plan": [],
                "rollback_allowed": False,
            }
            manifest_path = Path(tmp) / "update-manifest.json"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

            with self.assertRaises(ValueError):
                um.apply_update(root, bundle, manifest_path, key)
            self.assertEqual((root / "app" / "current.txt").read_text(encoding="utf-8").strip(), "1.0.0")

    def test_recover_previous_swaps_versions(self) -> None:
        with workspace_tmpdir("ship-update") as tmp:
            root = Path(tmp) / "ship"
            _seed_root(root, version="1.1.0")
            (root / "app" / "versions" / "1.0.0" / "scripts").mkdir(parents=True, exist_ok=True)
            _write(root / "app" / "versions" / "1.0.0" / "scripts" / "admin_bridge.py", "print('ok')\n")
            _write(root / "app" / "versions" / "1.0.0" / "index.html", "<html></html>\n")
            _write(root / "app" / "versions" / "1.0.0" / "jobs.html", "<html></html>\n")
            _write(root / "app" / "versions" / "1.0.0" / "saved.html", "<html></html>\n")
            state = json.loads((root / "app" / "update-state.json").read_text(encoding="utf-8"))
            state["previous_version"] = "1.0.0"
            (root / "app" / "update-state.json").write_text(json.dumps(state), encoding="utf-8")

            result = um.recover_previous(root)
            self.assertTrue(result["ok"])
            self.assertEqual((root / "app" / "current.txt").read_text(encoding="utf-8").strip(), "1.0.0")

    def test_validate_manifest_uses_numeric_semver_for_min_updater_version(self) -> None:
        previous = um.UPDATER_VERSION
        manifest = {
            "version": "2.0.0",
            "artifact_url": "file://local",
            "sha256": "1" * 64,
            "signature": "2" * 64,
            "min_updater_version": "1.0.10",
            "migration_plan": [],
            "rollback_allowed": False,
        }
        um.UPDATER_VERSION = "1.0.12"
        try:
            um.validate_manifest(manifest)
        finally:
            um.UPDATER_VERSION = previous


if __name__ == "__main__":
    unittest.main()
