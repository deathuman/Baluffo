import unittest
from pathlib import Path
from unittest import mock
from zipfile import ZipFile

from scripts.app_version import APP_VERSION
from scripts import build_ship_bundle
from scripts.build_portable_exe import (
    DEFAULT_BUNDLE_VERSION,
    build_portable_layout,
    create_zip,
    generate_icon_file,
    parse_args,
    resolve_icon_path,
)
from tests.helpers.temp_paths import workspace_tmpdir


class BuildPortableExeTests(unittest.TestCase):
    def test_portable_layout_wraps_ship_bundle_in_ship_folder(self) -> None:
        with workspace_tmpdir("build-portable-exe") as tmp:
            with mock.patch.object(
                build_ship_bundle,
                "_require_packaged_sync_config",
                return_value=build_ship_bundle.PACKAGED_SYNC_CONFIG_TEMPLATE_PATH,
            ):
                output = build_portable_layout(Path(tmp) / "dist" / "baluffo-portable", "9.9.9")
            self.assertTrue((output / "ship" / "app" / "current.txt").exists())
            self.assertTrue((output / "ship" / "run-site.ps1").exists())
            self.assertTrue((output / "ship" / "scripts" / "ship" / "runtime_launcher.py").exists())

    def test_create_zip_packages_portable_folder(self) -> None:
        with workspace_tmpdir("build-portable-exe") as tmp:
            output = Path(tmp) / "dist" / "baluffo-portable"
            (output / "ship").mkdir(parents=True, exist_ok=True)
            (output / "Baluffo.exe").write_text("exe", encoding="utf-8")
            archive = create_zip(output, version="1.0.0-test")
            self.assertTrue(archive.exists())
            with ZipFile(archive, "r") as handle:
                names = set(handle.namelist())
            self.assertIn("Baluffo.exe", names)
            self.assertIn("ship/", names)

    def test_parse_args_defaults_to_shared_app_version(self) -> None:
        with mock.patch("sys.argv", ["build_portable_exe.py"]):
            args = parse_args()
        self.assertEqual(DEFAULT_BUNDLE_VERSION, APP_VERSION)
        self.assertEqual(args.bundle_version, APP_VERSION)

    def test_generate_icon_file_writes_valid_ico_header(self) -> None:
        with workspace_tmpdir("build-portable-exe") as tmp:
            icon_path = generate_icon_file(Path(tmp) / "Baluffo.ico")
            payload = icon_path.read_bytes()
            self.assertTrue(payload.startswith(b"\x00\x00\x01\x00\x01\x00"))
            self.assertGreater(len(payload), 1024)

    def test_resolve_icon_path_generates_default_icon(self) -> None:
        with workspace_tmpdir("build-portable-exe") as tmp:
            output = Path(tmp) / "dist" / "baluffo-portable"
            icon_path = resolve_icon_path(output, exe_name="Baluffo")
            self.assertTrue(icon_path.exists())
            self.assertEqual(icon_path.suffix.lower(), ".ico")

if __name__ == "__main__":
    unittest.main()