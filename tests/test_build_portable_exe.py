from pathlib import Path
from unittest import mock
from zipfile import ZipFile

import pytest

from scripts import build_ship_bundle
from scripts.build_portable_exe import (
    DEFAULT_BUNDLE_VERSION,
    build_portable_layout,
    create_zip,
    generate_icon_file,
    parse_args,
    resolve_icon_path,
)
from src.app_version import APP_VERSION
from tests.helpers.temp_paths import workspace_tmpdir

pytestmark = pytest.mark.packaging


def test_portable_layout_wraps_ship_bundle_in_ship_folder() -> None:
    with workspace_tmpdir("build-portable-exe") as tmp:
        with mock.patch.object(
            build_ship_bundle,
            "_resolve_packaged_sync_config",
            return_value=build_ship_bundle.PACKAGED_SYNC_CONFIG_TEMPLATE_PATH,
        ):
            output = build_portable_layout(Path(tmp) / "dist" / "baluffo-portable", "9.9.9")
        assert (output / "ship" / "app" / "current.txt").exists()
        assert (output / "ship" / "run-site.ps1").exists()
        assert (output / "ship" / "src" / "ship" / "runtime_launcher.py").exists()
        assert (output / "ship" / "src" / "ship" / "desktop_update.py").exists()


def test_create_zip_packages_portable_folder() -> None:
    with workspace_tmpdir("build-portable-exe") as tmp:
        output = Path(tmp) / "dist" / "baluffo-portable"
        (output / "ship").mkdir(parents=True, exist_ok=True)
        (output / "Baluffo.exe").write_text("exe", encoding="utf-8")
        (output / "BaluffoUpdater.exe").write_text("helper", encoding="utf-8")
        archive = create_zip(output, version="1.0.0-test")
        assert archive.exists()
        with ZipFile(archive, "r") as handle:
            names = set(handle.namelist())
        assert "Baluffo.exe" in names
        assert "BaluffoUpdater.exe" in names
        assert "ship/" in names


def test_parse_args_defaults_to_shared_app_version() -> None:
    with mock.patch("sys.argv", ["build_portable_exe.py"]):
        args = parse_args()
    assert DEFAULT_BUNDLE_VERSION == APP_VERSION
    assert args.bundle_version == APP_VERSION


def test_generate_icon_file_writes_valid_ico_header() -> None:
    with workspace_tmpdir("build-portable-exe") as tmp:
        icon_path = generate_icon_file(Path(tmp) / "Baluffo.ico")
        payload = icon_path.read_bytes()
        assert payload.startswith(b"\x00\x00\x01\x00\x01\x00")
        assert len(payload) > 1024


def test_resolve_icon_path_generates_default_icon() -> None:
    with workspace_tmpdir("build-portable-exe") as tmp:
        output = Path(tmp) / "dist" / "baluffo-portable"
        icon_path = resolve_icon_path(output, exe_name="Baluffo")
        assert icon_path.exists()
        assert icon_path.suffix.lower() == ".ico"
