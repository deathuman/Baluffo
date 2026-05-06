import json
from pathlib import Path
from unittest import mock
from zipfile import ZipFile

import pytest

from scripts import build_ship_bundle
from scripts.build_portable_exe import (
    DEFAULT_BUNDLE_VERSION,
    DEFAULT_ICON_PATH,
    MAIN_RUNTIME_COLLECT_ALL_PACKAGES,
    MAIN_RUNTIME_COLLECT_DATA_PACKAGES,
    MAIN_RUNTIME_EXCLUDED_MODULES,
    MAIN_RUNTIME_HIDDEN_IMPORTS,
    OPTIONAL_GITHUB_TLS_RUNTIME_PACKAGES,
    OPTIONAL_SCRAPY_RUNTIME_PACKAGES,
    UPDATER_HELPER_COLLECT_DATA_PACKAGES,
    UPDATER_HELPER_HIDDEN_IMPORTS,
    build_portable_layout,
    create_zip,
    mirror_latest_portable,
    parse_args,
    resolve_icon_path,
)
from src.app_version import APP_VERSION
from tests.helpers.temp_paths import workspace_tmpdir

pytestmark = pytest.mark.packaging


def test_portable_layout_wraps_ship_bundle_in_ship_folder() -> None:
    with workspace_tmpdir("build-portable-exe") as tmp:

        def fake_build_bundle(output_dir: Path, version: str) -> Path:
            ship_root = Path(output_dir)
            version_root = ship_root / "app" / "versions" / version
            tooling_root = ship_root / "src" / "ship"
            (version_root / "src" / "ship").mkdir(parents=True, exist_ok=True)
            (version_root / "data" / "contracts").mkdir(parents=True, exist_ok=True)
            tooling_root.mkdir(parents=True, exist_ok=True)
            (ship_root / "app").mkdir(parents=True, exist_ok=True)
            (ship_root / "run-site.ps1").write_text("", encoding="utf-8")
            (ship_root / "app" / "current.txt").write_text(f"{version}\n", encoding="utf-8")
            (version_root / "src" / "ship" / "runtime_launcher.py").write_text(
                "# runtime launcher\n",
                encoding="utf-8",
            )
            (version_root / "src" / "ship" / "desktop_update.py").write_text(
                "# desktop update\n",
                encoding="utf-8",
            )
            (tooling_root / "runtime_launcher.py").write_text(
                "# runtime launcher\n", encoding="utf-8"
            )
            (tooling_root / "desktop_update.py").write_text("# desktop update\n", encoding="utf-8")
            for rel_path in build_ship_bundle.APP_VERSION_CONTRACT_FILES:
                (version_root / "data" / rel_path).write_text(
                    json.dumps({"fixture": rel_path}),
                    encoding="utf-8",
                )
            return ship_root

        with (
            mock.patch.object(
                build_ship_bundle,
                "_resolve_packaged_sync_config",
                return_value=build_ship_bundle.PACKAGED_SYNC_CONFIG_TEMPLATE_PATH,
            ),
            mock.patch("scripts.build_portable_exe.build_bundle", side_effect=fake_build_bundle),
        ):
            output = build_portable_layout(Path(tmp) / "dist" / "baluffo-portable", "9.9.9")
        assert (output / "ship" / "app" / "current.txt").exists()
        assert (output / "ship" / "run-site.ps1").exists()
        assert (output / "ship" / "src" / "ship" / "runtime_launcher.py").exists()
        assert (output / "ship" / "src" / "ship" / "desktop_update.py").exists()
        for rel_path in build_ship_bundle.APP_VERSION_CONTRACT_FILES:
            assert (output / "ship" / "app" / "versions" / "9.9.9" / "data" / rel_path).exists()


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


def test_mirror_latest_portable_replaces_stale_latest_artifact() -> None:
    with workspace_tmpdir("build-portable-exe") as tmp:
        root = Path(tmp)
        output = root / "dist" / "baluffo-portable"
        latest = root / "_out" / "latest" / "build" / "portable"
        output.mkdir(parents=True, exist_ok=True)
        latest.mkdir(parents=True, exist_ok=True)
        (output / "Baluffo.exe").write_text("new exe", encoding="utf-8")
        (output / "BaluffoUpdater.exe").write_text("new helper", encoding="utf-8")
        (output / "ship").mkdir()
        (output / "ship" / "run-site.ps1").write_text("new ship", encoding="utf-8")
        (latest / "Baluffo.exe").write_text("old exe", encoding="utf-8")
        (latest / "old.txt").write_text("stale", encoding="utf-8")

        mirrored = mirror_latest_portable(output, latest)

        assert mirrored == latest.resolve()
        assert (latest / "Baluffo.exe").read_text(encoding="utf-8") == "new exe"
        assert (latest / "BaluffoUpdater.exe").read_text(encoding="utf-8") == "new helper"
        assert (latest / "ship" / "run-site.ps1").read_text(encoding="utf-8") == "new ship"
        assert not (latest / "old.txt").exists()


def test_mirror_latest_portable_refuses_missing_executable() -> None:
    with workspace_tmpdir("build-portable-exe") as tmp:
        output = Path(tmp) / "dist" / "baluffo-portable"
        output.mkdir(parents=True, exist_ok=True)
        with pytest.raises(RuntimeError, match="Portable executable missing"):
            mirror_latest_portable(output, Path(tmp) / "_out" / "latest" / "build" / "portable")


def test_parse_args_defaults_to_shared_app_version() -> None:
    with mock.patch("sys.argv", ["build_portable_exe.py"]):
        args = parse_args()
    assert DEFAULT_BUNDLE_VERSION == APP_VERSION
    assert args.bundle_version == APP_VERSION


def test_resolve_icon_path_defaults_to_checked_in_favicon() -> None:
    icon_path = resolve_icon_path()
    assert icon_path == DEFAULT_ICON_PATH
    assert icon_path.exists()
    assert icon_path.suffix.lower() == ".ico"


def test_resolve_icon_path_rejects_missing_explicit_icon() -> None:
    with workspace_tmpdir("build-portable-exe") as tmp:
        missing_icon = Path(tmp) / "missing.ico"
        with pytest.raises(RuntimeError, match="Icon file not found"):
            resolve_icon_path(str(missing_icon))


def test_helper_hidden_imports_include_tkinter_progress_ui_modules() -> None:
    assert "tkinter" in UPDATER_HELPER_HIDDEN_IMPORTS
    assert "tkinter.ttk" in UPDATER_HELPER_HIDDEN_IMPORTS
    assert "src.shared.github_https" in UPDATER_HELPER_HIDDEN_IMPORTS


def test_main_runtime_hidden_imports_preserve_packaged_browser_fallback_support() -> None:
    assert "src.admin_bridge" in MAIN_RUNTIME_HIDDEN_IMPORTS
    assert "tkinter" in MAIN_RUNTIME_HIDDEN_IMPORTS
    assert "src.shared.github_https" in MAIN_RUNTIME_HIDDEN_IMPORTS
    assert "src.local_data_store_attachments" in MAIN_RUNTIME_HIDDEN_IMPORTS
    assert "src.local_data_store_backup" in MAIN_RUNTIME_HIDDEN_IMPORTS
    assert "src.local_data_store_profiles" in MAIN_RUNTIME_HIDDEN_IMPORTS
    assert "src.local_data_store_saved_jobs" in MAIN_RUNTIME_HIDDEN_IMPORTS
    assert "src.local_data_store_shared" in MAIN_RUNTIME_HIDDEN_IMPORTS
    assert "src.source_sync_runtime" in MAIN_RUNTIME_HIDDEN_IMPORTS


def test_main_runtime_collect_all_packages_include_scrapy_runtime_when_available() -> None:
    assert MAIN_RUNTIME_COLLECT_ALL_PACKAGES == tuple(
        package_name
        for package_name in OPTIONAL_SCRAPY_RUNTIME_PACKAGES
        if package_name not in {"twisted", "queuelib"}
    )
    if OPTIONAL_SCRAPY_RUNTIME_PACKAGES:
        assert "scrapy" in OPTIONAL_SCRAPY_RUNTIME_PACKAGES
        assert "twisted" in OPTIONAL_SCRAPY_RUNTIME_PACKAGES
        assert "scrapy_playwright" in OPTIONAL_SCRAPY_RUNTIME_PACKAGES


def test_portable_build_collects_optional_github_tls_runtime_data() -> None:
    assert MAIN_RUNTIME_COLLECT_DATA_PACKAGES == OPTIONAL_GITHUB_TLS_RUNTIME_PACKAGES
    assert UPDATER_HELPER_COLLECT_DATA_PACKAGES == OPTIONAL_GITHUB_TLS_RUNTIME_PACKAGES
    if OPTIONAL_GITHUB_TLS_RUNTIME_PACKAGES:
        assert "certifi" in OPTIONAL_GITHUB_TLS_RUNTIME_PACKAGES


def test_portable_build_excludes_test_only_pyinstaller_dependency_trees() -> None:
    expected_exclusions = {
        "twisted.trial.test",
        "twisted.trial._dist.test",
        "twisted.test",
        "twisted.internet.test",
        "twisted.python.test",
        "twisted.protocols.test",
        "twisted.conch.test",
        "twisted.application.test",
        "twisted.web.test",
        "twisted.words.test",
        "twisted._threads.test",
        "queuelib.tests",
        "pytest",
        "_pytest",
        "hypothesis",
        "hypothesis.strategies",
        "pydantic.v1._hypothesis_plugin",
        "hamcrest",
    }

    assert expected_exclusions.issubset(set(MAIN_RUNTIME_EXCLUDED_MODULES))


def test_portable_build_keeps_scrapy_runtime_collection() -> None:
    assert {"scrapy", "scrapy_playwright", "twisted"}.issubset(
        set(OPTIONAL_SCRAPY_RUNTIME_PACKAGES)
    )
    assert {"scrapy", "scrapy_playwright"}.issubset(set(MAIN_RUNTIME_COLLECT_ALL_PACKAGES))
    assert "twisted" not in MAIN_RUNTIME_COLLECT_ALL_PACKAGES
    assert "queuelib" not in MAIN_RUNTIME_COLLECT_ALL_PACKAGES
    assert {"scrapy", "scrapy_playwright", "twisted"}.issubset(set(MAIN_RUNTIME_HIDDEN_IMPORTS))


def test_updater_helper_does_not_inherit_main_scrapy_test_exclusions() -> None:
    assert "scrapy" not in UPDATER_HELPER_HIDDEN_IMPORTS
    assert "twisted" not in UPDATER_HELPER_HIDDEN_IMPORTS
    assert not set(MAIN_RUNTIME_EXCLUDED_MODULES).intersection(set(UPDATER_HELPER_HIDDEN_IMPORTS))


def test_helper_hidden_imports_omit_playwright_heavy_runtime_graph() -> None:
    assert "src.admin_bridge" not in UPDATER_HELPER_HIDDEN_IMPORTS
