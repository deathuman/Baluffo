#!/usr/bin/env python3
"""Build a portable Windows executable wrapper around the Baluffo ship bundle."""

from __future__ import annotations

import argparse
import importlib.util
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DIST_DIR = ROOT / "dist" / "baluffo-portable"
DEFAULT_EXE_NAME = "Baluffo"
DEFAULT_ICON_PATH = ROOT / "favicon.ico"
OPTIONAL_GITHUB_TLS_RUNTIME_PACKAGES = tuple(
    package_name
    for package_name in ("certifi",)
    if importlib.util.find_spec(package_name) is not None
)
OPTIONAL_SCRAPY_RUNTIME_PACKAGES = tuple(
    package_name
    for package_name in (
        "scrapy",
        "scrapy_playwright",
        "twisted",
        "parsel",
        "w3lib",
        "queuelib",
        "itemadapter",
        "itemloaders",
        "cssselect",
        "protego",
        "service_identity",
        "pydispatch",
    )
    if importlib.util.find_spec(package_name) is not None
)
MAIN_RUNTIME_HIDDEN_IMPORTS = (
    "src.admin_bridge",
    "src.app_version",
    "src.baluffo_config",
    "src.contracts",
    "src.exceptions",
    "src.fetcher_metrics",
    "src.jobs_fetcher",
    "src.jobs_fetcher_registry",
    "src.local_data_store_attachments",
    "src.local_data_store_backup",
    "src.local_data_store_profiles",
    "src.local_data_store_saved_jobs",
    "src.local_data_store_shared",
    "src.local_data_store",
    "src.pipeline_io",
    "src.shared",
    "src.shared.github_https",
    "src.shared.regex",
    "src.shared.utils",
    "src.ship.migrations",
    "src.ship.desktop_update",
    "src.ship.desktop_updater",
    "src.ship.runtime_launcher",
    "src.ship.startup_profile",
    "src.ship.update_manager",
    "src.source_discovery",
    "src.source_registry",
    "src.source_sync_config",
    "src.source_sync_crypto",
    "src.source_sync_runtime",
    "src.source_sync_snapshot",
    "src.source_sync",
    *OPTIONAL_GITHUB_TLS_RUNTIME_PACKAGES,
    *OPTIONAL_SCRAPY_RUNTIME_PACKAGES,
    "tkinter",
    "tkinter.ttk",
)
MAIN_RUNTIME_COLLECT_DATA_PACKAGES = OPTIONAL_GITHUB_TLS_RUNTIME_PACKAGES
MAIN_RUNTIME_COLLECT_ALL_PACKAGES = tuple(
    package_name
    for package_name in OPTIONAL_SCRAPY_RUNTIME_PACKAGES
    if package_name not in {"twisted", "queuelib"}
)
# PyInstaller's broad Scrapy/Twisted collection otherwise walks test-only dependency
# trees and reports missing optional test plugins; keep runtime packages, exclude tests.
MAIN_RUNTIME_EXCLUDED_MODULES = (
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
)
UPDATER_HELPER_HIDDEN_IMPORTS = (
    "src.shared.github_https",
    *OPTIONAL_GITHUB_TLS_RUNTIME_PACKAGES,
    "tkinter",
    "tkinter.ttk",
)
UPDATER_HELPER_COLLECT_DATA_PACKAGES = OPTIONAL_GITHUB_TLS_RUNTIME_PACKAGES
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_ship_bundle import DEFAULT_BUNDLE_VERSION, build_bundle
from src.python_version_guard import ensure_required_python
from src.ship.update_manager import REQUIRED_VERSION_FILES


def _copy_tree_contents(src: Path, dst: Path) -> None:
    dst.mkdir(parents=True, exist_ok=True)
    for item in src.iterdir():
        target = dst / item.name
        if item.is_dir():
            if target.exists():
                shutil.rmtree(target)
            shutil.copytree(item, target)
        else:
            shutil.copy2(item, target)


def resolve_icon_path(icon_arg: str = "") -> Path:
    if str(icon_arg or "").strip():
        icon_path = Path(icon_arg).expanduser().resolve()
        if not icon_path.exists():
            raise RuntimeError(f"Icon file not found: {icon_path}")
        return icon_path
    if not DEFAULT_ICON_PATH.exists():
        raise RuntimeError(f"Default icon file not found: {DEFAULT_ICON_PATH}")
    return DEFAULT_ICON_PATH


def build_portable_layout(output_dir: Path, version: str) -> Path:
    if output_dir.exists():
        last_error: Exception | None = None
        for _ in range(10):
            try:
                shutil.rmtree(output_dir)
                last_error = None
                break
            except PermissionError as exc:
                last_error = exc
                time.sleep(0.25)
        if output_dir.exists():
            raise RuntimeError(
                f"Could not clear existing portable output directory due to a file lock: {output_dir}"
            ) from last_error
    output_dir.mkdir(parents=True, exist_ok=True)
    build_bundle(output_dir / "ship", version)
    return output_dir


def run_pyinstaller(
    output_dir: Path, *, exe_name: str, icon_path: Path, bundle_version: str
) -> Path:
    pyinstaller_dist = output_dir.parent / ".pyinstaller-dist"
    pyinstaller_work = output_dir.parent / ".pyinstaller-work"
    pyinstaller_spec = output_dir.parent / ".pyinstaller-spec"
    for path in (pyinstaller_dist, pyinstaller_work, pyinstaller_spec):
        if path.exists():
            shutil.rmtree(path)
        path.mkdir(parents=True, exist_ok=True)
    command = [
        sys.executable,
        "-m",
        "PyInstaller",
        "--noconfirm",
        "--clean",
        "--onedir",
        "--windowed",
        "--name",
        exe_name,
        "--icon",
        str(icon_path),
        "--distpath",
        str(pyinstaller_dist),
        "--workpath",
        str(pyinstaller_work),
        "--specpath",
        str(pyinstaller_spec),
    ]
    for module_name in MAIN_RUNTIME_HIDDEN_IMPORTS:
        command.extend(["--hidden-import", module_name])
    for package_name in MAIN_RUNTIME_COLLECT_DATA_PACKAGES:
        command.extend(["--collect-data", package_name])
    for package_name in MAIN_RUNTIME_COLLECT_ALL_PACKAGES:
        command.extend(["--collect-all", package_name])
    for module_name in MAIN_RUNTIME_EXCLUDED_MODULES:
        command.extend(["--exclude-module", module_name])
    ship_version_dir = output_dir / "ship" / "app" / "versions" / bundle_version
    for rel in REQUIRED_VERSION_FILES:
        src_file = (ship_version_dir / rel).resolve()
        if not src_file.is_file():
            raise RuntimeError(f"Portable build cannot embed missing ship file: {src_file}")
        data_dest = "baluffo_embed/src" if rel.startswith("src/") else "baluffo_embed"
        command.extend(["--add-data", f"{src_file}{os.pathsep}{data_dest}"])
    # Ship desktop app is a package; PyInstaller needs a real script entrypoint.
    command.append(str(ROOT / "src" / "ship" / "desktop_app" / "__main__.py"))
    subprocess.run(command, check=True, cwd=str(ROOT))
    built_dir = pyinstaller_dist / exe_name
    if not built_dir.exists():
        raise RuntimeError(f"PyInstaller output not found: {built_dir}")
    _copy_tree_contents(built_dir, output_dir)
    exe_path = output_dir / f"{exe_name}.exe"
    if not exe_path.exists():
        raise RuntimeError(f"Portable executable not found: {exe_path}")
    return exe_path


def run_helper_pyinstaller(output_dir: Path, *, icon_path: Path) -> Path:
    helper_dist = output_dir.parent / ".pyinstaller-helper-dist"
    helper_work = output_dir.parent / ".pyinstaller-helper-work"
    helper_spec = output_dir.parent / ".pyinstaller-helper-spec"
    for path in (helper_dist, helper_work, helper_spec):
        if path.exists():
            shutil.rmtree(path)
        path.mkdir(parents=True, exist_ok=True)
    command = [
        sys.executable,
        "-m",
        "PyInstaller",
        "--noconfirm",
        "--clean",
        "--onefile",
        "--windowed",
        "--name",
        "BaluffoUpdater",
        "--icon",
        str(icon_path),
        "--distpath",
        str(helper_dist),
        "--workpath",
        str(helper_work),
        "--specpath",
        str(helper_spec),
    ]
    for module_name in UPDATER_HELPER_HIDDEN_IMPORTS:
        command.extend(["--hidden-import", module_name])
    for package_name in UPDATER_HELPER_COLLECT_DATA_PACKAGES:
        command.extend(["--collect-data", package_name])
    command.append(str(ROOT / "src" / "ship" / "desktop_updater.py"))
    subprocess.run(command, check=True, cwd=str(ROOT))
    helper_exe = helper_dist / "BaluffoUpdater.exe"
    if not helper_exe.exists():
        raise RuntimeError(f"Updater helper executable not found: {helper_exe}")
    target = output_dir / "BaluffoUpdater.exe"
    shutil.copy2(helper_exe, target)
    return target


def create_zip(output_dir: Path, *, version: str) -> Path:
    archive_base = output_dir.parent / f"{output_dir.name}-{version}"
    archive_path = archive_base.with_suffix(".zip")
    if archive_path.exists():
        archive_path.unlink()
    built = shutil.make_archive(str(archive_base), "zip", root_dir=str(output_dir))
    return Path(built)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build dist/baluffo-portable executable wrapper.")
    parser.add_argument("--output-dir", default=str(DIST_DIR))
    parser.add_argument("--bundle-version", default=DEFAULT_BUNDLE_VERSION)
    parser.add_argument("--exe-name", default=DEFAULT_EXE_NAME)
    parser.add_argument("--icon", default="")
    parser.add_argument("--skip-zip", action="store_true")
    return parser.parse_args()


def main() -> int:
    ensure_required_python()
    args = parse_args()
    output_dir = Path(args.output_dir).expanduser().resolve()
    version = str(args.bundle_version).strip() or DEFAULT_BUNDLE_VERSION
    portable_root = build_portable_layout(output_dir, version)
    exe_name = str(args.exe_name).strip() or DEFAULT_EXE_NAME
    icon_path = resolve_icon_path(str(args.icon or ""))
    exe_path = run_pyinstaller(
        portable_root, exe_name=exe_name, icon_path=icon_path, bundle_version=version
    )
    helper_path = run_helper_pyinstaller(portable_root, icon_path=icon_path)
    print(f"Portable executable ready: {exe_path}")
    print(f"Updater helper ready: {helper_path}")
    print(f"Ship bundle root: {portable_root / 'ship'}")
    print(f"Executable icon: {icon_path}")
    if not args.skip_zip:
        archive = create_zip(portable_root, version=version)
        print(f"Portable archive: {archive}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
