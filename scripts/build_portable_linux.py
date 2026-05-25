#!/usr/bin/env python3
"""Build a Linux AppImage from the Baluffo ship bundle."""

from __future__ import annotations

import argparse
import hashlib
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
DIST_DIR = ROOT / "dist"
APPIMAGE_TOOL_DIR = ROOT / "_out" / "appimagetool"
APPIMAGE_TOOL_URL = (
    "https://github.com/AppImage/appimagetool/releases/download/continuous/"
    "appimagetool-x86_64.AppImage"
)
DEFAULT_EXE_NAME = "baluffo"
DEFAULT_ICON_PATH = ROOT / "packaging" / "baluffo.png"
PYINSTALLER_HIDDEN_IMPORTS = [
    "encodings.idna",
    "sqlite3",
    "tkinter",
    "tkinter.ttk",
    "src.admin_bridge",
    "src.app_version",
    "src.baluffo_config",
    "src.contracts",
    "src.exceptions",
    "src.fetcher_metrics",
    "src.jobs_fetcher",
    "src.jobs_fetcher_registry",
    "src.local_data_store",
    "src.local_data_store_attachments",
    "src.local_data_store_backup",
    "src.local_data_store_profiles",
    "src.local_data_store_saved_jobs",
    "src.local_data_store_shared",
    "src.local_data_store_tracking",
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
    "src.source_sync",
    "src.source_sync_config",
    "src.source_sync_crypto",
    "src.source_sync_runtime",
    "src.source_sync_snapshot",
]
PYINSTALLER_EXCLUDE_MODULES = [
    "pytest",
    "_pytest",
    "hypothesis",
    "hypothesis.strategies",
    "hamcrest",
]
PYINSTALLER_COLLECT_DATA = ["src.storage"]


def _hash_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            chunk = fh.read(1 << 20)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def _ensure_appimagetool() -> Path:
    APPIMAGE_TOOL_DIR.mkdir(parents=True, exist_ok=True)
    tool_path = APPIMAGE_TOOL_DIR / "appimagetool-x86_64.AppImage"
    if tool_path.exists():
        tool_path.chmod(0o755)
        return tool_path
    print("Downloading appimagetool...")
    subprocess.run(
        ["curl", "-fLo", str(tool_path), APPIMAGE_TOOL_URL],
        check=True,
        timeout=120,
    )
    tool_path.chmod(0o755)
    return tool_path


def _ensure_icon() -> Path | None:
    icon_path = DEFAULT_ICON_PATH
    if icon_path.exists():
        return icon_path
    ico_path = ROOT / "favicon.ico"
    if not ico_path.exists():
        print("Warning: No icon found. Skipping AppImage icon.")
        return None
    try:
        from PIL import Image
    except ImportError:
        print(
            "Warning: PIL not available, skipping icon conversion."
            " Install PIL to embed icon in AppImage."
        )
        return None
    img = Image.open(ico_path)
    icon_path.parent.mkdir(parents=True, exist_ok=True)
    img.save(icon_path, "PNG")
    return icon_path


def build_ship_bundle(output_dir: Path, version: str) -> Path:
    from scripts.build_ship_bundle import build_bundle

    bundle_dir = output_dir / "ship"
    return build_bundle(bundle_dir, version)


def run_pyinstaller(output_dir: Path, exe_name: str) -> Path:
    icon_path = _ensure_icon()
    work_dir = output_dir / ".pyinstaller-work"
    dist_dir = output_dir / ".pyinstaller-dist"
    spec_dir = output_dir / ".pyinstaller-spec"

    command = [
        sys.executable,
        "-m",
        "PyInstaller",
        "--noconfirm",
        "--clean",
        "--onedir",
        "--name",
        exe_name,
        "--icon",
        str(icon_path),
        f"--distpath={dist_dir}",
        f"--workpath={work_dir}",
        f"--specpath={spec_dir}",
    ]

    for imp in PYINSTALLER_HIDDEN_IMPORTS:
        command.extend(["--hidden-import", imp])

    for pkg in PYINSTALLER_COLLECT_DATA:
        command.extend(["--collect-data", pkg])

    for mod in PYINSTALLER_EXCLUDE_MODULES:
        command.extend(["--exclude-module", mod])

    command.append(str(ROOT / "src" / "ship" / "desktop_app" / "__main__.py"))

    print(f"Running PyInstaller: {' '.join(command[:20])}...")
    subprocess.run(command, cwd=output_dir, check=True)

    built_dir = dist_dir / exe_name
    if not built_dir.exists():
        raise RuntimeError(f"PyInstaller output missing: {built_dir}")

    ship_src = output_dir / "ship"
    ship_dst = built_dir / "ship"
    if ship_src.exists():
        if ship_dst.exists():
            shutil.rmtree(ship_dst)
        shutil.copytree(ship_src, ship_dst)

    return built_dir


def build_appdir(output_dir: Path, built_dir: Path, exe_name: str) -> Path:
    appdir = output_dir / f"{exe_name}.AppDir"
    if appdir.exists():
        shutil.rmtree(appdir)

    def _copy_tree(src: Path, dst: Path) -> None:
        dst.mkdir(parents=True, exist_ok=True)
        for item in src.iterdir():
            dest = dst / item.name
            if item.is_dir():
                _copy_tree(item, dest)
            else:
                shutil.copy2(item, dest)

    _copy_tree(built_dir, appdir)

    apprun_src = ROOT / "packaging" / "AppRun"
    desktop_src = ROOT / "packaging" / "baluffo.desktop"
    icon_src = _ensure_icon()
    icon_dest = appdir / "baluffo.png"

    shutil.copy2(apprun_src, appdir / "AppRun")
    (appdir / "AppRun").chmod(0o755)
    shutil.copy2(desktop_src, appdir)
    if icon_src is not None:
        shutil.copy2(icon_src, appdir / "baluffo.png")

    return appdir


def build_appimage(appdir: Path, output_dir: Path, version: str) -> Path:
    appimagetool = _ensure_appimagetool()
    appimage_name = f"Baluffo-{version}-x86_64.AppImage"
    output_path = output_dir / appimage_name

    subprocess.run(
        [str(appimagetool), str(appdir), str(output_path)],
        cwd=output_dir,
        check=True,
        timeout=300,
    )
    return output_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Linux AppImage.")
    parser.add_argument("--output-dir", default=str(DIST_DIR))
    parser.add_argument("--bundle-version", default="")
    parser.add_argument("--exe-name", default=DEFAULT_EXE_NAME)
    parser.add_argument(
        "--skip-appimage",
        action="store_true",
        help="Skip AppImage packaging (dev: PyInstaller only)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_dir = Path(args.output_dir).expanduser().resolve()
    exe_name = str(args.exe_name).strip() or DEFAULT_EXE_NAME

    version = str(args.bundle_version).strip()
    if not version:
        from src.app_version import APP_VERSION

        version = APP_VERSION or "0.0.0"

    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Building ship bundle (version {version})...")
    build_ship_bundle(output_dir, version)

    print(f"Running PyInstaller (target: {exe_name})...")
    built_dir = run_pyinstaller(output_dir, exe_name)

    if args.skip_appimage:
        print(f"PyInstaller output ready: {built_dir}")
        return 0

    print("Assembling AppDir...")
    appdir = build_appdir(output_dir, built_dir, exe_name)

    print("Building AppImage...")
    appimage = build_appimage(appdir, output_dir, version)
    print(f"AppImage ready: {appimage}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
