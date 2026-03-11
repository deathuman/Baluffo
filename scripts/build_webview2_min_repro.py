#!/usr/bin/env python3
"""Build a standalone packaged executable for the minimal pywebview/WebView2 repro."""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DIST_DIR = ROOT / "dist" / "webview2-min-repro"
DEFAULT_EXE_NAME = "WebView2MinRepro"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_portable_exe import resolve_icon_path


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


def run_pyinstaller(output_dir: Path, *, exe_name: str, icon_path: Path) -> Path:
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
        str(ROOT / "scripts" / "webview2_min_repro.py"),
    ]
    subprocess.run(command, check=True, cwd=str(ROOT))
    built_dir = pyinstaller_dist / exe_name
    if not built_dir.exists():
        raise RuntimeError(f"PyInstaller output not found: {built_dir}")
    _copy_tree_contents(built_dir, output_dir)
    exe_path = output_dir / f"{exe_name}.exe"
    if not exe_path.exists():
        raise RuntimeError(f"Repro executable not found: {exe_path}")
    return exe_path


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the standalone WebView2 minimal repro executable.")
    parser.add_argument("--output-dir", default=str(DIST_DIR))
    parser.add_argument("--exe-name", default=DEFAULT_EXE_NAME)
    parser.add_argument("--icon", default="")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    output_dir = Path(args.output_dir).expanduser().resolve()
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    exe_name = str(args.exe_name).strip() or DEFAULT_EXE_NAME
    icon_path = resolve_icon_path(output_dir, exe_name=exe_name, icon_arg=str(args.icon or ""))
    exe_path = run_pyinstaller(output_dir, exe_name=exe_name, icon_path=icon_path)
    print(f"Minimal repro executable ready: {exe_path}")
    print(f"Artifacts root: {output_dir}")
    print(f"Executable icon: {icon_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
