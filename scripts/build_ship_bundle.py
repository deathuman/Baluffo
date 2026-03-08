#!/usr/bin/env python3
"""Build a Windows-first ship bundle for local Baluffo operations."""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DIST_DIR = ROOT / "dist" / "baluffo-ship"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _copy_file(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


def _copy_tree(src: Path, dst: Path) -> None:
    if dst.exists():
        shutil.rmtree(dst)
    shutil.copytree(src, dst)


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _seed_runtime_data(data_dir: Path) -> None:
    from scripts.jobs_fetcher import DEFAULT_STUDIO_SOURCE_REGISTRY  # local import to keep script lightweight

    data_dir.mkdir(parents=True, exist_ok=True)
    payloads = {
        "source-registry-active.json": DEFAULT_STUDIO_SOURCE_REGISTRY,
        "source-registry-pending.json": [],
        "source-registry-rejected.json": [],
        "source-discovery-candidates.json": [],
        "source-discovery-report.json": {"summary": {}, "candidates": [], "failures": []},
        "source-approval-state.json": {"approvedSinceLastRun": 0},
        "jobs-fetch-report.json": {"summary": {}, "sources": [], "runtime": {}, "outputs": {}},
        "jobs-fetch-tasks.json": {"summary": {}, "tasks": [], "outputs": {}},
        "jobs-source-state.json": {"schemaVersion": 1, "updatedAt": "", "sources": {}},
        "jobs-success-cache.json": {"updatedAt": "", "successfulSources": []},
        "admin-task-state.json": {},
        "admin-alert-state.json": {"schemaVersion": 1, "acked": {}, "updatedAt": ""},
        "admin-run-history.json": [],
    }
    for name, payload in payloads.items():
        _write_text(data_dir / name, json.dumps(payload, indent=2, ensure_ascii=False))


def build_bundle(output_dir: Path) -> Path:
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Static frontend/runtime assets.
    for rel in (
        "admin.html",
        "index.html",
        "jobs.html",
        "saved.html",
        "styles.css",
        "theme.js",
        "admin-config.js",
        "local-data-client.js",
        "jobs-parsing-utils.js",
        "jobs-state.js",
        "saved-zip-utils.js",
        "package.json",
        "package-lock.json",
        "README.md",
        "LOCAL_SETUP.md",
    ):
        _copy_file(ROOT / rel, output_dir / rel)

    _copy_tree(ROOT / "frontend", output_dir / "frontend")
    _copy_tree(ROOT / "scripts", output_dir / "scripts")

    # Launcher scripts + ship runbook.
    _copy_file(ROOT / "scripts" / "ship" / "run-bridge.ps1", output_dir / "run-bridge.ps1")
    _copy_file(ROOT / "scripts" / "ship" / "run-site.ps1", output_dir / "run-site.ps1")
    _copy_file(ROOT / "scripts" / "ship" / "run-all.ps1", output_dir / "run-all.ps1")
    _copy_file(ROOT / "docs" / "ship-bundle-runbook.md", output_dir / "SHIP_BUNDLE_RUNBOOK.md")

    _seed_runtime_data(output_dir / "data")
    return output_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build dist/baluffo-ship bundle.")
    parser.add_argument("--output-dir", default=str(DIST_DIR))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    out = build_bundle(Path(args.output_dir).expanduser().resolve())
    print(f"Ship bundle ready: {out}")
    print(f"Launchers: {out / 'run-bridge.ps1'} | {out / 'run-site.ps1'} | {out / 'run-all.ps1'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
