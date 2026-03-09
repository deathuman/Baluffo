#!/usr/bin/env python3
"""Build a Windows-first ship bundle for local Baluffo operations."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DIST_DIR = ROOT / "dist" / "baluffo-ship"
DEFAULT_BUNDLE_VERSION = "1.0.0"
APP_RUNTIME_FILES = (
    "admin.html",
    "index.html",
    "jobs.html",
    "saved.html",
    "styles.css",
    "theme.js",
    "admin-config.js",
    "app-local-data-client.js",
    "desktop-local-data-client.js",
    "local-data-client.js",
    "jobs-parsing-utils.js",
    "jobs-state.js",
    "saved-zip-utils.js",
    "README.md",
)
APP_RUNTIME_SCRIPTS = (
    "__init__.py",
    "admin_bridge.py",
    "contracts.py",
    "fetcher_metrics.py",
    "jobs_fetcher.py",
    "jobs_fetcher_registry.py",
    "pipeline_io.py",
    "source_discovery.py",
    "source_registry.py",
    "source_sync.py",
    "local_data_store.py",
    "discovery_seed_catalog.json",
)
PACKAGING_FILES = (
    "README.md",
    "github-app-sync-config.template.json",
)
APP_RUNTIME_DATA_FILES = (
    "jobs-unified-startup.json",
    "jobs-unified-light.json",
    "jobs-unified.json",
    "jobs-unified.csv",
    "jobs-fetch-report.json",
    "source-registry-active.json",
)
STARTUP_PREVIEW_LIMIT = 1400
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


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _state_payload(version: str) -> dict:
    return {
        "current_version": version,
        "previous_version": "",
        "last_update_status": "ready",
        "last_error_code": "",
        "updated_at": _iso_now(),
    }


def _manifest_payload(version: str, sha256: str) -> dict:
    return {
        "version": version,
        "artifact_url": "",
        "sha256": sha256,
        "signature": "",
        "min_updater_version": "1.0.0",
        "migration_plan": [],
        "rollback_allowed": False,
        "generated_at": _iso_now(),
    }


def _seed_runtime_data(data_dir: Path) -> None:
    from scripts.jobs_fetcher import DEFAULT_SOCIAL_CONFIG, DEFAULT_STUDIO_SOURCE_REGISTRY  # local import to keep script lightweight

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
        "social-sources-config.json": DEFAULT_SOCIAL_CONFIG,
        "admin-task-state.json": {},
        "admin-alert-state.json": {"schemaVersion": 1, "acked": {}, "updatedAt": ""},
        "admin-run-history.json": [],
    }
    for name, payload in payloads.items():
        _write_text(data_dir / name, json.dumps(payload, indent=2, ensure_ascii=False))


def _copy_app_version(version_dir: Path) -> None:
    # Static frontend/runtime assets.
    for rel in APP_RUNTIME_FILES:
        _copy_file(ROOT / rel, version_dir / rel)

    _copy_tree(ROOT / "frontend", version_dir / "frontend")
    for rel in APP_RUNTIME_SCRIPTS:
        _copy_file(ROOT / "scripts" / rel, version_dir / "scripts" / rel)
    _copy_file(ROOT / "scripts" / "ship" / "__init__.py", version_dir / "scripts" / "ship" / "__init__.py")
    for rel in PACKAGING_FILES:
        _copy_file(ROOT / "packaging" / rel, version_dir / "packaging" / rel)
    packaged_sync_config = ROOT / "packaging" / "github-app-sync-config.json"
    if packaged_sync_config.exists():
        _copy_file(packaged_sync_config, version_dir / "packaging" / "github-app-sync-config.json")

    for rel in APP_RUNTIME_DATA_FILES:
        src = ROOT / "data" / rel
        if not src.exists():
            continue
        _copy_file(src, version_dir / "data" / rel)
    _generate_startup_preview(version_dir / "data")


def _generate_startup_preview(data_dir: Path) -> None:
    light_path = data_dir / "jobs-unified-light.json"
    startup_path = data_dir / "jobs-unified-startup.json"
    if not light_path.exists():
        return
    try:
        payload = json.loads(light_path.read_text(encoding="utf-8"))
    except Exception:
        return
    rows = payload if isinstance(payload, list) else []
    startup_rows = rows[:STARTUP_PREVIEW_LIMIT]
    _write_text(startup_path, json.dumps(startup_rows, ensure_ascii=False))


def build_bundle(output_dir: Path, version: str) -> Path:
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    app_dir = output_dir / "app"
    versions_dir = app_dir / "versions"
    version_dir = versions_dir / version
    staging_dir = app_dir / "staging"
    tooling_dir = output_dir / "scripts" / "ship"
    logs_dir = output_dir / "logs"

    version_dir.mkdir(parents=True, exist_ok=True)
    staging_dir.mkdir(parents=True, exist_ok=True)
    tooling_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    _copy_app_version(version_dir)
    _copy_file(ROOT / "scripts" / "ship" / "update_manager.py", tooling_dir / "update_manager.py")
    _copy_file(ROOT / "scripts" / "ship" / "migrations.py", tooling_dir / "migrations.py")
    _copy_file(ROOT / "scripts" / "ship" / "runtime_launcher.py", tooling_dir / "runtime_launcher.py")

    # Launcher scripts + ship runbook.
    _copy_file(ROOT / "scripts" / "ship" / "run-bridge.ps1", output_dir / "run-bridge.ps1")
    _copy_file(ROOT / "scripts" / "ship" / "run-site.ps1", output_dir / "run-site.ps1")
    _copy_file(ROOT / "scripts" / "ship" / "run-all.ps1", output_dir / "run-all.ps1")
    _copy_file(ROOT / "scripts" / "ship" / "apply-update.ps1", output_dir / "apply-update.ps1")
    _copy_file(ROOT / "scripts" / "ship" / "recover-previous.ps1", output_dir / "recover-previous.ps1")
    _copy_file(ROOT / "scripts" / "ship" / "create-support-bundle.ps1", output_dir / "create-support-bundle.ps1")
    _copy_file(ROOT / "docs" / "ship-bundle-runbook.md", output_dir / "SHIP_BUNDLE_RUNBOOK.md")
    _copy_file(ROOT / "docs" / "update-manifest.schema.json", output_dir / "UPDATE_MANIFEST_SCHEMA.json")

    data_dir = output_dir / "data"
    _seed_runtime_data(data_dir)
    (data_dir / "backups").mkdir(parents=True, exist_ok=True)
    (data_dir / "migration-reports").mkdir(parents=True, exist_ok=True)

    _write_text(app_dir / "current.txt", f"{version}\n")
    _write_text(app_dir / "update-state.json", json.dumps(_state_payload(version), indent=2, ensure_ascii=False))

    # Hashing current version folder enables local integrity checks and a baseline manifest.
    version_hashes = {}
    for path in sorted(p for p in version_dir.rglob("*") if p.is_file()):
        rel = str(path.relative_to(version_dir)).replace("\\", "/")
        version_hashes[rel] = _hash_file(path)
    _write_text(app_dir / "version-hashes.json", json.dumps({"version": version, "files": version_hashes}, indent=2, ensure_ascii=False))
    aggregate_digest = hashlib.sha256(json.dumps(version_hashes, sort_keys=True).encode("utf-8")).hexdigest()
    _write_text(app_dir / "update-manifest.json", json.dumps(_manifest_payload(version, aggregate_digest), indent=2, ensure_ascii=False))
    return output_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build dist/baluffo-ship bundle.")
    parser.add_argument("--output-dir", default=str(DIST_DIR))
    parser.add_argument("--bundle-version", default=DEFAULT_BUNDLE_VERSION)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    version = str(args.bundle_version).strip() or DEFAULT_BUNDLE_VERSION
    out = build_bundle(Path(args.output_dir).expanduser().resolve(), version)
    print(f"Ship bundle ready: {out}")
    print(f"Launchers: {out / 'run-bridge.ps1'} | {out / 'run-site.ps1'} | {out / 'run-all.ps1'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
