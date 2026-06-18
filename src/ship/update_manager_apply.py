from __future__ import annotations

"""Apply/update orchestration for ship bundles."""

import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zipfile import ZIP_DEFLATED, ZipFile

from src.ship.migrations import resolve_migrations

from .update_manager_bootstrap import refresh_runtime_bootstrap
from .update_manager_paths import ShipPaths
from .update_manager_state import (
    ensure_state,
    log_event,
    read_json,
    write_json_atomic,
    write_state,
    write_text_atomic,
)
from .update_manager_validation import (
    health_check_version,
    is_downgrade,
    validate_manifest,
    verify_artifact,
)


def create_data_backup(paths: ShipPaths) -> Path:
    timestamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    backup_path = paths.backups / f"data-backup-{timestamp}.zip"
    paths.backups.mkdir(parents=True, exist_ok=True)
    with ZipFile(backup_path, "w", compression=ZIP_DEFLATED) as archive:
        for path in sorted(paths.data.rglob("*")):
            if not path.is_file():
                continue
            if paths.backups in path.parents:
                continue
            rel = path.relative_to(paths.data)
            archive.write(path, rel.as_posix())
    return backup_path


def restore_data_backup(paths: ShipPaths, backup_path: Path) -> None:
    if not backup_path.exists():
        raise FileNotFoundError(f"Backup file not found: {backup_path}")
    with ZipFile(backup_path, "r") as archive:
        archive.extractall(paths.data)


def run_migrations(paths: ShipPaths, migration_names: Any, backup_ref: Path) -> dict[str, Any]:
    report: dict[str, Any] = {"applied": [], "verified": [], "rolled_back": []}
    migrations = resolve_migrations(migration_names)
    for migration in migrations:
        result = migration.apply(paths.data)
        report["applied"].append({"name": migration.name, "ok": result.ok, "detail": result.detail})
        if not result.ok:
            raise RuntimeError(f"Migration apply failed: {migration.name}")
        verify_result = migration.verify(paths.data)
        report["verified"].append(
            {"name": migration.name, "ok": verify_result.ok, "detail": verify_result.detail}
        )
        if not verify_result.ok:
            raise RuntimeError(f"Migration verify failed: {migration.name}")
    return report


def rollback_migrations(paths: ShipPaths, migration_names: Any, backup_ref: Path) -> dict[str, Any]:
    report: dict[str, Any] = {"rolled_back": []}
    for migration in reversed(resolve_migrations(migration_names)):
        result = migration.rollback(paths.data, backup_ref)
        report["rolled_back"].append(
            {"name": migration.name, "ok": result.ok, "detail": result.detail}
        )
    return report


def locate_staged_version_dir(stage_root: Path, version: str) -> Path:
    candidates = list(stage_root.rglob(f"app/versions/{version}"))
    if len(candidates) != 1:
        raise RuntimeError(
            f"Expected one app/versions/{version} in artifact, found {len(candidates)}."
        )
    return candidates[0]


def apply_update(
    root: Path, bundle_zip: Path, manifest_path: Path, signing_key: str
) -> dict[str, Any]:
    paths = ShipPaths.from_root(root.resolve())
    state = ensure_state(paths)
    manifest = read_json(manifest_path)
    validate_manifest(manifest)
    verify_artifact(bundle_zip, manifest, signing_key)

    current_version = str(state.get("current_version") or "").strip()
    next_version = str(manifest["version"]).strip()
    if not current_version:
        raise RuntimeError("Current version missing in state.")
    if next_version == current_version:
        raise RuntimeError("Update target matches current version.")
    if is_downgrade(current_version, next_version) and not bool(manifest.get("rollback_allowed")):
        raise RuntimeError("Downgrade rejected by manifest policy.")

    state["previous_version"] = current_version
    write_state(paths, state, status="updating", error="")
    log_event(paths, "update_started", {"from": current_version, "to": next_version})

    stage_root = paths.staging / next_version
    if stage_root.exists():
        shutil.rmtree(stage_root)
    stage_root.mkdir(parents=True, exist_ok=True)

    backup_ref = create_data_backup(paths)
    migration_report: dict[str, Any] = {
        "backup_ref": str(backup_ref),
        "applied": [],
        "verified": [],
        "rolled_back": [],
    }
    target_dir = paths.versions / next_version
    if target_dir.exists():
        shutil.rmtree(target_dir)

    try:
        with ZipFile(bundle_zip, "r") as archive:
            archive.extractall(stage_root)

        staged_version = locate_staged_version_dir(stage_root, next_version)
        shutil.copytree(staged_version, target_dir)

        migration_report.update(
            run_migrations(paths, manifest.get("migration_plan") or [], backup_ref)
        )
        ok, health_error = health_check_version(target_dir)
        if not ok:
            raise RuntimeError(f"Health check failed: {health_error}")

        write_text_atomic(paths.current, f"{next_version}\n")
        state["current_version"] = next_version
        write_state(paths, state, status="ready", error="")
        refresh_runtime_bootstrap(paths, target_dir, version_name=next_version)
        log_event(paths, "update_succeeded", {"from": current_version, "to": next_version})
    except BaseException as exc:
        # Treat apply as a transactional cleanup boundary, then re-raise every failure.
        rollback_report = rollback_migrations(
            paths, manifest.get("migration_plan") or [], backup_ref
        )
        migration_report["rolled_back"] = rollback_report["rolled_back"]
        restore_data_backup(paths, backup_ref)
        if target_dir.exists():
            shutil.rmtree(target_dir)
        write_text_atomic(paths.current, f"{current_version}\n")
        state["current_version"] = current_version
        write_state(paths, state, status="failed", error=str(exc))
        log_event(
            paths, "update_failed", {"from": current_version, "to": next_version, "error": str(exc)}
        )
        raise
    finally:
        report_path = (
            paths.migration_reports
            / f"{datetime.now(UTC).strftime('%Y%m%d-%H%M%S')}-{next_version}.json"
        )
        write_json_atomic(report_path, migration_report)
        if stage_root.exists():
            shutil.rmtree(stage_root)

    return {"ok": True, "current_version": next_version, "previous_version": current_version}
