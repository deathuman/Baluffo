from __future__ import annotations

import json
import os
import shutil
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

APP_NAME = "Baluffo"
MIGRATION_REPORT_FILE = "windows-user-data-migration.json"


def _env_path(env_map: Mapping[str, str], key: str) -> Path | None:
    raw = str(env_map.get(key) or "").strip()
    return Path(raw).expanduser() if raw else None


def _home_dir(env_map: Mapping[str, str]) -> Path:
    return _env_path(env_map, "USERPROFILE") or Path.home()


def windows_roaming_app_data_dir(env_map: Mapping[str, str] | None = None) -> Path:
    env = os.environ if env_map is None else env_map
    base = _env_path(env, "APPDATA") or (_home_dir(env) / "AppData" / "Roaming")
    return (base / APP_NAME).expanduser()


def windows_local_app_data_dir(env_map: Mapping[str, str] | None = None) -> Path:
    env = os.environ if env_map is None else env_map
    base = _env_path(env, "LOCALAPPDATA") or (_home_dir(env) / "AppData" / "Local")
    return (base / APP_NAME).expanduser()


def windows_cache_dir(env_map: Mapping[str, str] | None = None) -> Path:
    return windows_local_app_data_dir(env_map) / "cache"


def default_windows_packaged_data_dir(env_map: Mapping[str, str] | None = None) -> Path:
    return windows_roaming_app_data_dir(env_map)


def windows_user_data_migration_report_path(data_dir: Path) -> Path:
    return Path(data_dir).expanduser().resolve() / "migration-reports" / MIGRATION_REPORT_FILE


def _path_is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def _write_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp_path.replace(path)


def _completed_migration_report_exists(path: Path) -> bool:
    if not path.exists():
        return False
    try:
        existing_report = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    return isinstance(existing_report, dict) and bool(existing_report.get("completed"))


def _legacy_path_has_packaged_smoke_marker(path: Path) -> bool:
    return any(str(part).strip().lower() == "packaged-desktop-smoke" for part in path.parts)


def _legacy_profiles_include_rehearsal_user(legacy: Path) -> bool:
    profiles_path = legacy / "local-user-data" / "profiles.json"
    try:
        raw = json.loads(profiles_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    rows = raw if isinstance(raw, list) else []
    for row in rows:
        if not isinstance(row, dict):
            continue
        uid = str(row.get("id") or "").strip().lower()
        name = str(row.get("name") or "").strip().lower()
        if uid == "local_packaged_update_rehearsal" or name == "packaged update rehearsal":
            return True
    return False


def _target_is_default_windows_packaged_data_dir(
    target: Path, env_map: Mapping[str, str] | None
) -> bool:
    try:
        return target == default_windows_packaged_data_dir(env_map).resolve()
    except OSError:
        return False


def _should_skip_packaged_smoke_migration(
    legacy: Path,
    target: Path,
    env_map: Mapping[str, str] | None,
) -> bool:
    if not _target_is_default_windows_packaged_data_dir(target, env_map):
        return False
    return _legacy_path_has_packaged_smoke_marker(
        legacy
    ) or _legacy_profiles_include_rehearsal_user(legacy)


def migrate_legacy_windows_user_data(
    legacy_data_dir: Path,
    target_data_dir: Path,
    *,
    now: datetime | None = None,
    env_map: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    legacy = Path(legacy_data_dir).expanduser().resolve()
    target = Path(target_data_dir).expanduser().resolve()
    report_path = windows_user_data_migration_report_path(target)
    if _completed_migration_report_exists(report_path):
        return {
            "status": "already_migrated",
            "legacyDataDir": str(legacy),
            "targetDataDir": str(target),
            "reportPath": str(report_path),
        }

    timestamp = (now or datetime.now(UTC)).astimezone(UTC).isoformat()
    report: dict[str, Any] = {
        "status": "",
        "completed": False,
        "createdAt": timestamp,
        "legacyDataDir": str(legacy),
        "targetDataDir": str(target),
        "copiedFiles": 0,
        "skippedExistingFiles": 0,
        "skippedTargetNestedFiles": 0,
        "conflicts": [],
    }

    target.mkdir(parents=True, exist_ok=True)
    if _should_skip_packaged_smoke_migration(legacy, target, env_map):
        report["status"] = "skipped_packaged_smoke_rehearsal"
        report["completed"] = True
        _write_report(report_path, report)
        return report
    if legacy == target:
        report["status"] = "same_path"
        report["completed"] = True
        _write_report(report_path, report)
        return report
    if not legacy.is_dir():
        report["status"] = "legacy_missing"
        report["completed"] = True
        _write_report(report_path, report)
        return report

    conflict_limit = 50
    for source in legacy.rglob("*"):
        if not source.is_file():
            continue
        resolved_source = source.resolve()
        if _path_is_relative_to(resolved_source, target):
            report["skippedTargetNestedFiles"] = int(report["skippedTargetNestedFiles"]) + 1
            continue
        rel = source.relative_to(legacy)
        destination = target / rel
        if destination.exists():
            report["skippedExistingFiles"] = int(report["skippedExistingFiles"]) + 1
            conflicts = report["conflicts"]
            if isinstance(conflicts, list) and len(conflicts) < conflict_limit:
                conflicts.append(str(rel))
            continue
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(resolved_source, destination)
        report["copiedFiles"] = int(report["copiedFiles"]) + 1

    report["status"] = "copied_with_conflicts" if int(report["skippedExistingFiles"]) else "copied"
    report["completed"] = True
    _write_report(report_path, report)
    return report
