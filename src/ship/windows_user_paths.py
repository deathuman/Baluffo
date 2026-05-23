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


def migrate_legacy_windows_user_data(
    legacy_data_dir: Path,
    target_data_dir: Path,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    legacy = Path(legacy_data_dir).expanduser().resolve()
    target = Path(target_data_dir).expanduser().resolve()
    report_path = windows_user_data_migration_report_path(target)
    if report_path.exists():
        try:
            existing_report = json.loads(report_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            existing_report = {}
        if bool(existing_report.get("completed")):
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
