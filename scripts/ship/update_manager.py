#!/usr/bin/env python3
"""Atomic updater and recovery manager for zip-first Baluffo ship bundles."""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import hmac
import json
import os
import shutil
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple
from zipfile import ZIP_DEFLATED, ZipFile

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.ship.migrations import resolve_migrations

UPDATER_VERSION = "1.0.0"
STATE_NAME = "update-state.json"
CURRENT_NAME = "current.txt"
LOG_NAME = "update-events.jsonl"
REQUIRED_VERSION_FILES = ("scripts/admin_bridge.py", "index.html", "jobs.html", "saved.html")


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path, fallback: Dict[str, Any] | None = None) -> Dict[str, Any]:
    if not path.exists():
        return dict(fallback or {})
    return json.loads(path.read_text(encoding="utf-8"))


def _write_atomic(path: Path, payload: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    tmp.write_text(payload, encoding="utf-8")
    try:
        for attempt in range(6):
            try:
                os.replace(tmp, path)
                return
            except PermissionError:
                if attempt >= 5:
                    raise
                time.sleep(0.03 * (attempt + 1))
    finally:
        if tmp.exists():
            with contextlib.suppress(OSError):
                tmp.unlink()


def write_json_atomic(path: Path, payload: Dict[str, Any]) -> None:
    _write_atomic(path, json.dumps(payload, indent=2, ensure_ascii=False))


def write_text_atomic(path: Path, text: str) -> None:
    _write_atomic(path, text)


def compute_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _parse_version(v: str) -> Tuple[int, ...]:
    numbers: List[int] = []
    for part in str(v or "").split("."):
        if part.isdigit():
            numbers.append(int(part))
        else:
            break
    return tuple(numbers)


def is_downgrade(current: str, target: str) -> bool:
    left = _parse_version(current)
    right = _parse_version(target)
    if left and right:
        return right < left
    return str(target) < str(current)


def _resolve_active_root(root: Path) -> Path:
    current = (root / "app" / CURRENT_NAME).read_text(encoding="utf-8").strip()
    return root / "app" / "versions" / current


@dataclass(frozen=True)
class ShipPaths:
    root: Path
    app: Path
    versions: Path
    staging: Path
    state: Path
    current: Path
    data: Path
    backups: Path
    migration_reports: Path
    logs: Path

    @staticmethod
    def from_root(root: Path) -> "ShipPaths":
        app = root / "app"
        data = root / "data"
        return ShipPaths(
            root=root,
            app=app,
            versions=app / "versions",
            staging=app / "staging",
            state=app / STATE_NAME,
            current=app / CURRENT_NAME,
            data=data,
            backups=data / "backups",
            migration_reports=data / "migration-reports",
            logs=root / "logs",
        )


def ensure_state(paths: ShipPaths) -> Dict[str, Any]:
    if not paths.current.exists():
        raise RuntimeError(f"Missing current pointer file: {paths.current}")
    current_version = paths.current.read_text(encoding="utf-8").strip()
    if not current_version:
        raise RuntimeError("Current pointer is empty.")
    state = read_json(
        paths.state,
        {
            "current_version": current_version,
            "previous_version": "",
            "last_update_status": "ready",
            "last_error_code": "",
            "updated_at": iso_now(),
        },
    )
    state.setdefault("current_version", current_version)
    state.setdefault("previous_version", "")
    state.setdefault("last_update_status", "ready")
    state.setdefault("last_error_code", "")
    state.setdefault("updated_at", iso_now())
    write_json_atomic(paths.state, state)
    return state


def write_state(paths: ShipPaths, state: Dict[str, Any], *, status: str, error: str = "") -> None:
    state["last_update_status"] = status
    state["last_error_code"] = error
    state["updated_at"] = iso_now()
    write_json_atomic(paths.state, state)


def log_event(paths: ShipPaths, event: str, payload: Dict[str, Any]) -> None:
    paths.logs.mkdir(parents=True, exist_ok=True)
    record = {"time": iso_now(), "event": event, **payload}
    with (paths.logs / LOG_NAME).open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def validate_manifest(manifest: Dict[str, Any]) -> None:
    required = ("version", "artifact_url", "sha256", "signature", "min_updater_version", "migration_plan", "rollback_allowed")
    missing = [key for key in required if key not in manifest]
    if missing:
        raise ValueError(f"Manifest missing fields: {', '.join(missing)}")
    minimum = str(manifest.get("min_updater_version") or "").strip()
    if minimum and _parse_version(minimum) and _parse_version(UPDATER_VERSION):
        if _parse_version(minimum) > _parse_version(UPDATER_VERSION):
            raise ValueError("Updater is too old for this package.")
    elif minimum > UPDATER_VERSION:
        raise ValueError("Updater is too old for this package.")
    if not isinstance(manifest.get("migration_plan"), list):
        raise ValueError("Manifest migration_plan must be a list.")
    if not str(manifest.get("signature") or "").strip():
        raise ValueError("Manifest signature is required.")


def validate_data_dir(paths: ShipPaths, data_dir: Path) -> None:
    resolved_data = data_dir.resolve()
    resolved_versions = paths.versions.resolve()
    if resolved_versions == resolved_data or resolved_versions in resolved_data.parents:
        raise ValueError("User data directory must be outside app/versions.")


def verify_artifact(bundle_zip: Path, manifest: Dict[str, Any], signing_key: str) -> None:
    expected_hash = str(manifest["sha256"]).strip().lower()
    computed_hash = compute_sha256(bundle_zip).lower()
    if computed_hash != expected_hash:
        raise ValueError("Checksum mismatch for update artifact.")

    message = f"{manifest['version']}:{expected_hash}".encode("utf-8")
    expected_signature = hmac.new(signing_key.encode("utf-8"), message, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected_signature, str(manifest["signature"]).strip().lower()):
        raise ValueError("Signature verification failed.")


def health_check_version(version_dir: Path) -> Tuple[bool, str]:
    for rel in REQUIRED_VERSION_FILES:
        if not (version_dir / rel).exists():
            return False, f"missing_required_file:{rel}"
    return True, ""


def create_data_backup(paths: ShipPaths) -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
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


def run_migrations(paths: ShipPaths, migration_names: Iterable[str], backup_ref: Path) -> Dict[str, Any]:
    report: Dict[str, Any] = {"applied": [], "verified": [], "rolled_back": []}
    migrations = resolve_migrations(migration_names)
    for migration in migrations:
        result = migration.apply(paths.data)
        report["applied"].append({"name": migration.name, "ok": result.ok, "detail": result.detail})
        if not result.ok:
            raise RuntimeError(f"Migration apply failed: {migration.name}")
        verify_result = migration.verify(paths.data)
        report["verified"].append({"name": migration.name, "ok": verify_result.ok, "detail": verify_result.detail})
        if not verify_result.ok:
            raise RuntimeError(f"Migration verify failed: {migration.name}")
    return report


def rollback_migrations(paths: ShipPaths, migration_names: Iterable[str], backup_ref: Path) -> Dict[str, Any]:
    report: Dict[str, Any] = {"rolled_back": []}
    for migration in reversed(resolve_migrations(migration_names)):
        result = migration.rollback(paths.data, backup_ref)
        report["rolled_back"].append({"name": migration.name, "ok": result.ok, "detail": result.detail})
    return report


def locate_staged_version_dir(stage_root: Path, version: str) -> Path:
    candidates = list(stage_root.rglob(f"app/versions/{version}"))
    if len(candidates) != 1:
        raise RuntimeError(f"Expected one app/versions/{version} in artifact, found {len(candidates)}.")
    return candidates[0]


def apply_update(root: Path, bundle_zip: Path, manifest_path: Path, signing_key: str) -> Dict[str, Any]:
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
    migration_report: Dict[str, Any] = {"backup_ref": str(backup_ref), "applied": [], "verified": [], "rolled_back": []}
    target_dir = paths.versions / next_version
    if target_dir.exists():
        shutil.rmtree(target_dir)

    try:
        with ZipFile(bundle_zip, "r") as archive:
            archive.extractall(stage_root)

        staged_version = locate_staged_version_dir(stage_root, next_version)
        shutil.copytree(staged_version, target_dir)

        migration_report.update(run_migrations(paths, manifest.get("migration_plan") or [], backup_ref))
        ok, health_error = health_check_version(target_dir)
        if not ok:
            raise RuntimeError(f"Health check failed: {health_error}")

        write_text_atomic(paths.current, f"{next_version}\n")
        state["current_version"] = next_version
        write_state(paths, state, status="ready", error="")
        log_event(paths, "update_succeeded", {"from": current_version, "to": next_version})
    except Exception as exc:
        rollback_report = rollback_migrations(paths, manifest.get("migration_plan") or [], backup_ref)
        migration_report["rolled_back"] = rollback_report["rolled_back"]
        restore_data_backup(paths, backup_ref)
        if target_dir.exists():
            shutil.rmtree(target_dir)
        write_text_atomic(paths.current, f"{current_version}\n")
        state["current_version"] = current_version
        write_state(paths, state, status="failed", error=str(exc))
        log_event(paths, "update_failed", {"from": current_version, "to": next_version, "error": str(exc)})
        raise
    finally:
        report_path = paths.migration_reports / f"{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}-{next_version}.json"
        write_json_atomic(report_path, migration_report)
        if stage_root.exists():
            shutil.rmtree(stage_root)

    return {"ok": True, "current_version": next_version, "previous_version": current_version}


def recover_previous(root: Path) -> Dict[str, Any]:
    paths = ShipPaths.from_root(root.resolve())
    state = ensure_state(paths)
    current = str(state.get("current_version") or "").strip()
    previous = str(state.get("previous_version") or "").strip()
    if not previous:
        raise RuntimeError("No previous version available for recovery.")
    previous_dir = paths.versions / previous
    if not previous_dir.exists():
        raise RuntimeError(f"Previous version directory missing: {previous_dir}")

    write_text_atomic(paths.current, f"{previous}\n")
    state["current_version"] = previous
    state["previous_version"] = current
    write_state(paths, state, status="recovered", error="")
    log_event(paths, "manual_recover", {"from": current, "to": previous})
    return {"ok": True, "current_version": previous, "previous_version": current}


def startup_check(root: Path, data_dir: Path) -> Dict[str, Any]:
    paths = ShipPaths.from_root(root.resolve())
    state = ensure_state(paths)
    validate_data_dir(paths, data_dir)

    current = str(state.get("current_version") or "").strip()
    current_dir = paths.versions / current
    ok, error = health_check_version(current_dir)
    if ok:
        return {"ok": True, "current_version": current}

    previous = str(state.get("previous_version") or "").strip()
    if previous and (paths.versions / previous).exists():
        write_text_atomic(paths.current, f"{previous}\n")
        state["current_version"] = previous
        state["previous_version"] = current
        write_state(paths, state, status="auto_rolled_back", error=error)
        log_event(paths, "startup_auto_rollback", {"from": current, "to": previous, "error": error})
        return {"ok": True, "current_version": previous, "rolled_back": True}

    raise RuntimeError(f"Startup check failed and no valid rollback target exists: {error}")


def create_support_bundle(root: Path, output: Path | None = None) -> Path:
    paths = ShipPaths.from_root(root.resolve())
    ensure_state(paths)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    out = output or (paths.root / "support" / f"support-bundle-{timestamp}.zip")
    out.parent.mkdir(parents=True, exist_ok=True)
    targets = [
        paths.app / STATE_NAME,
        paths.app / CURRENT_NAME,
        paths.logs / LOG_NAME,
        paths.data / "admin-run-history.json",
    ]
    latest_report = sorted(paths.migration_reports.glob("*.json"))
    if latest_report:
        targets.append(latest_report[-1])
    with ZipFile(out, "w", compression=ZIP_DEFLATED) as archive:
        for path in targets:
            if path.exists() and path.is_file():
                archive.write(path, path.relative_to(paths.root).as_posix())
    return out


def sign_manifest(version: str, sha256: str, signing_key: str) -> str:
    message = f"{version}:{sha256}".encode("utf-8")
    return hmac.new(signing_key.encode("utf-8"), message, hashlib.sha256).hexdigest()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Baluffo ship update manager.")
    sub = parser.add_subparsers(dest="command", required=True)

    apply_parser = sub.add_parser("apply", help="Apply an update artifact atomically.")
    apply_parser.add_argument("--root", required=True)
    apply_parser.add_argument("--bundle-zip", required=True)
    apply_parser.add_argument("--manifest", required=True)
    apply_parser.add_argument("--signing-key", default=os.getenv("BALUFFO_UPDATE_SIGNING_KEY", ""))

    recover_parser = sub.add_parser("recover", help="Switch back to previous version.")
    recover_parser.add_argument("--root", required=True)

    check_parser = sub.add_parser("startup-check", help="Validate active version and data path before startup.")
    check_parser.add_argument("--root", required=True)
    check_parser.add_argument("--data-dir", required=True)

    support_parser = sub.add_parser("support-bundle", help="Build a support bundle for diagnostics.")
    support_parser.add_argument("--root", required=True)
    support_parser.add_argument("--output", default="")

    sign_parser = sub.add_parser("sign-manifest", help="Sign version/hash for manifest usage.")
    sign_parser.add_argument("--version", required=True)
    sign_parser.add_argument("--sha256", required=True)
    sign_parser.add_argument("--signing-key", default=os.getenv("BALUFFO_UPDATE_SIGNING_KEY", ""))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        if args.command == "apply":
            if not str(args.signing_key).strip():
                raise RuntimeError("Missing signing key. Use --signing-key or BALUFFO_UPDATE_SIGNING_KEY.")
            result = apply_update(
                Path(args.root),
                Path(args.bundle_zip),
                Path(args.manifest),
                str(args.signing_key),
            )
            print(json.dumps(result, indent=2))
            return 0

        if args.command == "recover":
            print(json.dumps(recover_previous(Path(args.root)), indent=2))
            return 0

        if args.command == "startup-check":
            print(json.dumps(startup_check(Path(args.root), Path(args.data_dir)), indent=2))
            return 0

        if args.command == "support-bundle":
            output = Path(args.output).expanduser().resolve() if str(args.output).strip() else None
            bundle = create_support_bundle(Path(args.root), output=output)
            print(json.dumps({"ok": True, "bundle": str(bundle)}, indent=2))
            return 0

        if args.command == "sign-manifest":
            if not str(args.signing_key).strip():
                raise RuntimeError("Missing signing key. Use --signing-key or BALUFFO_UPDATE_SIGNING_KEY.")
            print(sign_manifest(args.version, args.sha256, str(args.signing_key)))
            return 0

    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, indent=2))
        return 1
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
