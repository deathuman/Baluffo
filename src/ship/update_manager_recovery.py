from __future__ import annotations

"""Recovery, startup check, and support bundle helpers for ship updates."""

from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zipfile import ZIP_DEFLATED, ZipFile

from .update_manager_bootstrap import repair_version_from_runtime_bootstrap
from .update_manager_paths import CURRENT_NAME, LOG_NAME, ShipPaths
from .update_manager_state import (
    _list_healthy_version_names,
    _prefer_higher_semver,
    ensure_state,
    log_event,
    write_state,
    write_text_atomic,
)
from .update_manager_validation import health_check_version, validate_data_dir


def recover_previous(root: Path) -> dict[str, Any]:
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


def startup_check(root: Path, data_dir: Path) -> dict[str, Any]:
    paths = ShipPaths.from_root(root.resolve(), data_dir=data_dir)
    state = ensure_state(paths)
    validate_data_dir(paths, data_dir)

    current = paths.current.read_text(encoding="utf-8").strip()
    if not current:
        raise RuntimeError("Current pointer is empty.")
    current_dir = paths.versions / current
    ok, error = health_check_version(current_dir)
    if ok:
        return {"ok": True, "current_version": current}

    restored = repair_version_from_runtime_bootstrap(paths, current_dir, current)
    if restored:
        ok_boot, err_boot = health_check_version(current_dir)
        if ok_boot:
            log_event(
                paths,
                "startup_runtime_bootstrap_repair",
                {"version": current, "files_restored": restored},
            )
            write_state(paths, state, status="ready", error="")
            return {"ok": True, "current_version": current, "bootstrap_repair": restored}
        error = f"{error}; after bootstrap repair: {err_boot}"

    previous = str(state.get("previous_version") or "").strip()
    previous_dir = paths.versions / previous if previous else None
    if previous_dir and previous_dir.exists():
        prev_ok, prev_err = health_check_version(previous_dir)
        if prev_ok:
            write_text_atomic(paths.current, f"{previous}\n")
            state["current_version"] = previous
            state["previous_version"] = current
            write_state(paths, state, status="auto_rolled_back", error=error)
            log_event(
                paths, "startup_auto_rollback", {"from": current, "to": previous, "error": error}
            )
            return {"ok": True, "current_version": previous, "rolled_back": True}
        error = f"{error}; rollback target unhealthy: {prev_err}"

    healthy = _list_healthy_version_names(paths)
    replacement = _prefer_higher_semver(healthy)
    if replacement:
        write_text_atomic(paths.current, f"{replacement}\n")
        state["current_version"] = replacement
        state["previous_version"] = current
        write_state(paths, state, status="auto_repaired_version", error=error)
        log_event(
            paths,
            "startup_auto_select_version",
            {"from": current, "to": replacement, "error": error},
        )
        return {"ok": True, "current_version": replacement, "repaired_pointer": True}

    raise RuntimeError(
        f"Startup check failed and no valid rollback target exists: {error}. "
        f"Active version dir: {current_dir.resolve()}. "
        "Replace the ``ship`` folder from a full portable build, or run recover-previous.ps1 "
        "if a prior version is still under app/versions."
    )


def create_support_bundle(root: Path, output: Path | None = None) -> Path:
    paths = ShipPaths.from_root(root.resolve())
    state = ensure_state(paths)
    state_path = Path(str(state.get("__state_path") or paths.state))
    timestamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    out = output or (paths.root / "support" / f"support-bundle-{timestamp}.zip")
    out.parent.mkdir(parents=True, exist_ok=True)
    targets = [
        state_path,
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
