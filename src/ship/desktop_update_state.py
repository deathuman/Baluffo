"""State and handoff helpers behind the root desktop-update facade."""

from __future__ import annotations

import contextlib
from pathlib import Path
from typing import Any

from src.app_version import get_app_version
from src.ship import desktop_update_constants as constants_mod
from src.ship.desktop_update_manifest import (
    DESKTOP_UPDATE_CHANNEL,
    DESKTOP_UPDATE_SCHEMA_VERSION,
    DESKTOP_UPDATER_VERSION,
)
from src.ship.desktop_update_shared import (
    compare_versions,
    compute_sha256,
    install_stage_label,
    iso_now,
    normalize_install_stage,
)

root: Any | None = None


def _root() -> Any:
    if root is None:
        raise RuntimeError("desktop_update_state.root is not configured")
    return root


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _as_str_dict(value: Any) -> dict[str, str]:
    return {str(key): str(item) for key, item in value.items()} if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _as_int(value: Any, default: int = 0) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return default
    return default


def default_status_payload(*, current_version: str | None = None) -> dict[str, Any]:
    return {
        "schemaVersion": DESKTOP_UPDATE_SCHEMA_VERSION,
        "channel": DESKTOP_UPDATE_CHANNEL,
        "currentVersion": str(current_version or get_app_version()),
        "latestVersion": "",
        "targetVersion": "",
        "updateAvailable": False,
        "availability": "unknown",
        "downloadState": "idle",
        "downloadedBytes": 0,
        "totalBytes": 0,
        "downloadPercent": 0,
        "installState": "idle",
        "releaseNotesUrl": "",
        "releaseNotesTitle": "",
        "releaseNotesBody": "",
        "releaseNotesPublishedAt": "",
        "releaseNotesHistory": [],
        "lastCheckedAt": "",
        "lastError": "",
        "blockedReason": "",
        "manifestPath": "",
        "downloadedZipPath": "",
        "helperVersion": DESKTOP_UPDATER_VERSION,
        "installStage": "idle",
        "installStageLabel": "",
        "helperUpdatedAt": "",
        "rollbackPath": "",
        "migrationBackupPath": "",
    }


def _load_credible_handoff_install_plan(paths: Any) -> dict[str, Any]:
    deps = _root()
    if not paths.handoff_request_path.exists():
        return {}
    try:
        plan = deps.validate_install_plan(deps.read_json(paths.install_plan_path, {}))
    except ValueError:
        return {}
    launcher_pid = int(plan.get("launcherPid") or 0)
    launcher_token = str(plan.get("launcherToken") or "").strip()
    session_root_raw = str(plan.get("desktopSessionRoot") or "").strip()
    if launcher_pid <= 0 or not launcher_token or not session_root_raw:
        return {}
    if not deps.pid_is_running(launcher_pid):
        return {}
    try:
        session_root = deps._resolve_runtime_path(session_root_raw)
    except Exception:
        return {}
    session_state = _as_dict(deps.read_desktop_session_state(session_root))
    if _as_int(session_state.get("launcherPid")) != launcher_pid:
        return {}
    if str(session_state.get("launcherToken") or "").strip() != launcher_token:
        return {}
    return dict(plan)


def _handoff_status_pending(status: dict[str, Any]) -> bool:
    return bool(
        str(status.get("installState") or "").strip().lower()
        in constants_mod.HANDOFF_PENDING_INSTALL_STATES
    )


def _apply_credible_handoff_status(status: dict[str, Any]) -> dict[str, Any]:
    next_status = dict(status or {})
    install_state = str(next_status.get("installState") or "").strip().lower()
    if install_state == "waiting_for_exit":
        next_status["installState"] = "waiting_for_exit"
        next_status["installStage"] = "waiting_for_exit"
    else:
        next_status["installState"] = "handoff_requested"
        next_status["installStage"] = "preparing"
    next_status["installStageLabel"] = install_stage_label(
        next_status.get("installState"),
        next_status.get("installStage"),
    )
    return next_status


def _reconcile_handoff_status(
    paths: Any,
    status: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], bool]:
    deps = _root()
    next_status = dict(status or {})
    install_state = str(next_status.get("installState") or "").strip().lower()
    credible_plan = deps._load_credible_handoff_install_plan(paths)
    if credible_plan:
        return deps._apply_credible_handoff_status(next_status), credible_plan, False
    handoff_marker_present = paths.handoff_request_path.exists()
    if (
        install_state not in constants_mod.HANDOFF_PENDING_INSTALL_STATES
        and not handoff_marker_present
    ):
        return next_status, {}, False
    if install_state in constants_mod.HANDOFF_PENDING_INSTALL_STATES:
        next_status.update(
            {
                "installState": "idle",
                "installStage": "idle",
                "installStageLabel": "",
                "lastError": "Stale desktop update handoff state was cleared.",
            }
        )
    elif handoff_marker_present:
        next_status["lastError"] = "Stale desktop update handoff state was cleared."
    return next_status, {}, True


def load_status(paths: Any, *, current_version: str | None = None) -> dict[str, Any]:
    deps = _root()
    status = deps.default_status_payload(current_version=current_version)
    status.update(_as_dict(deps.read_json(paths.install_state_path, {})))
    status["currentVersion"] = str(
        current_version or status.get("currentVersion") or get_app_version()
    )
    status, _credible_handoff_plan, _stale_handoff = deps._reconcile_handoff_status(paths, status)
    status["installStage"] = normalize_install_stage(
        status.get("installState"),
        status.get("installStage"),
    )
    status["installStageLabel"] = install_stage_label(
        status.get("installState"),
        status.get("installStage"),
    )
    return dict(status)


def save_status(paths: Any, payload: dict[str, Any]) -> dict[str, Any]:
    deps = _root()
    deps.write_json_atomic(paths.install_state_path, payload)
    return payload


def updater_install_requested(data_dir: Path) -> bool:
    deps = _root()
    paths = deps.DesktopUpdatePaths.from_data_dir(Path(data_dir))
    state = deps.load_status(paths)
    state, credible_handoff_plan, stale_handoff = deps._reconcile_handoff_status(paths, state)
    if stale_handoff:
        deps.clear_handoff_request(paths)
        deps.clear_install_plan(paths)
        deps.save_status(paths, state)
        return False
    if credible_handoff_plan:
        return True
    return bool(deps._handoff_status_pending(state))


def clear_success_marker(paths: Any) -> None:
    with contextlib.suppress(OSError):
        paths.success_marker_path.unlink()


def clear_handoff_request(paths: Any) -> None:
    with contextlib.suppress(OSError):
        paths.handoff_request_path.unlink()


def clear_handoff_diagnostics(paths: Any) -> None:
    with contextlib.suppress(OSError):
        paths.handoff_diagnostics_path.unlink()


def clear_install_plan(paths: Any) -> None:
    with contextlib.suppress(OSError):
        paths.install_plan_path.unlink()


def clear_staged_helper(path: Path | None) -> None:
    if path is None:
        return
    with contextlib.suppress(OSError):
        Path(path).unlink()


def helper_runtime_tmpdir() -> Path:
    deps = _root()
    return Path(
        Path(deps.tempfile.gettempdir()).resolve() / constants_mod.HELPER_RUNTIME_TMP_ROOT_NAME
    ).resolve()


def launch_staged_update_helper(paths: Any) -> None:
    deps = _root()
    plan = deps.validate_install_plan(deps.read_json(paths.install_plan_path, {}))
    helper_path = Path(str(plan.get("tempHelperPath") or "")).expanduser().resolve()
    if not helper_path.is_file():
        raise RuntimeError(f"Staged desktop updater helper not found: {helper_path}")
    paths.updater_dir.mkdir(parents=True, exist_ok=True)
    runtime_tmpdir = deps.helper_runtime_tmpdir()
    runtime_tmpdir.mkdir(parents=True, exist_ok=True)
    creationflags = 0
    env = deps.os.environ.copy()
    env["TEMP"] = str(runtime_tmpdir)
    env["TMP"] = str(runtime_tmpdir)
    data_dir = str(plan.get("dataDir") or "").strip()
    install_root = str(plan.get("installRoot") or "").strip()
    if data_dir:
        env["BALUFFO_DATA_DIR"] = data_dir
    if install_root:
        env["BALUFFO_INSTALL_ROOT"] = install_root
        env["BALUFFO_SHIP_ROOT"] = str(deps._resolve_runtime_path(install_root) / "ship")
    if deps.os.name == "nt":
        creationflags = int(getattr(deps.subprocess, "CREATE_NEW_PROCESS_GROUP", 0))
    with (
        paths.helper_stdout_log_path.open("ab") as helper_stdout,
        paths.helper_stderr_log_path.open("ab") as helper_stderr,
    ):
        deps.subprocess.Popen(
            [str(helper_path), "--install-plan", str(paths.install_plan_path)],
            cwd=str(paths.updater_dir),
            stdout=helper_stdout,
            stderr=helper_stderr,
            creationflags=creationflags,
            env=env,
        )


def write_success_marker(
    paths: Any,
    *,
    app_version: str,
    bridge_port: int,
    launcher_token: str,
) -> None:
    deps = _root()
    payload = {
        "writtenAt": iso_now(),
        "appVersion": str(app_version or ""),
        "bridgePort": int(bridge_port),
        "launcherToken": str(launcher_token or ""),
    }
    deps.write_json_atomic(paths.success_marker_path, payload)


def write_handoff_diagnostics(paths: Any) -> dict[str, Any]:
    deps = _root()
    payload: dict[str, Any] = {
        "writtenAt": iso_now(),
        "handoffRequestPresent": bool(paths.handoff_request_path.exists()),
        "installPlanPresent": bool(paths.install_plan_path.exists()),
        "installPlanValid": False,
        "launcherPid": 0,
        "launcherPidRunning": False,
        "desktopSessionRoot": "",
        "desktopSessionFilePresent": False,
        "sessionLauncherPid": 0,
        "launcherPidMatchesSession": False,
        "launcherTokenMatchesSession": False,
    }
    try:
        plan = deps.validate_install_plan(deps.read_json(paths.install_plan_path, {}))
    except ValueError as exc:
        payload["installPlanError"] = str(exc)
        deps.write_json_atomic(paths.handoff_diagnostics_path, payload)
        return payload
    payload["installPlanValid"] = True
    launcher_pid = _as_int(plan.get("launcherPid"))
    launcher_token = str(plan.get("launcherToken") or "").strip()
    session_root_raw = str(plan.get("desktopSessionRoot") or "").strip()
    payload["launcherPid"] = launcher_pid
    if launcher_pid > 0:
        payload["launcherPidRunning"] = bool(deps.pid_is_running(launcher_pid))
    if session_root_raw:
        try:
            session_root = deps._resolve_runtime_path(session_root_raw)
        except (OSError, RuntimeError, ValueError) as exc:
            payload["desktopSessionRoot"] = session_root_raw
            payload["desktopSessionRootError"] = type(exc).__name__
        else:
            session_state_path = session_root / "desktop-session.json"
            payload["desktopSessionRoot"] = str(session_root)
            payload["desktopSessionFilePresent"] = bool(session_state_path.exists())
            session_state = _as_dict(deps.read_desktop_session_state(session_root))
            session_launcher_pid = _as_int(session_state.get("launcherPid"))
            session_launcher_token = str(session_state.get("launcherToken") or "").strip()
            payload["sessionLauncherPid"] = session_launcher_pid
            payload["launcherPidMatchesSession"] = bool(
                launcher_pid > 0 and session_launcher_pid == launcher_pid
            )
            payload["launcherTokenMatchesSession"] = bool(
                launcher_token and session_launcher_token == launcher_token
            )
    deps.write_json_atomic(paths.handoff_diagnostics_path, payload)
    return payload


def read_cached_manifest(paths: Any) -> dict[str, Any]:
    deps = _root()
    return _as_dict(deps.read_json(paths.manifest_cache_path, {}))


def _normalize_release_notes_payload(
    payload: dict[str, Any] | None,
    *,
    fallback_url: str = "",
    fallback_title: str = "",
) -> dict[str, str]:
    source = dict(payload) if isinstance(payload, dict) else {}
    return {
        "releaseNotesUrl": str(
            source.get("releaseNotesUrl") or source.get("html_url") or fallback_url or ""
        ).strip(),
        "releaseNotesTitle": str(
            source.get("releaseNotesTitle") or source.get("name") or fallback_title or ""
        ).strip(),
        "releaseNotesBody": str(source.get("releaseNotesBody") or source.get("body") or "").strip(),
        "releaseNotesPublishedAt": str(
            source.get("releaseNotesPublishedAt") or source.get("published_at") or ""
        ).strip(),
    }


def _release_version_from_tag(tag: str) -> str:
    value = str(tag or "").strip()
    return value[1:] if value.lower().startswith("v") else value


def _normalize_release_notes_entry(
    payload: dict[str, Any] | None,
    *,
    fallback_url: str = "",
    fallback_title: str = "",
    fallback_version: str = "",
) -> dict[str, str]:
    source = dict(payload) if isinstance(payload, dict) else {}
    entry = dict(
        _normalize_release_notes_payload(
            source,
            fallback_url=fallback_url,
            fallback_title=fallback_title,
        )
    )
    release_tag = str(source.get("releaseTag") or source.get("tag_name") or "").strip()
    release_version = str(
        source.get("releaseVersion") or source.get("version") or fallback_version or ""
    ).strip()
    if not release_version and release_tag:
        release_version = _release_version_from_tag(release_tag)
    entry["releaseTag"] = release_tag
    entry["releaseVersion"] = release_version
    return entry


def _release_notes_entry_has_content(entry: dict[str, str]) -> bool:
    return bool(
        entry.get("releaseNotesUrl")
        or entry.get("releaseNotesTitle")
        or entry.get("releaseNotesBody")
        or entry.get("releaseNotesPublishedAt")
        or entry.get("releaseTag")
        or entry.get("releaseVersion")
    )


def _normalize_release_notes_history(payload: Any) -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in _as_list(payload):
        if not isinstance(item, dict):
            continue
        entry = _normalize_release_notes_entry(item)
        if not _release_notes_entry_has_content(entry):
            continue
        key = (
            entry.get("releaseTag")
            or entry.get("releaseVersion")
            or entry.get("releaseNotesUrl")
            or entry.get("releaseNotesTitle")
        )
        if key and key in seen:
            continue
        if key:
            seen.add(key)
        entries.append(entry)
    return entries


def _cached_release_notes(
    cached_manifest: dict[str, Any], *, target_version: str = "", manifest_url: str = ""
) -> dict[str, str]:
    deps = _root()
    payload = _as_dict(cached_manifest.get("releaseNotes"))
    return _as_str_dict(
        deps._normalize_release_notes_payload(
            payload,
            fallback_url=manifest_url,
            fallback_title=target_version,
        )
    )


def _cached_release_notes_history(cached_manifest: dict[str, Any]) -> list[dict[str, str]]:
    deps = _root()
    return list(deps._normalize_release_notes_history(cached_manifest.get("releaseNotesHistory")))


def _portable_artifact_name(manifest: dict[str, Any]) -> str:
    artifact = _as_dict(manifest.get("portable_artifact"))
    url = str(artifact.get("url") or "").strip()
    token = url.rsplit("/", 1)[-1]
    return token or f"baluffo-portable-{str(manifest.get('version') or '').strip()}.zip"


def _manifest_to_status(
    *,
    current_version: str,
    manifest: dict[str, Any],
    existing: dict[str, Any],
    release_notes: dict[str, Any] | None = None,
    release_notes_history: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    deps = _root()
    next_status = dict(existing)
    target_version = str(manifest.get("version") or "").strip()
    preserve_last_error = (
        str(existing.get("lastError") or "").strip()
        if (
            str(existing.get("downloadState") or "").strip().lower() == "failed"
            or str(existing.get("installState") or "").strip().lower() == "failed"
        )
        else ""
    )
    release_notes_payload = deps._normalize_release_notes_payload(
        release_notes,
        fallback_url=str(manifest.get("release_notes_url") or "").strip(),
        fallback_title=target_version,
    )
    release_notes_history_payload = deps._normalize_release_notes_history(
        release_notes_history
        if release_notes_history is not None
        else existing.get("releaseNotesHistory")
    )
    if not release_notes_history_payload and _release_notes_entry_has_content(
        {
            **release_notes_payload,
            "releaseTag": "",
            "releaseVersion": str(target_version or ""),
        }
    ):
        release_notes_history_payload = [
            deps._normalize_release_notes_entry(
                {
                    **release_notes_payload,
                    "releaseVersion": target_version,
                },
                fallback_url=str(manifest.get("release_notes_url") or "").strip(),
                fallback_title=target_version,
                fallback_version=target_version,
            )
        ]
    next_status.update(
        {
            "currentVersion": str(current_version or ""),
            "latestVersion": target_version,
            "targetVersion": target_version,
            "channel": str(manifest.get("channel") or DESKTOP_UPDATE_CHANNEL),
            **release_notes_payload,
            "releaseNotesHistory": release_notes_history_payload,
            "lastCheckedAt": iso_now(),
            "lastError": preserve_last_error,
            "blockedReason": "",
        }
    )
    if compare_versions(target_version, current_version) <= 0:
        next_status["availability"] = "up_to_date"
        next_status["updateAvailable"] = False
        next_status["installState"] = "idle"
        return next_status
    minimum_helper = str(manifest.get("min_desktop_updater_version") or "").strip()
    minimum_current = str(manifest.get("min_supported_current_version") or "").strip()
    if minimum_helper and compare_versions(DESKTOP_UPDATER_VERSION, minimum_helper) < 0:
        next_status["availability"] = "blocked"
        next_status["updateAvailable"] = True
        next_status["blockedReason"] = "helper_too_old"
        next_status["installState"] = "blocked"
        return next_status
    if minimum_current and compare_versions(current_version, minimum_current) < 0:
        next_status["availability"] = "blocked"
        next_status["updateAvailable"] = True
        next_status["blockedReason"] = "current_version_too_old"
        next_status["installState"] = "blocked"
        return next_status
    next_status["availability"] = "available"
    next_status["updateAvailable"] = True
    return next_status


def _reconcile_downloaded_artifact_status(
    *,
    paths: Any,
    manifest: dict[str, Any],
    status: dict[str, Any],
) -> dict[str, Any]:
    deps = _root()
    next_status = dict(status)
    install_state = str(next_status.get("installState") or "").strip().lower()
    artifact = _as_dict(manifest.get("portable_artifact"))
    artifact_path = paths.downloads_dir / deps._portable_artifact_name(manifest)
    expected_hash = str(artifact.get("sha256") or "").strip().lower()
    if artifact_path.is_file() and expected_hash:
        if compute_sha256(artifact_path).lower() == expected_hash:
            size_bytes = int(artifact_path.stat().st_size)
            next_status["downloadState"] = "downloaded"
            if install_state not in constants_mod.INSTALL_STATES_PRESERVING_DOWNLOADED_ARTIFACT:
                next_status["installState"] = "ready"
            next_status["downloadedBytes"] = size_bytes
            next_status["totalBytes"] = size_bytes
            next_status["downloadPercent"] = 100
            next_status["downloadedZipPath"] = str(artifact_path)
            return next_status
    if str(next_status.get("downloadState") or "").strip().lower() == "downloaded":
        next_status["downloadState"] = "idle"
        next_status["downloadedBytes"] = 0
        next_status["totalBytes"] = 0
        next_status["downloadPercent"] = 0
        next_status["downloadedZipPath"] = ""
    if str(next_status.get("installState") or "").strip().lower() == "ready":
        next_status["installState"] = "idle"
    return next_status


def _stale_download_failed_status(
    status: dict[str, Any],
    *,
    message: str,
) -> dict[str, Any]:
    next_status = dict(status)
    next_status.update(
        {
            "downloadState": "failed",
            "downloadedBytes": 0,
            "totalBytes": 0,
            "downloadPercent": 0,
            "installState": "idle",
            "downloadedZipPath": "",
            "lastError": str(message or "").strip(),
        }
    )
    return next_status


def _normalize_installed_status(
    status: dict[str, Any],
    *,
    current_version: str,
) -> dict[str, Any]:
    next_status = dict(status)
    install_state = str(next_status.get("installState") or "").strip().lower()
    target_version = str(
        next_status.get("targetVersion") or next_status.get("latestVersion") or ""
    ).strip()
    if install_state != "installed" or not target_version:
        return next_status
    if compare_versions(target_version, current_version) > 0:
        return next_status
    next_status.update(
        {
            "currentVersion": str(current_version or ""),
            "availability": "up_to_date",
            "updateAvailable": False,
            "downloadState": "idle",
            "downloadedBytes": 0,
            "totalBytes": 0,
            "downloadPercent": 0,
            "installState": "idle",
            "installStage": "idle",
            "installStageLabel": "",
            "downloadedZipPath": "",
            "lastError": "",
            "blockedReason": "",
        }
    )
    return next_status


def _failure_result(
    *,
    status: dict[str, Any],
    error: str,
    error_code: str,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "started": False,
        "status": status,
        "error": str(error or "").strip(),
        "errorCode": str(error_code or "").strip(),
    }
    if isinstance(extra, dict):
        payload.update(extra)
    return payload


def _retryable_install_status(
    status: dict[str, Any],
    *,
    zip_path: Path,
    error: str,
) -> dict[str, Any]:
    next_status = dict(status)
    size_bytes = int(zip_path.stat().st_size) if zip_path.is_file() else 0
    next_status.update(
        {
            "downloadState": "downloaded" if zip_path.is_file() else "idle",
            "downloadedBytes": size_bytes,
            "totalBytes": size_bytes,
            "downloadPercent": 100 if size_bytes > 0 else 0,
            "installState": "ready" if zip_path.is_file() else "idle",
            "installStage": "idle",
            "installStageLabel": "",
            "downloadedZipPath": str(zip_path) if zip_path.is_file() else "",
            "rollbackPath": "",
            "lastError": str(error or "").strip(),
        }
    )
    return next_status


def validate_install_plan(plan: dict[str, Any]) -> dict[str, Any]:
    required = (
        "planVersion",
        "installRoot",
        "dataDir",
        "tempHelperPath",
        "targetVersion",
        "currentVersion",
        "manifestPath",
        "downloadedZipPath",
        "expectedZipSha256",
        "manifestKeyId",
        "rollbackPath",
        "updaterWorkingDir",
        "createdAt",
        "launcherPid",
        "launcherToken",
        "desktopSessionRoot",
    )
    missing = [key for key in required if key not in plan]
    if missing:
        raise ValueError(f"Install plan missing fields: {', '.join(missing)}")
    return dict(plan)
