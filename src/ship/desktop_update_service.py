"""Desktop update service implementation behind the root compatibility facade."""

from __future__ import annotations

import contextlib
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.app_version import get_app_version
from src.ship import desktop_update_constants as constants_mod
from src.ship import desktop_update_manifest as manifest_mod
from src.ship.desktop_update_shared import (
    desktop_update_public_key_candidate_paths,
    fetch_json,
    install_stage_label,
    iso_now,
    load_desktop_update_public_keys,
    resolve_github_api_base,
    resolve_release_repo,
    validate_desktop_manifest,
    verify_manifest_signature,
)
from src.ship.desktop_update_state import (
    _cached_release_notes,
    _cached_release_notes_history,
    _failure_result,
    _handoff_status_pending,
    _manifest_to_status,
    _normalize_installed_status,
    _normalize_release_notes_entry,
    _normalize_release_notes_history,
    _normalize_release_notes_payload,
    _portable_artifact_name,
    _reconcile_downloaded_artifact_status,
    _reconcile_handoff_status,
    _retryable_install_status,
    _stale_download_failed_status,
    clear_handoff_diagnostics,
    clear_handoff_request,
    clear_install_plan,
    clear_staged_helper,
    clear_success_marker,
    read_cached_manifest,
    write_handoff_diagnostics,
)

root: Any | None = None


def _root() -> Any:
    if root is None:
        raise RuntimeError("desktop_update_service.root is not configured")
    return root


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _as_bytes_dict(value: Any) -> dict[str, bytes]:
    return (
        {str(key): item for key, item in value.items() if isinstance(item, bytes)}
        if isinstance(value, dict)
        else {}
    )


def _release_identity(release: dict[str, Any]) -> str:
    return (
        str(release.get("id") or "").strip()
        or str(release.get("tag_name") or release.get("releaseTag") or "").strip()
        or str(release.get("html_url") or release.get("releaseNotesUrl") or "").strip()
    )


def _merge_release_notes_history(
    latest_entry: dict[str, str],
    history: list[dict[str, str]],
) -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []
    seen: set[str] = set()
    for entry in [latest_entry, *history]:
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
        entries.append(dict(entry))
    return entries


class DesktopUpdateService:
    """App-side desktop updater state, fetch, download, and helper handoff service."""

    def __init__(
        self,
        *,
        data_dir: Path,
        install_root: Path | None = None,
        ship_root: Path | None = None,
        current_version_getter: Callable[[], str] | None = None,
    ) -> None:
        deps = _root()
        self._deps = deps
        env_install_root = str(deps.os.environ.get("BALUFFO_INSTALL_ROOT") or "").strip()
        env_ship_root = str(deps.os.environ.get("BALUFFO_SHIP_ROOT") or "").strip()
        self.paths = deps.DesktopUpdatePaths.from_data_dir(
            data_dir,
            install_root=install_root or (Path(env_install_root) if env_install_root else None),
            ship_root=ship_root or (Path(env_ship_root) if env_ship_root else None),
        )
        self._current_version_getter = current_version_getter or get_app_version
        self._lock = deps.threading.RLock()
        self._download_thread: Any | None = None
        self._stable_releases: list[dict[str, Any]] = []

    def current_version(self) -> str:
        return str(self._current_version_getter() or get_app_version()).strip()

    def _download_worker_alive_locked(self) -> bool:
        return self._download_thread is not None and self._download_thread.is_alive()

    def _load_cached_manifest_parts(
        self,
        *,
        cached_manifest: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, str], list[dict[str, str]]]:
        cached = (
            dict(cached_manifest)
            if isinstance(cached_manifest, dict)
            else read_cached_manifest(self.paths)
        )
        manifest = _as_dict(cached.get("manifest"))
        release_notes = _cached_release_notes(
            cached,
            target_version=str(manifest.get("version") or "").strip(),
            manifest_url=str(manifest.get("release_notes_url") or "").strip(),
        )
        release_notes_history = _cached_release_notes_history(cached)
        return dict(cached), manifest, dict(release_notes), list(release_notes_history)

    def _reconcile_status_locked(
        self,
        *,
        status: dict[str, Any] | None = None,
        cached_manifest: dict[str, Any] | None = None,
        manifest: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        deps = self._deps
        current_version = self.current_version()
        existing = (
            dict(status)
            if isinstance(status, dict)
            else deps.load_status(self.paths, current_version=current_version)
        )
        existing, _credible_handoff_plan, _stale_handoff = _reconcile_handoff_status(
            self.paths,
            existing,
        )
        (
            cached,
            cached_manifest_payload,
            release_notes,
            release_notes_history,
        ) = self._load_cached_manifest_parts(cached_manifest=cached_manifest)
        manifest_payload = (
            dict(manifest) if isinstance(manifest, dict) else dict(cached_manifest_payload)
        )
        next_status = dict(existing)
        if manifest_payload:
            next_status = _manifest_to_status(
                current_version=current_version,
                manifest=manifest_payload,
                existing=next_status,
                release_notes=release_notes,
                release_notes_history=release_notes_history,
            )
            next_status = _reconcile_downloaded_artifact_status(
                paths=self.paths,
                manifest=manifest_payload,
                status=next_status,
            )
            if (
                str(existing.get("downloadState") or "").strip().lower() == "downloading"
                and str(next_status.get("downloadState") or "").strip().lower() != "downloaded"
                and not self._download_worker_alive_locked()
            ):
                next_status = _stale_download_failed_status(
                    next_status,
                    message="The previous update download stopped before it finished. Download the update again.",
                )
        elif (
            str(existing.get("downloadState") or "").strip().lower() == "downloading"
            and not self._download_worker_alive_locked()
        ):
            next_status = _stale_download_failed_status(
                next_status,
                message="The previous update download stopped before it finished. Check for updates and try again.",
            )
        next_status, _credible_handoff_plan, _stale_handoff = _reconcile_handoff_status(
            self.paths,
            next_status,
        )
        next_status = _normalize_installed_status(next_status, current_version=current_version)
        if not (
            next_status.get("releaseNotesUrl")
            or next_status.get("releaseNotesTitle")
            or next_status.get("releaseNotesBody")
            or next_status.get("releaseNotesPublishedAt")
        ):
            next_status.update(release_notes)
        if not next_status.get("releaseNotesHistory") and release_notes_history:
            next_status["releaseNotesHistory"] = release_notes_history
        if next_status != existing:
            return _as_dict(deps.save_status(self.paths, next_status))
        return dict(next_status)

    def _download_failure_locked(
        self,
        *,
        status: dict[str, Any],
        error: str,
        error_code: str,
        mutate_status: bool = False,
    ) -> dict[str, Any]:
        deps = self._deps
        next_status = dict(status)
        if mutate_status:
            next_status = _as_dict(
                deps.save_status(
                    self.paths,
                    _stale_download_failed_status(next_status, message=error),
                )
            )
        return _as_dict(_failure_result(status=next_status, error=error, error_code=error_code))

    def _install_failure_locked(
        self,
        *,
        status: dict[str, Any],
        error: str,
        error_code: str,
    ) -> dict[str, Any]:
        deps = self._deps
        next_status = _as_dict(
            deps.save_status(
                self.paths,
                {
                    **dict(status),
                    "lastError": str(error or "").strip(),
                },
            )
        )
        return _as_dict(_failure_result(status=next_status, error=error, error_code=error_code))

    def _install_handoff_unconfirmed_locked(
        self,
        *,
        status: dict[str, Any],
        zip_path: Path,
        temp_helper: Path | None,
    ) -> dict[str, Any]:
        deps = self._deps
        error = "Baluffo did not confirm the install handoff. Try install again."
        with contextlib.suppress(Exception):
            write_handoff_diagnostics(self.paths)
        clear_handoff_request(self.paths)
        clear_install_plan(self.paths)
        clear_staged_helper(temp_helper)
        retryable_status = _as_dict(
            deps.save_status(
                self.paths,
                _retryable_install_status(status, zip_path=zip_path, error=error),
            )
        )
        return _as_dict(
            _failure_result(
                status=retryable_status,
                error=error,
                error_code="install_handoff_unconfirmed",
            )
        )

    def load_public_keys(self) -> dict[str, bytes]:
        return _as_bytes_dict(
            load_desktop_update_public_keys(
                candidate_paths=desktop_update_public_key_candidate_paths(self.paths.ship_root),
            )
        )

    def get_status_payload(self) -> dict[str, Any]:
        with self._lock:
            return dict(self._reconcile_status_locked())

    def _resolve_latest_release(self) -> dict[str, Any]:
        repo = resolve_release_repo(
            install_root=self.paths.install_root,
            ship_root=self.paths.ship_root,
        )
        if not repo:
            raise RuntimeError("Desktop update repository is not configured.")
        url = f"{resolve_github_api_base()}/repos/{repo}/releases?per_page=10"
        payload = fetch_json(url)
        if not isinstance(payload, list):
            raise RuntimeError("GitHub releases payload was not a list.")
        self._stable_releases = []
        for release in payload:
            if not isinstance(release, dict):
                continue
            if bool(release.get("draft")) or bool(release.get("prerelease")):
                continue
            self._stable_releases.append(dict(release))
        if self._stable_releases:
            return dict(self._stable_releases[0])
        raise RuntimeError("No stable GitHub release is available.")

    def _resolve_manifest_from_release(self, release: dict[str, Any]) -> dict[str, Any]:
        assets = _as_list(release.get("assets"))
        manifest_asset = next(
            (
                asset
                for asset in assets
                if isinstance(asset, dict)
                and str(asset.get("name") or "").strip()
                == manifest_mod.DESKTOP_UPDATE_MANIFEST_ASSET
            ),
            None,
        )
        if manifest_asset is None:
            raise RuntimeError("Stable GitHub release did not publish a desktop manifest asset.")
        manifest_url = str(manifest_asset.get("browser_download_url") or "").strip()
        if not manifest_url:
            raise RuntimeError("Desktop manifest asset is missing its download URL.")
        manifest = fetch_json(manifest_url)
        if not isinstance(manifest, dict):
            raise RuntimeError("Desktop manifest payload is invalid.")
        validate_desktop_manifest(manifest)
        verify_manifest_signature(manifest, public_keys=self.load_public_keys())
        if not str(manifest.get("release_notes_url") or "").strip():
            manifest["release_notes_url"] = str(release.get("html_url") or "").strip()
        return dict(manifest)

    def check_for_update(self, *, force: bool = False) -> dict[str, Any]:
        deps = self._deps
        with self._lock:
            current_version = self.current_version()
            status = self._reconcile_status_locked()
            last_checked_at = str(status.get("lastCheckedAt") or "").strip()
            if not force and last_checked_at and self.paths.manifest_cache_path.exists():
                try:
                    last_checked = datetime.fromisoformat(last_checked_at.replace("Z", "+00:00"))
                except ValueError:
                    last_checked = None
                if last_checked is not None:
                    age = (datetime.now(UTC) - last_checked).total_seconds()
                    if age < constants_mod.DEFAULT_RELEASE_CHECK_THROTTLE_SECONDS:
                        (
                            cached,
                            manifest,
                            release_notes,
                            release_notes_history,
                        ) = self._load_cached_manifest_parts()
                        if manifest:
                            return _as_dict(
                                deps.save_status(
                                    self.paths,
                                    self._reconcile_status_locked(
                                        status=_manifest_to_status(
                                            current_version=current_version,
                                            manifest=manifest,
                                            existing=status,
                                            release_notes=release_notes,
                                            release_notes_history=release_notes_history,
                                        ),
                                        cached_manifest=cached,
                                        manifest=manifest,
                                    ),
                                )
                            )
            deps.save_status(self.paths, {**status, "availability": "checking", "lastError": ""})
        try:
            release = self._resolve_latest_release()
            manifest = self._resolve_manifest_from_release(release)
            release_notes = _normalize_release_notes_payload(
                release,
                fallback_url=str(manifest.get("release_notes_url") or "").strip(),
                fallback_title=str(manifest.get("version") or "").strip(),
            )
            stable_releases = list(self._stable_releases)
            if not stable_releases or _release_identity(stable_releases[0]) != _release_identity(
                release
            ):
                stable_releases = [release]
            release_notes_history = _normalize_release_notes_history(stable_releases)
            latest_release_notes_entry = _normalize_release_notes_entry(
                {
                    **release,
                    **release_notes,
                    "releaseTag": str(release.get("tag_name") or ""),
                    "releaseVersion": str(manifest.get("version") or ""),
                },
                fallback_url=str(manifest.get("release_notes_url") or "").strip(),
                fallback_title=str(manifest.get("version") or "").strip(),
                fallback_version=str(manifest.get("version") or "").strip(),
            )
            release_notes_history = _merge_release_notes_history(
                latest_release_notes_entry,
                release_notes_history,
            )
            deps.write_json_atomic(
                self.paths.manifest_cache_path,
                {
                    "cachedAt": iso_now(),
                    "releaseId": int(release.get("id") or 0),
                    "releaseTag": str(release.get("tag_name") or ""),
                    "manifest": manifest,
                    "releaseNotes": release_notes,
                    "releaseNotesHistory": release_notes_history,
                },
            )
            next_status = _manifest_to_status(
                current_version=current_version,
                manifest=manifest,
                existing=deps.load_status(self.paths, current_version=current_version),
                release_notes=release_notes,
                release_notes_history=release_notes_history,
            )
            with self._lock:
                return dict(
                    self._reconcile_status_locked(
                        status=next_status,
                        cached_manifest=read_cached_manifest(self.paths),
                        manifest=manifest,
                    )
                )
        except Exception as exc:
            return _as_dict(
                deps.save_status(
                    self.paths,
                    {
                        **deps.load_status(self.paths, current_version=current_version),
                        "lastCheckedAt": iso_now(),
                        "availability": "error",
                        "updateAvailable": False,
                        "lastError": str(exc),
                    },
                )
            )

    def _run_download_worker(self, manifest: dict[str, Any]) -> None:
        deps = self._deps
        artifact = _as_dict(manifest.get("portable_artifact"))
        target = self.paths.downloads_dir / _portable_artifact_name(manifest)
        last_reported_percent = -1

        def on_progress(downloaded: int, total: int) -> None:
            nonlocal last_reported_percent
            total_bytes = total or int(artifact.get("size_bytes") or 0)
            percent = int((downloaded / total_bytes) * 100) if total_bytes > 0 else 0
            clamped_percent = max(0, min(100, percent))
            if clamped_percent == last_reported_percent:
                return
            last_reported_percent = clamped_percent
            try:
                deps.save_status(
                    self.paths,
                    {
                        **deps.load_status(self.paths, current_version=self.current_version()),
                        "downloadState": "downloading",
                        "downloadedBytes": int(downloaded),
                        "totalBytes": int(total_bytes),
                        "downloadPercent": clamped_percent,
                        "lastError": "",
                    },
                )
            except Exception:
                return

        try:
            deps.download_file(str(artifact.get("url") or ""), target, on_progress=on_progress)
            expected_hash = str(artifact.get("sha256") or "").strip().lower()
            if expected_hash and deps.compute_sha256(target).lower() != expected_hash:
                raise RuntimeError("Downloaded portable ZIP checksum mismatch.")
            deps.save_status(
                self.paths,
                {
                    **deps.load_status(self.paths, current_version=self.current_version()),
                    "downloadState": "downloaded",
                    "downloadedBytes": int(target.stat().st_size),
                    "totalBytes": int(target.stat().st_size),
                    "downloadPercent": 100,
                    "installState": "ready",
                    "downloadedZipPath": str(target),
                    "lastError": "",
                },
            )
        except Exception as exc:
            with contextlib.suppress(OSError):
                target.unlink()
            current_status = deps.load_status(self.paths, current_version=self.current_version())
            deps.save_status(
                self.paths,
                {
                    **current_status,
                    "downloadState": "failed",
                    "installState": "idle",
                    "installStage": "idle",
                    "downloadedBytes": 0,
                    "downloadPercent": 0,
                    "downloadedZipPath": "",
                    "lastError": str(exc),
                },
            )
        finally:
            with self._lock:
                self._download_thread = None

    def download_update(self) -> dict[str, Any]:
        deps = self._deps
        status = self.check_for_update(force=False)
        with self._lock:
            status = self._reconcile_status_locked(status=status)
            availability = str(status.get("availability") or "").strip().lower()
            download_state = str(status.get("downloadState") or "").strip().lower()
            install_state = str(status.get("installState") or "").strip().lower()
            if download_state == "downloading" and self._download_worker_alive_locked():
                return self._download_failure_locked(
                    status=status,
                    error="Update download already in progress.",
                    error_code="download_in_progress",
                )
            if download_state == "downloaded" or install_state == "ready":
                return self._download_failure_locked(
                    status=status,
                    error="The update ZIP is already downloaded and ready to install.",
                    error_code="update_ready_to_install",
                )
            if availability == "blocked":
                return self._download_failure_locked(
                    status=status,
                    error="This update is available but cannot be downloaded automatically from the current build.",
                    error_code=str(status.get("blockedReason") or "update_blocked"),
                )
            if availability != "available":
                return self._download_failure_locked(
                    status=status,
                    error=str(status.get("lastError") or "No update is available."),
                    error_code="no_update_available",
                )
            _cached, manifest, _release_notes, _release_notes_history = (
                self._load_cached_manifest_parts()
            )
            if not manifest:
                return self._download_failure_locked(
                    status=status,
                    error="No verified manifest is cached. Check for updates again.",
                    error_code="manifest_cache_missing",
                )
            try:
                self.paths.downloads_dir.mkdir(parents=True, exist_ok=True)
                state = _as_dict(
                    deps.save_status(
                        self.paths,
                        {
                            **status,
                            "downloadState": "downloading",
                            "downloadedBytes": 0,
                            "totalBytes": int(
                                (_as_dict(manifest.get("portable_artifact"))).get("size_bytes") or 0
                            ),
                            "downloadPercent": 0,
                            "lastError": "",
                        },
                    )
                )
                thread = deps.threading.Thread(
                    target=self._run_download_worker,
                    args=(manifest,),
                    daemon=True,
                    name="baluffo-desktop-update-download",
                )
                self._download_thread = thread
                thread.start()
                return {"started": True, "status": state}
            except Exception as exc:
                self._download_thread = None
                return self._download_failure_locked(
                    status=status,
                    error=f"Could not start the desktop update download: {exc}",
                    error_code="download_start_failed",
                    mutate_status=True,
                )

    def _ensure_install_preflight(self, zip_path: Path) -> None:
        deps = self._deps
        if not zip_path.is_file():
            raise RuntimeError(f"Downloaded update ZIP not found: {zip_path}")
        helper_path = self.paths.install_root / constants_mod.DESKTOP_UPDATE_HELPER_NAME
        if not helper_path.is_file():
            raise RuntimeError(f"Installed desktop updater helper not found: {helper_path}")
        self.paths.updater_dir.mkdir(parents=True, exist_ok=True)
        self.paths.rollback_root.mkdir(parents=True, exist_ok=True)
        required_free = max(int(zip_path.stat().st_size) * 3, 128 * 1024 * 1024)
        data_root_usage = deps.shutil.disk_usage(self.paths.updater_dir)
        if int(data_root_usage.free) < required_free:
            raise RuntimeError(
                "Not enough free disk space in the desktop update data root for staging and rollback."
            )
        install_root_usage = deps.shutil.disk_usage(self.paths.install_root)
        if int(install_root_usage.free) < required_free:
            raise RuntimeError(
                "Not enough free disk space in the Baluffo install root for runtime replacement."
            )

    def request_install(self) -> dict[str, Any]:
        deps = self._deps
        with self._lock:
            status = self._reconcile_status_locked()
            if str(status.get("downloadState") or "").strip().lower() != "downloaded":
                return self._install_failure_locked(
                    status=status,
                    error="Update ZIP is not ready to install.",
                    error_code="install_not_ready",
                )
            _cached, manifest, _release_notes, _release_notes_history = (
                self._load_cached_manifest_parts()
            )
            if not manifest:
                return self._install_failure_locked(
                    status=status,
                    error="No verified manifest is cached. Check for updates again.",
                    error_code="manifest_cache_missing",
                )
            zip_path = Path(str(status.get("downloadedZipPath") or "")).expanduser().resolve()
            try:
                self._ensure_install_preflight(zip_path)
            except Exception as exc:
                return self._install_failure_locked(
                    status=status,
                    error=str(exc),
                    error_code="install_preflight_failed",
                )
            session_root = deps.resolve_desktop_session_root()
            session_state = deps.read_desktop_session_state(session_root)
            launcher_pid = int(session_state.get("launcherPid") or 0)
            launcher_token = str(session_state.get("launcherToken") or "").strip()
            if launcher_pid <= 0 or not launcher_token:
                return self._install_failure_locked(
                    status=status,
                    error="The desktop launcher session is unavailable for updater handoff.",
                    error_code="install_session_unavailable",
                )
            helper_source = self.paths.install_root / constants_mod.DESKTOP_UPDATE_HELPER_NAME
            temp_helper = (
                Path(deps.tempfile.gettempdir()).resolve()
                / f"BaluffoUpdater-{uuid.uuid4().hex}.exe"
            )
            try:
                deps.shutil.copy2(helper_source, temp_helper)
            except Exception as exc:
                return self._install_failure_locked(
                    status=status,
                    error=f"Could not stage the updater helper: {exc}",
                    error_code="install_start_failed",
                )
            try:
                clear_success_marker(self.paths)
                clear_handoff_diagnostics(self.paths)
                rollback_path = self.paths.rollback_root / (
                    f"{str(manifest.get('version') or '').strip()}-"
                    f"{datetime.now(UTC).strftime('%Y%m%d-%H%M%S')}"
                )
                plan = {
                    "planVersion": 1,
                    "installRoot": str(self.paths.install_root),
                    "dataDir": str(self.paths.data_dir),
                    "tempHelperPath": str(temp_helper),
                    "targetVersion": str(manifest.get("version") or "").strip(),
                    "currentVersion": self.current_version(),
                    "manifestPath": str(self.paths.manifest_cache_path),
                    "downloadedZipPath": str(zip_path),
                    "expectedZipSha256": str(
                        _as_dict(manifest.get("portable_artifact")).get("sha256") or ""
                    ).strip(),
                    "manifestKeyId": str(manifest.get("key_id") or "").strip(),
                    "rollbackPath": str(rollback_path),
                    "updaterWorkingDir": str(self.paths.updater_dir),
                    "helperStdoutPath": str(self.paths.helper_stdout_log_path),
                    "helperStderrPath": str(self.paths.helper_stderr_log_path),
                    "helperDiagnosticsPath": str(self.paths.helper_diagnostics_log_path),
                    "createdAt": iso_now(),
                    "launcherPid": launcher_pid,
                    "launcherToken": launcher_token,
                    "desktopSessionRoot": str(session_root),
                }
                deps.write_json_atomic(self.paths.install_plan_path, plan)
                deps.write_json_atomic(
                    self.paths.handoff_request_path,
                    {
                        "requestedAt": iso_now(),
                        "targetVersion": str(manifest.get("version") or "").strip(),
                        "launcherPid": launcher_pid,
                        "launcherToken": launcher_token,
                    },
                )
                deps.save_status(
                    self.paths,
                    {
                        **status,
                        "installState": "handoff_requested",
                        "installStage": "preparing",
                        "installStageLabel": install_stage_label(
                            "handoff_requested",
                            "preparing",
                        ),
                        "helperUpdatedAt": iso_now(),
                        "lastError": "",
                        "manifestPath": str(self.paths.manifest_cache_path),
                        "downloadedZipPath": str(zip_path),
                        "rollbackPath": str(rollback_path),
                    },
                )
                verified_status = deps.load_status(
                    self.paths, current_version=self.current_version()
                )
                verified_status, credible_handoff_plan, _stale_handoff = _reconcile_handoff_status(
                    self.paths,
                    verified_status,
                )
                if not credible_handoff_plan or not _handoff_status_pending(verified_status):
                    return self._install_handoff_unconfirmed_locked(
                        status=status,
                        zip_path=zip_path,
                        temp_helper=temp_helper,
                    )
                return {
                    "started": True,
                    "status": verified_status,
                    "exitRequested": True,
                }
            except Exception as exc:
                return self._install_failure_locked(
                    status=status,
                    error=f"Could not start the desktop update install: {exc}",
                    error_code="install_start_failed",
                )
