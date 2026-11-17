"""Packaged desktop update rehearsal helpers behind the root smoke facade.

AI boundary owns: packaged update rehearsal manifests, helper launch, install handoff, and marker verification.
AI boundary implement in: this file for update rehearsal scenarios; update service implementation stays in src.ship.desktop_update_*.
AI boundary search before contracts: desktop update service, updater helper, and packaged update rehearsal tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused packaged update rehearsal tests.
"""

from __future__ import annotations

import base64
import contextlib
import http.server
import json
import shutil
import subprocess
import threading
import time
import zipfile
from pathlib import Path
from typing import Any, cast

from src.ship.desktop_update_shared import pid_is_running as _pid_is_running
from src.ship.desktop_update_state import load_status as _load_desktop_update_status
from src.ship.desktop_update_state import save_status as _save_desktop_update_status

root: Any | None = None


def _root() -> Any:
    if root is None:
        raise RuntimeError("packaged_smoke.rehearsal_update.root is not configured")
    return root


def _archive_portable_dir(portable_dir: Path, target_zip: Path) -> Path:
    if target_zip.exists():
        target_zip.unlink()
    built = shutil.make_archive(str(target_zip.with_suffix("")), "zip", root_dir=str(portable_dir))
    return Path(built).expanduser().resolve()


def _inject_desktop_update_public_keys(portable_root: Path, public_keys: dict[str, str]) -> None:
    deps = _root()
    portable = portable_root.expanduser().resolve()
    app_dir = portable / "ship" / "app"
    current_version_path = app_dir / "current.txt"
    current_version = str(current_version_path.read_text(encoding="utf-8").strip())
    if not current_version:
        raise RuntimeError(
            f"Portable build is missing current version metadata: {current_version_path}"
        )
    payload = json.dumps(public_keys, indent=2, sort_keys=True)
    targets = [
        app_dir / deps.update_manifest_helpers_mod.PUBLIC_KEYS_FILE,
        app_dir
        / "versions"
        / current_version
        / "packaging"
        / deps.update_manifest_helpers_mod.PUBLIC_KEYS_FILE,
    ]
    for target in targets:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(payload, encoding="utf-8")


def _remove_optional_psutil_runtime(portable_root: Path) -> list[str]:
    internal_dir = portable_root / "_internal"
    removed: list[str] = []
    if not internal_dir.exists():
        return removed
    for pattern in ("psutil", "psutil-*", "_psutil*.pyd"):
        for candidate in internal_dir.glob(pattern):
            if not candidate.exists():
                continue
            removed.append(candidate.relative_to(portable_root).as_posix())
            if candidate.is_dir():
                shutil.rmtree(candidate)
            else:
                candidate.unlink()
    return removed


def _prepare_desktop_update_rehearsal_roots(
    *,
    portable_root: Path,
    artifacts_dir: Path,
    public_keys: dict[str, str],
) -> tuple[Path, Path, list[str]]:
    deps = _root()
    source = portable_root.expanduser().resolve()
    install_root = artifacts_dir / "portable-install"
    target_root = artifacts_dir / "portable-update-target"
    for root_dir in (install_root, target_root):
        if root_dir.exists():
            shutil.rmtree(root_dir)
        shutil.copytree(source, root_dir)
        deps._inject_desktop_update_public_keys(root_dir, public_keys)
    source_psutil_removed = _remove_optional_psutil_runtime(install_root)
    return install_root, target_root, source_psutil_removed


def _seed_rehearsal_local_data(data_dir: Path) -> dict[str, Any]:
    deps = _root()
    store = deps.LocalDataStore(deps.LocalDataPaths.from_data_dir(data_dir))
    user = store.sign_in("Packaged Update Rehearsal")
    uid = str(user.get("uid") or "")
    job_key = store.save_job_for_user(
        uid,
        {
            "title": "Packaged Update QA",
            "company": "Baluffo QA",
            "city": "Amsterdam",
            "country": "Netherlands",
            "jobLink": "https://example.com/packaged-update-qa",
            "isCustom": True,
            "customSourceLabel": "Rehearsal",
            "applicationStatus": "bookmark",
        },
    )
    notes = "Preserve this saved job across the packaged updater rehearsal."
    store.update_job_notes(uid, job_key, notes)
    attachment_payload = b"desktop update rehearsal attachment"
    attachment_id = store.add_attachment_for_job(
        uid,
        job_key,
        {
            "name": "desktop-update-rehearsal.txt",
            "type": "text/plain",
            "size": len(attachment_payload),
        },
        "data:text/plain;base64," + base64.b64encode(attachment_payload).decode("ascii"),
    )
    return {
        "uid": uid,
        "jobKey": job_key,
        "notes": notes,
        "attachmentId": attachment_id,
        "attachmentName": "desktop-update-rehearsal.txt",
    }


class _DesktopUpdateReleaseHandler(http.server.BaseHTTPRequestHandler):
    def __init__(
        self,
        *args: Any,
        release_payload: list[dict[str, Any]],
        manifest: dict[str, Any],
        portable_zip: Path,
        **kwargs: Any,
    ) -> None:
        self._release_payload = release_payload
        self._manifest = manifest
        self._portable_zip = portable_zip
        super().__init__(*args, **kwargs)

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _send_json(self, payload: Any, *, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(int(status))
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_file(self, path: Path, *, content_type: str) -> None:
        self.send_response(200)
        self.send_header("Content-Type", str(content_type))
        self.send_header("Content-Length", str(int(path.stat().st_size)))
        self.end_headers()
        with path.open("rb") as handle:
            while True:
                chunk = handle.read(1024 * 1024)
                if not chunk:
                    break
                self.wfile.write(chunk)

    def do_GET(self) -> None:
        if self.path.startswith("/repos/local/baluffo-smoke/releases"):
            self._send_json(self._release_payload)
            return
        if self.path == "/assets/baluffo-desktop-update-manifest.json":
            self._send_json(self._manifest)
            return
        if self.path == "/assets/baluffo-portable-update.zip":
            self._send_file(self._portable_zip, content_type="application/zip")
            return
        if self.path == "/release-notes":
            body = b"Packaged desktop update rehearsal"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self.send_error(404)


_DesktopUpdateReleaseHandler.protocol_version = "HTTP/1.1"


def _start_desktop_update_release_server(
    *,
    manifest: dict[str, Any],
    portable_zip: Path,
) -> tuple[str, http.server.ThreadingHTTPServer, threading.Thread]:
    deps = _root()
    release_payload_holder: dict[str, list[dict[str, Any]]] = {"value": []}

    def _handler_factory(*args: Any, **kwargs: Any) -> _DesktopUpdateReleaseHandler:
        return _DesktopUpdateReleaseHandler(
            *args,
            release_payload=release_payload_holder["value"],
            manifest=manifest,
            portable_zip=portable_zip,
            **kwargs,
        )

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _handler_factory)
    base_url = f"http://127.0.0.1:{int(server.server_port)}"
    release_payload = [
        {
            "id": 1,
            "tag_name": f"v{manifest.get('version')}",
            "draft": False,
            "prerelease": False,
            "html_url": f"{base_url}/release-notes",
            "assets": [
                {
                    "name": deps.DESKTOP_UPDATE_MANIFEST_ASSET,
                    "browser_download_url": (
                        f"{base_url}/assets/{deps.DESKTOP_UPDATE_MANIFEST_ASSET}"
                    ),
                }
            ],
        }
    ]
    release_payload_holder["value"] = release_payload
    thread = threading.Thread(
        target=server.serve_forever,
        daemon=True,
        name="desktop-update-release-server",
    )
    thread.start()
    return base_url, server, thread


def _wait_for_process_exit(process: subprocess.Popen[Any], *, timeout_s: float) -> None:
    deps = _root()
    deadline = deps.time.monotonic() + max(5.0, float(timeout_s))
    while deps.time.monotonic() < deadline:
        if process.poll() is not None:
            return
        deps.time.sleep(0.5)
    raise TimeoutError("Packaged runtime did not exit for helper handoff in time.")


def _confirmed_install_handoff_status(paths: Any) -> dict[str, Any]:
    deps = _root()
    if not paths.handoff_request_path.exists():
        return {}
    try:
        status = json.loads(paths.install_state_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        status = {}
    if not isinstance(status, dict):
        return {}
    install_state = str(status.get("installState") or "").strip().lower()
    pending_states = getattr(
        deps.desktop_update_mod,
        "HANDOFF_PENDING_INSTALL_STATES",
        frozenset({"handoff_requested", "waiting_for_exit"}),
    )
    return status if install_state in pending_states else {}


def _wait_for_install_handoff_confirmation(
    *,
    bridge_port: int,
    paths: Any,
    process: subprocess.Popen[Any],
    timeout_s: float,
) -> dict[str, Any]:
    deps = _root()
    deadline = deps.time.monotonic() + max(5.0, float(timeout_s))
    last_status: dict[str, Any] = {}
    while deps.time.monotonic() < deadline:
        disk_status = _confirmed_install_handoff_status(paths)
        if disk_status:
            return disk_status
        handoff_marker_exists = paths.handoff_request_path.exists()
        try:
            status_code, status_payload = deps.request_json(
                f"http://127.0.0.1:{bridge_port}/app/update-status?t={deps.time.time_ns()}",
                timeout_s=10.0,
            )
        except OSError as exc:
            disk_status = _confirmed_install_handoff_status(paths)
            if disk_status:
                return disk_status
            last_status = {"error": str(exc), "handoffRequestPresent": handoff_marker_exists}
            deps.time.sleep(0.2)
            continue
        if status_code != 200:
            raise RuntimeError(
                f"Packaged update handoff status failed: {status_payload or {'status': status_code}}"
            )
        last_status = dict(status_payload) if isinstance(status_payload, dict) else {}
        install_state = str(last_status.get("installState") or "").strip().lower()
        if handoff_marker_exists and install_state in {"handoff_requested", "waiting_for_exit"}:
            return last_status
        if process.poll() is not None:
            raise RuntimeError(
                "Packaged runtime exited before updater handoff was confirmed: "
                f"{last_status or {'handoffRequestPresent': handoff_marker_exists}}"
            )
        deps.time.sleep(0.2)
    raise TimeoutError(
        "Packaged runtime did not confirm updater handoff in time: "
        f"{last_status or {'handoffRequestPresent': paths.handoff_request_path.exists()}}"
    )


def _post_install_update_handoff(
    *,
    bridge_port: int,
    paths: Any,
    timeout_s: float,
) -> tuple[int, dict[str, Any]]:
    deps = _root()
    try:
        return cast(
            tuple[int, dict[str, Any]],
            deps.post_json(
                f"http://127.0.0.1:{bridge_port}/app/install-update",
                {},
                timeout_s=timeout_s,
            ),
        )
    except OSError:
        handoff_status = _confirmed_install_handoff_status(paths)
        if not handoff_status:
            raise
        return 200, {"started": True, "status": handoff_status}


def _wait_for_relaunched_runtime(
    *,
    expected_data_dir: Path,
    expected_version: str,
    timeout_s: float,
    env: dict[str, str] | None = None,
) -> dict[str, Any]:
    deps = _root()
    session_root = deps.desktop_update_mod.resolve_desktop_session_root(env)
    session_path = session_root / "desktop-session.json"
    deadline = deps.time.monotonic() + max(10.0, float(timeout_s))
    last_health: dict[str, Any] = {}
    while deps.time.monotonic() < deadline:
        if not session_path.exists():
            deps.time.sleep(0.75)
            continue
        session = deps.desktop_update_mod.read_desktop_session_state(session_root)
        if (
            Path(str(session.get("dataDir") or "")).expanduser().resolve()
            != expected_data_dir.resolve()
        ):
            deps.time.sleep(0.75)
            continue
        bridge_port = int(session.get("bridgePort") or 0)
        if bridge_port <= 0:
            deps.time.sleep(0.75)
            continue
        try:
            last_health = deps.fetch_json(
                f"http://127.0.0.1:{bridge_port}/ops/health",
                timeout_s=5.0,
            )
        except (OSError, TimeoutError, json.JSONDecodeError):
            last_health = {}
            deps.time.sleep(0.75)
            continue
        if (
            isinstance(last_health, dict)
            and bool(last_health.get("desktopMode"))
            and bool(last_health.get("startupReady"))
            and str(last_health.get("appVersion") or "").strip()
            == str(expected_version or "").strip()
        ):
            return {"session": session, "health": last_health}
        deps.time.sleep(0.75)
    raise TimeoutError(f"Updated packaged runtime did not relaunch successfully: {last_health}")


def _verify_rehearsal_local_data(data_dir: Path, expected: dict[str, Any]) -> None:
    deps = _root()
    store = deps.LocalDataStore(deps.LocalDataPaths.from_data_dir(data_dir))
    uid = str(expected.get("uid") or "")
    current_user = store.get_current_user() or {}
    if str(current_user.get("uid") or "") != uid:
        raise RuntimeError("Desktop update rehearsal did not preserve the signed-in local profile.")
    rows = store.list_saved_jobs(uid)
    target = next(
        (row for row in rows if str(row.get("jobKey") or "") == str(expected.get("jobKey") or "")),
        None,
    )
    if not target:
        raise RuntimeError("Desktop update rehearsal did not preserve the saved custom job.")
    if str(target.get("notes") or "") != str(expected.get("notes") or ""):
        raise RuntimeError("Desktop update rehearsal did not preserve saved job notes.")
    attachments = store.list_attachments_for_job(uid, str(expected.get("jobKey") or ""))
    if not any(
        str(row.get("id") or "") == str(expected.get("attachmentId") or "") for row in attachments
    ):
        raise RuntimeError("Desktop update rehearsal did not preserve job attachments.")


def _read_json_object_file(path: Any) -> dict[str, Any]:
    """Read ``path`` as a JSON object, returning {} when missing or malformed."""
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, dict) else {}


def _read_helper_stdout_payload(paths: Any) -> dict[str, Any]:
    return _read_json_object_file(paths.helper_stdout_log_path)


def _helper_diagnostic_rows(paths: Any) -> list[dict[str, Any]]:
    if not paths.helper_diagnostics_log_path.is_file():
        return []
    rows: list[dict[str, Any]] = []
    for raw_line in paths.helper_diagnostics_log_path.read_text(encoding="utf-8").splitlines():
        line = str(raw_line or "").strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            rows.append(dict(payload))
    return rows


def _helper_failure_message(payload: dict[str, Any]) -> str:
    fields_raw = payload.get("fields")
    fields = fields_raw if isinstance(fields_raw, dict) else {}
    return str(fields.get("error") or payload.get("error") or payload).strip()


def _load_update_install_state(paths: Any) -> dict[str, Any]:
    return _read_json_object_file(paths.install_state_path)


def _wait_for_desktop_update_helper_completion(*, paths: Any, timeout_s: float) -> None:
    deps = _root()
    deadline = deps.time.monotonic() + max(5.0, float(timeout_s))
    last_status: dict[str, Any] = {}
    last_event = ""
    while deps.time.monotonic() < deadline:
        stdout_payload = _read_helper_stdout_payload(paths)
        if stdout_payload:
            if not bool(stdout_payload.get("ok")):
                raise RuntimeError(
                    f"Update helper reported failure: {stdout_payload.get('error') or stdout_payload}"
                )
            return

        for payload in _helper_diagnostic_rows(paths):
            event = str(payload.get("event") or "").strip()
            if event:
                last_event = event
            if event == "helper_main_failed":
                raise RuntimeError(
                    "Update helper diagnostics reported failure: "
                    f"{_helper_failure_message(payload)}"
                )
            if event == "helper_main_succeeded":
                return

        last_status = _load_update_install_state(paths)
        install_state = str(last_status.get("installState") or "").strip().lower()
        install_stage = str(last_status.get("installStage") or "").strip().lower()
        if install_state == "failed" or install_stage == "failed":
            raise RuntimeError(f"Update helper left failed updater state: {last_status}")
        deps.time.sleep(0.25)

    raise TimeoutError(
        "Update helper did not finish before rehearsal completion: "
        f"{last_status or {'lastHelperEvent': last_event}}"
    )


def _assert_desktop_update_helper_succeeded(
    *,
    paths: Any,
    relaunch_bridge_port: int,
) -> None:
    deps = _root()
    payload = _read_helper_stdout_payload(paths)
    if payload and not bool(payload.get("ok")):
        raise RuntimeError(f"Update helper reported failure: {payload.get('error') or payload}")
    for row in _helper_diagnostic_rows(paths):
        if str(row.get("event") or "").strip() == "helper_main_failed":
            raise RuntimeError(
                f"Update helper diagnostics reported failure: {_helper_failure_message(row)}"
            )
    if relaunch_bridge_port <= 0:
        return
    status_code, status_payload = deps.request_json(
        f"http://127.0.0.1:{relaunch_bridge_port}/app/update-status?t={deps.time.time_ns()}",
        timeout_s=10.0,
    )
    if status_code != 200:
        raise RuntimeError(
            f"Updated desktop app did not expose updater status after relaunch: {status_payload}"
        )
    status = (
        dict(status_payload.get("status") or {})
        if isinstance(status_payload.get("status"), dict)
        else dict(status_payload)
        if isinstance(status_payload, dict)
        else {}
    )
    install_state = str(status.get("installState") or "").strip().lower()
    install_stage = str(status.get("installStage") or "").strip().lower()
    download_state = str(status.get("downloadState") or "").strip().lower()
    if install_state == "failed" or install_stage == "failed" or download_state == "failed":
        raise RuntimeError(f"Updated desktop app reported a failed updater state: {status}")


def _assert_installed_tree_matches_target(install_root: Path, target_zip: Path) -> None:
    """The install must hold every file the target archive carries, not a subset.

    The desktop update that broke a live install fetched a complete archive and
    then left a version tree without the route modules it imports lazily, so a
    success signal that only reads `/ops/health` cannot prove the tree came back.
    """
    deps = _root()
    with zipfile.ZipFile(target_zip) as archive:
        expected = {
            info.filename.replace("\\", "/")
            for info in archive.infolist()
            if not info.is_dir() and not info.filename.replace("\\", "/").startswith("ship/data/")
        }
    missing = sorted(rel for rel in expected if not (install_root / rel).is_file())
    if missing:
        raise RuntimeError(
            "Installed tree is missing "
            f"{len(missing)} of {len(expected)} target files: " + ", ".join(missing[:5])
        )
    pointer = install_root / "ship" / "app" / "current.txt"
    installed_version = pointer.read_text(encoding="utf-8").strip() if pointer.is_file() else ""
    expected_version = str(deps.get_app_version() or "").strip()
    if installed_version != expected_version:
        raise RuntimeError(
            "Installed tree did not switch to the target version "
            f"(expected {expected_version!r}, found {installed_version!r})"
        )


_REHEARSAL_MODE_SUCCESS = "success"
_REHEARSAL_MODE_ROLLBACK = "rollback"


def _helper_verify_timeout_s_for_mode(mode: str, runtime_timeout_s: float) -> float:
    if mode == _REHEARSAL_MODE_ROLLBACK:
        # Readiness can never succeed in this mode, so the helper only has to wait
        # for the new app to boot before it gives up and rolls back.
        return 20.0
    return max(30.0, float(runtime_timeout_s))


def _rehearsal_manifest_version(mode: str, build_version: str) -> str:
    """Version the rehearsal release publishes; the real build cannot report it.

    The helper accepts an install only when the relaunched app's `/ops/health`
    reports the manifest's target version. In rollback mode the manifest names
    the next patch version while the archive is the real build, so the new app
    boots and answers health yet can never satisfy the readiness contract. The
    update is real -- signed manifest, real archive, real helper -- and it is the
    readiness check itself that fails, which is what runs the rollback.
    """
    if mode != _REHEARSAL_MODE_ROLLBACK:
        return build_version
    parts = build_version.split(".")
    if parts and parts[-1].isdigit():
        parts[-1] = str(int(parts[-1]) + 1)
        return ".".join(parts)
    return f"{build_version}.1"


def _install_tree_sizes_all(install_root: Path) -> dict[str, int]:
    """Every install file outside ``ship/data``, used to prove a repair restored it."""
    sizes: dict[str, int] = {}
    for path in install_root.rglob("*"):
        if not path.is_file():
            continue
        rel = path.relative_to(install_root).as_posix()
        if rel.startswith("ship/data/"):
            continue
        sizes[rel] = path.stat().st_size
    return sizes


def _install_tree_sizes(install_root: Path, *, mode: str) -> dict[str, int]:
    """File sizes of the install as the update finds it, for the rollback proof."""
    if mode != _REHEARSAL_MODE_ROLLBACK:
        return {}
    return _install_tree_sizes_all(install_root)


def _install_pointer_version(install_root: Path) -> str:
    pointer = install_root / "ship" / "app" / "current.txt"
    return pointer.read_text(encoding="utf-8").strip() if pointer.is_file() else ""


def _previous_reported_version(runtime_env: dict[str, str]) -> str:
    deps = _root()
    return (
        str(runtime_env.get("BALUFFO_APP_VERSION_OVERRIDE") or "").strip()
        or str(deps.get_app_version() or "").strip()
    )


def _assert_restored_previous_tree(
    install_root: Path,
    expected_sizes: dict[str, int],
    expected_pointer: str,
) -> None:
    missing = sorted(rel for rel in expected_sizes if not (install_root / rel).is_file())
    if missing:
        raise RuntimeError(
            f"Rollback left {len(missing)} of {len(expected_sizes)} previous install files "
            "missing: " + ", ".join(missing[:5])
        )
    resized = sorted(
        rel
        for rel, size in expected_sizes.items()
        if (install_root / rel).is_file() and (install_root / rel).stat().st_size != size
    )
    if resized:
        raise RuntimeError(
            f"Rollback restored {len(resized)} previous install files with another size: "
            + ", ".join(resized[:5])
        )
    restored_pointer = _install_pointer_version(install_root)
    if restored_pointer != expected_pointer:
        raise RuntimeError(
            "Rollback did not restore ship/app/current.txt "
            f"(expected {expected_pointer!r}, found {restored_pointer!r})"
        )


def _wait_for_rehearsal_launcher_pid(
    *,
    session_root: Path,
    expected_data_dir: Path,
    timeout_s: float,
) -> int:
    """Return the pid of the app the helper relaunches before readiness."""
    deps = _root()
    deadline = deps.time.monotonic() + max(10.0, float(timeout_s))
    while deps.time.monotonic() < deadline:
        session = deps.desktop_update_mod.read_desktop_session_state(session_root)
        data_dir = Path(str(session.get("dataDir") or "")).expanduser().resolve()
        pid = int(session.get("launcherPid") or 0)
        if pid > 0 and data_dir == expected_data_dir.resolve():
            return pid
        deps.time.sleep(0.1)
    raise TimeoutError(
        "The relaunched app did not write a desktop session before the rollback finished."
    )


def _wait_for_rollback_completion(*, paths: Any, timeout_s: float) -> dict[str, Any]:
    deps = _root()
    deadline = deps.time.monotonic() + max(20.0, float(timeout_s))
    last_status: dict[str, Any] = {}
    while deps.time.monotonic() < deadline:
        last_status = _load_update_install_state(paths)
        stage = str(last_status.get("installStage") or "").strip().lower()
        if stage in {"rolled_back", "rollback_failed"}:
            return last_status
        if stage == "failed":
            raise RuntimeError(f"Update helper failed before attempting a rollback: {last_status}")
        deps.time.sleep(0.1)
    raise TimeoutError(f"Update helper never recorded a rollback: {last_status}")


def _assert_rollback_helper_reported_failure(paths: Any, *, timeout_s: float) -> dict[str, Any]:
    deps = _root()
    deadline = deps.time.monotonic() + max(5.0, float(timeout_s))
    last_event = ""
    while deps.time.monotonic() < deadline:
        for payload in _helper_diagnostic_rows(paths):
            last_event = str(payload.get("event") or "").strip() or last_event
            if last_event == "helper_main_failed":
                stdout_payload = _read_helper_stdout_payload(paths)
                if bool(stdout_payload.get("ok")):
                    raise RuntimeError(
                        "Update helper reported success for an install that rolled back."
                    )
                return stdout_payload
        deps.time.sleep(0.1)
    raise RuntimeError(
        f"Update helper did not finish after the rollback (last event: {last_event or 'none'})."
    )


def _finish_successful_update(
    *,
    paths: Any,
    data_dir: Path,
    session_state_path: Path,
    runtime_env: dict[str, str],
    runtime_timeout_s: float,
    helper_verify_timeout_s: float,
    seeded: dict[str, Any],
) -> dict[str, Any]:
    deps = _root()
    with contextlib.suppress(OSError):
        session_state_path.unlink()
    relaunched = deps._wait_for_relaunched_runtime(
        expected_data_dir=data_dir,
        expected_version=deps.get_app_version(),
        timeout_s=max(45.0, runtime_timeout_s),
        env=runtime_env,
    )
    session = dict(relaunched.get("session") or {})
    deps._wait_for_desktop_update_helper_completion(
        paths=paths,
        timeout_s=max(helper_verify_timeout_s + 10.0, runtime_timeout_s),
    )
    deps._verify_rehearsal_local_data(data_dir, seeded)
    deps._assert_desktop_update_helper_succeeded(
        paths=paths,
        relaunch_bridge_port=int(session.get("bridgePort") or 0),
    )
    return {
        "session": session,
        "health": relaunched.get("health") or {},
        "rollback": {},
        "stoppedLauncherPid": 0,
    }


def _finish_rolled_back_update(
    *,
    paths: Any,
    data_dir: Path,
    install_root: Path,
    session_root: Path,
    session_state_path: Path,
    runtime_env: dict[str, str],
    runtime_timeout_s: float,
    helper_verify_timeout_s: float,
    seeded: dict[str, Any],
    previous_tree: dict[str, int],
    previous_pointer: str,
) -> dict[str, Any]:
    deps = _root()
    with contextlib.suppress(OSError):
        session_state_path.unlink()
    new_app_pid = _wait_for_rehearsal_launcher_pid(
        session_root=session_root,
        expected_data_dir=data_dir,
        timeout_s=max(30.0, runtime_timeout_s),
    )
    status = _wait_for_rollback_completion(
        paths=paths,
        timeout_s=max(helper_verify_timeout_s + 90.0, runtime_timeout_s),
    )
    if str(status.get("installStage") or "").strip().lower() != "rolled_back":
        raise RuntimeError(f"Update rollback was not verified: {status}")
    if not bool(status.get("rollbackVerified")):
        raise RuntimeError(f"Update helper reported an unverified rollback: {status}")
    stopped_pid = int(status.get("rollbackStoppedPid") or 0)
    if stopped_pid != new_app_pid or not bool(status.get("rollbackStopVerified")):
        raise RuntimeError(
            "Rollback did not record stopping the relaunched app "
            f"(stopped pid {stopped_pid}, expected {new_app_pid}): {status}"
        )
    if _pid_is_running(new_app_pid):
        raise RuntimeError(f"Rollback left the failed app running (pid {new_app_pid}).")
    _assert_rollback_helper_reported_failure(
        paths,
        timeout_s=max(30.0, runtime_timeout_s),
    )
    _assert_restored_previous_tree(install_root, previous_tree, previous_pointer)
    relaunched = deps._wait_for_relaunched_runtime(
        expected_data_dir=data_dir,
        expected_version=_previous_reported_version(runtime_env),
        timeout_s=max(45.0, runtime_timeout_s),
        env=runtime_env,
    )
    session = dict(relaunched.get("session") or {})
    restored_pid = int(session.get("launcherPid") or 0)
    if restored_pid <= 0 or restored_pid == new_app_pid:
        raise RuntimeError(
            "Rollback did not relaunch the previous install as a new process: " + str(session)
        )
    deps._verify_rehearsal_local_data(data_dir, seeded)
    return {
        "session": session,
        "health": relaunched.get("health") or {},
        "rollback": {
            "installState": status.get("installState"),
            "installStage": status.get("installStage"),
            "rollbackVerified": bool(status.get("rollbackVerified")),
            "rollbackFiles": int(status.get("rollbackFiles") or 0),
            "rollbackVersion": str(status.get("rollbackVersion") or ""),
            "lastError": str(status.get("lastError") or ""),
            "stoppedLauncherPid": stopped_pid,
            "previousPointerVersion": previous_pointer,
            "restoredFileCount": len(previous_tree),
        },
        "stoppedLauncherPid": new_app_pid,
    }


def _finish_update_rehearsal(
    *,
    mode: str,
    paths: Any,
    data_dir: Path,
    install_root: Path,
    session_root: Path,
    session_state_path: Path,
    runtime_env: dict[str, str],
    runtime_timeout_s: float,
    helper_verify_timeout_s: float,
    seeded: dict[str, Any],
    previous_tree: dict[str, int],
    previous_pointer: str,
) -> dict[str, Any]:
    if mode == _REHEARSAL_MODE_ROLLBACK:
        return _finish_rolled_back_update(
            paths=paths,
            data_dir=data_dir,
            install_root=install_root,
            session_root=session_root,
            session_state_path=session_state_path,
            runtime_env=runtime_env,
            runtime_timeout_s=runtime_timeout_s,
            helper_verify_timeout_s=helper_verify_timeout_s,
            seeded=seeded,
            previous_tree=previous_tree,
            previous_pointer=previous_pointer,
        )
    return _finish_successful_update(
        paths=paths,
        data_dir=data_dir,
        session_state_path=session_state_path,
        runtime_env=runtime_env,
        runtime_timeout_s=runtime_timeout_s,
        helper_verify_timeout_s=helper_verify_timeout_s,
        seeded=seeded,
    )


def run_desktop_update_rehearsal(
    *,
    exe_path: Path,
    artifacts_dir: Path,
    runtime_timeout_s: float,
    mode: str = _REHEARSAL_MODE_SUCCESS,
) -> dict[str, Any]:
    """Drive the real updater helper through install handoff and relaunch.

    ``mode='rollback'`` publishes an unreachable target version so the helper's
    readiness check fails, and asserts the rollback instead of the upgraded app;
    both modes share the manifest server, signed release, download, and helper.
    """
    deps = _root()
    started = time.perf_counter()
    portable_root = exe_path.parent.resolve()
    if deps.Ed25519SigningClass is None:
        raise RuntimeError("Desktop update rehearsal requires Ed25519 signing support.")
    private_key = deps.Ed25519SigningClass.generate()
    public_key_b64 = base64.b64encode(private_key.public_key().public_bytes_raw()).decode("ascii")
    key_id = "desktop-ed25519-rehearsal"
    install_root, target_root, source_psutil_removed = _prepare_desktop_update_rehearsal_roots(
        portable_root=portable_root,
        artifacts_dir=artifacts_dir,
        public_keys={key_id: public_key_b64},
    )
    install_exe = install_root / "Baluffo.exe"
    data_dir = install_root / "ship" / "data"
    seeded = deps._seed_rehearsal_local_data(data_dir)
    target_zip = deps._archive_portable_dir(
        target_root,
        artifacts_dir / "baluffo-portable-update.zip",
    )
    manifest = {
        "schema_version": deps.DESKTOP_UPDATE_SCHEMA_VERSION,
        "key_id": key_id,
        "channel": deps.update_manifest_helpers_mod.DESKTOP_UPDATE_CHANNEL,
        "version": _rehearsal_manifest_version(mode, deps.get_app_version()),
        "published_at": deps.utc_now_iso(),
        "release_notes_url": "",
        "min_desktop_updater_version": deps.DESKTOP_UPDATER_VERSION,
        "min_supported_current_version": "0.0.0",
        "data_schema_version": "1",
        "rollback_allowed": True,
        "portable_artifact": {
            "url": "",
            "sha256": deps.compute_sha256(target_zip),
            "size_bytes": int(target_zip.stat().st_size),
        },
        "migration_plan": [],
    }

    server = None
    server_thread = None
    process = None
    stdout_handle = None
    stderr_handle = None
    relaunch_launcher_pid = 0
    relaunch_site_port = 0
    relaunch_bridge_port = 0
    initial_site_port = 0
    initial_bridge_port = 0
    helper_verify_timeout_s = _helper_verify_timeout_s_for_mode(mode, runtime_timeout_s)
    try:
        base_url, server, server_thread = deps._start_desktop_update_release_server(
            manifest=manifest,
            portable_zip=target_zip,
        )
        manifest["release_notes_url"] = f"{base_url}/release-notes"
        manifest["portable_artifact"]["url"] = f"{base_url}/assets/baluffo-portable-update.zip"
        manifest["signature"] = deps.sign_manifest(
            manifest,
            private_key.private_bytes_raw(),
        )

        runtime_env = deps.os.environ.copy()
        runtime_env.update(
            {
                "BALUFFO_APP_VERSION_OVERRIDE": "0.0.9",
                "BALUFFO_DESKTOP_NO_BROWSER": "1",
                "BALUFFO_DESKTOP_UPDATE_REPO": "local/baluffo-smoke",
                "BALUFFO_DESKTOP_UPDATE_GITHUB_API_BASE": base_url,
                "BALUFFO_DESKTOP_UPDATE_PUBLIC_KEYS_JSON": json.dumps({key_id: public_key_b64}),
                "BALUFFO_DESKTOP_UPDATER_NO_DIALOG": "1",
                "BALUFFO_DESKTOP_UPDATER_VERIFY_TIMEOUT_S": f"{helper_verify_timeout_s:g}",
            }
        )
        runtime_env.update(
            deps.packaged_runtime_env_overrides(
                artifacts_dir=artifacts_dir,
                session_scope="desktop-update-rehearsal",
            )
        )
        runtime_env.update(deps._preferred_desktop_browser_env())
        deps.clear_packaged_desktop_session_state(runtime_env)
        initial_site_port = deps.choose_free_port()
        initial_bridge_port = deps.choose_free_port()
        stdout_path = artifacts_dir / "desktop-update-rehearsal.stdout.log"
        stderr_path = artifacts_dir / "desktop-update-rehearsal.stderr.log"
        process, stdout_handle, stderr_handle = deps.launch_packaged_exe(
            install_exe,
            site_port=initial_site_port,
            bridge_port=initial_bridge_port,
            data_dir=data_dir,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            open_path="jobs.html",
            startup_probe=False,
            env=runtime_env,
        )
        deps.wait_for_packaged_runtime(
            process,
            site_base_url=f"http://127.0.0.1:{initial_site_port}",
            bridge_base_url=f"http://127.0.0.1:{initial_bridge_port}",
            timeout_s=runtime_timeout_s,
            open_path="jobs.html",
        )

        status_code, check_payload = deps.post_json(
            f"http://127.0.0.1:{initial_bridge_port}/app/check-for-update",
            {"force": True},
            timeout_s=10.0,
        )
        if status_code != 200:
            raise RuntimeError(f"Update check failed: {check_payload}")
        check_status = (
            dict(check_payload.get("status") or {})
            if isinstance(check_payload.get("status"), dict)
            else dict(check_payload)
            if isinstance(check_payload, dict)
            else {}
        )
        if (
            not bool(check_status.get("updateAvailable"))
            or str(check_status.get("availability") or "") != "available"
        ):
            raise RuntimeError(f"Update check did not surface an available release: {check_status}")
        paths = deps.desktop_update_mod.DesktopUpdatePaths.from_data_dir(data_dir)
        previous_tree = _install_tree_sizes(install_root, mode=mode)
        previous_pointer = _install_pointer_version(install_root)
        status_code, download_payload = deps.post_json(
            f"http://127.0.0.1:{initial_bridge_port}/app/download-update",
            {},
            timeout_s=10.0,
        )
        if status_code != 200:
            raise RuntimeError(f"Update download could not start: {download_payload}")
        if not bool(download_payload.get("started")):
            raise RuntimeError(f"Update download did not start: {download_payload}")
        download_status = (
            dict(download_payload.get("status") or {})
            if isinstance(download_payload.get("status"), dict)
            else {}
        )
        download_deadline = deps.time.monotonic() + max(20.0, runtime_timeout_s)
        while True:
            download_state = str(download_status.get("downloadState") or "").strip().lower()
            install_state = str(download_status.get("installState") or "").strip().lower()
            if download_state == "downloaded" or install_state == "ready":
                break
            if download_state == "failed":
                raise RuntimeError(f"Update download failed during rehearsal: {download_status}")
            if deps.time.monotonic() >= download_deadline:
                raise RuntimeError(f"Update download did not finish in time: {download_status}")
            deps.time.sleep(0.2)
            status_code, download_status = deps.request_json(
                f"http://127.0.0.1:{initial_bridge_port}/app/update-status?t={deps.time.time_ns()}",
                timeout_s=10.0,
            )
            if status_code != 200:
                raise RuntimeError(
                    "Update status poll failed during rehearsal: "
                    f"{download_status or {'status': status_code}}"
                )
        status_code, install_payload = _post_install_update_handoff(
            bridge_port=initial_bridge_port,
            paths=paths,
            timeout_s=max(30.0, runtime_timeout_s),
        )
        if status_code != 200:
            raise RuntimeError(f"Update install handoff could not start: {install_payload}")
        if not bool(install_payload.get("started")):
            raise RuntimeError(f"Update install handoff did not start: {install_payload}")
        session_root = deps.desktop_update_mod.resolve_desktop_session_root(runtime_env)
        session_state_path = session_root / deps.DESKTOP_SESSION_STATE_FILE
        deps._wait_for_install_handoff_confirmation(
            bridge_port=initial_bridge_port,
            paths=paths,
            process=process,
            timeout_s=max(20.0, runtime_timeout_s),
        )
        deps._wait_for_process_exit(process, timeout_s=max(20.0, runtime_timeout_s))
        outcome = _finish_update_rehearsal(
            mode=mode,
            paths=paths,
            data_dir=data_dir,
            install_root=install_root,
            session_root=session_root,
            session_state_path=session_state_path,
            runtime_env=runtime_env,
            runtime_timeout_s=runtime_timeout_s,
            helper_verify_timeout_s=helper_verify_timeout_s,
            seeded=seeded,
            previous_tree=previous_tree,
            previous_pointer=previous_pointer,
        )
        relaunch_session: dict[str, Any] = (
            cast(dict[str, Any], outcome.get("session"))
            if isinstance(outcome.get("session"), dict)
            else {}
        )
        relaunch_launcher_pid = int(relaunch_session.get("launcherPid") or 0)
        relaunch_bridge_port = int(relaunch_session.get("bridgePort") or 0)
        relaunch_site_port = int(relaunch_session.get("sitePort") or 0)
        return {
            "name": "Packaged desktop updater rehearsal",
            "slug": "desktop-update-rehearsal",
            "status": "passed",
            "durationMs": int((time.perf_counter() - started) * 1000),
            "error": "",
            "details": {
                "installRoot": str(install_root),
                "targetZip": str(target_zip),
                "releaseBaseUrl": str(base_url),
                "relaunchBridgePort": relaunch_bridge_port,
                "relaunchHealth": outcome.get("health") or {},
                "rollback": outcome.get("rollback") or {},
                "stoppedLauncherPid": int(outcome.get("stoppedLauncherPid") or 0),
                "helperStdoutLog": str(paths.helper_stdout_log_path),
                "helperStderrLog": str(paths.helper_stderr_log_path),
                "helperDiagnosticsLog": str(paths.helper_diagnostics_log_path),
                "sourcePsutilRemoved": source_psutil_removed,
            },
        }
    except (OSError, RuntimeError, TimeoutError, ValueError) as exc:
        return {
            "name": "Packaged desktop updater rehearsal",
            "slug": "desktop-update-rehearsal",
            "status": "failed",
            "durationMs": int((time.perf_counter() - started) * 1000),
            "error": str(exc),
        }
    finally:
        deps.terminate_process_tree(process)
        if stdout_handle is not None:
            stdout_handle.close()
        if stderr_handle is not None:
            stderr_handle.close()
        if relaunch_launcher_pid > 0:
            with contextlib.suppress(OSError, subprocess.SubprocessError):
                if deps.os.name == "nt":
                    deps.subprocess.run(
                        ["taskkill", "/PID", str(relaunch_launcher_pid), "/T", "/F"],
                        stdout=deps.subprocess.DEVNULL,
                        stderr=deps.subprocess.DEVNULL,
                        check=False,
                        timeout=15,
                    )
        if server is not None:
            server.shutdown()
            server.server_close()
        if server_thread is not None:
            server_thread.join(timeout=2.0)
        deps.cleanup_orphaned_desktop_ports_nt(
            initial_site_port,
            initial_bridge_port,
            relaunch_site_port,
            relaunch_bridge_port,
        )


def run_desktop_update_path_rehearsal(
    *,
    exe_path: Path,
    artifacts_dir: Path,
    runtime_timeout_s: float,
) -> dict[str, Any]:
    """Update an installed copy from an older reported version and prove it boots.

    The install reports 0.0.9 through the rehearsal's version override, the helper
    replaces it with the current build, and the lane requires the relaunched bridge
    to answer `/ops/health` with `startupReady: true` over a complete installed
    tree -- not only the files the smoke happens to read.
    """
    result = run_desktop_update_rehearsal(
        exe_path=exe_path,
        artifacts_dir=artifacts_dir,
        runtime_timeout_s=runtime_timeout_s,
    )
    result["name"] = "Packaged desktop update path rehearsal"
    result["slug"] = "desktop-update-path-rehearsal"
    details = (
        cast(dict[str, Any], result.get("details"))
        if isinstance(result.get("details"), dict)
        else {}
    )
    health = details.get("relaunchHealth")
    if result.get("status") != "passed":
        return result
    if not bool(isinstance(health, dict) and health.get("startupReady")):
        return {
            **result,
            "status": "failed",
            "error": "Relaunched runtime did not report startupReady after the update.",
        }
    try:
        _assert_installed_tree_matches_target(
            Path(str(details.get("installRoot") or "")),
            Path(str(details.get("targetZip") or "")),
        )
    except RuntimeError as exc:
        return {**result, "status": "failed", "error": str(exc)}
    return result


def run_desktop_update_rollback_rehearsal(
    *,
    exe_path: Path,
    artifacts_dir: Path,
    runtime_timeout_s: float,
) -> dict[str, Any]:
    """Force the real update's readiness check to fail and prove the rollback saves it.

    The install reports 0.0.9 and a real signed update replaces it with the current
    build, but the manifest publishes the next patch version, which the build can
    never report. The helper's readiness check therefore fails after launching the
    new app, and the lane requires a verified ``rolled_back`` install with that app
    stopped, every previous file and ``current.txt`` back on disk, and the previous
    app answering ``/ops/health`` with ``startupReady: true``.
    """
    result = run_desktop_update_rehearsal(
        exe_path=exe_path,
        artifacts_dir=artifacts_dir,
        runtime_timeout_s=runtime_timeout_s,
        mode=_REHEARSAL_MODE_ROLLBACK,
    )
    result["name"] = "Packaged desktop update rollback rehearsal"
    result["slug"] = "desktop-update-rollback-rehearsal"
    if result.get("status") != "passed":
        return result
    details = (
        cast(dict[str, Any], result.get("details"))
        if isinstance(result.get("details"), dict)
        else {}
    )
    rollback = details.get("rollback")
    if not bool(isinstance(rollback, dict) and rollback.get("rollbackVerified")):
        return {
            **result,
            "status": "failed",
            "error": "Rollback was not verified by the update helper.",
        }
    health = details.get("relaunchHealth")
    if not bool(isinstance(health, dict) and health.get("startupReady")):
        return {
            **result,
            "status": "failed",
            "error": "Restored app did not report startupReady after the rollback.",
        }
    return result


_REPAIR_ACTION_SNAPSHOT = "restore-snapshot"
_REPAIR_ACTION_REEXTRACT = "reextract-zip"


def _create_terminal_repair_snapshot(install_root: Path, rollback_root: Path) -> int:
    """Build the complete snapshot the helper's restore mode expects.

    The real install writes this while it snapshots the last-good tree, so the
    rehearsal has to carry the same shape: every install file under ``runtime``
    except the external data root, which a repair must never touch.
    """
    snapshot_root = rollback_root / "runtime"
    if snapshot_root.exists():
        shutil.rmtree(snapshot_root)
    copied = 0
    for path in install_root.rglob("*"):
        if not path.is_file():
            continue
        rel = path.relative_to(install_root)
        if rel.as_posix().startswith("ship/data/"):
            continue
        target = snapshot_root / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target)
        copied += 1
    pointer = snapshot_root / "ship" / "app" / "current.txt"
    if not (snapshot_root / "Baluffo.exe").is_file() or not pointer.is_file():
        raise RuntimeError("Repair rehearsal could not build a complete install snapshot.")
    return copied


def _write_failed_update_marker(install_root: Path) -> Path:
    """Leave a file behind that only the failed update tree carries."""
    marker = install_root / "ship" / "app" / "desktop-update-failed-rehearsal.txt"
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text("failed update left this file in the install\n", encoding="utf-8")
    return marker


def _seed_terminal_rollback_status(
    paths: Any,
    *,
    rollback_path: Path,
    target_version: str,
    previous_version: str,
) -> dict[str, Any]:
    """Create the terminal state the failed updater leaves for the repair panel."""
    deps = _root()
    status = _load_desktop_update_status(
        paths,
        current_version=previous_version,
    )
    status.update(
        {
            "availability": "available",
            "updateAvailable": bool(target_version),
            "latestVersion": str(target_version),
            "targetVersion": str(target_version),
            "downloadState": "idle",
            "downloadedBytes": 0,
            "totalBytes": 0,
            "downloadPercent": 0,
            "downloadedZipPath": "",
            "installState": "failed",
            "installStage": "rollback_failed",
            "installStageLabel": "",
            "lastError": (
                "install_restore_failed: repair rehearsal seeded a terminal rollback failure"
            ),
            "rollbackPath": str(rollback_path),
            "rollbackVerified": False,
            "migrationBackupPath": "",
            "repairState": "idle",
            "repairAvailable": True,
            "repairAction": "",
            "repairError": "",
            "helperUpdatedAt": deps.utc_now_iso(),
        }
    )
    return dict(_save_desktop_update_status(paths, status))


def _post_repair_handoff(
    *,
    bridge_port: int,
    paths: Any,
    timeout_s: float,
) -> tuple[int, dict[str, Any]]:
    deps = _root()
    try:
        return cast(
            tuple[int, dict[str, Any]],
            deps.post_json(
                f"http://127.0.0.1:{bridge_port}/app/repair-update",
                {},
                timeout_s=timeout_s,
            ),
        )
    except OSError:
        status = _load_update_install_state(paths)
        if str(status.get("repairState") or "").strip().lower() != "repairing":
            raise
        return 200, {"started": True, "status": status, "exitRequested": True}


def _wait_for_terminal_repair_result(
    *,
    paths: Any,
    action: str,
    timeout_s: float,
) -> dict[str, Any]:
    deps = _root()
    deadline = deps.time.monotonic() + max(20.0, float(timeout_s))
    last_status: dict[str, Any] = {}
    while deps.time.monotonic() < deadline:
        last_status = _load_update_install_state(paths)
        repair_state = str(last_status.get("repairState") or "").strip().lower()
        if repair_state == "repair_failed":
            raise RuntimeError(f"Update helper could not repair the install: {last_status}")
        if repair_state == "repaired":
            last_repair = last_status.get("lastRepair")
            repaired_action = (
                str(last_repair.get("action") or "") if isinstance(last_repair, dict) else ""
            )
            if repaired_action != action:
                raise RuntimeError(
                    f"Update helper repaired with {repaired_action!r}, expected {action!r}: "
                    f"{last_status}"
                )
            return last_status
        deps.time.sleep(0.2)
    raise TimeoutError(f"Update helper did not finish the {action} repair: {last_status}")


def _wait_for_repair_helper_success(paths: Any, *, timeout_s: float) -> dict[str, Any]:
    """Read the helper's terminal JSON, failing on a diagnostic failure first."""
    deps = _root()
    deadline = deps.time.monotonic() + max(5.0, float(timeout_s))
    last_event = ""
    while deps.time.monotonic() < deadline:
        payload = _read_helper_stdout_payload(paths)
        if payload:
            if not bool(payload.get("ok")):
                raise RuntimeError(
                    f"Update helper reported a repair failure: {payload.get('error') or payload}"
                )
            return payload
        for row in _helper_diagnostic_rows(paths):
            event = str(row.get("event") or "").strip()
            if event:
                last_event = event
            if event == "helper_main_failed":
                raise RuntimeError(
                    "Update helper diagnostics reported a repair failure: "
                    f"{_helper_failure_message(row)}"
                )
        deps.time.sleep(0.1)
    raise TimeoutError(
        f"Update helper did not report repair completion (last event: {last_event or 'none'})."
    )


def _prepare_terminal_repair_install(
    *,
    mode: str,
    install_root: Path,
    data_dir: Path,
) -> tuple[Path, dict[str, int], str]:
    """Seed the retained snapshot the repair lane needs, before the app starts."""
    rollback_root = (data_dir / "updates" / "rollback" / f"packaged-repair-{mode}").resolve()
    previous_tree: dict[str, int] = {}
    previous_pointer = ""
    if mode == _REPAIR_ACTION_SNAPSHOT:
        if rollback_root.exists():
            shutil.rmtree(rollback_root)
        rollback_root.mkdir(parents=True, exist_ok=True)
        _create_terminal_repair_snapshot(install_root, rollback_root)
        previous_tree = _install_tree_sizes_all(install_root)
        previous_pointer = _install_pointer_version(install_root)
    return rollback_root, previous_tree, previous_pointer


def _cache_repair_release_manifest(
    *,
    paths: Any,
    bridge_port: int,
    build_version: str,
) -> str:
    """Ask the real update check to cache the signed manifest for the build."""
    deps = _root()
    status_code, check_payload = deps.post_json(
        f"http://127.0.0.1:{bridge_port}/app/check-for-update",
        {"force": True},
        timeout_s=10.0,
    )
    if status_code != 200:
        raise RuntimeError(f"Update check failed before the repair: {check_payload}")
    check_status = (
        dict(check_payload.get("status") or {})
        if isinstance(check_payload.get("status"), dict)
        else dict(check_payload)
        if isinstance(check_payload, dict)
        else {}
    )
    if not bool(check_status.get("updateAvailable")):
        raise RuntimeError(
            f"Update check did not cache the release ZIP the repair needs: {check_status}"
        )
    target_version = str(check_status.get("targetVersion") or "").strip()
    if target_version != build_version:
        raise RuntimeError(
            "Repair rehearsal release does not match the installed build "
            f"(expected {build_version!r}, found {target_version!r})."
        )
    if not paths.manifest_cache_path.is_file():
        raise RuntimeError("Update check did not cache the signed repair manifest.")
    return target_version


def _start_terminal_repair(
    *,
    mode: str,
    paths: Any,
    bridge_port: int,
    rollback_root: Path,
    target_version: str,
    runtime_timeout_s: float,
) -> None:
    """Seed the terminal failure and hand it to the panel repair route."""
    seeded_status = _seed_terminal_rollback_status(
        paths,
        rollback_path=rollback_root,
        target_version=target_version,
        previous_version="0.0.9",
    )
    if str(seeded_status.get("installStage") or "").strip().lower() != "rollback_failed":
        raise RuntimeError(f"Rehearsal could not seed a terminal install failure: {seeded_status}")
    status_code, repair_payload = _post_repair_handoff(
        bridge_port=bridge_port,
        paths=paths,
        timeout_s=max(30.0, runtime_timeout_s),
    )
    if status_code != 200:
        raise RuntimeError(f"Panel repair handoff could not start: {repair_payload}")
    if not bool(repair_payload.get("started")) or not bool(repair_payload.get("exitRequested")):
        raise RuntimeError(
            f"Panel repair handoff did not request the app to close: {repair_payload}"
        )
    handoff_status = (
        dict(repair_payload.get("status") or {})
        if isinstance(repair_payload.get("status"), dict)
        else {}
    )
    if str(handoff_status.get("repairState") or "").strip().lower() != "repairing":
        raise RuntimeError(f"Panel repair handoff is not in progress: {handoff_status}")
    if str(handoff_status.get("repairAction") or "").strip().lower() != mode:
        raise RuntimeError(
            f"Panel chose the {handoff_status.get('repairAction')!r} repair, expected {mode!r}."
        )


def _cleanup_desktop_repair_rehearsal(
    *,
    process: subprocess.Popen[Any] | None,
    stdout_handle: Any,
    stderr_handle: Any,
    relaunch_launcher_pid: int,
    server: http.server.ThreadingHTTPServer | None,
    server_thread: threading.Thread | None,
    ports: tuple[int, int, int, int],
) -> None:
    deps = _root()
    deps.terminate_process_tree(process)
    for handle in (stdout_handle, stderr_handle):
        if handle is not None:
            handle.close()
    if relaunch_launcher_pid > 0:
        with contextlib.suppress(OSError, subprocess.SubprocessError):
            if deps.os.name == "nt":
                deps.subprocess.run(
                    ["taskkill", "/PID", str(relaunch_launcher_pid), "/T", "/F"],
                    stdout=deps.subprocess.DEVNULL,
                    stderr=deps.subprocess.DEVNULL,
                    check=False,
                    timeout=15,
                )
    if server is not None:
        server.shutdown()
        server.server_close()
    if server_thread is not None:
        server_thread.join(timeout=2.0)
    deps.cleanup_orphaned_desktop_ports_nt(*ports)


def _assert_repair_cleared_terminal_state(status: dict[str, Any]) -> tuple[str, str]:
    install_state = str(status.get("installState") or "").strip().lower()
    install_stage = str(status.get("installStage") or "").strip().lower()
    if install_state != "idle" or install_stage != "idle":
        raise RuntimeError(f"Repair did not clear the terminal install failure: {status}")
    if str(status.get("lastError") or "").strip():
        raise RuntimeError(f"Repair left the terminal install error behind: {status}")
    return install_state, install_stage


def _assert_reextracted_install(
    repair_fields: dict[str, Any],
    install_root: Path,
    target_zip: Path,
    status: dict[str, Any],
) -> None:
    _assert_installed_tree_matches_target(install_root, target_zip)
    zip_path = str(repair_fields.get("zipPath") or "").strip()
    if not zip_path or not Path(zip_path).is_file():
        raise RuntimeError(f"Repair did not record a usable verified release ZIP: {status}")
    if not str(repair_fields.get("zipSha256") or "").strip():
        raise RuntimeError(f"Repair did not record the release ZIP digest: {status}")


def _assert_repair_result_fields(
    *,
    action: str,
    status: dict[str, Any],
    install_root: Path,
    target_zip: Path,
    target_version: str,
    previous_tree: dict[str, int],
    previous_pointer: str,
) -> dict[str, Any]:
    last_repair = status.get("lastRepair")
    repair_fields = dict(last_repair) if isinstance(last_repair, dict) else {}
    if action == _REPAIR_ACTION_SNAPSHOT:
        if not bool(status.get("rollbackVerified")):
            raise RuntimeError(f"Repair did not verify the restored snapshot: {status}")
        _assert_restored_previous_tree(install_root, previous_tree, previous_pointer)
    else:
        _assert_reextracted_install(repair_fields, install_root, target_zip, status)
    version = str(repair_fields.get("version") or "").strip()
    if version != target_version:
        raise RuntimeError(
            "Repair reported another install version "
            f"(expected {target_version!r}, found {version!r})."
        )
    return repair_fields


def _finish_repair_rehearsal(
    *,
    action: str,
    paths: Any,
    data_dir: Path,
    install_root: Path,
    session_state_path: Path,
    runtime_env: dict[str, str],
    runtime_timeout_s: float,
    helper_verify_timeout_s: float,
    seeded: dict[str, Any],
    target_zip: Path,
    target_version: str,
    marker: Path,
    rollback_root: Path,
    snapshot_root: Path,
    previous_tree: dict[str, int],
    previous_pointer: str,
    stopped_launcher_pid: int,
) -> dict[str, Any]:
    deps = _root()
    with contextlib.suppress(OSError):
        session_state_path.unlink()
    status = _wait_for_terminal_repair_result(
        paths=paths,
        action=action,
        timeout_s=max(helper_verify_timeout_s + 60.0, runtime_timeout_s),
    )
    install_state, install_stage = _assert_repair_cleared_terminal_state(status)
    relaunched = deps._wait_for_relaunched_runtime(
        expected_data_dir=data_dir,
        expected_version=deps.get_app_version(),
        timeout_s=max(45.0, runtime_timeout_s),
        env=runtime_env,
    )
    helper_payload = _wait_for_repair_helper_success(
        paths,
        timeout_s=max(helper_verify_timeout_s, runtime_timeout_s),
    )
    deps._verify_rehearsal_local_data(data_dir, seeded)
    if marker.exists():
        raise RuntimeError(f"Repair left the failed update file in the install: {marker}")
    if snapshot_root.exists():
        raise RuntimeError(f"Repair left the rollback snapshot on disk: {snapshot_root}")
    if rollback_root.exists():
        raise RuntimeError(f"Repair left its rollback path on disk: {rollback_root}")
    if not bool(helper_payload.get("snapshotRemoved")):
        raise RuntimeError(f"Update helper did not remove the rollback snapshot: {helper_payload}")
    repair_fields = _assert_repair_result_fields(
        action=action,
        status=status,
        install_root=install_root,
        target_zip=target_zip,
        target_version=target_version,
        previous_tree=previous_tree,
        previous_pointer=previous_pointer,
    )
    version = str(repair_fields.get("version") or "").strip()
    helper_action = str(helper_payload.get("action") or "").strip()
    if helper_action and helper_action != action:
        raise RuntimeError(f"Update helper ran {helper_action!r}, expected the {action!r} repair.")
    session = dict(relaunched.get("session") or {})
    return {
        "session": session,
        "health": relaunched.get("health") or {},
        "repair": {
            "action": action,
            "version": version,
            "installState": install_state,
            "installStage": install_stage,
            "repairState": str(status.get("repairState") or ""),
            "rollbackVerified": bool(status.get("rollbackVerified")),
            "snapshotPath": str(repair_fields.get("snapshotPath") or ""),
            "snapshotRemoved": bool(helper_payload.get("snapshotRemoved")),
            "zipPath": str(repair_fields.get("zipPath") or ""),
            "zipSha256": str(repair_fields.get("zipSha256") or ""),
            "files": int(helper_payload.get("files") or 0),
            "stoppedLauncherPid": stopped_launcher_pid,
        },
    }


def _run_desktop_repair_rehearsal(
    *,
    mode: str,
    exe_path: Path,
    artifacts_dir: Path,
    runtime_timeout_s: float,
) -> dict[str, Any]:
    """Drive the panel repair route and the real helper through a terminal install."""
    deps = _root()
    started = time.perf_counter()
    portable_root = exe_path.parent.resolve()
    if deps.Ed25519SigningClass is None:
        raise RuntimeError("Desktop repair rehearsal requires Ed25519 signing support.")
    private_key = deps.Ed25519SigningClass.generate()
    public_key_b64 = base64.b64encode(private_key.public_key().public_bytes_raw()).decode("ascii")
    key_id = "desktop-ed25519-rehearsal"
    install_root, target_root, source_psutil_removed = _prepare_desktop_update_rehearsal_roots(
        portable_root=portable_root,
        artifacts_dir=artifacts_dir,
        public_keys={key_id: public_key_b64},
    )
    install_exe = install_root / "Baluffo.exe"
    data_dir = install_root / "ship" / "data"
    seeded = deps._seed_rehearsal_local_data(data_dir)
    target_zip = deps._archive_portable_dir(
        target_root,
        artifacts_dir / "baluffo-portable-update.zip",
    )
    build_version = str(deps.get_app_version() or "").strip()
    manifest = {
        "schema_version": deps.DESKTOP_UPDATE_SCHEMA_VERSION,
        "key_id": key_id,
        "channel": deps.update_manifest_helpers_mod.DESKTOP_UPDATE_CHANNEL,
        "version": build_version,
        "published_at": deps.utc_now_iso(),
        "release_notes_url": "",
        "min_desktop_updater_version": deps.DESKTOP_UPDATER_VERSION,
        "min_supported_current_version": "0.0.0",
        "data_schema_version": "1",
        "rollback_allowed": True,
        "portable_artifact": {
            "url": "",
            "sha256": deps.compute_sha256(target_zip),
            "size_bytes": int(target_zip.stat().st_size),
        },
        "migration_plan": [],
    }

    server = None
    server_thread = None
    process = None
    stdout_handle = None
    stderr_handle = None
    relaunch_launcher_pid = 0
    relaunch_site_port = 0
    relaunch_bridge_port = 0
    initial_site_port = 0
    initial_bridge_port = 0
    helper_verify_timeout_s = max(30.0, float(runtime_timeout_s))
    rollback_root, previous_tree, previous_pointer = _prepare_terminal_repair_install(
        mode=mode,
        install_root=install_root,
        data_dir=data_dir,
    )
    snapshot_root = rollback_root / "runtime"
    marker = install_root / "ship" / "app" / "desktop-update-failed-rehearsal.txt"
    try:
        base_url, server, server_thread = deps._start_desktop_update_release_server(
            manifest=manifest,
            portable_zip=target_zip,
        )
        manifest["release_notes_url"] = f"{base_url}/release-notes"
        manifest["portable_artifact"]["url"] = f"{base_url}/assets/baluffo-portable-update.zip"
        manifest["signature"] = deps.sign_manifest(
            manifest,
            private_key.private_bytes_raw(),
        )

        runtime_env = deps.os.environ.copy()
        runtime_env.update(
            {
                "BALUFFO_APP_VERSION_OVERRIDE": "0.0.9",
                "BALUFFO_DESKTOP_NO_BROWSER": "1",
                "BALUFFO_DESKTOP_UPDATE_REPO": "local/baluffo-smoke",
                "BALUFFO_DESKTOP_UPDATE_GITHUB_API_BASE": base_url,
                "BALUFFO_DESKTOP_UPDATE_PUBLIC_KEYS_JSON": json.dumps({key_id: public_key_b64}),
                "BALUFFO_DESKTOP_UPDATER_NO_DIALOG": "1",
                "BALUFFO_DESKTOP_UPDATER_VERIFY_TIMEOUT_S": f"{helper_verify_timeout_s:g}",
            }
        )
        runtime_env.update(
            deps.packaged_runtime_env_overrides(
                artifacts_dir=artifacts_dir,
                session_scope=f"desktop-update-repair-{mode}-rehearsal",
            )
        )
        runtime_env.update(deps._preferred_desktop_browser_env())
        deps.clear_packaged_desktop_session_state(runtime_env)
        initial_site_port = deps.choose_free_port()
        initial_bridge_port = deps.choose_free_port()
        log_prefix = (
            "desktop-update-repair-rehearsal"
            if mode == _REPAIR_ACTION_SNAPSHOT
            else ("desktop-update-reextract-rehearsal")
        )
        process, stdout_handle, stderr_handle = deps.launch_packaged_exe(
            install_exe,
            site_port=initial_site_port,
            bridge_port=initial_bridge_port,
            data_dir=data_dir,
            stdout_path=artifacts_dir / f"{log_prefix}.stdout.log",
            stderr_path=artifacts_dir / f"{log_prefix}.stderr.log",
            open_path="jobs.html",
            startup_probe=False,
            env=runtime_env,
        )
        deps.wait_for_packaged_runtime(
            process,
            site_base_url=f"http://127.0.0.1:{initial_site_port}",
            bridge_base_url=f"http://127.0.0.1:{initial_bridge_port}",
            timeout_s=runtime_timeout_s,
            open_path="jobs.html",
        )
        paths = deps.desktop_update_mod.DesktopUpdatePaths.from_data_dir(data_dir)
        session_root = deps.desktop_update_mod.resolve_desktop_session_root(runtime_env)
        session_state_path = session_root / deps.DESKTOP_SESSION_STATE_FILE
        session_state = deps.desktop_update_mod.read_desktop_session_state(session_root)
        stopped_launcher_pid = int(session_state.get("launcherPid") or 0)

        target_version = build_version
        if mode == _REPAIR_ACTION_REEXTRACT:
            target_version = _cache_repair_release_manifest(
                paths=paths,
                bridge_port=initial_bridge_port,
                build_version=build_version,
            )
        marker = _write_failed_update_marker(install_root)
        _start_terminal_repair(
            mode=mode,
            paths=paths,
            bridge_port=initial_bridge_port,
            rollback_root=rollback_root,
            target_version=target_version,
            runtime_timeout_s=runtime_timeout_s,
        )
        deps._wait_for_process_exit(process, timeout_s=max(20.0, runtime_timeout_s))
        if stopped_launcher_pid > 0 and _pid_is_running(stopped_launcher_pid):
            raise RuntimeError(
                f"Repair handoff did not close the desktop app (pid {stopped_launcher_pid})."
            )
        outcome = _finish_repair_rehearsal(
            action=mode,
            paths=paths,
            data_dir=data_dir,
            install_root=install_root,
            session_state_path=session_state_path,
            runtime_env=runtime_env,
            runtime_timeout_s=runtime_timeout_s,
            helper_verify_timeout_s=helper_verify_timeout_s,
            seeded=seeded,
            target_zip=target_zip,
            target_version=target_version,
            marker=marker,
            rollback_root=rollback_root,
            snapshot_root=snapshot_root,
            previous_tree=previous_tree,
            previous_pointer=previous_pointer,
            stopped_launcher_pid=stopped_launcher_pid,
        )
        relaunch_session = (
            cast(dict[str, Any], outcome.get("session"))
            if isinstance(outcome.get("session"), dict)
            else {}
        )
        relaunch_launcher_pid = int(relaunch_session.get("launcherPid") or 0)
        relaunch_bridge_port = int(relaunch_session.get("bridgePort") or 0)
        relaunch_site_port = int(relaunch_session.get("sitePort") or 0)
        repair_details = (
            dict(outcome.get("repair") or {}) if isinstance(outcome.get("repair"), dict) else {}
        )
        return {
            "name": "Packaged desktop update repair rehearsal",
            "slug": "desktop-update-repair-rehearsal",
            "status": "passed",
            "durationMs": int((time.perf_counter() - started) * 1000),
            "error": "",
            "details": {
                "installRoot": str(install_root),
                "targetZip": str(target_zip),
                "releaseBaseUrl": str(base_url),
                "repair": repair_details,
                "relaunchBridgePort": relaunch_bridge_port,
                "relaunchHealth": outcome.get("health") or {},
                "stoppedLauncherPid": stopped_launcher_pid,
                "helperStdoutLog": str(paths.helper_stdout_log_path),
                "helperStderrLog": str(paths.helper_stderr_log_path),
                "helperDiagnosticsLog": str(paths.helper_diagnostics_log_path),
                "sourcePsutilRemoved": source_psutil_removed,
            },
        }
    except (OSError, RuntimeError, TimeoutError, ValueError) as exc:
        return {
            "name": "Packaged desktop update repair rehearsal",
            "slug": "desktop-update-repair-rehearsal",
            "status": "failed",
            "durationMs": int((time.perf_counter() - started) * 1000),
            "error": str(exc),
        }
    finally:
        _cleanup_desktop_repair_rehearsal(
            process=process,
            stdout_handle=stdout_handle,
            stderr_handle=stderr_handle,
            relaunch_launcher_pid=relaunch_launcher_pid,
            server=server,
            server_thread=server_thread,
            ports=(
                initial_site_port,
                initial_bridge_port,
                relaunch_site_port,
                relaunch_bridge_port,
            ),
        )


def _repair_rehearsal_result(
    result: dict[str, Any],
    *,
    action: str,
    name: str,
    slug: str,
) -> dict[str, Any]:
    """Require the lane's own proof, not just a helper that returned success."""
    result["name"] = name
    result["slug"] = slug
    if result.get("status") != "passed":
        return result
    details = (
        cast(dict[str, Any], result.get("details"))
        if isinstance(result.get("details"), dict)
        else {}
    )
    repair = details.get("repair")
    if not isinstance(repair, dict) or repair.get("action") != action:
        return {
            **result,
            "status": "failed",
            "error": f"The repair did not run through the {action} lane.",
        }
    if not bool(repair.get("snapshotRemoved")):
        return {
            **result,
            "status": "failed",
            "error": "The repair left the terminal rollback snapshot on disk.",
        }
    health = details.get("relaunchHealth")
    if not bool(isinstance(health, dict) and health.get("startupReady")):
        return {
            **result,
            "status": "failed",
            "error": "Repaired app did not report startupReady after reopening.",
        }
    return result


def run_desktop_update_repair_rehearsal(
    *,
    exe_path: Path,
    artifacts_dir: Path,
    runtime_timeout_s: float,
) -> dict[str, Any]:
    """Repair a terminal ``rollback_failed`` install from its retained snapshot.

    The lane seeds the terminal failure and a complete snapshot, asks the panel
    route to repair, and requires the launcher to close, the helper to restore
    every previous file and remove the snapshot, and the repaired app to reopen
    with the seeded local data intact.
    """
    return _repair_rehearsal_result(
        _run_desktop_repair_rehearsal(
            mode=_REPAIR_ACTION_SNAPSHOT,
            exe_path=exe_path,
            artifacts_dir=artifacts_dir,
            runtime_timeout_s=runtime_timeout_s,
        ),
        action=_REPAIR_ACTION_SNAPSHOT,
        name="Packaged desktop update repair rehearsal",
        slug="desktop-update-repair-rehearsal",
    )


def run_desktop_update_reextract_rehearsal(
    *,
    exe_path: Path,
    artifacts_dir: Path,
    runtime_timeout_s: float,
) -> dict[str, Any]:
    """Repair a terminal install with no snapshot from the verified release ZIP.

    The cached signed manifest must match the install's recorded target version,
    so the helper has to download the release ZIP, verify its digest and structure,
    rebuild the install from it, keep ``ship/data``, and clear the terminal state.
    """
    return _repair_rehearsal_result(
        _run_desktop_repair_rehearsal(
            mode=_REPAIR_ACTION_REEXTRACT,
            exe_path=exe_path,
            artifacts_dir=artifacts_dir,
            runtime_timeout_s=runtime_timeout_s,
        ),
        action=_REPAIR_ACTION_REEXTRACT,
        name="Packaged desktop update re-extract rehearsal",
        slug="desktop-update-reextract-rehearsal",
    )
