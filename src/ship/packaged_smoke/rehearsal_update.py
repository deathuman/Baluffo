"""Packaged desktop update rehearsal helpers behind the root smoke facade."""

from __future__ import annotations

import base64
import contextlib
import http.server
import json
import shutil
import subprocess
import threading
import time
from pathlib import Path
from typing import Any

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
        app_dir / deps.desktop_update_mod.PUBLIC_KEYS_FILE,
        app_dir
        / "versions"
        / current_version
        / "packaging"
        / deps.desktop_update_mod.PUBLIC_KEYS_FILE,
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
                    "name": deps.desktop_update_mod.DESKTOP_UPDATE_MANIFEST_ASSET,
                    "browser_download_url": (
                        f"{base_url}/assets/{deps.desktop_update_mod.DESKTOP_UPDATE_MANIFEST_ASSET}"
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
    except Exception:
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
        return deps.post_json(
            f"http://127.0.0.1:{bridge_port}/app/install-update",
            {},
            timeout_s=timeout_s,
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
        except Exception:
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


def _read_helper_stdout_payload(paths: Any) -> dict[str, Any]:
    if not paths.helper_stdout_log_path.is_file():
        return {}
    try:
        payload = json.loads(paths.helper_stdout_log_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return dict(payload) if isinstance(payload, dict) else {}


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
    fields = payload.get("fields") if isinstance(payload.get("fields"), dict) else {}
    return str(fields.get("error") or payload.get("error") or payload).strip()


def _load_update_install_state(paths: Any) -> dict[str, Any]:
    if not paths.install_state_path.is_file():
        return {}
    try:
        payload = json.loads(paths.install_state_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return dict(payload) if isinstance(payload, dict) else {}


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


def run_desktop_update_rehearsal(
    *,
    exe_path: Path,
    artifacts_dir: Path,
    runtime_timeout_s: float,
) -> dict[str, Any]:
    deps = _root()
    started = time.perf_counter()
    portable_root = exe_path.parent.resolve()
    if deps.desktop_update_mod.Ed25519PrivateKey is None:
        raise RuntimeError("Desktop update rehearsal requires Ed25519 signing support.")
    private_key = deps.desktop_update_mod.Ed25519PrivateKey.generate()
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
        "schema_version": deps.desktop_update_mod.DESKTOP_UPDATE_SCHEMA_VERSION,
        "key_id": key_id,
        "channel": deps.desktop_update_mod.DESKTOP_UPDATE_CHANNEL,
        "version": deps.desktop_update_mod.get_app_version(),
        "published_at": deps.utc_now_iso(),
        "release_notes_url": "",
        "min_desktop_updater_version": deps.desktop_update_mod.DESKTOP_UPDATER_VERSION,
        "min_supported_current_version": "0.0.0",
        "data_schema_version": "1",
        "rollback_allowed": True,
        "portable_artifact": {
            "url": "",
            "sha256": deps.desktop_update_mod.compute_sha256(target_zip),
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
    try:
        base_url, server, server_thread = deps._start_desktop_update_release_server(
            manifest=manifest,
            portable_zip=target_zip,
        )
        manifest["release_notes_url"] = f"{base_url}/release-notes"
        manifest["portable_artifact"]["url"] = f"{base_url}/assets/baluffo-portable-update.zip"
        manifest["signature"] = deps.desktop_update_mod.sign_manifest(
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
        with contextlib.suppress(OSError):
            session_state_path.unlink()
        relaunched = deps._wait_for_relaunched_runtime(
            expected_data_dir=data_dir,
            expected_version=deps.desktop_update_mod.get_app_version(),
            timeout_s=max(45.0, runtime_timeout_s),
            env=runtime_env,
        )
        relaunch_session = (
            relaunched.get("session") if isinstance(relaunched.get("session"), dict) else {}
        )
        relaunch_launcher_pid = int(relaunch_session.get("launcherPid") or 0)
        relaunch_bridge_port = int(relaunch_session.get("bridgePort") or 0)
        relaunch_site_port = int(relaunch_session.get("sitePort") or 0)
        deps._wait_for_desktop_update_helper_completion(
            paths=paths,
            timeout_s=max(helper_verify_timeout_s + 10.0, runtime_timeout_s),
        )
        deps._verify_rehearsal_local_data(data_dir, seeded)
        deps._assert_desktop_update_helper_succeeded(
            paths=paths,
            relaunch_bridge_port=relaunch_bridge_port,
        )
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
                "helperStdoutLog": str(paths.helper_stdout_log_path),
                "helperStderrLog": str(paths.helper_stderr_log_path),
                "helperDiagnosticsLog": str(paths.helper_diagnostics_log_path),
                "sourcePsutilRemoved": source_psutil_removed,
            },
        }
    except Exception as exc:
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
            with contextlib.suppress(Exception):
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
