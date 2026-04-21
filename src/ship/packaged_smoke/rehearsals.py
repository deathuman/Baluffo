"""Packaged rehearsal helpers behind the root smoke facade."""

from __future__ import annotations

import base64
import contextlib
import http.server
import json
import os
import shutil
import subprocess
import threading
import time
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

root: Any | None = None


def _root() -> Any:
    if root is None:
        raise RuntimeError("packaged_smoke.rehearsals.root is not configured")
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
        app_dir / "versions" / current_version / "packaging" / deps.desktop_update_mod.PUBLIC_KEYS_FILE,
    ]
    for target in targets:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(payload, encoding="utf-8")


def _portable_current_version(portable_root: Path) -> str:
    current_version_path = portable_root / "ship" / "app" / "current.txt"
    current_version = str(current_version_path.read_text(encoding="utf-8").strip())
    if not current_version:
        raise RuntimeError(
            f"Portable build is missing current version metadata: {current_version_path}"
        )
    return current_version


def _portable_packaged_sync_config_path(portable_root: Path) -> Path:
    current_version = _portable_current_version(portable_root)
    config_path = (
        portable_root
        / "ship"
        / "app"
        / "versions"
        / current_version
        / "packaging"
        / "github-app-sync-config.json"
    )
    if not config_path.is_file():
        raise RuntimeError(f"Portable build is missing packaged sync config: {config_path}")
    return config_path


def _load_portable_packaged_sync_rehearsal_config(
    portable_root: Path,
) -> tuple[Path, dict[str, Any], Any]:
    deps = _root()
    config_path = deps._portable_packaged_sync_config_path(portable_root)
    try:
        payload = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            f"Could not read packaged sync config from {config_path}: {exc}"
        ) from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"Packaged sync config at {config_path} must be a JSON object.")
    normalized = deps.source_sync_mod._normalize_packaged_payload(payload)  # noqa: SLF001
    if normalized.get("keyDerivation") == deps.source_sync_mod.KEY_DERIVATION_MACHINE:
        raise RuntimeError(
            "Packaged sync rehearsal requires a portable embedded config, but "
            f"{config_path} uses keyDerivation=machine."
        )
    loaded = deps.source_sync_mod.load_packaged_sync_config(
        env={**deps.os.environ, deps.source_sync_mod.PACKAGED_SYNC_CONFIG_ENV: str(config_path)}
    )
    if loaded is None:
        raise RuntimeError(f"Could not load packaged sync config from {config_path}.")
    if not str(loaded.private_key_pem or "").strip():
        detail = str(loaded.decryption_error or "missing decrypted private key").strip()
        raise RuntimeError(
            f"Could not decrypt packaged GitHub App key from {config_path}: {detail}"
        )
    return config_path, payload, loaded


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


class _PackagedSyncRehearsalHandler(http.server.BaseHTTPRequestHandler):
    def __init__(
        self,
        *args: Any,
        installation_id: str,
        repo: str,
        branch: str,
        remote_path: str,
        snapshot_payload: dict[str, Any],
        stats: dict[str, Any],
        **kwargs: Any,
    ) -> None:
        self._installation_id = str(installation_id or "")
        self._repo = str(repo or "")
        self._branch = str(branch or "")
        self._remote_path = str(remote_path or "")
        self._snapshot_payload = dict(snapshot_payload)
        self._stats = stats
        super().__init__(*args, **kwargs)

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        return

    def _send_json(self, payload: dict[str, Any], status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(int(status))
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlsplit(self.path)
        expected_path = f"/app/installations/{self._installation_id}/access_tokens"
        if parsed.path != expected_path:
            self.send_error(404)
            return
        auth_header = str(self.headers.get("Authorization") or "").strip()
        if not auth_header.startswith("Bearer "):
            self._send_json({"message": "Missing GitHub App bearer token."}, status=401)
            return
        self._stats["tokenRequests"] = int(self._stats.get("tokenRequests") or 0) + 1
        self._stats["lastJwtPrefix"] = auth_header[:32]
        self._send_json(
            {
                "token": "packaged-sync-rehearsal-token",
                "expires_at": "2099-01-01T00:00:00+00:00",
            }
        )

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlsplit(self.path)
        expected_prefix = f"/repos/{self._repo}/contents/"
        if not parsed.path.startswith(expected_prefix):
            self.send_error(404)
            return
        if parsed.path[len(expected_prefix) :] != self._remote_path:
            self.send_error(404)
            return
        query = parse_qs(parsed.query or "")
        if str((query.get("ref") or [""])[0]) != self._branch:
            self._send_json({"message": "Unexpected sync branch."}, status=404)
            return
        auth_header = str(self.headers.get("Authorization") or "").strip()
        if auth_header != "Bearer packaged-sync-rehearsal-token":
            self._send_json({"message": "Unexpected rehearsal access token."}, status=401)
            return
        self._stats["contentRequests"] = int(self._stats.get("contentRequests") or 0) + 1
        encoded = base64.b64encode(
            json.dumps(self._snapshot_payload, ensure_ascii=False).encode("utf-8")
        ).decode("ascii")
        self._send_json(
            {
                "sha": "packaged-sync-rehearsal-sha",
                "content": encoded,
                "encoding": "base64",
            }
        )


_PackagedSyncRehearsalHandler.protocol_version = "HTTP/1.1"


def _start_packaged_sync_rehearsal_server(
    *,
    packaged_config: Any,
    snapshot_payload: dict[str, Any],
) -> tuple[str, dict[str, Any], http.server.ThreadingHTTPServer, threading.Thread]:
    stats: dict[str, Any] = {"tokenRequests": 0, "contentRequests": 0, "lastJwtPrefix": ""}

    def _handler_factory(*args: Any, **kwargs: Any) -> _PackagedSyncRehearsalHandler:
        return _PackagedSyncRehearsalHandler(
            *args,
            installation_id=packaged_config.installation_id,
            repo=packaged_config.repo,
            branch=packaged_config.branch,
            remote_path=packaged_config.path,
            snapshot_payload=snapshot_payload,
            stats=stats,
            **kwargs,
        )

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _handler_factory)
    thread = threading.Thread(
        target=server.serve_forever,
        daemon=True,
        name="packaged-sync-rehearsal-server",
    )
    thread.start()
    return f"http://127.0.0.1:{int(server.server_port)}", stats, server, thread


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

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
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

    def do_GET(self) -> None:  # noqa: N802
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
        handoff_marker_exists = paths.handoff_request_path.exists()
        status_code, status_payload = deps.request_json(
            f"http://127.0.0.1:{bridge_port}/app/update-status?t={deps.time.time_ns()}",
            timeout_s=10.0,
        )
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


def _wait_for_pid_exit(pid: int, *, timeout_s: float) -> None:
    deps = _root()
    deadline = deps.time.monotonic() + max(5.0, float(timeout_s))
    while deps.time.monotonic() < deadline:
        if not deps.desktop_app_mod.is_process_alive(int(pid or 0)):
            return
        deps.time.sleep(0.5)
    raise TimeoutError(f"Managed browser pid {int(pid or 0)} remained alive after launcher exit.")


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
        if Path(str(session.get("dataDir") or "")).expanduser().resolve() != expected_data_dir.resolve():
            deps.time.sleep(0.75)
            continue
        bridge_port = int(session.get("bridgePort") or 0)
        if bridge_port <= 0:
            deps.time.sleep(0.75)
            continue
        try:
            last_health = deps.fetch_json(f"http://127.0.0.1:{bridge_port}/ops/health", timeout_s=5.0)
        except Exception:
            last_health = {}
            deps.time.sleep(0.75)
            continue
        if (
            isinstance(last_health, dict)
            and bool(last_health.get("desktopMode"))
            and bool(last_health.get("startupReady"))
            and str(last_health.get("appVersion") or "").strip() == str(expected_version or "").strip()
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


def _preferred_desktop_browser_env() -> dict[str, str]:
    try:
        from src.ship.desktop_app import resolve_chromium_browser_candidates
    except Exception:
        return {}
    candidates = resolve_chromium_browser_candidates()
    browser_path = str((candidates[0] or {}).get("path") or "").strip() if candidates else ""
    return {"BALUFFO_DESKTOP_BROWSER_PATH": browser_path} if browser_path else {}


def _select_packaged_browser_job_browser(
    env: dict[str, str] | None = None,
) -> tuple[dict[str, str], dict[str, str]]:
    deps = _root()
    env_map = dict(env or deps.os.environ)
    try:
        selected = deps.select_startup_probe_browser(env_map)
    except RuntimeError as base_exc:
        edge_env = dict(env_map)
        edge_env["BALUFFO_DESKTOP_ALLOW_EDGE_APP_MODE"] = "1"
        try:
            selected = deps.select_startup_probe_browser(edge_env)
            env_map = edge_env
        except RuntimeError as edge_exc:
            raise RuntimeError(str(base_exc)) from edge_exc
    browser_name = str(selected.get("browserName") or "").strip().lower()
    browser_path = str(selected.get("browserPath") or "").strip()
    if not browser_name or not browser_path:
        raise RuntimeError("Packaged browser job rehearsal could not resolve a managed browser.")
    env_overrides = {deps.desktop_app_mod.PREFERRED_BROWSER_PATH_ENV: browser_path}
    if browser_name == "msedge":
        env_overrides["BALUFFO_DESKTOP_ALLOW_EDGE_APP_MODE"] = "1"
    return {"browserName": browser_name, "browserPath": browser_path}, env_overrides


def _select_browser_shutdown_proof(rows: list[dict[str, Any]]) -> dict[str, Any]:
    deps = _root()
    attached_fields = deps.find_startup_metric_fields(rows, "desktop_browser_job_attached") or {}
    attached_pid = int(attached_fields.get("pid") or 0)
    if attached_pid > 0 and deps.desktop_app_mod.is_process_alive(attached_pid):
        return {
            "proofSource": "attached-browser-pid",
            "proofPid": attached_pid,
            "attachedPid": attached_pid,
            "windowPid": 0,
        }
    window_fields = (
        deps.find_startup_metric_fields(
            rows,
            "desktop_shell_window_shown",
            observed=True,
        )
        or {}
    )
    window_pid = int(window_fields.get("windowPid") or 0)
    if window_pid > 0 and deps.desktop_app_mod.is_process_alive(window_pid):
        return {
            "proofSource": "window-pid",
            "proofPid": window_pid,
            "attachedPid": attached_pid,
            "windowPid": window_pid,
        }
    raise RuntimeError(
        "Packaged browser job rehearsal could not establish a live attached PID or visible window PID."
    )


def _assert_desktop_update_helper_succeeded(
    *,
    paths: Any,
    relaunch_bridge_port: int,
) -> None:
    deps = _root()
    if paths.helper_stdout_log_path.is_file():
        helper_stdout = paths.helper_stdout_log_path.read_text(
            encoding="utf-8",
            errors="replace",
        ).strip()
        if helper_stdout:
            payload = json.loads(helper_stdout)
            if isinstance(payload, dict) and payload.get("ok") is False:
                raise RuntimeError(f"Update helper reported failure: {payload}")
    if paths.helper_diagnostics_log_path.is_file():
        for raw_line in paths.helper_diagnostics_log_path.read_text(
            encoding="utf-8",
            errors="replace",
        ).splitlines():
            line = raw_line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(row, dict):
                continue
            event = str(row.get("event") or "").strip().lower()
            if event in {"helper_worker_failed", "helper_main_failed"}:
                raise RuntimeError(f"Update helper diagnostics reported failure: {row}")
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


def run_packaged_sync_rehearsal(
    *,
    exe_path: Path,
    artifacts_dir: Path,
    runtime_timeout_s: float,
) -> dict[str, Any]:
    deps = _root()
    started = time.perf_counter()
    portable_root = exe_path.parent.resolve()
    process = None
    stdout_handle = None
    stderr_handle = None
    site_port = 0
    bridge_port = 0
    server = None
    server_thread = None
    try:
        config_path, raw_payload, packaged_config = deps._load_portable_packaged_sync_rehearsal_config(
            portable_root
        )
        snapshot_payload = {
            "schemaVersion": deps.source_sync_mod.SYNC_SCHEMA_VERSION,
            "generatedAt": deps.utc_now_iso(),
            "source": {"name": "packaged_sync_rehearsal"},
            "active": [],
            "pending": [],
            "rejected": [],
        }
        base_url, server_stats, server, server_thread = deps._start_packaged_sync_rehearsal_server(
            packaged_config=packaged_config,
            snapshot_payload=snapshot_payload,
        )
        runtime_env = deps.os.environ.copy()
        runtime_env.update(
            deps.packaged_runtime_env_overrides(
                artifacts_dir=artifacts_dir,
                session_scope="sync-rehearsal",
            )
        )
        runtime_env.update(
            {
                "BALUFFO_DESKTOP_NO_BROWSER": "1",
                deps.source_sync_mod.GITHUB_API_BASE_ENV: base_url,
            }
        )
        deps.clear_packaged_desktop_session_state(runtime_env)
        site_port = deps.choose_free_port()
        bridge_port = deps.choose_free_port()
        stdout_path = artifacts_dir / "packaged-sync-rehearsal.stdout.log"
        stderr_path = artifacts_dir / "packaged-sync-rehearsal.stderr.log"
        process, stdout_handle, stderr_handle = deps.launch_packaged_exe(
            exe_path,
            site_port=site_port,
            bridge_port=bridge_port,
            data_dir=artifacts_dir / "runtime-data",
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            open_path="jobs.html",
            startup_probe=False,
            env=runtime_env,
        )
        deps.wait_for_packaged_runtime(
            process,
            site_base_url=f"http://127.0.0.1:{site_port}",
            bridge_base_url=f"http://127.0.0.1:{bridge_port}",
            timeout_s=runtime_timeout_s,
            open_path="jobs.html",
        )
        status_code, status_payload = deps.request_json(
            f"http://127.0.0.1:{bridge_port}/sync/status?t={deps.time.time_ns()}",
            timeout_s=10.0,
        )
        if status_code != 200:
            raise RuntimeError(f"Packaged sync status failed: {status_payload}")
        status_config = (
            dict(status_payload.get("config") or {})
            if isinstance(status_payload.get("config"), dict)
            else {}
        )
        if not bool(status_config.get("credentialsPackaged")):
            raise RuntimeError(
                f"Packaged sync config was not loaded by the runtime: {status_config}"
            )
        if str(status_config.get("keyDerivation") or "").strip().lower() == "machine":
            raise RuntimeError(
                "Packaged sync runtime reported keyDerivation=machine for the shipped config."
            )
        if not bool(status_config.get("ready")):
            raise RuntimeError(f"Packaged sync config was not ready: {status_config}")
        status_code, sync_test = deps.post_json(
            f"http://127.0.0.1:{bridge_port}/sync/test",
            {},
            timeout_s=max(20.0, runtime_timeout_s),
        )
        if status_code != 200 or not bool(sync_test.get("ok")):
            raise RuntimeError(
                f"Packaged sync rehearsal could not verify the GitHub connection: {sync_test}"
            )
        if not bool(sync_test.get("remoteFound")):
            raise RuntimeError(
                f"Packaged sync rehearsal did not read the remote snapshot: {sync_test}"
            )
        if int(server_stats.get("tokenRequests") or 0) <= 0:
            raise RuntimeError("Packaged sync rehearsal never requested a GitHub App token.")
        if int(server_stats.get("contentRequests") or 0) <= 0:
            raise RuntimeError("Packaged sync rehearsal never read the remote snapshot content.")
        return {
            "name": "Packaged sync rehearsal",
            "slug": "packaged-sync-rehearsal",
            "status": "passed",
            "durationMs": int((time.perf_counter() - started) * 1000),
            "error": "",
            "details": {
                "packagedConfigPath": str(config_path),
                "keyDerivation": str(raw_payload.get("keyDerivation") or ""),
                "syncApiBaseUrl": str(base_url),
                "tokenRequests": int(server_stats.get("tokenRequests") or 0),
                "contentRequests": int(server_stats.get("contentRequests") or 0),
                "runtimeStdout": str(stdout_path),
                "runtimeStderr": str(stderr_path),
            },
        }
    except Exception as exc:
        return {
            "name": "Packaged sync rehearsal",
            "slug": "packaged-sync-rehearsal",
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
        if server is not None:
            server.shutdown()
            server.server_close()
        if server_thread is not None:
            server_thread.join(timeout=2.0)
        deps.cleanup_orphaned_desktop_ports_nt(site_port, bridge_port)


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
    deps._inject_desktop_update_public_keys(portable_root, {key_id: public_key_b64})
    install_root = artifacts_dir / "portable-install"
    if install_root.exists():
        shutil.rmtree(install_root)
    shutil.copytree(portable_root, install_root)
    install_exe = install_root / "Baluffo.exe"
    data_dir = install_root / "ship" / "data"
    seeded = deps._seed_rehearsal_local_data(data_dir)
    target_zip = deps._archive_portable_dir(portable_root, artifacts_dir / "baluffo-portable-update.zip")
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
                "BALUFFO_DESKTOP_UPDATER_VERIFY_TIMEOUT_S": "10",
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
        if not bool(check_status.get("updateAvailable")) or str(check_status.get("availability") or "") != "available":
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
        status_code, install_payload = deps.post_json(
            f"http://127.0.0.1:{initial_bridge_port}/app/install-update",
            {},
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
        deps._verify_rehearsal_local_data(data_dir, seeded)
        relaunch_session = relaunched.get("session") if isinstance(relaunched.get("session"), dict) else {}
        relaunch_launcher_pid = int(relaunch_session.get("launcherPid") or 0)
        relaunch_bridge_port = int(relaunch_session.get("bridgePort") or 0)
        relaunch_site_port = int(relaunch_session.get("sitePort") or 0)
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


def run_packaged_browser_job_rehearsal(
    *,
    exe_path: Path,
    artifacts_dir: Path,
    runtime_timeout_s: float,
) -> dict[str, Any]:
    deps = _root()
    started = time.perf_counter()
    if deps.os.name != "nt":
        return {
            "name": "Packaged browser job rehearsal",
            "slug": "packaged-browser-job-rehearsal",
            "status": "failed",
            "durationMs": int((time.perf_counter() - started) * 1000),
            "error": "Packaged browser job rehearsal requires Windows.",
        }
    runtime_env = deps.os.environ.copy()
    runtime_env.update(
        deps.packaged_runtime_env_overrides(
            artifacts_dir=artifacts_dir,
            session_scope="browser-job-rehearsal",
        )
    )
    deps.clear_packaged_desktop_session_state(runtime_env)
    runtime_data_dir = artifacts_dir / "runtime-data"
    runtime_data_dir.mkdir(parents=True, exist_ok=True)
    selected_browser: dict[str, str] = {"browserName": "", "browserPath": ""}
    session_root = deps.packaged_desktop_session_paths(runtime_env)["sessionRoot"]
    requested_site_port = deps.choose_free_port()
    requested_bridge_port = deps.choose_free_port()
    actual_site_port = requested_site_port
    actual_bridge_port = requested_bridge_port
    port_retry_observed = False
    proof_pid = 0
    attached_pid = 0
    window_pid = 0
    proof_source = ""
    runtime_process = None
    runtime_stdout_handle = None
    runtime_stderr_handle = None
    runtime_stdout_path = artifacts_dir / "browser-job-rehearsal-runtime.stdout.log"
    runtime_stderr_path = artifacts_dir / "browser-job-rehearsal-runtime.stderr.log"
    metrics_path = artifacts_dir / "browser-job-rehearsal.startup-metrics.json"
    try:
        selected_browser, browser_env = deps._select_packaged_browser_job_browser(runtime_env)
        runtime_env.update(browser_env)
        session_root = deps.packaged_desktop_session_paths(runtime_env)["sessionRoot"]
        runtime_process, runtime_stdout_handle, runtime_stderr_handle = deps.launch_packaged_exe(
            exe_path,
            site_port=requested_site_port,
            bridge_port=requested_bridge_port,
            data_dir=runtime_data_dir,
            stdout_path=runtime_stdout_path,
            stderr_path=runtime_stderr_path,
            open_path="jobs.html",
            startup_probe=False,
            env=runtime_env,
        )
        runtime_state = deps.wait_for_packaged_runtime_with_port_pivot(
            runtime_process,
            requested_site_port=requested_site_port,
            requested_bridge_port=requested_bridge_port,
            expected_data_dir=runtime_data_dir,
            timeout_s=runtime_timeout_s,
            open_path="jobs.html",
            env=runtime_env,
        )
        actual_site_port = int(runtime_state.get("actualSitePort") or requested_site_port)
        actual_bridge_port = int(runtime_state.get("actualBridgePort") or requested_bridge_port)
        port_retry_observed = bool(runtime_state.get("portRetryObserved"))
        metrics_rows = list(runtime_state.get("startupMetrics") or [])
        deps.write_json(metrics_path, {"rows": metrics_rows})
        launch_mode = deps.startup_metric_launch_mode(metrics_rows)
        if launch_mode != "chromium-app":
            raise RuntimeError(
                "Packaged browser job rehearsal required chromium-app launch mode; "
                f"desktop launch mode was '{launch_mode or 'unknown'}'."
            )
        if not deps.startup_metric_event_present(metrics_rows, "desktop_browser_process_spawn_started"):
            raise RuntimeError(
                "Packaged browser job rehearsal never emitted desktop_browser_process_spawn_started."
            )
        if not deps.startup_metric_event_present(metrics_rows, "desktop_browser_job_attached"):
            raise RuntimeError(
                "Packaged browser job rehearsal never emitted desktop_browser_job_attached."
            )
        if deps.startup_metric_event_present(metrics_rows, "desktop_browser_job_attach_failed"):
            raise RuntimeError(
                "Packaged browser job rehearsal emitted desktop_browser_job_attach_failed."
            )
        if not deps.startup_metric_event_present(metrics_rows, "desktop_browser_launch_accepted"):
            raise RuntimeError(
                "Packaged browser job rehearsal never emitted desktop_browser_launch_accepted."
            )
        if not deps.startup_metric_event_present(metrics_rows, "desktop_browser_launch_selected"):
            raise RuntimeError(
                "Packaged browser job rehearsal never emitted desktop_browser_launch_selected."
            )
        proof = deps._select_browser_shutdown_proof(metrics_rows)
        proof_source = str(proof.get("proofSource") or "")
        proof_pid = int(proof.get("proofPid") or 0)
        attached_pid = int(proof.get("attachedPid") or 0)
        window_pid = int(proof.get("windowPid") or 0)
        if proof_pid <= 0 or not deps.desktop_app_mod.is_process_alive(proof_pid):
            raise RuntimeError(
                "Packaged browser job rehearsal proof PID was not alive before launcher shutdown."
            )
        deps.terminate_process_only(runtime_process)
        deps._wait_for_pid_exit(proof_pid, timeout_s=max(15.0, float(runtime_timeout_s)))
        return {
            "name": "Packaged browser job rehearsal",
            "slug": "packaged-browser-job-rehearsal",
            "status": "passed",
            "durationMs": int((time.perf_counter() - started) * 1000),
            "error": "",
            "details": {
                "sessionRoot": str(session_root),
                "requestedSitePort": requested_site_port,
                "requestedBridgePort": requested_bridge_port,
                "actualSitePort": actual_site_port,
                "actualBridgePort": actual_bridge_port,
                "portRetryObserved": port_retry_observed,
                "selectedBrowserName": str(selected_browser.get("browserName") or ""),
                "selectedBrowserPath": str(selected_browser.get("browserPath") or ""),
                "attachedPid": attached_pid,
                "windowPid": window_pid,
                "proofPid": proof_pid,
                "proofSource": proof_source,
                "runtimeStdout": str(runtime_stdout_path),
                "runtimeStderr": str(runtime_stderr_path),
                "startupMetrics": str(metrics_path),
            },
        }
    except Exception as exc:
        return {
            "name": "Packaged browser job rehearsal",
            "slug": "packaged-browser-job-rehearsal",
            "status": "failed",
            "durationMs": int((time.perf_counter() - started) * 1000),
            "error": str(exc),
            "details": {
                "sessionRoot": str(session_root),
                "requestedSitePort": requested_site_port,
                "requestedBridgePort": requested_bridge_port,
                "actualSitePort": actual_site_port,
                "actualBridgePort": actual_bridge_port,
                "portRetryObserved": port_retry_observed,
                "selectedBrowserName": str(selected_browser.get("browserName") or ""),
                "selectedBrowserPath": str(selected_browser.get("browserPath") or ""),
                "attachedPid": attached_pid,
                "windowPid": window_pid,
                "proofPid": proof_pid,
                "proofSource": proof_source,
                "runtimeStdout": str(runtime_stdout_path),
                "runtimeStderr": str(runtime_stderr_path),
                "startupMetrics": str(metrics_path),
            },
        }
    finally:
        deps.terminate_process_tree(runtime_process)
        if runtime_stdout_handle is not None:
            runtime_stdout_handle.close()
        if runtime_stderr_handle is not None:
            runtime_stderr_handle.close()
        deps.cleanup_orphaned_desktop_ports_nt(
            requested_site_port,
            requested_bridge_port,
            actual_site_port,
            actual_bridge_port,
        )
        deps.clear_packaged_desktop_session_state(runtime_env)


def run_packaged_orphan_reclaim_rehearsal(
    *,
    exe_path: Path,
    artifacts_dir: Path,
    runtime_timeout_s: float,
) -> dict[str, Any]:
    deps = _root()
    started = time.perf_counter()
    runtime_env = deps.os.environ.copy()
    runtime_env.update(
        deps.packaged_runtime_env_overrides(
            artifacts_dir=artifacts_dir,
            session_scope="orphan-reclaim-rehearsal",
        )
    )
    runtime_env["BALUFFO_DESKTOP_NO_BROWSER"] = "1"
    session_paths = deps.packaged_desktop_session_paths(runtime_env)
    runtime_data_dir = artifacts_dir / "runtime-data"
    runtime_data_dir.mkdir(parents=True, exist_ok=True)
    deps.clear_packaged_desktop_session_state(runtime_env)
    site_port = deps.choose_free_port()
    bridge_port = deps.choose_free_port()
    owner_token = deps.generate_packaged_smoke_run_token()
    launcher_token = deps.generate_packaged_smoke_run_token()
    desktop_session_id = deps.generate_packaged_smoke_run_token()
    stale_launcher_pid = 2_147_483_647
    stale_started_at = deps.utc_now_iso()
    stale_site_process = None
    stale_site_stdout_handle = None
    stale_site_stderr_handle = None
    stale_bridge_process = None
    stale_bridge_stdout_handle = None
    stale_bridge_stderr_handle = None
    runtime_process = None
    runtime_stdout_handle = None
    runtime_stderr_handle = None
    relaunch_site_port = 0
    relaunch_bridge_port = 0
    runtime_state: dict[str, Any] = {}
    stale_site_stdout_path = artifacts_dir / "orphan-reclaim-site.stdout.log"
    stale_site_stderr_path = artifacts_dir / "orphan-reclaim-site.stderr.log"
    stale_bridge_stdout_path = artifacts_dir / "orphan-reclaim-bridge.stdout.log"
    stale_bridge_stderr_path = artifacts_dir / "orphan-reclaim-bridge.stderr.log"
    runtime_stdout_path = artifacts_dir / "orphan-reclaim-runtime.stdout.log"
    runtime_stderr_path = artifacts_dir / "orphan-reclaim-runtime.stderr.log"
    try:
        stale_site_process, stale_site_stdout_handle, stale_site_stderr_handle = (
            deps.launch_packaged_desktop_child(
                exe_path,
                mode="site",
                port=site_port,
                stdout_path=stale_site_stdout_path,
                stderr_path=stale_site_stderr_path,
                env=runtime_env,
            )
        )
        stale_bridge_process, stale_bridge_stdout_handle, stale_bridge_stderr_handle = (
            deps.launch_packaged_desktop_child(
                exe_path,
                mode="bridge",
                port=bridge_port,
                data_dir=runtime_data_dir,
                owner_token=owner_token,
                desktop_session_id=desktop_session_id,
                stdout_path=stale_bridge_stdout_path,
                stderr_path=stale_bridge_stderr_path,
                env=runtime_env,
            )
        )
        deps.wait_for_packaged_child_runtime(
            stale_site_process,
            stale_bridge_process,
            site_base_url=f"http://127.0.0.1:{site_port}",
            bridge_base_url=f"http://127.0.0.1:{bridge_port}",
            owner_token=owner_token,
            timeout_s=runtime_timeout_s,
        )

        session_paths["sessionRoot"].mkdir(parents=True, exist_ok=True)
        deps.write_json(
            session_paths["sessionState"],
            {
                "appVersion": deps.desktop_update_mod.get_app_version(),
                "launcherPid": stale_launcher_pid,
                "launcherToken": launcher_token,
                "desktopSessionId": desktop_session_id,
                "desktopOwnerToken": owner_token,
                "launcherStartedAt": stale_started_at,
                "sitePort": site_port,
                "sitePid": int(stale_site_process.pid),
                "bridgePort": bridge_port,
                "bridgePid": int(stale_bridge_process.pid),
                "bridgeHost": "127.0.0.1",
                "url": f"http://127.0.0.1:{site_port}/jobs.html?desktop=1",
                "launchMode": "no-browser",
                "browserPath": "",
                "exePath": str(exe_path.resolve()),
                "dataDir": str(runtime_data_dir.resolve()),
                "timestamp": deps.utc_now_iso(),
            },
        )
        deps.write_json(
            session_paths["instanceLock"],
            {
                "pid": stale_launcher_pid,
                "createdAt": stale_started_at,
                "launcherToken": launcher_token,
                "exePath": str(exe_path.resolve()),
                "sessionRoot": str(session_paths["sessionRoot"]),
                "state": "running",
            },
        )

        runtime_process, runtime_stdout_handle, runtime_stderr_handle = deps.launch_packaged_exe(
            exe_path,
            site_port=site_port,
            bridge_port=bridge_port,
            data_dir=runtime_data_dir,
            stdout_path=runtime_stdout_path,
            stderr_path=runtime_stderr_path,
            open_path="jobs.html",
            startup_probe=False,
            env=runtime_env,
        )
        runtime_state = deps.wait_for_packaged_runtime_with_port_pivot(
            runtime_process,
            requested_site_port=site_port,
            requested_bridge_port=bridge_port,
            expected_data_dir=runtime_data_dir,
            timeout_s=runtime_timeout_s,
            open_path="jobs.html",
            env=runtime_env,
        )
        relaunch_site_port = int(runtime_state.get("actualSitePort") or site_port)
        relaunch_bridge_port = int(runtime_state.get("actualBridgePort") or bridge_port)
        port_retry_observed = bool(runtime_state.get("portRetryObserved"))
        metrics_rows = list(runtime_state.get("startupMetrics") or [])
        if relaunch_site_port != site_port or relaunch_bridge_port != bridge_port:
            raise RuntimeError(
                "Packaged orphan reclaim rehearsal did not preserve the requested ports after relaunch."
            )
        if not deps.startup_metric_event_present(metrics_rows, "desktop_stale_runtime_reclaim_started"):
            raise RuntimeError(
                "Packaged orphan reclaim rehearsal never emitted desktop_stale_runtime_reclaim_started."
            )
        if not deps.startup_metric_event_present(
            metrics_rows,
            "desktop_stale_runtime_reclaim_result",
            target="bridge",
            outcome="killed",
        ):
            raise RuntimeError(
                "Packaged orphan reclaim rehearsal did not prove bridge reclaim in startup metrics."
            )
        if not deps.startup_metric_event_present(
            metrics_rows,
            "desktop_stale_runtime_reclaim_result",
            target="site",
            outcome="killed",
        ):
            raise RuntimeError(
                "Packaged orphan reclaim rehearsal did not prove site reclaim in startup metrics."
            )
        if deps.startup_metric_event_present(metrics_rows, "desktop_lock_reclaim_failed"):
            raise RuntimeError(
                "Packaged orphan reclaim rehearsal reported desktop_lock_reclaim_failed."
            )
        if port_retry_observed:
            raise RuntimeError(
                "Packaged orphan reclaim rehearsal retried to different runtime ports instead of reclaiming stale children."
            )
        deps._wait_for_process_exit(stale_site_process, timeout_s=15.0)
        deps._wait_for_process_exit(stale_bridge_process, timeout_s=15.0)
        return {
            "name": "Packaged orphan reclaim rehearsal",
            "slug": "packaged-orphan-reclaim-rehearsal",
            "status": "passed",
            "durationMs": int((time.perf_counter() - started) * 1000),
            "error": "",
            "details": {
                "sessionRoot": str(session_paths["sessionRoot"]),
                "sitePort": site_port,
                "bridgePort": bridge_port,
                "actualSitePort": relaunch_site_port,
                "actualBridgePort": relaunch_bridge_port,
                "portRetryObserved": port_retry_observed,
                "runtimeStdout": str(runtime_stdout_path),
                "runtimeStderr": str(runtime_stderr_path),
                "staleSiteStdout": str(stale_site_stdout_path),
                "staleSiteStderr": str(stale_site_stderr_path),
                "staleBridgeStdout": str(stale_bridge_stdout_path),
                "staleBridgeStderr": str(stale_bridge_stderr_path),
            },
        }
    except Exception as exc:
        return {
            "name": "Packaged orphan reclaim rehearsal",
            "slug": "packaged-orphan-reclaim-rehearsal",
            "status": "failed",
            "durationMs": int((time.perf_counter() - started) * 1000),
            "error": str(exc),
        }
    finally:
        deps.terminate_process_tree(runtime_process)
        deps.terminate_process_tree(stale_bridge_process)
        deps.terminate_process_tree(stale_site_process)
        if runtime_stdout_handle is not None:
            runtime_stdout_handle.close()
        if runtime_stderr_handle is not None:
            runtime_stderr_handle.close()
        if stale_site_stdout_handle is not None:
            stale_site_stdout_handle.close()
        if stale_site_stderr_handle is not None:
            stale_site_stderr_handle.close()
        if stale_bridge_stdout_handle is not None:
            stale_bridge_stdout_handle.close()
        if stale_bridge_stderr_handle is not None:
            stale_bridge_stderr_handle.close()
        deps.cleanup_orphaned_desktop_ports_nt(
            site_port,
            bridge_port,
            relaunch_site_port,
            relaunch_bridge_port,
        )
        deps.clear_packaged_desktop_session_state(runtime_env)
