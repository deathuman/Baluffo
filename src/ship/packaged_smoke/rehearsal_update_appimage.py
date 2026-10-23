"""Rehearse a real AppImage self-update (N -> N+1) against a local release.

The Windows rehearsal builds a portable directory and mutates it; a Linux build
is one file, so this one serves a second AppImage and a locally signed manifest
over loopback, then drives the running app's own update routes until it
replaces itself and reports the new version.

Nothing is published and no release is touched: the signing key is generated for
the run and handed to the app through its public-key environment variable.

AI boundary owns: the AppImage update rehearsal.
AI boundary implement in: this file; update behavior stays in src.ship.desktop_update_*.
AI boundary verify: `npm run test:frontend:packaged:update-rehearsal:appimage`.
"""

from __future__ import annotations

import base64
import contextlib
import http.server
import json
import os
import shutil
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.request
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from src.shared.hashing import sha256_file
from src.ship.desktop_artifact_paths import APPIMAGE_EXTRACT_AND_RUN_ARG
from src.ship.desktop_update_manifest import (
    DESKTOP_UPDATE_MANIFEST_ASSET_LINUX,
    DESKTOP_UPDATE_SCHEMA_VERSION,
    DESKTOP_UPDATER_VERSION,
    Ed25519SigningClass,
    canonical_manifest_bytes,
)
from src.ship.packaged_smoke.runtime_process import launch_packaged_command

REHEARSAL_REPO = "local/baluffo-smoke"
REHEARSAL_KEY_ID = "baluffo-appimage-rehearsal"
UPSTREAM_APPIMAGE_ENV = "BALUFFO_APPIMAGE_PATH"
POLL_INTERVAL_S = 0.5
_HTTP_ERRORS = (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError, ValueError)


def add_cli_arguments(parser: Any) -> None:
    """Register this rehearsal's flags on the packaged-smoke CLI."""
    parser.add_argument("--desktop-update-rehearsal-appimage", action="store_true")
    parser.add_argument(
        "--update-appimage-before",
        default=os.environ.get("BALUFFO_UPDATE_APPIMAGE_BEFORE", ""),
    )
    parser.add_argument(
        "--update-appimage-after",
        default=os.environ.get("BALUFFO_UPDATE_APPIMAGE_AFTER", ""),
    )
    parser.add_argument(
        "--update-target-version",
        default=os.environ.get("BALUFFO_UPDATE_TARGET_VERSION", "0.0.0-rehearsal"),
    )
    parser.add_argument(
        "--update-before-version",
        default=os.environ.get("BALUFFO_UPDATE_BEFORE_VERSION", ""),
    )


def run_from_args(args: Any, *, default_artifact_root: Path) -> dict[str, Any]:
    before = str(getattr(args, "update_appimage_before", "") or "").strip()
    after = str(getattr(args, "update_appimage_after", "") or "").strip()
    if not before or not after:
        raise RuntimeError(
            "--desktop-update-rehearsal-appimage needs --update-appimage-before and "
            "--update-appimage-after."
        )
    artifacts_dir = str(getattr(args, "artifacts_dir", "") or "").strip()
    return run_appimage_update_rehearsal(
        before_appimage=Path(before),
        after_appimage=Path(after),
        artifacts_dir=(Path(artifacts_dir) if artifacts_dir else default_artifact_root),
        target_version=str(getattr(args, "update_target_version", "") or "0.0.0-rehearsal"),
        before_version=str(getattr(args, "update_before_version", "") or ""),
        runtime_timeout_s=float(getattr(args, "runtime_timeout", 180.0)),
    )


def print_report(report: dict[str, Any]) -> int:
    print(json.dumps(report, indent=2))
    if not bool(report.get("ok")):
        print(
            f"\n[SMOKE FAILURE] Step: appimage-update\n[SMOKE FAILURE] Error: {report.get('error', '')}"
        )
    return 0 if bool(report.get("ok")) else 1


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _http_json(
    url: str,
    *,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
    timeout_s: float = 10.0,
) -> dict[str, Any]:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=data,
        method=str(method),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=timeout_s) as response:
        body = response.read().decode("utf-8")
    parsed = json.loads(body or "{}")
    return parsed if isinstance(parsed, dict) else {}


class _ReleaseHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def __init__(
        self,
        *args: Any,
        manifest_holder: dict[str, dict[str, Any]],
        artifact_path: Path,
        **kwargs: Any,
    ) -> None:
        self._manifest_holder = manifest_holder
        self._artifact_path = artifact_path
        super().__init__(*args, **kwargs)

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _send_json(self, payload: Any) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_text(self, body: bytes) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_artifact(self) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(int(self._artifact_path.stat().st_size)))
        self.end_headers()
        with self._artifact_path.open("rb") as handle:
            while True:
                chunk = handle.read(1024 * 1024)
                if not chunk:
                    break
                self.wfile.write(chunk)

    def do_GET(self) -> None:
        manifest = self._manifest_holder.get("value") or {}
        if self.path.startswith(f"/repos/{REHEARSAL_REPO}/releases"):
            version = str(manifest.get("version") or "")
            base = f"http://127.0.0.1:{int(cast(Any, self.server).server_port)}"
            self._send_json(
                [
                    {
                        "id": 1,
                        "tag_name": f"v{version}",
                        "draft": False,
                        "prerelease": False,
                        "html_url": f"{base}/release-notes",
                        "assets": [
                            {
                                "name": DESKTOP_UPDATE_MANIFEST_ASSET_LINUX,
                                "browser_download_url": (
                                    f"{base}/assets/{DESKTOP_UPDATE_MANIFEST_ASSET_LINUX}"
                                ),
                            }
                        ],
                    }
                ]
            )
            return
        if self.path == f"/assets/{DESKTOP_UPDATE_MANIFEST_ASSET_LINUX}":
            self._send_json(manifest)
            return
        if self.path == f"/assets/{self._artifact_path.name}":
            self._send_artifact()
            return
        if self.path == "/release-notes":
            self._send_text(b"AppImage update rehearsal")
            return
        self.send_error(404)


def _start_release_server(
    *,
    manifest_holder: dict[str, dict[str, Any]],
    artifact_path: Path,
) -> tuple[str, http.server.ThreadingHTTPServer, threading.Thread]:
    def handler_factory(*args: Any, **kwargs: Any) -> _ReleaseHandler:
        return _ReleaseHandler(
            *args,
            manifest_holder=manifest_holder,
            artifact_path=artifact_path,
            **kwargs,
        )

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler_factory)
    base_url = f"http://127.0.0.1:{int(server.server_port)}"
    thread = threading.Thread(
        target=server.serve_forever,
        daemon=True,
        name="appimage-update-release-server",
    )
    thread.start()
    return base_url, server, thread


def build_rehearsal_manifest(
    *,
    version: str,
    artifact_url: str,
    artifact_path: Path,
    release_notes_url: str,
    private_key: Any,
    key_id: str = REHEARSAL_KEY_ID,
) -> dict[str, Any]:
    manifest: dict[str, Any] = {
        "schema_version": DESKTOP_UPDATE_SCHEMA_VERSION,
        "key_id": key_id,
        "channel": "stable",
        "version": str(version),
        "published_at": datetime.now(UTC).isoformat(),
        "release_notes_url": str(release_notes_url),
        "min_desktop_updater_version": DESKTOP_UPDATER_VERSION,
        "min_supported_current_version": "0.0.0",
        "data_schema_version": "2",
        "rollback_allowed": True,
        "portable_artifact": {
            "url": str(artifact_url),
            "sha256": sha256_file(artifact_path),
            "size_bytes": int(artifact_path.stat().st_size),
        },
        "migration_plan": [],
    }
    manifest["signature"] = base64.b64encode(
        private_key.sign(canonical_manifest_bytes(manifest))
    ).decode("ascii")
    return manifest


def _seed_data_dir(data_dir: Path) -> None:
    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "rehearsal-seed.json").write_text(
        json.dumps({"seededAt": datetime.now(UTC).isoformat()}),
        encoding="utf-8",
    )


def _read_session_state(session_root: Path) -> dict[str, Any]:
    path = session_root / "desktop-session.json"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _wait_for_health(
    *,
    session_root: Path,
    expected_version: str,
    timeout_s: float,
    excluded_launcher_pid: int = 0,
) -> dict[str, Any]:
    """Wait for a ready bridge in this session; an empty version accepts any build."""
    deadline = time.monotonic() + max(5.0, float(timeout_s))
    last_error = ""
    while time.monotonic() < deadline:
        session = _read_session_state(session_root)
        launcher_pid = int(session.get("launcherPid") or 0)
        bridge_port = int(session.get("bridgePort") or 0)
        if bridge_port > 0 and launcher_pid > 0 and launcher_pid != int(excluded_launcher_pid or 0):
            try:
                health = _http_json(f"http://127.0.0.1:{bridge_port}/ops/health", timeout_s=2.0)
                reported_version = str(health.get("appVersion") or "").strip()
                if (
                    str(health.get("service") or "") == "baluffo-bridge"
                    and bool(health.get("startupReady"))
                    and (not expected_version or reported_version == expected_version)
                ):
                    return {"health": health, "session": session}
            except _HTTP_ERRORS as exc:
                last_error = str(exc)
        time.sleep(POLL_INTERVAL_S)
    raise TimeoutError(
        f"The AppImage never reported version {expected_version} on a ready bridge."
        + (f" Last error: {last_error}" if last_error else "")
    )


def _wait_for_status(
    bridge_base_url: str,
    predicate: Any,
    *,
    timeout_s: float,
    description: str,
) -> dict[str, Any]:
    deadline = time.monotonic() + max(5.0, float(timeout_s))
    last_status: dict[str, Any] = {}
    while time.monotonic() < deadline:
        try:
            last_status = _http_json(f"{bridge_base_url}/app/update-status", timeout_s=5.0)
        except _HTTP_ERRORS:
            time.sleep(POLL_INTERVAL_S)
            continue
        if predicate(last_status):
            return last_status
        time.sleep(POLL_INTERVAL_S)
    raise TimeoutError(
        f"The update never reached {description}; last status: "
        f"{json.dumps(last_status, sort_keys=True)[:400]}"
    )


def _archive_copy(source: Path, target: Path) -> Path:
    target.parent.mkdir(parents=True, exist_ok=True)
    with contextlib.suppress(FileNotFoundError):
        target.unlink()
    shutil.copyfile(source, target)
    target.chmod(0o755)
    return target


def run_appimage_update_rehearsal(
    *,
    before_appimage: Path,
    after_appimage: Path,
    artifacts_dir: Path,
    target_version: str = "0.0.0-rehearsal",
    before_version: str = "",
    app_version_override: str = "0.0.9",
    runtime_timeout_s: float = 180.0,
) -> dict[str, Any]:
    """Run the AppImage N -> N+1 rehearsal and return a report.

    Both AppImages are built from this tree, so the running version is lowered
    for the rehearsal through the app's version override -- exactly as the
    Windows rehearsal does. The helper clears that override when it relaunches,
    so the updated app must report the manifest's version on its own.
    """
    before_source = Path(before_appimage).expanduser().resolve()
    after_source = Path(after_appimage).expanduser().resolve()
    if not before_source.is_file():
        raise RuntimeError(f"Rehearsal baseline AppImage not found: {before_source}")
    if not after_source.is_file():
        raise RuntimeError(f"Rehearsal target AppImage not found: {after_source}")
    run_root = Path(artifacts_dir).expanduser().resolve() / f"run-{uuid.uuid4().hex[:8]}"
    run_root.mkdir(parents=True, exist_ok=True)
    install_dir = run_root / "apps"
    install_dir.mkdir(parents=True, exist_ok=True)
    # The install replaces this copy, never the build output it came from.
    target_appimage = _archive_copy(before_source, install_dir / before_source.name)
    after_copy = _archive_copy(after_source, run_root / "release" / after_source.name)
    data_dir = run_root / "data"
    session_root = run_root / "session"
    session_root.mkdir(parents=True, exist_ok=True)
    _seed_data_dir(data_dir)

    if Ed25519SigningClass is None:
        raise RuntimeError("Ed25519 signing is unavailable in this runtime.")
    private_key = Ed25519SigningClass.generate()
    public_key_b64 = base64.b64encode(private_key.public_key().public_bytes_raw()).decode("ascii")

    manifest_holder: dict[str, dict[str, Any]] = {}
    base_url, server, thread = _start_release_server(
        manifest_holder=manifest_holder,
        artifact_path=after_copy,
    )
    checks: list[dict[str, Any]] = []

    def record(name: str, ok: bool, detail: Any = "") -> None:
        checks.append({"name": name, "ok": bool(ok), "detail": detail})

    site_port = _free_port()
    bridge_port = _free_port()
    env = os.environ.copy()
    env.update(
        {
            "BALUFFO_DATA_DIR": str(data_dir),
            "BALUFFO_DESKTOP_SESSION_ROOT": str(session_root),
            "BALUFFO_DESKTOP_NO_BROWSER": "1",
            "BALUFFO_DESKTOP_UPDATE_GITHUB_API_BASE": base_url,
            "BALUFFO_DESKTOP_UPDATE_REPO": REHEARSAL_REPO,
            "BALUFFO_DESKTOP_UPDATE_PUBLIC_KEYS_JSON": json.dumps(
                {REHEARSAL_KEY_ID: public_key_b64}
            ),
            UPSTREAM_APPIMAGE_ENV: str(target_appimage),
            "BALUFFO_APP_VERSION_OVERRIDE": str(app_version_override),
            "BALUFFO_DESKTOP_UPDATER_NO_DIALOG": "1",
            "BALUFFO_DESKTOP_UPDATER_VERIFY_TIMEOUT_S": str(max(30.0, runtime_timeout_s / 2)),
        }
    )
    env.pop("DISPLAY", None)
    env.pop("WAYLAND_DISPLAY", None)
    stdout_path = run_root / "before.stdout.log"
    stderr_path = run_root / "before.stderr.log"
    process: subprocess.Popen[bytes] | None = None
    stdout_handle: Any | None = None
    stderr_handle: Any | None = None
    report: dict[str, Any] = {"ok": False, "runRoot": str(run_root), "checks": checks}
    try:
        manifest = build_rehearsal_manifest(
            version=target_version,
            artifact_url=f"{base_url}/assets/{after_copy.name}",
            artifact_path=after_copy,
            release_notes_url=f"{base_url}/release-notes",
            private_key=private_key,
        )
        manifest_holder["value"] = manifest

        process, stdout_handle, stderr_handle = launch_packaged_command(
            target_appimage,
            args=[
                APPIMAGE_EXTRACT_AND_RUN_ARG,
                "--data-dir",
                str(data_dir),
                "--site-port",
                str(site_port),
                "--bridge-port",
                str(bridge_port),
                "--open-path",
                "jobs.html",
            ],
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            env=env,
        )
        before = _wait_for_health(
            session_root=session_root,
            expected_version=str(before_version or app_version_override),
            timeout_s=runtime_timeout_s,
        )
        launcher_pid = int(before["session"].get("launcherPid") or 0)
        bridge_base_url = f"http://127.0.0.1:{int(before['session']['bridgePort'])}"

        checked = _http_json(
            f"{bridge_base_url}/app/check-for-update",
            method="POST",
            payload={"force": True},
        )
        record(
            "check finds the rehearsal release",
            str(checked.get("availability") or "") == "available"
            and str(checked.get("targetVersion") or "") == target_version,
            {
                "availability": checked.get("availability"),
                "targetVersion": checked.get("targetVersion"),
            },
        )
        started = _http_json(f"{bridge_base_url}/app/download-update", method="POST")
        record("download starts", bool(started.get("started")), started.get("error") or "")
        _wait_for_status(
            bridge_base_url,
            lambda status: str(status.get("downloadState") or "") == "downloaded",
            timeout_s=runtime_timeout_s,
            description="downloaded",
        )
        record("downloaded artifact verifies", True)
        installed = _http_json(f"{bridge_base_url}/app/install-update", method="POST")
        record(
            "install handoff starts", bool(installed.get("started")), installed.get("error") or ""
        )

        deadline = time.monotonic() + max(10.0, float(runtime_timeout_s))
        while time.monotonic() < deadline and process.poll() is None:
            time.sleep(POLL_INTERVAL_S)
        record("launcher exits for the install", process.poll() is not None, process.poll())
        after = _wait_for_health(
            session_root=session_root,
            expected_version=target_version,
            timeout_s=runtime_timeout_s,
            excluded_launcher_pid=launcher_pid,
        )
        record(
            "relaunched app reports the new version",
            str(after["health"].get("appVersion") or "") == target_version,
            after["health"].get("appVersion"),
        )
        status = _http_json(
            f"http://127.0.0.1:{int(after['session']['bridgePort'])}/app/update-status",
            timeout_s=5.0,
        )
        record("install state is installed", str(status.get("installState") or "") == "installed")
        record("download state is cleared", str(status.get("downloadState") or "") == "idle")
        record(
            "the AppImage on disk is the new build",
            sha256_file(target_appimage) == sha256_file(after_copy),
        )
        rollback_files = sorted(path.name for path in install_dir.glob(".Baluffo-*.rollback"))
        record("no rollback snapshot is left behind", not rollback_files, rollback_files)
        record(
            "user data survived the update",
            (data_dir / "rehearsal-seed.json").is_file(),
        )
        report["ok"] = all(check["ok"] for check in checks)
    except (OSError, RuntimeError, TimeoutError, ValueError, KeyError) as exc:
        report["ok"] = False
        report["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        if process is not None and process.poll() is None:
            with contextlib.suppress(OSError):
                process.terminate()
            with contextlib.suppress(subprocess.TimeoutExpired):
                process.wait(timeout=15)
        with contextlib.suppress(OSError):
            server.shutdown()
        with contextlib.suppress(OSError):
            server.server_close()
        if thread.is_alive():
            thread.join(timeout=5)
        for handle in (stdout_handle, stderr_handle):
            if handle is None:
                continue
            with contextlib.suppress(OSError):
                handle.close()
        report_path = run_root / "update-rehearsal-report.json"
        report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        report["reportPath"] = str(report_path)
    return report


__all__ = [
    "REHEARSAL_KEY_ID",
    "REHEARSAL_REPO",
    "add_cli_arguments",
    "build_rehearsal_manifest",
    "print_report",
    "run_appimage_update_rehearsal",
    "run_from_args",
]
