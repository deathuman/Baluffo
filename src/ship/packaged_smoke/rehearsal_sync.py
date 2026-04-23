"""Packaged sync rehearsal helpers behind the root smoke facade."""

from __future__ import annotations

import base64
import http.server
import json
import threading
import time
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

root: Any | None = None


def _root() -> Any:
    if root is None:
        raise RuntimeError("packaged_smoke.rehearsal_sync.root is not configured")
    return root


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
