"""Packaged sync rehearsal helpers behind the root smoke facade."""

from __future__ import annotations

import base64
import hashlib
import http.server
import json
import threading
import time
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

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
        remote_objects: dict[str, dict[str, Any]],
        stats: dict[str, Any],
        **kwargs: Any,
    ) -> None:
        self._installation_id = str(installation_id or "")
        self._repo = str(repo or "")
        self._branch = str(branch or "")
        self._remote_objects = remote_objects
        self._stats = stats
        super().__init__(*args, **kwargs)

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _send_json(self, payload: Any, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(int(status))
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _content_path(self) -> str:
        parsed = urlsplit(self.path)
        expected_prefix = f"/repos/{self._repo}/contents/"
        if not parsed.path.startswith(expected_prefix):
            return ""
        return unquote(parsed.path[len(expected_prefix) :]).replace("\\", "/").strip("/")

    def _authorized(self) -> bool:
        auth_header = str(self.headers.get("Authorization") or "").strip()
        return auth_header == "Bearer packaged-sync-rehearsal-token"

    def _send_content_object(self, path: str, row: dict[str, Any]) -> None:
        raw = bytes(row.get("content") or b"")
        self._send_json(
            {
                "path": path,
                "name": Path(path).name,
                "type": "file",
                "sha": str(row.get("sha") or ""),
                "size": len(raw),
                "content": base64.b64encode(raw).decode("ascii"),
                "encoding": "base64",
            }
        )

    def _read_json_body(self) -> dict[str, Any]:
        content_length = int(str(self.headers.get("Content-Length") or "0") or 0)
        raw = self.rfile.read(content_length) if content_length > 0 else b"{}"
        payload = json.loads(raw.decode("utf-8") or "{}")
        return payload if isinstance(payload, dict) else {}

    def do_POST(self) -> None:
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

    def do_GET(self) -> None:
        parsed = urlsplit(self.path)
        path = self._content_path()
        if not path:
            self.send_error(404)
            return
        query = parse_qs(parsed.query or "")
        if str((query.get("ref") or [""])[0]) != self._branch:
            self._send_json({"message": "Unexpected sync branch."}, status=404)
            return
        if not self._authorized():
            self._send_json({"message": "Unexpected rehearsal access token."}, status=401)
            return
        self._stats["contentRequests"] = int(self._stats.get("contentRequests") or 0) + 1
        if path in self._remote_objects:
            self._send_content_object(path, self._remote_objects[path])
            return
        prefix = path.rstrip("/") + "/"
        matches = [
            (object_path, row)
            for object_path, row in sorted(self._remote_objects.items())
            if object_path.startswith(prefix)
        ]
        if matches:
            self._send_json(
                [
                    {
                        "path": object_path,
                        "name": Path(object_path).name,
                        "type": "file",
                        "sha": str(row.get("sha") or ""),
                        "size": len(bytes(row.get("content") or b"")),
                    }
                    for object_path, row in matches
                ]
            )
            return
        self._send_json({"message": "Not Found"}, status=404)

    def do_PUT(self) -> None:
        path = self._content_path()
        if not path:
            self.send_error(404)
            return
        if not self._authorized():
            self._send_json({"message": "Unexpected rehearsal access token."}, status=401)
            return
        try:
            payload = self._read_json_body()
            content = base64.b64decode(str(payload.get("content") or ""))
        except (ValueError, json.JSONDecodeError) as exc:
            self._send_json({"message": f"Invalid PUT payload: {exc}"}, status=400)
            return
        existing = self._remote_objects.get(path)
        expected_sha = str(payload.get("sha") or "").strip()
        if existing is not None and expected_sha and expected_sha != str(existing.get("sha") or ""):
            self._send_json({"message": "sha does not match"}, status=409)
            return
        sha = "packaged-sync-" + hashlib.sha1(content).hexdigest()
        self._remote_objects[path] = {"content": content, "sha": sha}
        self._stats["putRequests"] = int(self._stats.get("putRequests") or 0) + 1
        self._stats["bytesWritten"] = int(self._stats.get("bytesWritten") or 0) + len(content)
        self._send_json({"content": {"path": path, "sha": sha}})

    def do_DELETE(self) -> None:
        path = self._content_path()
        if not path:
            self.send_error(404)
            return
        if not self._authorized():
            self._send_json({"message": "Unexpected rehearsal access token."}, status=401)
            return
        self._remote_objects.pop(path, None)
        self._stats["deleteRequests"] = int(self._stats.get("deleteRequests") or 0) + 1
        self._send_json({}, status=200)


_PackagedSyncRehearsalHandler.protocol_version = "HTTP/1.1"


def _start_packaged_sync_rehearsal_server(
    *,
    packaged_config: Any,
    snapshot_payload: dict[str, Any],
) -> tuple[str, dict[str, Any], http.server.ThreadingHTTPServer, threading.Thread]:
    snapshot_bytes = json.dumps(snapshot_payload, ensure_ascii=False).encode("utf-8")
    remote_objects: dict[str, dict[str, Any]] = {
        str(packaged_config.path): {
            "content": snapshot_bytes,
            "sha": "packaged-sync-rehearsal-sha",
        }
    }
    stats: dict[str, Any] = {
        "tokenRequests": 0,
        "contentRequests": 0,
        "putRequests": 0,
        "deleteRequests": 0,
        "bytesWritten": 0,
        "lastJwtPrefix": "",
    }

    def _handler_factory(*args: Any, **kwargs: Any) -> _PackagedSyncRehearsalHandler:
        return _PackagedSyncRehearsalHandler(
            *args,
            installation_id=packaged_config.installation_id,
            repo=packaged_config.repo,
            branch=packaged_config.branch,
            remote_objects=remote_objects,
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


def _packaged_sync_snapshot_payload(deps: Any) -> dict[str, Any]:
    return {
        "schemaVersion": deps.source_sync_mod.SYNC_SCHEMA_VERSION,
        "generatedAt": deps.utc_now_iso(),
        "source": {"name": "packaged_sync_rehearsal"},
        "active": [],
        "pending": [],
        "rejected": [],
    }


def _packaged_sync_runtime_env(
    deps: Any,
    *,
    artifacts_dir: Path,
    sync_api_base_url: str,
) -> dict[str, str]:
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
            deps.source_sync_mod.GITHUB_API_BASE_ENV: sync_api_base_url,
        }
    )
    deps.clear_packaged_desktop_session_state(runtime_env)
    return runtime_env


def _start_process_memory_sampler(deps: Any, process: Any) -> Any | None:
    if process is None:
        return None
    memory_sampler = deps.ProcessMemorySampler(int(getattr(process, "pid", 0) or 0))
    memory_sampler.start()
    return memory_sampler


def _stop_process_memory_sampler(memory_sampler: Any | None) -> dict[str, Any]:
    if memory_sampler is None:
        return {}
    metrics = memory_sampler.stop()
    return dict(metrics) if isinstance(metrics, dict) else {}


def _check_packaged_sync_status(deps: Any, *, bridge_port: int) -> None:
    status_code, status_payload = deps.request_json(
        f"http://127.0.0.1:{bridge_port}/sync/status?t={deps.time.time_ns()}",
        timeout_s=10.0,
    )
    if status_code != 200:
        raise RuntimeError(f"Packaged sync status failed: {status_payload}")
    payload = status_payload if isinstance(status_payload, dict) else {}
    status_config = (
        dict(payload.get("config") or {}) if isinstance(payload.get("config"), dict) else {}
    )
    if not bool(status_config.get("credentialsPackaged")):
        raise RuntimeError(f"Packaged sync config was not loaded by the runtime: {status_config}")
    if str(status_config.get("keyDerivation") or "").strip().lower() == "machine":
        raise RuntimeError(
            "Packaged sync runtime reported keyDerivation=machine for the shipped config."
        )
    if not bool(status_config.get("ready")):
        raise RuntimeError(f"Packaged sync config was not ready: {status_config}")


def _post_packaged_sync_action(
    deps: Any,
    *,
    bridge_port: int,
    action: str,
    runtime_timeout_s: float,
    failure_message: str,
) -> dict[str, Any]:
    status_code, payload = deps.post_json(
        f"http://127.0.0.1:{bridge_port}/sync/{action}",
        {},
        timeout_s=max(20.0, runtime_timeout_s),
    )
    result = payload if isinstance(payload, dict) else {}
    if status_code != 200 or not bool(result.get("ok")):
        raise RuntimeError(f"{failure_message}: {payload}")
    return result


def _check_sync_test_result(sync_test: dict[str, Any]) -> None:
    if not bool(sync_test.get("remoteFound")):
        raise RuntimeError(f"Packaged sync rehearsal did not read the remote snapshot: {sync_test}")


def _check_packaged_sync_rehearsal_stats(server_stats: dict[str, Any]) -> None:
    if int(server_stats.get("tokenRequests") or 0) <= 0:
        raise RuntimeError("Packaged sync rehearsal never requested a GitHub App token.")
    if int(server_stats.get("contentRequests") or 0) <= 0:
        raise RuntimeError("Packaged sync rehearsal never read the remote snapshot content.")
    if int(server_stats.get("putRequests") or 0) <= 0:
        raise RuntimeError("Packaged sync rehearsal never wrote the remote snapshot content.")


def _packaged_sync_success_result(
    *,
    started: float,
    config_path: Path,
    raw_payload: dict[str, Any],
    sync_api_base_url: str,
    server_stats: dict[str, Any],
    sync_test: dict[str, Any],
    sync_push: dict[str, Any],
    sync_pull: dict[str, Any],
    memory_metrics: dict[str, Any],
    stdout_path: Path,
    stderr_path: Path,
) -> dict[str, Any]:
    return {
        "name": "Packaged sync rehearsal",
        "slug": "packaged-sync-rehearsal",
        "status": "passed",
        "durationMs": int((time.perf_counter() - started) * 1000),
        "error": "",
        "memoryMetrics": memory_metrics,
        "details": {
            "packagedConfigPath": str(config_path),
            "keyDerivation": str(raw_payload.get("keyDerivation") or ""),
            "syncApiBaseUrl": str(sync_api_base_url),
            "tokenRequests": int(server_stats.get("tokenRequests") or 0),
            "contentRequests": int(server_stats.get("contentRequests") or 0),
            "putRequests": int(server_stats.get("putRequests") or 0),
            "deleteRequests": int(server_stats.get("deleteRequests") or 0),
            "bytesWritten": int(server_stats.get("bytesWritten") or 0),
            "syncTest": sync_test,
            "syncPush": sync_push,
            "syncPull": sync_pull,
            "pushTiming": dict(sync_push.get("timing") or {}),
            "pullTiming": dict(sync_pull.get("timing") or {}),
            "runtimeStdout": str(stdout_path),
            "runtimeStderr": str(stderr_path),
        },
    }


def _packaged_sync_failure_result(
    *,
    started: float,
    error: Exception,
    memory_metrics: dict[str, Any],
) -> dict[str, Any]:
    return {
        "name": "Packaged sync rehearsal",
        "slug": "packaged-sync-rehearsal",
        "status": "failed",
        "durationMs": int((time.perf_counter() - started) * 1000),
        "error": str(error),
        "memoryMetrics": memory_metrics,
    }


def _cleanup_packaged_sync_rehearsal(
    deps: Any,
    *,
    memory_sampler: Any | None,
    process: Any,
    stdout_handle: Any,
    stderr_handle: Any,
    server: http.server.ThreadingHTTPServer | None,
    server_thread: threading.Thread | None,
    site_port: int,
    bridge_port: int,
) -> None:
    if memory_sampler is not None:
        memory_sampler.stop()
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
    memory_sampler = None
    stdout_handle = None
    stderr_handle = None
    site_port = 0
    bridge_port = 0
    server = None
    server_thread = None
    try:
        config_path, raw_payload, packaged_config = (
            deps._load_portable_packaged_sync_rehearsal_config(portable_root)
        )
        base_url, server_stats, server, server_thread = deps._start_packaged_sync_rehearsal_server(
            packaged_config=packaged_config,
            snapshot_payload=_packaged_sync_snapshot_payload(deps),
        )
        runtime_env = _packaged_sync_runtime_env(
            deps,
            artifacts_dir=artifacts_dir,
            sync_api_base_url=base_url,
        )
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
        memory_sampler = _start_process_memory_sampler(deps, process)
        deps.wait_for_packaged_runtime(
            process,
            site_base_url=f"http://127.0.0.1:{site_port}",
            bridge_base_url=f"http://127.0.0.1:{bridge_port}",
            timeout_s=runtime_timeout_s,
            open_path="jobs.html",
        )
        _check_packaged_sync_status(deps, bridge_port=bridge_port)
        sync_test = _post_packaged_sync_action(
            deps,
            bridge_port=bridge_port,
            action="test",
            runtime_timeout_s=runtime_timeout_s,
            failure_message="Packaged sync rehearsal could not verify the GitHub connection",
        )
        _check_sync_test_result(sync_test)
        sync_push = _post_packaged_sync_action(
            deps,
            bridge_port=bridge_port,
            action="push",
            runtime_timeout_s=runtime_timeout_s,
            failure_message="Packaged sync rehearsal push failed",
        )
        sync_pull = _post_packaged_sync_action(
            deps,
            bridge_port=bridge_port,
            action="pull",
            runtime_timeout_s=runtime_timeout_s,
            failure_message="Packaged sync rehearsal pull failed",
        )
        _check_packaged_sync_rehearsal_stats(server_stats)
        memory_metrics = _stop_process_memory_sampler(memory_sampler)
        memory_sampler = None
        return _packaged_sync_success_result(
            started=started,
            config_path=config_path,
            raw_payload=raw_payload,
            sync_api_base_url=base_url,
            server_stats=server_stats,
            sync_test=sync_test,
            sync_push=sync_push,
            sync_pull=sync_pull,
            memory_metrics=memory_metrics,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
        )
    except Exception as exc:
        memory_metrics = _stop_process_memory_sampler(memory_sampler)
        memory_sampler = None
        return _packaged_sync_failure_result(
            started=started,
            error=exc,
            memory_metrics=memory_metrics,
        )
    finally:
        _cleanup_packaged_sync_rehearsal(
            deps,
            memory_sampler=memory_sampler,
            process=process,
            stdout_handle=stdout_handle,
            stderr_handle=stderr_handle,
            server=server,
            server_thread=server_thread,
            site_port=site_port,
            bridge_port=bridge_port,
        )
