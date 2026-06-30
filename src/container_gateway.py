#!/usr/bin/env python3
"""Container public gateway for stable same-origin UI/control routes."""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import threading
import time
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlparse
from urllib.request import Request, urlopen

from src import container_server
from src.app_version import get_app_version
from src.bridge.active_task_snapshot import (
    live_summary_from_snapshot,
    load_fresh_snapshot,
    pipeline_is_active,
    snapshot_has_active_task,
    snapshot_path,
    task_state_summary_from_snapshot,
)
from src.bridge.pipeline_control_files import (
    read_pipeline_status,
    write_abort_request,
)
from src.bridge.server.static_files import StaticFileService
from src.runtime_seed import seed_runtime_data

DEFAULT_INTERNAL_BRIDGE_PORT = 18080
DEFAULT_PROXY_TIMEOUT_SECONDS = 8.0
CONTROL_PROXY_TIMEOUT_SECONDS = 1.0
SCHEDULE_BRIDGE_TIMEOUT_SECONDS = 2.5


def _coerce_port(value: Any, default: int) -> int:
    try:
        port = int(str(value or "").strip())
    except (TypeError, ValueError):
        return int(default)
    return port if 1 <= port <= 65535 else int(default)


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _json_bytes(payload: Any) -> bytes:
    return json.dumps(payload, ensure_ascii=True, separators=(",", ":")).encode("utf-8")


def _route_path(raw_path: str) -> str:
    return str(urlparse(raw_path).path or "/").strip() or "/"


def _query_params(raw_path: str) -> dict[str, list[str]]:
    return parse_qs(urlparse(raw_path).query, keep_blank_values=True)


def _forwardable_headers(headers: Any) -> dict[str, str]:
    forwarded: dict[str, str] = {}
    for name in ("accept", "accept-encoding", "content-type"):
        value = headers.get(name)
        if value:
            forwarded[name.title()] = str(value)
    return forwarded


class _GatewayState:
    def __init__(
        self,
        *,
        data_dir: Path,
        static_root: Path,
        internal_base_url: str,
        bridge_process: subprocess.Popen[bytes],
    ) -> None:
        self.data_dir = Path(data_dir)
        self.static_service = StaticFileService(
            static_root=Path(static_root).resolve(),
            data_dir=Path(data_dir).resolve(),
        )
        self.internal_base_url = internal_base_url.rstrip("/")
        parsed_internal = urlparse(self.internal_base_url)
        self.internal_host = str(parsed_internal.hostname or "127.0.0.1")
        self.internal_port = _coerce_port(parsed_internal.port, DEFAULT_INTERNAL_BRIDGE_PORT)
        self.bridge_process = bridge_process
        self.started_at = _now_iso()

    def bridge_alive(self) -> bool:
        return self.bridge_process.poll() is None

    def bridge_listening(self, *, timeout: float = 0.15) -> bool:
        if not self.bridge_alive():
            return False
        try:
            with socket.create_connection(
                (self.internal_host, self.internal_port), timeout=float(timeout)
            ):
                return True
        except OSError:
            return False

    def ready_payload(self) -> dict[str, Any]:
        alive = self.bridge_alive()
        listening = self.bridge_listening()
        return {
            "ok": True,
            "service": "baluffo-container-gateway",
            "status": "healthy" if listening else "degraded",
            "summaryView": True,
            "detailLevel": "ready",
            "timestamp": _now_iso(),
            "startupReady": True,
            "desktopMode": False,
            "runtime": {"mode": "container", "localDataMode": "bridge"},
            "appVersion": get_app_version(),
            "bridge": {
                "mode": "internal",
                "alive": alive,
                "listening": listening,
                "exitCode": self.bridge_process.poll(),
            },
            "gateway": {"startedAt": self.started_at},
        }

    def pipeline_status_payload(self) -> dict[str, Any]:
        payload = read_pipeline_status(
            self.data_dir,
            app_version=get_app_version(),
            now_iso=_now_iso(),
        )
        payload["gatewayReady"] = True
        payload["bridgeAlive"] = self.bridge_alive()
        payload["bridgeListening"] = self.bridge_listening()
        return payload

    def _fresh_active_snapshot(self) -> dict[str, Any] | None:
        return load_fresh_snapshot(snapshot_path(self.data_dir))

    def _hot_snapshot_is_active(
        self,
        snapshot: dict[str, Any] | None,
        pipeline_status: dict[str, Any],
    ) -> bool:
        return bool(
            (snapshot and snapshot_has_active_task(snapshot)) or pipeline_is_active(pipeline_status)
        )

    def task_state_summary_payload(self) -> dict[str, Any] | None:
        pipeline_status = self.pipeline_status_payload()
        snapshot = self._fresh_active_snapshot()
        if not self._hot_snapshot_is_active(snapshot, pipeline_status):
            return {
                "tasks": [],
                "count": 0,
                "summary": True,
                "summaryView": True,
                "source": "container-gateway-idle",
                "gatewayReady": True,
                "bridgeAlive": self.bridge_alive(),
                "bridgeListening": self.bridge_listening(),
                "pipeline": pipeline_status,
            }
        return task_state_summary_from_snapshot(snapshot, pipeline_status=pipeline_status)

    def task_live_summary_payload(self, task_type: str) -> dict[str, Any] | None:
        pipeline_status = self.pipeline_status_payload()
        snapshot = self._fresh_active_snapshot()
        if not self._hot_snapshot_is_active(snapshot, pipeline_status):
            normalized_task_type = str(task_type or "").strip().lower()
            return {
                "ok": True,
                "summaryView": True,
                "taskType": normalized_task_type,
                "active": False,
                "status": "idle",
                "tasks": [],
                "workItems": [],
                "recentEvents": [],
                "source": "container-gateway-idle",
                "gatewayReady": True,
                "bridgeAlive": self.bridge_alive(),
                "bridgeListening": self.bridge_listening(),
                "pipeline": pipeline_status,
            }
        return live_summary_from_snapshot(snapshot, task_type, pipeline_status=pipeline_status)

    def pipeline_schedule_payload(self) -> dict[str, Any]:
        path = self.data_dir / "jobs-pipeline-schedule-config.json"
        saved_config: dict[str, Any] = {}
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                saved_config = raw
        except (OSError, json.JSONDecodeError, UnicodeDecodeError):
            saved_config = {}
        enabled = bool(saved_config.get("enabled", False))
        interval_hours = saved_config.get("intervalHours")
        configured_at = str(saved_config.get("configuredAt") or "").strip()
        schedule: dict[str, Any] = {"enabled": enabled}
        if isinstance(interval_hours, int | float) and interval_hours > 0:
            schedule["intervalHours"] = int(interval_hours)
        if enabled and configured_at:
            schedule["configuredAt"] = configured_at
        pipeline_status = self.pipeline_status_payload()
        computed = self._pipeline_schedule_status_fallback(
            enabled=enabled,
            interval_hours=int(schedule.get("intervalHours") or 0),
            configured_at=configured_at,
            pipeline_status=pipeline_status,
        )
        last_pipeline_finished_at = str(computed.get("lastPipelineFinishedAt") or "")
        next_run_at = str(computed.get("nextRunAt") or "")
        due = bool(computed.get("due"))
        schedule_delayed = False
        if due and not last_pipeline_finished_at:
            due = False
            next_run_at = ""
            schedule_delayed = True
        status = {
            "enabled": enabled,
            "pending": bool(computed.get("pending")),
            "due": due,
            "nextRunAt": next_run_at,
            "lastPipelineFinishedAt": last_pipeline_finished_at,
            "lastTriggerRunId": "",
            "lastTriggerError": "",
            "pipeline": pipeline_status,
        }
        if computed.get("nextAfterCurrentCompletes"):
            status["nextAfterCurrentCompletes"] = True
        if schedule_delayed:
            status["scheduleDelayed"] = True
            status["scheduleAuthority"] = "degraded"
        return {
            "ok": True,
            "summaryView": True,
            "degraded": True,
            "source": "container-gateway-fallback",
            "savedConfig": schedule,
            "status": status,
            "schedule": {"pipeline": {**status, **schedule}},
            "gatewayReady": True,
            "bridgeAlive": self.bridge_alive(),
            "bridgeListening": self.bridge_listening(),
        }

    def _bridge_json_payload(self, path: str, *, timeout: float = 0.5) -> dict[str, Any] | None:
        if not self.bridge_alive() or not self.bridge_listening(timeout=0.05):
            return None
        target = f"{self.internal_base_url}{path}"
        request = Request(target, method="GET", headers={"Accept": "application/json"})
        try:
            with urlopen(request, timeout=max(0.1, float(timeout))) as response:
                body = response.read()
                charset = response.headers.get_content_charset() or "utf-8"
            payload = json.loads(body.decode(charset, errors="replace") or "{}")
        except (
            HTTPError,
            OSError,
            TimeoutError,
            URLError,
            UnicodeDecodeError,
            json.JSONDecodeError,
            ValueError,
        ):
            return None
        return payload if isinstance(payload, dict) else None

    @staticmethod
    def _schedule_from_pipeline_schedule_payload(
        payload: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        if not isinstance(payload, dict):
            return None
        schedule = payload.get("schedule")
        if isinstance(schedule, dict) and isinstance(schedule.get("pipeline"), dict):
            return schedule
        status = payload.get("status")
        if not isinstance(status, dict):
            return None
        saved_config = payload.get("savedConfig")
        saved = saved_config if isinstance(saved_config, dict) else {}
        pipeline = dict(status)
        if "enabled" not in pipeline and "enabled" in saved:
            pipeline["enabled"] = bool(saved.get("enabled"))
        if "intervalHours" not in pipeline:
            try:
                interval_hours = int(saved.get("intervalHours") or 0)
            except (TypeError, ValueError):
                interval_hours = 0
            if interval_hours > 0:
                pipeline["intervalHours"] = interval_hours
        return {"pipeline": pipeline}

    def _admin_schedule_payload(self) -> dict[str, Any]:
        bridge_payload = self._bridge_json_payload(
            "/tasks/jobs-pipeline-schedule",
            timeout=SCHEDULE_BRIDGE_TIMEOUT_SECONDS,
        )
        bridge_schedule = self._schedule_from_pipeline_schedule_payload(bridge_payload)
        if bridge_schedule is not None:
            return {
                "ok": True,
                "summaryView": True,
                "degraded": True,
                "source": "container-gateway-bridge-schedule",
                "schedule": bridge_schedule,
                "gatewayReady": True,
                "bridgeAlive": self.bridge_alive(),
                "bridgeListening": self.bridge_listening(),
            }
        fallback = self.pipeline_schedule_payload()
        return {
            "ok": True,
            "summaryView": True,
            "degraded": True,
            "source": "container-gateway-fallback",
            "schedule": fallback.get("schedule") or {},
            "savedConfig": fallback.get("savedConfig") or {},
            "status": fallback.get("status") or {},
            "scheduleDelayed": True,
            "message": "Pipeline schedule delayed; retrying authoritative schedule route.",
            "gatewayReady": True,
            "bridgeAlive": self.bridge_alive(),
            "bridgeListening": self.bridge_listening(),
        }

    @staticmethod
    def _parse_iso_datetime(value: Any) -> datetime | None:
        text = str(value or "").strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            return None

    def _lifecycle_rows(self) -> list[dict[str, Any]]:
        path = self.data_dir / "jobs-lifecycle-state.json"
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, UnicodeDecodeError):
            return []
        rows = raw.get("rows") if isinstance(raw, dict) else []
        if not isinstance(rows, list):
            return []
        return [dict(row) for row in rows if isinstance(row, dict)]

    def _latest_terminal_pipeline_row(self) -> dict[str, Any] | None:
        candidates: list[tuple[datetime, int, dict[str, Any]]] = []
        for index, row in enumerate(self._lifecycle_rows()):
            task_type = str(row.get("taskType") or row.get("type") or "").strip().lower()
            if task_type != "pipeline":
                continue
            finished_at = str(row.get("finishedAt") or "").strip()
            parsed = self._parse_iso_datetime(finished_at)
            if parsed is None:
                continue
            candidates.append((parsed, index, dict(row)))
        if not candidates:
            return None
        return max(candidates, key=lambda item: (item[0], item[1]))[2]

    def _pipeline_schedule_status_fallback(
        self,
        *,
        enabled: bool,
        interval_hours: int,
        configured_at: str,
        pipeline_status: dict[str, Any],
    ) -> dict[str, Any]:
        active = bool(pipeline_status.get("active") or pipeline_status.get("running"))
        if not enabled:
            return {
                "pending": False,
                "due": False,
                "nextRunAt": "",
                "lastPipelineFinishedAt": "",
            }
        last_row = self._latest_terminal_pipeline_row()
        last_finished_at = str((last_row or {}).get("finishedAt") or "").strip()
        last_finished = self._parse_iso_datetime(last_finished_at)
        if last_finished is not None and interval_hours > 0:
            next_run_at = last_finished + timedelta(hours=interval_hours)
            now = (
                datetime.now(next_run_at.tzinfo)
                if next_run_at.tzinfo is not None
                else datetime.now()
            )
            due = now >= next_run_at
            if active and due:
                return {
                    "pending": False,
                    "due": False,
                    "nextRunAt": next_run_at.isoformat(),
                    "lastPipelineFinishedAt": last_finished_at,
                    "blockedByActiveRun": True,
                    "activeScheduledRun": True,
                    "nextAfterCurrentCompletes": True,
                }
            return {
                "pending": False,
                "due": due,
                "nextRunAt": next_run_at.isoformat(),
                "lastPipelineFinishedAt": last_finished_at,
            }
        if interval_hours > 0:
            anchor = self._pipeline_schedule_anchor_datetime(configured_at)
            next_run_at = anchor + timedelta(hours=interval_hours)
            now = (
                datetime.now(next_run_at.tzinfo)
                if next_run_at.tzinfo is not None
                else datetime.now()
            )
            due = now >= next_run_at
            if active and due:
                return {
                    "pending": False,
                    "due": False,
                    "nextRunAt": next_run_at.isoformat(),
                    "lastPipelineFinishedAt": last_finished_at,
                    "blockedByActiveRun": True,
                    "activeScheduledRun": True,
                    "nextAfterCurrentCompletes": True,
                }
            return {
                "pending": False,
                "due": due,
                "nextRunAt": next_run_at.isoformat(),
                "lastPipelineFinishedAt": last_finished_at,
            }
        return {
            "pending": False,
            "due": not active,
            "nextRunAt": "",
            "lastPipelineFinishedAt": last_finished_at,
            **({"nextAfterCurrentCompletes": True} if active else {}),
        }

    def _pipeline_schedule_anchor_datetime(self, configured_at: str) -> datetime:
        parsed = self._parse_iso_datetime(configured_at)
        if parsed is not None:
            return parsed
        try:
            mtime = (self.data_dir / "jobs-pipeline-schedule-config.json").stat().st_mtime
            return datetime.fromtimestamp(mtime, tz=UTC)
        except OSError:
            return datetime.now(UTC)

    def _dashboard_health_summary_payload_with_schedule(
        self, schedule_payload: dict[str, Any]
    ) -> dict[str, Any]:
        return {
            "ok": True,
            "status": "degraded",
            "summaryView": True,
            "detailLevel": "summary",
            "degraded": True,
            "source": "container-gateway-fallback",
            "alerts": [],
            "alertsEvaluated": False,
            "alertBasis": "gateway-degraded",
            "suppressedAlertsCount": 0,
            "kpis": {},
            "kpisDelayed": True,
            "schedule": schedule_payload.get("schedule") or {},
            "scheduleDelayed": bool(schedule_payload.get("scheduleDelayed")),
            "gatewayReady": True,
            "bridgeAlive": self.bridge_alive(),
            "bridgeListening": self.bridge_listening(),
            "message": "Admin data delayed; retrying.",
        }

    def dashboard_health_summary_payload(self) -> dict[str, Any]:
        schedule_payload = self._admin_schedule_payload()
        return self._dashboard_health_summary_payload_with_schedule(schedule_payload)

    def admin_bootstrap_payload(self) -> dict[str, Any]:
        task_state = self.task_state_summary_payload() or {}
        schedule_payload = self._admin_schedule_payload()
        return {
            "ok": True,
            "summaryView": True,
            "degraded": True,
            "source": "container-gateway-fallback",
            "app": self.ready_payload(),
            "overview": {"degraded": True, "delayed": True},
            "ops": self._dashboard_health_summary_payload_with_schedule(schedule_payload),
            "tasks": {
                "current": task_state.get("tasks") or [],
                "recent": [],
                "summary": True,
                "source": task_state.get("source") or "container-gateway-fallback",
            },
            "sync": {"ok": True, "summaryView": True, "degraded": True, "delayed": True},
            "registrySummary": {
                "ok": True,
                "summaryStatus": "unavailable",
                "degraded": True,
                "delayed": True,
            },
            "schedule": schedule_payload.get("schedule") or {},
            "pipeline": self.pipeline_status_payload(),
            "gatewayReady": True,
            "bridgeAlive": self.bridge_alive(),
            "bridgeListening": self.bridge_listening(),
            "message": "Admin data delayed; retrying.",
        }


class _GatewayHandler(BaseHTTPRequestHandler):
    gateway_state: _GatewayState | None = None

    def _state(self) -> _GatewayState:
        if self.gateway_state is None:
            raise RuntimeError("container gateway state is not configured")
        return self.gateway_state

    def log_message(self, format: str, *args: Any) -> None:
        if str(os.environ.get("BALUFFO_GATEWAY_QUIET_REQUESTS") or "").lower() in {
            "1",
            "true",
            "yes",
        }:
            return
        super().log_message(format, *args)

    def send_json(self, payload: Any, status: int = 200) -> None:
        body = _json_bytes(payload)
        self.send_response(int(status or 200))
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_bytes(
        self,
        body: bytes,
        *,
        content_type: str,
        filename: str = "",
        disposition: str = "inline",
        status: int = 200,
        cache_control: str = "no-store",
        content_encoding: str = "",
    ) -> None:
        self.send_response(int(status or 200))
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", cache_control)
        if content_encoding:
            self.send_header("Content-Encoding", content_encoding)
        if filename:
            safe_filename = str(filename).replace('"', "")
            safe_disposition = (
                "attachment" if str(disposition).lower() == "attachment" else "inline"
            )
            self.send_header(
                "Content-Disposition", f'{safe_disposition}; filename="{safe_filename}"'
            )
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _proxy(self, *, timeout: float = DEFAULT_PROXY_TIMEOUT_SECONDS) -> None:
        state = self._state()
        target = f"{state.internal_base_url}{self.path}"
        body: bytes | None = None
        if self.command.upper() in {"POST", "PUT", "PATCH"}:
            length = int(self.headers.get("content-length") or 0)
            body = self.rfile.read(max(0, length))
        request = Request(
            target,
            data=body,
            method=self.command.upper(),
            headers=_forwardable_headers(self.headers),
        )
        try:
            with urlopen(request, timeout=float(timeout)) as response:
                response_body = response.read()
                self._send_proxied_response(
                    int(response.status or 200),
                    response.headers,
                    response_body,
                )
        except HTTPError as exc:
            self._send_proxied_response(int(exc.code or 500), exc.headers, exc.read())
        except (OSError, TimeoutError, URLError) as exc:
            self.send_json(
                {
                    "ok": False,
                    "error": "bridge_degraded",
                    "detail": str(exc),
                    "gatewayReady": True,
                },
                status=504,
            )

    def _proxy_or_fallback(
        self,
        fallback_payload: Any,
        *,
        timeout: float = 0.75,
    ) -> None:
        state = self._state()
        if not state.bridge_alive() or not state.bridge_listening(timeout=0.05):
            self.send_json(fallback_payload() if callable(fallback_payload) else fallback_payload)
            return
        target = f"{state.internal_base_url}{self.path}"
        request = Request(
            target,
            method=self.command.upper(),
            headers=_forwardable_headers(self.headers),
        )
        try:
            with urlopen(request, timeout=float(timeout)) as response:
                self._send_proxied_response(
                    int(response.status or 200),
                    response.headers,
                    response.read(),
                )
        except HTTPError as exc:
            if int(exc.code or 0) == 504:
                self.send_json(
                    fallback_payload() if callable(fallback_payload) else fallback_payload
                )
                return
            self._send_proxied_response(int(exc.code or 500), exc.headers, exc.read())
        except (OSError, TimeoutError, URLError):
            self.send_json(fallback_payload() if callable(fallback_payload) else fallback_payload)

    def _send_proxied_response(self, status: int, headers: Any, body: bytes) -> None:
        self.send_response(int(status or 200))
        blocked = {"connection", "content-length", "transfer-encoding", "server", "date"}
        for name, value in headers.items():
            if str(name).lower() in blocked:
                continue
            self.send_header(str(name), str(value))
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _handle_abort(self) -> None:
        state = self._state()
        length = int(self.headers.get("content-length") or 0)
        raw = self.rfile.read(max(0, length))
        try:
            payload = json.loads(raw.decode("utf-8") or "{}")
        except (json.JSONDecodeError, UnicodeDecodeError):
            self.send_json({"ok": False, "error": "invalid_json"}, status=400)
            return
        task_type = str(payload.get("taskType") or payload.get("type") or "").strip().lower()
        run_id = str(payload.get("runId") or payload.get("id") or "").strip()
        if task_type == "pipeline" and run_id:
            accepted = write_abort_request(
                state.data_dir,
                run_id=run_id,
                task_type=task_type,
                reason=str(payload.get("reason") or "user_abort_requested").strip(),
                requested_at=_now_iso(),
            )
            try:
                request = Request(
                    f"{state.internal_base_url}{self.path}",
                    data=raw,
                    method="POST",
                    headers=_forwardable_headers(self.headers),
                )
                with urlopen(request, timeout=CONTROL_PROXY_TIMEOUT_SECONDS) as response:
                    self._send_proxied_response(
                        int(response.status or 200),
                        response.headers,
                        response.read(),
                    )
                    return
            except (HTTPError, OSError, TimeoutError, URLError):
                self.send_json(
                    {
                        **accepted,
                        "state": "abort_queued",
                        "deferred": True,
                        "gatewayAccepted": True,
                    },
                    status=202,
                )
                return
        self._proxy(timeout=DEFAULT_PROXY_TIMEOUT_SECONDS)

    def _handle_gateway_control_get(self, path: str, view: str) -> bool:
        state = self._state()
        if path == "/tasks/jobs-pipeline-schedule":
            self._proxy_or_fallback(state.pipeline_schedule_payload)
            return True
        if path == "/ops/dashboard-health" and view == "summary":
            self._proxy_or_fallback(state.dashboard_health_summary_payload)
            return True
        if path == "/admin/bootstrap":
            self._proxy_or_fallback(state.admin_bootstrap_payload)
            return True
        return False

    def do_GET(self) -> None:
        state = self._state()
        path = _route_path(self.path)
        query = _query_params(self.path)
        if path == "/app/ready":
            self.send_json(state.ready_payload())
            return
        if path == "/tasks/run-jobs-pipeline-status":
            self.send_json(state.pipeline_status_payload())
            return
        view = str((query.get("view") or [""])[0] or "").strip().lower()
        if path == "/ops/task-state" and view == "summary":
            payload = state.task_state_summary_payload()
            if payload is not None:
                self.send_json(payload)
                return
        if path.startswith("/ops/task-live/") and view == "summary":
            task_type = path.removeprefix("/ops/task-live/").strip().lower()
            payload = state.task_live_summary_payload(task_type)
            if payload is not None:
                self.send_json(payload)
                return
        if self._handle_gateway_control_get(path, view):
            return
        if state.static_service.handle_get(self, path=path):
            return
        self._proxy()

    def do_POST(self) -> None:
        path = _route_path(self.path)
        if path == "/tasks/abort":
            self._handle_abort()
            return
        self._proxy()

    def do_OPTIONS(self) -> None:
        self.send_response(204)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", "0")
        self.end_headers()


def _make_gateway_handler(state: _GatewayState) -> type[BaseHTTPRequestHandler]:
    return type("GatewayHandler", (_GatewayHandler,), {"gateway_state": state})


def _spawn_bridge(config: Any, *, internal_port: int) -> subprocess.Popen[bytes]:
    command = [
        sys.executable,
        "-m",
        "src.container_server",
        "--host",
        "127.0.0.1",
        "--port",
        str(internal_port),
        "--data-dir",
        str(config.data_dir),
        "--log-format",
        str(config.log_format or "jsonl"),
    ]
    if getattr(config, "quiet_requests", False):
        command.append("--quiet-requests")
    env = dict(os.environ)
    env["BALUFFO_INTERNAL_BRIDGE"] = "1"
    return subprocess.Popen(command, cwd=str(config.root), env=env)


def _start_exit_monitor(process: subprocess.Popen[bytes]) -> None:
    def _monitor() -> None:
        code = process.wait()
        os._exit(int(code or 1))

    thread = threading.Thread(target=_monitor, name="baluffo-bridge-monitor", daemon=True)
    thread.start()


def _terminate_bridge(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    with suppress(OSError):
        process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        with suppress(OSError):
            process.kill()


def main(argv: list[str] | None = None) -> int:
    config = container_server.parse_args(argv)
    seed_runtime_data(Path(config.data_dir), source_root=Path(config.root), overwrite=False)
    internal_port = _coerce_port(
        os.environ.get("BALUFFO_INTERNAL_BRIDGE_PORT"),
        DEFAULT_INTERNAL_BRIDGE_PORT,
    )
    bridge = _spawn_bridge(config, internal_port=internal_port)
    _start_exit_monitor(bridge)
    state = _GatewayState(
        data_dir=Path(config.data_dir),
        static_root=Path(config.root),
        internal_base_url=f"http://127.0.0.1:{internal_port}",
        bridge_process=bridge,
    )
    handler_cls = _make_gateway_handler(state)
    server = ThreadingHTTPServer((str(config.host), int(config.port)), handler_cls)
    server.daemon_threads = True
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        return 0
    finally:
        server.server_close()
        _terminate_bridge(bridge)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
