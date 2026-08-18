"""Side effects: instance locking, bridge health checks, stale runtime reclaim. Verify: npm run test:frontend:packaged:desktop-lifecycle-rehearsal.

AI boundary owns: desktop session roots, instance locking, bridge health checks, and stale runtime reclaim.
AI boundary implement in: this file for session state and ownership; platform primitives stay in _windows/_linux.
AI boundary search before contracts: launcher recovery, runtime launcher, and desktop session tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused desktop session tests.
"""

from __future__ import annotations

import contextlib
import json
import os
import random
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from ._compat import desktop_api
from .config import ACTIVE_WORK_TASK_TYPES, INSTANCE_CONFLICT_RETRY_S, INSTANCE_LOCK_WAIT_S

_EXPECTED_RECLAIM_CALLBACK_EXCEPTIONS = (OSError, RuntimeError, TypeError, ValueError)
_LOCK_INVALID_PAYLOAD_GRACE_S = 1.0
_LOCK_BACKOFF_BASE_S = 0.05
_LOCK_BACKOFF_MAX_S = 0.25


@dataclass(frozen=True)
class InstanceLock:
    path: Path
    handle: int
    launcher_token: str = ""
    created_at: str = ""


def _os_error_trace_fields(exc: OSError) -> dict[str, object]:
    fields: dict[str, object] = {
        "error": str(exc),
        "errno": int(exc.errno or 0),
    }
    winerror = getattr(exc, "winerror", None)
    if winerror is not None:
        fields["winerror"] = int(winerror or 0)
    return fields


def _append_startup_trace_from_env(
    env: dict[str, str] | None,
    event: str,
    **fields: object,
) -> None:
    env_map = env if env is not None else os.environ
    data_dir = str(env_map.get("BALUFFO_DATA_DIR") or "").strip()
    if not data_dir:
        return
    with contextlib.suppress(Exception):
        desktop_api()._append_startup_trace(Path(data_dir), event, **fields)


def _write_text_atomic(path: Path, text: str) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(f"{target.name}.{os.getpid()}.{time.time_ns()}.tmp")
    try:
        tmp.write_text(text, encoding="utf-8")
        os.replace(tmp, target)
    finally:
        with contextlib.suppress(OSError):
            if tmp.exists():
                tmp.unlink()


def _lock_path_is_recent(path: Path, *, grace_s: float = _LOCK_INVALID_PAYLOAD_GRACE_S) -> bool:
    try:
        age_s = time.time() - path.stat().st_mtime
    except OSError:
        return False
    return 0.0 <= age_s <= max(0.0, float(grace_s))


def _lock_path_may_still_be_initializing(path: Path) -> bool:
    try:
        return path.stat().st_size == 0
    except OSError:
        return False


def _lock_backoff_delay(attempt: int) -> float:
    delay = min(_LOCK_BACKOFF_MAX_S, _LOCK_BACKOFF_BASE_S * (2 ** max(0, int(attempt))))
    jitter = random.uniform(0.0, delay * 0.25)
    return float(min(_LOCK_BACKOFF_MAX_S, delay + jitter))


def _sleep_for_lock_retry(attempt: int, deadline: float) -> None:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        return
    time.sleep(min(_lock_backoff_delay(attempt), remaining))


def _as_dict(value: object) -> dict[str, object]:
    return value if isinstance(value, dict) else {}


def _as_list(value: object) -> list[object]:
    return value if isinstance(value, list) else []


def _as_int(value: object, default: int = 0) -> int:
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


def load_session_state(env: dict[str, str] | None = None) -> dict[str, object]:
    api = desktop_api()
    path = api.resolve_session_state_path(env)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def save_session_state(payload: dict[str, object], env: dict[str, str] | None = None) -> Path:
    api = desktop_api()
    path = Path(api.resolve_session_state_path(env))
    try:
        _write_text_atomic(path, json.dumps(payload, ensure_ascii=False, indent=2))
    except OSError as exc:
        _append_startup_trace_from_env(
            env,
            "desktop_session_state_write_failed",
            path=str(path),
            **_os_error_trace_fields(exc),
        )
        raise
    return path


def clear_session_state(env: dict[str, str] | None = None) -> None:
    api = desktop_api()
    path = api.resolve_session_state_path(env)
    with contextlib.suppress(OSError):
        path.unlink()


def _read_instance_lock_payload(path: Path) -> dict[str, object]:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return {}
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return {}
    if not isinstance(payload, dict):
        return {}
    pid = _as_int(payload.get("pid"))
    if pid <= 0:
        return {}
    if not str(payload.get("createdAt") or "").strip():
        return {}
    if not str(payload.get("launcherToken") or "").strip():
        return {}
    if not str(payload.get("exePath") or "").strip():
        return {}
    if not str(payload.get("sessionRoot") or "").strip():
        return {}
    state = str(payload.get("state") or "").strip()
    if state not in {"launching", "running"}:
        return {}
    return payload


def _make_lock_payload(
    *, launcher_token: str, state: str, session_root: Path, created_at: str | None = None
) -> dict[str, object]:
    api = desktop_api()
    return {
        "pid": int(os.getpid()),
        "createdAt": str(created_at or datetime.now(UTC).isoformat()),
        "launcherToken": str(launcher_token or ""),
        "exePath": api._current_exe_path(),
        "sessionRoot": str(session_root),
        "state": str(state or "launching"),
    }


def _write_lock_payload_to_handle(handle: int, payload: dict[str, object]) -> None:
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8", errors="replace")
    os.lseek(handle, 0, os.SEEK_SET)
    os.write(handle, data)
    os.ftruncate(handle, len(data))


def _write_lock_payload(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _process_identity_matches(lock_payload: dict[str, object]) -> bool:
    api = desktop_api()
    pid = _as_int(lock_payload.get("pid"))
    if pid <= 0 or not api.is_process_alive(pid):
        return False
    if os.name != "nt":
        return True
    process_path = api._normalize_path_text(api._get_windows_process_image_path(pid))
    if not process_path:
        return False
    lock_exe = api._normalize_path_text(lock_payload.get("exePath"))
    if not lock_exe:
        return False
    if process_path != lock_exe:
        return False
    lock_created_ts = api._parse_metric_ts(lock_payload.get("createdAt"))
    process_created_ts = api._get_windows_process_start_ts(pid)
    if (
        lock_created_ts > 0.0
        and process_created_ts > 0.0
        and abs(lock_created_ts - process_created_ts) > 180.0
    ):
        return False
    return True


def acquire_instance_lock(
    *,
    timeout_s: float = INSTANCE_LOCK_WAIT_S,
    env: dict[str, str] | None = None,
    launcher_token: str = "",
    on_reclaim: Callable[[str], None] | None = None,
) -> InstanceLock | None:
    api = desktop_api()
    path = api.resolve_instance_lock_path(env)
    session_root = path.parent
    session_root.mkdir(parents=True, exist_ok=True)
    token = str(launcher_token or uuid.uuid4().hex)
    deadline = time.monotonic() + max(0.2, float(timeout_s))
    sleep_attempt = 0
    while time.monotonic() < deadline:
        try:
            # O_CREAT|O_EXCL|O_RDWR provides atomic cross-process file-based locking.
            handle = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_RDWR)
        except FileExistsError:
            lock_payload = api._read_instance_lock_payload(path)
            if (
                not lock_payload
                and api._lock_path_may_still_be_initializing(path)
                and api._lock_path_is_recent(path)
            ):
                api._sleep_for_lock_retry(sleep_attempt, deadline)
                sleep_attempt += 1
                continue
            if not api._process_identity_matches(lock_payload):
                try:
                    path.unlink()
                except OSError as exc:
                    api._append_startup_trace_from_env(
                        env,
                        "desktop_lock_reclaim_unlink_failed",
                        path=str(path),
                        **api._os_error_trace_fields(exc),
                    )
                    api._sleep_for_lock_retry(sleep_attempt, deadline)
                    sleep_attempt += 1
                    continue
                if callable(on_reclaim):
                    with contextlib.suppress(*_EXPECTED_RECLAIM_CALLBACK_EXCEPTIONS):
                        on_reclaim("stale_lock_owner")
                continue
            api._sleep_for_lock_retry(sleep_attempt, deadline)
            sleep_attempt += 1
            continue
        except OSError:
            api._sleep_for_lock_retry(sleep_attempt, deadline)
            sleep_attempt += 1
            continue
        payload = api._make_lock_payload(
            launcher_token=token, state="launching", session_root=session_root
        )
        try:
            api._write_lock_payload_to_handle(handle, payload)
        except OSError as exc:
            api._append_startup_trace_from_env(
                env,
                "desktop_lock_payload_write_failed",
                path=str(path),
                **api._os_error_trace_fields(exc),
            )
            with contextlib.suppress(OSError):
                os.close(handle)
            with contextlib.suppress(OSError):
                path.unlink()
            api._sleep_for_lock_retry(sleep_attempt, deadline)
            sleep_attempt += 1
            continue
        return InstanceLock(
            path=path,
            handle=handle,
            launcher_token=token,
            created_at=str(payload.get("createdAt") or ""),
        )
    return None


def update_instance_lock_state(lock: InstanceLock, state: str) -> None:
    api = desktop_api()
    if not lock:
        return
    payload = api._read_instance_lock_payload(lock.path)
    if not payload:
        payload = api._make_lock_payload(
            launcher_token=str(lock.launcher_token or uuid.uuid4().hex),
            state=str(state or "launching"),
            session_root=lock.path.parent,
            created_at=str(lock.created_at or datetime.now(UTC).isoformat()),
        )
    else:
        payload["state"] = str(state or "launching")
        payload.setdefault("launcherToken", str(lock.launcher_token or ""))
        payload.setdefault("createdAt", str(lock.created_at or datetime.now(UTC).isoformat()))
        payload.setdefault("exePath", api._current_exe_path())
    if int(lock.handle or 0) <= 2:
        with contextlib.suppress(OSError):
            api._write_lock_payload(lock.path, payload)
        return
    with contextlib.suppress(OSError):
        api._write_lock_payload_to_handle(lock.handle, payload)


def release_instance_lock(lock: InstanceLock | None) -> None:
    if lock is None:
        return
    with contextlib.suppress(OSError):
        os.close(lock.handle)
    with contextlib.suppress(OSError):
        lock.path.unlink()


def is_process_alive(pid: int) -> bool:
    api = desktop_api()
    if int(pid or 0) <= 0:
        return False
    if api.os.name == "nt":
        handle = api.ctypes.windll.kernel32.OpenProcess(
            api._PROCESS_SYNCHRONIZE | api._PROCESS_QUERY_LIMITED_INFORMATION,
            False,
            int(pid),
        )
        if not handle:
            return False
        try:
            wait_result = int(api.ctypes.windll.kernel32.WaitForSingleObject(handle, 0))
            if wait_result != api._WAIT_TIMEOUT:
                return False
            exit_code = api.ctypes.wintypes.DWORD(0)
            ok = api.ctypes.windll.kernel32.GetExitCodeProcess(handle, api.ctypes.byref(exit_code))
            return bool(ok) and int(exit_code.value) == api._STILL_ACTIVE
        finally:
            api.ctypes.windll.kernel32.CloseHandle(handle)
    try:
        os.kill(int(pid), 0)
        return True
    except OSError:
        return False


def _fetch_json(url: str, timeout_s: float = 2.5) -> dict[str, object]:
    # HTTP GET to an external URL — network side effect.
    with urllib.request.urlopen(url, timeout=timeout_s) as response:  # noqa: S310
        payload = json.loads(response.read().decode("utf-8", errors="replace") or "{}")
    return payload if isinstance(payload, dict) else {}


def is_baluffo_bridge_healthy(
    bridge_port: int,
    *,
    timeout_s: float = 2.0,
    require_desktop_mode: bool = False,
) -> bool:
    try:
        payload = _fetch_json(
            f"http://127.0.0.1:{int(bridge_port)}/ops/health", timeout_s=timeout_s
        )
    except (OSError, ValueError, urllib.error.URLError, json.JSONDecodeError):
        return False
    if str(payload.get("service") or "") != "baluffo-bridge":
        return False
    if require_desktop_mode and not bool(payload.get("desktopMode")):
        return False
    return True


def get_baluffo_bridge_health(bridge_port: int, *, timeout_s: float = 2.0) -> dict[str, object]:
    try:
        payload = _fetch_json(
            f"http://127.0.0.1:{int(bridge_port)}/ops/health", timeout_s=timeout_s
        )
    except (OSError, ValueError, urllib.error.URLError, json.JSONDecodeError):
        return {}
    return payload if str(payload.get("service") or "") == "baluffo-bridge" else {}


def _bridge_health_matches_owner_session(payload: dict[str, object], *, owner_token: str) -> bool:
    if str(payload.get("service") or "") != "baluffo-bridge":
        return False
    if not bool(payload.get("desktopMode")):
        return False
    owner = _as_dict(payload.get("owner"))
    return str(owner.get("token") or "").strip() == str(owner_token or "").strip()


def validate_session_state(
    state: dict[str, object],
    *,
    expected_launcher_token: str = "",
) -> tuple[bool, str]:
    api = desktop_api()
    launcher_pid = _as_int(state.get("launcherPid"))
    bridge_port = _as_int(state.get("bridgePort"))
    if launcher_pid <= 0:
        return False, "missing_launcher_pid"
    if bridge_port <= 0:
        return False, "missing_bridge_port"
    launcher_token = str(state.get("launcherToken") or "").strip()
    if not launcher_token:
        return False, "missing_launcher_token"
    launcher_started_at = str(state.get("launcherStartedAt") or "").strip()
    if not launcher_started_at:
        return False, "missing_launcher_started_at"
    session_exe_path = str(state.get("exePath") or "").strip()
    if not session_exe_path:
        return False, "missing_exe_path"
    if not api._process_identity_matches(
        {
            "pid": launcher_pid,
            "createdAt": launcher_started_at,
            "exePath": session_exe_path,
        },
    ):
        return False, "launcher_identity_mismatch"
    if expected_launcher_token and launcher_token != expected_launcher_token:
        return False, "launcher_token_mismatch"
    if not api.is_baluffo_bridge_healthy(bridge_port, require_desktop_mode=True):
        return False, "bridge_unhealthy"
    return True, "ok"


def get_valid_session_state(
    env: dict[str, str] | None = None,
    *,
    expected_launcher_token: str = "",
    clear_invalid: bool = True,
) -> dict[str, object]:
    api = desktop_api()
    state = _as_dict(api.load_session_state(env))
    if not state:
        return {}
    ok, _reason = api.validate_session_state(
        state,
        expected_launcher_token=expected_launcher_token,
    )
    if ok:
        return state
    if clear_invalid:
        api.clear_session_state(env)
    return {}


def _truncate_reason(reason: object, *, limit: int = 120) -> str:
    text = str(reason or "").strip()
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)] + "..."


def _clear_stale_instance_artifacts(*, env: dict[str, str] | None = None) -> None:
    api = desktop_api()
    lock_path = api.resolve_instance_lock_path(env)
    with contextlib.suppress(OSError):
        lock_path.unlink()
    api.clear_session_state(env)


def _reclaim_stale_instance_artifacts(
    *,
    data_dir: Path,
    stale_state: dict[str, object] | None = None,
    env: dict[str, str] | None = None,
) -> dict[str, object]:
    api = desktop_api()
    reclaim_result = _as_dict(
        api._windows_reclaim_stale_runtime_children(
            stale_state if isinstance(stale_state, dict) else {},
            data_dir=data_dir,
        )
    )
    api._clear_stale_instance_artifacts(env=env)
    return reclaim_result


def _normalize_active_task_descriptor(
    row: dict[str, object], *, fallback_task_type: str = ""
) -> dict[str, str]:
    return {
        "taskType": str(row.get("taskType") or row.get("type") or fallback_task_type or "")
        .strip()
        .lower(),
        "runId": str(row.get("runId") or "").strip(),
        "status": str(row.get("status") or "").strip().lower(),
    }


def _task_descriptor_is_active(task: dict[str, str], row: dict[str, object]) -> bool:
    if task["taskType"] not in ACTIVE_WORK_TASK_TYPES:
        return False
    if bool(row.get("active")):
        return True
    if task["status"] in {"running", "pending"}:
        return True
    if _as_int(row.get("pid")) > 0 and not str(row.get("finishedAt") or "").strip():
        return True
    return False


def _load_active_critical_desktop_tasks(
    data_dir: Path,
    *,
    bridge_port: int,
    timeout_s: float = 1.5,
    allow_disk_fallback: bool = True,
) -> list[dict[str, str]]:
    try:
        payload = _fetch_json(
            f"http://127.0.0.1:{int(bridge_port)}/ops/task-state?view=summary",
            timeout_s=timeout_s,
        )
    except (OSError, ValueError, urllib.error.URLError, json.JSONDecodeError):
        payload = {}
    rows = _as_list(payload.get("tasks"))
    active_tasks: list[dict[str, str]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        task = _normalize_active_task_descriptor(row)
        if _task_descriptor_is_active(task, row):
            active_tasks.append(task)
    if active_tasks:
        return active_tasks

    if not allow_disk_fallback:
        return []

    task_state_path = Path(data_dir) / "admin-task-state.json"
    try:
        task_state_payload = json.loads(task_state_path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return []
    if not isinstance(task_state_payload, dict):
        return []

    disk_tasks: list[dict[str, str]] = []
    for fallback_task_type, row in task_state_payload.items():
        if not isinstance(row, dict):
            continue
        task = _normalize_active_task_descriptor(row, fallback_task_type=str(fallback_task_type))
        if _task_descriptor_is_active(task, row):
            disk_tasks.append(task)
    return disk_tasks


def diagnose_instance_conflict(
    *,
    data_dir: Path,
    timeout_s: float = INSTANCE_CONFLICT_RETRY_S,
    env: dict[str, str] | None = None,
) -> dict[str, object]:
    api = desktop_api()
    api._append_startup_trace(data_dir, "desktop_lock_contended")
    deadline = time.monotonic() + max(0.5, float(timeout_s))
    sleep_attempt = 0
    while time.monotonic() < deadline:
        lock_path = api.resolve_instance_lock_path(env)
        lock_payload = api._read_instance_lock_payload(lock_path)
        if not lock_payload:
            return {"action": "retry", "reason": "missing_lock"}
        owner_active = api._process_identity_matches(lock_payload)
        lock_token = str(lock_payload.get("launcherToken") or "")
        if not owner_active:
            reclaim_result = api._reclaim_stale_instance_artifacts(
                data_dir=data_dir,
                stale_state=api.load_session_state(env),
                env=env,
            )
            if bool(reclaim_result.get("blocked")):
                target = str(reclaim_result.get("target") or "")
                reason = str(reclaim_result.get("reason") or "stale_runtime_cleanup_failed")
                api._append_startup_trace(
                    data_dir,
                    "desktop_lock_reclaim_failed",
                    reason=api._truncate_reason(reason),
                    target=target,
                )
                return {
                    "action": "blocked",
                    "reason": reason,
                    "target": target,
                    "reclaim": reclaim_result,
                }
            api._append_startup_trace(
                data_dir,
                "desktop_lock_reclaimed",
                reason="stale_lock_owner",
            )
            return {"action": "reclaimed", "reason": "stale_lock_owner"}
        raw_state = api.load_session_state(env)
        if raw_state:
            session_ok, reason = api.validate_session_state(
                raw_state,
                expected_launcher_token=lock_token,
            )
            if session_ok:
                return {
                    "action": "active",
                    "reason": "healthy_active_session",
                    "session": raw_state,
                }
            api._append_startup_trace(
                data_dir,
                "desktop_session_invalid_reason",
                reason=api._truncate_reason(reason),
            )
            reclaim_result = api._reclaim_stale_instance_artifacts(
                data_dir=data_dir,
                stale_state=raw_state,
                env=env,
            )
            if bool(reclaim_result.get("blocked")):
                target = str(reclaim_result.get("target") or "")
                blocked_reason = str(reclaim_result.get("reason") or "stale_runtime_cleanup_failed")
                api._append_startup_trace(
                    data_dir,
                    "desktop_lock_reclaim_failed",
                    reason=api._truncate_reason(blocked_reason),
                    target=target,
                )
                return {
                    "action": "blocked",
                    "reason": blocked_reason,
                    "target": target,
                    "reclaim": reclaim_result,
                }
            api._append_startup_trace(
                data_dir,
                "desktop_lock_reclaimed",
                reason="invalid_session_state",
            )
            return {"action": "reclaimed", "reason": "invalid_session_state"}
        api._sleep_for_lock_retry(sleep_attempt, deadline)
        sleep_attempt += 1
    api._append_startup_trace(
        data_dir,
        "desktop_lock_reclaim_failed",
        reason="owner_active_no_session",
    )
    return {"action": "active_starting", "reason": "owner_active_no_session"}
