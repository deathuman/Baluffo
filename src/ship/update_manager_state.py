from __future__ import annotations

"""State and JSON IO helpers for the ship update manager."""

import contextlib
import json
import os
import time
import uuid
from collections.abc import Iterable
from datetime import UTC, datetime
from functools import cmp_to_key
from pathlib import Path
from typing import Any

from src.baluffo_version import compare_baluffo_versions

from .update_manager_paths import LOG_NAME, STATE_NAME, ShipPaths
from .update_manager_validation import health_check_version


def iso_now() -> str:
    return datetime.now(UTC).isoformat()


def read_json(path: Path, fallback: dict[str, Any] | None = None) -> dict[str, Any]:
    if not path.exists():
        return dict(fallback or {})
    payload = json.loads(path.read_text(encoding="utf-8"))
    return dict(payload) if isinstance(payload, dict) else dict(fallback or {})


def _write_atomic(path: Path, payload: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    tmp.write_text(payload, encoding="utf-8")
    try:
        for attempt in range(6):
            try:
                os.replace(tmp, path)
                return
            except PermissionError:
                if attempt >= 5:
                    raise
                time.sleep(0.03 * (attempt + 1))
    finally:
        if tmp.exists():
            with contextlib.suppress(OSError):
                tmp.unlink()


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    _write_atomic(path, json.dumps(payload, indent=2, ensure_ascii=False))


def write_text_atomic(path: Path, text: str) -> None:
    _write_atomic(path, text)


def _healthy_version_name(paths: ShipPaths, version_name: str) -> str:
    candidate_name = str(version_name or "").strip()
    if not candidate_name:
        return ""
    ok, _ = health_check_version(paths.versions / candidate_name)
    return candidate_name if ok else ""


def _list_healthy_version_names(paths: ShipPaths) -> list[str]:
    names: list[str] = []
    if not paths.versions.is_dir():
        return names
    for child in paths.versions.iterdir():
        if not child.is_dir():
            continue
        ok, _ = health_check_version(child)
        if ok:
            names.append(child.name)
    return names


def _prefer_higher_semver(names: Iterable[str]) -> str:
    bucket = list(names)
    if not bucket:
        return ""
    return sorted(bucket, key=cmp_to_key(compare_baluffo_versions))[-1]


def _recover_current_version(paths: ShipPaths, state: dict[str, Any]) -> str:
    state_current = _healthy_version_name(paths, str(state.get("current_version") or ""))
    if state_current:
        return state_current
    previous = _healthy_version_name(paths, str(state.get("previous_version") or ""))
    if previous:
        return previous
    replacement = _prefer_higher_semver(_list_healthy_version_names(paths))
    if replacement:
        return replacement
    raise RuntimeError(
        "Current pointer is missing or empty and no recoverable healthy version tree was found. "
        f"Pointer path: {paths.current}. Versions root: {paths.versions}."
    )


def ensure_state(paths: ShipPaths) -> dict[str, Any]:
    fallback_state = {
        "current_version": "",
        "previous_version": "",
        "last_update_status": "ready",
        "last_error_code": "",
        "updated_at": iso_now(),
    }
    preferred_state_path = paths.state
    fallback_state_path = paths.data / STATE_NAME
    state_path = preferred_state_path
    state: dict[str, Any] = {}
    try:
        state = read_json(preferred_state_path, fallback_state)
    except OSError:
        state_path = fallback_state_path
        state = read_json(fallback_state_path, fallback_state)
    current_version = (
        paths.current.read_text(encoding="utf-8").strip() if paths.current.exists() else ""
    )
    repaired_pointer = False
    if not current_version:
        current_version = _recover_current_version(paths, state)
        write_text_atomic(paths.current, f"{current_version}\n")
        repaired_pointer = True
    state["current_version"] = current_version
    state.setdefault("previous_version", "")
    state.setdefault("last_update_status", "ready")
    state.setdefault("last_error_code", "")
    state.setdefault("updated_at", iso_now())
    if repaired_pointer:
        previous_version = _healthy_version_name(paths, str(state.get("previous_version") or ""))
        state["previous_version"] = (
            previous_version if previous_version and previous_version != current_version else ""
        )
    try:
        write_json_atomic(state_path, state)
    except OSError:
        state_path = fallback_state_path
        write_json_atomic(state_path, state)
    state["__state_path"] = str(state_path)
    return state


def write_state(paths: ShipPaths, state: dict[str, Any], *, status: str, error: str = "") -> None:
    state_path_raw = str(state.get("__state_path") or "")
    state_path = Path(state_path_raw).expanduser().resolve() if state_path_raw else paths.state
    transient_keys = [key for key in state.keys() if str(key).startswith("__")]
    state["last_update_status"] = status
    state["last_error_code"] = error
    state["updated_at"] = iso_now()
    payload = {key: value for key, value in state.items() if key not in transient_keys}
    try:
        write_json_atomic(state_path, payload)
    except OSError:
        fallback_state_path = paths.data / STATE_NAME
        write_json_atomic(fallback_state_path, payload)
        state["__state_path"] = str(fallback_state_path)


def log_event(paths: ShipPaths, event: str, payload: dict[str, Any]) -> None:
    paths.logs.mkdir(parents=True, exist_ok=True)
    record = {"time": iso_now(), "event": event, **payload}
    with (paths.logs / LOG_NAME).open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")
