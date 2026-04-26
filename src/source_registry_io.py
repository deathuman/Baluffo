"""JSON IO and path defaults for source registry files."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any

from src.baluffo_config import get_storage_defaults

_STORAGE_DEFAULTS = get_storage_defaults()
_DEFAULT_DATA_DIR = _STORAGE_DEFAULTS["data_dir"]
DATA_DIR = Path(os.getenv("BALUFFO_DATA_DIR") or _DEFAULT_DATA_DIR).expanduser().resolve()
ACTIVE_PATH = DATA_DIR / "source-registry-active.json"
PENDING_PATH = DATA_DIR / "source-registry-pending.json"
REJECTED_PATH = DATA_DIR / "source-registry-rejected.json"
DISCOVERY_REPORT_PATH = DATA_DIR / "source-discovery-report.json"
DISCOVERY_CANDIDATES_PATH = DATA_DIR / "source-discovery-candidates.json"
M5_STRATEGIC_BACKLOG_PATH = DATA_DIR / "m5-strategic-backlog.json"
URL_PATCH_MANIFEST_PATH = DATA_DIR / "url-patch-manifest.json"
APPROVAL_STATE_PATH = DATA_DIR / "source-approval-state.json"
TOMBSTONES_PATH = DATA_DIR / "source-registry-tombstones.json"


def ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def load_json_array(
    path: Path, default: list[dict[str, Any]] | None = None
) -> list[dict[str, Any]]:
    fallback = default or []
    try:
        if not path.exists():
            return [dict(row) for row in fallback]
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            return [dict(row) for row in fallback]
        return [row for row in payload if isinstance(row, dict)]
    except (OSError, json.JSONDecodeError):
        return [dict(row) for row in fallback]


def load_json_object(path: Path, default: dict[str, Any] | None = None) -> dict[str, Any]:
    fallback = dict(default or {})
    try:
        if not path.exists():
            return fallback
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else fallback
    except (OSError, json.JSONDecodeError):
        return fallback


def save_json_atomic(path: Path, payload: Any) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    ensure_data_dir()
    # Use a unique temp file per write to avoid collisions across threads/processes.
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.{time.time_ns()}.tmp")
    try:
        tmp.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        last_error: Exception | None = None
        for attempt in range(18):
            try:
                os.replace(tmp, path)
                last_error = None
                break
            except PermissionError as exc:
                last_error = exc
                # Windows can transiently lock the destination while another thread replaces it.
                time.sleep(0.012 * (attempt + 1))
        if last_error is not None:
            raise last_error
    finally:
        try:
            if tmp.exists():
                tmp.unlink()
        except OSError:
            pass
