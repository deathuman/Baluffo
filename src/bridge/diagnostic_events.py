from __future__ import annotations

import json
from pathlib import Path
from threading import Lock
from typing import Any

SCHEMA_VERSION = 1
DEFAULT_MAX_BYTES = 1_048_576
DEFAULT_MAX_ROWS = 2_000

# `append_bridge_event` runs on every bridge GET, so pruning unconditionally
# meant re-reading and re-parsing the whole event log per request: measured at
# 3.2ms with 1,908 rows and 8.4ms once past the row cap, paid again by every
# concurrent request in a page-load burst while the accept thread competed for
# the GIL. Pruning now runs only when the file is measurably over a limit, or
# once per row window, which is a bounded stat() check per append.
_PRUNE_BYTES_TRIGGER = 900_000
# Half the row cap, so retention stays near the intended 2,000 rows instead of
# overshooting to twice that between prunes.
_PRUNE_ROW_WINDOW = DEFAULT_MAX_ROWS // 2

_prune_lock = Lock()
_appended_since_prune: dict[str, int] = {}

REDACTED_VALUE = "[redacted]"
SENSITIVE_KEY_TOKENS = (
    "token",
    "password",
    "secret",
    "api_key",
    "apikey",
    "authorization",
)


def _is_sensitive_key(key: Any) -> bool:
    normalized = str(key or "").strip().lower()
    return any(token in normalized for token in SENSITIVE_KEY_TOKENS)


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        result: dict[str, Any] = {}
        for key, item in value.items():
            clean_key = str(key)
            result[clean_key] = REDACTED_VALUE if _is_sensitive_key(clean_key) else _json_safe(item)
        return result
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    try:
        json.dumps(value)
    except (TypeError, ValueError):
        return str(value)
    return value


def _clean_fields(fields: dict[str, Any] | None) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in dict(fields or {}).items():
        if value is None or value == "":
            continue
        clean_key = str(key)
        result[clean_key] = REDACTED_VALUE if _is_sensitive_key(clean_key) else _json_safe(value)
    return result


def build_bridge_event(
    level: str,
    message: str,
    fields: dict[str, Any] | None,
    ts: str,
) -> dict[str, Any]:
    event = str(message or "").strip() or "unknown"
    return {
        "schemaVersion": SCHEMA_VERSION,
        "ts": str(ts or ""),
        "level": str(level or "info").strip().lower() or "info",
        "event": event,
        "message": event,
        "fields": _clean_fields(fields),
    }


def _load_rows(path: Path) -> tuple[list[dict[str, Any]], bool]:
    try:
        text = Path(path).read_text(encoding="utf-8")
    except OSError:
        return [], False
    rows: list[dict[str, Any]] = []
    invalid_found = False
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            parsed = json.loads(line)
        except json.JSONDecodeError:
            invalid_found = True
            continue
        if isinstance(parsed, dict):
            rows.append(parsed)
        else:
            invalid_found = True
    return rows, invalid_found


def prune_bridge_events(
    path: Path,
    *,
    max_bytes: int = DEFAULT_MAX_BYTES,
    max_rows: int = DEFAULT_MAX_ROWS,
) -> None:
    event_path = Path(path)
    try:
        if not event_path.exists():
            return
        rows, invalid_found = _load_rows(event_path)
        if event_path.stat().st_size <= max(1, int(max_bytes)) and max_rows > 0:
            if len(rows) <= int(max_rows) and not invalid_found:
                return
        rows = rows[-max(1, int(max_rows)) :]
        event_path.parent.mkdir(parents=True, exist_ok=True)
        with event_path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    except OSError:
        return


def append_bridge_event(path: Path, event: dict[str, Any]) -> None:
    event_path = Path(path)
    try:
        event_path.parent.mkdir(parents=True, exist_ok=True)
        with event_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(_json_safe(dict(event)), ensure_ascii=False) + "\n")
    except OSError:
        return

    # Cheap bounds check first: only pay for the full read+rewrite when the log
    # is over the byte cap, or once per row window (past which the row cap must
    # have been exceeded). Concurrent appends coalesce onto one prune.
    key = str(event_path)
    with _prune_lock:
        pending = _appended_since_prune.get(key, 0) + 1
        _appended_since_prune[key] = pending
    try:
        size = event_path.stat().st_size
    except OSError:
        return
    if size < _PRUNE_BYTES_TRIGGER and pending < _PRUNE_ROW_WINDOW:
        return
    with _prune_lock:
        if _appended_since_prune.get(key, 0) != pending:
            return
        _appended_since_prune[key] = 0
    prune_bridge_events(event_path)


def read_bridge_events(path: Path, limit: int = 200) -> list[dict[str, Any]]:
    rows, _invalid_found = _load_rows(Path(path))
    return rows[-max(1, int(limit or 1)) :]
