"""Sync GET route handlers."""

from __future__ import annotations

from typing import Any

from src.bridge.api import BridgeApi
from src.bridge.performance_profile import time_operation
from src.bridge.routes.response_writer import BridgeResponseWriter
from src.bridge.routes.route_payload_helpers import (
    as_dict as _as_dict,
)
from src.bridge.routes.route_payload_helpers import (
    as_list as _as_list,
)
from src.bridge.routes.route_payload_helpers import (
    clean_text as _clean_text,
)


def _sync_status_summary_payload(payload: dict[str, Any]) -> dict[str, Any]:
    config = _as_dict(payload.get("config"))
    runtime = _as_dict(payload.get("runtime"))
    saved_config = _as_dict(payload.get("savedConfig"))
    saved_enabled = (
        saved_config.get("enabled") if "enabled" in saved_config else config.get("enabled")
    )
    last_push = _as_dict(runtime.get("lastPush")) or _as_dict(runtime.get("push"))
    last_pull = _as_dict(runtime.get("lastPull")) or _as_dict(runtime.get("pull"))
    return {
        "ok": bool(payload.get("ok", True)),
        "appVersion": _clean_text(payload.get("appVersion")),
        "summaryView": True,
        "detailLevel": "summary",
        "config": {
            "enabled": bool(config.get("enabled")),
            "state": _clean_text(config.get("state")),
            "ready": bool(config.get("ready")),
            "repo": _clean_text(config.get("repo")),
            "branch": _clean_text(config.get("branch")),
            "path": _clean_text(config.get("path")),
            "missing": [
                _clean_text(item) for item in _as_list(config.get("missing")) if _clean_text(item)
            ][:20],
            "message": _clean_text(config.get("message")),
            "credentialsPackaged": bool(config.get("credentialsPackaged")),
        },
        "savedConfig": {"enabled": bool(saved_enabled)},
        "runtime": {
            "state": _clean_text(runtime.get("state") or runtime.get("code")),
            "message": _clean_text(runtime.get("message")),
            "lastPullAt": _clean_text(runtime.get("lastPullAt")),
            "lastPushAt": _clean_text(runtime.get("lastPushAt")),
            "lastAction": _clean_text(runtime.get("lastAction")),
            "lastResult": _clean_text(runtime.get("lastResult")),
            "lastError": _clean_text(runtime.get("lastError")),
            "lastPull": {
                "result": _clean_text(last_pull.get("result")),
                "finishedAt": _clean_text(last_pull.get("finishedAt")),
                "error": _clean_text(last_pull.get("error")),
            },
            "lastPush": {
                "result": _clean_text(last_push.get("result")),
                "finishedAt": _clean_text(last_push.get("finishedAt")),
                "error": _clean_text(last_push.get("error")),
            },
        },
    }


def handle_sync_routes(
    handler: BridgeResponseWriter, *, api: BridgeApi, path: str, query: dict[str, list[str]]
) -> bool:
    if path == "/sync/status":
        view = str((query.get("view") or ["full"])[0] or "full").strip().lower()
        if view not in {"", "full", "summary"}:
            handler.send_json(
                {"ok": False, "error": f"unsupported sync status view: {view}"},
                status=400,
            )
            return True
        with time_operation("sync.status.summary" if view == "summary" else "sync.status"):
            runtime_state: dict[str, Any] = {}
            if view == "summary":
                try:
                    runtime_state = _as_dict(api.load_sync_runtime_state())
                except (OSError, TypeError, ValueError):
                    runtime_state = {}
            payload = (
                _sync_status_summary_payload(
                    {
                        "ok": True,
                        "config": api.sync_config_status(),
                        "savedConfig": {},
                        "runtime": runtime_state,
                    }
                )
                if view == "summary"
                else api.get_sync_status_payload()
            )
        handler.send_json(payload)
        return True

    return False
