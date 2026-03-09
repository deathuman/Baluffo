#!/usr/bin/env python3
"""GitHub-backed source registry sync helpers."""

from __future__ import annotations

import base64
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from scripts.source_registry import ensure_source_id, source_identity

SYNC_SCHEMA_VERSION = 1
DEFAULT_BRANCH = "main"
DEFAULT_PATH = "baluffo/source-sync.json"


@dataclass
class SyncConfig:
    enabled: bool
    token: str
    repo: str
    branch: str
    path: str
    timeout_s: int = 20


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def resolve_sync_config(*, env: Optional[Dict[str, str]] = None) -> SyncConfig:
    env_map = env if isinstance(env, dict) else os.environ
    enabled_token = str(env_map.get("BALUFFO_SYNC_ENABLED") or "").strip().lower()
    # Default-on: sync is enabled unless explicitly disabled.
    enabled = enabled_token not in {"0", "false", "no", "off"}
    token = str(env_map.get("BALUFFO_SYNC_GITHUB_TOKEN") or "").strip()
    repo = str(env_map.get("BALUFFO_SYNC_REPO") or "").strip()
    branch = str(env_map.get("BALUFFO_SYNC_BRANCH") or DEFAULT_BRANCH).strip() or DEFAULT_BRANCH
    path = str(env_map.get("BALUFFO_SYNC_PATH") or DEFAULT_PATH).strip() or DEFAULT_PATH
    return SyncConfig(enabled=enabled, token=token, repo=repo, branch=branch, path=path)


def config_status(config: SyncConfig) -> Dict[str, Any]:
    missing: List[str] = []
    if not config.token:
        missing.append("BALUFFO_SYNC_GITHUB_TOKEN")
    if not config.repo:
        missing.append("BALUFFO_SYNC_REPO")
    state = "disabled"
    ready = False
    if config.enabled:
        if missing:
            state = "misconfigured"
        else:
            state = "ready"
            ready = True
    return {
        "enabled": bool(config.enabled),
        "state": state,
        "ready": ready,
        "repo": config.repo,
        "branch": config.branch,
        "path": config.path,
        "missing": missing,
    }


def _content_api_url(config: SyncConfig, *, with_ref: bool = False) -> str:
    repo_token = quote(config.repo, safe="/")
    path_token = quote(config.path, safe="/")
    base = f"https://api.github.com/repos/{repo_token}/contents/{path_token}"
    if with_ref:
        ref_token = quote(config.branch, safe="")
        return f"{base}?ref={ref_token}"
    return base


def _request_json(
    *,
    method: str,
    url: str,
    token: str,
    timeout_s: int,
    payload: Optional[Dict[str, Any]] = None,
    opener: Callable[..., Any] = urlopen,
) -> Tuple[int, Dict[str, Any], Dict[str, str]]:
    body: Optional[bytes] = None
    if payload is not None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = Request(
        url=url,
        data=body,
        method=method.upper(),
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "User-Agent": "baluffo-source-sync/1.0",
            "X-GitHub-Api-Version": "2022-11-28",
            "Content-Type": "application/json; charset=utf-8",
        },
    )
    try:
        with opener(request, timeout=timeout_s) as response:
            raw = response.read().decode("utf-8")
            parsed = json.loads(raw) if raw else {}
            return int(response.getcode() or 200), parsed if isinstance(parsed, dict) else {}, {
                key.lower(): str(value) for key, value in response.headers.items()
            }
    except HTTPError as exc:
        raw = exc.read().decode("utf-8") if hasattr(exc, "read") else ""
        parsed = {}
        if raw:
            try:
                candidate = json.loads(raw)
                if isinstance(candidate, dict):
                    parsed = candidate
            except json.JSONDecodeError:
                parsed = {}
        return int(exc.code or 500), parsed, {key.lower(): str(value) for key, value in (exc.headers or {}).items()}
    except URLError as exc:
        raise RuntimeError(f"Sync request failed: {exc}") from exc


def _parse_iso(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _pick_row(local_row: Dict[str, Any], remote_row: Dict[str, Any]) -> Dict[str, Any]:
    local_updated = _parse_iso(local_row.get("updatedAt")) or _parse_iso(local_row.get("manualAddedAt"))
    remote_updated = _parse_iso(remote_row.get("updatedAt")) or _parse_iso(remote_row.get("manualAddedAt"))
    if local_updated and remote_updated:
        if remote_updated > local_updated:
            return ensure_source_id(remote_row)
        if local_updated >= remote_updated:
            return ensure_source_id(local_row)
    if remote_updated and not local_updated:
        return ensure_source_id(remote_row)
    return ensure_source_id(local_row)


def _merge_bucket(local_rows: List[Dict[str, Any]], remote_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {}
    order: List[str] = []
    for row in local_rows:
        if not isinstance(row, dict):
            continue
        key = source_identity(row)
        if key not in merged:
            order.append(key)
            merged[key] = ensure_source_id(row)
        else:
            merged[key] = _pick_row(merged[key], row)
    for row in remote_rows:
        if not isinstance(row, dict):
            continue
        key = source_identity(row)
        if key not in merged:
            order.append(key)
            merged[key] = ensure_source_id(row)
        else:
            merged[key] = _pick_row(merged[key], row)
    return [merged[key] for key in order if key in merged]


def normalize_snapshot(payload: Dict[str, Any]) -> Dict[str, Any]:
    data = payload if isinstance(payload, dict) else {}
    return {
        "schemaVersion": int(data.get("schemaVersion") or SYNC_SCHEMA_VERSION),
        "generatedAt": str(data.get("generatedAt") or ""),
        "source": data.get("source") if isinstance(data.get("source"), dict) else {},
        "active": [ensure_source_id(row) for row in (data.get("active") or []) if isinstance(row, dict)],
        "pending": [ensure_source_id(row) for row in (data.get("pending") or []) if isinstance(row, dict)],
        "rejected": [ensure_source_id(row) for row in (data.get("rejected") or []) if isinstance(row, dict)],
    }


def merge_registry_state(local_state: Dict[str, Any], remote_snapshot: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    local_active = [row for row in (local_state.get("active") or []) if isinstance(row, dict)]
    local_pending = [row for row in (local_state.get("pending") or []) if isinstance(row, dict)]
    local_rejected = [row for row in (local_state.get("rejected") or []) if isinstance(row, dict)]

    remote = normalize_snapshot(remote_snapshot)
    merged_by_bucket = {
        "active": _merge_bucket(local_active, remote["active"]),
        "pending": _merge_bucket(local_pending, remote["pending"]),
        "rejected": _merge_bucket(local_rejected, remote["rejected"]),
    }

    # Explicit membership by bucket with precedence active > pending > rejected.
    out: Dict[str, List[Dict[str, Any]]] = {"active": [], "pending": [], "rejected": []}
    seen = set()
    for bucket in ("active", "pending", "rejected"):
        for row in merged_by_bucket[bucket]:
            key = source_identity(row)
            if key in seen:
                continue
            seen.add(key)
            out[bucket].append(ensure_source_id(row))
    return out


def read_remote_snapshot(
    config: SyncConfig,
    *,
    opener: Callable[..., Any] = urlopen,
) -> Dict[str, Any]:
    url = _content_api_url(config, with_ref=True)
    status, payload, _headers = _request_json(
        method="GET",
        url=url,
        token=config.token,
        timeout_s=config.timeout_s,
        opener=opener,
    )
    if status == 404:
        return {"exists": False, "sha": "", "snapshot": None}
    if status >= 400:
        message = str(payload.get("message") or f"GitHub GET failed with HTTP {status}")
        raise RuntimeError(message)
    encoded_content = str(payload.get("content") or "").strip()
    if not encoded_content:
        return {"exists": False, "sha": str(payload.get("sha") or ""), "snapshot": None}
    normalized_b64 = encoded_content.replace("\n", "")
    try:
        raw_bytes = base64.b64decode(normalized_b64)
        parsed = json.loads(raw_bytes.decode("utf-8"))
    except (ValueError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Invalid remote sync snapshot payload: {exc}") from exc
    snapshot = normalize_snapshot(parsed if isinstance(parsed, dict) else {})
    return {"exists": True, "sha": str(payload.get("sha") or ""), "snapshot": snapshot}


def build_snapshot(local_state: Dict[str, Any], *, source_label: str = "admin_bridge") -> Dict[str, Any]:
    return {
        "schemaVersion": SYNC_SCHEMA_VERSION,
        "generatedAt": now_iso(),
        "source": {"name": source_label},
        "active": [ensure_source_id(row) for row in (local_state.get("active") or []) if isinstance(row, dict)],
        "pending": [ensure_source_id(row) for row in (local_state.get("pending") or []) if isinstance(row, dict)],
        "rejected": [ensure_source_id(row) for row in (local_state.get("rejected") or []) if isinstance(row, dict)],
    }


def write_remote_snapshot(
    config: SyncConfig,
    snapshot: Dict[str, Any],
    *,
    sha: str = "",
    message: str = "Update Baluffo source sync snapshot",
    opener: Callable[..., Any] = urlopen,
) -> Dict[str, Any]:
    encoded = base64.b64encode(
        json.dumps(snapshot, ensure_ascii=False, indent=2).encode("utf-8")
    ).decode("ascii")
    payload: Dict[str, Any] = {
        "message": str(message or "Update Baluffo source sync snapshot"),
        "content": encoded,
        "branch": config.branch,
    }
    if sha:
        payload["sha"] = sha

    status, body, _headers = _request_json(
        method="PUT",
        url=_content_api_url(config, with_ref=False),
        token=config.token,
        timeout_s=config.timeout_s,
        payload=payload,
        opener=opener,
    )
    if status >= 400:
        message = str(body.get("message") or f"GitHub PUT failed with HTTP {status}")
        raise RuntimeError(message)
    content = body.get("content") if isinstance(body.get("content"), dict) else {}
    return {"ok": True, "sha": str(content.get("sha") or "")}


def pull_and_merge_sources(
    config: SyncConfig,
    local_state: Dict[str, Any],
    *,
    opener: Callable[..., Any] = urlopen,
) -> Dict[str, Any]:
    remote = read_remote_snapshot(config, opener=opener)
    if not remote.get("exists"):
        return {"changed": False, "remoteFound": False, "mergedState": local_state, "remoteSha": ""}
    snapshot = remote.get("snapshot") if isinstance(remote.get("snapshot"), dict) else {}
    merged_state = merge_registry_state(local_state, snapshot)
    changed = json.dumps(merged_state, sort_keys=True, ensure_ascii=False) != json.dumps(
        {
            "active": [ensure_source_id(row) for row in (local_state.get("active") or []) if isinstance(row, dict)],
            "pending": [ensure_source_id(row) for row in (local_state.get("pending") or []) if isinstance(row, dict)],
            "rejected": [ensure_source_id(row) for row in (local_state.get("rejected") or []) if isinstance(row, dict)],
        },
        sort_keys=True,
        ensure_ascii=False,
    )
    return {
        "changed": changed,
        "remoteFound": True,
        "remoteSha": str(remote.get("sha") or ""),
        "mergedState": merged_state,
        "remoteGeneratedAt": str(snapshot.get("generatedAt") or ""),
    }


def push_sources_snapshot(
    config: SyncConfig,
    local_state: Dict[str, Any],
    *,
    opener: Callable[..., Any] = urlopen,
) -> Dict[str, Any]:
    remote = read_remote_snapshot(config, opener=opener)
    snapshot = build_snapshot(local_state)
    write_result = write_remote_snapshot(
        config,
        snapshot,
        sha=str(remote.get("sha") or ""),
        opener=opener,
    )
    return {
        "pushed": True,
        "remotePreviouslyExisted": bool(remote.get("exists")),
        "remoteSha": str(write_result.get("sha") or ""),
        "snapshot": snapshot,
    }
