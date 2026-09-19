"""Source-sync shard garbage collection: prune remote shard objects no committed manifest references.

AI boundary owns: unreferenced source-sync shard discovery, GC path validation, delete-limit enforcement, and delete warnings.
AI boundary implement in: this leaf for GC semantics; remote request plumbing stays in source_sync_shard_remote and manifest trust rules in source_sync_shard_manifest.
AI boundary search before contracts: shard push callers, shard tests, and remote GC warnings.
AI boundary verify: `python -m pytest tests/test_source_sync_sharded_push.py -q`.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from src.source_sync_shard_manifest import validate_manifest
from src.source_sync_shard_model import (
    DEFAULT_BASE_PATH,
    DEFAULT_GC_DELETE_LIMIT,
    _normalize_base_path,
)
from src.source_sync_shard_remote import _content_api_url


def prune_unreferenced_shards(
    module: Any,
    config: Any,
    manifest: dict[str, Any],
    *,
    base_path: str = DEFAULT_BASE_PATH,
    delete_limit: int = DEFAULT_GC_DELETE_LIMIT,
    opener: Callable[..., Any],
) -> dict[str, Any]:
    module.validate_sync_config(config)
    trusted = validate_manifest(manifest)
    normalized_base = _normalize_base_path(base_path)
    referenced = {
        entry["path"]
        for entry in trusted.get("shards", [])
        if _is_gc_candidate_path(str(entry.get("path") or ""), normalized_base)
    }
    warnings: list[str] = []
    deleted_paths: list[str] = []
    skipped_paths: list[str] = []
    delete_attempts = 0
    delete_cap = max(0, int(delete_limit or 0))
    if delete_cap <= 0:
        return {
            "ok": True,
            "deletedCount": 0,
            "deleteAttemptCount": 0,
            "skippedCount": 0,
            "deleteLimit": delete_cap,
            "deletedPaths": [],
            "warnings": [],
        }
    for item in _list_content_tree(module, config, normalized_base, opener=opener):
        path = str(item.get("path") or "").replace("\\", "/").strip()
        if not _is_gc_candidate_path(path, normalized_base):
            skipped_paths.append(path)
            warnings.append(f"skipped invalid source-sync shard GC path: {path}")
            continue
        if path in referenced:
            skipped_paths.append(path)
            continue
        sha = str(item.get("sha") or "").strip()
        if not sha:
            skipped_paths.append(path)
            warnings.append(f"skipped source-sync shard without remote sha: {path}")
            continue
        if delete_attempts >= delete_cap:
            skipped_paths.append(path)
            continue
        delete_attempts += 1
        warning = _delete_shard_object(module, config, path, sha, opener=opener)
        if warning:
            warnings.append(warning)
            skipped_paths.append(path)
            continue
        deleted_paths.append(path)
    return {
        "ok": not warnings,
        "deletedCount": len(deleted_paths),
        "deleteAttemptCount": delete_attempts,
        "skippedCount": len(skipped_paths),
        "deleteLimit": delete_cap,
        "deletedPaths": deleted_paths,
        "warnings": warnings,
    }


def _list_content_tree(
    module: Any,
    config: Any,
    path: str,
    *,
    opener: Callable[..., Any],
) -> list[dict[str, Any]]:
    status, payload, _headers = module._request_json(
        method="GET",
        url=_content_api_url(module, config, path, with_ref=True),
        config=config,
        timeout_s=config.timeout_s,
        opener=opener,
    )
    if status == 404:
        return []
    if status >= 400:
        message = str(payload.get("message") or f"GitHub GET failed with HTTP {status}")
        raise RuntimeError(message)
    items = payload if isinstance(payload, list) else [payload] if isinstance(payload, dict) else []
    files: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        item_type = str(item.get("type") or "").strip().lower()
        item_path = str(item.get("path") or "").replace("\\", "/").strip()
        if item_type == "dir" and item_path:
            files.extend(_list_content_tree(module, config, item_path, opener=opener))
        elif item_type in {"file", ""} and item_path:
            files.append(dict(item))
    return files


def _delete_shard_object(
    module: Any,
    config: Any,
    path: str,
    sha: str,
    *,
    opener: Callable[..., Any],
) -> str:
    status, payload, _headers = module._request_json(
        method="DELETE",
        url=_content_api_url(module, config, path, with_ref=False),
        config=config,
        timeout_s=config.timeout_s,
        payload={
            "message": f"Prune Baluffo source sync shard {path}",
            "sha": sha,
            "branch": config.branch,
        },
        opener=opener,
    )
    if status in {200, 202, 204, 404}:
        return ""
    return str(payload.get("message") or f"GitHub DELETE failed with HTTP {status}")


def _is_gc_candidate_path(path: str, base_path: str) -> bool:
    normalized = str(path or "").replace("\\", "/").strip()
    if (
        not normalized
        or normalized != normalized.strip("/")
        or "//" in normalized
        or not normalized.endswith(".json.gz")
    ):
        return False
    try:
        _normalize_base_path(normalized)
    except ValueError:
        return False
    return normalized.startswith(f"{base_path}/")
