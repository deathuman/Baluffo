"""Source-sync shard garbage collection: prune remote shard objects no committed
manifest generation references.

A pull reads the manifest first and its shards afterward. A push that commits a
new generation in that window must not delete shards the older manifest still
names, or the pull's shard read 404s. GC therefore retains the shards of the
last few committed manifest generations, read from the branch's manifest commit
history, and skips deletion entirely when that history cannot be established.

Shard discovery reads the committed branch tree rather than the Contents API,
whose 1,000-entry directory cap silently hid the tail of a busy shard
directory. A truncated tree listing is an error, so GC deletes nothing from a
partial view.

AI boundary owns: unreferenced source-sync shard discovery, generation-retention
window, GC path validation, delete-limit enforcement, and delete warnings.
AI boundary implement in: this leaf for GC semantics; remote request plumbing stays in source_sync_shard_remote and manifest trust rules in source_sync_shard_manifest.
AI boundary search before contracts: shard push callers, shard tests, and remote GC warnings.
AI boundary verify: `python -m pytest tests/test_source_sync_sharded_push.py tests/test_source_sync_shard_gc_retention.py -q`.
"""

from __future__ import annotations

import base64
import json
from collections.abc import Callable
from typing import Any
from urllib.parse import quote

from src.source_sync_shard_manifest import manifest_path, validate_manifest
from src.source_sync_shard_model import (
    DEFAULT_BASE_PATH,
    DEFAULT_GC_DELETE_LIMIT,
    DEFAULT_GC_RETAIN_GENERATIONS,
    _normalize_base_path,
)
from src.source_sync_shard_remote import (
    _content_api_url,
    _repo_api_url,
    read_branch_commit,
    read_git_tree,
)

_RETENTION_UNAVAILABLE = "source-sync shard GC retention unavailable"


def prune_unreferenced_shards(
    module: Any,
    config: Any,
    manifest: dict[str, Any],
    *,
    base_path: str = DEFAULT_BASE_PATH,
    delete_limit: int = DEFAULT_GC_DELETE_LIMIT,
    retain_generations: int = DEFAULT_GC_RETAIN_GENERATIONS,
    current_commit_sha: str = "",
    opener: Callable[..., Any],
) -> dict[str, Any]:
    module.validate_sync_config(config)
    trusted = validate_manifest(manifest)
    normalized_base = _normalize_base_path(base_path)
    referenced = _manifest_shard_paths(trusted, normalized_base)
    delete_cap = max(0, int(delete_limit or 0))
    generations = max(0, int(retain_generations or 0))
    if delete_cap <= 0:
        return {
            "ok": True,
            "deletedCount": 0,
            "deleteAttemptCount": 0,
            "skippedCount": 0,
            "deleteLimit": delete_cap,
            "retainGenerations": generations,
            "retainedGenerationCount": 0,
            "retainedShardCount": 0,
            "deletedPaths": [],
            "warnings": [],
        }
    retained, retained_generations, retention_error = _retained_shard_paths(
        module,
        config,
        normalized_base,
        retain_generations=generations,
        current_commit_sha=current_commit_sha,
        opener=opener,
    )
    base_result = {
        "deleteLimit": delete_cap,
        "retainGenerations": generations,
        "retainedGenerationCount": retained_generations,
        "retainedShardCount": len(retained - referenced),
    }
    if retention_error:
        # Deleting without the prior generations is exactly how an in-flight pull
        # loses its shards, so any retention failure leaves every shard in place.
        return {
            "ok": False,
            "deletedCount": 0,
            "deleteAttemptCount": 0,
            "skippedCount": 0,
            **base_result,
            "deletedPaths": [],
            "warnings": [retention_error],
        }
    referenced |= retained
    warnings: list[str] = []
    deleted_paths: list[str] = []
    skipped_paths: list[str] = []
    delete_attempts = 0
    for item in _list_shard_tree(module, config, normalized_base, opener=opener):
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
        **base_result,
        "deletedPaths": deleted_paths,
        "warnings": warnings,
    }


def _manifest_shard_paths(manifest: dict[str, Any], base_path: str) -> set[str]:
    """Return the GC-eligible shard paths named by a validated manifest."""
    paths: set[str] = set()
    for entry in manifest.get("shards", []):
        path = str(entry.get("path") or "")
        if _is_gc_candidate_path(path, base_path):
            paths.add(path)
    return paths


def _manifest_history_commits(
    module: Any,
    config: Any,
    manifest_file: str,
    *,
    retain_generations: int,
    opener: Callable[..., Any],
) -> tuple[list[dict[str, Any]], str]:
    page_size = min(retain_generations + 1, 100)
    url = _repo_api_url(
        module,
        config,
        "commits?path={path}&sha={branch}&per_page={page}".format(
            path=quote(manifest_file, safe="/"),
            branch=quote(str(config.branch or ""), safe=""),
            page=page_size,
        ),
    )
    try:
        status, payload, _headers = module._request_json(
            method="GET",
            url=url,
            config=config,
            timeout_s=config.timeout_s,
            opener=opener,
        )
    except (OSError, RuntimeError, ValueError) as exc:
        return [], f"{_RETENTION_UNAVAILABLE}: {exc}"
    if status == 404:
        # The push just committed the manifest, so an unreadable history is a
        # failure to establish retention, not an empty repository.
        return [], f"{_RETENTION_UNAVAILABLE}: manifest history is not readable"
    if status >= 400:
        body = payload if isinstance(payload, dict) else {}
        message = str(body.get("message") or f"GitHub GET failed with HTTP {status}")
        return [], f"{_RETENTION_UNAVAILABLE}: {message}"
    if not isinstance(payload, list):
        return [], f"{_RETENTION_UNAVAILABLE}: unexpected manifest history payload"
    if not payload:
        return [], f"{_RETENTION_UNAVAILABLE}: manifest history is empty"
    # Validate the whole payload before reading any generation. A malformed row
    # outside the retained window still means the history is not trustworthy;
    # deleting based on a partial view could drop shards an in-flight pull needs.
    commits: list[dict[str, Any]] = []
    for row in payload:
        if not isinstance(row, dict):
            return [], f"{_RETENTION_UNAVAILABLE}: manifest history contains a malformed row"
        if not str(row.get("sha") or "").strip():
            return [], f"{_RETENTION_UNAVAILABLE}: manifest history row is missing a commit sha"
        commits.append(row)
    return commits, ""


def _read_prior_manifest(
    module: Any,
    config: Any,
    manifest_file: str,
    commit_sha: str,
    *,
    opener: Callable[..., Any],
) -> tuple[dict[str, Any] | None, str]:
    status, body, _headers = module._request_json(
        method="GET",
        url=_content_api_url(module, config, manifest_file, with_ref=True, ref=commit_sha),
        config=config,
        timeout_s=config.timeout_s,
        opener=opener,
    )
    if status == 404:
        # A commit returned by the manifest-path history should contain the
        # manifest; a missing generation means retention cannot be proven.
        return None, (
            f"{_RETENTION_UNAVAILABLE}: manifest generation {commit_sha[:12]} is not readable"
        )
    if status >= 400:
        body_dict = body if isinstance(body, dict) else {}
        message = str(body_dict.get("message") or f"GitHub GET failed with HTTP {status}")
        return None, f"{_RETENTION_UNAVAILABLE}: {message}"
    encoded = str(body.get("content") or "").strip() if isinstance(body, dict) else ""
    if not encoded:
        return None, (
            f"{_RETENTION_UNAVAILABLE}: manifest generation {commit_sha[:12]} has no content"
        )
    try:
        parsed = json.loads(base64.b64decode(encoded.replace("\n", "")).decode("utf-8"))
        return validate_manifest(parsed), ""
    except (TypeError, ValueError) as exc:
        return None, (
            f"{_RETENTION_UNAVAILABLE}: manifest generation {commit_sha[:12]} is invalid: {exc}"
        )


def _retained_shard_paths(
    module: Any,
    config: Any,
    base_path: str,
    *,
    retain_generations: int,
    current_commit_sha: str,
    opener: Callable[..., Any],
) -> tuple[set[str], int, str]:
    """Read the shard paths named by the manifest generations before this one.

    The manifest lives at a fixed path and is rewritten by every push, so its
    generation history is the branch's commit history for that path. A failure
    to read any of it is returned as an error rather than an empty retention set.
    """
    if retain_generations <= 0:
        return set(), 0, ""
    manifest_file = manifest_path(config.path)
    commits, error = _manifest_history_commits(
        module,
        config,
        manifest_file,
        retain_generations=retain_generations,
        opener=opener,
    )
    if error:
        return set(), 0, error
    retained: set[str] = set()
    read_count = 0
    current_sha = str(current_commit_sha or "").strip()
    for row in commits:
        if read_count >= retain_generations:
            break
        commit_sha = str(row.get("sha") or "").strip()
        if commit_sha == current_sha:
            continue
        prior, error = _read_prior_manifest(
            module, config, manifest_file, commit_sha, opener=opener
        )
        if error:
            return set(), read_count, error
        if prior is not None:
            retained |= _manifest_shard_paths(prior, base_path)
        read_count += 1
    return retained, read_count, ""


def _list_shard_tree(
    module: Any,
    config: Any,
    base_path: str,
    *,
    opener: Callable[..., Any],
) -> list[dict[str, Any]]:
    """List shard blobs from the committed branch tree.

    The Contents API returns at most 1,000 entries per directory and does not
    paginate, so the tail of a busy shard directory was invisible to GC. The
    recursive Git tree has a much higher cap; if GitHub truncates even that,
    the partial view is rejected rather than used as a deletion set.
    """
    head = read_branch_commit(module, config, opener=opener)
    tree = read_git_tree(
        module, config, str(head.get("treeSha") or ""), recursive=True, opener=opener
    )
    if bool(tree["truncated"]):
        raise RuntimeError(
            "source-sync shard GC listing is truncated; refusing to delete unreferenced shards"
        )
    prefix = f"{base_path}/"
    entries: list[dict[str, Any]] = []
    for entry in tree["entries"]:
        path = str(entry.get("path") or "").replace("\\", "/").strip()
        if not path.startswith(prefix):
            continue
        if str(entry.get("type") or "").lower() not in {"blob", ""}:
            continue
        entries.append({"path": path, "sha": str(entry.get("sha") or "").strip()})
    return entries


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
