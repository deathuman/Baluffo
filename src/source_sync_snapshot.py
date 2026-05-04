from __future__ import annotations

import base64
import hashlib
import json
import logging
import ssl
import time
from collections.abc import Callable, Mapping
from typing import Any
from urllib.error import URLError

from src.source_registry import (
    REGISTRY_MIGRATION_V2,
    REGISTRY_REASON_PENDING_DEFAULT,
    canonicalize_registry_row,
    ensure_source_id,
    sort_sources_by_identity,
    source_identity,
)
from src.source_sync_runtime import parse_iso

logger = logging.getLogger(__name__)

_REMOTE_SNAPSHOT_TOP_LEVEL_KEYS = {
    "schemaVersion",
    "generatedAt",
    "source",
    "active",
    "pending",
    "rejected",
}


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _snapshot_transition_text(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _backfill_snapshot_transition_metadata(
    row: dict[str, Any], *, bucket: str, generated_at: str
) -> dict[str, Any]:
    updated = dict(row)
    bucket_token = str(bucket or "").strip().lower()
    generated_at = str(generated_at or "").strip()
    if bucket_token == "active":
        state_changed_at = _snapshot_transition_text(
            updated.get("stateChangedAt"),
            updated.get("approvedAt"),
            updated.get("liveAt"),
            generated_at,
        )
        state_changed_by = _snapshot_transition_text(
            updated.get("stateChangedBy"),
            updated.get("approvedBy"),
        )
        if state_changed_at and not state_changed_by:
            state_changed_by = REGISTRY_MIGRATION_V2
        updated["stateChangedAt"] = state_changed_at
        updated["stateChangedBy"] = state_changed_by
        updated["pendingReason"] = ""
        updated["lastPromotedAt"] = _snapshot_transition_text(
            updated.get("lastPromotedAt"),
            state_changed_at,
        )
        updated["approvedAt"] = _snapshot_transition_text(
            updated.get("approvedAt"),
            state_changed_at,
        )
        updated["approvedBy"] = _snapshot_transition_text(
            updated.get("approvedBy"),
            state_changed_by,
        )
        updated["liveAt"] = _snapshot_transition_text(updated.get("liveAt"), state_changed_at)
    elif bucket_token == "pending":
        state_changed_at = _snapshot_transition_text(
            updated.get("stateChangedAt"),
            updated.get("lastDemotedAt"),
            updated.get("quarantinedAt"),
            generated_at,
        )
        state_changed_by = _snapshot_transition_text(
            updated.get("stateChangedBy"),
            updated.get("quarantinedBy"),
            updated.get("approvedBy"),
        )
        if state_changed_at and not state_changed_by:
            state_changed_by = REGISTRY_MIGRATION_V2
        updated["stateChangedAt"] = state_changed_at
        updated["stateChangedBy"] = state_changed_by
        updated["pendingReason"] = _snapshot_transition_text(
            updated.get("pendingReason"),
            updated.get("quarantineReason"),
            updated.get("reason"),
            REGISTRY_REASON_PENDING_DEFAULT,
        )
        updated["lastDemotedAt"] = _snapshot_transition_text(
            updated.get("lastDemotedAt"),
            state_changed_at,
        )
    return ensure_source_id(updated)


def _canonicalize_snapshot_rows(
    rows: list[dict[str, Any]], *, bucket: str, generated_at: str = ""
) -> list[dict[str, Any]]:
    canonical = [
        _backfill_snapshot_transition_metadata(
            canonicalize_registry_row(row, bucket=bucket),
            bucket=bucket,
            generated_at=generated_at,
        )
        for row in rows
        if isinstance(row, dict)
    ]
    return sort_sources_by_identity(canonical)


def _row_transition_score(row: dict[str, Any]) -> int:
    timestamps = []
    for key in (
        "stateChangedAt",
        "lastPromotedAt",
        "lastDemotedAt",
        "approvedAt",
        "quarantinedAt",
        "liveAt",
    ):
        dt = parse_iso(row.get(key))
        if dt is not None:
            timestamps.append(int(dt.timestamp()))
    return max(timestamps) if timestamps else 0


def _row_bucket_rank(row: dict[str, Any]) -> int:
    bucket = str(row.get("registryState") or "").strip().lower()
    return {"active": 3, "pending": 2, "rejected": 1}.get(bucket, 0)


def _row_merge_key(row: dict[str, Any]) -> tuple[int, int]:
    return _row_transition_score(row), _row_bucket_rank(row)


def _choose_more_recent_row(
    local_row: dict[str, Any] | None,
    remote_row: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if local_row is None:
        return remote_row
    if remote_row is None:
        return local_row
    local_key = _row_merge_key(local_row)
    remote_key = _row_merge_key(remote_row)
    if remote_key > local_key:
        return remote_row
    return local_row


def _snapshot_content_view(module: Any, snapshot: dict[str, Any]) -> dict[str, Any]:
    normalized = normalize_snapshot(module, snapshot)
    # Fingerprint only the semantic rows that should trigger a remote write.
    return {
        "schemaVersion": int(normalized.get("schemaVersion") or module.SYNC_SCHEMA_VERSION),
        "active": list(normalized.get("active") or []),
        "pending": list(normalized.get("pending") or []),
    }


def _snapshot_content_fingerprint(module: Any, snapshot: dict[str, Any]) -> str:
    view = _snapshot_content_view(module, snapshot)
    encoded = json.dumps(view, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _snapshot_size_bytes(snapshot: dict[str, Any]) -> int:
    encoded = json.dumps(snapshot, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return len(encoded.encode("utf-8"))


def _push_sources_snapshot_after_conflict(
    module: Any,
    config: Any,
    local_state: Mapping[str, Any],
    snapshot: dict[str, Any],
    snapshot_fingerprint: str,
    snapshot_size_bytes: int,
    size_warning: bool,
    max_snapshot_size_bytes: int,
    remote: Mapping[str, Any],
    opener: Callable[..., Any],
    exc: Exception,
) -> dict[str, Any]:
    module.record_sync_counters(conflictsDetected=1)
    module._clear_runtime_state(module.RUNTIME_STATE_REMOTE_CONFLICT)
    refreshed_remote = read_remote_snapshot(module, config, opener=opener)
    refreshed_snapshot = _as_dict(refreshed_remote.get("snapshot"))
    refreshed_sha = str(refreshed_remote.get("sha") or "")
    refreshed_fingerprint = _snapshot_content_fingerprint(module, refreshed_snapshot)
    if refreshed_fingerprint == snapshot_fingerprint:
        counters = module.record_sync_counters(conflictsResolved=1)
        return {
            "pushed": True,
            "remotePreviouslyExisted": bool(remote.get("exists")),
            "remoteSha": refreshed_sha,
            "snapshot": snapshot,
            "skipped": False,
            "sizeBytes": snapshot_size_bytes,
            "sizeWarning": size_warning,
            "maxSnapshotSizeBytes": max_snapshot_size_bytes,
            "counters": counters,
        }
    retry_state = merge_registry_state(module, local_state, refreshed_snapshot)
    retry_snapshot = build_snapshot(module, retry_state)
    retry_snapshot_size_bytes = _snapshot_size_bytes(retry_snapshot)
    retry_max_snapshot_size_bytes = int(
        getattr(config, "max_snapshot_size_bytes", module.DEFAULT_MAX_SNAPSHOT_SIZE_BYTES)
        or module.DEFAULT_MAX_SNAPSHOT_SIZE_BYTES
    )
    retry_size_warning = retry_snapshot_size_bytes > module.SNAPSHOT_SIZE_WARN_BYTES
    if retry_snapshot_size_bytes > retry_max_snapshot_size_bytes:
        raise module.SyncOperationError(
            "snapshot_too_large",
            (
                f"Snapshot size {retry_snapshot_size_bytes} bytes exceeds configured limit "
                f"{retry_max_snapshot_size_bytes} bytes"
            ),
        ) from exc
    write_result = write_remote_snapshot(
        module,
        config,
        retry_snapshot,
        sha=refreshed_sha,
        opener=opener,
    )
    counters = module.record_sync_counters(conflictsResolved=1)
    return {
        "pushed": True,
        "remotePreviouslyExisted": bool(remote.get("exists")),
        "remoteSha": str(write_result.get("sha") or refreshed_sha),
        "snapshot": retry_snapshot,
        "skipped": False,
        "sizeBytes": retry_snapshot_size_bytes,
        "sizeWarning": retry_size_warning,
        "maxSnapshotSizeBytes": retry_max_snapshot_size_bytes,
        "counters": counters,
    }


def _push_sources_snapshot_after_transient(
    module: Any,
    config: Any,
    local_state: Mapping[str, Any],
    snapshot: dict[str, Any],
    snapshot_fingerprint: str,
    snapshot_size_bytes: int,
    size_warning: bool,
    max_snapshot_size_bytes: int,
    remote: Mapping[str, Any],
    remote_sha: str,
    opener: Callable[..., Any],
    exc: Exception,
) -> dict[str, Any]:
    refreshed_remote = read_remote_snapshot(module, config, opener=opener)
    refreshed_snapshot = _as_dict(refreshed_remote.get("snapshot"))
    refreshed_sha = str(refreshed_remote.get("sha") or "")
    refreshed_fingerprint = _snapshot_content_fingerprint(module, refreshed_snapshot)
    if refreshed_sha == remote_sha:
        write_result = write_remote_snapshot(
            module,
            config,
            snapshot,
            sha=refreshed_sha,
            opener=opener,
        )
        return {
            "pushed": True,
            "remotePreviouslyExisted": bool(remote.get("exists")),
            "remoteSha": str(write_result.get("sha") or refreshed_sha),
            "snapshot": snapshot,
            "skipped": False,
            "sizeBytes": snapshot_size_bytes,
            "sizeWarning": size_warning,
            "maxSnapshotSizeBytes": max_snapshot_size_bytes,
            "counters": module.sync_counters_payload(),
        }
    if refreshed_fingerprint == snapshot_fingerprint:
        return {
            "pushed": True,
            "remotePreviouslyExisted": bool(remote.get("exists")),
            "remoteSha": refreshed_sha,
            "snapshot": snapshot,
            "skipped": False,
            "sizeBytes": snapshot_size_bytes,
            "sizeWarning": size_warning,
            "maxSnapshotSizeBytes": max_snapshot_size_bytes,
            "counters": module.sync_counters_payload(),
        }
    retry_state = merge_registry_state(module, local_state, refreshed_snapshot)
    retry_snapshot = build_snapshot(module, retry_state)
    retry_snapshot_size_bytes = _snapshot_size_bytes(retry_snapshot)
    retry_max_snapshot_size_bytes = int(
        getattr(config, "max_snapshot_size_bytes", module.DEFAULT_MAX_SNAPSHOT_SIZE_BYTES)
        or module.DEFAULT_MAX_SNAPSHOT_SIZE_BYTES
    )
    retry_size_warning = retry_snapshot_size_bytes > module.SNAPSHOT_SIZE_WARN_BYTES
    if retry_snapshot_size_bytes > retry_max_snapshot_size_bytes:
        raise module.SyncOperationError(
            "snapshot_too_large",
            (
                f"Snapshot size {retry_snapshot_size_bytes} bytes exceeds configured limit "
                f"{retry_max_snapshot_size_bytes} bytes"
            ),
        ) from exc
    write_result = write_remote_snapshot(
        module,
        config,
        retry_snapshot,
        sha=refreshed_sha,
        opener=opener,
    )
    return {
        "pushed": True,
        "remotePreviouslyExisted": bool(remote.get("exists")),
        "remoteSha": str(write_result.get("sha") or refreshed_sha),
        "snapshot": retry_snapshot,
        "skipped": False,
        "sizeBytes": retry_snapshot_size_bytes,
        "sizeWarning": retry_size_warning,
        "maxSnapshotSizeBytes": retry_max_snapshot_size_bytes,
        "counters": module.sync_counters_payload(),
    }


def _is_transient_request_error(exc: BaseException) -> bool:
    return isinstance(getattr(exc, "__cause__", None), (URLError, ssl.SSLError))


def _retry_transient_get(
    request: Callable[[], dict[str, Any]], *, attempts: int = 3, base_backoff_s: float = 1.0
) -> dict[str, Any]:
    last_exc: RuntimeError | None = None
    for attempt in range(max(1, int(attempts))):
        try:
            return request()
        except RuntimeError as exc:
            if not _is_transient_request_error(exc):
                raise
            last_exc = exc
            if attempt >= max(0, int(attempts) - 1):
                raise
            delay_s = min(base_backoff_s * (2**attempt), 5.0)
            time.sleep(delay_s)
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("GET retry failed unexpectedly")


def _assert_unique_snapshot_identity(
    module: Any, rows: list[dict[str, Any]], *, scope: str
) -> None:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        row_id = source_identity(row)
        if not row_id:
            continue
        if row_id in seen:
            duplicates.add(row_id)
        else:
            seen.add(row_id)
    if duplicates:
        joined = ", ".join(sorted(duplicates))
        raise module.SyncOperationError(
            "duplicate_source_identity",
            f"Duplicate canonical source identity in {scope}: {joined}",
        )


def _remote_snapshot_error(detail: str) -> RuntimeError:
    message = f"Invalid remote sync snapshot payload: {detail}"
    logger.error(message)
    return RuntimeError(message)


def _validate_remote_snapshot_payload(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise _remote_snapshot_error("expected a JSON object")
    unexpected_keys = sorted(key for key in payload if key not in _REMOTE_SNAPSHOT_TOP_LEVEL_KEYS)
    if unexpected_keys:
        logger.warning(
            "Remote sync snapshot contains unexpected top-level keys: %s",
            ", ".join(unexpected_keys),
        )
    schema_version = payload.get("schemaVersion")
    if (
        not isinstance(schema_version, int)
        or isinstance(schema_version, bool)
        or schema_version < 1
    ):
        raise _remote_snapshot_error("schemaVersion must be an integer >= 1")
    generated_at = payload.get("generatedAt")
    if not isinstance(generated_at, str) or not generated_at.strip():
        raise _remote_snapshot_error("generatedAt must be a non-empty string")
    active_rows = payload.get("active")
    if not isinstance(active_rows, list):
        raise _remote_snapshot_error("active must be an array")
    pending_rows = payload.get("pending")
    if not isinstance(pending_rows, list):
        raise _remote_snapshot_error("pending must be an array")
    rejected_rows = payload.get("rejected")
    if rejected_rows is not None and not isinstance(rejected_rows, list):
        raise _remote_snapshot_error("rejected must be an array when present")
    for bucket, rows in (("active", active_rows), ("pending", pending_rows)):
        for index, row in enumerate(rows):
            if not isinstance(row, dict):
                raise _remote_snapshot_error(f"{bucket}[{index}] must be an object")
    if isinstance(rejected_rows, list):
        for index, row in enumerate(rejected_rows):
            if not isinstance(row, dict):
                raise _remote_snapshot_error(f"rejected[{index}] must be an object")
    return payload


def _validate_normalized_remote_snapshot(snapshot: dict[str, Any]) -> None:
    for bucket in ("active", "pending"):
        rows = snapshot.get(bucket)
        if not isinstance(rows, list):
            raise _remote_snapshot_error(f"{bucket} must remain an array after normalization")
        for index, row in enumerate(rows):
            if not isinstance(row, dict):
                raise _remote_snapshot_error(
                    f"{bucket}[{index}] must remain an object after normalization"
                )
            if not str(row.get("id") or "").strip():
                raise _remote_snapshot_error(
                    f"{bucket}[{index}] missing source identity after normalization"
                )


def normalize_snapshot(module: Any, payload: dict[str, Any]) -> dict[str, Any]:
    data = payload if isinstance(payload, dict) else {}
    generated_at = str(data.get("generatedAt") or "")
    return {
        "schemaVersion": int(data.get("schemaVersion") or 1),
        "generatedAt": generated_at,
        "source": data.get("source") if isinstance(data.get("source"), dict) else {},
        "active": _canonicalize_snapshot_rows(
            list(data.get("active") or []), bucket="active", generated_at=generated_at
        ),
        "pending": _canonicalize_snapshot_rows(
            list(data.get("pending") or []), bucket="pending", generated_at=generated_at
        ),
        "rejected": _canonicalize_snapshot_rows(
            list(data.get("rejected") or []), bucket="rejected", generated_at=generated_at
        ),
    }


def merge_registry_state(
    module: Any, local_state: dict[str, Any], remote_snapshot: dict[str, Any]
) -> dict[str, list[dict[str, Any]]]:
    remote = normalize_snapshot(module, remote_snapshot)
    tombstones = module.load_tombstones()
    generated_at = str(remote.get("generatedAt") or "")
    local = {
        "active": module.filter_tombstoned_rows(
            _canonicalize_snapshot_rows(
                list(local_state.get("active") or []), bucket="active", generated_at=generated_at
            ),
            tombstones,
        ),
        "pending": module.filter_tombstoned_rows(
            _canonicalize_snapshot_rows(
                list(local_state.get("pending") or []), bucket="pending", generated_at=generated_at
            ),
            tombstones,
        ),
        "rejected": module.filter_tombstoned_rows(
            _canonicalize_snapshot_rows(
                list(local_state.get("rejected") or []),
                bucket="rejected",
                generated_at=generated_at,
            ),
            tombstones,
        ),
    }
    local_rejected_ids = {
        source_identity(row) for row in local["rejected"] if isinstance(row, dict)
    }
    merged: dict[str, list[dict[str, Any]]] = {
        "active": [],
        "pending": [],
        "rejected": sort_sources_by_identity(local["rejected"]),
    }
    candidates: dict[str, dict[str, Any]] = {}
    for bucket in ("active", "pending"):
        for row in local[bucket]:
            candidates[source_identity(row)] = dict(row)
    for bucket in ("active", "pending"):
        for row in remote[bucket]:
            row_id = source_identity(row)
            if row_id in local_rejected_ids:
                continue
            candidates[row_id] = dict(_choose_more_recent_row(candidates.get(row_id), row) or row)
    for row in candidates.values():
        bucket = str(row.get("registryState") or "").strip().lower()
        if bucket == "active":
            merged["active"].append(ensure_source_id(row))
        elif bucket == "pending":
            merged["pending"].append(ensure_source_id(row))
    merged["active"] = sort_sources_by_identity(merged["active"])
    merged["pending"] = sort_sources_by_identity(merged["pending"])
    return merged


def read_remote_snapshot(
    module: Any,
    config: Any,
    *,
    opener: Callable[..., Any],
) -> dict[str, Any]:
    module.validate_sync_config(config)

    def _read_once() -> dict[str, Any]:
        url = module._content_api_url(config, with_ref=True)
        status, payload, _headers = module._request_json(
            method="GET",
            url=url,
            config=config,
            timeout_s=config.timeout_s,
            opener=opener,
        )
        if status == 404:
            module._clear_runtime_state(module.RUNTIME_STATE_REMOTE_CONFLICT)
            return {"exists": False, "sha": "", "snapshot": None}
        if status >= 400:
            message = str(payload.get("message") or f"GitHub GET failed with HTTP {status}")
            raise RuntimeError(message)
        encoded_content = str(payload.get("content") or "").strip()
        if not encoded_content:
            download_url = str(payload.get("download_url") or "").strip()
            if download_url:
                raw_status, raw_body, _raw_headers = module._request_raw_json(
                    method="GET",
                    url=download_url,
                    headers=module._github_json_headers(
                        f"Bearer {module._get_auth_manager(config).get_installation_token(opener=opener)}"
                    ),
                    timeout_s=config.timeout_s,
                    opener=opener,
                )
                if raw_status == 200 and isinstance(raw_body, dict):
                    snapshot = normalize_snapshot(
                        module, _validate_remote_snapshot_payload(raw_body)
                    )
                    _validate_normalized_remote_snapshot(snapshot)
                    module._clear_runtime_state(module.RUNTIME_STATE_REMOTE_CONFLICT)
                    return {
                        "exists": True,
                        "sha": str(payload.get("sha") or ""),
                        "snapshot": snapshot,
                    }

        if not encoded_content:
            return {"exists": False, "sha": str(payload.get("sha") or ""), "snapshot": None}
        normalized_b64 = encoded_content.replace("\n", "")
        try:
            raw_bytes = base64.b64decode(normalized_b64)
            parsed = json.loads(raw_bytes.decode("utf-8"))
        except (ValueError, json.JSONDecodeError) as exc:
            raise _remote_snapshot_error(f"invalid JSON payload: {exc}") from exc
        snapshot = normalize_snapshot(module, _validate_remote_snapshot_payload(parsed))
        _validate_normalized_remote_snapshot(snapshot)
        module._clear_runtime_state(module.RUNTIME_STATE_REMOTE_CONFLICT)
        return {"exists": True, "sha": str(payload.get("sha") or ""), "snapshot": snapshot}

    return _retry_transient_get(_read_once)


def build_snapshot(
    module: Any, local_state: dict[str, Any], *, source_label: str = "admin_bridge"
) -> dict[str, Any]:
    generated_at = module.now_iso()
    canonical_state = merge_registry_state(
        module,
        local_state,
        {
            "schemaVersion": module.SYNC_SCHEMA_VERSION,
            "generatedAt": generated_at,
            "source": {"name": source_label},
            "active": [],
            "pending": [],
            "rejected": [],
        },
    )
    canonical_state = {
        "active": _canonicalize_snapshot_rows(
            list(canonical_state.get("active") or []), bucket="active", generated_at=generated_at
        ),
        "pending": _canonicalize_snapshot_rows(
            list(canonical_state.get("pending") or []), bucket="pending", generated_at=generated_at
        ),
    }
    return {
        "schemaVersion": module.SYNC_SCHEMA_VERSION,
        "generatedAt": generated_at,
        "source": {"name": source_label},
        "active": canonical_state["active"],
        "pending": canonical_state["pending"],
    }


def write_remote_snapshot(
    module: Any,
    config: Any,
    snapshot: dict[str, Any],
    *,
    sha: str = "",
    message: str = "Update Baluffo source sync snapshot",
    opener: Callable[..., Any],
) -> dict[str, Any]:
    module.validate_sync_config(config)
    encoded = base64.b64encode(
        json.dumps(snapshot, ensure_ascii=False, indent=2).encode("utf-8")
    ).decode("ascii")
    payload: dict[str, Any] = {
        "message": str(message or "Update Baluffo source sync snapshot"),
        "content": encoded,
        "branch": config.branch,
    }
    if sha:
        payload["sha"] = sha
    status, body, _headers = module._request_json(
        method="PUT",
        url=module._content_api_url(config, with_ref=False),
        config=config,
        timeout_s=config.timeout_s,
        payload=payload,
        opener=opener,
    )
    if status >= 400:
        msg = str(body.get("message") or f"GitHub PUT failed with HTTP {status}")
        if int(status or 0) == 409:
            module._set_runtime_state(module.RUNTIME_STATE_REMOTE_CONFLICT, msg)
            raise module.SyncOperationError(module.RUNTIME_STATE_REMOTE_CONFLICT, msg)
        raise RuntimeError(msg)
    content = body.get("content") if isinstance(body.get("content"), dict) else {}
    module._clear_runtime_state(module.RUNTIME_STATE_REMOTE_CONFLICT)
    return {"ok": True, "sha": str(content.get("sha") or "")}


def pull_and_merge_sources(
    module: Any,
    config: Any,
    local_state: dict[str, Any],
    *,
    opener: Callable[..., Any],
) -> dict[str, Any]:
    module.record_sync_counters(totalPulls=1)
    remote = read_remote_snapshot(module, config, opener=opener)
    empty_remote = {
        "schemaVersion": module.SYNC_SCHEMA_VERSION,
        "generatedAt": "",
        "source": {},
        "active": [],
        "pending": [],
        "rejected": [],
    }
    if not remote.get("exists"):
        canonical_local = merge_registry_state(module, local_state, empty_remote)
        return {
            "changed": False,
            "remoteFound": False,
            "mergedState": canonical_local,
            "remoteSha": "",
            "counters": module.sync_counters_payload(),
        }
    snapshot = _as_dict(remote.get("snapshot"))
    merged_state = merge_registry_state(module, local_state, snapshot)
    changed = json.dumps(merged_state, sort_keys=True, ensure_ascii=False) != json.dumps(
        merge_registry_state(module, local_state, empty_remote),
        sort_keys=True,
        ensure_ascii=False,
    )
    local_count = len(list(_as_dict(local_state).get("active") or [])) + len(
        list(_as_dict(local_state).get("pending") or [])
    )
    merged_count = len(list(merged_state.get("active") or [])) + len(
        list(merged_state.get("pending") or [])
    )
    if merged_count > local_count:
        module.record_sync_counters(sourcesAdded=merged_count - local_count)
    elif merged_count < local_count:
        module.record_sync_counters(sourcesRemoved=local_count - merged_count)
    return {
        "changed": changed,
        "remoteFound": True,
        "remoteSha": str(remote.get("sha") or ""),
        "mergedState": merged_state,
        "remoteGeneratedAt": str(snapshot.get("generatedAt") or ""),
        "counters": module.sync_counters_payload(),
    }


def push_sources_snapshot(
    module: Any,
    config: Any,
    local_state: dict[str, Any],
    *,
    dry_run: bool = False,
    opener: Callable[..., Any],
) -> dict[str, Any]:
    remote = read_remote_snapshot(module, config, opener=opener)
    remote_snapshot = _as_dict(remote.get("snapshot"))
    remote_sha = str(remote.get("sha") or "")
    _assert_unique_snapshot_identity(
        module,
        list(_as_dict(local_state).get("active") or [])
        + list(_as_dict(local_state).get("pending") or []),
        scope="local active/pending snapshot",
    )
    _assert_unique_snapshot_identity(
        module,
        list(remote_snapshot.get("active") or []) + list(remote_snapshot.get("pending") or []),
        scope="remote active/pending snapshot",
    )
    merged_state = merge_registry_state(module, local_state, remote_snapshot)
    snapshot = build_snapshot(module, merged_state)
    snapshot_fingerprint = _snapshot_content_fingerprint(module, snapshot)
    remote_fingerprint = _snapshot_content_fingerprint(module, remote_snapshot)
    remote_exists = bool(remote.get("exists"))
    snapshot_size_bytes = _snapshot_size_bytes(snapshot)
    max_snapshot_size_bytes = int(
        getattr(config, "max_snapshot_size_bytes", module.DEFAULT_MAX_SNAPSHOT_SIZE_BYTES)
        or module.DEFAULT_MAX_SNAPSHOT_SIZE_BYTES
    )
    size_warning = snapshot_size_bytes > module.SNAPSHOT_SIZE_WARN_BYTES
    would_change = not remote_exists or snapshot_fingerprint != remote_fingerprint
    if dry_run:
        return {
            "pushed": False,
            "remotePreviouslyExisted": remote_exists,
            "remoteSha": remote_sha,
            "snapshot": snapshot,
            "skipped": True,
            "skipReason": "dryRun",
            "dryRun": True,
            "wouldChange": would_change,
            "sizeBytes": snapshot_size_bytes,
            "sizeWarning": size_warning,
            "maxSnapshotSizeBytes": max_snapshot_size_bytes,
            "counters": module.sync_counters_payload(),
        }
    module.record_sync_counters(totalPushes=1)
    if remote_exists and snapshot_fingerprint == remote_fingerprint:
        counters = module.record_sync_counters(noOpSkips=1)
        return {
            "pushed": False,
            "remotePreviouslyExisted": True,
            "remoteSha": remote_sha,
            "snapshot": snapshot,
            "skipped": True,
            "skipReason": "no_meaningful_change",
            "sizeBytes": snapshot_size_bytes,
            "sizeWarning": size_warning,
            "maxSnapshotSizeBytes": max_snapshot_size_bytes,
            "counters": counters,
        }
    if snapshot_size_bytes > max_snapshot_size_bytes:
        raise module.SyncOperationError(
            "snapshot_too_large",
            (
                f"Snapshot size {snapshot_size_bytes} bytes exceeds configured limit "
                f"{max_snapshot_size_bytes} bytes"
            ),
        )
    try:
        write_result = write_remote_snapshot(
            module,
            config,
            snapshot,
            sha=remote_sha,
            opener=opener,
        )
    except module.SyncOperationError as exc:
        if exc.code != module.RUNTIME_STATE_REMOTE_CONFLICT:
            raise
        return _push_sources_snapshot_after_conflict(
            module,
            config,
            local_state,
            snapshot,
            snapshot_fingerprint,
            snapshot_size_bytes,
            size_warning,
            max_snapshot_size_bytes,
            remote,
            opener,
            exc,
        )
    except RuntimeError as exc:
        if not _is_transient_request_error(exc):
            raise
        return _push_sources_snapshot_after_transient(
            module,
            config,
            local_state,
            snapshot,
            snapshot_fingerprint,
            snapshot_size_bytes,
            size_warning,
            max_snapshot_size_bytes,
            remote,
            remote_sha,
            opener,
            exc,
        )
    return {
        "pushed": True,
        "remotePreviouslyExisted": bool(remote.get("exists")),
        "remoteSha": str(write_result.get("sha") or ""),
        "snapshot": snapshot,
        "skipped": False,
        "sizeBytes": snapshot_size_bytes,
        "sizeWarning": size_warning,
        "maxSnapshotSizeBytes": max_snapshot_size_bytes,
        "counters": module.sync_counters_payload(),
    }
