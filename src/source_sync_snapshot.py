from __future__ import annotations

import base64
import json
from collections.abc import Callable
from typing import Any

from src.source_registry import (
    REGISTRY_MIGRATION_V2,
    REGISTRY_REASON_PENDING_DEFAULT,
    canonicalize_registry_row,
    ensure_source_id,
    sort_sources_by_identity,
    source_identity,
)
from src.source_sync_runtime import parse_iso


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
                snapshot = normalize_snapshot(module, raw_body)
                module._clear_runtime_state(module.RUNTIME_STATE_REMOTE_CONFLICT)
                return {"exists": True, "sha": str(payload.get("sha") or ""), "snapshot": snapshot}

    if not encoded_content:
        return {"exists": False, "sha": str(payload.get("sha") or ""), "snapshot": None}
    normalized_b64 = encoded_content.replace("\n", "")
    try:
        raw_bytes = base64.b64decode(normalized_b64)
        parsed = json.loads(raw_bytes.decode("utf-8"))
    except (ValueError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Invalid remote sync snapshot payload: {exc}") from exc
    snapshot = normalize_snapshot(module, parsed if isinstance(parsed, dict) else {})
    module._clear_runtime_state(module.RUNTIME_STATE_REMOTE_CONFLICT)
    return {"exists": True, "sha": str(payload.get("sha") or ""), "snapshot": snapshot}


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
        }
    snapshot = _as_dict(remote.get("snapshot"))
    merged_state = merge_registry_state(module, local_state, snapshot)
    changed = json.dumps(merged_state, sort_keys=True, ensure_ascii=False) != json.dumps(
        merge_registry_state(module, local_state, empty_remote),
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
    module: Any,
    config: Any,
    local_state: dict[str, Any],
    *,
    opener: Callable[..., Any],
) -> dict[str, Any]:
    remote = read_remote_snapshot(module, config, opener=opener)
    remote_snapshot = _as_dict(remote.get("snapshot"))
    merged_state = merge_registry_state(module, local_state, remote_snapshot)
    snapshot = build_snapshot(module, merged_state)
    write_result = write_remote_snapshot(
        module,
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
