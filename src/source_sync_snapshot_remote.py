"""Source-sync snapshot remote transport and payload validation.

AI boundary owns: source-sync remote snapshot reads and writes, payload validation, and sharded-read adaptation.
AI boundary implement in: this leaf for remote snapshot IO; merge semantics stay in the source_sync_snapshot coordinator and push orchestration in source_sync_snapshot_push.
AI boundary search before contracts: source sync facade, shard reader, snapshot payload tests, and transient retry tests.
AI boundary verify: `python -m pytest tests/test_source_sync.py tests/test_source_sync_push_churn.py -q`.
"""

from __future__ import annotations

import base64
import json
import logging
import ssl
import time
from collections.abc import Callable
from typing import Any
from urllib.error import URLError

from src.source_sync_shard import read_sharded_snapshot

logger = logging.getLogger(__name__)

from src.source_sync_snapshot import normalize_snapshot

_REMOTE_SNAPSHOT_TOP_LEVEL_KEYS = {
    "schemaVersion",
    "generatedAt",
    "source",
    "active",
    "pending",
    "rejected",
}


def _remote_snapshot_error(detail: str) -> RuntimeError:
    message = f"Invalid remote sync snapshot payload: {detail}"
    logger.error(message)
    return RuntimeError(message)


def _warn_unexpected_remote_snapshot_keys(payload: dict[str, Any]) -> None:
    unexpected_keys = sorted(key for key in payload if key not in _REMOTE_SNAPSHOT_TOP_LEVEL_KEYS)
    if unexpected_keys:
        logger.warning(
            "Remote sync snapshot contains unexpected top-level keys: count=%d",
            len(unexpected_keys),
        )


def _require_remote_snapshot_schema_version(payload: dict[str, Any]) -> None:
    schema_version = payload.get("schemaVersion")
    if (
        not isinstance(schema_version, int)
        or isinstance(schema_version, bool)
        or schema_version < 1
    ):
        raise _remote_snapshot_error("schemaVersion must be an integer >= 1")


def _require_remote_snapshot_generated_at(payload: dict[str, Any]) -> None:
    generated_at = payload.get("generatedAt")
    if not isinstance(generated_at, str) or not generated_at.strip():
        raise _remote_snapshot_error("generatedAt must be a non-empty string")


def _require_remote_snapshot_row_list(payload: dict[str, Any], key: str) -> list[Any]:
    rows = payload.get(key)
    if not isinstance(rows, list):
        raise _remote_snapshot_error(f"{key} must be an array")
    return rows


def _validate_remote_snapshot_rows(bucket: str, rows: list[Any]) -> None:
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            raise _remote_snapshot_error(f"{bucket}[{index}] must be an object")


def _validate_remote_snapshot_payload(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise _remote_snapshot_error("expected a JSON object")
    _warn_unexpected_remote_snapshot_keys(payload)
    _require_remote_snapshot_schema_version(payload)
    _require_remote_snapshot_generated_at(payload)
    _validate_remote_snapshot_rows("active", _require_remote_snapshot_row_list(payload, "active"))
    _validate_remote_snapshot_rows("pending", _require_remote_snapshot_row_list(payload, "pending"))
    rejected_rows = payload.get("rejected")
    if rejected_rows is not None:
        _validate_remote_snapshot_rows(
            "rejected", _require_remote_snapshot_row_list(payload, "rejected")
        )
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


def _remote_snapshot_payload_view(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "schemaVersion": int(payload.get("schemaVersion") or 1),
        "generatedAt": str(payload.get("generatedAt") or ""),
        "source": payload.get("source") if isinstance(payload.get("source"), dict) else {},
        "active": list(payload.get("active") or []),
        "pending": list(payload.get("pending") or []),
        "rejected": list(payload.get("rejected") or []),
    }


def _normalized_remote_snapshot_result(
    module: Any,
    payload: dict[str, Any],
    *,
    sha: str,
    snapshot_format: str,
) -> dict[str, Any]:
    snapshot = normalize_snapshot(module, _validate_remote_snapshot_payload(payload))
    _validate_normalized_remote_snapshot(snapshot)
    module._clear_runtime_state(module.RUNTIME_STATE_REMOTE_CONFLICT)
    return {
        "exists": True,
        "sha": str(sha or ""),
        "snapshot": snapshot,
        "snapshotFormat": snapshot_format,
    }


def _read_sharded_remote_snapshot(
    module: Any,
    config: Any,
    *,
    progress_callback: Callable[..., None] | None = None,
    known_remote_sha: str = "",
    max_shard_read_workers: int | None = None,
    opener: Callable[..., Any],
) -> dict[str, Any] | None:
    sharded_snapshot = read_sharded_snapshot(
        module,
        config,
        progress_callback=progress_callback,
        known_manifest_sha=known_remote_sha,
        max_workers=max_shard_read_workers,
        opener=opener,
    )
    if sharded_snapshot is None:
        return None
    if bool(sharded_snapshot.get("skipped")):
        return {
            "exists": True,
            "sha": str(sharded_snapshot.get("manifestSha") or ""),
            "snapshot": {},
            "snapshotFormat": "sharded-v3",
            "committedManifest": dict(sharded_snapshot.get("manifest") or {}),
            "remoteGeneratedAt": str(sharded_snapshot.get("generatedAt") or ""),
            "skipped": True,
            "skipReason": str(sharded_snapshot.get("skipReason") or ""),
            "shardCount": int(sharded_snapshot.get("shardCount") or 0),
            "shardsReadBytes": int(sharded_snapshot.get("shardsReadBytes") or 0),
            "totalShardBytes": int(sharded_snapshot.get("totalShardBytes") or 0),
            "manifestSizeBytes": int(sharded_snapshot.get("manifestSizeBytes") or 0),
        }
    result = _normalized_remote_snapshot_result(
        module,
        _remote_snapshot_payload_view(sharded_snapshot),
        sha=str(sharded_snapshot.get("manifestSha") or ""),
        snapshot_format="sharded-v3",
    )
    result["committedManifest"] = dict(sharded_snapshot.get("manifest") or {})
    result["shardCount"] = int(sharded_snapshot.get("shardCount") or 0)
    result["shardsReadBytes"] = int(sharded_snapshot.get("shardsReadBytes") or 0)
    result["totalShardBytes"] = int(sharded_snapshot.get("totalShardBytes") or 0)
    result["manifestSizeBytes"] = int(sharded_snapshot.get("manifestSizeBytes") or 0)
    return result


def _read_remote_snapshot_download_url(
    module: Any,
    config: Any,
    payload: dict[str, Any],
    *,
    opener: Callable[..., Any],
) -> dict[str, Any]:
    download_url = str(payload.get("download_url") or "").strip()
    if not download_url:
        return {"exists": False, "sha": str(payload.get("sha") or ""), "snapshot": None}
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
        return _normalized_remote_snapshot_result(
            module,
            raw_body,
            sha=str(payload.get("sha") or ""),
            snapshot_format="monolithic-v2",
        )
    return {"exists": False, "sha": str(payload.get("sha") or ""), "snapshot": None}


def _read_monolithic_remote_snapshot(
    module: Any,
    config: Any,
    *,
    opener: Callable[..., Any],
) -> dict[str, Any]:
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
        return _read_remote_snapshot_download_url(module, config, payload, opener=opener)
    try:
        raw_bytes = base64.b64decode(encoded_content.replace("\n", ""))
        parsed = json.loads(raw_bytes.decode("utf-8"))
    except (ValueError, json.JSONDecodeError) as exc:
        raise _remote_snapshot_error(f"invalid JSON payload: {exc}") from exc
    return _normalized_remote_snapshot_result(
        module,
        parsed,
        sha=str(payload.get("sha") or ""),
        snapshot_format="monolithic-v2",
    )


def read_remote_snapshot(
    module: Any,
    config: Any,
    *,
    opener: Callable[..., Any],
    prefer_sharded: bool = False,
    progress_callback: Callable[..., None] | None = None,
    known_remote_sha: str = "",
    max_shard_read_workers: int | None = None,
) -> dict[str, Any]:
    module.validate_sync_config(config)

    def _read_once() -> dict[str, Any]:
        if prefer_sharded:
            sharded_result = _read_sharded_remote_snapshot(
                module,
                config,
                progress_callback=progress_callback,
                known_remote_sha=known_remote_sha,
                max_shard_read_workers=max_shard_read_workers,
                opener=opener,
            )
            if sharded_result is not None:
                return sharded_result
        return _read_monolithic_remote_snapshot(module, config, opener=opener)

    return _retry_transient_get(_read_once)


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
