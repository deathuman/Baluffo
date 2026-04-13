#!/usr/bin/env python3
"""GitHub App-backed source registry sync helpers."""

from __future__ import annotations

import base64
import ctypes
import json
import os
import platform
import ssl
import sys
import threading
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.error import HTTPError as _HTTPError
from urllib.error import URLError as _URLError
from urllib.parse import quote
from urllib.request import Request as _Request
from urllib.request import urlopen

import src.source_sync_config as _source_sync_config
import src.source_sync_crypto as _source_sync_crypto
import src.source_sync_snapshot as _source_sync_snapshot
from src.baluffo_config import get_security_defaults, get_sync_defaults
from src.bridge.registry_tombstones import (
    filter_tombstoned_rows as _filter_tombstoned_rows,
)
from src.bridge.registry_tombstones import load_tombstones as _load_tombstones
from src.shared.utils import now_iso, now_utc
from src.source_registry import (
    REGISTRY_MIGRATION_V2,
    REGISTRY_REASON_PENDING_DEFAULT,
    canonicalize_registry_row,
    ensure_source_id,
    sort_sources_by_identity,
)
from src.source_registry import (
    source_identity as _source_identity,
)

try:
    import certifi
except ImportError:  # pragma: no cover - optional dependency at runtime
    certifi = None  # type: ignore[assignment]

ROOT = Path(__file__).resolve().parents[1]
_SYNC_DEFAULTS = get_sync_defaults()
_SECURITY_DEFAULTS = get_security_defaults()
HTTPError = _HTTPError
URLError = _URLError
Request = _Request
load_tombstones = _load_tombstones
filter_tombstoned_rows = _filter_tombstoned_rows
source_identity = _source_identity
SYNC_SCHEMA_VERSION = 2
DEFAULT_BRANCH = str(_SYNC_DEFAULTS["default_branch"])
DEFAULT_PATH = str(_SYNC_DEFAULTS["default_path"])
DEFAULT_TIMEOUT_S = 20
PACKAGED_SYNC_CONFIG_ENV = "BALUFFO_SYNC_APP_CONFIG_PATH"
PACKAGED_SYNC_PASSPHRASE_ENV = "BALUFFO_SYNC_KEY_PASSPHRASE"
SYNC_CA_BUNDLE_ENV = "BALUFFO_SYNC_CA_BUNDLE"
SYNC_DISABLE_ENV = "BALUFFO_SYNC_DISABLE"
SYNC_ALLOWED_REPO_ENV = "BALUFFO_SYNC_ALLOWED_REPO"
SYNC_ALLOWED_BRANCH_ENV = "BALUFFO_SYNC_ALLOWED_BRANCH"
SYNC_ALLOWED_PATH_PREFIX_ENV = "BALUFFO_SYNC_ALLOWED_PATH_PREFIX"
DEFAULT_PACKAGED_SYNC_CONFIG_PATH = Path(_SYNC_DEFAULTS["packaged_config_path"])
MACHINE_SCOPE = "baluffo-github-app-sync"
JWT_TTL_SECONDS = 9 * 60
INSTALLATION_TOKEN_REFRESH_SKEW_SECONDS = 10 * 60
SHA256_DIGEST_INFO_PREFIX = bytes.fromhex("3031300d060960864801650304020105000420")
KEY_DERIVATION_MACHINE = "machine"
KEY_DERIVATION_PASSPHRASE = "passphrase"
KEY_DERIVATION_EMBEDDED = "embedded"
KEY_DERIVATION_PLAINTEXT = "plaintext"
RUNTIME_STATE_RATE_LIMITED = "rate_limited"
RUNTIME_STATE_REMOTE_CONFLICT = "remote_conflict"
RATE_LIMIT_WINDOW_S = 60
RATE_LIMIT_MAX_REQUESTS = 45
RATE_LIMIT_BACKOFF_BASE_S = 6
RATE_LIMIT_BACKOFF_MAX_S = 180
EMBEDDED_KEY_VERSION_DEFAULT = "v1"
_EMBEDDED_SECRET_PARTS = (
    "bA1uFf0",
    "o.Sync",
    ".Emb3d",
    "ded.KeY",
)


def _self_module() -> Any:
    return sys.modules[__name__]


class SyncOperationError(RuntimeError):
    def __init__(self, code: str, message: str):
        super().__init__(str(message or code or "sync error"))
        self.code = str(code or "").strip().lower() or "sync_error"


_RUNTIME_STATE_LOCK = threading.RLock()
_RUNTIME_STATE: dict[str, Any] = {"code": "", "message": "", "until": "", "updatedAt": ""}
_RATE_LIMIT_LOCK = threading.RLock()
_RATE_LIMIT_STATE: dict[str, Any] = {"calls": [], "strike": 0, "until": None}


class _DPAPI_BLOB(ctypes.Structure):
    _fields_ = [("cbData", ctypes.c_uint32), ("pbData", ctypes.POINTER(ctypes.c_byte))]


if os.name == "nt":
    _crypt32 = ctypes.windll.crypt32
    _kernel32 = ctypes.windll.kernel32
    _crypt_protect_data = _crypt32.CryptProtectData
    _crypt_protect_data.argtypes = [
        ctypes.POINTER(_DPAPI_BLOB),
        ctypes.c_wchar_p,
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.c_uint32,
        ctypes.POINTER(_DPAPI_BLOB),
    ]
    _crypt_protect_data.restype = ctypes.c_bool
    _crypt_unprotect_data = _crypt32.CryptUnprotectData
    _crypt_unprotect_data.argtypes = [
        ctypes.POINTER(_DPAPI_BLOB),
        ctypes.POINTER(ctypes.c_wchar_p),
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.c_uint32,
        ctypes.POINTER(_DPAPI_BLOB),
    ]
    _crypt_unprotect_data.restype = ctypes.c_bool
    _local_free = _kernel32.LocalFree
    _local_free.argtypes = [ctypes.c_void_p]
    _local_free.restype = ctypes.c_void_p
else:
    _crypt_protect_data = None
    _crypt_unprotect_data = None
    _local_free = lambda _x: None  # type: ignore[assignment]


@dataclass
class PackagedGitHubAppConfig:
    app_id: str
    installation_id: str
    repo: str
    branch: str
    path: str
    private_key_pem: str
    config_path: str
    key_derivation: str = KEY_DERIVATION_MACHINE
    decryption_error: str = ""
    policy_error: str = ""
    allowed_repo: str = ""
    allowed_branch: str = ""
    allowed_path_prefix: str = ""


@dataclass
class SyncConfig:
    enabled: bool
    repo: str
    branch: str
    path: str
    auth_mode: str
    packaged_config: PackagedGitHubAppConfig | None
    timeout_s: int = DEFAULT_TIMEOUT_S
    disabled_reason: str = ""


def _truthy(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "on"}


def _set_runtime_state(code: str = "", message: str = "", *, until: datetime | None = None) -> None:
    with _RUNTIME_STATE_LOCK:
        _RUNTIME_STATE["code"] = str(code or "").strip().lower()
        _RUNTIME_STATE["message"] = str(message or "").strip()
        _RUNTIME_STATE["until"] = until.isoformat() if isinstance(until, datetime) else ""
        _RUNTIME_STATE["updatedAt"] = now_iso()


def _clear_runtime_state(*codes: str) -> None:
    normalized = {str(item or "").strip().lower() for item in codes if str(item or "").strip()}
    with _RUNTIME_STATE_LOCK:
        current = str(_RUNTIME_STATE.get("code") or "").strip().lower()
        if normalized and current not in normalized:
            return
        _RUNTIME_STATE.update({"code": "", "message": "", "until": "", "updatedAt": now_iso()})


def _runtime_state_payload() -> dict[str, str]:
    with _RUNTIME_STATE_LOCK:
        code = str(_RUNTIME_STATE.get("code") or "").strip().lower()
        message = str(_RUNTIME_STATE.get("message") or "").strip()
        until = str(_RUNTIME_STATE.get("until") or "").strip()
        updated = str(_RUNTIME_STATE.get("updatedAt") or "").strip()
    if code == RUNTIME_STATE_RATE_LIMITED and until:
        until_dt = _parse_iso(until)
        if until_dt and until_dt <= now_utc():
            _clear_runtime_state(RUNTIME_STATE_RATE_LIMITED)
            return {"code": "", "message": "", "until": "", "updatedAt": ""}
    return {"code": code, "message": message, "until": until, "updatedAt": updated}


def _machine_fingerprint() -> str:
    user = str(os.getenv("USERNAME") or os.getenv("USER") or "").strip().lower()
    return "|".join(
        [
            MACHINE_SCOPE,
            platform.system().strip().lower(),
            platform.machine().strip().lower(),
            platform.node().strip().lower(),
            user,
        ]
    )


def _base64url_encode(raw: bytes) -> str:
    return _source_sync_crypto.base64url_encode(raw)


def _base64url_decode(text: str) -> bytes:
    return _source_sync_crypto.base64url_decode(text)


def _stream_encrypt(raw: bytes, key: bytes) -> bytes:
    return _source_sync_crypto.stream_encrypt(raw, key)


def _derive_private_key_binding_key(*, salt_b64: str, app_id: str, installation_id: str) -> bytes:
    return _source_sync_crypto.derive_private_key_binding_key(
        salt_b64=salt_b64,
        app_id=app_id,
        installation_id=installation_id,
        machine_fingerprint=_machine_fingerprint(),
    )


def encrypt_private_key_pem(
    private_key_pem: str, *, salt_b64: str, app_id: str, installation_id: str
) -> str:
    key = _derive_private_key_binding_key(
        salt_b64=salt_b64, app_id=app_id, installation_id=installation_id
    )
    return _source_sync_crypto.encrypt_private_key_pem(
        private_key_pem,
        salt_b64=salt_b64,
        app_id=app_id,
        installation_id=installation_id,
        key=key,
    )


def decrypt_private_key_pem(
    private_key_pem_enc: str, *, salt_b64: str, app_id: str, installation_id: str
) -> str:
    key = _derive_private_key_binding_key(
        salt_b64=salt_b64, app_id=app_id, installation_id=installation_id
    )
    return _source_sync_crypto.decrypt_private_key_pem(private_key_pem_enc, key=key)


def _derive_passphrase_key(
    *, salt_b64: str, app_id: str, installation_id: str, passphrase: str
) -> bytes:
    return _source_sync_crypto.derive_passphrase_key(
        salt_b64=salt_b64,
        app_id=app_id,
        installation_id=installation_id,
        passphrase=passphrase,
    )


def encrypt_private_key_pem_with_passphrase(
    private_key_pem: str,
    *,
    salt_b64: str,
    app_id: str,
    installation_id: str,
    passphrase: str,
) -> str:
    key = _derive_passphrase_key(
        salt_b64=salt_b64,
        app_id=app_id,
        installation_id=installation_id,
        passphrase=passphrase,
    )
    return _source_sync_crypto.encrypt_private_key_pem(
        private_key_pem,
        salt_b64=salt_b64,
        app_id=app_id,
        installation_id=installation_id,
        key=key,
    )


def decrypt_private_key_pem_with_passphrase(
    private_key_pem_enc: str,
    *,
    salt_b64: str,
    app_id: str,
    installation_id: str,
    passphrase: str,
) -> str:
    key = _derive_passphrase_key(
        salt_b64=salt_b64,
        app_id=app_id,
        installation_id=installation_id,
        passphrase=passphrase,
    )
    return _source_sync_crypto.decrypt_private_key_pem(private_key_pem_enc, key=key)


def build_embedded_passphrase(*, hint: str, version: str = EMBEDDED_KEY_VERSION_DEFAULT) -> str:
    return _source_sync_crypto.build_embedded_passphrase(hint=hint, version=version)


def _local_key_cache_fingerprint(normalized: dict[str, str]) -> str:
    return _source_sync_crypto.local_key_cache_fingerprint(normalized)


def _dpapi_protect(raw: bytes) -> str:
    if os.name != "nt":
        raise RuntimeError("DPAPI unavailable")
    data_in = ctypes.create_string_buffer(raw)
    blob_in = _DPAPI_BLOB(len(raw), ctypes.cast(data_in, ctypes.POINTER(ctypes.c_byte)))
    blob_out = _DPAPI_BLOB()
    ok = _crypt_protect_data(
        ctypes.byref(blob_in), None, None, None, None, 0, ctypes.byref(blob_out)
    )
    if not ok:
        raise RuntimeError("CryptProtectData failed")
    try:
        encrypted = ctypes.string_at(blob_out.pbData, blob_out.cbData)
        return _base64url_encode(encrypted)
    finally:
        _local_free(blob_out.pbData)


def _dpapi_unprotect(encoded: str) -> bytes:
    if os.name != "nt":
        raise RuntimeError("DPAPI unavailable")
    raw = _base64url_decode(encoded)
    data_in = ctypes.create_string_buffer(raw)
    blob_in = _DPAPI_BLOB(len(raw), ctypes.cast(data_in, ctypes.POINTER(ctypes.c_byte)))
    blob_out = _DPAPI_BLOB()
    ok = _crypt_unprotect_data(
        ctypes.byref(blob_in), None, None, None, None, 0, ctypes.byref(blob_out)
    )
    if not ok:
        raise RuntimeError("CryptUnprotectData failed")
    try:
        decrypted = ctypes.string_at(blob_out.pbData, blob_out.cbData)
        return decrypted
    finally:
        _local_free(blob_out.pbData)


def _local_key_cache_path(config_path: Path) -> Path:
    return config_path.with_suffix(".localkey.json")


def _read_local_wrapped_key(config_path: Path, fingerprint: str) -> str:
    cache_path = _local_key_cache_path(config_path)
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except Exception:
        return ""
    if not isinstance(payload, dict):
        return ""
    if str(payload.get("fingerprint") or "").strip() != str(fingerprint or "").strip():
        return ""
    wrapped = str(payload.get("dpapi") or "").strip()
    if not wrapped:
        return ""
    try:
        return _dpapi_unprotect(wrapped).decode("utf-8")
    except Exception:
        return ""


def _write_local_wrapped_key(config_path: Path, fingerprint: str, private_key_pem: str) -> None:
    if os.name != "nt":
        return
    wrapped = _dpapi_protect(str(private_key_pem or "").encode("utf-8"))
    payload = {
        "schemaVersion": 1,
        "fingerprint": str(fingerprint or ""),
        "dpapi": wrapped,
        "updatedAt": now_iso(),
    }
    cache_path = _local_key_cache_path(config_path)
    cache_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _allowlist_error(
    *, repo: str, branch: str, path: str, normalized: dict[str, str], env_map: dict[str, str]
) -> str:
    allowed_repo = str(
        env_map.get(SYNC_ALLOWED_REPO_ENV) or normalized.get("allowedRepo") or ""
    ).strip()
    allowed_branch = str(
        env_map.get(SYNC_ALLOWED_BRANCH_ENV) or normalized.get("allowedBranch") or ""
    ).strip()
    allowed_prefix = str(
        env_map.get(SYNC_ALLOWED_PATH_PREFIX_ENV) or normalized.get("allowedPathPrefix") or ""
    ).strip()
    if allowed_repo and str(repo or "").strip().lower() != allowed_repo.lower():
        return f"Blocked by allowlist: repo must be {allowed_repo}."
    if allowed_branch and str(branch or "").strip() != allowed_branch:
        return f"Blocked by allowlist: branch must be {allowed_branch}."
    if allowed_prefix and not str(path or "").strip().startswith(allowed_prefix):
        return f"Blocked by allowlist: path must start with {allowed_prefix}."
    return ""


def _normalize_packaged_payload(payload: dict[str, Any]) -> dict[str, str]:
    return _source_sync_config.normalize_packaged_payload(_self_module(), payload)


def load_packaged_sync_config(
    *, env: dict[str, str] | None = None
) -> PackagedGitHubAppConfig | None:
    return _source_sync_config.load_packaged_sync_config(_self_module(), env=env)


def resolve_sync_config(
    *, settings: dict[str, Any] | None = None, env: dict[str, str] | None = None
) -> SyncConfig:
    return _source_sync_config.resolve_sync_config(_self_module(), settings=settings, env=env)


def config_status(config: SyncConfig) -> dict[str, Any]:
    return _source_sync_config.config_status(_self_module(), config)


def validate_sync_config(config: SyncConfig) -> None:
    _source_sync_config.validate_sync_config(_self_module(), config)


def _content_api_url(config: SyncConfig, *, with_ref: bool = False) -> str:
    repo_token = quote(config.repo, safe="/")
    path_token = quote(config.path, safe="/")
    base = f"https://api.github.com/repos/{repo_token}/contents/{path_token}"
    if with_ref:
        ref_token = quote(config.branch, safe="")
        return f"{base}?ref={ref_token}"
    return base


def _github_json_headers(authorization: str) -> dict[str, str]:
    return {
        "Accept": "application/vnd.github+json",
        "Authorization": authorization,
        "User-Agent": "baluffo-source-sync/2.0",
        "X-GitHub-Api-Version": "2022-11-28",
        "Content-Type": "application/json; charset=utf-8",
    }


def _build_sync_ssl_context() -> ssl.SSLContext:
    return _source_sync_config.build_sync_ssl_context(_self_module())


def _request_raw_json(
    *,
    method: str,
    url: str,
    headers: dict[str, str],
    timeout_s: int,
    payload: dict[str, Any] | None = None,
    opener: Callable[..., Any] | None = None,
) -> tuple[int, dict[str, Any], dict[str, str]]:
    return _source_sync_config.request_raw_json(
        _self_module(),
        method=method,
        url=url,
        headers=headers,
        timeout_s=timeout_s,
        payload=payload,
        opener=opener,
    )


def _parse_iso(value: Any) -> datetime | None:
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
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


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
        dt = _parse_iso(row.get(key))
        if dt is not None:
            timestamps.append(int(dt.timestamp()))
    return max(timestamps) if timestamps else 0


def _row_bucket_rank(row: dict[str, Any]) -> int:
    bucket = str(row.get("registryState") or "").strip().lower()
    return {"active": 3, "pending": 2, "rejected": 1}.get(bucket, 0)


def _row_merge_key(row: dict[str, Any]) -> tuple[int, int]:
    return _row_transition_score(row), _row_bucket_rank(row)


def _choose_more_recent_row(
    local_row: dict[str, Any] | None, remote_row: dict[str, Any] | None
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


def _asn1_read_tlv(raw: bytes, offset: int) -> tuple[int, bytes, int]:
    if offset >= len(raw):
        raise ValueError("ASN.1 offset out of range")
    tag = raw[offset]
    offset += 1
    if offset >= len(raw):
        raise ValueError("ASN.1 missing length")
    first = raw[offset]
    offset += 1
    if first & 0x80:
        count = first & 0x7F
        if count <= 0 or offset + count > len(raw):
            raise ValueError("ASN.1 invalid length")
        length = int.from_bytes(raw[offset : offset + count], "big")
        offset += count
    else:
        length = first
    if offset + length > len(raw):
        raise ValueError("ASN.1 truncated value")
    value = raw[offset : offset + length]
    return tag, value, offset + length


def _asn1_read_children(raw: bytes) -> list[tuple[int, bytes]]:
    children: list[tuple[int, bytes]] = []
    offset = 0
    while offset < len(raw):
        tag, value, offset = _asn1_read_tlv(raw, offset)
        children.append((tag, value))
    return children


def _asn1_integer(value: bytes) -> int:
    raw = bytes(value)
    while len(raw) > 1 and raw[0] == 0x00:
        raw = raw[1:]
    return int.from_bytes(raw, "big", signed=False)


def _pem_to_der(private_key_pem: str) -> bytes:
    lines = []
    for raw in str(private_key_pem or "").strip().splitlines():
        line = str(raw or "").strip()
        if not line or line.startswith("-----BEGIN") or line.startswith("-----END"):
            continue
        lines.append(line)
    if not lines:
        raise RuntimeError("Missing PEM private key content")
    return base64.b64decode("".join(lines))


def _parse_rsa_private_key_der(der: bytes) -> tuple[int, int]:
    tag, value, end = _asn1_read_tlv(der, 0)
    if tag != 0x30 or end != len(der):
        raise ValueError("Invalid RSA private key sequence")
    children = _asn1_read_children(value)
    if len(children) >= 9 and all(tag_value[0] == 0x02 for tag_value in children[:9]):
        return _asn1_integer(children[1][1]), _asn1_integer(children[3][1])
    if len(children) >= 3 and children[2][0] == 0x04:
        return _parse_rsa_private_key_der(children[2][1])
    raise ValueError("Unsupported RSA private key encoding")


def _rsa_pkcs1_sign_sha256(message: bytes, private_key_pem: str) -> bytes:
    return _source_sync_crypto.rsa_pkcs1_sign_sha256(message, private_key_pem)


def build_app_jwt(app_id: str, private_key_pem: str, *, issued_at: datetime | None = None) -> str:
    return _source_sync_crypto.build_app_jwt(
        app_id,
        private_key_pem,
        issued_at=issued_at,
        now_utc_fn=now_utc,
        base64url_encode_fn=_base64url_encode,
        sign_sha256_fn=_rsa_pkcs1_sign_sha256,
        jwt_ttl_seconds=JWT_TTL_SECONDS,
    )


class GitHubAppAuth:
    def __init__(self, packaged_config: PackagedGitHubAppConfig):
        self.packaged_config = packaged_config
        self._token = ""
        self._token_expires_at: datetime | None = None
        self._lock = threading.RLock()

    def _token_is_fresh(self) -> bool:
        if not self._token or not self._token_expires_at:
            return False
        return (
            self._token_expires_at - now_utc()
        ).total_seconds() > INSTALLATION_TOKEN_REFRESH_SKEW_SECONDS

    def _refresh_installation_token(self, *, opener: Callable[..., Any] = urlopen) -> str:
        jwt_token = build_app_jwt(self.packaged_config.app_id, self.packaged_config.private_key_pem)
        url = f"https://api.github.com/app/installations/{quote(self.packaged_config.installation_id, safe='')}/access_tokens"
        status, payload, _headers = _request_raw_json(
            method="POST",
            url=url,
            headers=_github_json_headers(f"Bearer {jwt_token}"),
            timeout_s=DEFAULT_TIMEOUT_S,
            payload={},
            opener=opener,
        )
        if status >= 400:
            message = str(
                payload.get("message") or f"GitHub App token request failed with HTTP {status}"
            )
            raise RuntimeError(message)
        token = str(payload.get("token") or "").strip()
        expires_at = _parse_iso(payload.get("expires_at"))
        if not token or not expires_at:
            raise RuntimeError("GitHub App token response missing token or expires_at")
        self._token = token
        self._token_expires_at = expires_at
        return token

    def get_installation_token(
        self, *, opener: Callable[..., Any] = urlopen, force_refresh: bool = False
    ) -> str:
        with self._lock:
            if not force_refresh and self._token_is_fresh():
                return self._token
            return self._refresh_installation_token(opener=opener)


_AUTH_MANAGER_LOCK = threading.RLock()
_AUTH_MANAGER: dict[str, GitHubAppAuth] = {}


def _auth_manager_key(config: SyncConfig) -> str:
    packaged = config.packaged_config
    if not packaged:
        return ""
    return "|".join(
        [
            packaged.app_id,
            packaged.installation_id,
            packaged.repo,
            packaged.branch,
            packaged.path,
            packaged.config_path,
        ]
    )


def _get_auth_manager(config: SyncConfig) -> GitHubAppAuth:
    validate_sync_config(config)
    key = _auth_manager_key(config)
    with _AUTH_MANAGER_LOCK:
        manager = _AUTH_MANAGER.get(key)
        if manager is None:
            manager = GitHubAppAuth(config.packaged_config)  # type: ignore[arg-type]
            _AUTH_MANAGER[key] = manager
        return manager


def _rate_limit_retry_after_seconds(headers: dict[str, str], payload: dict[str, Any]) -> int:
    retry_after = str((headers or {}).get("retry-after") or "").strip()
    if retry_after.isdigit():
        return max(1, min(RATE_LIMIT_BACKOFF_MAX_S, int(retry_after)))
    reset_raw = str((headers or {}).get("x-ratelimit-reset") or "").strip()
    if reset_raw.isdigit():
        reset_at = int(reset_raw)
        delta = reset_at - int(now_utc().timestamp())
        if delta > 0:
            return max(1, min(RATE_LIMIT_BACKOFF_MAX_S, delta))
    msg = str((payload or {}).get("message") or "").lower()
    if "secondary rate limit" in msg:
        return min(RATE_LIMIT_BACKOFF_MAX_S, RATE_LIMIT_BACKOFF_BASE_S * 5)
    return RATE_LIMIT_BACKOFF_BASE_S


def _rate_limit_preflight() -> None:
    with _RATE_LIMIT_LOCK:
        now = now_utc()
        until = _RATE_LIMIT_STATE.get("until")
        if isinstance(until, datetime) and now < until:
            _set_runtime_state(
                RUNTIME_STATE_RATE_LIMITED,
                f"Sync rate limited locally until {until.isoformat()}",
                until=until,
            )
            raise SyncOperationError(RUNTIME_STATE_RATE_LIMITED, "Sync temporarily rate limited.")
        calls = [
            item
            for item in (_RATE_LIMIT_STATE.get("calls") or [])
            if isinstance(item, datetime) and (now - item).total_seconds() < RATE_LIMIT_WINDOW_S
        ]
        if len(calls) >= RATE_LIMIT_MAX_REQUESTS:
            strike = int(_RATE_LIMIT_STATE.get("strike") or 0) + 1
            wait_s = min(
                RATE_LIMIT_BACKOFF_MAX_S, RATE_LIMIT_BACKOFF_BASE_S * (2 ** max(0, strike - 1))
            )
            cooldown = now + timedelta(seconds=wait_s)
            _RATE_LIMIT_STATE.update({"calls": calls, "strike": strike, "until": cooldown})
            _set_runtime_state(
                RUNTIME_STATE_RATE_LIMITED,
                f"Sync rate limited locally for {wait_s}s",
                until=cooldown,
            )
            raise SyncOperationError(RUNTIME_STATE_RATE_LIMITED, "Sync temporarily rate limited.")
        calls.append(now)
        _RATE_LIMIT_STATE["calls"] = calls


def _rate_limit_note_response(
    status: int, headers: dict[str, str], payload: dict[str, Any]
) -> None:
    if int(status or 0) in {429, 403}:
        message = str((payload or {}).get("message") or "").lower()
        if int(status or 0) == 429 or "rate limit" in message:
            retry_s = _rate_limit_retry_after_seconds(headers, payload)
            until = now_utc() + timedelta(seconds=retry_s)
            with _RATE_LIMIT_LOCK:
                strike = int(_RATE_LIMIT_STATE.get("strike") or 0) + 1
                _RATE_LIMIT_STATE["strike"] = strike
                _RATE_LIMIT_STATE["until"] = until
            _set_runtime_state(
                RUNTIME_STATE_RATE_LIMITED,
                f"GitHub API rate limited sync for {retry_s}s",
                until=until,
            )
            raise SyncOperationError(
                RUNTIME_STATE_RATE_LIMITED, "GitHub rate limit reached for sync."
            )
    with _RATE_LIMIT_LOCK:
        strike = int(_RATE_LIMIT_STATE.get("strike") or 0)
        if strike > 0:
            _RATE_LIMIT_STATE["strike"] = max(0, strike - 1)
        until = _RATE_LIMIT_STATE.get("until")
        if isinstance(until, datetime) and now_utc() >= until:
            _RATE_LIMIT_STATE["until"] = None
    _clear_runtime_state(RUNTIME_STATE_RATE_LIMITED)


def _request_json(
    *,
    method: str,
    url: str,
    config: SyncConfig,
    timeout_s: int,
    payload: dict[str, Any] | None = None,
    opener: Callable[..., Any] = urlopen,
    allow_retry_401: bool = True,
) -> tuple[int, dict[str, Any], dict[str, str]]:
    _rate_limit_preflight()
    manager = _get_auth_manager(config)
    token = manager.get_installation_token(opener=opener)
    status, body, headers = _request_raw_json(
        method=method,
        url=url,
        headers=_github_json_headers(f"Bearer {token}"),
        timeout_s=timeout_s,
        payload=payload,
        opener=opener,
    )
    _rate_limit_note_response(status, headers, body)
    if status == 401 and allow_retry_401:
        token = manager.get_installation_token(opener=opener, force_refresh=True)
        status, body, headers = _request_raw_json(
            method=method,
            url=url,
            headers=_github_json_headers(f"Bearer {token}"),
            timeout_s=timeout_s,
            payload=payload,
            opener=opener,
        )
        _rate_limit_note_response(status, headers, body)
        return status, body, headers
    return status, body, headers


def normalize_snapshot(payload: dict[str, Any]) -> dict[str, Any]:
    return _source_sync_snapshot.normalize_snapshot(_self_module(), payload)


def merge_registry_state(
    local_state: dict[str, Any], remote_snapshot: dict[str, Any]
) -> dict[str, list[dict[str, Any]]]:
    return _source_sync_snapshot.merge_registry_state(_self_module(), local_state, remote_snapshot)


def read_remote_snapshot(
    config: SyncConfig,
    *,
    opener: Callable[..., Any] = urlopen,
) -> dict[str, Any]:
    return _source_sync_snapshot.read_remote_snapshot(_self_module(), config, opener=opener)


def build_snapshot(
    local_state: dict[str, Any], *, source_label: str = "admin_bridge"
) -> dict[str, Any]:
    return _source_sync_snapshot.build_snapshot(
        _self_module(), local_state, source_label=source_label
    )


def write_remote_snapshot(
    config: SyncConfig,
    snapshot: dict[str, Any],
    *,
    sha: str = "",
    message: str = "Update Baluffo source sync snapshot",
    opener: Callable[..., Any] = urlopen,
) -> dict[str, Any]:
    return _source_sync_snapshot.write_remote_snapshot(
        _self_module(),
        config,
        snapshot,
        sha=sha,
        message=message,
        opener=opener,
    )


def pull_and_merge_sources(
    config: SyncConfig,
    local_state: dict[str, Any],
    *,
    opener: Callable[..., Any] = urlopen,
) -> dict[str, Any]:
    return _source_sync_snapshot.pull_and_merge_sources(
        _self_module(), config, local_state, opener=opener
    )


def push_sources_snapshot(
    config: SyncConfig,
    local_state: dict[str, Any],
    *,
    opener: Callable[..., Any] = urlopen,
) -> dict[str, Any]:
    return _source_sync_snapshot.push_sources_snapshot(
        _self_module(), config, local_state, opener=opener
    )
