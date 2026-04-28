#!/usr/bin/env python3
"""GitHub App-backed source registry sync helpers."""

from __future__ import annotations

import os
import sys
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, cast
from urllib.error import HTTPError as _HTTPError
from urllib.error import URLError as _URLError
from urllib.parse import quote
from urllib.request import Request as _Request
from urllib.request import urlopen

import src.source_sync_config as _source_sync_config
import src.source_sync_crypto as _source_sync_crypto
import src.source_sync_runtime as _source_sync_runtime
import src.source_sync_snapshot as _source_sync_snapshot
from src.baluffo_config import get_security_defaults, get_sync_defaults
from src.bridge.registry_tombstones import (
    filter_tombstoned_rows as _filter_tombstoned_rows,
)
from src.bridge.registry_tombstones import load_tombstones as _load_tombstones
from src.shared.utils import now_iso as _now_iso
from src.shared.utils import now_utc
from src.source_registry import (
    REGISTRY_MIGRATION_V2 as _REGISTRY_MIGRATION_V2,
)
from src.source_registry import (
    REGISTRY_REASON_PENDING_DEFAULT as _REGISTRY_REASON_PENDING_DEFAULT,
)
from src.source_registry import source_identity as _source_identity

ROOT = Path(__file__).resolve().parents[1]
_SYNC_DEFAULTS = get_sync_defaults()
_SECURITY_DEFAULTS = get_security_defaults()
HTTPError = _HTTPError
URLError = _URLError
Request = _Request
load_tombstones = _load_tombstones
filter_tombstoned_rows = _filter_tombstoned_rows
source_identity = _source_identity
# Re-exported for extracted leaves that still import clock helpers from the
# stable sync facade.
now_iso = _now_iso
REGISTRY_MIGRATION_V2 = _REGISTRY_MIGRATION_V2
REGISTRY_REASON_PENDING_DEFAULT = _REGISTRY_REASON_PENDING_DEFAULT
SYNC_SCHEMA_VERSION = 2
DEFAULT_BRANCH = str(_SYNC_DEFAULTS["default_branch"])
DEFAULT_PATH = str(_SYNC_DEFAULTS["default_path"])
DEFAULT_TIMEOUT_S = 20
PACKAGED_SYNC_CONFIG_ENV = "BALUFFO_SYNC_APP_CONFIG_PATH"
PACKAGED_SYNC_BUILD_CONFIG_ENV = "BALUFFO_SYNC_BUILD_CONFIG_PATH"
PACKAGED_SYNC_PASSPHRASE_ENV = "BALUFFO_SYNC_KEY_PASSPHRASE"
SYNC_CA_BUNDLE_ENV = "BALUFFO_SYNC_CA_BUNDLE"
GITHUB_API_BASE_ENV = "BALUFFO_SYNC_GITHUB_API_BASE"
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


def _github_api_base() -> str:
    override = str(os.environ.get(GITHUB_API_BASE_ENV) or "").strip().rstrip("/")
    return override or "https://api.github.com"


class SyncOperationError(RuntimeError):
    def __init__(self, code: str, message: str):
        super().__init__(str(message or code or "sync error"))
        self.code = str(code or "").strip().lower() or "sync_error"


_RUNTIME_STATE_LOCK = _source_sync_runtime._RUNTIME_STATE_LOCK
_RUNTIME_STATE = _source_sync_runtime._RUNTIME_STATE
_RATE_LIMIT_LOCK = _source_sync_runtime._RATE_LIMIT_LOCK
_RATE_LIMIT_STATE = _source_sync_runtime._RATE_LIMIT_STATE
_AUTH_MANAGER_LOCK = _source_sync_runtime._AUTH_MANAGER_LOCK
_AUTH_MANAGER = _source_sync_runtime._AUTH_MANAGER
_DPAPI_BLOB = _source_sync_runtime._DPAPI_BLOB
_crypt_protect_data = _source_sync_runtime._crypt_protect_data
_crypt_unprotect_data = _source_sync_runtime._crypt_unprotect_data
_local_free = _source_sync_runtime._local_free


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
    _source_sync_runtime.set_runtime_state(_self_module(), code, message, until=until)


def _clear_runtime_state(*codes: str) -> None:
    _source_sync_runtime.clear_runtime_state(_self_module(), *codes)


def _runtime_state_payload() -> dict[str, str]:
    return _source_sync_runtime.runtime_state_payload(_self_module())


def _machine_fingerprint() -> str:
    return _source_sync_runtime.machine_fingerprint(_self_module())


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
        salt_b64=salt_b64,
        app_id=app_id,
        installation_id=installation_id,
    )
    encrypted = _stream_encrypt(str(private_key_pem or "").encode("utf-8"), key)
    return _base64url_encode(encrypted)


def decrypt_private_key_pem(
    private_key_pem_enc: str, *, salt_b64: str, app_id: str, installation_id: str
) -> str:
    key = _derive_private_key_binding_key(
        salt_b64=salt_b64,
        app_id=app_id,
        installation_id=installation_id,
    )
    decrypted = _stream_encrypt(_base64url_decode(private_key_pem_enc), key)
    return decrypted.decode("utf-8")


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
    encrypted = _stream_encrypt(str(private_key_pem or "").encode("utf-8"), key)
    return _base64url_encode(encrypted)


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
    decrypted = _stream_encrypt(_base64url_decode(private_key_pem_enc), key)
    return decrypted.decode("utf-8")


def build_embedded_passphrase(*, hint: str, version: str = EMBEDDED_KEY_VERSION_DEFAULT) -> str:
    return _source_sync_crypto.build_embedded_passphrase(hint=hint, version=version)


def _local_key_cache_fingerprint(normalized: dict[str, str]) -> str:
    return _source_sync_crypto.local_key_cache_fingerprint(normalized)


def _dpapi_protect(raw: bytes) -> str:
    return _source_sync_runtime.dpapi_protect(_self_module(), raw)


def _dpapi_unprotect(encoded: str) -> bytes:
    return _source_sync_runtime.dpapi_unprotect(_self_module(), encoded)


def _local_key_cache_path(config_path: Path) -> Path:
    return _source_sync_runtime.local_key_cache_path(config_path)


def _read_local_wrapped_key(config_path: Path, fingerprint: str) -> str:
    return _source_sync_runtime.read_local_wrapped_key(_self_module(), config_path, fingerprint)


def _write_local_wrapped_key(config_path: Path, fingerprint: str, private_key_pem: str) -> None:
    _source_sync_runtime.write_local_wrapped_key(
        _self_module(),
        config_path,
        fingerprint,
        private_key_pem,
    )


def _allowlist_error(
    *, repo: str, branch: str, path: str, normalized: dict[str, str], env_map: dict[str, str]
) -> str:
    return _source_sync_runtime.allowlist_error(
        _self_module(),
        repo=repo,
        branch=branch,
        path=path,
        normalized=normalized,
        env_map=env_map,
    )


def _normalize_packaged_payload(payload: dict[str, Any]) -> dict[str, str]:
    return _source_sync_config.normalize_packaged_payload(_self_module(), payload)


def _candidate_packaged_sync_config_paths(
    *,
    env: dict[str, str] | None = None,
    default_path: Path | None = None,
) -> list[Path]:
    return _source_sync_config.candidate_packaged_sync_config_paths(
        _self_module(),
        env=env,
        default_path=default_path,
    )


def load_packaged_sync_config(
    *, env: dict[str, str] | None = None
) -> PackagedGitHubAppConfig | None:
    return cast(
        PackagedGitHubAppConfig | None,
        _source_sync_config.load_packaged_sync_config(_self_module(), env=env),
    )


def resolve_sync_config(
    *, settings: dict[str, Any] | None = None, env: dict[str, str] | None = None
) -> SyncConfig:
    return cast(
        SyncConfig,
        _source_sync_config.resolve_sync_config(_self_module(), settings=settings, env=env),
    )


def config_status(config: SyncConfig) -> dict[str, Any]:
    return _source_sync_config.config_status(_self_module(), config)


def validate_sync_config(config: SyncConfig) -> None:
    _source_sync_config.validate_sync_config(_self_module(), config)


def _content_api_url(config: SyncConfig, *, with_ref: bool = False) -> str:
    repo_token = quote(config.repo, safe="/")
    path_token = quote(config.path, safe="/")
    base = f"{_github_api_base()}/repos/{repo_token}/contents/{path_token}"
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
    return _source_sync_runtime.parse_iso(value)


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
        self.packaged_config: PackagedGitHubAppConfig = packaged_config
        self._token: str = ""
        self._token_expires_at: datetime | None = None
        self._lock: Any = None
        _source_sync_runtime.init_github_app_auth(self, packaged_config)

    def _token_is_fresh(self) -> bool:
        return _source_sync_runtime.github_app_auth_token_is_fresh(_self_module(), self)

    def _refresh_installation_token(self, *, opener: Callable[..., Any] = urlopen) -> str:
        return _source_sync_runtime.github_app_auth_refresh_installation_token(
            _self_module(),
            self,
            opener=opener,
        )

    def get_installation_token(
        self, *, opener: Callable[..., Any] = urlopen, force_refresh: bool = False
    ) -> str:
        with self._lock:
            if not force_refresh and self._token_is_fresh():
                return self._token
            return self._refresh_installation_token(opener=opener)


def _auth_manager_key(config: SyncConfig) -> str:
    return _source_sync_runtime.auth_manager_key(config)


def _get_auth_manager(config: SyncConfig) -> GitHubAppAuth:
    return cast(GitHubAppAuth, _source_sync_runtime.get_auth_manager(_self_module(), config))


def _rate_limit_retry_after_seconds(headers: dict[str, str], payload: dict[str, Any]) -> int:
    return _source_sync_runtime.rate_limit_retry_after_seconds(_self_module(), headers, payload)


def _rate_limit_preflight() -> None:
    _source_sync_runtime.rate_limit_preflight(_self_module())


def _rate_limit_note_response(
    status: int, headers: dict[str, str], payload: dict[str, Any]
) -> None:
    _source_sync_runtime.rate_limit_note_response(_self_module(), status, headers, payload)


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
    return _source_sync_runtime.request_json(
        _self_module(),
        method=method,
        url=url,
        config=config,
        timeout_s=timeout_s,
        payload=payload,
        opener=opener,
        allow_retry_401=allow_retry_401,
    )


def normalize_snapshot(payload: dict[str, Any]) -> dict[str, Any]:
    return _source_sync_snapshot.normalize_snapshot(_self_module(), payload)


def merge_registry_state(
    local_state: dict[str, Any],
    remote_snapshot: dict[str, Any],
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
        _self_module(),
        config,
        local_state,
        opener=opener,
    )


def push_sources_snapshot(
    config: SyncConfig,
    local_state: dict[str, Any],
    *,
    opener: Callable[..., Any] = urlopen,
) -> dict[str, Any]:
    return _source_sync_snapshot.push_sources_snapshot(
        _self_module(),
        config,
        local_state,
        opener=opener,
    )
