#!/usr/bin/env python3
"""GitHub App-backed source registry sync helpers."""

from __future__ import annotations

import base64
import json
import os
import platform
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from scripts.source_registry import ensure_source_id, source_identity

ROOT = Path(__file__).resolve().parents[1]
SYNC_SCHEMA_VERSION = 1
DEFAULT_BRANCH = "main"
DEFAULT_PATH = "baluffo/source-sync.json"
DEFAULT_TIMEOUT_S = 20
PACKAGED_SYNC_CONFIG_ENV = "BALUFFO_SYNC_APP_CONFIG_PATH"
PACKAGED_SYNC_PASSPHRASE_ENV = "BALUFFO_SYNC_KEY_PASSPHRASE"
DEFAULT_PACKAGED_SYNC_CONFIG_PATH = ROOT / "packaging" / "github-app-sync-config.json"
MACHINE_SCOPE = "baluffo-github-app-sync"
JWT_TTL_SECONDS = 9 * 60
INSTALLATION_TOKEN_REFRESH_SKEW_SECONDS = 10 * 60
SHA256_DIGEST_INFO_PREFIX = bytes.fromhex("3031300d060960864801650304020105000420")
KEY_DERIVATION_MACHINE = "machine"
KEY_DERIVATION_PASSPHRASE = "passphrase"


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


@dataclass
class SyncConfig:
    enabled: bool
    repo: str
    branch: str
    path: str
    auth_mode: str
    packaged_config: Optional[PackagedGitHubAppConfig]
    timeout_s: int = DEFAULT_TIMEOUT_S


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _machine_fingerprint() -> str:
    user = str(os.getenv("USERNAME") or os.getenv("USER") or "").strip().lower()
    return "|".join([
        MACHINE_SCOPE,
        platform.system().strip().lower(),
        platform.machine().strip().lower(),
        platform.node().strip().lower(),
        user,
    ])


def _base64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _base64url_decode(text: str) -> bytes:
    padded = str(text or "").strip()
    padded += "=" * ((4 - (len(padded) % 4)) % 4)
    return base64.urlsafe_b64decode(padded.encode("ascii"))


def _stream_encrypt(raw: bytes, key: bytes) -> bytes:
    if not key:
        raise RuntimeError("Missing encryption key")
    out = bytearray()
    counter = 0
    while len(out) < len(raw):
        block = __import__("hashlib").sha256(key + counter.to_bytes(4, "big")).digest()
        out.extend(block)
        counter += 1
    return bytes(a ^ b for a, b in zip(raw, out[: len(raw)]))


def _derive_private_key_binding_key(*, salt_b64: str, app_id: str, installation_id: str) -> bytes:
    salt = _base64url_decode(salt_b64)
    material = "|".join([
        _machine_fingerprint(),
        str(app_id or "").strip(),
        str(installation_id or "").strip(),
        _base64url_encode(salt),
    ]).encode("utf-8")
    return __import__("hashlib").sha256(material).digest()


def encrypt_private_key_pem(private_key_pem: str, *, salt_b64: str, app_id: str, installation_id: str) -> str:
    key = _derive_private_key_binding_key(salt_b64=salt_b64, app_id=app_id, installation_id=installation_id)
    encrypted = _stream_encrypt(str(private_key_pem or "").encode("utf-8"), key)
    return _base64url_encode(encrypted)


def decrypt_private_key_pem(private_key_pem_enc: str, *, salt_b64: str, app_id: str, installation_id: str) -> str:
    key = _derive_private_key_binding_key(salt_b64=salt_b64, app_id=app_id, installation_id=installation_id)
    decrypted = _stream_encrypt(_base64url_decode(private_key_pem_enc), key)
    return decrypted.decode("utf-8")


def _derive_passphrase_key(*, salt_b64: str, app_id: str, installation_id: str, passphrase: str) -> bytes:
    salt = _base64url_decode(salt_b64)
    material = "|".join([
        MACHINE_SCOPE,
        KEY_DERIVATION_PASSPHRASE,
        str(app_id or "").strip(),
        str(installation_id or "").strip(),
        _base64url_encode(salt),
        str(passphrase or ""),
    ]).encode("utf-8")
    return __import__("hashlib").sha256(material).digest()


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


def _normalize_packaged_payload(payload: Dict[str, Any]) -> Dict[str, str]:
    data = payload if isinstance(payload, dict) else {}
    return {
        "appId": str(data.get("appId") or "").strip(),
        "installationId": str(data.get("installationId") or "").strip(),
        "repo": str(data.get("repo") or "").strip(),
        "branch": str(data.get("branch") or DEFAULT_BRANCH).strip() or DEFAULT_BRANCH,
        "path": str(data.get("path") or DEFAULT_PATH).strip() or DEFAULT_PATH,
        "privateKeyPemEnc": str(data.get("privateKeyPemEnc") or "").strip(),
        "privateKeyPem": str(data.get("privateKeyPem") or "").strip(),
        "keySalt": str(data.get("keySalt") or "").strip(),
        "keyDerivation": str(data.get("keyDerivation") or KEY_DERIVATION_MACHINE).strip().lower() or KEY_DERIVATION_MACHINE,
    }


def load_packaged_sync_config(*, env: Optional[Dict[str, str]] = None) -> Optional[PackagedGitHubAppConfig]:
    env_map = env if isinstance(env, dict) else os.environ
    path_raw = str(env_map.get(PACKAGED_SYNC_CONFIG_ENV) or DEFAULT_PACKAGED_SYNC_CONFIG_PATH).strip()
    config_path = Path(path_raw).expanduser().resolve()
    if not config_path.exists():
        return None
    try:
        payload = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    normalized = _normalize_packaged_payload(payload if isinstance(payload, dict) else {})
    private_key_pem = normalized["privateKeyPem"]
    key_derivation = normalized["keyDerivation"]
    decryption_error = ""
    if not private_key_pem and normalized["privateKeyPemEnc"] and normalized["keySalt"]:
        try:
            if key_derivation == KEY_DERIVATION_PASSPHRASE:
                passphrase = str(env_map.get(PACKAGED_SYNC_PASSPHRASE_ENV) or "").strip()
                if not passphrase:
                    raise RuntimeError(f"Missing {PACKAGED_SYNC_PASSPHRASE_ENV} for passphrase-encrypted sync key.")
                private_key_pem = decrypt_private_key_pem_with_passphrase(
                    normalized["privateKeyPemEnc"],
                    salt_b64=normalized["keySalt"],
                    app_id=normalized["appId"],
                    installation_id=normalized["installationId"],
                    passphrase=passphrase,
                )
            elif key_derivation in {"", KEY_DERIVATION_MACHINE}:
                key_derivation = KEY_DERIVATION_MACHINE
                private_key_pem = decrypt_private_key_pem(
                    normalized["privateKeyPemEnc"],
                    salt_b64=normalized["keySalt"],
                    app_id=normalized["appId"],
                    installation_id=normalized["installationId"],
                )
            else:
                raise RuntimeError(f"Unsupported keyDerivation mode: {key_derivation}")
        except Exception as exc:  # noqa: BLE001
            decryption_error = str(exc)
    return PackagedGitHubAppConfig(
        app_id=normalized["appId"],
        installation_id=normalized["installationId"],
        repo=normalized["repo"],
        branch=normalized["branch"],
        path=normalized["path"],
        private_key_pem=private_key_pem,
        config_path=str(config_path),
        key_derivation=key_derivation,
        decryption_error=decryption_error,
    )


def resolve_sync_config(*, settings: Optional[Dict[str, Any]] = None, env: Optional[Dict[str, str]] = None) -> SyncConfig:
    settings_map = settings if isinstance(settings, dict) else {}
    env_map = env if isinstance(env, dict) else os.environ
    enabled_raw = settings_map.get("enabled")
    enabled = True if enabled_raw is None else bool(enabled_raw)
    packaged_config = load_packaged_sync_config(env=env_map)
    repo = packaged_config.repo if packaged_config else ""
    branch = packaged_config.branch if packaged_config else DEFAULT_BRANCH
    path = packaged_config.path if packaged_config else DEFAULT_PATH
    return SyncConfig(
        enabled=enabled,
        repo=repo,
        branch=branch,
        path=path,
        auth_mode="github_app",
        packaged_config=packaged_config,
        timeout_s=DEFAULT_TIMEOUT_S,
    )


def config_status(config: SyncConfig) -> Dict[str, Any]:
    missing: List[str] = []
    message = ""
    if not config.packaged_config:
        missing.append("packaged_github_app_config")
    else:
        if config.packaged_config.decryption_error:
            missing.append("privateKeyPemEnc")
            message = f"Could not decrypt packaged GitHub App key: {config.packaged_config.decryption_error}"
        if not config.packaged_config.app_id:
            missing.append("appId")
        if not config.packaged_config.installation_id:
            missing.append("installationId")
        if not config.packaged_config.repo:
            missing.append("repo")
        if not config.packaged_config.private_key_pem:
            missing.append("privateKeyPemEnc")
    ready = bool(config.enabled and not missing)
    state = "ready" if ready else ("disabled" if not config.enabled else "misconfigured")
    if config.enabled and not ready and not message:
        if "packaged_github_app_config" in missing:
            message = (
                "Missing packaged GitHub App config. "
                f"Expected {PACKAGED_SYNC_CONFIG_ENV} or {DEFAULT_PACKAGED_SYNC_CONFIG_PATH.name}."
            )
        else:
            message = "Packaged GitHub App config is incomplete."
    return {
        "enabled": bool(config.enabled),
        "state": state,
        "ready": ready,
        "repo": config.repo,
        "branch": config.branch,
        "path": config.path,
        "missing": missing,
        "message": message,
        "authMode": str(config.auth_mode or "github_app"),
        "credentialsPackaged": bool(config.packaged_config),
        "configPath": str(config.packaged_config.config_path if config.packaged_config else ""),
    }


def validate_sync_config(config: SyncConfig) -> None:
    status = config_status(config)
    if not status["ready"]:
        raise RuntimeError(str(status["message"] or "Sync is not configured"))


def _content_api_url(config: SyncConfig, *, with_ref: bool = False) -> str:
    repo_token = quote(config.repo, safe="/")
    path_token = quote(config.path, safe="/")
    base = f"https://api.github.com/repos/{repo_token}/contents/{path_token}"
    if with_ref:
        ref_token = quote(config.branch, safe="")
        return f"{base}?ref={ref_token}"
    return base


def _github_json_headers(authorization: str) -> Dict[str, str]:
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
    headers: Dict[str, str],
    timeout_s: int,
    payload: Optional[Dict[str, Any]] = None,
    opener: Callable[..., Any] = urlopen,
) -> Tuple[int, Dict[str, Any], Dict[str, str]]:
    body: Optional[bytes] = None
    if payload is not None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = Request(url=url, data=body, method=method.upper(), headers=headers)
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


def _asn1_read_tlv(raw: bytes, offset: int) -> Tuple[int, bytes, int]:
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
        length = int.from_bytes(raw[offset: offset + count], "big")
        offset += count
    else:
        length = first
    if offset + length > len(raw):
        raise ValueError("ASN.1 truncated value")
    value = raw[offset: offset + length]
    return tag, value, offset + length


def _asn1_read_children(raw: bytes) -> List[Tuple[int, bytes]]:
    children: List[Tuple[int, bytes]] = []
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


def _parse_rsa_private_key_der(der: bytes) -> Tuple[int, int]:
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
    n, d = _parse_rsa_private_key_der(_pem_to_der(private_key_pem))
    digest = __import__("hashlib").sha256(message).digest()
    digest_info = SHA256_DIGEST_INFO_PREFIX + digest
    modulus_len = max(1, (n.bit_length() + 7) // 8)
    padding_len = modulus_len - len(digest_info) - 3
    if padding_len < 8:
        raise RuntimeError("RSA key too small for RS256 signing")
    encoded = b"\x00\x01" + (b"\xff" * padding_len) + b"\x00" + digest_info
    signature = pow(int.from_bytes(encoded, "big"), d, n)
    return signature.to_bytes(modulus_len, "big")


def build_app_jwt(app_id: str, private_key_pem: str, *, issued_at: Optional[datetime] = None) -> str:
    now = issued_at.astimezone(timezone.utc) if issued_at else now_utc()
    iat = int(now.timestamp()) - 30
    exp = iat + JWT_TTL_SECONDS
    header = _base64url_encode(json.dumps({"alg": "RS256", "typ": "JWT"}, separators=(",", ":"), sort_keys=True).encode("utf-8"))
    payload = _base64url_encode(json.dumps({"iat": iat, "exp": exp, "iss": str(app_id or "").strip()}, separators=(",", ":"), sort_keys=True).encode("utf-8"))
    signing_input = f"{header}.{payload}".encode("ascii")
    signature = _base64url_encode(_rsa_pkcs1_sign_sha256(signing_input, private_key_pem))
    return f"{header}.{payload}.{signature}"


class GitHubAppAuth:
    def __init__(self, packaged_config: PackagedGitHubAppConfig):
        self.packaged_config = packaged_config
        self._token = ""
        self._token_expires_at: Optional[datetime] = None
        self._lock = threading.RLock()

    def _token_is_fresh(self) -> bool:
        if not self._token or not self._token_expires_at:
            return False
        return (self._token_expires_at - now_utc()).total_seconds() > INSTALLATION_TOKEN_REFRESH_SKEW_SECONDS

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
            message = str(payload.get("message") or f"GitHub App token request failed with HTTP {status}")
            raise RuntimeError(message)
        token = str(payload.get("token") or "").strip()
        expires_at = _parse_iso(payload.get("expires_at"))
        if not token or not expires_at:
            raise RuntimeError("GitHub App token response missing token or expires_at")
        self._token = token
        self._token_expires_at = expires_at
        return token

    def get_installation_token(self, *, opener: Callable[..., Any] = urlopen, force_refresh: bool = False) -> str:
        with self._lock:
            if not force_refresh and self._token_is_fresh():
                return self._token
            return self._refresh_installation_token(opener=opener)


_AUTH_MANAGER_LOCK = threading.RLock()
_AUTH_MANAGER: Dict[str, GitHubAppAuth] = {}


def _auth_manager_key(config: SyncConfig) -> str:
    packaged = config.packaged_config
    if not packaged:
        return ""
    return "|".join([packaged.app_id, packaged.installation_id, packaged.repo, packaged.branch, packaged.path, packaged.config_path])


def _get_auth_manager(config: SyncConfig) -> GitHubAppAuth:
    validate_sync_config(config)
    key = _auth_manager_key(config)
    with _AUTH_MANAGER_LOCK:
        manager = _AUTH_MANAGER.get(key)
        if manager is None:
            manager = GitHubAppAuth(config.packaged_config)  # type: ignore[arg-type]
            _AUTH_MANAGER[key] = manager
        return manager


def _request_json(
    *,
    method: str,
    url: str,
    config: SyncConfig,
    timeout_s: int,
    payload: Optional[Dict[str, Any]] = None,
    opener: Callable[..., Any] = urlopen,
    allow_retry_401: bool = True,
) -> Tuple[int, Dict[str, Any], Dict[str, str]]:
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
    if status == 401 and allow_retry_401:
        token = manager.get_installation_token(opener=opener, force_refresh=True)
        return _request_raw_json(
            method=method,
            url=url,
            headers=_github_json_headers(f"Bearer {token}"),
            timeout_s=timeout_s,
            payload=payload,
            opener=opener,
        )
    return status, body, headers


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
    remote = normalize_snapshot(remote_snapshot)
    return {
        "active": list(remote["active"]),
        "pending": list(remote["pending"]),
        "rejected": list(remote["rejected"]),
    }


def read_remote_snapshot(
    config: SyncConfig,
    *,
    opener: Callable[..., Any] = urlopen,
) -> Dict[str, Any]:
    validate_sync_config(config)
    url = _content_api_url(config, with_ref=True)
    status, payload, _headers = _request_json(
        method="GET",
        url=url,
        config=config,
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


def _merge_without_losing_active_pending(local_snapshot: Dict[str, Any], remote_snapshot: Dict[str, Any]) -> Dict[str, Any]:
    local = normalize_snapshot(local_snapshot)
    remote = normalize_snapshot(remote_snapshot)
    rejected_ids = {
        source_identity(row)
        for row in (local.get("rejected") or [])
        if isinstance(row, dict)
    }

    merged: Dict[str, Any] = {
        "schemaVersion": int(local.get("schemaVersion") or SYNC_SCHEMA_VERSION),
        "generatedAt": str(local.get("generatedAt") or now_iso()),
        "source": dict(local.get("source") or {}),
        "active": [ensure_source_id(row) for row in (local.get("active") or []) if isinstance(row, dict)],
        "pending": [ensure_source_id(row) for row in (local.get("pending") or []) if isinstance(row, dict)],
        "rejected": [ensure_source_id(row) for row in (local.get("rejected") or []) if isinstance(row, dict)],
    }

    seen = {
        source_identity(row)
        for row in [*merged["active"], *merged["pending"]]
        if isinstance(row, dict)
    }
    for bucket in ("active", "pending"):
        for row in (remote.get(bucket) or []):
            if not isinstance(row, dict):
                continue
            key = source_identity(row)
            if key in seen or key in rejected_ids:
                continue
            merged[bucket].append(ensure_source_id(row))
            seen.add(key)
    return merged


def write_remote_snapshot(
    config: SyncConfig,
    snapshot: Dict[str, Any],
    *,
    sha: str = "",
    message: str = "Update Baluffo source sync snapshot",
    opener: Callable[..., Any] = urlopen,
) -> Dict[str, Any]:
    validate_sync_config(config)
    encoded = base64.b64encode(json.dumps(snapshot, ensure_ascii=False, indent=2).encode("utf-8")).decode("ascii")
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
        config=config,
        timeout_s=config.timeout_s,
        payload=payload,
        opener=opener,
    )
    if status >= 400:
        msg = str(body.get("message") or f"GitHub PUT failed with HTTP {status}")
        raise RuntimeError(msg)
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
    remote_snapshot = remote.get("snapshot") if isinstance(remote.get("snapshot"), dict) else {}
    if remote_snapshot:
        snapshot = _merge_without_losing_active_pending(snapshot, remote_snapshot)
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
