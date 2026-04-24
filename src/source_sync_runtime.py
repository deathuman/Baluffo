from __future__ import annotations

import ctypes
import json
import os
import platform
import threading
from collections.abc import Callable
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from src.shared.utils import parse_iso as parse_iso_from_utils

_RUNTIME_STATE_LOCK = threading.RLock()
_RUNTIME_STATE: dict[str, Any] = {"code": "", "message": "", "until": "", "updatedAt": ""}
_RATE_LIMIT_LOCK = threading.RLock()
_RATE_LIMIT_STATE: dict[str, Any] = {"calls": [], "strike": 0, "until": None}
_AUTH_MANAGER_LOCK = threading.RLock()
_AUTH_MANAGER: dict[str, Any] = {}
_crypt_protect_data: Callable[..., bool] | None
_crypt_unprotect_data: Callable[..., bool] | None
_local_free: Callable[[Any], Any]


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
    _local_free = lambda _x: None


def set_runtime_state(
    root_mod: Any,
    code: str = "",
    message: str = "",
    *,
    until: datetime | None = None,
) -> None:
    with _RUNTIME_STATE_LOCK:
        _RUNTIME_STATE["code"] = str(code or "").strip().lower()
        _RUNTIME_STATE["message"] = str(message or "").strip()
        _RUNTIME_STATE["until"] = until.isoformat() if isinstance(until, datetime) else ""
        _RUNTIME_STATE["updatedAt"] = root_mod.now_iso()


def clear_runtime_state(root_mod: Any, *codes: str) -> None:
    normalized = {str(item or "").strip().lower() for item in codes if str(item or "").strip()}
    with _RUNTIME_STATE_LOCK:
        current = str(_RUNTIME_STATE.get("code") or "").strip().lower()
        if normalized and current not in normalized:
            return
        _RUNTIME_STATE.update(
            {"code": "", "message": "", "until": "", "updatedAt": root_mod.now_iso()}
        )


def runtime_state_payload(root_mod: Any) -> dict[str, str]:
    with _RUNTIME_STATE_LOCK:
        code = str(_RUNTIME_STATE.get("code") or "").strip().lower()
        message = str(_RUNTIME_STATE.get("message") or "").strip()
        until = str(_RUNTIME_STATE.get("until") or "").strip()
        updated = str(_RUNTIME_STATE.get("updatedAt") or "").strip()
    if code == root_mod.RUNTIME_STATE_RATE_LIMITED and until:
        until_dt = parse_iso(until)
        if until_dt and until_dt <= root_mod.now_utc():
            clear_runtime_state(root_mod, root_mod.RUNTIME_STATE_RATE_LIMITED)
            return {"code": "", "message": "", "until": "", "updatedAt": ""}
    return {"code": code, "message": message, "until": until, "updatedAt": updated}


def machine_fingerprint(root_mod: Any) -> str:
    user = str(os.getenv("USERNAME") or os.getenv("USER") or "").strip().lower()
    return "|".join(
        [
            root_mod.MACHINE_SCOPE,
            platform.system().strip().lower(),
            platform.machine().strip().lower(),
            platform.node().strip().lower(),
            user,
        ]
    )


def dpapi_protect(root_mod: Any, raw: bytes) -> str:
    if os.name != "nt":
        raise RuntimeError("DPAPI unavailable")
    crypt_protect_data = _crypt_protect_data
    if crypt_protect_data is None:
        raise RuntimeError("DPAPI unavailable")
    data_in = ctypes.create_string_buffer(raw)
    blob_in = _DPAPI_BLOB(len(raw), ctypes.cast(data_in, ctypes.POINTER(ctypes.c_byte)))
    blob_out = _DPAPI_BLOB()
    ok = crypt_protect_data(
        ctypes.byref(blob_in), None, None, None, None, 0, ctypes.byref(blob_out)
    )
    if not ok:
        raise RuntimeError("CryptProtectData failed")
    try:
        encrypted = bytes(ctypes.string_at(blob_out.pbData, blob_out.cbData))
        return str(root_mod._base64url_encode(encrypted))
    finally:
        _local_free(blob_out.pbData)


def dpapi_unprotect(root_mod: Any, encoded: str) -> bytes:
    if os.name != "nt":
        raise RuntimeError("DPAPI unavailable")
    crypt_unprotect_data = _crypt_unprotect_data
    if crypt_unprotect_data is None:
        raise RuntimeError("DPAPI unavailable")
    raw = root_mod._base64url_decode(encoded)
    data_in = ctypes.create_string_buffer(raw)
    blob_in = _DPAPI_BLOB(len(raw), ctypes.cast(data_in, ctypes.POINTER(ctypes.c_byte)))
    blob_out = _DPAPI_BLOB()
    ok = crypt_unprotect_data(
        ctypes.byref(blob_in), None, None, None, None, 0, ctypes.byref(blob_out)
    )
    if not ok:
        raise RuntimeError("CryptUnprotectData failed")
    try:
        return bytes(ctypes.string_at(blob_out.pbData, blob_out.cbData))
    finally:
        _local_free(blob_out.pbData)


def local_key_cache_path(config_path: Path) -> Path:
    return config_path.with_suffix(".localkey.json")


def read_local_wrapped_key(root_mod: Any, config_path: Path, fingerprint: str) -> str:
    cache_path = local_key_cache_path(config_path)
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
        return dpapi_unprotect(root_mod, wrapped).decode("utf-8")
    except Exception:
        return ""


def write_local_wrapped_key(
    root_mod: Any,
    config_path: Path,
    fingerprint: str,
    private_key_pem: str,
) -> None:
    if os.name != "nt":
        return
    wrapped = dpapi_protect(root_mod, str(private_key_pem or "").encode("utf-8"))
    payload = {
        "schemaVersion": 1,
        "fingerprint": str(fingerprint or ""),
        "dpapi": wrapped,
        "updatedAt": root_mod.now_iso(),
    }
    cache_path = local_key_cache_path(config_path)
    cache_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def allowlist_error(
    root_mod: Any,
    *,
    repo: str,
    branch: str,
    path: str,
    normalized: dict[str, str],
    env_map: dict[str, str],
) -> str:
    allowed_repo = str(
        env_map.get(root_mod.SYNC_ALLOWED_REPO_ENV) or normalized.get("allowedRepo") or ""
    ).strip()
    allowed_branch = str(
        env_map.get(root_mod.SYNC_ALLOWED_BRANCH_ENV) or normalized.get("allowedBranch") or ""
    ).strip()
    allowed_prefix = str(
        env_map.get(root_mod.SYNC_ALLOWED_PATH_PREFIX_ENV)
        or normalized.get("allowedPathPrefix")
        or ""
    ).strip()
    if allowed_repo and str(repo or "").strip().lower() != allowed_repo.lower():
        return f"Blocked by allowlist: repo must be {allowed_repo}."
    if allowed_branch and str(branch or "").strip() != allowed_branch:
        return f"Blocked by allowlist: branch must be {allowed_branch}."
    if allowed_prefix and not str(path or "").strip().startswith(allowed_prefix):
        return f"Blocked by allowlist: path must start with {allowed_prefix}."
    return ""


def parse_iso(value: Any) -> datetime | None:
    return parse_iso_from_utils(value)


def init_github_app_auth(auth: Any, packaged_config: Any) -> None:
    auth.packaged_config = packaged_config
    auth._token = ""
    auth._token_expires_at = None
    auth._lock = threading.RLock()


def github_app_auth_token_is_fresh(root_mod: Any, auth: Any) -> bool:
    if not auth._token or not auth._token_expires_at:
        return False
    seconds_remaining = (auth._token_expires_at - root_mod.now_utc()).total_seconds()
    return bool(seconds_remaining > root_mod.INSTALLATION_TOKEN_REFRESH_SKEW_SECONDS)


def github_app_auth_refresh_installation_token(
    root_mod: Any,
    auth: Any,
    *,
    opener: Callable[..., Any],
) -> str:
    jwt_token = root_mod.build_app_jwt(
        auth.packaged_config.app_id, auth.packaged_config.private_key_pem
    )
    url = (
        f"{root_mod._github_api_base()}/app/installations/"
        f"{root_mod.quote(auth.packaged_config.installation_id, safe='')}/access_tokens"
    )
    status, payload, _headers = root_mod._request_raw_json(
        method="POST",
        url=url,
        headers=root_mod._github_json_headers(f"Bearer {jwt_token}"),
        timeout_s=root_mod.DEFAULT_TIMEOUT_S,
        payload={},
        opener=opener,
    )
    if status >= 400:
        message = str(
            payload.get("message") or f"GitHub App token request failed with HTTP {status}"
        )
        raise RuntimeError(message)
    token = str(payload.get("token") or "").strip()
    expires_at = parse_iso(payload.get("expires_at"))
    if not token or not expires_at:
        raise RuntimeError("GitHub App token response missing token or expires_at")
    auth._token = token
    auth._token_expires_at = expires_at
    return token


def github_app_auth_get_installation_token(
    root_mod: Any,
    auth: Any,
    *,
    opener: Callable[..., Any],
    force_refresh: bool = False,
) -> str:
    with auth._lock:
        if not force_refresh and github_app_auth_token_is_fresh(root_mod, auth):
            return str(auth._token)
        return github_app_auth_refresh_installation_token(root_mod, auth, opener=opener)


def auth_manager_key(config: Any) -> str:
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


def get_auth_manager(root_mod: Any, config: Any) -> Any:
    root_mod.validate_sync_config(config)
    key = auth_manager_key(config)
    with _AUTH_MANAGER_LOCK:
        manager = _AUTH_MANAGER.get(key)
        if manager is None:
            manager = root_mod.GitHubAppAuth(config.packaged_config)
            _AUTH_MANAGER[key] = manager
        return manager


def rate_limit_retry_after_seconds(
    root_mod: Any, headers: dict[str, str], payload: dict[str, Any]
) -> int:
    retry_after = str((headers or {}).get("retry-after") or "").strip()
    if retry_after.isdigit():
        return max(1, min(int(root_mod.RATE_LIMIT_BACKOFF_MAX_S), int(retry_after)))
    reset_raw = str((headers or {}).get("x-ratelimit-reset") or "").strip()
    if reset_raw.isdigit():
        reset_at = int(reset_raw)
        delta = reset_at - int(root_mod.now_utc().timestamp())
        if delta > 0:
            return max(1, min(int(root_mod.RATE_LIMIT_BACKOFF_MAX_S), delta))
    msg = str((payload or {}).get("message") or "").lower()
    if "secondary rate limit" in msg:
        return min(
            int(root_mod.RATE_LIMIT_BACKOFF_MAX_S),
            int(root_mod.RATE_LIMIT_BACKOFF_BASE_S) * 5,
        )
    return int(root_mod.RATE_LIMIT_BACKOFF_BASE_S)


def rate_limit_preflight(root_mod: Any) -> None:
    with _RATE_LIMIT_LOCK:
        now = root_mod.now_utc()
        until = _RATE_LIMIT_STATE.get("until")
        if isinstance(until, datetime) and now < until:
            set_runtime_state(
                root_mod,
                root_mod.RUNTIME_STATE_RATE_LIMITED,
                f"Sync rate limited locally until {until.isoformat()}",
                until=until,
            )
            raise root_mod.SyncOperationError(
                root_mod.RUNTIME_STATE_RATE_LIMITED, "Sync temporarily rate limited."
            )
        calls = [
            item
            for item in (_RATE_LIMIT_STATE.get("calls") or [])
            if isinstance(item, datetime)
            and (now - item).total_seconds() < root_mod.RATE_LIMIT_WINDOW_S
        ]
        if len(calls) >= root_mod.RATE_LIMIT_MAX_REQUESTS:
            strike = int(_RATE_LIMIT_STATE.get("strike") or 0) + 1
            wait_s = min(
                root_mod.RATE_LIMIT_BACKOFF_MAX_S,
                root_mod.RATE_LIMIT_BACKOFF_BASE_S * (2 ** max(0, strike - 1)),
            )
            cooldown = now + timedelta(seconds=wait_s)
            _RATE_LIMIT_STATE.update({"calls": calls, "strike": strike, "until": cooldown})
            set_runtime_state(
                root_mod,
                root_mod.RUNTIME_STATE_RATE_LIMITED,
                f"Sync rate limited locally for {wait_s}s",
                until=cooldown,
            )
            raise root_mod.SyncOperationError(
                root_mod.RUNTIME_STATE_RATE_LIMITED, "Sync temporarily rate limited."
            )
        calls.append(now)
        _RATE_LIMIT_STATE["calls"] = calls


def rate_limit_note_response(
    root_mod: Any,
    status: int,
    headers: dict[str, str],
    payload: dict[str, Any],
) -> None:
    if int(status or 0) in {429, 403}:
        message = str((payload or {}).get("message") or "").lower()
        if int(status or 0) == 429 or "rate limit" in message:
            retry_s = rate_limit_retry_after_seconds(root_mod, headers, payload)
            until = root_mod.now_utc() + timedelta(seconds=retry_s)
            with _RATE_LIMIT_LOCK:
                strike = int(_RATE_LIMIT_STATE.get("strike") or 0) + 1
                _RATE_LIMIT_STATE["strike"] = strike
                _RATE_LIMIT_STATE["until"] = until
            set_runtime_state(
                root_mod,
                root_mod.RUNTIME_STATE_RATE_LIMITED,
                f"GitHub API rate limited sync for {retry_s}s",
                until=until,
            )
            raise root_mod.SyncOperationError(
                root_mod.RUNTIME_STATE_RATE_LIMITED, "GitHub rate limit reached for sync."
            )
    with _RATE_LIMIT_LOCK:
        strike = int(_RATE_LIMIT_STATE.get("strike") or 0)
        if strike > 0:
            _RATE_LIMIT_STATE["strike"] = max(0, strike - 1)
        until = _RATE_LIMIT_STATE.get("until")
        if isinstance(until, datetime) and root_mod.now_utc() >= until:
            _RATE_LIMIT_STATE["until"] = None
    clear_runtime_state(root_mod, root_mod.RUNTIME_STATE_RATE_LIMITED)


def request_json(
    root_mod: Any,
    *,
    method: str,
    url: str,
    config: Any,
    timeout_s: int,
    payload: dict[str, Any] | None = None,
    opener: Callable[..., Any],
    allow_retry_401: bool = True,
) -> tuple[int, dict[str, Any], dict[str, str]]:
    rate_limit_preflight(root_mod)
    manager = get_auth_manager(root_mod, config)
    token = manager.get_installation_token(opener=opener)
    status, body, headers = root_mod._request_raw_json(
        method=method,
        url=url,
        headers=root_mod._github_json_headers(f"Bearer {token}"),
        timeout_s=timeout_s,
        payload=payload,
        opener=opener,
    )
    rate_limit_note_response(root_mod, status, headers, body)
    if status == 401 and allow_retry_401:
        token = manager.get_installation_token(opener=opener, force_refresh=True)
        status, body, headers = root_mod._request_raw_json(
            method=method,
            url=url,
            headers=root_mod._github_json_headers(f"Bearer {token}"),
            timeout_s=timeout_s,
            payload=payload,
            opener=opener,
        )
        rate_limit_note_response(root_mod, status, headers, body)
    return status, body, headers
