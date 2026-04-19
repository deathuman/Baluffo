from __future__ import annotations

import json
import os
import ssl
from collections.abc import Callable
from pathlib import Path
from typing import Any
from urllib.error import URLError

from src.shared.github_https import (
    GITHUB_CA_BUNDLE_ENV,
    build_github_ssl_context,
    wrap_github_request_error,
)


def normalize_packaged_payload(module: Any, payload: dict[str, Any]) -> dict[str, str]:
    data = payload if isinstance(payload, dict) else {}
    return {
        "appId": str(data.get("appId") or "").strip(),
        "installationId": str(data.get("installationId") or "").strip(),
        "repo": str(data.get("repo") or "").strip(),
        "branch": str(data.get("branch") or module.DEFAULT_BRANCH).strip() or module.DEFAULT_BRANCH,
        "path": str(data.get("path") or module.DEFAULT_PATH).strip() or module.DEFAULT_PATH,
        "privateKeyPemEnc": str(data.get("privateKeyPemEnc") or "").strip(),
        "privateKeyPem": str(data.get("privateKeyPem") or "").strip(),
        "keySalt": str(data.get("keySalt") or "").strip(),
        "keyDerivation": str(data.get("keyDerivation") or module.KEY_DERIVATION_MACHINE)
        .strip()
        .lower()
        or module.KEY_DERIVATION_MACHINE,
        "embeddedKeyHint": str(data.get("embeddedKeyHint") or "").strip(),
        "embeddedKeyVersion": str(
            data.get("embeddedKeyVersion") or module.EMBEDDED_KEY_VERSION_DEFAULT
        ).strip()
        or module.EMBEDDED_KEY_VERSION_DEFAULT,
        "allowedRepo": str(data.get("allowedRepo") or "").strip(),
        "allowedBranch": str(data.get("allowedBranch") or "").strip(),
        "allowedPathPrefix": str(data.get("allowedPathPrefix") or "").strip(),
    }


def load_packaged_sync_config(module: Any, *, env: dict[str, str] | None = None) -> Any | None:
    env_map = env if isinstance(env, dict) else os.environ
    path_raw = str(
        env_map.get(module.PACKAGED_SYNC_CONFIG_ENV) or module.DEFAULT_PACKAGED_SYNC_CONFIG_PATH
    ).strip()
    config_path = Path(path_raw).expanduser().resolve()
    if not config_path.exists():
        return None
    try:
        payload = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    normalize_payload = getattr(module, "_normalize_packaged_payload", None)
    if callable(normalize_payload):
        normalized = normalize_payload(payload if isinstance(payload, dict) else {})
    else:
        normalized = normalize_packaged_payload(
            module, payload if isinstance(payload, dict) else {}
        )
    private_key_pem = normalized["privateKeyPem"]
    key_derivation = normalized["keyDerivation"]
    decryption_error = ""
    policy_error = module._allowlist_error(
        repo=normalized["repo"],
        branch=normalized["branch"],
        path=normalized["path"],
        normalized=normalized,
        env_map=env_map,
    )
    fingerprint = module._local_key_cache_fingerprint(normalized)
    if not private_key_pem:
        private_key_pem = module._read_local_wrapped_key(config_path, fingerprint)
    if not private_key_pem and normalized["privateKeyPemEnc"] and normalized["keySalt"]:
        try:
            if key_derivation == module.KEY_DERIVATION_PASSPHRASE:
                passphrase = str(env_map.get(module.PACKAGED_SYNC_PASSPHRASE_ENV) or "").strip()
                if not passphrase:
                    raise RuntimeError(
                        f"Missing {module.PACKAGED_SYNC_PASSPHRASE_ENV} for passphrase-encrypted sync key."
                    )
                private_key_pem = module.decrypt_private_key_pem_with_passphrase(
                    normalized["privateKeyPemEnc"],
                    salt_b64=normalized["keySalt"],
                    app_id=normalized["appId"],
                    installation_id=normalized["installationId"],
                    passphrase=passphrase,
                )
            elif key_derivation == module.KEY_DERIVATION_EMBEDDED:
                passphrase = str(env_map.get(module.PACKAGED_SYNC_PASSPHRASE_ENV) or "").strip()
                if not passphrase:
                    passphrase = module.build_embedded_passphrase(
                        hint=normalized["embeddedKeyHint"],
                        version=normalized["embeddedKeyVersion"],
                    )
                private_key_pem = module.decrypt_private_key_pem_with_passphrase(
                    normalized["privateKeyPemEnc"],
                    salt_b64=normalized["keySalt"],
                    app_id=normalized["appId"],
                    installation_id=normalized["installationId"],
                    passphrase=passphrase,
                )
            elif key_derivation in {"", module.KEY_DERIVATION_MACHINE}:
                key_derivation = module.KEY_DERIVATION_MACHINE
                private_key_pem = module.decrypt_private_key_pem(
                    normalized["privateKeyPemEnc"],
                    salt_b64=normalized["keySalt"],
                    app_id=normalized["appId"],
                    installation_id=normalized["installationId"],
                )
            elif key_derivation == module.KEY_DERIVATION_PLAINTEXT:
                private_key_pem = normalized["privateKeyPem"]
            else:
                raise RuntimeError(f"Unsupported keyDerivation mode: {key_derivation}")
        except Exception as exc:  # noqa: BLE001
            if key_derivation == module.KEY_DERIVATION_MACHINE:
                decryption_error = (
                    "Packaged GitHub App key is machine-bound and cannot be decrypted on this "
                    "machine. Rebuild the packaged sync config with embedded or passphrase derivation."
                )
            else:
                decryption_error = str(exc)
    if private_key_pem:
        try:
            module._write_local_wrapped_key(config_path, fingerprint, private_key_pem)
        except Exception:
            pass
    return module.PackagedGitHubAppConfig(
        app_id=normalized["appId"],
        installation_id=normalized["installationId"],
        repo=normalized["repo"],
        branch=normalized["branch"],
        path=normalized["path"],
        private_key_pem=private_key_pem,
        config_path=str(config_path),
        key_derivation=key_derivation,
        decryption_error=decryption_error,
        policy_error=policy_error,
        allowed_repo=str(normalized.get("allowedRepo") or ""),
        allowed_branch=str(normalized.get("allowedBranch") or ""),
        allowed_path_prefix=str(normalized.get("allowedPathPrefix") or ""),
    )


def resolve_sync_config(
    module: Any, *, settings: dict[str, Any] | None = None, env: dict[str, str] | None = None
) -> Any:
    settings_map = settings if isinstance(settings, dict) else {}
    env_map = env if isinstance(env, dict) else os.environ
    default_enabled = bool(
        module._SECURITY_DEFAULTS["github_app_enabled_default"]
        and module._SYNC_DEFAULTS["local_enabled_default"]
    )
    enabled_raw = settings_map.get("enabled")
    enabled = default_enabled if enabled_raw is None else bool(enabled_raw)
    disabled_reason = ""
    if module._truthy(env_map.get(module.SYNC_DISABLE_ENV)):
        enabled = False
        disabled_reason = f"Sync disabled by {module.SYNC_DISABLE_ENV}."
    packaged_config = load_packaged_sync_config(module, env=env_map)
    repo = packaged_config.repo if packaged_config else str(module._SYNC_DEFAULTS["default_repo"])
    branch = packaged_config.branch if packaged_config else module.DEFAULT_BRANCH
    path = packaged_config.path if packaged_config else module.DEFAULT_PATH
    return module.SyncConfig(
        enabled=enabled,
        repo=repo,
        branch=branch,
        path=path,
        auth_mode="github_app",
        packaged_config=packaged_config,
        timeout_s=module.DEFAULT_TIMEOUT_S,
        disabled_reason=disabled_reason,
    )


def config_status(module: Any, config: Any) -> dict[str, Any]:
    missing: list[str] = []
    message = str(config.disabled_reason or "")
    if not config.packaged_config:
        missing.append("packaged_github_app_config")
    else:
        if config.packaged_config.policy_error:
            missing.append("allowlist")
            message = str(config.packaged_config.policy_error)
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
                f"Expected {module.PACKAGED_SYNC_CONFIG_ENV} or {module.DEFAULT_PACKAGED_SYNC_CONFIG_PATH.name}."
            )
        else:
            message = "Packaged GitHub App config is incomplete."
    runtime_state = module._runtime_state_payload()
    runtime_code = str(runtime_state.get("code") or "").strip().lower()
    if ready and runtime_code in {
        module.RUNTIME_STATE_RATE_LIMITED,
        module.RUNTIME_STATE_REMOTE_CONFLICT,
    }:
        ready = False
        state = runtime_code
        if not message:
            message = str(runtime_state.get("message") or "")
    else:
        state = "ready" if ready else ("disabled" if not config.enabled else "misconfigured")
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
        "runtimeState": runtime_state,
        "keyDerivation": str(
            config.packaged_config.key_derivation if config.packaged_config else ""
        ),
        "allowlist": {
            "repo": str(config.packaged_config.allowed_repo if config.packaged_config else ""),
            "branch": str(config.packaged_config.allowed_branch if config.packaged_config else ""),
            "pathPrefix": str(
                config.packaged_config.allowed_path_prefix if config.packaged_config else ""
            ),
        },
    }


def validate_sync_config(module: Any, config: Any) -> None:
    status = config_status(module, config)
    if not status["ready"]:
        raise module.SyncOperationError(
            str(status.get("state") or "misconfigured"),
            str(status["message"] or "Sync is not configured"),
        )


def request_raw_json(
    module: Any,
    *,
    method: str,
    url: str,
    headers: dict[str, str],
    timeout_s: int,
    payload: dict[str, Any] | None = None,
    opener: Callable[..., Any] | None = None,
) -> tuple[int, dict[str, Any], dict[str, str]]:
    body: bytes | None = None
    if payload is not None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = module.Request(url=url, data=body, method=method.upper(), headers=headers)
    try:
        active_opener = opener or module.urlopen
        uses_default_opener = opener is None or opener is module.urlopen
        if uses_default_opener:
            try:
                ssl_context = build_github_ssl_context(
                    ca_bundle_envs=(module.SYNC_CA_BUNDLE_ENV, GITHUB_CA_BUNDLE_ENV)
                )
            except RuntimeError as exc:
                raise RuntimeError(f"Sync request failed: {exc}") from exc
            response_ctx = active_opener(
                request,
                timeout=timeout_s,
                context=ssl_context,
            )
        else:
            response_ctx = active_opener(request, timeout=timeout_s)
        with response_ctx as response:
            raw = response.read().decode("utf-8")
            parsed = json.loads(raw) if raw else {}
            return (
                int(response.getcode() or 200),
                parsed if isinstance(parsed, dict) else {},
                {key.lower(): str(value) for key, value in response.headers.items()},
            )
    except module.HTTPError as exc:
        raw = exc.read().decode("utf-8") if hasattr(exc, "read") else ""
        parsed = {}
        if raw:
            try:
                candidate = json.loads(raw)
                if isinstance(candidate, dict):
                    parsed = candidate
            except json.JSONDecodeError:
                parsed = {}
        return (
            int(exc.code or 500),
            parsed,
            {key.lower(): str(value) for key, value in (exc.headers or {}).items()},
        )
    except ssl.SSLError as exc:
        raise wrap_github_request_error(exc, prefix="Sync request failed") from exc
    except URLError as exc:
        raise wrap_github_request_error(exc, prefix="Sync request failed") from exc
