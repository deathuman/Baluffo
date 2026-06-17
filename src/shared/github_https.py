from __future__ import annotations

import os
import ssl
from pathlib import Path
from types import ModuleType
from urllib.error import URLError

certifi: ModuleType | None
try:
    import certifi as _certifi
except ImportError:  # pragma: no cover - optional dependency at runtime
    certifi = None
else:
    certifi = _certifi


GITHUB_CA_BUNDLE_ENV = "BALUFFO_GITHUB_CA_BUNDLE"
SSL_VERIFY_FAILURE_MESSAGE = "SSL certificate verification failed while connecting to GitHub."


def _resolve_ca_bundle_override(*, ca_bundle_envs: tuple[str, ...]) -> Path | None:
    seen_envs: set[str] = set()
    for env_name in (*ca_bundle_envs, GITHUB_CA_BUNDLE_ENV):
        name = str(env_name or "").strip()
        if not name or name in seen_envs:
            continue
        seen_envs.add(name)
        raw_path = str(os.environ.get(name) or "").strip()
        if not raw_path:
            continue
        resolved_path = Path(raw_path).expanduser()
        if not resolved_path.is_file():
            raise RuntimeError(
                f"CA bundle not found at {resolved_path} (set {name} to a valid PEM bundle)."
            )
        return resolved_path
    return None


def build_github_ssl_context(*, ca_bundle_envs: tuple[str, ...] = ()) -> ssl.SSLContext:
    context = ssl.create_default_context()
    context.load_default_certs()
    ca_bundle_path = _resolve_ca_bundle_override(ca_bundle_envs=ca_bundle_envs)
    if ca_bundle_path is not None:
        context.load_verify_locations(cafile=str(ca_bundle_path))
    certifi_path = ""
    try:
        certifi_path = str(certifi.where() if certifi else "").strip()
    except (AttributeError, OSError):
        certifi_path = ""
    if certifi_path:
        context.load_verify_locations(cafile=certifi_path)
    return context


def is_certificate_verify_failure(exc: BaseException) -> bool:
    if isinstance(exc, ssl.SSLError):
        return True
    if isinstance(exc, URLError):
        reason = getattr(exc, "reason", None)
        if isinstance(reason, ssl.SSLError):
            return True
        if "certificate verify failed" in str(exc).lower():
            return True
    return False


def wrap_github_request_error(exc: BaseException, *, prefix: str) -> RuntimeError:
    if is_certificate_verify_failure(exc):
        return RuntimeError(f"{prefix}: {SSL_VERIFY_FAILURE_MESSAGE} Original error: {exc}")
    return RuntimeError(f"{prefix}: {exc}")


__all__ = [
    "GITHUB_CA_BUNDLE_ENV",
    "SSL_VERIFY_FAILURE_MESSAGE",
    "build_github_ssl_context",
    "is_certificate_verify_failure",
    "wrap_github_request_error",
]
