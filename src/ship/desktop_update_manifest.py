"""Pure desktop update manifest helpers.

This module is safe for build tooling: it does not depend on desktop_update root binding.
"""

from __future__ import annotations

import base64
import json
from typing import Any

try:
    from cryptography.hazmat.primitives.asymmetric.ed25519 import (
        Ed25519PrivateKey as _Ed25519SigningClass,
    )
    from cryptography.hazmat.primitives.asymmetric.ed25519 import (
        Ed25519PublicKey as _Ed25519VerifierClass,
    )
except Exception:  # noqa: BLE001
    Ed25519SigningClass: Any = None
    Ed25519VerifierClass: Any = None
else:
    Ed25519SigningClass = _Ed25519SigningClass
    Ed25519VerifierClass = _Ed25519VerifierClass


DESKTOP_UPDATE_SCHEMA_VERSION = 1
DESKTOP_UPDATE_MANIFEST_ASSET = "baluffo-desktop-update-manifest.json"
DESKTOP_UPDATE_CHANNEL = "stable"
DESKTOP_UPDATER_VERSION = "2.0.1"
PUBLIC_KEYS_FILE = "desktop-update-public-keys.json"


def sort_json(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: sort_json(value[key]) for key in sorted(value)}
    if isinstance(value, list):
        return [sort_json(item) for item in value]
    return value


def canonical_manifest_bytes(manifest: dict[str, Any]) -> bytes:
    payload = {key: value for key, value in dict(manifest).items() if key != "signature"}
    canonical = sort_json(payload)
    return json.dumps(canonical, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def sign_manifest(
    manifest: dict[str, Any],
    private_key_bytes: bytes,
    *,
    private_key_cls: Any | None = None,
) -> str:
    key_cls = Ed25519SigningClass if private_key_cls is None else private_key_cls
    if key_cls is None:
        raise RuntimeError("Ed25519 signing is unavailable in this runtime.")
    key = key_cls.from_private_bytes(private_key_bytes)
    signature = key.sign(canonical_manifest_bytes(manifest))
    return base64.b64encode(signature).decode("ascii")


def verify_manifest_signature(
    manifest: dict[str, Any],
    *,
    public_keys: dict[str, bytes],
    public_key_cls: Any | None = None,
) -> None:
    key_cls = Ed25519VerifierClass if public_key_cls is None else public_key_cls
    if key_cls is None:
        raise RuntimeError("Ed25519 verification is unavailable in this runtime.")
    key_id = str(manifest.get("key_id") or "").strip()
    if not key_id:
        raise ValueError("Desktop manifest key_id is required.")
    public_key_bytes = public_keys.get(key_id)
    if not public_key_bytes:
        raise ValueError(f"Desktop manifest key_id is unknown: {key_id}")
    signature_b64 = str(manifest.get("signature") or "").strip()
    if not signature_b64:
        raise ValueError("Desktop manifest signature is required.")
    signature = base64.b64decode(signature_b64)
    public_key = key_cls.from_public_bytes(public_key_bytes)
    public_key.verify(signature, canonical_manifest_bytes(manifest))


__all__ = [
    "DESKTOP_UPDATE_MANIFEST_ASSET",
    "DESKTOP_UPDATE_CHANNEL",
    "DESKTOP_UPDATE_SCHEMA_VERSION",
    "DESKTOP_UPDATER_VERSION",
    "Ed25519SigningClass",
    "Ed25519VerifierClass",
    "PUBLIC_KEYS_FILE",
    "canonical_manifest_bytes",
    "sign_manifest",
    "sort_json",
    "verify_manifest_signature",
]
