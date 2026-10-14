"""Shared verified TLS context construction, anchored on certifi beside the OS store.

AI boundary owns: verified SSLContext construction (default certs plus optional PEM/certifi anchors).
AI boundary implement in: this file for context construction only; callers own request, retry and
failure-classification policy.
AI boundary search before contracts: GitHub and desktop update helpers, the discovery sync fetchers,
and the jobs lane's shared HTTP fallback.
AI boundary verify: `npm run lint:repo-guardrails` plus focused TLS context tests.

Why this exists: a checkout whose OS cert store holds a stale or reduced chain member fails chain
building with "certificate has expired" for hosts whose served chain is valid. Measured on this
repo's Windows box: 40 of 40 sampled discovery homepages that `urlopen` refused under the OS store
read fine under a certifi-anchored context, and the two anchored shapes agreed on all 40 (see
`docs/snapshots/discovery-sync-trust-anchor-2026-10-12.md`). Python only calls
`load_default_certs()` -- the OS store -- when no `cafile` is passed, which is why the bare sync
`urlopen` sees the failure and `httpx` does not.

The anchor is deliberately a **union**, not a swap: the OS store stays loaded and certifi is added,
so `CERT_REQUIRED` and the hostname check are untouched and a host that verifies today cannot start
failing because of this. `src.jobs.adapters.provider_structured_listing` keeps its own certifi-only
context on purpose -- the OS store poisons chain building for `*.myworkdayjobs.com` rather than
merely failing it -- so that rule is not this one.
"""

from __future__ import annotations

import os
import ssl
from pathlib import Path
from types import ModuleType

certifi: ModuleType | None
try:
    import certifi as _certifi
except ImportError:  # pragma: no cover - optional dependency at runtime
    certifi = None
else:
    certifi = _certifi

_SHARED_CONTEXT: ssl.SSLContext | None = None


def resolve_ca_bundle_override(*, ca_bundle_envs: tuple[str, ...] = ()) -> Path | None:
    """The first existing PEM bundle named by ``ca_bundle_envs``, else ``None``.

    A named env pointing at a missing file is an operator error and raises, so a typo surfaces
    at the call rather than as a silent fallback to the store that is already suspect.
    """
    seen_envs: set[str] = set()
    for env_name in ca_bundle_envs:
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


def _certifi_bundle_path() -> str:
    """certifi's bundle path, or ``""`` when certifi is absent or unreadable.

    An absent or broken certifi is not an error: the context is then exactly today's default
    one. Only an *unexpected* error from certifi propagates, so a real defect is not swallowed
    into a silent verification downgrade.
    """
    try:
        return str(certifi.where() if certifi else "").strip()
    except (AttributeError, OSError):
        return ""


def build_verified_ssl_context(*, ca_bundle_envs: tuple[str, ...] = ()) -> ssl.SSLContext:
    """A fresh verified context: default certs, any named PEM bundle, then certifi."""
    context = ssl.create_default_context()
    context.load_default_certs()
    ca_bundle_path = resolve_ca_bundle_override(ca_bundle_envs=ca_bundle_envs)
    if ca_bundle_path is not None:
        context.load_verify_locations(cafile=str(ca_bundle_path))
    certifi_path = _certifi_bundle_path()
    if certifi_path:
        context.load_verify_locations(cafile=certifi_path)
    return context


def verified_ssl_context() -> ssl.SSLContext:
    """The process-wide context for sync fetch loops, built on first use.

    One context per process: loading the anchors parses ~150 certificates from disk, which is
    worth paying once for a lane that fetches thousands of pages. An ``SSLContext`` is safe to
    share across threads once configured, and every call site only reads it.
    """
    global _SHARED_CONTEXT
    if _SHARED_CONTEXT is None:
        _SHARED_CONTEXT = build_verified_ssl_context()
    return _SHARED_CONTEXT


__all__ = [
    "build_verified_ssl_context",
    "resolve_ca_bundle_override",
    "verified_ssl_context",
]
