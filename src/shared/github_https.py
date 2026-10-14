"""Shared HTTPS context helpers for GitHub and update checks.

AI boundary owns: GitHub HTTPS context construction (CA-bundle env naming and certificate-failure
classification); the context itself is built by `src.shared.tls_https`.
AI boundary implement in: this file for GitHub-specific SSL naming and error wrapping; callers own
request/retry policy.
AI boundary search before contracts: desktop update helpers, GitHub release fetchers, and certificate fallback tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused GitHub HTTPS tests.
"""

from __future__ import annotations

import ssl
from urllib.error import URLError

from src.shared.tls_https import build_verified_ssl_context

GITHUB_CA_BUNDLE_ENV = "BALUFFO_GITHUB_CA_BUNDLE"
SSL_VERIFY_FAILURE_MESSAGE = "SSL certificate verification failed while connecting to GitHub."


def build_github_ssl_context(*, ca_bundle_envs: tuple[str, ...] = ()) -> ssl.SSLContext:
    """Verified context: default certs, the GitHub CA env, any caller env, then certifi."""
    return build_verified_ssl_context(
        ca_bundle_envs=(*ca_bundle_envs, GITHUB_CA_BUNDLE_ENV),
    )


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
