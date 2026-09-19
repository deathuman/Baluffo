"""Browser-fallback environment-error token table.

AI boundary owns: the canonical browser-fallback environment-error token set and its substring scan.
AI boundary implement in: this file for token membership only; callers own their text normalization.
AI boundary search before contracts: jobs browser fallback policy, bridge source checking, and fallback tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused browser fallback tests.

This leaf is deliberately dependency-free and lives in ``src/shared`` because
both ``src.jobs`` and ``src.bridge`` consume it: ``src.jobs.browser_fallback``
transitively imports ``src.bridge.source_check_http``, so a jobs-side home for
this table would close an import cycle.
"""

from __future__ import annotations

BROWSER_FALLBACK_ENVIRONMENT_ERROR_TOKENS = (
    "browser fallback unavailable",
    "playwright is not installed",
    "spawn eperm",
    "permission denied",
    "access is denied",
    "operation not permitted",
    "failed to launch browser",
    "cannot launch browser",
    "could not find browser",
    "browser_type.launch",
    "executable doesn't exist",
    "executable does not exist",
    "worker spawn blocked",
    "write epipe",
    "broken pipe",
    "pipetransport",
    "target closed",
    "transport closed",
    "connection closed",
    "browser has been closed",
)


def matches_browser_fallback_environment_error(normalized_text: str) -> bool:
    """Scan already-normalized text for browser-fallback environment tokens.

    Callers own their normalization (lowercasing and any text cleanup), because
    the bridge source checker and the jobs adapter historically differ there;
    only the token set and the substring scan are shared.
    """
    if not normalized_text:
        return False
    return any(token in normalized_text for token in BROWSER_FALLBACK_ENVIRONMENT_ERROR_TOKENS)
