from __future__ import annotations

"""Identity and family helpers for discovery candidates."""

from typing import Any
from urllib.parse import urlparse

from .io_runtime import endpoint_url
from .scoring import clean_token


def adapter_domain_fingerprint(candidate: dict[str, Any]) -> str:
    adapter = str(candidate.get("adapter") or "").strip().lower()
    url = endpoint_url(candidate)
    if not adapter or not url:
        return ""
    try:
        parsed = urlparse(url)
        domain = (parsed.netloc or "").lower().strip()
        path = (parsed.path or "").rstrip("/").lower()
    except ValueError:
        domain = ""
        path = ""
    if not domain:
        return ""
    return f"{adapter}:{domain}:{path}"


def root_domain(host: str) -> str:
    token = str(host or "").strip().lower()
    if not token:
        return ""
    parts = [part for part in token.split(".") if part]
    if len(parts) >= 2:
        return ".".join(parts[-2:])
    return token


def queue_family_key(candidate: dict[str, Any]) -> str:
    url = endpoint_url(candidate) or str(candidate.get("careersUrl") or "")
    try:
        host = (urlparse(url).netloc or "").lower()
    except ValueError:
        host = ""
    adapter = str(candidate.get("adapter") or "").strip().lower()
    studio = clean_token(str(candidate.get("studio") or candidate.get("name") or ""))
    domain_key = root_domain(host) or studio or "unknown"
    return f"{adapter}:{domain_key}"
