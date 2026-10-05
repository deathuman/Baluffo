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


# Providers that host many studios on one registrable domain. The tenant lives in the
# host label and the path, never in the domain, so a root-domain family key merges every
# tenant on the platform into a single family.
MULTI_TENANT_PROVIDER_DOMAINS: dict[str, str] = {
    "myworkdayjobs.com": "workday",
    "workday.com": "workday",
    "bamboohr.com": "bamboohr",
}


def multi_tenant_provider(host: str) -> str:
    """The platform family that serves this host, else "".

    ``*.myworkdayjobs.com`` and ``*.bamboohr.com`` each serve hundreds of unrelated
    studios, so the host decides the family and the row's declared adapter does not.
    A Workday board is routinely registered as ``static``; that is a registration detail,
    not a different platform, and letting it change the answer is how a static row stopped
    being recognised as tenant-scoped.

    Whether a board is a duplicate is a question about ``(host, tenant)``, never about the
    platform.
    """
    label = str(host or "").strip().lower().split(":")[0]
    if not label:
        return ""
    parts = [part for part in label.split(".") if part]
    for index in range(len(parts) - 1, 0, -1):
        family = MULTI_TENANT_PROVIDER_DOMAINS.get(".".join(parts[index:]))
        if family:
            return family
    return ""


def tenant_host_label(host: str) -> str:
    """The subdomain label that distinguishes tenants on a multi-tenant provider.

    ``nvidia.wd5.myworkdayjobs.com`` -> ``nvidia.wd5``. Returns "" for a bare platform
    host, which carries no tenant and so must not be treated as one.
    """
    label = str(host or "").strip().lower().split(":")[0]
    parts = [part for part in label.split(".") if part]
    family = ""
    for index in range(len(parts) - 1, 0, -1):
        candidate = ".".join(parts[index:])
        if candidate in MULTI_TENANT_PROVIDER_DOMAINS:
            family = candidate
            break
    if not family or len(parts) <= len(family.split(".")):
        return ""
    return ".".join(parts[: len(parts) - len(family.split("."))])


def tenant_path(url: str) -> str:
    """The board's own path segment on a multi-tenant provider.

    Workday puts the site in the path (``/NVIDIAExternalCareerSite``) and the tenant in
    the host; BambooHR does the reverse. Both are needed, because two boards can share a
    host label and differ only by path, or share a path and differ only by host.
    """
    try:
        path = (urlparse(str(url or "")).path or "").strip().lower().strip("/")
    except ValueError:
        return ""
    return "/".join(part for part in path.split("/") if part)


def board_identity_key(candidate: dict[str, Any]) -> str:
    """``(host, tenant)`` for a board, as one comparable string.

    Identity is host + tenant. On a multi-tenant provider the root domain is shared by
    every studio on the platform, so using it merges NVIDIA with Aristocrat and every
    later board with whichever was indexed first.
    """
    url = endpoint_url(candidate) or str(candidate.get("careersUrl") or "")
    try:
        host = (urlparse(url).netloc or "").lower()
    except ValueError:
        host = ""
    if multi_tenant_provider(host):
        label = tenant_host_label(host)
        path = tenant_path(url)
        if label or path:
            return f"{host}|{label}|{path}"
    return host


def queue_family_key(candidate: dict[str, Any]) -> str:
    url = endpoint_url(candidate) or str(candidate.get("careersUrl") or "")
    try:
        host = (urlparse(url).netloc or "").lower()
    except ValueError:
        host = ""
    adapter = str(candidate.get("adapter") or "").strip().lower()
    studio = clean_token(str(candidate.get("studio") or candidate.get("name") or ""))
    # A multi-tenant provider needs the tenant in the key. Falling back to the root domain
    # here is what made every Workday board a "family match" for every other one, which
    # cost -8 rank, then a duplicate flag, then promotion.
    if multi_tenant_provider(host):
        identity = board_identity_key(candidate)
        if "|" in identity:
            return f"{adapter}:{identity}"
    domain_key = root_domain(host) or studio or "unknown"
    return f"{adapter}:{domain_key}"
