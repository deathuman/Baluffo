from __future__ import annotations

from urllib.parse import urlparse


def normalized_host(value: object) -> str:
    return str(value or "").strip().lower().rstrip(".")


def host_matches_domain(host: object, domain: object) -> bool:
    normalized = normalized_host(host)
    normalized_domain = normalized_host(domain).removeprefix(".")
    return bool(
        normalized
        and normalized_domain
        and (normalized == normalized_domain or normalized.endswith(f".{normalized_domain}"))
    )


def host_matches_subdomain(host: object, domain: object) -> bool:
    normalized = normalized_host(host)
    normalized_domain = normalized_host(domain).removeprefix(".")
    return bool(
        normalized
        and normalized_domain
        and normalized != normalized_domain
        and normalized.endswith(f".{normalized_domain}")
    )


def host_matches_domain_pattern(host: object, pattern: object) -> bool:
    text = normalized_host(pattern)
    if text.startswith("."):
        return host_matches_subdomain(host, text)
    return host_matches_domain(host, text)


def host_matches_any_domain_pattern(host: object, patterns: object) -> bool:
    if not isinstance(patterns, (list, tuple, set, frozenset)):
        return False
    return any(host_matches_domain_pattern(host, pattern) for pattern in patterns)


def url_host(url: object) -> str:
    try:
        return normalized_host(urlparse(str(url or "").strip()).hostname)
    except ValueError:
        return ""


def url_host_matches_domain(url: object, domain: object) -> bool:
    return host_matches_domain(url_host(url), domain)
