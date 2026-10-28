from __future__ import annotations

import functools
from urllib.parse import urlparse

# Host and domain-pattern normalization is the homepage-analysis phase's hottest leaf: on a captured
# production arm (digest `66165df36a10c023`) it ran 29.2M times over 28.2k distinct strings, because
# every provider admission re-normalizes the same host once per pattern. Memoizing it is
# output-identical and saves 27.2 ns a call, which is 0.79 s over that phase's 29.19M calls
# (`_out/perf-runs/discovery/homepage-analysis-memos-20261028/`); the phase replay itself could not
# resolve an effect that size, so the per-call reading is the evidence. Bounded because a
# long-lived app process sees every host it ever audits, and keyed on the text the transform would
# have produced so no input type can make it raise where the uncached call did not.
_NORMALIZED_HOST_CACHE_MAXSIZE = 32768


@functools.lru_cache(maxsize=_NORMALIZED_HOST_CACHE_MAXSIZE)
def _normalized_host_text(text: str) -> str:
    return text.strip().lower().rstrip(".")


def normalized_host(value: object) -> str:
    return _normalized_host_text(value if isinstance(value, str) else str(value or ""))


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
