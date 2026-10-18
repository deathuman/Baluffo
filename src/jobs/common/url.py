"""URL helpers shared across jobs fetching."""

from __future__ import annotations

import hashlib
import ipaddress
import re
from functools import lru_cache
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse
from urllib.request import Request, urlopen

from src.jobs.common.config import (
    DEFAULT_REDIRECT_HEADERS,
    DEFAULT_TIMEOUT_S,
    SUPPORTED_REDIRECT_HOSTS,
)
from src.jobs.common.greenhouse_identity import greenhouse_job_identity_from_url
from src.jobs.text_utils import (
    LEGACY_URL_NORMALIZATION,
    URL_NORMALIZATION_CACHE_SIZE,
    clean_text,
    normalize_url,
)


def _canonical_fingerprint_seed_from_normalized(normalized: str) -> str:
    """The dedup seed of an already-normalised URL, without normalising it again."""
    if not normalized:
        return ""
    parsed = urlparse(normalized)
    host = parsed.netloc.lower()
    path = parsed.path or "/"
    query = parsed.query

    if greenhouse_identity := greenhouse_job_identity_from_url(normalized):
        return greenhouse_identity

    if host in {"jobs.smartrecruiters.com", "api.smartrecruiters.com"}:
        jobs_match = re.match(r"^/([^/]+)/(\d+)(?:-[^/]+)?$", path)
        if jobs_match:
            company_id, posting_id = jobs_match.groups()
            return f"smartrecruiters:{company_id.lower()}:{posting_id}"
        api_match = re.match(r"^/v1/companies/([^/]+)/postings/(\d+)$", path)
        if api_match:
            company_id, posting_id = api_match.groups()
            return f"smartrecruiters:{company_id.lower()}:{posting_id}"

    if host == "www.personio.de" and re.match(r"^/job/\d+$", path):
        pairs = []
        for key, values in parse_qs(query, keep_blank_values=True).items():
            if key.lower() in {"language", "lang"}:
                continue
            for value in values:
                pairs.append((key, value))
        pairs.sort(key=lambda item: (item[0].lower(), item[1]))
        scoped_query = urlencode(pairs, doseq=True)
        return urlunparse((parsed.scheme.lower(), host, path, "", scoped_query, ""))

    return normalized


def _canonical_url_fingerprint_seed_uncached(url: str) -> str:
    return _canonical_fingerprint_seed_from_normalized(normalize_url(url))


_canonical_url_fingerprint_seed_cached = lru_cache(maxsize=URL_NORMALIZATION_CACHE_SIZE)(
    _canonical_url_fingerprint_seed_uncached
)


def canonical_url_fingerprint_seed(url: Any) -> str:
    """This URL's dedup seed, memoised on the cleaned URL.

    The key is ``clean_text(url)`` -- the same value the uncached body already derives through
    ``normalize_url`` -- so a cache hit returns exactly the string the body would have built, and
    the seed work (a ``urlparse`` plus a host-dispatch chain that reaches an ``ipaddress`` check)
    runs once per distinct URL instead of once per call.
    """
    if LEGACY_URL_NORMALIZATION:
        return _canonical_url_fingerprint_seed_uncached(clean_text(url))
    return _canonical_url_fingerprint_seed_cached(clean_text(url))


def fingerprint_url(url: Any) -> str:
    seed = canonical_url_fingerprint_seed(url)
    return hashlib.sha1(seed.encode("utf-8")).hexdigest() if seed else ""


def fingerprint_normalized_url(normalized: str) -> str:
    """``fingerprint_url`` for an already-normalised URL (no second normalisation).

    ``fingerprint_url`` normalises before seeding; a caller that already holds the
    normalised form -- such as the identity preflight, which needs publicity and the
    fingerprint of the same link -- can hand it straight here instead of paying for a
    second ``normalize_url`` lookup and its ``clean_text`` key.
    """
    seed = _canonical_fingerprint_seed_from_normalized(normalized)
    return hashlib.sha1(seed.encode("utf-8")).hexdigest() if seed else ""


def _is_public_normalized_url(normalized: str) -> bool:
    """Publicity of an already-normalised URL, without normalising it again."""
    if not normalized:
        return False
    try:
        parsed = urlparse(normalized)
        host = (parsed.hostname or "").casefold().rstrip(".")
        if not host or parsed.username or parsed.password:
            return False
        if host in {"localhost", "localhost.localdomain"} or host.endswith(".local"):
            return False
        try:
            return ipaddress.ip_address(host).is_global
        except ValueError:
            return True
    except ValueError:
        return False


def _is_public_job_url_uncached(url: str) -> bool:
    return _is_public_normalized_url(normalize_url(url))


_is_public_job_url_cached = lru_cache(maxsize=URL_NORMALIZATION_CACHE_SIZE)(
    _is_public_job_url_uncached
)


def is_public_normalized_url(normalized: str) -> bool:
    """``is_public_job_url`` for an already-normalised URL (no second normalisation).

    Paired with ``fingerprint_normalized_url`` for callers that hold the normalised form
    and would otherwise normalise the same link twice for two answers about it.
    """
    return _is_public_normalized_url(normalized)


def is_public_job_url(url: Any) -> bool:
    """Whether this URL is a publicly reachable job link, memoised on the cleaned URL.

    The key is ``clean_text(url)`` for the same reason as ``canonical_url_fingerprint_seed``: it is
    the value the body already derives first. The answer is a ``bool``, so a hit cannot be mutated
    by anyone; the body's per-host work -- including an ``ipaddress.ip_address`` call that raises
    for every hostname -- runs once per distinct URL.
    """
    if LEGACY_URL_NORMALIZATION:
        return _is_public_job_url_uncached(clean_text(url))
    return _is_public_job_url_cached(clean_text(url))


def is_supported_redirect_url(url: Any) -> bool:
    normalized = normalize_url(url)
    if not normalized:
        return False
    parsed = urlparse(normalized)
    return parsed.netloc.lower() in SUPPORTED_REDIRECT_HOSTS and parsed.path.startswith("/rd/")


def resolve_supported_redirect_url(url: Any, *, timeout_s: int = DEFAULT_TIMEOUT_S) -> str:
    normalized = normalize_url(url)
    if not is_supported_redirect_url(normalized):
        return normalized
    last_error: Exception | None = None
    for method in ("HEAD", "GET"):
        request = Request(normalized, headers=DEFAULT_REDIRECT_HEADERS, method=method)
        try:
            with urlopen(request, timeout=max(1, int(timeout_s or DEFAULT_TIMEOUT_S))) as response:
                resolved = normalize_url(response.geturl())
                return resolved or normalized
        except HTTPError as exc:
            last_error = exc
            if method == "HEAD" and int(getattr(exc, "code", 0) or 0) in {
                400,
                403,
                405,
                429,
                500,
                501,
                503,
            }:
                continue
            return normalized
        except (URLError, ValueError) as exc:
            last_error = exc
            if method == "HEAD":
                continue
            break
    _ = last_error
    return normalized
