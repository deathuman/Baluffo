"""URL helpers shared across jobs fetching."""

from __future__ import annotations

import hashlib
import re
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
from src.jobs.text_utils import normalize_url


def canonical_url_fingerprint_seed(url: Any) -> str:
    normalized = normalize_url(url)
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


def fingerprint_url(url: Any) -> str:
    seed = canonical_url_fingerprint_seed(url)
    return hashlib.sha1(seed.encode("utf-8")).hexdigest() if seed else ""


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
