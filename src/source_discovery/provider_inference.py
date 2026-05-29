from __future__ import annotations

from collections.abc import Callable
from typing import Any
from urllib.parse import ParseResult, urlparse

from src.url_hosts import host_matches_any_domain_pattern, host_matches_domain

from .scoring import careers_keyword_count, clean_token, studio_domain_match

PROVIDER_DISPLAY_NAMES = {
    "ashby": "Ashby",
    "bamboohr": "BambooHR",
    "breezy": "Breezy",
    "greenhouse": "Greenhouse",
    "jazzhr": "JazzHR",
    "lever": "Lever",
    "oracle_hcm": "Oracle HCM",
    "personio": "Personio",
    "pinpoint": "Pinpoint",
    "recruitee": "Recruitee",
    "smartrecruiters": "SmartRecruiters",
    "teamtailor": "Teamtailor",
    "workable": "Workable",
    "workday": "Workday",
}

_HOST_DOMAIN_PATTERNS = (
    ("greenhouse", ("boards.greenhouse.io", "jobs.greenhouse.io", "boards-api.greenhouse.io")),
    ("ashby", ("jobs.ashbyhq.com",)),
    ("bamboohr", ("bamboohr.com", ".bamboohr.com")),
    ("breezy", (".breezy.hr",)),
    ("jazzhr", (".applytojob.com",)),
    ("recruitee", (".recruitee.com",)),
    ("pinpoint", (".pinpointhq.com",)),
    ("workable", ("apply.workable.com", ".workable.com")),
    ("teamtailor", (".teamtailor.com",)),
    ("personio", (".jobs.personio.de",)),
    ("workday", (".myworkdayjobs.com",)),
)


def _is_oraclecloud_host(host: str) -> bool:
    return host_matches_domain(host, "oraclecloud.com")


def _is_oracle_hcm_candidate_path(path: str) -> bool:
    tokens = [piece.lower() for piece in _path_tokens(path)]
    if "hcmui" not in tokens or "candidateexperience" not in tokens:
        return False
    if "sites" not in tokens or not tokens:
        return False
    return tokens[-1] == "jobs"


# ATS HTML content signatures used by infer_provider_adapter() fallback.
# Mirrors _ATS_HTML_SIGNATURES in web_search_candidates.py:
# only adapters whose builders are runtime-flexible or safely return None.
_ATS_HTML_FALLBACK: list[tuple[str, str]] = [
    ("bamboohr", "bamboohr"),
    ("teamtailor", "teamtailor"),
    ("workday", "myworkdayjobs"),
    ("workday", "workday"),
    ("smartrecruiters", "smartrecruiters"),
]


def _html_matches_any_provider(html: str) -> str | None:
    html_lower = html.lower()
    for adapter, signature in _ATS_HTML_FALLBACK:
        if signature in html_lower:
            return adapter
    return None


def infer_provider_adapter(host: str, path: str, html: str | None = None) -> str | None:
    if _is_oraclecloud_host(host) and _is_oracle_hcm_candidate_path(path):
        return "oracle_hcm"
    for adapter, patterns in _HOST_DOMAIN_PATTERNS:
        if host_matches_any_domain_pattern(host, patterns):
            return adapter
    if (host == "api.lever.co" and "/v0/postings/" in path) or (
        host_matches_domain(host, "lever.co") and host != "api.lever.co"
    ):
        return "lever"
    if (host == "api.smartrecruiters.com" and "/companies/" in path) or (
        host == "jobs.smartrecruiters.com"
    ):
        return "smartrecruiters"
    if html:
        return _html_matches_any_provider(html)
    return None


def provider_candidate_base(
    *,
    studio: str,
    adapter: str,
    nl_priority: bool,
    discovery_method: str,
    url: str,
    evidence_types: list[str],
    evidence_source: str,
    evidence_score: int,
) -> dict[str, Any]:
    return {
        "name": f"{studio} ({PROVIDER_DISPLAY_NAMES[adapter]})",
        "studio": studio,
        "adapter": adapter,
        "nlPriority": nl_priority,
        "discoveryMethod": discovery_method,
        "discoveryStage": "web_provider",
        "careersUrl": url,
        "evidenceScore": evidence_score,
        "evidenceTypes": evidence_types,
        "evidenceSource": evidence_source,
    }


def _path_tokens(path: str) -> list[str]:
    return [piece for piece in path.split("/") if piece]


def _greenhouse_candidate(
    base: dict[str, Any],
    _parsed: ParseResult,
    host: str,
    path: str,
    _studio: str,
) -> dict[str, Any] | None:
    slug = (
        clean_token(path.split("/boards/", 1)[1].split("/", 1)[0])
        if host == "boards-api.greenhouse.io" and "/boards/" in path
        else clean_token((_path_tokens(path) or [""])[0])
    )
    if not slug:
        return None
    return {
        **base,
        "slug": slug,
        "api_url": f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true",
    }


def _lever_candidate(
    base: dict[str, Any],
    _parsed: ParseResult,
    host: str,
    path: str,
    _studio: str,
) -> dict[str, Any] | None:
    account = (
        clean_token(path.split("/v0/postings/", 1)[1].split("/", 1)[0])
        if "api.lever.co" in host and "/v0/postings/" in path
        else clean_token((_path_tokens(path) or [""])[0])
    )
    if not account:
        return None
    return {
        **base,
        "account": account,
        "api_url": f"https://api.lever.co/v0/postings/{account}?mode=json",
    }


def _smartrecruiters_company_id(host: str, path: str) -> str:
    if host == "api.smartrecruiters.com" and "/companies/" in path:
        pieces = _path_tokens(path)
        if "companies" in pieces:
            idx = pieces.index("companies")
            if idx + 1 < len(pieces):
                return pieces[idx + 1].strip()
    if host == "jobs.smartrecruiters.com":
        return (_path_tokens(path) or [""])[0].strip()
    return ""


def _smartrecruiters_candidate(
    base: dict[str, Any],
    _parsed: ParseResult,
    host: str,
    path: str,
    _studio: str,
) -> dict[str, Any] | None:
    company_id = _smartrecruiters_company_id(host, path)
    if not company_id:
        return None
    return {
        **base,
        "company_id": company_id,
        "api_url": f"https://api.smartrecruiters.com/v1/companies/{company_id}/postings",
    }


def _workable_candidate(
    base: dict[str, Any],
    _parsed: ParseResult,
    host: str,
    path: str,
    _studio: str,
) -> dict[str, Any] | None:
    if (
        host != "workable.com"
        and host != "apply.workable.com"
        and host_matches_domain(host, "workable.com")
    ):
        account = host.split(".workable.com", 1)[0].strip().lower()
    else:
        account = ((_path_tokens(path) or [""])[-1]).strip().lower()
    if not account:
        return None
    return {
        **base,
        "account": account,
        "api_url": f"https://apply.workable.com/api/v1/widget/accounts/{account}?details=true",
    }


def _recruitee_candidate(
    base: dict[str, Any],
    _parsed: ParseResult,
    host: str,
    _path: str,
    _studio: str,
) -> dict[str, Any] | None:
    subdomain = host.split(".recruitee.com", 1)[0]
    if not subdomain:
        return None
    return {
        **base,
        "subdomain": subdomain,
        "api_url": f"https://{host}/api/offers/",
    }


def _pinpoint_candidate(
    base: dict[str, Any],
    _parsed: ParseResult,
    host: str,
    _path: str,
    _studio: str,
) -> dict[str, Any] | None:
    subdomain = host.split(".pinpointhq.com", 1)[0]
    if not subdomain:
        return None
    return {
        **base,
        "subdomain": subdomain,
        "api_url": f"https://{host}/postings.json",
    }


def _teamtailor_candidate(
    base: dict[str, Any],
    parsed: ParseResult,
    host: str,
    _path: str,
    studio: str,
) -> dict[str, Any]:
    base_url = f"{parsed.scheme}://{host}" if parsed.scheme else f"https://{host}"
    return {
        **base,
        "listing_url": f"{base_url}/jobs",
        "base_url": base_url,
        "company": studio,
    }


def _ashby_candidate(
    base: dict[str, Any],
    _parsed: ParseResult,
    _host: str,
    path: str,
    _studio: str,
) -> dict[str, Any] | None:
    slug = clean_token((_path_tokens(path) or [""])[0])
    if not slug:
        return None
    return {
        **base,
        "board_url": f"https://jobs.ashbyhq.com/{slug}",
    }


def _bamboohr_candidate(
    base: dict[str, Any],
    parsed: ParseResult,
    host: str,
    path: str,
    studio: str,
) -> dict[str, Any]:
    base_url = f"{parsed.scheme}://{host}" if parsed.scheme else f"https://{host}"
    listing_path = path.rstrip("/") or "/careers"
    return {
        **base,
        "listing_url": f"{base_url}{listing_path}",
        "company": studio,
    }


def _breezy_candidate(
    base: dict[str, Any],
    parsed: ParseResult,
    host: str,
    _path: str,
    _studio: str,
) -> dict[str, Any] | None:
    account = host.split(".breezy.hr", 1)[0]
    if not account:
        return None
    scheme = parsed.scheme or "https"
    return {
        **base,
        "board_url": f"{scheme}://{host}/",
    }


def _jazzhr_candidate(
    base: dict[str, Any],
    parsed: ParseResult,
    host: str,
    _path: str,
    _studio: str,
) -> dict[str, Any] | None:
    account = host.split(".applytojob.com", 1)[0]
    if not account:
        return None
    scheme = parsed.scheme or "https"
    return {
        **base,
        "board_url": f"{scheme}://{host}/apply",
    }


def _oracle_hcm_candidate(
    base: dict[str, Any],
    parsed: ParseResult,
    host: str,
    path: str,
    _studio: str,
) -> dict[str, Any] | None:
    if not _is_oraclecloud_host(host) or not _is_oracle_hcm_candidate_path(path):
        return None
    scheme = parsed.scheme or "https"
    base_url = f"{scheme}://{host}"
    site_path = path.rstrip("/")
    if not site_path:
        return None
    listing_url = parsed._replace(scheme=scheme, netloc=host, fragment="").geturl()
    return {
        **base,
        "listing_url": listing_url,
        "base_url": base_url,
        "site_path": site_path,
    }


def _personio_candidate(
    base: dict[str, Any],
    _parsed: ParseResult,
    host: str,
    _path: str,
    _studio: str,
) -> dict[str, Any] | None:
    token = host.split(".jobs.personio.de", 1)[0]
    if not token:
        return None
    return {
        **base,
        "feed_url": f"https://{token}.jobs.personio.de/xml",
    }


def _workday_candidate(
    base: dict[str, Any],
    parsed: ParseResult,
    host: str,
    path: str,
    studio: str,
) -> dict[str, Any] | None:
    if host == "myworkdayjobs.com" or not host_matches_domain(host, "myworkdayjobs.com"):
        return None
    listing_path = path.rstrip("/")
    if not listing_path:
        return None
    base_url = f"{parsed.scheme}://{host}" if parsed.scheme else f"https://{host}"
    query = f"?{parsed.query}" if parsed.query else ""
    return {
        **base,
        "listing_url": f"{base_url}{listing_path}{query}",
        "company": studio,
    }


ProviderCandidateBuilder = Callable[
    [dict[str, Any], ParseResult, str, str, str],
    dict[str, Any] | None,
]

_PROVIDER_CANDIDATE_BUILDERS: dict[str, ProviderCandidateBuilder] = {
    "ashby": _ashby_candidate,
    "bamboohr": _bamboohr_candidate,
    "breezy": _breezy_candidate,
    "greenhouse": _greenhouse_candidate,
    "jazzhr": _jazzhr_candidate,
    "lever": _lever_candidate,
    "oracle_hcm": _oracle_hcm_candidate,
    "personio": _personio_candidate,
    "pinpoint": _pinpoint_candidate,
    "recruitee": _recruitee_candidate,
    "smartrecruiters": _smartrecruiters_candidate,
    "teamtailor": _teamtailor_candidate,
    "workable": _workable_candidate,
    "workday": _workday_candidate,
}


def provider_candidate(
    *,
    studio: str,
    adapter: str,
    url: str,
    nl_priority: bool,
    discovery_method: str,
    evidence_types: list[str],
    evidence_source: str,
    evidence_score: int,
) -> dict[str, Any] | None:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    path = parsed.path or ""
    builder = _PROVIDER_CANDIDATE_BUILDERS.get(adapter)
    if builder is None:
        return None
    base = provider_candidate_base(
        studio=studio,
        adapter=adapter,
        nl_priority=nl_priority,
        discovery_method=discovery_method,
        url=url,
        evidence_types=evidence_types,
        evidence_source=evidence_source,
        evidence_score=evidence_score,
    )
    return builder(base, parsed, host, path, studio)


def infer_web_candidate(
    url: str,
    studio: str,
    *,
    nl_priority: bool,
    discovery_method: str = "web_search",
) -> dict[str, Any] | None:
    try:
        parsed = urlparse(url)
    except ValueError:
        return None
    adapter = infer_provider_adapter((parsed.hostname or "").lower(), parsed.path or "")
    if not adapter:
        return None
    evidence_score = (
        28
        + (12 if studio_domain_match(studio, url) else 0)
        + (4 if careers_keyword_count(url) else 0)
    )
    return provider_candidate(
        studio=studio,
        adapter=adapter,
        url=url,
        nl_priority=nl_priority,
        discovery_method=discovery_method,
        evidence_types=["web_provider_url"],
        evidence_source="url",
        evidence_score=evidence_score,
    )
