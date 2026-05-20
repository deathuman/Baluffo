from __future__ import annotations

from typing import Any
from urllib.parse import urlparse

from src.source_registry import unique_sources
from src.url_hosts import host_matches_domain, host_matches_subdomain

from .config import SUPPORTED_PROVIDERS
from .scoring import careers_keyword_count, clean_token, to_slug, unique_string_list


def expand_aliases(seed: dict[str, Any]) -> list[str]:
    aliases = [str(seed.get("studio") or "")]
    aliases.extend(str(item) for item in (seed.get("aliases") or []) if item)
    normalized: list[str] = []
    seen = set()
    for raw in aliases:
        slug = to_slug(raw)
        token = clean_token(raw)
        for value in (slug, token):
            if not value or value in seen:
                continue
            seen.add(value)
            normalized.append(value)
    return normalized


def likely_providers_for_seed(seed: dict[str, Any]) -> list[str]:
    explicit = [
        str(item).strip().lower()
        for item in (seed.get("likelyProviders") or [])
        if str(item).strip()
    ]
    if explicit:
        return [item for item in explicit if item in SUPPORTED_PROVIDERS or item == "static"]
    providers = {"greenhouse", "workable", "teamtailor"}
    if not bool(seed.get("nlPriority")):
        providers.update({"lever", "smartrecruiters", "ashby", "recruitee", "pinpoint"})
    return [item for item in SUPPORTED_PROVIDERS if item in providers]


def provider_reinforcement_score(seed: dict[str, Any], provider: str) -> int:
    careers_url = str(seed.get("careersUrl") or "").strip().lower()
    if not careers_url:
        return 0
    parsed = urlparse(careers_url)
    host = (parsed.hostname or "").lower()
    path = (parsed.path or "").lower()
    if provider == "teamtailor":
        return _teamtailor_reinforcement_score(careers_url, host, path)
    return _host_reinforcement_score(provider, host, path)


def _host_reinforcement_score(provider: str, host: str, path: str) -> int:
    host_domains = {
        "greenhouse": ("greenhouse.io",),
        "lever": ("lever.co",),
        "smartrecruiters": ("smartrecruiters.com",),
        "workable": ("workable.com",),
        "ashby": ("ashbyhq.com",),
        "recruitee": ("recruitee.com",),
        "pinpoint": ("pinpointhq.com",),
        "personio": ("jobs.personio.de",),
    }
    domains = host_domains.get(provider, ())
    if any(host_matches_domain(host, domain) for domain in domains):
        return 18
    if provider == "greenhouse" and "greenhouse" in path:
        return 18
    return 0


def _teamtailor_reinforcement_score(careers_url: str, host: str, path: str) -> int:
    if host_matches_subdomain(host, "teamtailor.com"):
        return 18
    if path.startswith("/jobs") and careers_keyword_count(careers_url):
        return 8
    return 0


def _pattern_aliases_for_provider(seed: dict[str, Any], provider: str) -> list[str]:
    aliases = expand_aliases(seed)
    scoped = (
        aliases[:2]
        if provider in {"greenhouse", "lever", "workable", "teamtailor", "recruitee"}
        else aliases[:1]
    )
    if provider in {"lever", "teamtailor", "recruitee", "pinpoint"}:
        expanded: list[str] = []
        seen = set()
        for alias in scoped:
            for variant in (alias, alias.replace("-", ""), alias.replace("_", "")):
                token = clean_token(variant)
                if not token or token in seen:
                    continue
                seen.add(token)
                expanded.append(token)
        return expanded[:4]
    return scoped


def _pattern_seed_context(seed: dict[str, Any]) -> dict[str, Any] | None:
    studio = str(seed.get("studio") or "").strip()
    if not studio:
        return None
    explicit = [
        str(item).strip().lower()
        for item in (seed.get("likelyProviders") or [])
        if str(item).strip()
    ]
    return {
        "studio": studio,
        "nlPriority": bool(seed.get("nlPriority")),
        "careersUrl": str(seed.get("careersUrl") or "").strip(),
        "explicit": explicit,
    }


def _base_pattern_candidate(
    *, seed_context: dict[str, Any], provider: str, reinforcement: int
) -> dict[str, Any]:
    evidence_types = ["seed_provider_hint", "seed_catalog"]
    explicit = list(seed_context.get("explicit") or [])
    return {
        "studio": seed_context["studio"],
        "nlPriority": bool(seed_context.get("nlPriority")),
        "discoveryMethod": "pattern",
        "discoveryStage": "provider_pattern",
        "careersUrl": str(seed_context.get("careersUrl") or ""),
        "evidenceScore": 14 + (10 if provider in explicit else 0) + reinforcement,
        "evidenceTypes": unique_string_list(
            [*evidence_types, "seed_provider_reinforced"] if reinforcement else evidence_types
        ),
        "evidenceSource": "pattern",
    }


def _lever_pattern_row(base: dict[str, Any], studio: str, alias: str) -> dict[str, Any]:
    return {
        **base,
        "name": f"{studio} (Lever)",
        "adapter": "lever",
        "account": alias,
        "api_url": f"https://api.lever.co/v0/postings/{alias}?mode=json",
    }


def _greenhouse_pattern_row(base: dict[str, Any], studio: str, alias: str) -> dict[str, Any]:
    return {
        **base,
        "name": f"{studio} (Greenhouse)",
        "adapter": "greenhouse",
        "slug": alias,
        "api_url": f"https://boards-api.greenhouse.io/v1/boards/{alias}/jobs?content=true",
    }


def _smartrecruiters_pattern_row(base: dict[str, Any], studio: str, alias: str) -> dict[str, Any]:
    return {
        **base,
        "name": f"{studio} (SmartRecruiters)",
        "adapter": "smartrecruiters",
        "company_id": alias.upper(),
        "api_url": f"https://api.smartrecruiters.com/v1/companies/{alias.upper()}/postings",
    }


def _workable_pattern_row(base: dict[str, Any], studio: str, alias: str) -> dict[str, Any]:
    return {
        **base,
        "name": f"{studio} (Workable)",
        "adapter": "workable",
        "account": alias,
        "api_url": f"https://apply.workable.com/api/v1/widget/accounts/{alias}?details=true",
    }


def _teamtailor_pattern_row(
    base: dict[str, Any], studio: str, alias: str, careers_url: str
) -> dict[str, Any]:
    if careers_url and "/jobs" in careers_url.lower():
        parsed = urlparse(careers_url)
        if parsed.scheme and parsed.netloc:
            return {
                **base,
                "name": f"{studio} (Teamtailor)",
                "adapter": "teamtailor",
                "company": studio,
                "listing_url": careers_url,
                "base_url": f"{parsed.scheme}://{parsed.netloc}",
            }
    return {
        **base,
        "name": f"{studio} (Teamtailor)",
        "adapter": "teamtailor",
        "company": studio,
        "listing_url": f"https://{alias}.teamtailor.com/jobs",
        "base_url": f"https://{alias}.teamtailor.com",
    }


def _ashby_pattern_row(base: dict[str, Any], studio: str, alias: str) -> dict[str, Any]:
    return {
        **base,
        "name": f"{studio} (Ashby)",
        "adapter": "ashby",
        "board_url": f"https://jobs.ashbyhq.com/{alias}",
    }


def _recruitee_pattern_row(base: dict[str, Any], studio: str, alias: str) -> dict[str, Any]:
    host = alias if host_matches_subdomain(alias, "recruitee.com") else f"{alias}.recruitee.com"
    return {
        **base,
        "name": f"{studio} (Recruitee)",
        "adapter": "recruitee",
        "subdomain": host.split(".recruitee.com", 1)[0],
        "api_url": f"https://{host}/api/offers/",
    }


def _pinpoint_pattern_row(base: dict[str, Any], studio: str, alias: str) -> dict[str, Any]:
    host = alias if host_matches_subdomain(alias, "pinpointhq.com") else f"{alias}.pinpointhq.com"
    return {
        **base,
        "name": f"{studio} (Pinpoint)",
        "adapter": "pinpoint",
        "subdomain": host.split(".pinpointhq.com", 1)[0],
        "api_url": f"https://{host}/postings.json",
    }


def _personio_pattern_row(base: dict[str, Any], studio: str, alias: str) -> dict[str, Any]:
    return {
        **base,
        "name": f"{studio} (Personio)",
        "adapter": "personio",
        "feed_url": f"https://{alias}.jobs.personio.de/xml",
    }


def _provider_pattern_row(
    *, seed_context: dict[str, Any], provider: str, alias: str, reinforcement: int
) -> dict[str, Any] | None:
    studio = str(seed_context["studio"])
    careers_url = str(seed_context.get("careersUrl") or "")
    base = _base_pattern_candidate(
        seed_context=seed_context,
        provider=provider,
        reinforcement=reinforcement,
    )
    builders = {
        "lever": lambda: _lever_pattern_row(base, studio, alias),
        "greenhouse": lambda: _greenhouse_pattern_row(base, studio, alias),
        "smartrecruiters": lambda: _smartrecruiters_pattern_row(base, studio, alias),
        "workable": lambda: _workable_pattern_row(base, studio, alias),
        "teamtailor": lambda: _teamtailor_pattern_row(base, studio, alias, careers_url),
        "ashby": lambda: _ashby_pattern_row(base, studio, alias),
        "recruitee": lambda: _recruitee_pattern_row(base, studio, alias),
        "pinpoint": lambda: _pinpoint_pattern_row(base, studio, alias),
        "personio": lambda: _personio_pattern_row(base, studio, alias),
    }
    builder = builders.get(provider)
    return builder() if builder else None


def build_pattern_candidates(studio_seeds: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for seed in studio_seeds:
        seed_context = _pattern_seed_context(seed)
        if seed_context is None:
            continue
        for provider in likely_providers_for_seed(seed):
            reinforcement = provider_reinforcement_score(seed, provider)
            for alias in _pattern_aliases_for_provider(seed, provider):
                row = _provider_pattern_row(
                    seed_context=seed_context,
                    provider=provider,
                    alias=alias,
                    reinforcement=reinforcement,
                )
                if row is not None:
                    rows.append(row)
    return unique_sources(rows)
