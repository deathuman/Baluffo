from __future__ import annotations

from typing import Any, Dict, List
from urllib.parse import urlparse

from src.source_registry import unique_sources

from .config import SUPPORTED_PROVIDERS
from .scoring import careers_keyword_count, clean_token, to_slug, unique_string_list


def expand_aliases(seed: Dict[str, Any]) -> List[str]:
    aliases = [str(seed.get("studio") or "")]
    aliases.extend(str(item) for item in (seed.get("aliases") or []) if item)
    normalized: List[str] = []
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


def likely_providers_for_seed(seed: Dict[str, Any]) -> List[str]:
    explicit = [str(item).strip().lower() for item in (seed.get("likelyProviders") or []) if str(item).strip()]
    if explicit:
        return [item for item in explicit if item in SUPPORTED_PROVIDERS or item == "static"]
    providers = {"greenhouse", "workable", "teamtailor"}
    if not bool(seed.get("nlPriority")):
        providers.update({"lever", "smartrecruiters", "ashby", "recruitee", "pinpoint"})
    return [item for item in SUPPORTED_PROVIDERS if item in providers]


def provider_reinforcement_score(seed: Dict[str, Any], provider: str) -> int:
    careers_url = str(seed.get("careersUrl") or "").strip().lower()
    if not careers_url:
        return 0
    parsed = urlparse(careers_url)
    host = (parsed.netloc or "").lower()
    path = (parsed.path or "").lower()
    if provider == "greenhouse":
        return 18 if "greenhouse" in host or "greenhouse" in path else 0
    if provider == "lever":
        return 18 if "lever.co" in host else 0
    if provider == "smartrecruiters":
        return 18 if "smartrecruiters" in host else 0
    if provider == "workable":
        return 18 if "workable" in host else 0
    if provider == "ashby":
        return 18 if "ashbyhq" in host else 0
    if provider == "recruitee":
        return 18 if ".recruitee.com" in host else 0
    if provider == "pinpoint":
        return 18 if ".pinpointhq.com" in host else 0
    if provider == "personio":
        return 18 if ".jobs.personio.de" in host else 0
    if provider == "teamtailor":
        if ".teamtailor.com" in host:
            return 18
        if path.startswith("/jobs") and careers_keyword_count(careers_url):
            return 8
        return 0
    return 0


def _pattern_aliases_for_provider(seed: Dict[str, Any], provider: str) -> List[str]:
    aliases = expand_aliases(seed)
    scoped = aliases[:2] if provider in {"greenhouse", "lever", "workable", "teamtailor", "recruitee"} else aliases[:1]
    if provider in {"lever", "teamtailor", "recruitee", "pinpoint"}:
        expanded: List[str] = []
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


def build_pattern_candidates(studio_seeds: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for seed in studio_seeds:
        studio = str(seed.get("studio") or "").strip()
        if not studio:
            continue
        nl_priority = bool(seed.get("nlPriority"))
        careers_url = str(seed.get("careersUrl") or "").strip()
        evidence_types = ["seed_provider_hint", "seed_catalog"]
        explicit = [str(item).strip().lower() for item in (seed.get("likelyProviders") or []) if str(item).strip()]
        for provider in likely_providers_for_seed(seed):
            reinforcement = provider_reinforcement_score(seed, provider)
            for alias in _pattern_aliases_for_provider(seed, provider):
                base: Dict[str, Any] = {
                    "studio": studio,
                    "nlPriority": nl_priority,
                    "discoveryMethod": "pattern",
                    "discoveryStage": "provider_pattern",
                    "careersUrl": careers_url,
                    "evidenceScore": 14 + (10 if provider in explicit else 0) + reinforcement,
                    "evidenceTypes": unique_string_list(
                        [*evidence_types, "seed_provider_reinforced"] if reinforcement else evidence_types
                    ),
                    "evidenceSource": "pattern",
                }
                if provider == "lever":
                    rows.append({
                        **base,
                        "name": f"{studio} (Lever)",
                        "adapter": "lever",
                        "account": alias,
                        "api_url": f"https://api.lever.co/v0/postings/{alias}?mode=json",
                    })
                elif provider == "greenhouse":
                    rows.append({
                        **base,
                        "name": f"{studio} (Greenhouse)",
                        "adapter": "greenhouse",
                        "slug": alias,
                        "api_url": f"https://boards-api.greenhouse.io/v1/boards/{alias}/jobs?content=true",
                    })
                elif provider == "smartrecruiters":
                    rows.append({
                        **base,
                        "name": f"{studio} (SmartRecruiters)",
                        "adapter": "smartrecruiters",
                        "company_id": alias.upper(),
                        "api_url": f"https://api.smartrecruiters.com/v1/companies/{alias.upper()}/postings",
                    })
                elif provider == "workable":
                    rows.append({
                        **base,
                        "name": f"{studio} (Workable)",
                        "adapter": "workable",
                        "account": alias,
                        "api_url": f"https://apply.workable.com/api/v1/widget/accounts/{alias}?details=true",
                    })
                elif provider == "teamtailor":
                    if careers_url and "/jobs" in careers_url.lower():
                        parsed = urlparse(careers_url)
                        if parsed.scheme and parsed.netloc:
                            base_url = f"{parsed.scheme}://{parsed.netloc}"
                            rows.append({
                                **base,
                                "name": f"{studio} (Teamtailor)",
                                "adapter": "teamtailor",
                                "company": studio,
                                "listing_url": careers_url,
                                "base_url": base_url,
                            })
                            continue
                    rows.append({
                        **base,
                        "name": f"{studio} (Teamtailor)",
                        "adapter": "teamtailor",
                        "company": studio,
                        "listing_url": f"https://{alias}.teamtailor.com/jobs",
                        "base_url": f"https://{alias}.teamtailor.com",
                    })
                elif provider == "ashby":
                    rows.append({
                        **base,
                        "name": f"{studio} (Ashby)",
                        "adapter": "ashby",
                        "board_url": f"https://jobs.ashbyhq.com/{alias}",
                    })
                elif provider == "recruitee":
                    host = alias if ".recruitee.com" in alias else f"{alias}.recruitee.com"
                    rows.append({
                        **base,
                        "name": f"{studio} (Recruitee)",
                        "adapter": "recruitee",
                        "subdomain": host.split(".recruitee.com", 1)[0],
                        "api_url": f"https://{host}/api/offers/",
                    })
                elif provider == "pinpoint":
                    host = alias if ".pinpointhq.com" in alias else f"{alias}.pinpointhq.com"
                    rows.append({
                        **base,
                        "name": f"{studio} (Pinpoint)",
                        "adapter": "pinpoint",
                        "subdomain": host.split(".pinpointhq.com", 1)[0],
                        "api_url": f"https://{host}/postings.json",
                    })
                elif provider == "personio":
                    rows.append({
                        **base,
                        "name": f"{studio} (Personio)",
                        "adapter": "personio",
                        "feed_url": f"https://{alias}.jobs.personio.de/xml",
                    })
    return unique_sources(rows)
