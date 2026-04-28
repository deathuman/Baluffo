from __future__ import annotations

"""Configuration and static defaults for source discovery.

This module owns:
- paths (seed catalog, discovery config/log files)
- global discovery constants (stages, thresholds, adapter caps)
- default studio seeds and discovery config payload
"""

import json
import os
from pathlib import Path
from typing import Any

from src.baluffo_config import get_storage_defaults

ROOT = Path(__file__).resolve().parents[1]
# For SEED_CATALOG_PATH we need repo root (parent of src)
_REPO_ROOT = ROOT.parent
_STORAGE_DEFAULTS = get_storage_defaults()

SEED_CATALOG_PATH = _REPO_ROOT / "src" / "discovery_seed_catalog.json"
DISCOVERY_STAGES: tuple[str, ...] = (
    "curated_seed",
    "sheet_directory",
    "provider_pattern",
    "web_provider",
    "generic_static",
)
# Evidence types vocabulary (canonical source of truth for evidenceTypes values).
# - Note: "sheet_directory" is intentionally both a stage name and an evidence type.
# - Evidence type families:
#   - gamedevmap_*: gamedevmap_directory, gamedevmap_category, gamedevmap_ai_reviewed,
#     gamedevmap_homepage_fetch, gamedevmap_direct_url, gamedevmap_careers_url,
#     gamedevmap_recovery_page
#   - gamesmap_*: gamesmap_directory, gamesmap_category_match, gamesmap_website, gamesmap_website_only,
#     gamesmap_manual_website_only, gamesmap_careers_url, gamesmap_location, gamesmap_website_fetch
#   - sheet_*: sheet_directory, sheet_row, sheet_roles_open_yes/no/speculative/unknown
#   - seed_*: seed_catalog, seed_provider_hint, seed_provider_reinforced, seed_curated
#   - web_*: web_provider_url
#   - Shared structural (keep as-is): careers_keyword, structured_job_links, jobposting_jsonld,
#     studio_domain_match, careers_page, html_embed
EVIDENCE_TYPES: tuple[str, ...] = (
    # Gameprog evidence
    "gameprog_directory",
    "gameprog_website",
    "gameprog_website_only",
    "gameprog_manual_website_only",
    "gameprog_careers_url",
    "gameprog_location",
    "gameprog_website_fetch",
    # GameDevMap evidence
    "gamedevmap_directory",
    "gamedevmap_category",
    "gamedevmap_ai_reviewed",
    "gamedevmap_homepage_fetch",
    "gamedevmap_direct_url",
    "gamedevmap_careers_url",
    "gamedevmap_recovery_page",
    # Gamesmap evidence
    "gamesmap_directory",
    "gamesmap_category_match",
    "gamesmap_website",
    "gamesmap_website_only",
    "gamesmap_manual_website_only",
    "gamesmap_careers_url",
    "gamesmap_location",
    "gamesmap_website_fetch",
    # Sheet directory evidence
    "sheet_directory",
    "sheet_row",
    "sheet_roles_open_yes",
    "sheet_roles_open_no",
    "sheet_roles_open_speculative",
    "sheet_roles_open_unknown",
    # Seed / pattern evidence
    "seed_catalog",
    "seed_provider_hint",
    "seed_provider_reinforced",
    "seed_curated",
    # Web inference evidence
    "web_provider_url",
    # Shared structural evidence
    "careers_keyword",
    "structured_job_links",
    "jobposting_jsonld",
    "studio_domain_match",
    "careers_page",
    "html_embed",
)
EVIDENCE_TYPES_SET = set(EVIDENCE_TYPES)
SUPPORTED_PROVIDERS: tuple[str, ...] = (
    "greenhouse",
    "lever",
    "smartrecruiters",
    "workable",
    "teamtailor",
    "ashby",
    "recruitee",
    "pinpoint",
    "personio",
)
DISCOVERY_CONFIG_PATH = Path(str(_STORAGE_DEFAULTS["source_discovery_config_path"]))
CAREERS_URL_HINTS: tuple[str, ...] = (
    "careers",
    "career",
    "jobs",
    "join-us",
    "open-positions",
    "vacancies",
    "work-with-us",
)
GENERIC_STATIC_BLOCKED_DOMAINS: tuple[str, ...] = (
    "linkedin.com",
    "indeed.com",
    "glassdoor.com",
    "ziprecruiter.com",
    "monster.com",
    "welcome to the jungle.com",
    "welcometothejungle.com",
)
FOCUS_KEYWORDS: tuple[str, ...] = (
    "technical artist",
    "tech artist",
    "environment artist",
    "environment art",
    "world artist",
    "terrain artist",
)
DUCKDUCKGO_HTML_SEARCH = "https://duckduckgo.com/html/?q={query}"
WEB_SEARCH_QUERY_SUFFIX: tuple[str, ...] = ("careers", "jobs")
FETCH_MAX_RETRIES = 2
RETRYABLE_HTTP_CODES = {429, 500, 502, 503, 504}
MAX_SEARCH_LINKS_PER_QUERY = 8
MIN_PROVIDER_EVIDENCE_TO_PROBE = 18
MIN_STATIC_EVIDENCE_TO_PROBE = 22
MIN_PROVIDER_EVIDENCE_TO_QUEUE = 26
MIN_STATIC_EVIDENCE_TO_QUEUE = 34
LOW_EVIDENCE_PROBE_LIMIT = 12
PATTERN_PROVIDER_PROBE_THRESHOLD = 30
PATTERN_PROVIDER_QUEUE_THRESHOLD = 40
DOMAIN_QUEUE_CAP_DEFAULT = 2
ADAPTER_QUEUE_CAPS: dict[str, int] = {
    "greenhouse": 12,
    "lever": 10,
    "smartrecruiters": 8,
    "workable": 8,
    "teamtailor": 8,
    "ashby": 10,
    "recruitee": 6,
    "pinpoint": 6,
    "personio": 3,
    "static": 8,
}
UNCAPPED_DISCOVERY_DOMAIN_QUEUE_CAP = 8
UNCAPPED_DISCOVERY_ADAPTER_QUEUE_CAPS: dict[str, int] = {
    "greenhouse": 24,
    "lever": 20,
    "smartrecruiters": 16,
    "workable": 16,
    "teamtailor": 16,
    "ashby": 20,
    "recruitee": 12,
    "pinpoint": 12,
    "personio": 6,
    "static": 16,
}

DEFAULT_DISCOVERY_THRESHOLDS: dict[str, int] = {
    "minProviderEvidenceToProbe": MIN_PROVIDER_EVIDENCE_TO_PROBE,
    "minStaticEvidenceToProbe": MIN_STATIC_EVIDENCE_TO_PROBE,
    "minProviderEvidenceToQueue": MIN_PROVIDER_EVIDENCE_TO_QUEUE,
    "minStaticEvidenceToQueue": MIN_STATIC_EVIDENCE_TO_QUEUE,
    "lowEvidenceProbeLimit": LOW_EVIDENCE_PROBE_LIMIT,
    "patternProviderProbeThreshold": PATTERN_PROVIDER_PROBE_THRESHOLD,
    "patternProviderQueueThreshold": PATTERN_PROVIDER_QUEUE_THRESHOLD,
}

DISCOVERY_LOG_PATH = str(
    os.getenv("BALUFFO_DISCOVERY_LOG_PATH") or _STORAGE_DEFAULTS["source_discovery_log_path"]
).strip()

GAME_STUDIOS_SHEET_ID = "1nHKWmwElNhap2It0jY7QHaRIdWojhaKt6Mll4UBOTT4"
GAME_STUDIOS_SHEET_GID = "567781753"
GAME_STUDIOS_SHEET_URL = f"https://docs.google.com/spreadsheets/d/{GAME_STUDIOS_SHEET_ID}/edit?gid={GAME_STUDIOS_SHEET_GID}"

DEFAULT_STUDIO_SEEDS: list[dict[str, Any]] = [
    {
        "studio": "Guerrilla Games",
        "aliases": ["guerrilla-games", "guerrillagames"],
        "nlPriority": True,
        "likelyProviders": ["greenhouse"],
        "careersUrl": "https://www.guerrilla-games.com/join",
    },
    {
        "studio": "Nixxes",
        "aliases": ["nixxes"],
        "nlPriority": True,
        "likelyProviders": ["static"],
        "careersUrl": "https://www.nixxes.com/careers",
    },
    {
        "studio": "Vertigo Games",
        "aliases": ["vertigo-games", "vertigogames"],
        "nlPriority": True,
        "likelyProviders": ["workable", "smartrecruiters"],
        "careersUrl": "https://vertigo-games.com/careers",
    },
    {
        "studio": "Triumph Studios",
        "aliases": ["triumph-studios", "triumphstudios"],
        "nlPriority": True,
        "likelyProviders": ["static"],
        "careersUrl": "https://www.triumphstudios.com/careers",
    },
    {
        "studio": "Little Chicken",
        "aliases": ["littlechicken", "little-chicken"],
        "nlPriority": True,
        "likelyProviders": ["static"],
        "careersUrl": "https://www.littlechicken.nl/about-us/jobs/",
    },
]

DEFAULT_DISCOVERY_CONFIG: dict[str, Any] = {
    "gameprog": {
        "enabled": True,
        "activeAuditEnabled": True,
        "activeAuditRecoveryEnabled": True,
        "activeAuditRecoveryUrlLimit": 6,
        "teamsUrl": "https://gameprog.it/teams.json",
        "websiteOnlyFallback": True,
        "maxStudios": 200,
        "fetchConcurrency": 24,
        "perHostConcurrency": 3,
    },
    "gamesmap": {
        "enabled": False,
        "activeAuditEnabled": True,
        "activeAuditRecoveryEnabled": True,
        "activeAuditRecoveryUrlLimit": 6,
        "baseUrl": "https://www.gamesmap.de",
        "indexUrls": [
            "https://www.gamesmap.de/en",
        ],
        "preferEnglish": True,
        "websiteOnlyFallback": False,
        "maxDetailPages": 60,
        "allowedCategoryTokens": [
            "developer",
            "publisher",
            "developer and publisher",
            "pc",
            "console",
            "mobile",
            "browser",
            "online",
            "vr",
            "ar",
            "serious games",
        ],
        "blockedCategoryTokens": [
            "association",
            "university",
            "education",
            "public institution",
            "government",
            "service provider",
        ],
        "fetchConcurrency": 24,
        "perHostConcurrency": 3,
    },
    "sheetDirectory": {
        "activeAuditEnabled": True,
        "activeAuditRecoveryEnabled": True,
        "activeAuditRecoveryUrlLimit": 6,
        "activeAuditPath": "data/sheet-directory-discovery-audit.json",
        "activeAuditTtlMinutes": 360,
    },
    "webSearch": {
        "activeAuditEnabled": True,
        "activeAuditRecoveryEnabled": True,
        "activeAuditRecoveryUrlLimit": 6,
        "activeAuditPath": "data/web-search-discovery-audit.json",
        "activeAuditTtlMinutes": 360,
        "maxQueries": 24,
        "maxLinksPerQuery": 8,
        "browserRecoveryBatchSize": 50,
        "browserRecoveryMaxBatchesPerRun": 1,
        "browserRecoveryConcurrency": 2,
        "browserRecoveryTimeoutSeconds": 15,
    },
    "gamedevmap": {
        "enabled": True,
        "csvUrl": "https://www.gamedevmap.com/cmsdata/gamedevmapdata.csv",
        "indexUrl": "https://www.gamedevmap.com/index.php",
        "cacheTtlMinutes": 360,
        "activeAuditEnabled": True,
        "activeAuditTtlMinutes": 360,
        "activeAuditBatchSize": 1000,
        "activeAuditMaxBatchesPerDiscoveryRun": 0,
        "activeAuditHomepageFetchConcurrency": 32,
        "activeAuditRecoveryFetchConcurrency": 72,
        "activeAuditRecoveryPerHostConcurrency": 4,
        "activeAuditRecoveryTimeoutSeconds": 5,
        "activeAuditBrowserRecoveryConcurrency": 2,
        "activeAuditBrowserRecoveryTimeoutSeconds": 15,
        "activeAuditBrowserRecoveryLimit": 0,
        "promoteValidatedStatic": True,
        "validatedStaticQueueCap": 500,
        "validatedStaticDomainCap": 8,
        "maxRows": 0,
        "maxHomepageFetches": 60,
        "allowedCategories": [
            "Developer",
            "Developer and Publisher",
            "Publisher",
            "Mobile",
            "Online",
            "Microstudio",
            "Extended Reality (XR)",
            "Serious Games",
            "Social",
        ],
        "blockedCategories": [
            "Organization",
            "Investment",
            "Incubator/Accelerator",
            "Health",
        ],
        "requireAiReviewed": False,
        "fetchConcurrency": 24,
        "perHostConcurrency": 3,
    },
    "thresholds": dict(DEFAULT_DISCOVERY_THRESHOLDS),
}

STATIC_DISCOVERY_CANDIDATES: list[dict[str, Any]] = [
    {
        "name": "Sandbox VR (Lever)",
        "studio": "Sandbox VR",
        "adapter": "lever",
        "account": "sandboxvr",
        "api_url": "https://api.lever.co/v0/postings/sandboxvr?mode=json",
        "nlPriority": False,
    },
    {
        "name": "Voodoo (Lever)",
        "studio": "Voodoo",
        "adapter": "lever",
        "account": "voodoo",
        "api_url": "https://api.lever.co/v0/postings/voodoo?mode=json",
        "nlPriority": False,
    },
    {
        "name": "CD PROJEKT RED (SmartRecruiters)",
        "studio": "CD PROJEKT RED",
        "adapter": "smartrecruiters",
        "company_id": "CDPROJEKTRED",
        "api_url": "https://api.smartrecruiters.com/v1/companies/CDPROJEKTRED/postings",
        "nlPriority": False,
    },
    {
        "name": "Gameloft (SmartRecruiters)",
        "studio": "Gameloft",
        "adapter": "smartrecruiters",
        "company_id": "Gameloft",
        "api_url": "https://api.smartrecruiters.com/v1/companies/Gameloft/postings",
        "nlPriority": False,
    },
    {
        "name": "Hutch (Workable)",
        "studio": "Hutch",
        "adapter": "workable",
        "account": "hutch",
        "api_url": "https://apply.workable.com/api/v1/widget/accounts/hutch?details=true",
        "nlPriority": False,
    },
    {
        "name": "Wargaming (Workable)",
        "studio": "Wargaming",
        "adapter": "workable",
        "account": "wargaming",
        "api_url": "https://apply.workable.com/api/v1/widget/accounts/wargaming?details=true",
        "nlPriority": False,
    },
    {
        "name": "CrazyGames (Recruitee)",
        "studio": "CrazyGames",
        "adapter": "recruitee",
        "subdomain": "jobs.crazygames.com",
        "api_url": "https://jobs.crazygames.com/api/offers/",
        "nlPriority": False,
    },
    {
        "name": "Gameplay Galaxy (Pinpoint)",
        "studio": "Gameplay Galaxy",
        "adapter": "pinpoint",
        "subdomain": "gameplaygalaxy",
        "api_url": "https://gameplaygalaxy.pinpointhq.com/postings.json",
        "nlPriority": False,
    },
    {
        "name": "Ubisoft (SmartRecruiters)",
        "studio": "Ubisoft",
        "adapter": "smartrecruiters",
        "company_id": "Ubisoft2",
        "api_url": "https://api.smartrecruiters.com/v1/companies/Ubisoft2/postings",
        "nlPriority": False,
    },
    {
        "name": "Bandai Namco Entertainment America (Greenhouse)",
        "studio": "Bandai Namco Entertainment America Inc.",
        "adapter": "greenhouse",
        "slug": "bandainamco",
        "nlPriority": False,
    },
]


def load_studio_seeds() -> list[dict[str, Any]]:
    try:
        payload = json.loads(SEED_CATALOG_PATH.read_text(encoding="utf-8"))
        if isinstance(payload, list):
            return [row for row in payload if isinstance(row, dict)] or list(DEFAULT_STUDIO_SEEDS)
    except (OSError, json.JSONDecodeError):
        pass
    return list(DEFAULT_STUDIO_SEEDS)


def load_discovery_config(config_path: Path | str | None = None) -> dict[str, Any]:
    path = Path(config_path) if config_path is not None else Path(DISCOVERY_CONFIG_PATH)
    payload = {
        key: (dict(value) if isinstance(value, dict) else value)
        for key, value in DEFAULT_DISCOVERY_CONFIG.items()
    }
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        raw = {}
    if not isinstance(raw, dict):
        return payload
    for key, value in raw.items():
        if not isinstance(value, dict):
            continue
        base = payload.get(key)
        if isinstance(base, dict):
            merged = dict(base)
            merged.update(value)
            payload[key] = merged
            continue
        payload[key] = dict(value)
    return payload


STUDIO_SEEDS: list[dict[str, Any]] = load_studio_seeds()
