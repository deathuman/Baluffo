"""Source discovery configuration helpers.

Configuration and static defaults for source discovery.

This module owns:
- paths (seed catalog, discovery config/log files)
- global discovery constants (stages, thresholds, adapter caps)
- default studio seeds and discovery config payload

AI boundary owns: discovery config defaults, provider toggles, and saved discovery settings shape.
AI boundary implement in: this file for discovery config shape; stage execution belongs in orchestrator/stage leaves.
AI boundary search before contracts: bridge discovery service, stage control, and discovery config tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused discovery config tests.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from src.baluffo_config import get_storage_defaults

ROOT = Path(__file__).resolve().parents[1]
# For SEED_CATALOG_PATH we need repo root (parent of src)
_REPO_ROOT = ROOT.parent
_STORAGE_DEFAULTS = get_storage_defaults()


def env_int(name: str, default: int) -> int:
    raw = str(os.getenv(name) or "").strip()
    try:
        return max(1, int(raw)) if raw else int(default)
    except ValueError:
        return int(default)


SEED_CATALOG_PATH = _REPO_ROOT / "src" / "discovery_seed_catalog.json"
# Boards added by the coverage audit against the external job index. Kept as data
# rather than a literal in this module because there are hundreds of them and a
# diff of a Python literal is unreviewable. Same loader contract as the studio seed
# catalog: a missing or malformed file must not break discovery.
COVERAGE_BOARDS_PATH = _REPO_ROOT / "src" / "curated_coverage_boards.json"
DISCOVERY_STAGES: tuple[str, ...] = (
    "curated_seed",
    "sheet_directory",
    "provider_pattern",
    "provider_migration_advisory",
    "web_provider",
    "generic_static",
)
# Evidence types vocabulary (canonical source of truth for evidenceTypes values).
# - Note: "sheet_directory" is intentionally both a stage name and an evidence type.
# - Evidence type families:
#   - gamedevmap_*: gamedevmap_directory, gamedevmap_category, gamedevmap_ai_reviewed,
#     gamedevmap_homepage_fetch, gamedevmap_direct_url, gamedevmap_careers_url,
#     gamedevmap_recovery_page
#   - gameprog_*: gameprog_directory, gameprog_website, gameprog_website_only,
#     gameprog_manual_website_only, gameprog_careers_url, gameprog_location,
#     gameprog_website_fetch, gameprog_no_current_openings
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
    "gameprog_no_current_openings",
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
    "bamboohr",
    "breezy",
    "jazzhr",
    "oracle_hcm",
    "workday",
    "recruitee",
    "pinpoint",
    "personio",
)
STRUCTURED_BATCH_ADAPTERS = frozenset({"greenhouse", "lever", "ashby"})
ZERO_JOB_CONFIDENCE_ADAPTERS = frozenset(
    {
        "lever",
        "greenhouse",
        "smartrecruiters",
        "workable",
        "teamtailor",
        "ashby",
        "recruitee",
        "pinpoint",
        "personio",
    }
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
    # Non-English career page keywords
    "stellenanzeigen",  # German: job postings
    "karriere",  # German: career
    "offene-stellen",  # German: open positions
    "emploi",  # French: job
    "recrutement",  # French: recruitment
    "offres",  # French: job offers
    "trabajo",  # Spanish: job (also matches "trabajos")
    "empleo",  # Spanish: employment
    "vacantes",  # Spanish: vacancies
    "lavora",  # Italian: works (careers)
    "offerte",  # Italian: job offers
    "posizioni-aperte",  # Italian: open positions
    "vagas",  # Portuguese: vacancies
    "carreiras",  # Portuguese: careers
    "recruitment",
    "openings",
    "recruit",
    "hiring",
    # CJK career keywords
    "採用",  # Japanese: recruitment
    "キャリア",  # Japanese: career (katakana)
    "求人",  # Japanese: job posting
    "채용",  # Korean: recruitment
    "招聘",  # Chinese: recruitment
    "职业",  # Chinese: career
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
FETCH_RETRY_BASE_DELAY_S = 1.2
FETCH_RETRY_MAX_DELAY_S = 5.0
FETCH_RETRY_JITTER_RATIO = 0.25
FETCH_ADAPTER_INITIAL_DELAY_S = 0.18
FETCH_INITIAL_DELAY_ADAPTERS = frozenset({"workable", "personio", "ashby", "recruitee", "pinpoint"})
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
    "oracle_hcm": 4,
    "recruitee": 6,
    "pinpoint": 6,
    "personio": 3,
    "static": 8,
}
PROBE_FAILURE_QUARANTINE_THRESHOLD = 3
PROBE_FAILURE_MEMORY_RETENTION_DAYS = 45
UNCAPPED_DISCOVERY_DOMAIN_QUEUE_CAP = 8
UNCAPPED_DISCOVERY_ADAPTER_QUEUE_CAPS: dict[str, int] = {
    "greenhouse": 24,
    "lever": 20,
    "smartrecruiters": 16,
    "workable": 16,
    "teamtailor": 16,
    "ashby": 20,
    "oracle_hcm": 8,
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
        "activeAuditPath": "data/gameprog-discovery-audit.json",
        "activeAuditRecoveryEnabled": True,
        "activeAuditRecoveryUrlLimit": 6,
        "teamsUrl": "https://gameprog.it/teams.json",
        "websiteOnlyFallback": True,
        "maxStudios": 200,
        "fetchConcurrency": 24,
        "perHostConcurrency": 3,
    },
    "gamesmap": {
        "enabled": True,
        "activeAuditPath": "data/gamesmap-discovery-audit.json",
        "activeAuditRecoveryEnabled": True,
        "activeAuditRecoveryUrlLimit": 6,
        "activeAuditTtlMinutes": 360,
        "baseUrl": "https://www.gamesmap.de",
        "indexUrls": [
            "https://www.gamesmap.de/en",
        ],
        "preferEnglish": True,
        "websiteOnlyFallback": True,
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
        "activeAuditRecoveryEnabled": True,
        "activeAuditRecoveryUrlLimit": 6,
        "activeAuditPath": "data/sheet-directory-discovery-audit.json",
        "activeAuditTtlMinutes": 360,
    },
    "webSearch": {
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
        "activeAuditTtlMinutes": 360,
        "activeAuditBatchSize": 1000,
        "activeAuditMaxBatchesPerDiscoveryRun": 0,
        "activeAuditHomepageFetchConcurrency": 32,
        "activeAuditRecoveryFetchConcurrency": 72,
        "activeAuditRecoveryPerHostConcurrency": 4,
        "activeAuditRecoveryTimeoutSeconds": 5,
        "activeAuditRecoveryCacheScope": "batch",
        "activeAuditBrowserRecoveryConcurrency": 2,
        "activeAuditBrowserRecoveryTimeoutSeconds": 15,
        "activeAuditBrowserRecoveryLimit": 0,
        "activeAuditRecoveryEscalationEnabled": True,
        "activeAuditRecoveryEscalationMaxRows": 200,
        "activeAuditRecoveryEscalationPatternLimit": 4,
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


def _board_locator_key(row: dict[str, Any]) -> tuple[str, str] | None:
    """``(adapter, locator)`` for a curated row, or None when it carries no locator.

    The locator is the first populated id field, which is what identifies a board. The
    studio label does not: the same board is registered as both "Lost Boys Interactive"
    and "Lost Boys Interactive (Embracer Group)".
    """
    for field in ("slug", "account", "company_id", "board_url", "api_url", "listing_url"):
        value = row.get(field)
        if value:
            return (str(row.get("adapter") or ""), str(value))
    return None


def _drop_already_curated(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Data-file rows whose board the hand-curated literal above already registers."""
    known = {k for k in (_board_locator_key(row) for row in STATIC_DISCOVERY_CANDIDATES) if k}
    return [row for row in rows if _board_locator_key(row) not in known]


def load_curated_coverage_boards() -> list[dict[str, Any]]:
    """Boards the coverage audit verified would collect, as curated-seed candidates.

    Every row here was proposed because a specific missed opening pointed at that
    board, and was registered because the board verifiably serves openings -- 270
    returned rows when fetched, and 234 render in a browser. There is no volume
    threshold, because the point of the product is that no opening is left behind.

    The browser-rendered class is registered rather than deferred on evidence, not
    assumption: of the boards this repo already collects, 101 of 159 reach their
    rows only through the browser path, so a listing that a plain GET cannot read is
    the normal case rather than a warning sign.

    Returns an empty list on a missing or malformed file so discovery still runs.
    """
    try:
        payload = json.loads(COVERAGE_BOARDS_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    if not isinstance(payload, list):
        return []
    rows: list[dict[str, Any]] = []
    for raw in payload:
        if not isinstance(raw, dict) or not raw.get("adapter") or not raw.get("studio"):
            continue
        row = dict(raw)
        row.setdefault("nlPriority", False)
        rows.append(row)
    return rows


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
        # Voodoo's board moved from Lever to Ashby; both Lever endpoints 404. The
        # Ashby board is registered via ashby_registry_refresh.CURATED_ASHBY_ROWS.
        "name": "Voodoo (Ashby)",
        "studio": "Voodoo",
        "adapter": "ashby",
        "board_url": "https://jobs.ashbyhq.com/voodoo",
        "careersUrl": "https://jobs.ashbyhq.com/voodoo",
        "coverageAuditOpenings": 90,
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
    {
        # Coverage audit + live board probe: the Greenhouse board serves
        # "Lead Technical Artist - Shaders" and the registered 2kczech.com static
        # row does not, so the ATS board itself was unregistered.
        "name": "2K Czech (Greenhouse)",
        "studio": "2K Czech",
        "adapter": "greenhouse",
        "slug": "2kczech",
        "coverageAuditOpenings": 7,
        "nlPriority": False,
    },
    {
        # Same Brno role as 2K Czech, published on Hangar 13's own Greenhouse board.
        # Both are indexed separately upstream, so both are registered.
        "name": "Hangar 13 (Greenhouse)",
        "studio": "Hangar 13",
        "adapter": "greenhouse",
        "slug": "hangar13",
        "coverageAuditOpenings": 9,
        "nlPriority": False,
    },
    {
        # Probe-confirmed live with "Technical Artist" on the board and no registry
        # row for it. The org is YggdrasilSandbox, not Yggdrasil.
        "name": "Yggdrasil (SmartRecruiters)",
        "studio": "Yggdrasil",
        "adapter": "smartrecruiters",
        "company_id": "YggdrasilSandbox",
        "api_url": "https://api.smartrecruiters.com/v1/companies/YggdrasilSandbox/postings",
        "coverageAuditOpenings": 3,
        "nlPriority": False,
    },
]

# The literal and the data file are concatenated rather than merged, so a board present in
# both is fetched twice and double-counted. Four were (Voodoo, 2K Czech, Hangar 13,
# Yggdrasil) and the fifth arrived with the second registration wave: AppLovin's Greenhouse
# board was proposed by the audit while the literal already carried it. Matched on adapter
# plus locator, never on the studio label. The literal wins, being hand-maintained.
STATIC_DISCOVERY_CANDIDATES += _drop_already_curated(load_curated_coverage_boards())


def load_studio_seeds() -> list[dict[str, Any]]:
    try:
        payload = json.loads(SEED_CATALOG_PATH.read_text(encoding="utf-8"))
        if isinstance(payload, list):
            return [row for row in payload if isinstance(row, dict)] or list(DEFAULT_STUDIO_SEEDS)
    except (OSError, json.JSONDecodeError):
        pass
    return list(DEFAULT_STUDIO_SEEDS)


DISCOVERY_FEED_RECHECK_QUEUE_PATH = (
    _STORAGE_DEFAULTS.get("data_dir", Path("data")) / "discovery-feed-recheck-queue.json"
)


def load_feed_recheck_seeds() -> list[dict[str, Any]]:
    """Load dead/migrated provider-feed studios queued for re-discovery.

    The jobs pipeline appends rows here (e.g. a Personio feed URL that now
    redirects to the vendor marketing homepage), so the next discovery run
    re-stages the studio instead of letting the broken feed error forever.
    """
    try:
        payload = json.loads(DISCOVERY_FEED_RECHECK_QUEUE_PATH.read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            return []
    except (OSError, json.JSONDecodeError):
        return []
    seeds: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in payload:
        if not isinstance(row, dict):
            continue
        studio = str(row.get("studio") or "").strip()
        if not studio or studio.lower() in seen:
            continue
        seen.add(studio.lower())
        seeds.append(
            {
                "name": str(row.get("name") or studio),
                "studio": studio,
                "nlPriority": False,
            }
        )
    return seeds


def studio_seeds_with_feed_recheck() -> list[dict[str, Any]]:
    seeds = list(STUDIO_SEEDS)
    recheck = load_feed_recheck_seeds()
    known = {str(seed.get("studio") or "").strip().lower() for seed in seeds}
    for seed in recheck:
        if str(seed.get("studio") or "").strip().lower() not in known:
            seeds.append(seed)
    return seeds


def studio_seeds_with_feed_recheck_priority() -> list[dict[str, Any]]:
    """Recheck-queue seeds first so the bounded web-search budget reaches them.

    The web-search stage caps queries at `maxQueries`; the curated seed catalog
    alone can consume that budget, starving dead/migrated-feed studios queued for
    re-discovery. Returning recheck seeds first guarantees they get searched.
    """
    recheck = load_feed_recheck_seeds()
    known = {str(seed.get("studio") or "").strip().lower() for seed in recheck}
    seeds = [
        seed for seed in STUDIO_SEEDS if str(seed.get("studio") or "").strip().lower() not in known
    ]
    return [*recheck, *seeds]


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
