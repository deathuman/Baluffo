"""Canonicalization and typed boundary helpers."""

from __future__ import annotations

import json
import re
import threading
import time
import urllib.error
import urllib.request
from collections import Counter
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, cast
from urllib.parse import quote, unquote, urlparse

from src.jobs.adapters import community
from src.jobs.adapters.parsers.location import normalize_location_details
from src.jobs.adapters.parsers.provider_html import parse_ashby_jobs_from_html
from src.jobs.common.datetime_utils import to_iso
from src.jobs.common.fetch import fetch_with_retries
from src.jobs.common.heuristics import (
    classify_company_type,
    compute_focus_score,
    compute_quality_score,
    map_profession,
    normalize_company_value,
)
from src.jobs.common.parsing import normalize_contract_type
from src.jobs.common.url import is_supported_redirect_url
from src.jobs.interfaces import JobProcessor
from src.jobs.models import CanonicalJob, RawJob
from src.jobs.normalizers import normalize_country, normalize_sector, normalize_work_type
from src.jobs.page_gating import looks_like_source_specific_static_noise_row
from src.jobs.text_utils import (
    REMOTEISH_TOKENS,
    clean_text,
    norm_text,
    normalize_url,
    resolve_country_acceptance_value,
    sanitize_location_text,
    sanitize_public_text,
)
from src.jobs.transport import PooledRedirectResolver
from src.shared.utils import env_flag
from src.url_hosts import host_matches_domain

from .common import config as common_config

UNKNOWN_COMPANY_LABEL = common_config.UNKNOWN_COMPANY_LABEL
UNTRUSTWORTHY_COMPANY_LABELS = common_config.UNTRUSTWORTHY_COMPANY_LABELS
REQUIRED_FIELDS = common_config.REQUIRED_FIELDS
OPTIONAL_FIELDS = common_config.OPTIONAL_FIELDS
OUTPUT_FIELDS = common_config.OUTPUT_FIELDS
LIGHTWEIGHT_OUTPUT_FIELDS = common_config.LIGHTWEIGHT_OUTPUT_FIELDS
TARGET_PROFESSIONS = common_config.TARGET_PROFESSIONS
DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY = community.DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY
GOOGLE_SHEETS_CATEGORY_LINK_STATUS_CONCURRENCY = 32
DEFAULT_CANONICAL_STRICT_URL = common_config.DEFAULT_CANONICAL_STRICT_URL
REDIRECT_RESOLUTION_SKIP_SOURCES = {"gracklehq"}
_GOOGLE_SHEETS_CATEGORY_LABEL_TERMS = frozenset(
    {
        "Accounting",
        "Account-management",
        "Administartive",
        "Administrative",
        "Audio",
        "Audio-production",
        "Auditing",
        "Backend",
        "Backend-development",
        "Business-analysis",
        "Business-development",
        "Campaign-management",
        "Character-art",
        "Community-management",
        "Combat-design",
        "Concept-art",
        "Curriculum-design",
        "Customer-service",
        "Cyber-security",
        "Data-analysis",
        "Data-science",
        "Design",
        "Devops",
        "Digital-marketing",
        "Editorial",
        "Education",
        "Facility-management",
        "Finance",
        "Financial-analysis",
        "Frontend",
        "Frontend-development",
        "Full-stack-development",
        "Game-ai",
        "Game-design",
        "Game-economy",
        "Game-engine",
        "Game-production",
        "Game-programmer",
        "Gameplay",
        "Graphic-design",
        "Graphics-engineer",
        "HR",
        "Human-resource",
        "Human-resources",
        "IT & infrastructure",
        "It-&-infrastructure",
        "Legal",
        "Level-art",
        "Level-design",
        "Live-ops",
        "Localization",
        "Logistics",
        "Marketing",
        "Mobile-development",
        "Network-admin",
        "Network-engineering",
        "Operations",
        "Physics-engine",
        "Product-design",
        "Product",
        "Product-management",
        "Program-management",
        "Programming",
        "Project-management",
        "Prop-art",
        "Public-relation",
        "QA",
        "Quality-assurance",
        "Quality-analysis",
        "Quest-design",
        "Rendering",
        "Research-development",
        "Risk-management",
        "Sales",
        "Social-media",
        "Software-development-&-engineering",
        "Software-development-engineering",
        "System-admin",
        "System-design",
        "Talent-acquisition",
        "Taxation",
        "Teaching",
        "Technical-art",
        "Testing",
        "UI-art",
        "Ui-ux-design",
        "Vfx",
        "Video-editing",
        "Videography",
        "Web-development",
    }
)
_GOOGLE_SHEETS_GAME_ADJACENT_CATEGORY_LABEL_TERMS = frozenset(
    {
        "Audio",
        "Community-management",
        "Digital-marketing",
        "Game-design",
        "Game-economy",
        "Game-production",
        "Game-programmer",
        "Gameplay",
        "Level-design",
        "Live-ops",
        "Localization",
        "Product",
        "Product-management",
        "Rendering",
        "Social-media",
        "Technical-art",
        "UI-art",
        "Vfx",
        "Video-editing",
    }
)
_GOOGLE_SHEETS_GAME_EVIDENCE_TERMS = frozenset(
    {
        "arena net",
        "arenanet",
        "cd projekt",
        "cdprojekt",
        "game",
        "gamedev",
        "gameplay",
        "games",
        "gameloft",
        "gaming",
        "insomniac",
        "interactive",
        "nintendo",
        "people can fly",
        "playstation",
        "riot games",
        "scopely",
        "studio",
        "studios",
        "ubisoft",
        "unity",
        "unreal",
        "xbox",
        "zynga",
    }
)
_GOOGLE_SHEETS_LINK_EMPLOYER_GAME_EVIDENCE_TERMS = frozenset(
    term
    for term in _GOOGLE_SHEETS_GAME_EVIDENCE_TERMS
    if term not in {"game", "interactive", "studio", "studios"}
)
_GOOGLE_SHEETS_NON_GAME_EVIDENCE_TERMS = frozenset(
    {
        "abercrombie",
        "accor",
        "accorhotel",
        "ace tate",
        "aecom",
        "afry",
        "allstate",
        "applus",
        "ariens",
        "autodesk",
        "bdo",
        "blackrock",
        "bosch",
        "boskalis",
        "brickwell",
        "broadcom",
        "cadence",
        "carda health",
        "cigna",
        "clearwater",
        "conde nast",
        "culina",
        "deangelo",
        "delta electronics",
        "dnv",
        "domino",
        "doordash",
        "dpd",
        "ebay",
        "energy jobline",
        "enphase",
        "enverus",
        "eurofins",
        "ge vernova",
        "globalization partners",
        "greencross",
        "guardian life",
        "illumina",
        "international sos",
        "jysk",
        "kanadevia",
        "kipp",
        "kpmg",
        "labcorp",
        "lockheed",
        "london stock exchange",
        "lucid hearing",
        "marvell",
        "mcdonald",
        "medhealth",
        "morningstar",
        "motorola",
        "mufg",
        "nasdaq",
        "northrop grumman",
        "nxp",
        "paypal",
        "pentair",
        "philips",
        "plug power",
        "publicis groupe",
        "pwc",
        "quest global",
        "redcare pharmacy",
        "salesforce",
        "saxobank",
        "scripps",
        "segula technologies",
        "servicenow",
        "serviceplan group",
        "sgi",
        "shiji",
        "silfab solar",
        "simcorp",
        "sofar sounds",
        "state of oklahoma",
        "thales",
        "the hill",
        "the rank group",
        "thriving center of psychology",
        "transperfect",
        "transunion",
        "trek bikes",
        "trupanion",
        "tutor me education",
        "univision",
        "valeo",
        "veolia",
        "veracity",
        "vertex",
        "visa",
        "walmart",
        "wayman learning trust",
        "westgate resorts",
        "wind river",
        "wynn resorts",
        # P3.0 gap closure: non-game employers with ATS URLs that lose mismatch-check coverage after URL extraction
        "axel springer",
        "cae",
        "devoteam",
        "flywire",
        "kpn",
        "nike",
        "pluralsight",
        "portman dentex",
        "ramboll",
        "rexel",
        "scalable gmbh",
        "sgs",
        "spavia",
        "trellix",
        "turner & townsend",
        "unilever",
    }
)
_GOOGLE_SHEETS_BEBEE_NON_GAME_EMPLOYER_MARKERS = (
    "adecco",
    "securiguard",
)

_LOCATION_AUDIT_LOCK = threading.Lock()
_LOCATION_AUDIT_FIELD_COUNTS: Counter[str] = Counter()
_LOCATION_AUDIT_REASON_COUNTS: Counter[str] = Counter()
_LOCATION_AUDIT_EXAMPLES: list[dict[str, Any]] = []
_SECTOR_AUDIT_LOCK = threading.Lock()
_SECTOR_AUDIT_DOWNGRADED_COUNT = 0
_SECTOR_AUDIT_EXAMPLES: list[dict[str, Any]] = []


def _normalized_evidence_text(*values: Any) -> str:
    text = " ".join(clean_text(value) for value in values if clean_text(value))
    return norm_text(re.sub(r"[^a-zA-Z0-9]+", " ", text))


def _contains_evidence_term(text: str, term: str) -> bool:
    normalized_term = _normalized_evidence_text(term)
    if not normalized_term:
        return False
    padded_text = f" {text} "
    if f" {normalized_term} " in padded_text:
        return True
    compact_text = text.replace(" ", "")
    compact_term = normalized_term.replace(" ", "")
    return len(compact_term) >= 4 and compact_term in compact_text


def _google_sheets_category_label_keys(value: Any) -> set[str]:
    raw = clean_text(value)
    if not raw:
        return set()
    spaced = norm_text(re.sub(r"[-_]+", " ", raw).replace("&", " and "))
    compact_and = norm_text(re.sub(r"[-_]+", " ", raw).replace("&", " "))
    return {
        key
        for key in {
            norm_text(raw),
            spaced,
            compact_and,
            spaced.replace(" ", "-"),
            compact_and.replace(" ", "-"),
        }
        if key
    }


_GOOGLE_SHEETS_CATEGORY_LABEL_KEYS = frozenset(
    key
    for term in _GOOGLE_SHEETS_CATEGORY_LABEL_TERMS
    for key in _google_sheets_category_label_keys(term)
)
_GOOGLE_SHEETS_GAME_ADJACENT_CATEGORY_LABEL_KEYS = frozenset(
    key
    for term in _GOOGLE_SHEETS_GAME_ADJACENT_CATEGORY_LABEL_TERMS
    for key in _google_sheets_category_label_keys(term)
)


def _google_sheets_category_term_matches(value: Any, term_keys: frozenset[str]) -> bool:
    return bool(_google_sheets_category_label_keys(value) & term_keys)


def _is_google_sheets_exact_category_label(value: Any) -> bool:
    return _google_sheets_category_term_matches(value, _GOOGLE_SHEETS_CATEGORY_LABEL_KEYS)


def _looks_like_google_sheets_residual_category_label(value: Any) -> bool:
    raw = clean_text(value)
    if not re.fullmatch(r"[A-Za-z0-9]+(?:-[A-Za-z0-9&]+)+", raw):
        return False
    tokens = set(_google_sheets_slug_tokens(raw))
    if not tokens or tokens & _GOOGLE_SHEETS_RESIDUAL_CATEGORY_VETO_TOKENS:
        return False
    return bool(tokens & _GOOGLE_SHEETS_RESIDUAL_CATEGORY_TOKENS)


def _is_google_sheets_category_label(value: Any) -> bool:
    return _is_google_sheets_exact_category_label(
        value
    ) or _looks_like_google_sheets_residual_category_label(value)


def _is_google_sheets_game_adjacent_category_label(value: Any) -> bool:
    if _google_sheets_category_term_matches(
        value,
        _GOOGLE_SHEETS_GAME_ADJACENT_CATEGORY_LABEL_KEYS,
    ):
        return True
    if not _looks_like_google_sheets_residual_category_label(value):
        return False
    return bool(
        set(_google_sheets_slug_tokens(value))
        & _GOOGLE_SHEETS_RESIDUAL_GAME_ADJACENT_CATEGORY_TOKENS
    )


_GOOGLE_SHEETS_TITLE_SLUG_STOP_SEGMENTS = frozenset(
    {
        "apply",
        "career",
        "careers",
        "detail",
        "details",
        "en",
        "en-us",
        "external",
        "job",
        "job-detail",
        "job-details",
        "jobs",
        "listing",
        "openings",
        "opportunities",
        "position",
        "positions",
        "search",
        "vacancies",
        "vacancy",
        "view",
    }
)
_GOOGLE_SHEETS_TITLE_SLUG_REJECT_TRAILING_TOKENS = frozenset(
    {"careers", "jobs", "openings", "opportunities", "search"}
)
_GOOGLE_SHEETS_TITLECASE_UPPER_TOKENS = frozenset(
    {
        "2d",
        "3d",
        "ai",
        "api",
        "ar",
        "b2b",
        "b2c",
        "c#",
        "c++",
        "crm",
        "cfx",
        "fx",
        "hr",
        "ios",
        "ip",
        "it",
        "qa",
        "td",
        "ui",
        "uk",
        "us",
        "ux",
        "vr",
        "xr",
    }
)
_GOOGLE_SHEETS_REPAIRABLE_BROAD_ROLE_TOKENS = frozenset(
    {
        "3d",
        "animation",
        "animator",
        "animators",
        "cinematic",
        "cinematics",
        "technical",
    }
)
_GOOGLE_SHEETS_ANIMATION_FAMILY_TOKENS = frozenset({"animation", "animator", "animators"})
_GOOGLE_SHEETS_SPECIFIC_TITLE_TOKENS = frozenset(
    {
        "advanced",
        "associate",
        "cinematic",
        "cinematics",
        "expert",
        "lead",
        "principal",
        "senior",
        "sr",
        "staff",
        "technical",
    }
)
_GOOGLE_SHEETS_TITLE_EVIDENCE_TOKENS = frozenset(
    {
        "account",
        "administrator",
        "analyst",
        "analytics",
        "animation",
        "animator",
        "architect",
        "art",
        "artist",
        "assistant",
        "associate",
        "backend",
        "brand",
        "builder",
        "business",
        "c++",
        "cinematic",
        "client",
        "community",
        "concept",
        "consultant",
        "content",
        "coordinator",
        "counsel",
        "creative",
        "customer",
        "data",
        "designer",
        "developer",
        "development",
        "devops",
        "director",
        "economy",
        "engineer",
        "engineering",
        "environment",
        "executive",
        "frontend",
        "full",
        "gameplay",
        "generalist",
        "graphic",
        "head",
        "intern",
        "internship",
        "lead",
        "legal",
        "manager",
        "marketing",
        "material",
        "monetization",
        "operations",
        "owner",
        "producer",
        "product",
        "programmer",
        "project",
        "qa",
        "receptionist",
        "recruiter",
        "research",
        "researcher",
        "sales",
        "senior",
        "software",
        "specialist",
        "stack",
        "strategist",
        "strategy",
        "support",
        "systems",
        "td",
        "technical",
        "tester",
        "texture",
        "ui",
        "unity",
        "unreal",
        "user",
        "ux",
        "video",
        "web3",
        "writer",
    }
)
_GOOGLE_SHEETS_RESIDUAL_CATEGORY_TOKENS = frozenset(
    {
        "account",
        "administrative",
        "analysis",
        "animation",
        "art",
        "audio",
        "business",
        "campaign",
        "community",
        "concept",
        "content",
        "customer",
        "cyber",
        "data",
        "design",
        "development",
        "devops",
        "editing",
        "engineering",
        "environment",
        "finance",
        "frontend",
        "game",
        "graphic",
        "influencer",
        "infrastructure",
        "legal",
        "level",
        "live",
        "localization",
        "management",
        "marketing",
        "media",
        "motion",
        "network",
        "operations",
        "production",
        "program",
        "project",
        "public",
        "quality",
        "relation",
        "relations",
        "research",
        "sales",
        "security",
        "social",
        "software",
        "sound",
        "system",
        "technical",
        "testing",
        "ui",
        "ux",
        "video",
        "web",
    }
)
_GOOGLE_SHEETS_RESIDUAL_CATEGORY_VETO_TOKENS = frozenset(
    {
        "administrator",
        "analyst",
        "architect",
        "artist",
        "assistant",
        "associate",
        "consultant",
        "coordinator",
        "designer",
        "developer",
        "director",
        "engineer",
        "executive",
        "intern",
        "internship",
        "junior",
        "lead",
        "manager",
        "principal",
        "producer",
        "programmer",
        "recruiter",
        "researcher",
        "senior",
        "specialist",
        "staff",
        "tester",
        "writer",
    }
)
_GOOGLE_SHEETS_RESIDUAL_GAME_ADJACENT_CATEGORY_TOKENS = frozenset(
    {
        "animation",
        "art",
        "audio",
        "community",
        "concept",
        "design",
        "editing",
        "game",
        "graphic",
        "influencer",
        "level",
        "live",
        "localization",
        "marketing",
        "media",
        "motion",
        "production",
        "rendering",
        "social",
        "sound",
        "technical",
        "ui",
        "ux",
        "video",
    }
)


def _google_sheets_slug_tokens(value: Any) -> list[str]:
    raw = clean_text(value)
    if not raw:
        return []
    return [
        token.lower() for token in re.findall(r"[A-Za-z0-9+#]+", raw.replace("&", " ")) if token
    ]


def _google_sheets_slug_has_title_evidence(value: Any) -> bool:
    return bool(set(_google_sheets_slug_tokens(value)) & _GOOGLE_SHEETS_TITLE_EVIDENCE_TOKENS)


def _google_sheets_slug_identity_key(value: Any) -> str:
    tokens = [token for token in _google_sheets_slug_tokens(value) if not token.isdigit()]
    return " ".join(tokens)


def _google_sheets_compact_id_pattern() -> str:
    return r"(?=[a-z0-9]*\d)[a-z0-9]{10,}"


def _looks_like_google_sheets_opaque_slug_segment(segment: str) -> bool:
    normalized = segment.strip().strip("-_").lower()
    if not normalized:
        return True
    if normalized in _GOOGLE_SHEETS_TITLE_SLUG_STOP_SEGMENTS:
        return True
    if re.fullmatch(r"(?=[a-z0-9.]*\d)[a-z0-9]{1,4}\.[a-z0-9]{2,6}", normalized):
        return True
    if re.fullmatch(r"(?:r|jr|req|job)?[-_]?\d{4,}", normalized):
        return True
    if re.fullmatch(
        r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
        normalized,
    ):
        return True
    compact = re.sub(r"[-_]", "", normalized)
    if re.fullmatch(r"[0-9a-f]{16,}", compact):
        return True
    if re.fullmatch(_google_sheets_compact_id_pattern(), normalized):
        return True
    return bool(
        re.fullmatch(r"[a-z0-9]{12,}", compact)
        and compact == normalized
        and re.search(r"\d", compact)
    )


def _strip_google_sheets_title_slug_ids(segment: str) -> str:
    slug = unquote(segment or "").strip().strip("/").strip("-_")
    if not slug:
        return ""
    compact_id = _google_sheets_compact_id_pattern()
    strip_patterns = (
        rf"(?P<id>{compact_id})[-_]+(?P<rest>.+)",
        rf"(?P<rest>.+?)[-_]+(?P<id>{compact_id})",
        r"\d{6,}[-_]+(?P<rest>.+)",
        r"(?P<rest>.+?)[-_]+(?:r|jr|req|job|wd)?[-_]?\d{3,}[a-z0-9]*(?:[-_]\d+)?",
        (
            r"(?P<rest>.+?)[-_]+"
            r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
        ),
    )
    changed = True
    while changed:
        changed = False
        for pattern in strip_patterns:
            match = re.fullmatch(pattern, slug, flags=re.IGNORECASE)
            if not match:
                continue
            rest = match.group("rest").strip().strip("-_")
            if not _google_sheets_slug_has_title_evidence(rest):
                continue
            slug = rest
            changed = True
            break
    return slug.strip().strip("-_")


def _google_sheets_titlecase_from_slug_text(text: str) -> str:
    words = re.findall(r"[A-Za-z0-9+#]+", text)
    title_words: list[str] = []
    for word in words:
        lower = word.lower()
        if lower in _GOOGLE_SHEETS_TITLECASE_UPPER_TOKENS:
            title_words.append(lower.upper())
        elif re.fullmatch(r"[a-z]\d[a-z0-9]*", lower):
            title_words.append(lower.upper())
        else:
            title_words.append(lower.capitalize())
    return " ".join(title_words)


def _google_sheets_title_tokens(value: Any) -> list[str]:
    return _google_sheets_slug_tokens(value)


def _google_sheets_animation_family(value: Any) -> set[str]:
    tokens = set(_google_sheets_title_tokens(value))
    return {"animation"} if tokens & _GOOGLE_SHEETS_ANIMATION_FAMILY_TOKENS else set()


def _is_google_sheets_repairable_broad_title(value: Any) -> bool:
    tokens = _google_sheets_title_tokens(value)
    if not tokens or len(tokens) > 3:
        return False
    token_set = set(tokens)
    return bool(token_set & _GOOGLE_SHEETS_ANIMATION_FAMILY_TOKENS) and token_set.issubset(
        _GOOGLE_SHEETS_REPAIRABLE_BROAD_ROLE_TOKENS
    )


def _is_stricter_same_family_google_sheets_title(original: str, candidate: str) -> bool:
    if norm_text(original) == norm_text(candidate):
        return False
    original_family = _google_sheets_animation_family(original)
    if not original_family:
        return False
    if not original_family & _google_sheets_animation_family(candidate):
        return False
    original_tokens = _google_sheets_title_tokens(original)
    candidate_tokens = _google_sheets_title_tokens(candidate)
    if len(candidate_tokens) <= len(original_tokens):
        return False
    original_required_tokens = set(original_tokens) - _GOOGLE_SHEETS_ANIMATION_FAMILY_TOKENS
    if not original_required_tokens.issubset(set(candidate_tokens)):
        return False
    candidate_gain = set(candidate_tokens) - set(original_tokens)
    return bool(candidate_gain & _GOOGLE_SHEETS_SPECIFIC_TITLE_TOKENS)


def _should_accept_google_sheets_repaired_title(original: str, candidate: str) -> bool:
    if not _is_google_sheets_repairable_broad_title(original):
        return True
    return _is_stricter_same_family_google_sheets_title(original, candidate)


def _google_sheets_title_candidate_from_slug(
    segment: str,
    *,
    blocked_identity_keys: set[str] | None = None,
) -> str:
    if _looks_like_google_sheets_opaque_slug_segment(segment):
        return ""
    raw_slug = unquote(segment or "").strip().strip("/").strip("-_")
    slug = _strip_google_sheets_title_slug_ids(segment)
    if _looks_like_google_sheets_opaque_slug_segment(slug):
        return ""
    stripped_ats_id = slug.lower() != raw_slug.lower()
    has_title_evidence = _google_sheets_slug_has_title_evidence(slug)
    if not has_title_evidence and _google_sheets_slug_identity_key(slug) in (
        blocked_identity_keys or set()
    ):
        return ""
    if not has_title_evidence:
        return ""
    slug_text = re.sub(r"[-_+]+", " ", slug)
    slug_text = re.sub(r"\s+", " ", slug_text).strip()
    title = _google_sheets_titlecase_from_slug_text(slug_text)
    if not title:
        return ""
    normalized_words = norm_text(title).split()
    alpha_words = [word for word in normalized_words if re.search(r"[a-z]", word)]
    if len(alpha_words) < 2 and not stripped_ats_id:
        return ""
    if len(normalized_words) > 14:
        return ""
    if normalized_words[-1] in _GOOGLE_SHEETS_TITLE_SLUG_REJECT_TRAILING_TOKENS:
        return ""
    if _is_google_sheets_category_label(title):
        return ""
    return title


def _google_sheets_title_slug_segments(job_link: str) -> list[str]:
    parsed = urlparse(clean_text(job_link) or "")
    host = parsed.netloc.lower().removeprefix("www.")
    parts = [unquote(part).strip() for part in parsed.path.split("/") if part.strip()]
    if not host or not parts:
        return []

    candidates: list[str] = []
    if host == "jobs.smartrecruiters.com" and len(parts) >= 2:
        candidates.append(parts[-1])
    if host_matches_domain(host, "myworkdayjobs.com"):
        candidates.append(parts[-1])

    lowered_parts = [part.lower() for part in parts]
    for marker in ("job", "jobs", "job-detail", "job-details"):
        if marker in lowered_parts:
            marker_index = lowered_parts.index(marker)
            if marker_index > 0:
                candidates.append(parts[marker_index - 1])
            if marker_index + 1 < len(parts):
                candidates.append(parts[marker_index + 1])

    candidates.extend(reversed(parts))

    seen: set[str] = set()
    ordered_candidates: list[str] = []
    for candidate in candidates:
        key = candidate.lower()
        if key in seen:
            continue
        seen.add(key)
        ordered_candidates.append(candidate)
    return ordered_candidates


def _google_sheets_blocked_title_identity_keys(job_link: str, company: str) -> set[str]:
    blocked = {_google_sheets_slug_identity_key(company)}
    parsed = urlparse(clean_text(job_link) or "")
    parts = [unquote(part).strip() for part in parsed.path.split("/") if part.strip()]
    lowered_parts = [part.lower() for part in parts]

    for marker in ("j", "p", "job", "jobs", "job-detail", "job-details"):
        if marker not in lowered_parts:
            continue
        marker_index = lowered_parts.index(marker)
        if marker_index > 0:
            blocked.add(_google_sheets_slug_identity_key(parts[marker_index - 1]))

    if len(parts) >= 2 and _looks_like_google_sheets_opaque_slug_segment(parts[-1]):
        blocked.add(_google_sheets_slug_identity_key(parts[-2]))

    blocked.discard("")
    return blocked


def _derive_google_sheets_title_from_url(
    *,
    source: str,
    title: str,
    company: str,
    job_link: str,
) -> str:
    if not clean_text(source).startswith("google_sheets"):
        return ""
    repairable_broad_title = _is_google_sheets_repairable_broad_title(title)
    if not _is_google_sheets_category_label(title) and not repairable_broad_title:
        return ""
    blocked_identity_keys = _google_sheets_blocked_title_identity_keys(job_link, company)
    for segment in _google_sheets_title_slug_segments(job_link):
        candidate = _google_sheets_title_candidate_from_slug(
            segment,
            blocked_identity_keys=blocked_identity_keys,
        )
        if candidate and _should_accept_google_sheets_repaired_title(title, candidate):
            return candidate
    return ""


def _validated_opening_title_or_reason(
    *,
    title: str,
    job_link: str,
    source: str,
) -> tuple[str | None, str]:
    if looks_like_source_specific_static_noise_row(
        title=title,
        job_link=job_link,
        source_name=source,
    ):
        return None, "non_job_static_page"
    return title, ""


def _google_sheets_original_title_or_category_drop(
    title: str,
    *,
    is_category_title: bool,
) -> tuple[str | None, str]:
    if is_category_title:
        return None, "google_sheets_category_row"
    return title, ""


def _validated_google_sheets_candidate_title_or_reason(
    *,
    original_title: str,
    candidate_title: str,
    is_category_title: bool,
    job_link: str,
    source: str,
) -> tuple[str | None, str]:
    if _is_google_sheets_category_label(candidate_title):
        return _google_sheets_original_title_or_category_drop(
            original_title,
            is_category_title=is_category_title,
        )
    return _validated_opening_title_or_reason(
        title=candidate_title,
        job_link=job_link,
        source=source,
    )


def _should_reject_google_sheets_hydrated_title(
    original_title: str,
    hydrated_title: str,
) -> bool:
    if not hydrated_title:
        return True
    if _is_google_sheets_category_label(hydrated_title):
        return True
    return not _should_accept_google_sheets_repaired_title(original_title, hydrated_title)


def _google_sheets_repaired_title_or_reason(
    *,
    title: str,
    source: str,
    company: str,
    job_link: str,
    title_hydration_resolver: GoogleSheetsProviderTitleResolver | None,
) -> tuple[str | None, str]:
    if _looks_like_google_sheets_category_row_noise(
        source=source,
        title=title,
        company=company,
        job_link=job_link,
    ):
        return None, "google_sheets_category_row"

    is_category_title = clean_text(source).startswith(
        "google_sheets"
    ) and _is_google_sheets_category_label(title)
    repaired_title = _derive_google_sheets_title_from_url(
        source=source,
        title=title,
        company=company,
        job_link=job_link,
    )
    if repaired_title:
        return _validated_google_sheets_candidate_title_or_reason(
            original_title=title,
            candidate_title=repaired_title,
            is_category_title=is_category_title,
            job_link=job_link,
            source=source,
        )

    if title_hydration_resolver is None:
        return _google_sheets_original_title_or_category_drop(
            title,
            is_category_title=is_category_title,
        )
    hydrated_title = title_hydration_resolver.resolve_title(job_link)
    if _should_reject_google_sheets_hydrated_title(title, hydrated_title):
        return _google_sheets_original_title_or_category_drop(
            title,
            is_category_title=is_category_title,
        )
    return _validated_opening_title_or_reason(
        title=hydrated_title,
        job_link=job_link,
        source=source,
    )


_GOOGLE_SHEETS_TITLE_HYDRATION_STAT_KEYS = (
    "title_hydration_candidates",
    "title_hydration_feed_fetches",
    "title_hydration_cache_hits",
    "title_hydration_repaired",
    "title_hydration_missed",
    "title_hydration_errors",
    "title_hydration_ms",
)
_GOOGLE_SHEETS_CATEGORY_LINK_STAT_KEYS = (
    "category_link_status_candidates",
    "category_link_status_checked",
    "category_link_status_cache_hits",
    "category_link_status_stale_dropped",
    "category_link_status_errors",
    "category_link_status_ms",
)
_GOOGLE_SHEETS_GREENHOUSE_HOSTS = frozenset(
    {
        "boards.greenhouse.io",
        "job-boards.greenhouse.io",
        "job-boards.eu.greenhouse.io",
    }
)
_GOOGLE_SHEETS_LEVER_HOSTS = frozenset({"jobs.lever.co", "jobs.eu.lever.co"})
_GOOGLE_SHEETS_WORKABLE_HOSTS = frozenset({"apply.workable.com"})
_GOOGLE_SHEETS_ASHBY_HOSTS = frozenset({"jobs.ashbyhq.com"})


def _google_sheets_provider_title_target(
    job_link: str,
) -> tuple[str, str, str, tuple[str, ...]] | None:
    parsed = urlparse(clean_text(job_link) or "")
    host = parsed.netloc.lower().removeprefix("www.")
    parts = [unquote(part).strip() for part in parsed.path.split("/") if part.strip()]
    normalized_link = normalize_url(job_link)
    if host in _GOOGLE_SHEETS_GREENHOUSE_HOSTS and len(parts) >= 3 and parts[1].lower() == "jobs":
        board_slug = clean_text(parts[0])
        job_id = clean_text(parts[2])
        if not board_slug or not job_id:
            return None
        feed_url = (
            "https://boards-api.greenhouse.io/v1/boards/"
            f"{quote(board_slug, safe='')}/jobs?content=true"
        )
        return "greenhouse", board_slug, feed_url, (f"id:{job_id}", f"url:{normalized_link}")
    if host in _GOOGLE_SHEETS_LEVER_HOSTS and len(parts) >= 2:
        account = clean_text(parts[0])
        posting_id = clean_text(parts[-1])
        if not account or not posting_id or posting_id.lower() == "jobs":
            return None
        feed_url = f"https://api.lever.co/v0/postings/{quote(account, safe='')}?mode=json"
        return "lever", account, feed_url, (f"id:{posting_id}", f"url:{normalized_link}")
    if host in _GOOGLE_SHEETS_WORKABLE_HOSTS and len(parts) >= 3 and parts[1].lower() == "j":
        account = clean_text(parts[0])
        shortcode = clean_text(parts[2])
        if not account or not shortcode:
            return None
        feed_url = (
            "https://apply.workable.com/api/v1/widget/accounts/"
            f"{quote(account, safe='')}?details=true"
        )
        return "workable", account, feed_url, (f"id:{shortcode}", f"url:{normalized_link}")
    if host in _GOOGLE_SHEETS_ASHBY_HOSTS and len(parts) >= 2:
        board = clean_text(parts[0])
        posting_id = clean_text(parts[-1])
        if not board or not posting_id or posting_id.lower() == "jobs":
            return None
        feed_url = f"https://jobs.ashbyhq.com/{quote(board, safe='')}"
        return "ashby", board, feed_url, (f"id:{posting_id}", f"url:{normalized_link}")
    return None


def _google_sheets_provider_title_lookup_keys(provider: str, row: dict[str, Any]) -> set[str]:
    keys: set[str] = set()
    for value in (
        row.get("id"),
        row.get("internal_job_id"),
        row.get("requisitionCode"),
        row.get("shortcode"),
    ):
        text = clean_text(value)
        if text:
            keys.add(f"id:{text}")
    if provider == "greenhouse":
        urls = (row.get("absolute_url"), row.get("url"))
    elif provider == "workable":
        urls = (row.get("url"), row.get("shortlink"))
    else:
        urls = (row.get("hostedUrl"), row.get("applyUrl"), row.get("url"))
    for value in urls:
        normalized = normalize_url(value)
        if normalized:
            keys.add(f"url:{normalized}")
    return keys


def _google_sheets_provider_title_from_row(provider: str, row: dict[str, Any]) -> str:
    if provider == "greenhouse":
        return sanitize_public_text(row.get("title") or row.get("text"))
    return sanitize_public_text(row.get("text") or row.get("title"))


def _google_sheets_provider_title_map(provider: str, payload: Any) -> dict[str, str]:
    if provider == "greenhouse":
        rows = payload.get("jobs") if isinstance(payload, dict) else None
    elif provider == "workable":
        rows = payload.get("jobs") if isinstance(payload, dict) else None
    else:
        rows = payload if isinstance(payload, list) else None
    if not isinstance(rows, list):
        return {}
    title_by_key: dict[str, str] = {}
    for row_value in rows:
        if not isinstance(row_value, dict):
            continue
        title = _google_sheets_provider_title_from_row(provider, row_value)
        if not title:
            continue
        for key in _google_sheets_provider_title_lookup_keys(provider, row_value):
            title_by_key.setdefault(key, title)
    return title_by_key


def _google_sheets_ashby_title_map(feed_url: str, html_text: str) -> dict[str, str]:
    rows = parse_ashby_jobs_from_html(html_text, feed_url, "")
    title_by_key: dict[str, str] = {}
    for row in rows:
        title = sanitize_public_text(row.get("title"))
        if not title:
            continue
        source_job_id = clean_text(row.get("sourceJobId"))
        if source_job_id.startswith("ashby:"):
            title_by_key.setdefault(f"id:{source_job_id.removeprefix('ashby:')}", title)
        normalized = normalize_url(row.get("jobLink"))
        if normalized:
            title_by_key.setdefault(f"url:{normalized}", title)
    return title_by_key


class GoogleSheetsProviderTitleResolver:
    """Per-run Google Sheets title resolver backed by provider JSON feeds."""

    def __init__(
        self,
        *,
        fetch_text: Callable[[str, int], str],
        timeout_s: int,
        retries: int,
        backoff_s: float,
    ) -> None:
        self._fetch_text = fetch_text
        self._timeout_s = max(1, int(timeout_s or 1))
        self._retries = max(0, int(retries or 0))
        self._backoff_s = max(0.0, float(backoff_s or 0.0))
        self._cache: dict[tuple[str, str], dict[str, str]] = {}
        self._row_feed_uses: set[tuple[str, str]] = set()
        self._stats: Counter[str] = Counter()
        self._lock = threading.Lock()

    def supports(self, job_link: str) -> bool:
        return _google_sheets_provider_title_target(job_link) is not None

    def prefetch(
        self,
        job_links: Sequence[str],
        *,
        concurrency: int = 1,
        progress_callback: Callable[..., Any] | None = None,
    ) -> None:
        targets = [
            target
            for link in job_links
            if (target := _google_sheets_provider_title_target(link)) is not None
        ]
        feed_targets: dict[tuple[str, str], tuple[str, str, str, tuple[str, ...]]] = {
            (provider, feed_key): target
            for target in targets
            for provider, feed_key, _feed_url, _lookup_keys in (target,)
        }
        pending = [target for key, target in feed_targets.items() if key not in self._cache]
        if not pending:
            return
        max_workers = max(1, min(int(concurrency or 1), len(pending)))

        def emit_title_progress(completed: int, *, force: bool = False) -> None:
            if progress_callback is None:
                return
            if not force and completed < len(pending):
                return
            stats = self.snapshot_stats()
            progress_callback(
                phase_key="hydrating_sheet_titles",
                phase_label="Hydrating sheet titles",
                counts={
                    "titleHydrationCandidates": len(targets),
                    "titleHydrationFeedsPending": len(pending),
                    "titleHydrationFeedsCompleted": completed,
                    "titleHydrationFeedFetches": int(
                        stats.get("title_hydration_feed_fetches") or 0
                    ),
                    "titleHydrationErrors": int(stats.get("title_hydration_errors") or 0),
                },
                message=(
                    "Hydrating Google Sheets provider titles: "
                    f"{completed} of {len(pending)} feeds checked."
                ),
            )

        emit_title_progress(0, force=True)
        if max_workers <= 1:
            for completed, (provider, feed_key, feed_url, _lookup_keys) in enumerate(
                pending, start=1
            ):
                self._ensure_feed(provider, feed_key, feed_url)
                emit_title_progress(completed)
            return
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(self._ensure_feed, provider, feed_key, feed_url)
                for provider, feed_key, feed_url, _lookup_keys in pending
            ]
            for completed, future in enumerate(as_completed(futures), start=1):
                future.result()
                emit_title_progress(completed)

    def resolve_title(self, job_link: str) -> str:
        target = _google_sheets_provider_title_target(job_link)
        if target is None:
            return ""
        provider, feed_key, feed_url, lookup_keys = target
        cache_key = (provider, feed_key)
        with self._lock:
            self._stats["title_hydration_candidates"] += 1
            if cache_key in self._row_feed_uses:
                self._stats["title_hydration_cache_hits"] += 1
            else:
                self._row_feed_uses.add(cache_key)
        title_by_key = self._ensure_feed(provider, feed_key, feed_url)
        for lookup_key in lookup_keys:
            title = sanitize_public_text(title_by_key.get(lookup_key))
            if title and not _is_google_sheets_category_label(title):
                with self._lock:
                    self._stats["title_hydration_repaired"] += 1
                return title
        with self._lock:
            self._stats["title_hydration_missed"] += 1
        return ""

    def snapshot_stats(self) -> dict[str, int]:
        with self._lock:
            return {
                key: int(self._stats.get(key, 0))
                for key in _GOOGLE_SHEETS_TITLE_HYDRATION_STAT_KEYS
            }

    def _ensure_feed(self, provider: str, feed_key: str, feed_url: str) -> dict[str, str]:
        cache_key = (provider, feed_key)
        with self._lock:
            cached = self._cache.get(cache_key)
            if cached is not None:
                return cached
        started = time.perf_counter()
        title_by_key: dict[str, str] = {}
        try:
            with self._lock:
                self._stats["title_hydration_feed_fetches"] += 1
            text = fetch_with_retries(
                feed_url,
                self._fetch_text,
                self._timeout_s,
                self._retries,
                self._backoff_s,
            )
            if provider == "ashby":
                title_by_key = _google_sheets_ashby_title_map(feed_url, text)
            else:
                title_by_key = _google_sheets_provider_title_map(provider, json.loads(text))
        except (RuntimeError, OSError, ValueError):
            with self._lock:
                self._stats["title_hydration_errors"] += 1
            title_by_key = {}
        finally:
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            with self._lock:
                self._stats["title_hydration_ms"] += elapsed_ms
                self._cache[cache_key] = title_by_key
        return title_by_key


class GoogleSheetsCategoryLinkStatusResolver:
    """Per-run liveness resolver for suspicious Google Sheets category-title rows."""

    def __init__(
        self,
        *,
        timeout_s: int,
        fetch_status: Callable[[str, int], int] | None = None,
    ) -> None:
        self._timeout_s = max(1, int(timeout_s or 1))
        self._fetch_status = fetch_status or self._default_fetch_status
        self._cache: dict[str, int] = {}
        self._stats: Counter[str] = Counter()
        self._lock = threading.Lock()

    def prefetch(
        self,
        job_links: Sequence[str],
        *,
        concurrency: int = 1,
        progress_callback: Callable[..., Any] | None = None,
    ) -> None:
        normalized_links = [normalize_url(link) for link in job_links if normalize_url(link)]
        unique_links = list(dict.fromkeys(normalized_links))
        with self._lock:
            self._stats["category_link_status_candidates"] += len(normalized_links)
            pending = [link for link in unique_links if link not in self._cache]
            self._stats["category_link_status_cache_hits"] += len(unique_links) - len(pending)
        if not pending:
            return
        started = time.perf_counter()
        last_progress_at = started

        def emit_status_progress(completed: int, *, force: bool = False) -> None:
            nonlocal last_progress_at
            if progress_callback is None:
                return
            now = time.perf_counter()
            if not force and completed < len(pending) and now - last_progress_at < 2.0:
                return
            last_progress_at = now
            stats = self.snapshot_stats()
            progress_callback(
                phase_key="checking_category_links",
                phase_label="Checking category links",
                counts={
                    "categoryLinkStatusCandidates": len(normalized_links),
                    "categoryLinkStatusChecked": int(
                        stats.get("category_link_status_checked") or 0
                    ),
                    "categoryLinkStatusCompleted": completed,
                    "categoryLinkStatusStaleDropped": int(
                        stats.get("category_link_status_stale_dropped") or 0
                    ),
                    "categoryLinkStatusErrors": int(stats.get("category_link_status_errors") or 0),
                },
                message=(
                    "Checking repaired Google Sheets category links: "
                    f"{completed} of {len(pending)} checked."
                ),
            )

        emit_status_progress(0, force=True)
        max_workers = max(1, min(int(concurrency or 1), len(pending)))
        try:
            if max_workers <= 1:
                for completed, link in enumerate(pending, start=1):
                    self._ensure_status(link, track_ms=False)
                    emit_status_progress(completed)
                return
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [
                    executor.submit(self._ensure_status, link, track_ms=False) for link in pending
                ]
                for completed, future in enumerate(as_completed(futures), start=1):
                    future.result()
                    emit_status_progress(completed)
        finally:
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            with self._lock:
                self._stats["category_link_status_ms"] += elapsed_ms
            emit_status_progress(len(pending), force=True)

    def is_stale(self, job_link: str) -> bool:
        normalized_link = normalize_url(job_link)
        if not normalized_link:
            return False
        return self._ensure_status(normalized_link) in {404, 410}

    def note_stale_drop(self) -> None:
        with self._lock:
            self._stats["category_link_status_stale_dropped"] += 1

    def snapshot_stats(self) -> dict[str, int]:
        with self._lock:
            return {
                key: int(self._stats.get(key, 0)) for key in _GOOGLE_SHEETS_CATEGORY_LINK_STAT_KEYS
            }

    def _ensure_status(self, job_link: str, *, track_ms: bool = True) -> int:
        normalized_link = normalize_url(job_link)
        with self._lock:
            cached = self._cache.get(normalized_link)
            if cached is not None:
                return cached
        started = time.perf_counter() if track_ms else 0.0
        status = 0
        try:
            status = int(self._fetch_status(normalized_link, self._timeout_s) or 0)
            with self._lock:
                self._stats["category_link_status_checked"] += 1
        except (RuntimeError, OSError, ValueError):
            with self._lock:
                self._stats["category_link_status_errors"] += 1
            status = 0
        finally:
            with self._lock:
                if track_ms:
                    self._stats["category_link_status_ms"] += int(
                        (time.perf_counter() - started) * 1000
                    )
                self._cache[normalized_link] = status
        return status

    @staticmethod
    def _default_fetch_status(url: str, timeout_s: int) -> int:
        last_error: Exception | None = None
        for method in ("HEAD", "GET"):
            try:
                request = urllib.request.Request(
                    url,
                    headers={"User-Agent": "Mozilla/5.0"},
                    method=method,
                )
                with urllib.request.urlopen(request, timeout=max(1, int(timeout_s or 1))) as resp:
                    return int(getattr(resp, "status", 0) or 0)
            except urllib.error.HTTPError as exc:
                code = int(getattr(exc, "code", 0) or 0)
                if method == "HEAD" and code in {400, 403, 405, 429, 500, 501, 503}:
                    last_error = exc
                    continue
                return code
            except (OSError, ValueError) as exc:
                last_error = exc
                if method == "HEAD":
                    continue
                break
        _ = last_error
        return 0


def _google_sheets_url_evidence_text(job_link: str) -> str:
    parsed = urlparse(clean_text(job_link) or "")
    return _normalized_evidence_text(parsed.netloc, parsed.path)


_GOOGLE_SHEETS_EMPLOYER_LEGAL_SUFFIXES = (
    "corporation",
    "company",
    "limited",
    "studio",
    "studios",
    "group",
    "gmbh",
    "inc",
    "llc",
    "ltd",
    "plc",
    "pvt",
)


def _google_sheets_link_employer_candidate(job_link: str) -> str:
    parsed = urlparse(clean_text(job_link) or "")
    host = parsed.netloc.lower().removeprefix("www.")
    parts = [unquote(part) for part in parsed.path.split("/") if part]
    if host == "jobs.smartrecruiters.com" and parts:
        return parts[0]
    if host == "himalayas.app" and len(parts) >= 2 and parts[0].lower() == "companies":
        return parts[1]
    if host == "shine.com" and len(parts) >= 4 and parts[0].lower() == "jobs":
        return parts[-2]
    if host == "bebee.com":
        path_evidence = _normalized_evidence_text(*parts)
        for marker in _GOOGLE_SHEETS_BEBEE_NON_GAME_EMPLOYER_MARKERS:
            if _contains_evidence_term(path_evidence, marker):
                return marker
    return ""


def _google_sheets_employer_identity_key(value: Any) -> str:
    raw = clean_text(value)
    if not raw:
        return ""
    raw = unquote(raw)
    raw = re.sub(r"(?<=[A-Z])(?=[A-Z][a-z])", " ", raw)
    raw = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", raw)
    compact = _normalized_evidence_text(raw).replace(" ", "")
    for suffix in _GOOGLE_SHEETS_EMPLOYER_LEGAL_SUFFIXES:
        if compact.endswith(suffix) and len(compact) > len(suffix) + 3:
            compact = compact[: -len(suffix)]
            break
    return compact


def _has_google_sheets_link_employer_mismatch_without_game_evidence(
    company: str, job_link: str
) -> bool:
    link_employer = _google_sheets_link_employer_candidate(job_link)
    if not link_employer:
        return False
    company_key = _google_sheets_employer_identity_key(company)
    link_employer_key = _google_sheets_employer_identity_key(link_employer)
    unknown_key = _google_sheets_employer_identity_key(UNKNOWN_COMPANY_LABEL)
    if not company_key or not link_employer_key or company_key == unknown_key:
        return False
    if company_key == link_employer_key:
        return False
    if company_key in link_employer_key or link_employer_key in company_key:
        return False
    link_evidence_text = _normalized_evidence_text(link_employer)
    return not any(
        _contains_evidence_term(link_evidence_text, term)
        for term in _GOOGLE_SHEETS_LINK_EMPLOYER_GAME_EVIDENCE_TERMS
    )


def _has_google_sheets_non_game_evidence(company: str, job_link: str) -> bool:
    if _has_google_sheets_link_employer_mismatch_without_game_evidence(company, job_link):
        return True
    evidence_text = _normalized_evidence_text(
        company,
        _google_sheets_url_evidence_text(job_link),
    )
    return any(
        _contains_evidence_term(evidence_text, term)
        for term in _GOOGLE_SHEETS_NON_GAME_EVIDENCE_TERMS
    )


def _has_google_sheets_plausible_game_evidence(company: str, job_link: str) -> bool:
    evidence_text = _normalized_evidence_text(
        company,
        _google_sheets_url_evidence_text(job_link),
    )
    return any(
        _contains_evidence_term(evidence_text, term) for term in _GOOGLE_SHEETS_GAME_EVIDENCE_TERMS
    )


def _looks_like_google_sheets_category_row_noise(
    *,
    source: str,
    title: str,
    company: str,
    job_link: str,
) -> bool:
    if not clean_text(source).startswith("google_sheets"):
        return False
    if not _is_google_sheets_category_label(title):
        return False
    if _has_google_sheets_non_game_evidence(company, job_link):
        return True
    if _is_google_sheets_game_adjacent_category_label(title):
        return False
    return not _has_google_sheets_plausible_game_evidence(company, job_link)


def reset_location_quality_audit() -> None:
    with _LOCATION_AUDIT_LOCK:
        _LOCATION_AUDIT_FIELD_COUNTS.clear()
        _LOCATION_AUDIT_REASON_COUNTS.clear()
        _LOCATION_AUDIT_EXAMPLES.clear()


def reset_sector_quality_audit() -> None:
    global _SECTOR_AUDIT_DOWNGRADED_COUNT
    with _SECTOR_AUDIT_LOCK:
        _SECTOR_AUDIT_DOWNGRADED_COUNT = 0
        _SECTOR_AUDIT_EXAMPLES.clear()


def snapshot_sector_quality_audit(*, total_rows: int = 0) -> dict[str, Any]:
    with _SECTOR_AUDIT_LOCK:
        return {
            "totalRows": max(0, int(total_rows or 0)),
            "downgradedGameSectorCount": int(_SECTOR_AUDIT_DOWNGRADED_COUNT),
            "examples": list(_SECTOR_AUDIT_EXAMPLES[:20]),
        }


def _record_location_quality_issue(
    *,
    field_name: str,
    reason: str,
    raw_value: Any,
    source: str,
    company: str,
    title: str,
    job_link: Any,
) -> None:
    clean_reason = clean_text(reason)
    clean_field = clean_text(field_name)
    if not clean_reason or not clean_field:
        return
    with _LOCATION_AUDIT_LOCK:
        _LOCATION_AUDIT_FIELD_COUNTS[clean_field] += 1
        _LOCATION_AUDIT_REASON_COUNTS[clean_reason] += 1
        if len(_LOCATION_AUDIT_EXAMPLES) < 20:
            _LOCATION_AUDIT_EXAMPLES.append(
                {
                    "company": clean_text(company),
                    "title": clean_text(title),
                    "source": clean_text(source),
                    "jobLink": clean_text(job_link),
                    "field": clean_field,
                    "reason": clean_reason,
                    "value": clean_text(raw_value),
                }
            )


def _looks_like_game_sector_label(value: Any) -> bool:
    return bool(re.search(r"\b(game|gaming|games|esports)\b", norm_text(value)))


def _record_sector_quality_issue(
    *,
    raw_sector: Any,
    normalized_sector: str,
    source: str,
    company: str,
    title: str,
    job_link: Any,
) -> None:
    global _SECTOR_AUDIT_DOWNGRADED_COUNT
    if normalized_sector != "Tech" or not _looks_like_game_sector_label(raw_sector):
        return
    with _SECTOR_AUDIT_LOCK:
        _SECTOR_AUDIT_DOWNGRADED_COUNT += 1
        if len(_SECTOR_AUDIT_EXAMPLES) < 20:
            _SECTOR_AUDIT_EXAMPLES.append(
                {
                    "company": clean_text(company),
                    "title": clean_text(title),
                    "source": clean_text(source),
                    "jobLink": clean_text(job_link),
                    "rawSector": clean_text(raw_sector),
                    "normalizedSector": normalized_sector,
                }
            )


def _resolve_job_link(
    *,
    raw: dict[str, Any],
    source: str,
    resolve_redirect_url: Callable[[str], str] | None,
    resolved_job_link: Any,
) -> tuple[str, str]:
    normalized_link_source = raw.get("jobLink") if resolved_job_link is None else resolved_job_link
    normalized_link = normalize_url(normalized_link_source)
    skip_redirect_resolution = norm_text(source) in REDIRECT_RESOLUTION_SKIP_SOURCES
    if (
        resolved_job_link is None
        and normalized_link
        and callable(resolve_redirect_url)
        and not skip_redirect_resolution
    ):
        try:
            resolved_link = normalize_url(resolve_redirect_url(normalized_link))
        except Exception:  # noqa: BLE001
            resolved_link = normalized_link
        if resolved_link:
            normalized_link = resolved_link
    return normalized_link, clean_text(raw.get("jobLink"))


def _normalize_source_bundle(value: Any) -> list[dict[str, Any]]:
    entries = value
    if isinstance(entries, str):
        try:
            entries = json.loads(entries)
        except json.JSONDecodeError:
            entries = []
    if not isinstance(entries, list):
        entries = []
    normalized_entries: list[dict[str, Any]] = []
    seen = set()
    for item in entries:
        if not isinstance(item, dict):
            continue
        normalized_item = {
            "source": clean_text(item.get("source")),
            "sourceJobId": clean_text(item.get("sourceJobId")),
            "jobLink": normalize_url(item.get("jobLink")),
            "postedAt": to_iso(item.get("postedAt")),
            "adapter": clean_text(item.get("adapter")),
            "studio": sanitize_public_text(item.get("studio")),
        }
        token = "|".join(
            [
                norm_text(normalized_item.get("source")),
                norm_text(normalized_item.get("sourceJobId")),
                norm_text(normalized_item.get("jobLink")),
            ]
        )
        if token in seen:
            continue
        seen.add(token)
        normalized_entries.append(normalized_item)
    return normalized_entries


def _default_source_bundle(
    *,
    raw: dict[str, Any],
    source: str,
    adapter: str,
    studio: str,
) -> list[dict[str, Any]]:
    return [
        {
            "source": source,
            "sourceJobId": clean_text(raw.get("sourceJobId") or raw.get("id")),
            "jobLink": normalize_url(raw.get("jobLink")),
            "postedAt": to_iso(raw.get("postedAt")),
            "adapter": adapter,
            "studio": studio,
        }
    ]


def _has_structured_location(details: dict[str, Any]) -> bool:
    detail_locations = details.get("locations") or []
    return (
        any(
            clean_text(item.get("city")) or clean_text(item.get("country"))
            for item in detail_locations
            if isinstance(item, dict)
        )
        or clean_text(details.get("city"))
        or clean_text(details.get("country"))
        not in {
            "",
            "Unknown",
        }
    )


def _details_with_city_country_fallback(
    *,
    value: Any,
    raw_city: Any,
    raw_country: Any,
) -> dict[str, Any]:
    details = normalize_location_details(value)
    if _has_structured_location(details):
        return details
    city_fragment = sanitize_location_text(raw_city, field_name="city")[0]
    country_fragment = sanitize_location_text(raw_country, field_name="country")[0]
    if not city_fragment and (
        not country_fragment or norm_text(country_fragment) in {"", "unknown"}
    ):
        return details
    raw_fragments = [city_fragment]
    if country_fragment and norm_text(country_fragment) != "unknown":
        raw_fragments.append(country_fragment)
    return normalize_location_details(", ".join(fragment for fragment in raw_fragments if fragment))


def _locations_from_details(details: dict[str, Any]) -> list[dict[str, str]]:
    locations = [
        {
            "city": sanitize_location_text(item.get("city"), field_name="city")[0],
            "country": normalize_country(
                sanitize_location_text(item.get("country"), field_name="country")[0]
            )
            if sanitize_location_text(item.get("country"), field_name="country")[0]
            else "",
        }
        for item in details.get("locations") or []
        if clean_text(item.get("city")) or clean_text(item.get("country"))
    ]
    if locations:
        return locations
    city_value = sanitize_location_text(details.get("city"), field_name="city")[0]
    country_value = sanitize_location_text(details.get("country"), field_name="country")[0]
    if not city_value and not country_value:
        return []
    return [
        {
            "city": city_value,
            "country": normalize_country(country_value) if country_value else "",
        }
    ]


def _normalize_job_locations(
    value: Any, *, raw_city: Any = "", raw_country: Any = ""
) -> list[dict[str, str]]:
    details = _details_with_city_country_fallback(
        value=value,
        raw_city=raw_city,
        raw_country=raw_country,
    )
    return _locations_from_details(details)


def _primary_location(normalized_locations: list[dict[str, str]]) -> dict[str, str]:
    return next(
        (item for item in normalized_locations if item.get("city") or item.get("country")),
        {},
    )


def _raw_city_primary_location(raw_city: Any) -> dict[str, Any]:
    raw_city_details = normalize_location_details(raw_city)
    raw_city_locations = raw_city_details.get("locations") or []
    return next(
        (
            item
            for item in raw_city_locations
            if isinstance(item, dict)
            and (clean_text(item.get("city")) or clean_text(item.get("country")))
        ),
        {},
    )


def _should_promote_primary_city(
    *,
    raw_city: Any,
    primary_location: dict[str, str],
    country_value: str,
) -> bool:
    raw_city_primary = _raw_city_primary_location(raw_city)
    return (
        clean_text(raw_city_primary.get("city")) == primary_location.get("city")
        and (
            not clean_text(raw_city_primary.get("country"))
            or clean_text(raw_city_primary.get("country"))
            == clean_text(primary_location.get("country"))
        )
        and norm_text(country_value) in {"", "unknown"}
    )


def _resolve_city_country_values(
    *,
    raw: dict[str, Any],
    primary_location: dict[str, str],
) -> tuple[str, str, str, str]:
    city_value, city_reason = sanitize_location_text(raw.get("city"), field_name="city")
    country_value, country_reason = sanitize_location_text(raw.get("country"), field_name="country")
    if not city_value and primary_location.get("city"):
        city_value = primary_location["city"]
    elif (
        city_value
        and primary_location.get("city")
        and _should_promote_primary_city(
            raw_city=raw.get("city"),
            primary_location=primary_location,
            country_value=country_value,
        )
    ):
        city_value = primary_location["city"]
    if (
        not country_value or country_reason or norm_text(country_value) == "unknown"
    ) and primary_location.get("country"):
        country_value = primary_location["country"]
        country_reason = ""
    if not country_value and norm_text(raw.get("city")) not in REMOTEISH_TOKENS:
        promoted_country = resolve_country_acceptance_value(raw.get("city"))
        if promoted_country:
            country_value = promoted_country
            country_reason = ""
    return city_value, country_value, city_reason, country_reason


def _ensure_normalized_locations(
    *,
    normalized_locations: list[dict[str, str]],
    city_value: str,
    country_value: str,
    country_reason: str,
) -> list[dict[str, str]]:
    if normalized_locations or not (city_value or country_value):
        return normalized_locations
    return [
        {
            "city": city_value,
            "country": "" if country_reason else normalize_country(country_value),
        }
    ]


def _record_location_quality_issues(
    *,
    raw: dict[str, Any],
    source: str,
    company: str,
    title: str,
    normalized_link: str,
    city_reason: str,
    country_reason: str,
) -> None:
    if city_reason:
        _record_location_quality_issue(
            field_name="city",
            reason=city_reason,
            raw_value=raw.get("city"),
            source=source,
            company=company,
            title=title,
            job_link=normalized_link,
        )
    if country_reason:
        _record_location_quality_issue(
            field_name="country",
            reason=country_reason,
            raw_value=raw.get("country"),
            source=source,
            company=company,
            title=title,
            job_link=normalized_link,
        )


def _location_summary(normalized_locations: list[dict[str, str]]) -> str:
    return " | ".join(
        ", ".join(part for part in [item.get("city", ""), item.get("country", "")] if part)
        for item in normalized_locations
        if item.get("city", "") or item.get("country", "")
    )


def _build_canonical_job(
    *,
    raw: dict[str, Any],
    source: str,
    fetched_at: str,
    title: str,
    company: str,
    normalized_link: str,
    normalized_sector: str,
    source_bundle: list[dict[str, Any]],
    normalized_locations: list[dict[str, str]],
    city_value: str,
    country_value: str,
    country_reason: str,
    adapter: str,
    studio: str,
) -> CanonicalJob:
    sanitized_contract_type = sanitize_public_text(raw.get("contractType"))
    return CanonicalJob.from_mapping(
        {
            "id": "",
            "title": title,
            "company": company,
            "city": city_value,
            "country": "" if country_reason else normalize_country(country_value),
            "workType": normalize_work_type(sanitize_public_text(raw.get("workType")), title),
            "contractType": normalize_contract_type(sanitized_contract_type, title),
            "jobLink": normalized_link,
            "sector": normalized_sector,
            "profession": map_profession(title),
            "companyType": classify_company_type(
                company, title, source, normalized_link, source_bundle
            ),
            "description": f"{title} at {company}",
            "source": source,
            "sourceJobId": clean_text(raw.get("sourceJobId") or raw.get("id")),
            "fetchedAt": to_iso(raw.get("fetchedAt")) or fetched_at,
            "postedAt": to_iso(raw.get("postedAt")),
            "status": "active",
            "firstSeenAt": "",
            "lastSeenAt": "",
            "removedAt": "",
            "lifecycleEvent": "",
            "lifecycleReason": "",
            "dedupKey": "",
            "qualityScore": 0,
            "focusScore": 0,
            "sourceBundleCount": len(source_bundle),
            "sourceBundle": source_bundle,
            "locations": normalized_locations,
            "locationSummary": _location_summary(normalized_locations),
            "adapter": adapter,
            "studio": studio,
        }
    )


def _score_canonical_job(normalized: CanonicalJob) -> CanonicalJob:
    normalized_dict = normalized.to_dict()
    return CanonicalJob.from_mapping(
        {
            **normalized_dict,
            "qualityScore": compute_quality_score(normalized_dict),
            "focusScore": compute_focus_score(normalized_dict),
        }
    )


def canonicalize_job_with_reason(
    raw: Any,
    *,
    source: str,
    fetched_at: str,
    resolve_redirect_url: Callable[[str], str] | None = None,
    resolved_job_link: Any = None,
    title_hydration_resolver: GoogleSheetsProviderTitleResolver | None = None,
) -> tuple[CanonicalJob | None, str]:
    if not isinstance(raw, dict):
        return None, "invalid_payload"
    title = sanitize_public_text(raw.get("title"))
    company = normalize_company_value(sanitize_public_text(raw.get("company")))
    if not title:
        return None, "missing_title"
    if not company:
        return None, "missing_company"

    normalized_link, raw_link = _resolve_job_link(
        raw=raw,
        source=source,
        resolve_redirect_url=resolve_redirect_url,
        resolved_job_link=resolved_job_link,
    )
    if not normalized_link:
        return None, "missing_job_link"
    if env_flag("BALUFFO_CANONICAL_STRICT_URL", DEFAULT_CANONICAL_STRICT_URL) and raw_link:
        if not normalized_link:
            return None, "invalid_url"
    if looks_like_source_specific_static_noise_row(
        title=title,
        job_link=normalized_link,
        source_name=source,
    ):
        return None, "non_job_static_page"

    adapter = clean_text(raw.get("adapter"))
    studio = sanitize_public_text(raw.get("studio"))
    source_bundle = _normalize_source_bundle(raw.get("sourceBundle")) or _default_source_bundle(
        raw=raw,
        source=source,
        adapter=adapter,
        studio=studio,
    )
    title, drop_reason = _google_sheets_repaired_title_or_reason(
        title=title,
        source=source,
        company=company,
        job_link=normalized_link,
        title_hydration_resolver=title_hydration_resolver,
    )
    if drop_reason:
        return None, drop_reason
    if title is None:
        return None, "missing_title"
    raw_sector = sanitize_public_text(raw.get("sector"))
    normalized_sector = normalize_sector(
        raw_sector,
        company,
        title,
        source,
        normalized_link,
        source_bundle,
    )
    _record_sector_quality_issue(
        raw_sector=raw_sector,
        normalized_sector=normalized_sector,
        source=source,
        company=company,
        title=title,
        job_link=normalized_link,
    )

    normalized_locations = _normalize_job_locations(
        raw.get("locations"),
        raw_city=raw.get("city"),
        raw_country=raw.get("country"),
    )
    city_value, country_value, city_reason, country_reason = _resolve_city_country_values(
        raw=raw,
        primary_location=_primary_location(normalized_locations),
    )
    normalized_locations = _ensure_normalized_locations(
        normalized_locations=normalized_locations,
        city_value=city_value,
        country_value=country_value,
        country_reason=country_reason,
    )
    _record_location_quality_issues(
        raw=raw,
        source=source,
        company=company,
        title=title,
        normalized_link=normalized_link,
        city_reason=city_reason,
        country_reason=country_reason,
    )
    normalized = _build_canonical_job(
        raw=raw,
        source=source,
        fetched_at=fetched_at,
        title=title,
        company=company,
        normalized_link=normalized_link,
        normalized_sector=normalized_sector,
        source_bundle=source_bundle,
        normalized_locations=normalized_locations,
        city_value=city_value,
        country_value=country_value,
        country_reason=country_reason,
        adapter=adapter,
        studio=studio,
    )
    return _score_canonical_job(normalized), ""


def canonicalize_job(
    raw: RawJob,
    *,
    source: str,
    fetched_at: str,
    resolve_redirect_url: Callable[[str], str] | None = None,
    resolved_job_link: Any = None,
    title_hydration_resolver: GoogleSheetsProviderTitleResolver | None = None,
) -> CanonicalJob | None:
    normalized, _reason = canonicalize_job_with_reason(
        raw,
        source=source,
        fetched_at=fetched_at,
        resolve_redirect_url=resolve_redirect_url,
        resolved_job_link=resolved_job_link,
        title_hydration_resolver=title_hydration_resolver,
    )
    return normalized


def _google_sheet_redirect_candidates(raw_rows: Sequence[RawJob]) -> list[tuple[int, str]]:
    redirect_candidates: list[tuple[int, str]] = []
    for idx, raw in enumerate(raw_rows):
        normalized_link = normalize_url((raw or {}).get("jobLink"))
        if normalized_link and is_supported_redirect_url(normalized_link):
            redirect_candidates.append((idx, normalized_link))
    return redirect_candidates


def _resolve_google_sheet_redirects(
    *,
    redirect_candidates: list[tuple[int, str]],
    redirect_resolver: PooledRedirectResolver | None,
    redirect_concurrency: int,
    progress_callback: Callable[..., Any] | None = None,
) -> tuple[dict[int, str], dict[str, Any], dict[str, Any], int]:
    snapshot_stats = getattr(redirect_resolver, "snapshot_stats", None)
    resolver_stats_before = snapshot_stats() if callable(snapshot_stats) else {}
    redirect_started = time.perf_counter()
    resolve_fn = getattr(redirect_resolver, "resolve", None)
    resolved_links: dict[int, str] = {}
    if redirect_candidates and callable(resolve_fn):
        if redirect_concurrency <= 1 or len(redirect_candidates) <= 1:
            resolved_links = _resolve_redirects_serial(
                redirect_candidates=redirect_candidates,
                resolve_fn=resolve_fn,
                progress_callback=progress_callback,
            )
        else:
            resolved_links = _resolve_redirects_parallel(
                redirect_candidates=redirect_candidates,
                resolve_fn=resolve_fn,
                redirect_concurrency=redirect_concurrency,
                progress_callback=progress_callback,
            )
    redirect_resolve_ms = int((time.perf_counter() - redirect_started) * 1000)
    resolver_stats_after = snapshot_stats() if callable(snapshot_stats) else {}
    return resolved_links, resolver_stats_before, resolver_stats_after, redirect_resolve_ms


def _resolve_redirects_serial(
    *,
    redirect_candidates: list[tuple[int, str]],
    resolve_fn: Callable[[str], str],
    progress_callback: Callable[..., Any] | None = None,
) -> dict[int, str]:
    resolved_links: dict[int, str] = {}
    for completed, (row_idx, url) in enumerate(redirect_candidates, start=1):
        resolved_links[row_idx] = resolve_fn(url)
        _emit_redirect_progress(
            progress_callback,
            redirect_candidates=redirect_candidates,
            resolved_links=resolved_links,
            completed=completed,
        )
    return resolved_links


def _resolve_redirects_parallel(
    *,
    redirect_candidates: list[tuple[int, str]],
    resolve_fn: Callable[[str], str],
    redirect_concurrency: int,
    progress_callback: Callable[..., Any] | None = None,
) -> dict[int, str]:
    resolved_links: dict[int, str] = {}
    with ThreadPoolExecutor(
        max_workers=min(redirect_concurrency, len(redirect_candidates))
    ) as executor:
        future_map = {
            executor.submit(resolve_fn, url): row_idx for row_idx, url in redirect_candidates
        }
        for completed, future in enumerate(as_completed(future_map), start=1):
            resolved_links[future_map[future]] = future.result()
            _emit_redirect_progress(
                progress_callback,
                redirect_candidates=redirect_candidates,
                resolved_links=resolved_links,
                completed=completed,
            )
    return resolved_links


def _emit_redirect_progress(
    progress_callback: Callable[..., Any] | None,
    *,
    redirect_candidates: list[tuple[int, str]],
    resolved_links: dict[int, str],
    completed: int,
) -> None:
    if progress_callback is None:
        return
    total = len(redirect_candidates)
    if completed < total and completed % 100 != 0:
        return
    resolved_count = sum(
        1
        for idx, original in redirect_candidates
        if normalize_url(resolved_links.get(idx))
        and normalize_url(resolved_links.get(idx)) != normalize_url(original)
    )
    progress_callback(
        phase_key="resolving_sheet_redirects",
        phase_label="Resolving sheet redirects",
        counts={
            "redirectCandidates": total,
            "redirectCompleted": completed,
            "redirectResolved": int(resolved_count),
        },
        message=f"Resolving Google Sheets redirects: {completed} of {total} checked.",
    )


def _google_sheet_final_link(raw: RawJob, idx: int, resolved_links: dict[int, str]) -> str:
    return normalize_url(resolved_links.get(idx)) or normalize_url((raw or {}).get("jobLink"))


def _google_sheet_title_hydration_candidate_link(
    *,
    raw: RawJob,
    idx: int,
    source: str,
    resolved_links: dict[int, str],
    title_hydration_resolver: GoogleSheetsProviderTitleResolver,
) -> str:
    if not isinstance(raw, dict):
        return ""
    title = sanitize_public_text(raw.get("title"))
    company = normalize_company_value(sanitize_public_text(raw.get("company")))
    normalized_link = _google_sheet_final_link(raw, idx, resolved_links)
    if not title or not company or not normalized_link:
        return ""
    if (
        not clean_text(source).startswith("google_sheets")
        or (
            not _is_google_sheets_category_label(title)
            and not _is_google_sheets_repairable_broad_title(title)
        )
        or looks_like_source_specific_static_noise_row(
            title=title,
            job_link=normalized_link,
            source_name=source,
        )
        or _looks_like_google_sheets_category_row_noise(
            source=source,
            title=title,
            company=company,
            job_link=normalized_link,
        )
        or _derive_google_sheets_title_from_url(
            source=source,
            title=title,
            company=company,
            job_link=normalized_link,
        )
    ):
        return ""
    if not title_hydration_resolver.supports(normalized_link):
        return ""
    return normalized_link


def _google_sheet_title_hydration_candidate_links(
    *,
    raw_rows: Sequence[RawJob],
    source: str,
    resolved_links: dict[int, str],
    title_hydration_resolver: GoogleSheetsProviderTitleResolver | None,
) -> list[str]:
    if title_hydration_resolver is None:
        return []
    candidate_links: list[str] = []
    for idx, raw in enumerate(raw_rows):
        candidate_link = _google_sheet_title_hydration_candidate_link(
            raw=raw,
            idx=idx,
            source=source,
            resolved_links=resolved_links,
            title_hydration_resolver=title_hydration_resolver,
        )
        if candidate_link:
            candidate_links.append(candidate_link)
    return candidate_links


def _is_surviving_google_sheets_category_link_status_candidate(
    *,
    raw: RawJob,
    source: str,
    normalized: CanonicalJob,
) -> bool:
    if not clean_text(source).startswith("google_sheets"):
        return False
    if not isinstance(raw, dict):
        return False
    raw_title = sanitize_public_text(raw.get("title"))
    if not _is_google_sheets_category_label(raw_title):
        return False
    if _is_google_sheets_category_label(normalized.title):
        return False
    return bool(normalize_url(normalized.jobLink))


def _emit_google_sheets_progress(
    progress_callback: Callable[..., Any] | None,
    *,
    phase_key: str,
    phase_label: str,
    counts: dict[str, Any],
    message: str,
) -> None:
    if progress_callback is None:
        return
    progress_callback(
        phase_key=phase_key,
        phase_label=phase_label,
        counts=counts,
        message=message,
    )


def _filter_stale_google_sheet_category_links(
    *,
    canonical_batch: list[CanonicalJob],
    candidate_indexes: list[int],
    category_link_status_resolver: GoogleSheetsCategoryLinkStatusResolver | None,
    drop_reasons: Counter[str],
    redirect_concurrency: int,
    progress_callback: Callable[..., Any] | None,
) -> list[CanonicalJob]:
    if category_link_status_resolver is None or not candidate_indexes:
        return canonical_batch
    candidate_links = [
        canonical_batch[idx].jobLink
        for idx in candidate_indexes
        if 0 <= idx < len(canonical_batch) and normalize_url(canonical_batch[idx].jobLink)
    ]
    _emit_google_sheets_progress(
        progress_callback,
        phase_key="checking_category_links",
        phase_label="Checking category links",
        counts={
            "categoryLinkStatusCandidates": len(candidate_links),
            "categoryLinkStatusChecked": 0,
            "categoryLinkStatusStaleDropped": 0,
            "categoryLinkStatusErrors": 0,
        },
        message=f"Checking {len(candidate_links)} repaired Google Sheets category links.",
    )
    category_link_status_resolver.prefetch(
        candidate_links,
        concurrency=max(
            redirect_concurrency,
            min(GOOGLE_SHEETS_CATEGORY_LINK_STATUS_CONCURRENCY, len(candidate_links) or 1),
        ),
        progress_callback=progress_callback,
    )
    stale_indexes = {
        idx
        for idx in candidate_indexes
        if 0 <= idx < len(canonical_batch)
        and category_link_status_resolver.is_stale(canonical_batch[idx].jobLink)
    }
    if not stale_indexes:
        return canonical_batch
    for _idx in stale_indexes:
        category_link_status_resolver.note_stale_drop()
    drop_reasons["google_sheets_category_row"] += len(stale_indexes)
    stats = category_link_status_resolver.snapshot_stats()
    _emit_google_sheets_progress(
        progress_callback,
        phase_key="checking_category_links",
        phase_label="Checking category links",
        counts={
            "categoryLinkStatusCandidates": len(candidate_links),
            "categoryLinkStatusChecked": int(stats.get("category_link_status_checked") or 0),
            "categoryLinkStatusStaleDropped": int(
                stats.get("category_link_status_stale_dropped") or 0
            ),
            "categoryLinkStatusErrors": int(stats.get("category_link_status_errors") or 0),
        },
        message=(
            "Checked repaired Google Sheets category links; "
            f"dropped {len(stale_indexes)} stale rows."
        ),
    )
    return [row for idx, row in enumerate(canonical_batch) if idx not in stale_indexes]


def _canonicalize_google_sheet_rows_with_resolved_links(
    *,
    raw_rows: Sequence[RawJob],
    source: str,
    fetched_at: str,
    resolved_links: dict[int, str],
    title_hydration_resolver: GoogleSheetsProviderTitleResolver | None,
    category_link_status_resolver: GoogleSheetsCategoryLinkStatusResolver | None,
    redirect_concurrency: int,
    progress_callback: Callable[..., Any] | None,
) -> tuple[list[CanonicalJob], Counter[str], int]:
    canonical_started = time.perf_counter()
    last_progress_at = canonical_started
    canonical_batch: list[CanonicalJob] = []
    category_link_candidate_indexes: list[int] = []
    drop_reasons: Counter[str] = Counter()
    for idx, raw in enumerate(raw_rows):
        normalized, drop_reason = canonicalize_job_with_reason(
            raw,
            source=source,
            fetched_at=fetched_at,
            resolved_job_link=resolved_links.get(idx),
            title_hydration_resolver=title_hydration_resolver,
        )
        if normalized:
            if _is_surviving_google_sheets_category_link_status_candidate(
                raw=raw,
                source=source,
                normalized=normalized,
            ):
                category_link_candidate_indexes.append(len(canonical_batch))
            canonical_batch.append(normalized)
        elif drop_reason:
            drop_reasons[drop_reason] += 1
        now = time.perf_counter()
        if ((idx + 1) % 1000 == 0) or (now - last_progress_at >= 2.0):
            last_progress_at = now
            _emit_google_sheets_progress(
                progress_callback,
                phase_key="normalizing_rows",
                phase_label="Normalizing rows",
                counts={
                    "fetchedCount": len(raw_rows),
                    "normalizedCount": idx + 1,
                    "keptCount": len(canonical_batch),
                    "canonicalDropped": sum(drop_reasons.values()),
                },
                message=(
                    f"Normalizing Google Sheets rows: {idx + 1} of {len(raw_rows)} processed."
                ),
            )
    canonical_batch = _filter_stale_google_sheet_category_links(
        canonical_batch=canonical_batch,
        candidate_indexes=category_link_candidate_indexes,
        category_link_status_resolver=category_link_status_resolver,
        drop_reasons=drop_reasons,
        redirect_concurrency=redirect_concurrency,
        progress_callback=progress_callback,
    )
    canonicalize_ms = int((time.perf_counter() - canonical_started) * 1000)
    return canonical_batch, drop_reasons, canonicalize_ms


def _google_sheet_redirect_stats(
    *,
    redirect_candidates: list[tuple[int, str]],
    resolved_links: dict[int, str],
    resolver_stats_before: dict[str, Any],
    resolver_stats_after: dict[str, Any],
    redirect_resolve_ms: int,
    canonicalize_ms: int,
    title_hydration_stats: dict[str, int] | None = None,
    category_link_status_stats: dict[str, int] | None = None,
) -> dict[str, int]:
    redirect_resolved = sum(
        1
        for idx, original in redirect_candidates
        if normalize_url(resolved_links.get(idx))
        and normalize_url(resolved_links.get(idx)) != normalize_url(original)
    )
    stats = {
        "redirect_candidates": len(redirect_candidates),
        "redirect_resolved": int(redirect_resolved),
        "redirect_cache_hits": max(
            0,
            int(resolver_stats_after.get("cacheHits", 0))
            - int(resolver_stats_before.get("cacheHits", 0)),
        ),
        "redirect_resolve_ms": int(redirect_resolve_ms),
        "canonicalize_ms": int(canonicalize_ms),
    }
    for key in _GOOGLE_SHEETS_TITLE_HYDRATION_STAT_KEYS:
        stats[key] = int((title_hydration_stats or {}).get(key) or 0)
    for key in _GOOGLE_SHEETS_CATEGORY_LINK_STAT_KEYS:
        stats[key] = int((category_link_status_stats or {}).get(key) or 0)
    return stats


def canonicalize_google_sheets_rows(
    raw_rows: Sequence[RawJob],
    *,
    source: str,
    fetched_at: str,
    redirect_resolver: PooledRedirectResolver | None = None,
    redirect_concurrency: int = DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY,
    title_hydration_resolver: GoogleSheetsProviderTitleResolver | None = None,
    category_link_status_resolver: GoogleSheetsCategoryLinkStatusResolver | None = None,
    progress_callback: Callable[..., Any] | None = None,
) -> tuple[list[CanonicalJob], Counter, dict[str, int]]:
    redirect_concurrency = max(
        1, int(redirect_concurrency or DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY)
    )
    redirect_candidates = _google_sheet_redirect_candidates(raw_rows)
    _emit_google_sheets_progress(
        progress_callback,
        phase_key="resolving_sheet_redirects",
        phase_label="Resolving sheet redirects",
        counts={
            "redirectCandidates": len(redirect_candidates),
            "redirectCompleted": 0,
            "redirectResolved": 0,
        },
        message=f"Resolving {len(redirect_candidates)} Google Sheets redirect links.",
    )
    (
        resolved_links,
        resolver_stats_before,
        resolver_stats_after,
        redirect_resolve_ms,
    ) = _resolve_google_sheet_redirects(
        redirect_candidates=redirect_candidates,
        redirect_resolver=redirect_resolver,
        redirect_concurrency=redirect_concurrency,
        progress_callback=progress_callback,
    )
    if title_hydration_resolver is not None:
        title_hydration_resolver.prefetch(
            _google_sheet_title_hydration_candidate_links(
                raw_rows=raw_rows,
                source=source,
                resolved_links=resolved_links,
                title_hydration_resolver=title_hydration_resolver,
            ),
            concurrency=redirect_concurrency,
            progress_callback=progress_callback,
        )
    canonical_batch, drop_reasons, canonicalize_ms = (
        _canonicalize_google_sheet_rows_with_resolved_links(
            raw_rows=raw_rows,
            source=source,
            fetched_at=fetched_at,
            resolved_links=resolved_links,
            title_hydration_resolver=title_hydration_resolver,
            category_link_status_resolver=category_link_status_resolver,
            redirect_concurrency=redirect_concurrency,
            progress_callback=progress_callback,
        )
    )
    return (
        canonical_batch,
        drop_reasons,
        _google_sheet_redirect_stats(
            redirect_candidates=redirect_candidates,
            resolved_links=resolved_links,
            resolver_stats_before=resolver_stats_before,
            resolver_stats_after=resolver_stats_after,
            redirect_resolve_ms=redirect_resolve_ms,
            canonicalize_ms=canonicalize_ms,
            title_hydration_stats=(
                title_hydration_resolver.snapshot_stats()
                if title_hydration_resolver is not None
                else None
            ),
            category_link_status_stats=(
                category_link_status_resolver.snapshot_stats()
                if category_link_status_resolver is not None
                else None
            ),
        ),
    )


class CanonicalNormalizer(JobProcessor):
    """Structural normalizer implementing the JobProcessor protocol."""

    def __init__(
        self,
        source: str,
        fetched_at: str,
        resolve_redirect_url: Callable[[str], str] | None = None,
        redirect_resolver: PooledRedirectResolver | None = None,
        redirect_concurrency: int = DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY,
        title_hydration_resolver: GoogleSheetsProviderTitleResolver | None = None,
        category_link_status_resolver: GoogleSheetsCategoryLinkStatusResolver | None = None,
        progress_callback: Callable[..., Any] | None = None,
    ) -> None:
        self.source = source
        self.fetched_at = fetched_at
        self.resolve_redirect_url = resolve_redirect_url
        self.redirect_resolver = redirect_resolver
        self.redirect_concurrency = redirect_concurrency
        self.title_hydration_resolver = title_hydration_resolver
        self.category_link_status_resolver = category_link_status_resolver
        self.progress_callback = progress_callback
        self.stats: dict[str, Any] = {}
        self.drop_reasons: Counter[str] = Counter()

    def process(self, jobs: list[CanonicalJob], **options: Any) -> list[CanonicalJob]:
        # Implementation accepts RawJob masquerading as CanonicalJob initially
        # during the adapter -> pipeline boundary transition.
        raw_rows = cast(list[RawJob], jobs)
        if self.source.startswith("google_sheets"):
            canonical_batch, self.drop_reasons, self.stats = canonicalize_google_sheets_rows(
                raw_rows,
                source=self.source,
                fetched_at=self.fetched_at,
                redirect_resolver=self.redirect_resolver,
                redirect_concurrency=self.redirect_concurrency,
                title_hydration_resolver=self.title_hydration_resolver,
                category_link_status_resolver=self.category_link_status_resolver,
                progress_callback=self.progress_callback,
            )
            return canonical_batch

        canonical_batch = []
        canonical_started = time.perf_counter()
        for raw in raw_rows:
            normalized, drop_reason = canonicalize_job_with_reason(
                raw,
                source=self.source,
                fetched_at=self.fetched_at,
                resolve_redirect_url=self.resolve_redirect_url,
            )
            if normalized:
                canonical_batch.append(normalized)
            elif drop_reason:
                self.drop_reasons[drop_reason] += 1

        self.stats["canonicalize_ms"] = int((time.perf_counter() - canonical_started) * 1000)
        return canonical_batch
