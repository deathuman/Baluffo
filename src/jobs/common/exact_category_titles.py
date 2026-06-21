"""Exact category-title detection for jobs normalization.

AI boundary owns: category-title dictionaries, exact title matching, and canonical title noise classification.
AI boundary implement in: this file for category title policy; broader title normalization stays in jobs text and canonicalization modules.
AI boundary search before contracts: jobs canonicalization, taxonomy helpers, adapter parsers, and title quality tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused jobs title normalization tests.
"""

from __future__ import annotations

import re
from html import unescape
from typing import Any
from urllib.parse import urlparse

from src.jobs.text_utils import clean_text, norm_text, normalize_url

_EXACT_CATEGORY_LABEL_TERMS = frozenset(
    {
        "Accounting",
        "Account-management",
        "Account Management",
        "Administrative",
        "Animation",
        "Art",
        "Audio",
        "Audio-production",
        "Backend",
        "Backend-development",
        "Business-analysis",
        "Business-development",
        "Campaign-management",
        "Character-art",
        "Community",
        "Community-management",
        "Combat-design",
        "Concept-art",
        "Content",
        "Customer-service",
        "Cyber-security",
        "Data",
        "Data-analysis",
        "Data-science",
        "Design",
        "Development",
        "Devops",
        "Digital Marketing",
        "Digital-marketing",
        "Editorial",
        "Education",
        "Engineering",
        "Events",
        "Facility-management",
        "Finance",
        "Financial-analysis",
        "Frontend",
        "Frontend-development",
        "Full-stack-development",
        "Game Design",
        "Game-design",
        "Game-production",
        "Game-economy",
        "Game-programmer",
        "Gameplay",
        "Graphic-design",
        "Graphics-engineer",
        "HR",
        "Human-resource",
        "Human-resources",
        "IT & infrastructure",
        "It-&-infrastructure",
        "Infrastructure",
        "Legal",
        "Level-art",
        "Level-design",
        "Live-ops",
        "Localization",
        "Logistics",
        "Manufacturing",
        "Marketing",
        "Mobile-development",
        "Monetization",
        "Network-admin",
        "Network-engineering",
        "Operations",
        "Physics-engine",
        "Product",
        "Product-design",
        "Product Management",
        "Product-management",
        "Program-management",
        "Programming",
        "Project-management",
        "Prop-art",
        "Public-relation",
        "QA",
        "Quality Assurance",
        "Quality-assurance",
        "Quality-analysis",
        "Quest-design",
        "Rendering",
        "Research",
        "Research & Development",
        "Research-development",
        "Risk-management",
        "Sales",
        "Security",
        "Social-media",
        "Software",
        "Software Development",
        "Software-development-&-engineering",
        "Software-development-engineering",
        "System-admin",
        "System-design",
        "Talent-acquisition",
        "Taxation",
        "Teaching",
        "Tech",
        "Technical art",
        "Technical-art",
        "Testing",
        "UI-art",
        "Ui-ux-design",
        "VFX",
        "Vfx",
        "Video-editing",
        "Videography",
        "Web",
        "Web-development",
    }
)

_EXACT_CATEGORY_VETO_TOKENS = frozenset(
    {
        "administrator",
        "adviser",
        "advisers",
        "advisor",
        "advisors",
        "analyst",
        "architect",
        "architects",
        "artist",
        "assistant",
        "associate",
        "consultant",
        "coordinator",
        "counsel",
        "designer",
        "designers",
        "developer",
        "developers",
        "director",
        "directors",
        "engineer",
        "engineers",
        "executive",
        "intern",
        "internship",
        "junior",
        "lead",
        "manager",
        "principal",
        "producer",
        "producers",
        "programmer",
        "programmers",
        "recruiter",
        "scientist",
        "specialist",
        "strategist",
        "supervisor",
        "technical",
        "writer",
    }
)
_ROLE_TITLE_VETO_TOKENS = frozenset(
    {
        *_EXACT_CATEGORY_VETO_TOKENS,
        "accountant",
        "accountants",
        "agent",
        "agents",
        "animator",
        "animators",
        "apprentice",
        "bd",
        "bdr",
        "collector",
        "controller",
        "contract",
        "creator",
        "copywriter",
        "editor",
        "editors",
        "expert",
        "generalist",
        "generalists",
        "grouper",
        "illustrator",
        "illustrators",
        "insights",
        "integrator",
        "interpreter",
        "interns",
        "leader",
        "localizer",
        "media",
        "modeler",
        "office",
        "operator",
        "outreacher",
        "owner",
        "partner",
        "planner",
        "pm",
        "president",
        "prototyper",
        "qe",
        "red",
        "representative",
        "representatives",
        "retoucher",
        "researcher",
        "scout",
        "sdet",
        "sales",
        "senior",
        "solution",
        "sourcer",
        "staff",
        "teacher",
        "teamer",
        "technician",
        "tester",
        "testers",
        "testing",
        "trainee",
        "translator",
        "translators",
        "vp",
    }
)
_CATEGORY_PATH_FRAGMENTS = (
    "/department/",
    "/departments/",
    "/job-category/",
    "/job-categories/",
    "/categories/",
    "/category/",
    "/function-",
    "/functions/",
    "/team/",
    "/teams/",
    "department-is-",
    "job-category/",
)

_CATEGORY_PATH_SEGMENTS = frozenset(
    {
        "account-management",
        "animation",
        "art",
        "audio",
        "community",
        "community-management",
        "design",
        "digital-marketing",
        "engineering",
        "finance",
        "game-design",
        "game-production",
        "legal",
        "localization",
        "marketing",
        "operations",
        "product",
        "product-management",
        "qa",
        "quality-assurance",
        "research",
        "research-development",
        "sales",
        "security",
        "software-development",
        "tech",
        "technical-art",
        "vfx",
        "web",
    }
)

_STATIC_CONTAINER_LABEL_TERMS = frozenset(
    {
        "3D",
        "AI",
        "All",
        "All categories",
        "Analytics",
        "Back Office",
        "Careers",
        "Creative",
        "Job",
        "Jobs",
        "Open positions",
        "Skip to content",
        "Vacancies",
        "\u0410\u043d\u0430\u043b\u0438\u0442\u0438\u043a\u0430",
        "\u0410\u0440\u0442",
        "\u0412\u0430\u043a\u0430\u043d\u0441\u0438\u0438",
        "\u0413\u0435\u0439\u043c-\u0434\u0438\u0437\u0430\u0439\u043d",
        "\u0418\u043d\u0444\u0440\u0430\u0441\u0442\u0440\u0443\u043a\u0442\u0443\u0440\u0430",
        "\u041a\u0430\u0440\u044c\u0435\u0440\u0430",
        "\u041a\u043e\u043c\u044c\u044e\u043d\u0438\u0442\u0438",
        "\u041c\u0430\u0440\u043a\u0435\u0442\u0438\u043d\u0433",
        "\u041c\u043e\u043d\u0435\u0442\u0438\u0437\u0430\u0446\u0438\u044f",
        "\u0420\u0430\u0437\u0440\u0430\u0431\u043e\u0442\u043a\u0430",
    }
)

_STATIC_CONTAINER_PATH_FRAGMENTS = (
    "/filter/",
    "/filters/",
    "/vacancies/filter/",
    "/careers/all",
    "/jobs/all",
    "department-is-",
    "function-all",
    "function-is-",
    "page-is-",
)

_STATIC_CONTAINER_PATH_SEGMENTS = frozenset(
    {
        "3d",
        "all",
        "all-categories",
        "analytics",
        "back-office",
        "creative",
    }
)

_STATIC_CONTAINER_QUERY_FRAGMENTS = (
    "category=",
    "department=",
    "department-is-",
    "filter",
    "function=",
    "function-all",
    "function-is-",
    "job-category",
    "lang=",
    "language=",
    "locale=",
    "page=",
    "page-is-",
    "team=",
)

_LANGUAGE_SWITCH_WORDS = frozenset(
    {
        "chinesisch",
        "chinese",
        "coreano",
        "deutsch",
        "english",
        "espanol",
        "español",
        "francais",
        "français",
        "german",
        "inglese",
        "italiano",
        "japanisch",
        "korean",
        "koreanisch",
        "polski",
        "portugues",
        "português",
        "ruso",
        "russisch",
        "thai",
        "turkce",
        "türkçe",
    }
)
_LANGUAGE_CODE_TITLES = frozenset(
    {"de", "en", "es", "fr", "it", "ja", "ko", "pl", "pt", "ru", "th", "tr", "zh"}
)
_STATIC_CONTAINER_ROOT_SEGMENTS = frozenset(
    {"career", "careers", "job", "jobs", "open-positions", "vacancies"}
)


def category_label_keys(value: Any) -> set[str]:
    raw = clean_text(unescape(clean_text(value)))
    if not raw:
        return set()
    stripped = clean_text(re.sub(r"^[^\w]+|[^\w]+$", "", raw))
    spaced = norm_text(raw.replace("-", " ").replace("_", " ").replace("&", " and "))
    compact_and = norm_text(raw.replace("-", " ").replace("_", " ").replace("&", " "))
    stripped_spaced = norm_text(stripped.replace("-", " ").replace("_", " ").replace("&", " and "))
    stripped_compact_and = norm_text(stripped.replace("-", " ").replace("_", " ").replace("&", " "))
    return {
        key
        for key in {
            norm_text(raw),
            norm_text(stripped),
            spaced,
            compact_and,
            stripped_spaced,
            stripped_compact_and,
            spaced.replace(" ", "-"),
            compact_and.replace(" ", "-"),
            stripped_spaced.replace(" ", "-"),
            stripped_compact_and.replace(" ", "-"),
        }
        if key
    }


_EXACT_CATEGORY_LABEL_KEYS = frozenset(
    key for term in _EXACT_CATEGORY_LABEL_TERMS for key in category_label_keys(term)
)
_EXACT_CATEGORY_OVERRIDE_KEYS = frozenset(
    key for term in {"Technical art", "Technical-art"} for key in category_label_keys(term)
)
_STATIC_CONTAINER_LABEL_KEYS = frozenset(
    key for term in _STATIC_CONTAINER_LABEL_TERMS for key in category_label_keys(term)
)


def category_tokens(value: Any) -> set[str]:
    raw = norm_text(unescape(clean_text(value)))
    if not raw:
        return set()
    return {token for token in re.split(r"[^a-z0-9]+", raw) if token}


def is_exact_category_title(value: Any) -> bool:
    keys = category_label_keys(value)
    if not keys or not (keys & _EXACT_CATEGORY_LABEL_KEYS):
        return False
    if keys & _EXACT_CATEGORY_OVERRIDE_KEYS:
        return True
    return not bool(category_tokens(value) & _EXACT_CATEGORY_VETO_TOKENS)


def _strip_static_status_suffix(value: Any) -> str:
    key = norm_text(unescape(clean_text(value)))
    if " : " not in key:
        return key
    base, suffix = key.split(" : ", 1)
    if base and suffix and len(suffix) <= 24:
        return base.strip()
    return key


def _looks_like_language_switch_title(value: Any) -> bool:
    raw = clean_text(value)
    if not raw or "(" not in raw or ")" not in raw:
        return False
    if category_tokens(raw) & _ROLE_TITLE_VETO_TOKENS:
        return False
    lowered = norm_text(raw)
    if any(word in lowered for word in _LANGUAGE_SWITCH_WORDS):
        return True
    return bool(re.search(r"[\u3040-\u30ff\u3400-\u9fff\u0e00-\u0e7f]", raw))


def _looks_like_numeric_page_title(value: Any) -> bool:
    return bool(re.fullmatch(r"\d{1,3}", clean_text(value)))


def _looks_like_ellipsis_title(value: Any) -> bool:
    return bool(re.fullmatch(r"\.{2,}", clean_text(value)))


def is_static_container_artifact_title(value: Any) -> bool:
    raw = clean_text(unescape(clean_text(value)))
    if not raw:
        return False
    if (
        _looks_like_numeric_page_title(raw)
        or _looks_like_ellipsis_title(raw)
        or _looks_like_language_switch_title(raw)
        or norm_text(raw) in _LANGUAGE_CODE_TITLES
    ):
        return True
    keys = category_label_keys(raw)
    base = _strip_static_status_suffix(raw)
    suffix_stripped = bool(base and base != norm_text(raw))
    if suffix_stripped:
        base_keys = category_label_keys(base)
        if base_keys & _EXACT_CATEGORY_LABEL_KEYS:
            return True
        keys |= base_keys
    return bool(keys & _STATIC_CONTAINER_LABEL_KEYS)


def _looks_like_short_container_label(value: Any) -> bool:
    raw = clean_text(value)
    if not raw:
        return False
    if is_static_container_artifact_title(raw):
        return True
    tokens = category_tokens(raw)
    if not tokens or len(tokens) > 3:
        return False
    return not bool(tokens & _ROLE_TITLE_VETO_TOKENS)


def _looks_like_categoryish_slug_title(value: Any) -> bool:
    base = _strip_static_status_suffix(value)
    keys = category_label_keys(base)
    categoryish_keys = _STATIC_CONTAINER_LABEL_KEYS | _EXACT_CATEGORY_LABEL_KEYS
    if keys & categoryish_keys:
        return True
    parts = [
        part.strip()
        for part in re.split(r"\s+(?:and|&)\s+|[/+,]", clean_text(base))
        if part.strip()
    ]
    if len(parts) <= 1:
        tokens = category_tokens(base)
        return 1 < len(tokens) <= 3 and all(
            category_label_keys(token) & categoryish_keys for token in tokens
        )
    return all(category_label_keys(part) & categoryish_keys for part in parts)


def looks_like_category_container_url(value: Any) -> bool:
    normalized = normalize_url(value) or clean_text(value)
    if not normalized:
        return False
    parsed = urlparse(normalized)
    path = (parsed.path or "").lower()
    query = (parsed.query or "").lower()
    if any(fragment in path for fragment in _CATEGORY_PATH_FRAGMENTS):
        return True
    if "department-is-" in query or "job-category" in query:
        return True
    segments = [segment for segment in re.split(r"[\\/]+", path) if segment]
    if not segments:
        return False
    if any(
        segment.endswith("-department") or segment.endswith("-category") for segment in segments
    ):
        return True
    return any(segment in _CATEGORY_PATH_SEGMENTS for segment in segments)


def looks_like_static_container_url(value: Any) -> bool:
    normalized = normalize_url(value) or clean_text(value)
    if not normalized:
        return False
    if looks_like_category_container_url(normalized):
        return True
    parsed = urlparse(normalized)
    path = (parsed.path or "").lower()
    query = (parsed.query or "").lower()
    combined = f"{path}?{query}" if query else path
    if any(fragment in combined for fragment in _STATIC_CONTAINER_PATH_FRAGMENTS):
        return True
    if any(fragment in query for fragment in _STATIC_CONTAINER_QUERY_FRAGMENTS):
        return True
    segments = [segment for segment in re.split(r"[\\/]+", path) if segment]
    if any(segment in _STATIC_CONTAINER_PATH_SEGMENTS for segment in segments):
        return True
    if segments and segments[-1] in _STATIC_CONTAINER_ROOT_SEGMENTS and len(segments) <= 2:
        return True
    return any(
        segment.startswith(("filter-", "function-", "page-is-", "project-")) for segment in segments
    )


def _looks_like_explicit_static_container_url(value: Any) -> bool:
    normalized = normalize_url(value) or clean_text(value)
    if not normalized:
        return False
    if looks_like_category_container_url(normalized):
        return True
    parsed = urlparse(normalized)
    path = (parsed.path or "").lower()
    query = (parsed.query or "").lower()
    combined = f"{path}?{query}" if query else path
    if any(fragment in combined for fragment in _STATIC_CONTAINER_PATH_FRAGMENTS):
        return True
    if any(fragment in query for fragment in _STATIC_CONTAINER_QUERY_FRAGMENTS):
        return True
    segments = [segment for segment in re.split(r"[\\/]+", path) if segment]
    if any(segment in _STATIC_CONTAINER_PATH_SEGMENTS for segment in segments):
        return True
    return any(
        segment.startswith(("filter-", "function-", "page-is-", "project-")) for segment in segments
    )


def _title_matches_url_slug(title: Any, url: Any) -> bool:
    if not _looks_like_categoryish_slug_title(title):
        return False
    normalized = normalize_url(url) or clean_text(url)
    if not normalized:
        return False
    parsed = urlparse(normalized)
    segments = [segment for segment in re.split(r"[\\/]+", (parsed.path or "").lower()) if segment]
    if not segments:
        return False
    title_keys = category_label_keys(_strip_static_status_suffix(title))
    return bool(title_keys and segments[-1] in title_keys)


def has_static_container_artifact_evidence(title: Any, url: Any = "") -> bool:
    if is_exact_category_title(title):
        return True
    title_is_static_artifact = is_static_container_artifact_title(title)
    if title_is_static_artifact and _looks_like_language_switch_title(title):
        return True
    if title_is_static_artifact:
        return True
    if not _looks_like_short_container_label(title):
        return False
    if not url:
        return False
    if _looks_like_categoryish_slug_title(title):
        return looks_like_static_container_url(url) or _title_matches_url_slug(title, url)
    return _looks_like_explicit_static_container_url(url)
