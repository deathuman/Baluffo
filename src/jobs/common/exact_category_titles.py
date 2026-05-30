from __future__ import annotations

import re
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
        "analyst",
        "architect",
        "artist",
        "assistant",
        "associate",
        "consultant",
        "coordinator",
        "counsel",
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
        "scientist",
        "specialist",
        "strategist",
        "supervisor",
        "technical",
        "writer",
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


def category_label_keys(value: Any) -> set[str]:
    raw = clean_text(value)
    if not raw:
        return set()
    spaced = norm_text(raw.replace("-", " ").replace("_", " ").replace("&", " and "))
    compact_and = norm_text(raw.replace("-", " ").replace("_", " ").replace("&", " "))
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


_EXACT_CATEGORY_LABEL_KEYS = frozenset(
    key for term in _EXACT_CATEGORY_LABEL_TERMS for key in category_label_keys(term)
)
_EXACT_CATEGORY_OVERRIDE_KEYS = frozenset(
    key for term in {"Technical art", "Technical-art"} for key in category_label_keys(term)
)


def category_tokens(value: Any) -> set[str]:
    raw = norm_text(value)
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
