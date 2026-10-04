"""Game/studio job detection (extracted from common)."""

from __future__ import annotations

import re
from typing import Any

GAME_KEYWORDS = {
    "game",
    "gaming",
    "unity",
    "unreal",
    "gamedev",
    "gameplay",
    "technical artist",
    "tech art",
    "tech artist",
    "shader",
    "shader artist",
    "material artist",
    "world artist",
    "terrain artist",
    "environment art",
    "environment artist",
    "character artist",
    "engine programmer",
    "graphics programmer",
}

# Craft terms that identify a row as games work without identifying the *sector*.
#
# Kept separate from GAME_KEYWORDS on purpose. ``looks_like_game_job`` decides whether a row
# belongs in the feed; ``has_positive_game_evidence`` decides what sector an employer is, and
# the second is deliberately stricter -- a bare role word at an unnamed employer must not
# imply Game, or every UX Designer at a bank lands in the games feed. Folding these into
# GAME_KEYWORDS made exactly that happen and broke
# tests/test_game_detection_company_url_evidence.py.
#
# Measured, not guessed. The audit found 1,226 catalogue roles missing from boards Baluffo
# already collects that the row filter rejected outright -- Ubisoft's "Senior Hard Surface
# Artist", CD Projekt Red's "Senior VFX Artist", Marvelous's ゲームデザイナー. Every token
# recovered at least one and admitted none of a control set of plainly non-game roles
# (Logistics Director, Marketing Manager, Junior .NET Developer, Principal Security Software
# Engineer), via tools/coverage_classifier_candidates.py. 199 roles across 97 studios.
#
# Business and support roles are deliberately absent. A games studio hires project managers,
# producers and finance business partners, and admitting those turns a games-jobs feed into a
# jobs board -- which is why this was measured for what it admits as well as what it recovers.
GAME_ROLE_KEYWORDS = {
    "2d artist",
    "3d artist",
    "3d generalist",
    "3d modeler",
    "art director",
    "technical director",
    "creative director",
    "concept artist",
    "prop artist",
    "vehicle artist",
    "hard surface artist",
    "environmental artist",
    "illustrator",
    "animator",
    "animation",
    "ui/ux",
    "ui artist",
    "ui designer",
    "ux designer",
    "vfx",
    "level design",
    "level designer",
    "combat designer",
    "systems designer",
    "narrative designer",
    "sound designer",
    "qa analyst",
    "tools programmer",
    # Non-English listings. An ASCII substring test cannot see these at all: Japanese studios
    # publish titles in Japanese (Marvelous's ゲームデザイナー, Bandai Namco's
    # ゲームAIエンジニア) and Ubisoft publishes "Animateur.trice Cinématique Sénior" in
    # French. Unambiguous craft terms, not whole sentences.
    "ゲームデザイナー",
    "デザイナー",
    "エンジニア",
    "アート",
    "animateur",
    "concepteur",
    "développeur",
    "ingénieur",
}

GAME_ROW_KEYWORDS = GAME_KEYWORDS | GAME_ROLE_KEYWORDS

GAME_SOURCE_FAMILY_HINTS = {
    "8bitplay",
    "epic_games_careers",
    "gamejobs",
    "gamesindustry",
    "gracklehq",
    "workwithindies",
}

# Curated games-industry employer identities. Substring-matched against the
# row's COMPANY field only, never titles or URLs: these names carry
# employer-scope evidence the keyword/provenance branches cannot ("Electronic
# Arts", "Avalanche Studios", "Square Enix" have no game token in the name and
# no dedicated board source), and short-name employers need multi-token hints
# so unrelated look-alikes stay unmatched ("EACH1"/"Eataly" are not "EA").
# Recovers the URL-keyword-carried employers lost to the 2026-09-06 keyword
# scoping (docs/snapshots/sector-signal-contamination-2026-09-06.md).
GAME_EMPLOYER_NAME_HINTS = {
    "playrix",
    "daybreak",
    "metacore",
    "avalanche",
    "square enix",
    "lightbulb crew",
    "electronic arts",
    "ea sports",
    "ea create",
}

NON_GAME_INDUSTRY_HINTS = {
    "electrical product",
    "electrical products",
    "electronics",
    "industrial products",
    "manufacturing",
}

# Adapters whose bundle items are generic listing rows, not per-employer game
# boards. Their studio fields never assert game provenance.
STATIC_ADAPTERS = {"csv", "static", "scrapy_static"}

# Adapters that name a shared hiring *platform*, not an employer-specific board.
# `phenom` is the J2W/Phenom multi-tenant platform: the tenant -- not the adapter --
# is the employer, so a phenom bundle item alone cannot certify game provenance
# (a media conglomerate like RTL Enterprises inherits `sector: Game` otherwise,
# the defect that put 25 controlling/editorial/retail roles into the Game bucket).
# A platform item only counts when the studio name itself carries game scope.
PLATFORM_ADAPTERS = {"phenom"}

# Tokens too generic to tie two employer names together ("Sony Interactive
# Entertainment" vs "PlayStation Global" share none of these).
_GENERIC_EMPLOYER_TOKENS = {
    "games",
    "game",
    "gaming",
    "entertainment",
    "interactive",
    "studios",
    "studio",
    "inc",
    "llc",
    "ltd",
    "the",
    "and",
    "co",
    "corp",
    "company",
    "group",
    "holdings",
}


def _employer_tokens(value: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-z0-9]+", value.lower())
        if token not in _GENERIC_EMPLOYER_TOKENS
    }


def _studio_consistent_with_company(studio: str, company: str) -> bool:
    """Loose employer-identity match: substring either way or token overlap."""
    studio_norm = re.sub(r"[^a-z0-9]", "", studio.lower())
    company_norm = re.sub(r"[^a-z0-9]", "", company.lower())
    if not studio_norm or not company_norm:
        return False
    if studio_norm in company_norm or company_norm in studio_norm:
        return True
    return bool(_employer_tokens(studio) & _employer_tokens(company))


def _bundle_adapter_role(adapter: str, studio: str, company_text: str) -> str:
    """Classify one bundle item for provenance: "static", "provider", "foreign", or "" (neutral).

    A platform item with a plain studio name is neutral rather than static: it must neither count
    as a provider nor switch multi-board scoping on if counted as static.
    """
    if adapter in STATIC_ADAPTERS:
        return "static"
    if not studio:
        return ""
    if adapter in PLATFORM_ADAPTERS and not _studio_carries_game_scope(studio):
        return ""
    return "foreign" if not _studio_consistent_with_company(studio, company_text) else "provider"


def _flatten_source_bundle(source_bundle: Any) -> list[dict[str, Any]]:
    if not isinstance(source_bundle, list):
        return []
    return [item for item in source_bundle if isinstance(item, dict)]


def _studio_carries_game_scope(studio: str) -> bool:
    """Whether a studio name alone asserts game-employer scope.

    The tight rescue platform_adapter tenants get: the curated names plus the
    keyword table against the studio name only. A tenant whose name carries
    neither (a media or manufacturing conglomerate's J2W board) must not be
    certified Game by the adapter it happens to share.
    """
    text = studio.lower()
    return any(keyword in text for keyword in GAME_KEYWORDS) or any(
        hint in text for hint in GAME_EMPLOYER_NAME_HINTS
    )


def has_game_source_provenance(
    source: Any = "",
    source_bundle: Any = None,
    company: Any = "",
) -> bool:
    """True when the row's source identity is a games-industry board.

    Evidence: the row source or a bundle item source matches a
    GAME_SOURCE_FAMILY_HINTS name, or a bundle item carries a
    provider-adapter board (studio + non-static adapter).

    Scoping (multi-board statics): when the bundle mixes provider-adapter
    items with static-adapter items — the signature of a multi-board static
    row that aggregates several employers — provider provenance only counts
    if EVERY provider item's studio is employer-consistent with the row's
    company. Otherwise one employer's board must not certify a row that
    belongs to a different employer on the same aggregated site.
    """
    source_text = str(source or "").strip().lower()
    if source_text and any(hint in source_text for hint in GAME_SOURCE_FAMILY_HINTS):
        return True

    company_text = str(company or "")
    saw_provider_item = False
    saw_static_item = False
    provider_inconsistent = False
    for item in _flatten_source_bundle(source_bundle):
        item_source = str(item.get("source") or "").strip().lower()
        if item_source and any(hint in item_source for hint in GAME_SOURCE_FAMILY_HINTS):
            return True
        adapter = str(item.get("adapter") or "").strip().lower()
        if not adapter:
            continue
        studio = str(item.get("studio") or "").strip().lower()
        role = _bundle_adapter_role(adapter, studio, str(company_text))
        if role == "static":
            saw_static_item = True
            continue
        if not role:
            continue
        saw_provider_item = True
        if role == "foreign":
            provider_inconsistent = True
    if not saw_provider_item:
        return False
    if not saw_static_item:
        return True
    return not provider_inconsistent


def looks_like_game_job(*values: Any) -> bool:
    """True if any value string contains a game-related keyword.

    Consults the sector-grade ``GAME_KEYWORDS`` and the craft-grade
    ``GAME_ROLE_KEYWORDS`` together: this decides whether a *row* is games work, which is a
    weaker question than deciding what sector an employer is in.
    """
    text = " ".join(str(v or "").strip().lower() for v in values if v is not None)
    return bool(text) and any(keyword in text for keyword in GAME_ROW_KEYWORDS)


def has_positive_game_evidence(
    company: Any,
    title: Any = "",
    source: Any = "",
    job_link: Any = "",
    source_bundle: Any = None,
) -> bool:
    """True when company/title text (or source provenance) carries game-sector evidence.

    Evidence, in firing order: non-game industry hints veto (e.g. "electrical
    products"), games-source provenance (source-family hints or provider
    sourceBundle rows), then a GAME_KEYWORDS substring across
    company/title ONLY.

    Deliberately NOT evidence: the company name appearing in the job's own
    source/job URL. ATS hosts (greenhouse/lever/workable/ashby/bamboohr) and
    own-domain careers sites embed the company slug for every employer, so that
    corroboration carries no employer-specific information and misclassified
    ~7,700 non-games rows as Game (Apple, NVIDIA, Lockheed Martin, ...). See
    docs/snapshots/sector-signal-contamination-2026-09-06.md.

    GAME_KEYWORDS are equally NOT evidence in source/job_link text: board URLs
    like ``careers.wbd.com/.../wb-games-jobs`` and
    ``disneycareers.com/en/search-jobs/game/...`` carry a games-flavored path
    for a whole-company site, so the URL keyword misattributed every employer's
    rows (WBD, CNN, HBO Max, Disney, SciGames) as Game — 458 rows in the same
    snapshot's dataset. Source-family provenance (``gamejobs``,
    ``gamesindustry``, ...) stays the dedicated-board signal.

    Employer NAME evidence (``GAME_EMPLOYER_NAME_HINTS``, company field only)
    restores the URL-keyword-carried games employers lost to that scoping:
    their rows ride shared aggregator sources (``google_sheets``) or static
    boards with no per-employer provenance, and their names carry no game
    token. Hints are multi-token or unambiguous; short-name employers use
    multi-token hints so unrelated look-alikes (EACH1, Eataly) cannot match.

    Provenance is scoped for multi-board static rows: see
    has_game_source_provenance.
    """
    text = " ".join(
        str(v or "").strip().lower() for v in (company, title, source, job_link) if v is not None
    )
    if not text:
        return False
    normalized_text = re.sub(r"[\s_-]+", " ", text)
    if any(hint in normalized_text for hint in NON_GAME_INDUSTRY_HINTS):
        return False
    if has_game_source_provenance(source, source_bundle, company):
        return True
    company_text = str(company or "").strip().lower()
    if company_text and any(hint in company_text for hint in GAME_EMPLOYER_NAME_HINTS):
        return True
    employer_text = " ".join(
        str(v or "").strip().lower() for v in (company, title) if v is not None
    )
    if any(keyword in employer_text for keyword in GAME_KEYWORDS):
        return True
    return False
