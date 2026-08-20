"""Google Sheets link-employer evidence.

AI boundary owns: game/non-game evidence terms, evidence text helpers, and link-employer
identity/mismatch checks used to classify Google Sheets category-row noise.
AI boundary implement in: this leaf for link-employer evidence; category detection comes
from ``canonicalize_google_sheets_category.py``.
"""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import unquote, urlparse

from src.jobs.canonicalize_google_sheets_category import (
    _is_google_sheets_category_label,
    _is_google_sheets_game_adjacent_category_label,
)
from src.jobs.text_utils import (
    clean_text,
    norm_text,
)

from .common import config as common_config

UNKNOWN_COMPANY_LABEL = common_config.UNKNOWN_COMPANY_LABEL


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
