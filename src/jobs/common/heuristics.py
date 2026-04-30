"""Heuristics and scoring helpers for job canonicalization."""

from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Any

from src.jobs.common.datetime_utils import parse_datetime
from src.jobs.game_detection import has_positive_game_evidence
from src.jobs.models import RawJob
from src.jobs.text_utils import clean_text, norm_text

from . import config as common_config

PROFESSION_RULES: tuple[tuple[str, tuple[str, ...], tuple[str, ...]], ...] = (
    ("technical-animator", ("technical animator",), ()),
    ("technical-director", ("technical director",), (r"\btd\b",)),
    (
        "technical-artist",
        (
            "technical artist",
            "tech artist",
            "tech-art",
            "tech art",
            "shader artist",
            "material artist",
        ),
        (),
    ),
    (
        "environment-artist",
        ("environment artist", "environment art", "world artist", "terrain artist"),
        (),
    ),
    ("character-artist", ("character artist",), ()),
    ("rigging", ("rigging", "rigger"), ()),
    ("vfx-artist", ("vfx artist", "visual effects artist", "fx artist"), ()),
    ("ui-ux-artist", ("ui artist", "ux artist", "ui/ux"), ()),
    ("concept-artist", ("concept artist",), ()),
    ("3d-artist", ("3d artist", "3d modeler", "3d modeller"), ()),
    ("art-director", ("art director",), ()),
    ("gameplay", ("gameplay", "game mechanics"), ()),
    ("graphics", ("graphics", "rendering", "shader"), ()),
    ("engine", ("engine", "architecture", "systems"), ()),
    ("ai", ("artificial intelligence", "behavior"), (r"\bai\b",)),
    ("animator", ("animator", "animation"), ()),
    ("tools", ("tools", "pipeline"), ()),
    ("designer", ("designer",), ()),
)


def classify_company_type(
    company: Any,
    title: Any = "",
    source: Any = "",
    job_link: Any = "",
    source_bundle: Any = None,
) -> str:
    text = f"{norm_text(company)} {norm_text(title)} {norm_text(source)} {norm_text(job_link)}"
    if has_positive_game_evidence(company, title, source, job_link, source_bundle) or re.search(
        r"\b(game|gaming|games|esports|studio|studios|interactive|publisher|entertainment)\b", text
    ):
        return "Game"
    return "Tech"


def map_profession(title: Any) -> str:
    lower = norm_text(title)
    for profession, phrases, patterns in PROFESSION_RULES:
        if any(phrase in lower for phrase in phrases):
            return profession
        if any(re.search(pattern, lower) for pattern in patterns):
            return profession
    return "other"


def is_untrustworthy_company_label(value: str) -> bool:
    return norm_text(value) in common_config.UNTRUSTWORTHY_COMPANY_LABELS


def normalize_company_value(value: Any) -> str:
    company = clean_text(value)
    if not company:
        return ""
    if is_untrustworthy_company_label(company):
        return common_config.UNKNOWN_COMPANY_LABEL
    return company


def compute_quality_score(job: RawJob) -> int:
    fields = [
        "title",
        "company",
        "city",
        "country",
        "workType",
        "contractType",
        "jobLink",
        "sector",
        "profession",
        "sourceJobId",
        "postedAt",
    ]
    filled = sum(1 for field in fields if clean_text(job.get(field)))
    return max(0, min(100, int(round((filled / len(fields)) * 100))))


def title_has_focus_role(title: Any) -> bool:
    lower = norm_text(title)
    if not lower:
        return False
    focus_tokens = (
        "technical artist",
        "tech artist",
        "tech-art",
        "tech art",
        "environment artist",
        "environment art",
        "world artist",
        "terrain artist",
        "material artist",
        "shader artist",
    )
    return any(token in lower for token in focus_tokens)


def compute_focus_score(job: RawJob) -> int:
    score = 0
    profession = norm_text(job.get("profession"))
    title = job.get("title")
    country = clean_text(job.get("country")).upper()
    work_type = clean_text(job.get("workType")).lower()

    if profession in common_config.TARGET_PROFESSIONS:
        score += 55
    elif title_has_focus_role(title):
        score += 45

    if country == "NL":
        score += 20
        if work_type == "hybrid":
            score += 3
        elif work_type == "onsite":
            score += 5

    if work_type == "remote":
        score += 16

    posted = parse_datetime(job.get("postedAt"))
    if posted:
        age_days = max(0.0, (datetime.now(UTC) - posted).total_seconds() / 86400.0)
        if age_days <= 7:
            score += 12
        elif age_days <= 30:
            score += 8
        else:
            score += 3

    return max(0, min(100, score))
