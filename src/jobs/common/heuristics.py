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
    if "technical animator" in lower:
        return "technical-animator"
    if "technical director" in lower or re.search(r"\btd\b", lower):
        return "technical-director"
    if (
        "technical artist" in lower
        or "tech artist" in lower
        or "tech-art" in lower
        or "tech art" in lower
        or "shader artist" in lower
        or "material artist" in lower
    ):
        return "technical-artist"
    if (
        "environment artist" in lower
        or "environment art" in lower
        or "world artist" in lower
        or "terrain artist" in lower
    ):
        return "environment-artist"
    if "character artist" in lower:
        return "character-artist"
    if "rigging" in lower or "rigger" in lower:
        return "rigging"
    if "vfx artist" in lower or "visual effects artist" in lower or "fx artist" in lower:
        return "vfx-artist"
    if "ui artist" in lower or "ux artist" in lower or "ui/ux" in lower:
        return "ui-ux-artist"
    if "concept artist" in lower:
        return "concept-artist"
    if "3d artist" in lower or "3d modeler" in lower or "3d modeller" in lower:
        return "3d-artist"
    if "art director" in lower:
        return "art-director"
    if "gameplay" in lower or "game mechanics" in lower:
        return "gameplay"
    if "graphics" in lower or "rendering" in lower or "shader" in lower:
        return "graphics"
    if "engine" in lower or "architecture" in lower or "systems" in lower:
        return "engine"
    if re.search(r"\bai\b", lower) or "artificial intelligence" in lower or "behavior" in lower:
        return "ai"
    if "animator" in lower or "animation" in lower:
        return "animator"
    if "tools" in lower or "pipeline" in lower:
        return "tools"
    if "designer" in lower:
        return "designer"
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
