"""Parsing and normalization helpers used by the jobs pipeline."""

from __future__ import annotations

import re
from typing import Any

from src.jobs.models import RawJob
from src.jobs.text_utils import clean_text, norm_text


def normalize_contract_type(contract_text: Any, title: Any = "") -> str:
    lower = f"{norm_text(contract_text)} {norm_text(title)}"
    if "internship" in lower or re.search(r"\bintern\b", lower):
        return "Internship"
    if "full-time" in lower or "full time" in lower or "permanent" in lower:
        return "Full-time"
    if (
        "temporary" in lower
        or "contract" in lower
        or "freelance" in lower
        or "part-time" in lower
        or "part time" in lower
        or "fixed-term" in lower
        or "fixed term" in lower
    ):
        return "Temporary"
    return "Unknown"


_REMOTE_OK_GENERIC_NON_JOB_TITLE_PATTERNS = (
    re.compile(r"^join (our )?(talent )?community$"),
    re.compile(r"^(join )?(our )?talent (community|pool)$"),
    re.compile(r"^(general|open|spontaneous) application$"),
    re.compile(r"^general interest$"),
    re.compile(r"^join our team$"),
)


def _looks_like_remote_ok_generic_non_job_title(title: Any) -> bool:
    normalized = norm_text(title)
    return bool(normalized) and any(
        pattern.search(normalized) for pattern in _REMOTE_OK_GENERIC_NON_JOB_TITLE_PATTERNS
    )


def parse_remote_ok_payload(payload: Any, *, looks_like_game_job) -> list[RawJob]:
    """
    Parse RemoteOK API responses into RawJob rows.

    `looks_like_game_job` is injected to avoid reintroducing a root-package symbol barrel
    (and to prevent adapter cycles).
    """

    if isinstance(payload, list):
        rows = [row for row in payload if isinstance(row, dict)]
    elif isinstance(payload, dict) and isinstance(payload.get("jobs"), list):
        rows = [row for row in payload["jobs"] if isinstance(row, dict)]
    else:
        return []

    jobs: list[RawJob] = []
    for row in rows:
        title = clean_text(row.get("position") or row.get("title"))
        company = clean_text(row.get("company") or row.get("company_name"))
        tags = row.get("tags") or []
        tags_text = (
            " ".join(str(tag) for tag in tags) if isinstance(tags, list) else clean_text(tags)
        )
        if not title or not company:
            continue
        if _looks_like_remote_ok_generic_non_job_title(title):
            continue
        if not looks_like_game_job(title, company, tags_text):
            continue
        location = clean_text(row.get("location") or "Remote")
        remote = "remote" in norm_text(location)
        jobs.append(
            {
                "sourceJobId": clean_text(row.get("id")),
                "title": title,
                "company": company,
                "city": "Remote" if remote else "",
                "country": "Remote" if remote else location,
                "workType": "Remote" if remote else location,
                "contractType": tags_text,
                "jobLink": clean_text(row.get("url") or row.get("apply_url")),
                "sector": clean_text(row.get("category") or ""),
                "postedAt": row.get("date") or row.get("epoch") or row.get("time"),
            }
        )
    return jobs
