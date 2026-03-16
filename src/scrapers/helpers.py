"""Shared helpers for scrapers (job dict building, text coercion, safe id)."""

from __future__ import annotations

import hashlib
from typing import Any, Dict


def clean_text(value: Any) -> str:
    return str(value or "").strip()


def to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def safe_id(seed: str) -> str:
    return hashlib.sha1(seed.encode("utf-8")).hexdigest()[:12]


def build_job(
    *,
    source_name: str,
    studio: str,
    title: str,
    company: str,
    job_link: str,
    source_job_id: str,
    city: str = "",
    country: str = "Unknown",
    work_type: str = "",
    contract_type: str = "",
    posted_at: str = "",
) -> Dict[str, Any]:
    """Build a job dict in the envelope shape expected by static_scrapy adapter."""
    return {
        "sourceJobId": source_job_id,
        "title": title,
        "company": company,
        "city": city,
        "country": country or "Unknown",
        "workType": work_type,
        "contractType": contract_type,
        "jobLink": job_link,
        "sector": "Game",
        "postedAt": posted_at,
        "source": source_name,
        "studio": studio,
        "adapter": "scrapy_static",
        "sourceBundle": [
            {
                "source": source_name,
                "sourceJobId": source_job_id,
                "jobLink": job_link,
                "postedAt": posted_at,
                "adapter": "scrapy_static",
                "studio": studio,
            }
        ],
    }
