from __future__ import annotations

import re
from typing import Any
from urllib.parse import urlparse

from src.jobs.text_utils import normalize_url

GREENHOUSE_JOB_HOSTS = {"boards.greenhouse.io", "job-boards.greenhouse.io"}


def greenhouse_job_identity_from_url(url: Any) -> str:
    normalized = normalize_url(url)
    if not normalized:
        return ""
    parsed = urlparse(normalized)
    if parsed.netloc.lower() not in GREENHOUSE_JOB_HOSTS:
        return ""
    match = re.match(r"^/([^/]+)/jobs/(\d+)(?:/)?$", parsed.path or "")
    if not match:
        return ""
    board_slug, job_id = match.groups()
    return f"greenhouse:{board_slug.lower()}:{job_id}"
