from __future__ import annotations

import re
from typing import Any
from urllib.parse import urlparse

from src.jobs.text_utils import normalize_url

GREENHOUSE_JOB_HOSTS = {
    "boards.greenhouse.io",
    "job-boards.greenhouse.io",
}
_GREENHOUSE_JOB_HOSTS_WITH_EU = frozenset(
    GREENHOUSE_JOB_HOSTS | {"boards.eu.greenhouse.io", "job-boards.eu.greenhouse.io"}
)


def greenhouse_job_identity_from_url(url: Any, *, include_eu_hosts: bool = False) -> str:
    normalized = normalize_url(url)
    if not normalized:
        return ""
    parsed = urlparse(normalized)
    hosts = _GREENHOUSE_JOB_HOSTS_WITH_EU if include_eu_hosts else GREENHOUSE_JOB_HOSTS
    if parsed.netloc.lower() not in hosts:
        return ""
    match = re.match(r"^/([^/]+)/jobs/(\d+)(?:/)?$", parsed.path or "")
    if not match:
        return ""
    board_slug, job_id = match.groups()
    return f"greenhouse:{board_slug.lower()}:{job_id}"
