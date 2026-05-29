from __future__ import annotations

import re
from urllib.parse import unquote, urlparse


def company_from_job_link(job_link: str) -> str:
    raw = _raw_company_from_job_link(job_link)
    return _extract_company_name(raw)


def _raw_company_from_job_link(job_link: str) -> str:
    if not job_link:
        return ""
    parsed = urlparse(job_link)
    host = parsed.netloc.lower().removeprefix("www.")
    parts = [unquote(p) for p in parsed.path.split("/") if p]

    company = _subdomain_company(host)
    if company:
        return company
    company = _workday_company(host)
    if company:
        return company
    company = _personio_company(host)
    if company:
        return company
    return _host_path_company(host, parts)


_SUBDOMAIN_SUFFIXES = (
    ".bamboohr.com",
    ".breezy.hr",
    ".teamtailor.com",
    ".applytojob.com",
    ".recruitee.com",
)


def _subdomain_company(host: str) -> str:
    for suffix in _SUBDOMAIN_SUFFIXES:
        if host.endswith(suffix):
            prefix = host[: -len(suffix)]
            token = prefix.rstrip(".").rsplit(".", maxsplit=1)[-1]
            if token and token != "www":
                return token
    return ""


def _workday_company(host: str) -> str:
    m = re.match(r"^([\w-]+)\.wd\d+\.myworkdayjobs\.com$", host)
    if m:
        return m.group(1)
    return ""


def _personio_company(host: str) -> str:
    suffix = ".jobs.personio.de"
    if host.endswith(suffix):
        prefix = host[: -len(suffix)]
        token = prefix.rstrip(".").rsplit(".", maxsplit=1)[-1]
        if token and token != "www":
            return token
    return ""


_PATH_COMPANY_HOSTS = frozenset(
    {
        "smartrecruiters.com",
        "jobs.smartrecruiters.com",
        "boards.greenhouse.io",
        "job-boards.greenhouse.io",
        "job-boards.eu.greenhouse.io",
        "jobs.lever.co",
        "jobs.eu.lever.co",
        "jobs.ashbyhq.com",
    }
)


def _host_path_company(host: str, parts: list[str]) -> str:
    if host in _PATH_COMPANY_HOSTS and parts:
        return parts[0]

    if host == "apply.workable.com" and len(parts) >= 2 and parts[1] == "j":
        return parts[0]

    if host == "himalayas.app" and len(parts) >= 2 and parts[0].lower() == "companies":
        return parts[1]

    if host == "shine.com" and len(parts) >= 3 and parts[0].lower() == "jobs":
        return parts[-2]

    return ""


_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)


def _extract_company_name(raw: str) -> str:
    raw = raw.strip()
    if not raw:
        return ""
    if _UUID_RE.match(raw):
        return ""
    cleaned = re.sub(r"-+", " ", raw).strip()
    if len(cleaned) <= 1:
        return ""
    if cleaned.isupper():
        return cleaned
    return cleaned.title()
