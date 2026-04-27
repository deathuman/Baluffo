from __future__ import annotations

"""Shared filters for provider URLs that are too generic to identify a company board."""

from typing import Any
from urllib.parse import urlparse

BAD_GREENHOUSE_SLUGS = frozenset(
    {"api", "board", "boards", "embed", "greenhouse", "job", "jobs", "v1"}
)
BAD_TEAMTAILOR_HOSTS = frozenset({"teamtailor.com", "www.teamtailor.com", "api.teamtailor.com"})
BAD_TEAMTAILOR_SUBDOMAINS = frozenset({"api", "career", "careers", "jobs", "www"})


def bad_provider_inference_detail(candidate: dict[str, Any]) -> str:
    adapter = str(candidate.get("adapter") or "").strip().lower()
    url = str(
        candidate.get("url")
        or candidate.get("listing_url")
        or candidate.get("base_url")
        or candidate.get("greenhouse_board")
        or ""
    ).strip()
    parsed = urlparse(url)
    if adapter == "greenhouse":
        slug = str(
            candidate.get("slug")
            or candidate.get("greenhouse_board")
            or parsed.path.strip("/").split("/", 1)[0]
        )
        if slug.strip().lower() in BAD_GREENHOUSE_SLUGS:
            return "bad_greenhouse_slug"
    if adapter == "teamtailor":
        host = parsed.netloc.lower()
        subdomain = host.split(".", 1)[0] if host else ""
        if host in BAD_TEAMTAILOR_HOSTS or subdomain in BAD_TEAMTAILOR_SUBDOMAINS:
            return "bad_teamtailor_host"
    return ""


def split_bad_provider_inferences(
    candidates: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    accepted: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for candidate in candidates:
        row = dict(candidate)
        detail = bad_provider_inference_detail(row)
        if detail:
            row["reasonDetail"] = detail
            rejected.append(row)
        else:
            accepted.append(row)
    return accepted, rejected
