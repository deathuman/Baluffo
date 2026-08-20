"""Registry conflict row helpers — listing-path heuristics.

AI boundary owns: host/path family matching, career/homepage path detection, and single-host listing-path scoring.
AI boundary implement in: this registry_conflicts_row_path.py leaf.
AI boundary search before contracts: registry conflict routes, registry_conflicts coordinator, and frontend registry conflict callers.
AI boundary verify: `npm run lint:repo-guardrails` plus focused registry conflict row tests."""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import urlparse

from src.bridge.registry_conflicts_row_core import _row_urls


def _static_url_host_paths(row: dict[str, Any]) -> set[tuple[str, str]]:
    host_paths: set[tuple[str, str]] = set()
    for url in _row_urls(row):
        parsed = urlparse(url)
        host = parsed.netloc.lower().removeprefix("www.")
        if not host:
            continue
        path = parsed.path.strip().lower().rstrip("/") or "/"
        host_paths.add((host, path))
    return host_paths


def _family_tokens(family_key: str) -> set[str]:
    stop_words = {
        "digital",
        "entertainment",
        "game",
        "games",
        "group",
        "interactive",
        "online",
        "software",
        "studio",
        "studios",
        "world",
    }
    return {
        token
        for token in re.split(r"[^a-z0-9]+", family_key.lower())
        if len(token) > 2 and token not in stop_words
    }


def _host_matches_family(host: str, family_key: str) -> bool:
    compact_host = host.replace("-", "").replace(".", "")
    return any(token in compact_host for token in _family_tokens(family_key))


def _is_parent_child_path(left: str, right: str) -> bool:
    if left == right:
        return True
    return left.startswith(f"{right}/") or right.startswith(f"{left}/")


def _hosts_same_or_subdomain(left: str, right: str) -> bool:
    return left == right or left.endswith(f".{right}") or right.endswith(f".{left}")


def _is_careerish_path(path: str) -> bool:
    return bool(
        set(re.split(r"[^a-z0-9]+", path.lower()))
        & {
            "career",
            "careers",
            "hiring",
            "job",
            "jobs",
            "join",
            "opening",
            "openings",
            "position",
            "positions",
            "vacancies",
            "work",
        }
    )


def _is_homepage_path(path: str) -> bool:
    return path.strip().lower().rstrip("/") in {"", "/"}


def _has_parent_child_listing_path(
    winner_host_paths: set[tuple[str, str]], loser_host_paths: set[tuple[str, str]]
) -> bool:
    return any(
        _is_parent_child_path(winner_path, loser_path)
        for winner_host, winner_path in winner_host_paths
        for loser_host, loser_path in loser_host_paths
        if winner_host == loser_host
    )


def _has_homepage_to_career_site_path(
    *,
    family_key: str,
    winner_host_paths: set[tuple[str, str]],
    loser_host_paths: set[tuple[str, str]],
) -> bool:
    return any(
        _is_careerish_path(winner_path)
        and _is_homepage_path(loser_path)
        and _host_matches_family(winner_host, family_key)
        and _host_matches_family(loser_host, family_key)
        and _hosts_same_or_subdomain(winner_host, loser_host)
        for winner_host, winner_path in winner_host_paths
        for loser_host, loser_path in loser_host_paths
    )


def _single_static_host_path(row: dict[str, Any]) -> tuple[str, str]:
    host_paths = _static_url_host_paths(row)
    if len(host_paths) != 1:
        return "", ""
    return next(iter(host_paths))
