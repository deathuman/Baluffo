from __future__ import annotations

"""Gameprog.it directory parsing and candidate extraction.

Responsibilities:
- Fetch and parse gameprog.it teams.json into studio entries
- For each studio website, fetch and infer careers URL
- Emit provider/static candidates (similar to gamesmap pattern)
"""

import json
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from src.source_registry import unique_sources

from .scoring import unique_string_list
from .web_search import (
    extract_links_from_html,
    fetch_text,
    infer_provider_candidates_from_html,
)

GAMEPROG_TEAMS_URL = "https://gameprog.it/teams.json"
GAMEPROG_BASE_URL = "https://gameprog.it/"


CAREERS_KEYWORDS = [
    "careers",
    "career",
    "jobs",
    "job",
    "hiring",
    "work-with-us",
    "join-us",
    "join-team",
    "open-positions",
    "open-roles",
    "vacancies",
    "job-openings",
    "job-opportunities",
    "we-are-hiring",
    "hiring-now",
    "careers-page",
    "job-page",
    "lavora",
    "lavoro",
    "posizioni",
    "lavora-con-noi",
    "lavora con noi",
    "posizioni-aperte",
    "cerchiamo",
    "collabora",
    "offerta di lavoro",
    "annunci di lavoro",
]


COMMON_CAREERS_PATTERNS = [
    "/careers",
    "/jobs",
    "/hiring",
    "/work-with-us",
    "/lavora-con-noi",
    "/lavoro",
    "/posizioni-aperte",
    "/about-us/careers",
    "/we-are-hiring",
]


def _strip_html_tags(html: str) -> str:
    text = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", str(html or ""))
    text = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", text)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _gameprog_config_value(config: dict[str, Any] | None, key: str, default: Any) -> Any:
    source = config if isinstance(config, dict) else {}
    gameprog_cfg = source.get("gameprog")
    if isinstance(gameprog_cfg, dict):
        return gameprog_cfg.get(key, default)
    return default


def _gameprog_enabled(config: dict[str, Any] | None) -> bool:
    return bool(_gameprog_config_value(config, "enabled", True))


def _gameprog_cache_path(config: dict[str, Any] | None) -> Path | None:
    source = config if isinstance(config, dict) else {}
    if isinstance(source.get("gameprog"), dict):
        source = source.get("gameprog") or {}
    raw = str(source.get("cachePath") or "").strip()
    if not raw:
        return Path(__file__).resolve().parents[2] / "data" / "gameprog-discovery-cache.json"
    return Path(raw)


def _gameprog_cache_ttl_minutes(config: dict[str, Any] | None) -> int:
    source = config if isinstance(config, dict) else {}
    if isinstance(source.get("gameprog"), dict):
        source = source.get("gameprog") or {}
    raw = source.get("cacheTtlMinutes", "")
    if raw in {"", None}:
        raw = 360
    try:
        return max(0, int(raw))
    except (TypeError, ValueError):
        return 360


def _gameprog_cache_signature(cfg: dict[str, Any]) -> dict[str, Any]:
    return {
        "baseUrl": str(cfg.get("baseUrl") or "").strip(),
        "teamsUrl": str(cfg.get("teamsUrl") or "").strip(),
        "websiteOnlyFallback": bool(cfg.get("websiteOnlyFallback", True)),
        "maxStudios": max(0, int(cfg.get("maxStudios") or 0)),
    }


def _load_gameprog_cache(
    config: dict[str, Any] | None, cfg: dict[str, Any], *, fetcher: Any
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]] | None:
    cache_path = _gameprog_cache_path(config)
    ttl_minutes = _gameprog_cache_ttl_minutes(config)
    if ttl_minutes <= 0 or cache_path is None:
        return None
    source = config if isinstance(config, dict) else {}
    if isinstance(source.get("gameprog"), dict):
        source = source.get("gameprog") or {}
    if fetcher is not fetch_text and not str(source.get("cachePath") or "").strip():
        return None
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    updated_at_raw = str(payload.get("updatedAt") or "").strip()
    if not updated_at_raw:
        return None
    try:
        updated_at = datetime.fromisoformat(updated_at_raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if datetime.now(UTC) - updated_at > timedelta(minutes=ttl_minutes):
        return None
    if payload.get("configSignature") != _gameprog_cache_signature(cfg):
        return None
    provider_rows = payload.get("providerCandidates")
    static_rows = payload.get("staticCandidates")
    failures = payload.get("failures")
    if (
        not isinstance(provider_rows, list)
        or not isinstance(static_rows, list)
        or not isinstance(failures, list)
    ):
        return None
    return unique_sources(provider_rows), unique_sources(static_rows), failures


def _write_gameprog_cache(
    config: dict[str, Any] | None,
    cfg: dict[str, Any],
    *,
    provider_candidates: list[dict[str, Any]],
    static_candidates: list[dict[str, Any]],
    failures: list[dict[str, Any]],
) -> None:
    cache_path = _gameprog_cache_path(config)
    if cache_path is None:
        return
    payload = {
        "updatedAt": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "configSignature": _gameprog_cache_signature(cfg),
        "providerCandidates": unique_sources(provider_candidates),
        "staticCandidates": unique_sources(static_candidates),
        "failures": failures,
    }
    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    except OSError:
        return


def parse_gameprog_teams_json(json_text: str) -> list[dict[str, Any]]:
    try:
        data = json.loads(str(json_text or ""))
    except json.JSONDecodeError:
        return []
    if not isinstance(data, list):
        return []
    out: list[dict[str, Any]] = []
    seen = set()
    for entry in data:
        if not isinstance(entry, dict):
            continue
        studio = str(entry.get("name") or "").strip()
        url = str(entry.get("url") or "").strip()
        place = str(entry.get("place") or "").strip()
        if not studio or not url:
            continue
        if studio.lower() in seen:
            continue
        seen.add(studio.lower())
        if not url.startswith("http://") and not url.startswith("https://"):
            continue
        out.append(
            {
                "studio": studio,
                "url": url,
                "place": place,
            }
        )
    return out


def build_gameprog_static_candidate(
    *,
    studio: str,
    target_url: str,
    nl_priority: bool,
    website_only: bool,
    detail_url: str,
    location: str,
    manual_only: bool = False,
    weak_signal: bool = False,
) -> dict[str, Any]:
    evidence_types = ["gameprog_directory"]
    evidence_score = 24
    if website_only:
        evidence_types.append("gameprog_website")
        evidence_types.append("gameprog_website_only")
        if manual_only:
            evidence_types.append("gameprog_manual_website_only")
    else:
        evidence_types.append("gameprog_careers_url")
        evidence_score = 40
    if location:
        evidence_types.append("gameprog_location")
    if weak_signal:
        evidence_types.append("gameprog_no_current_openings")
    return {
        "name": f"{studio} (Gameprog)",
        "studio": studio,
        "company": studio,
        "adapter": "static",
        "pages": [target_url],
        "listing_url": target_url,
        "nlPriority": nl_priority,
        "enabledByDefault": False,
        "discoveryMethod": "gameprog",
        "discoveryStage": "generic_static",
        "careersUrl": "" if website_only else target_url,
        "evidenceSource": "gameprog",
        "evidenceTypes": evidence_types,
        "evidenceScore": evidence_score,
        "weakSignal": bool(website_only),
        "sourceDirectory": "gameprog",
        "sourceDirectoryUrl": GAMEPROG_BASE_URL,
        "sourceDirectoryEntryUrl": detail_url,
        "sourceDirectoryLocation": str(location or "").strip(),
        "manualOnly": bool(manual_only),
    }


def discover_gameprog_candidates(
    timeout_s: int,
    *,
    config: dict[str, Any] | None = None,
    fetcher=None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    from .reporting import emit_log

    fetcher = fetcher or fetch_text
    if not _gameprog_enabled(config):
        emit_log("Gameprog directory disabled, skipping.")
        return [], [], []
    cfg = dict(_gameprog_config_value(config, "gameprog", {}))
    cached = _load_gameprog_cache(config, cfg, fetcher=fetcher)
    if cached is not None:
        return cached
    teams_url = str(cfg.get("teamsUrl") or GAMEPROG_TEAMS_URL).strip()
    website_only_fallback = bool(cfg.get("websiteOnlyFallback", True))
    max_studios = max(0, int(cfg.get("maxStudios") or 0))

    provider_candidates: list[dict[str, Any]] = []
    static_candidates: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []

    teams_json = ""
    try:
        teams_json = fetcher(teams_url, timeout_s)
    except Exception as exc:
        failures.append(
            {
                "name": teams_url,
                "adapter": "gameprog",
                "error": str(exc),
                "stage": "teams_json_fetch",
            }
        )
        return [], [], failures

    entries = parse_gameprog_teams_json(teams_json)
    if not entries:
        failures.append(
            {
                "name": "gameprog_teams",
                "adapter": "gameprog",
                "error": "no entries parsed from teams.json",
                "stage": "teams_json_parse",
            }
        )
        return [], [], failures

    if max_studios:
        entries = entries[:max_studios]

    emit_log(f"Gameprog directory entries: {len(entries)}")

    for idx, entry in enumerate(entries):
        studio = str(entry.get("studio") or "").strip()
        website_url = str(entry.get("url") or "").strip()
        location = str(entry.get("place") or "").strip()
        if not studio or not website_url:
            continue

        nl_priority = False

        try:
            website_html = fetcher(website_url, timeout_s)
        except Exception as exc:
            failures.append(
                {
                    "name": website_url,
                    "adapter": "gameprog",
                    "error": str(exc),
                    "stage": "website_fetch",
                }
            )
            if website_only_fallback:
                static_candidates.append(
                    build_gameprog_static_candidate(
                        studio=studio,
                        target_url=website_url,
                        nl_priority=nl_priority,
                        website_only=True,
                        detail_url=website_url,
                        location=location,
                        manual_only=True,
                    )
                )
            continue

        providers = infer_provider_candidates_from_html(
            page_url=website_url,
            html=website_html,
            studio=studio,
            nl_priority=nl_priority,
            discovery_method="gameprog",
        )

        careers_url = ""
        if providers:
            for inferred in providers:
                inferred["evidenceSource"] = "gameprog"
                inferred["evidenceTypes"] = unique_string_list(
                    [
                        *(inferred.get("evidenceTypes") or []),
                        "gameprog_directory",
                        "gameprog_website_fetch",
                    ]
                )
                inferred["evidenceScore"] = max(int(inferred.get("evidenceScore") or 0), 44)
                inferred["sourceDirectory"] = "gameprog"
                inferred["sourceDirectoryUrl"] = GAMEPROG_BASE_URL
                inferred["sourceDirectoryEntryUrl"] = website_url
                inferred["sourceDirectoryLocation"] = location
            provider_candidates.extend(providers)
        else:
            homepage_links = extract_links_from_html(website_html)
            homepage_careers = [
                link
                for link in homepage_links
                if any(kw in link.lower() for kw in CAREERS_KEYWORDS)
            ]

            careers_url = ""
            if homepage_careers:
                careers_url = homepage_careers[0]
            elif fetcher:
                parsed = urlparse(website_url)
                base = f"{parsed.scheme}://{parsed.netloc}"
                for pattern in COMMON_CAREERS_PATTERNS[:3]:
                    careers_url = base + pattern
                    break

            if careers_url:
                is_homepage_link = bool(homepage_careers)
                static_candidates.append(
                    build_gameprog_static_candidate(
                        studio=studio,
                        target_url=careers_url,
                        nl_priority=nl_priority,
                        website_only=False,
                        detail_url=website_url,
                        location=location,
                        manual_only=False,
                        weak_signal=not is_homepage_link,
                    )
                )
            elif website_only_fallback:
                static_candidates.append(
                    build_gameprog_static_candidate(
                        studio=studio,
                        target_url=website_url,
                        nl_priority=nl_priority,
                        website_only=True,
                        detail_url=website_url,
                        location=location,
                        manual_only=False,
                        weak_signal=True,
                    )
                )

    provider_candidates = unique_sources(provider_candidates)
    static_candidates = unique_sources(static_candidates)

    emit_log(
        f"Gameprog candidates: provider={len(provider_candidates)}, static={len(static_candidates)}, failures={len(failures)}."
    )

    _write_gameprog_cache(
        config,
        cfg,
        provider_candidates=provider_candidates,
        static_candidates=static_candidates,
        failures=failures,
    )
    return provider_candidates, static_candidates, failures
