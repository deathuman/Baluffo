from __future__ import annotations

"""Gameprog.it directory parsing and candidate extraction.

Responsibilities:
- Fetch and parse gameprog.it teams.json into studio entries
- For each studio website, fetch and infer careers URL
- Emit provider/static candidates (similar to gamesmap pattern)
"""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from src.source_registry import unique_sources

from .directory_fetch import fetch_directory_pages, resolve_directory_fetch_limits
from .page_analysis import analyze_fetched_page
from .recovery_url_planner import common_recovery_urls
from .scoring import unique_string_list
from .static_candidates import build_known_careers_url_candidate
from .web_search import fetch_text

GAMEPROG_TEAMS_URL = "https://gameprog.it/teams.json"
GAMEPROG_BASE_URL = "https://gameprog.it/"


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
    if website_only:
        evidence_score = 24
        evidence_types.append("gameprog_website")
        evidence_types.append("gameprog_website_only")
        if manual_only:
            evidence_types.append("gameprog_manual_website_only")
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
            "careersUrl": "",
            "evidenceSource": "gameprog",
            "evidenceTypes": evidence_types,
            "evidenceScore": evidence_score,
            "weakSignal": True,
            "sourceDirectory": "gameprog",
            "sourceDirectoryUrl": GAMEPROG_BASE_URL,
            "sourceDirectoryEntryUrl": detail_url,
            "sourceDirectoryLocation": str(location or "").strip(),
            "manualOnly": bool(manual_only),
        }
    evidence_types.append("gameprog_careers_url")
    if location:
        evidence_types.append("gameprog_location")
    if weak_signal:
        evidence_types.append("gameprog_no_current_openings")
    return build_known_careers_url_candidate(
        target_url,
        studio=studio,
        name_suffix="Gameprog",
        nl_priority=nl_priority,
        discovery_method="gameprog",
        evidence_source="gameprog",
        evidence_types=evidence_types,
        evidence_score=40,
        enabled_by_default=False,
        extra_fields={
            "sourceDirectory": "gameprog",
            "sourceDirectoryUrl": GAMEPROG_BASE_URL,
            "sourceDirectoryEntryUrl": detail_url,
            "sourceDirectoryLocation": str(location or "").strip(),
            "manualOnly": bool(manual_only),
        },
    )


def _apply_gameprog_static_page_provenance(
    candidate: dict[str, Any],
    *,
    website_url: str,
    location: str,
) -> dict[str, Any]:
    enriched = dict(candidate)
    evidence_types = [
        *(enriched.get("evidenceTypes") or []),
        "gameprog_directory",
        "gameprog_website_fetch",
    ]
    if location:
        evidence_types.append("gameprog_location")
    enriched["name"] = f"{str(enriched.get('studio') or '').strip()} (Gameprog)"
    enriched["evidenceSource"] = "gameprog"
    enriched["evidenceTypes"] = unique_string_list(evidence_types)
    enriched["sourceDirectory"] = "gameprog"
    enriched["sourceDirectoryUrl"] = GAMEPROG_BASE_URL
    enriched["sourceDirectoryEntryUrl"] = website_url
    enriched["sourceDirectoryLocation"] = str(location or "").strip()
    enriched["careersUrl"] = str(enriched.get("careersUrl") or website_url).strip() or website_url
    return enriched


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
    fetch_concurrency, per_host_concurrency = resolve_directory_fetch_limits(cfg)

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

    website_fetch_results = fetch_directory_pages(
        timeout_s,
        [
            {
                "url": str(entry.get("url") or "").strip(),
                "payload": entry,
                "name": str(entry.get("url") or "").strip(),
                "adapter": "gameprog",
                "failureStage": "website_fetch",
            }
            for entry in entries
            if str(entry.get("studio") or "").strip() and str(entry.get("url") or "").strip()
        ],
        fetcher=fetcher,
        total_concurrency=fetch_concurrency,
        per_host_concurrency=per_host_concurrency,
        progress_label="Gameprog website fetch",
    )

    for result in website_fetch_results:
        entry = dict(result.get("payload") or {})
        studio = str(entry.get("studio") or "").strip()
        website_url = str(result.get("url") or entry.get("url") or "").strip()
        location = str(entry.get("place") or "").strip()

        nl_priority = False

        if not bool(result.get("ok")):
            failure = result.get("failure")
            if isinstance(failure, dict):
                failures.append(failure)
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
        website_html = str(result.get("text") or "")
        analyzed = analyze_fetched_page(
            page_url=website_url,
            html=website_html,
            studio=studio,
            nl_priority=nl_priority,
            discovery_method="gameprog",
        )
        providers = list(analyzed.get("provider_candidates") or [])
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
            continue
        explicit_careers_url = str(analyzed.get("explicit_careers_url") or "").strip()
        if explicit_careers_url:
            static_candidates.append(
                build_gameprog_static_candidate(
                    studio=studio,
                    target_url=explicit_careers_url,
                    nl_priority=nl_priority,
                    website_only=False,
                    detail_url=website_url,
                    location=location,
                    manual_only=False,
                    weak_signal=False,
                )
            )
            continue
        generic_static_candidate = analyzed.get("generic_static_candidate")
        if generic_static_candidate:
            static_candidates.append(
                _apply_gameprog_static_page_provenance(
                    generic_static_candidate,
                    website_url=website_url,
                    location=location,
                )
            )
            continue
        careers_urls = common_recovery_urls(website_url, tuple(COMMON_CAREERS_PATTERNS[:3]))
        careers_url = careers_urls[0] if careers_urls else ""
        if careers_url:
            static_candidates.append(
                build_gameprog_static_candidate(
                    studio=studio,
                    target_url=careers_url,
                    nl_priority=nl_priority,
                    website_only=False,
                    detail_url=website_url,
                    location=location,
                    manual_only=False,
                    weak_signal=True,
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
