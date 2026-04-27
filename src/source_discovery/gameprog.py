from __future__ import annotations

"""Gameprog.it directory parsing and candidate extraction.

Responsibilities:
- Fetch and parse gameprog.it teams.json into studio entries
- For each studio website, fetch and infer careers URL
- Emit provider/static candidates (similar to gamesmap pattern)
"""

import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.source_registry import unique_sources

from . import audit_ledger
from .directory_cache import load_directory_cache, write_directory_cache
from .directory_fetch import fetch_directory_pages, resolve_directory_fetch_limits
from .directory_fetch_jobs import build_directory_fetch_jobs
from .page_analysis import analyze_fetched_page
from .recovery_url_planner import common_recovery_urls
from .scoring import unique_string_list
from .static_candidates import build_known_careers_url_candidate
from .web_search import fetch_text

GAMEPROG_TEAMS_URL = "https://gameprog.it/teams.json"
GAMEPROG_BASE_URL = "https://gameprog.it/"
GAMEPROG_AUDIT_SCHEMA_VERSION = 1
GAMEPROG_AUDIT_FAILURE_SAMPLE_LIMIT = 100


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


def _gameprog_config_section(config: dict[str, Any] | None) -> dict[str, Any]:
    source = config if isinstance(config, dict) else {}
    gameprog_cfg = source.get("gameprog")
    if isinstance(gameprog_cfg, dict):
        return dict(gameprog_cfg)
    return dict(source)


def _gameprog_config_value(config: dict[str, Any] | None, key: str, default: Any) -> Any:
    return _gameprog_config_section(config).get(key, default)


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


def _gameprog_audit_enabled(config: dict[str, Any] | None) -> bool:
    return bool(_gameprog_config_value(config, "activeAuditEnabled", False))


def _gameprog_audit_path(config: dict[str, Any] | None) -> Path:
    raw = str(_gameprog_config_value(config, "activeAuditPath", "") or "").strip()
    if raw:
        return Path(raw)
    return Path(__file__).resolve().parents[2] / "data" / "gameprog-discovery-audit.json"


def _gameprog_audit_ttl_minutes(config: dict[str, Any] | None) -> int:
    raw = _gameprog_config_value(config, "activeAuditTtlMinutes", None)
    if raw in {"", None}:
        return _gameprog_cache_ttl_minutes(config)
    try:
        return max(0, int(raw))
    except (TypeError, ValueError):
        return _gameprog_cache_ttl_minutes(config)


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
    source = config if isinstance(config, dict) else {}
    if isinstance(source.get("gameprog"), dict):
        source = source.get("gameprog") or {}
    return load_directory_cache(
        cache_path,
        ttl_minutes=ttl_minutes,
        expected_signature=_gameprog_cache_signature(cfg),
        use_cache=fetcher is fetch_text or bool(str(source.get("cachePath") or "").strip()),
    )


def _write_gameprog_cache(
    config: dict[str, Any] | None,
    cfg: dict[str, Any],
    *,
    provider_candidates: list[dict[str, Any]],
    static_candidates: list[dict[str, Any]],
    failures: list[dict[str, Any]],
) -> None:
    write_directory_cache(
        _gameprog_cache_path(config),
        signature=_gameprog_cache_signature(cfg),
        provider_candidates=provider_candidates,
        static_candidates=static_candidates,
        failures=failures,
    )


def _load_gameprog_audit_artifact(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _gameprog_audit_rows(
    artifact: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    provider_rows = artifact.get("providerCandidates")
    static_rows = artifact.get("staticCandidates")
    failures = artifact.get("failures")
    if not isinstance(provider_rows, list):
        provider_rows = []
    if not isinstance(static_rows, list):
        static_rows = []
    if not isinstance(failures, list):
        failures = []
    return unique_sources(provider_rows), unique_sources(static_rows), list(failures)


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


def _empty_gameprog_scan_result(
    *,
    failures: list[dict[str, Any]],
    batch_timing: dict[str, Any],
) -> dict[str, Any]:
    return {
        "providerCandidates": [],
        "staticCandidates": [],
        "failures": failures,
        "summary": {
            "teamsRows": 0,
            "parsedRows": 0,
            "eligibleRows": 0,
            "websiteFetchJobs": 0,
            "websiteFetchFailures": 0,
        },
        "websiteFetchJobs": [],
        "batchTiming": batch_timing,
        "writeCache": False,
    }


def _apply_gameprog_provider_provenance(
    providers: list[dict[str, Any]],
    *,
    website_url: str,
    location: str,
) -> list[dict[str, Any]]:
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
    return providers


def _gameprog_fetch_result_candidates(
    result: dict[str, Any],
    *,
    website_only_fallback: bool,
) -> dict[str, Any]:
    entry = dict(result.get("payload") or {})
    studio = str(entry.get("studio") or "").strip()
    website_url = str(result.get("url") or entry.get("url") or "").strip()
    location = str(entry.get("place") or "").strip()
    nl_priority = False

    if not bool(result.get("ok")):
        static_candidates = []
        failure = result.get("failure")
        failures = [failure] if isinstance(failure, dict) else []
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
        return {
            "providerCandidates": [],
            "staticCandidates": static_candidates,
            "failures": failures,
            "fetchFailed": True,
        }

    analyzed = analyze_fetched_page(
        page_url=website_url,
        html=str(result.get("text") or ""),
        studio=studio,
        nl_priority=nl_priority,
        discovery_method="gameprog",
    )
    providers = list(analyzed.get("provider_candidates") or [])
    if providers:
        return {
            "providerCandidates": _apply_gameprog_provider_provenance(
                providers,
                website_url=website_url,
                location=location,
            ),
            "staticCandidates": [],
            "failures": [],
            "fetchFailed": False,
        }

    explicit_careers_url = str(analyzed.get("explicit_careers_url") or "").strip()
    if explicit_careers_url:
        static_candidate = build_gameprog_static_candidate(
            studio=studio,
            target_url=explicit_careers_url,
            nl_priority=nl_priority,
            website_only=False,
            detail_url=website_url,
            location=location,
            manual_only=False,
            weak_signal=False,
        )
    elif analyzed.get("generic_static_candidate"):
        static_candidate = _apply_gameprog_static_page_provenance(
            analyzed["generic_static_candidate"],
            website_url=website_url,
            location=location,
        )
    else:
        careers_urls = common_recovery_urls(website_url, tuple(COMMON_CAREERS_PATTERNS[:3]))
        careers_url = careers_urls[0] if careers_urls else ""
        if not careers_url and not website_only_fallback:
            return {
                "providerCandidates": [],
                "staticCandidates": [],
                "failures": [],
                "fetchFailed": False,
            }
        static_candidate = build_gameprog_static_candidate(
            studio=studio,
            target_url=careers_url or website_url,
            nl_priority=nl_priority,
            website_only=not bool(careers_url),
            detail_url=website_url,
            location=location,
            manual_only=False,
            weak_signal=True,
        )
    return {
        "providerCandidates": [],
        "staticCandidates": [static_candidate],
        "failures": [],
        "fetchFailed": False,
    }


def _gameprog_scan(
    timeout_s: int,
    *,
    cfg: dict[str, Any],
    fetcher: Any,
    emit_log: Any,
) -> dict[str, Any]:
    teams_url = str(cfg.get("teamsUrl") or GAMEPROG_TEAMS_URL).strip()
    website_only_fallback = bool(cfg.get("websiteOnlyFallback", True))
    max_studios = max(0, int(cfg.get("maxStudios") or 0))
    fetch_concurrency, per_host_concurrency = resolve_directory_fetch_limits(cfg)

    provider_candidates: list[dict[str, Any]] = []
    static_candidates: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    batch_timing: dict[str, Any] = {
        "teamsUrl": teams_url,
        "maxStudios": max_studios,
        "fetchConcurrency": fetch_concurrency,
        "perHostConcurrency": per_host_concurrency,
    }

    teams_json = ""
    started = time.perf_counter()
    try:
        teams_json = fetcher(teams_url, timeout_s)
    except Exception as exc:
        batch_timing["teamsFetchMs"] = audit_ledger.duration_ms(started)
        failures.append(
            {
                "name": teams_url,
                "adapter": "gameprog",
                "error": str(exc),
                "stage": "teams_json_fetch",
            }
        )
        return _empty_gameprog_scan_result(failures=failures, batch_timing=batch_timing)
    batch_timing["teamsFetchMs"] = audit_ledger.duration_ms(started)

    started = time.perf_counter()
    entries = parse_gameprog_teams_json(teams_json)
    batch_timing["parseMs"] = audit_ledger.duration_ms(started)
    parsed_count = len(entries)
    if not entries:
        failures.append(
            {
                "name": "gameprog_teams",
                "adapter": "gameprog",
                "error": "no entries parsed from teams.json",
                "stage": "teams_json_parse",
            }
        )
        return _empty_gameprog_scan_result(failures=failures, batch_timing=batch_timing)

    if max_studios:
        entries = entries[:max_studios]

    emit_log(f"Gameprog directory entries: {len(entries)}")
    website_fetch_jobs = build_directory_fetch_jobs(
        entries,
        url_field="url",
        adapter="gameprog",
        failure_stage="website_fetch",
        required_fields=("studio",),
    )

    started = time.perf_counter()
    website_fetch_results = fetch_directory_pages(
        timeout_s,
        website_fetch_jobs,
        fetcher=fetcher,
        total_concurrency=fetch_concurrency,
        per_host_concurrency=per_host_concurrency,
        progress_label="Gameprog website fetch",
    )
    batch_timing["websiteFetchMs"] = audit_ledger.duration_ms(started)

    website_fetch_failures = 0
    started = time.perf_counter()
    for result in website_fetch_results:
        rows = _gameprog_fetch_result_candidates(
            result,
            website_only_fallback=website_only_fallback,
        )
        provider_candidates.extend(list(rows.get("providerCandidates") or []))
        static_candidates.extend(list(rows.get("staticCandidates") or []))
        failures.extend(list(rows.get("failures") or []))
        if bool(rows.get("fetchFailed")):
            website_fetch_failures += 1
    batch_timing["candidateAnalysisMs"] = audit_ledger.duration_ms(started)

    provider_candidates = unique_sources(provider_candidates)
    static_candidates = unique_sources(static_candidates)
    return {
        "providerCandidates": provider_candidates,
        "staticCandidates": static_candidates,
        "failures": failures,
        "summary": {
            "teamsRows": parsed_count,
            "parsedRows": parsed_count,
            "eligibleRows": len(entries),
            "websiteFetchJobs": len(website_fetch_jobs),
            "websiteFetchFailures": website_fetch_failures,
        },
        "websiteFetchJobs": website_fetch_jobs,
        "batchTiming": batch_timing,
        "writeCache": True,
    }


def _initial_gameprog_audit_artifact(*, cfg: dict[str, Any], timeout_s: int) -> dict[str, Any]:
    now = datetime.now(UTC).isoformat()
    fetch_concurrency, per_host_concurrency = resolve_directory_fetch_limits(cfg)
    return {
        "schemaVersion": GAMEPROG_AUDIT_SCHEMA_VERSION,
        "adapter": "gameprog",
        "startedAt": now,
        "updatedAt": now,
        "runtime": {
            "configSignature": _gameprog_cache_signature(cfg),
            "timeoutSeconds": int(timeout_s),
            "fetchConcurrency": fetch_concurrency,
            "perHostConcurrency": per_host_concurrency,
            "teamsUrl": str(cfg.get("teamsUrl") or GAMEPROG_TEAMS_URL).strip(),
        },
        "progress": {"complete": False, "cursor": 0, "completedUrlIdentities": []},
        "summary": {
            "teamsRows": 0,
            "parsedRows": 0,
            "eligibleRows": 0,
            "websiteFetchJobs": 0,
            "websiteFetchFailures": 0,
            "providerCandidates": 0,
            "staticCandidates": 0,
            "failures": 0,
        },
        "providerCandidates": [],
        "staticCandidates": [],
        "failures": [],
        "failureCounts": {},
        "failureErrorCounts": {},
        "failureSamples": [],
        "timings": {"batches": [], "totalsMs": {}},
    }


def run_gameprog_directory_audit(
    timeout_s: int,
    *,
    config: dict[str, Any] | None = None,
    fetcher: Any = None,
) -> tuple[dict[str, Any], bool]:
    from .reporting import emit_log

    fetcher = fetcher or fetch_text
    cfg = _gameprog_config_section(config)
    output_path = _gameprog_audit_path(config)
    expected_signature = _gameprog_cache_signature(cfg)
    ttl_minutes = _gameprog_audit_ttl_minutes(config)
    existing = _load_gameprog_audit_artifact(output_path)
    if existing is not None and audit_ledger.artifact_is_fresh(
        existing,
        schema_version=GAMEPROG_AUDIT_SCHEMA_VERSION,
        expected_signature=expected_signature,
        ttl_minutes=ttl_minutes,
    ):
        emit_log(f"Gameprog directory audit cache hit: {output_path}.")
        return existing, True

    artifact = _initial_gameprog_audit_artifact(cfg=cfg, timeout_s=timeout_s)
    scan_started = time.perf_counter()
    scan = _gameprog_scan(timeout_s, cfg=cfg, fetcher=fetcher, emit_log=emit_log)
    artifact["providerCandidates"] = list(scan.get("providerCandidates") or [])
    artifact["staticCandidates"] = list(scan.get("staticCandidates") or [])
    failures = list(scan.get("failures") or [])
    audit_ledger.record_failures(
        artifact,
        failures,
        sample_limit=GAMEPROG_AUDIT_FAILURE_SAMPLE_LIMIT,
    )
    summary = dict(artifact.get("summary") or {})
    summary.update(dict(scan.get("summary") or {}))
    summary["providerCandidates"] = len(artifact["providerCandidates"])
    summary["staticCandidates"] = len(artifact["staticCandidates"])
    summary["failures"] = audit_ledger.failure_count(artifact)
    artifact["summary"] = summary
    batch_timing = dict(scan.get("batchTiming") or {})
    batch_timing["totalMs"] = audit_ledger.duration_ms(scan_started)
    audit_ledger.append_batch_timing(artifact, batch_timing)
    artifact["progress"] = {
        "complete": True,
        "cursor": int(summary.get("eligibleRows") or 0),
        "completedUrlIdentities": [
            str(row.get("url") or "").strip()
            for row in list(scan.get("websiteFetchJobs") or [])
            if isinstance(row, dict)
        ],
    }
    artifact["finishedAt"] = datetime.now(UTC).isoformat()
    artifact["updatedAt"] = artifact["finishedAt"]
    audit_ledger.save_artifact_atomic(artifact, output_path)
    return artifact, False


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
    cfg = _gameprog_config_section(config)
    if _gameprog_audit_enabled(config):
        artifact, _cache_hit = run_gameprog_directory_audit(
            timeout_s,
            config=config,
            fetcher=fetcher,
        )
        return _gameprog_audit_rows(artifact)

    cached = _load_gameprog_cache(config, cfg, fetcher=fetcher)
    if cached is not None:
        return cached
    scan = _gameprog_scan(timeout_s, cfg=cfg, fetcher=fetcher, emit_log=emit_log)
    provider_candidates = list(scan.get("providerCandidates") or [])
    static_candidates = list(scan.get("staticCandidates") or [])
    failures = list(scan.get("failures") or [])

    emit_log(
        f"Gameprog candidates: provider={len(provider_candidates)}, static={len(static_candidates)}, failures={len(failures)}."
    )

    if bool(scan.get("writeCache")):
        _write_gameprog_cache(
            config,
            cfg,
            provider_candidates=provider_candidates,
            static_candidates=static_candidates,
            failures=failures,
        )
    return provider_candidates, static_candidates, failures
