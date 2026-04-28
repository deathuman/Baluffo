from __future__ import annotations

"""Gameprog.it directory parsing and candidate extraction.

Responsibilities:
- Fetch and parse gameprog.it teams.json into studio entries
- For each studio website, fetch and infer careers URL
- Emit provider/static candidates (similar to gamesmap pattern)
"""

import json
import time
from pathlib import Path
from typing import Any

from src.source_registry import unique_sources

from . import audit_ledger
from .audit_config import audit_artifact_path, audit_enabled, audit_ttl_minutes, config_section
from .directory_adapter_templates import (
    apply_directory_provenance,
    build_directory_static_candidate,
    empty_directory_scan_result,
    run_directory_entry_selection_scan,
)
from .directory_audit import (
    DirectoryAuditRunSpec,
    discover_directory_adapter_candidates,
    run_directory_audit_spec,
)
from .directory_cache import (
    directory_cache_ttl_minutes,
    load_adapter_directory_cache,
    write_adapter_directory_cache,
)
from .directory_fetch import fetch_directory_pages, resolve_directory_fetch_limits
from .directory_page_recovery import (
    DEFAULT_RECOVERY_URL_LIMIT,
    RECOVERY_LOGIC_VERSION,
    DirectoryRecoveryRequest,
    resolve_recovery_url_limit,
)
from .page_outcomes import (
    FetchedPageContext,
    PageOutcome,
    PageOutcomeStrategy,
    classify_fetched_page_with_strategy,
    classify_recovery_page_with_strategy,
)
from .recovery_url_planner import common_recovery_urls
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
    return config_section(config, "gameprog")


def _gameprog_config_value(config: dict[str, Any] | None, key: str, default: Any) -> Any:
    return _gameprog_config_section(config).get(key, default)


def _gameprog_enabled(config: dict[str, Any] | None) -> bool:
    return bool(_gameprog_config_value(config, "enabled", True))


def _gameprog_cache_ttl_minutes(config: dict[str, Any] | None) -> int:
    return directory_cache_ttl_minutes(config, "gameprog")


def _gameprog_audit_enabled(config: dict[str, Any] | None) -> bool:
    return audit_enabled(config, "gameprog")


def _gameprog_audit_path(config: dict[str, Any] | None) -> Path:
    return audit_artifact_path(
        config,
        "gameprog",
        default_filename="gameprog-discovery-audit.json",
    )


def _gameprog_audit_ttl_minutes(config: dict[str, Any] | None) -> int:
    return audit_ttl_minutes(
        config,
        "gameprog",
        fallback_ttl=_gameprog_cache_ttl_minutes(config),
    )


def _gameprog_cache_signature(cfg: dict[str, Any]) -> dict[str, Any]:
    return {
        "baseUrl": str(cfg.get("baseUrl") or "").strip(),
        "teamsUrl": str(cfg.get("teamsUrl") or "").strip(),
        "websiteOnlyFallback": bool(cfg.get("websiteOnlyFallback", True)),
        "maxStudios": max(0, int(cfg.get("maxStudios") or 0)),
        "activeAuditRecoveryEnabled": bool(cfg.get("activeAuditRecoveryEnabled", True)),
        "activeAuditRecoveryUrlLimit": resolve_recovery_url_limit(cfg),
        "recoveryLogicVersion": RECOVERY_LOGIC_VERSION,
    }


def _load_gameprog_cache(
    config: dict[str, Any] | None, cfg: dict[str, Any], *, fetcher: Any
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]] | None:
    return load_adapter_directory_cache(
        config,
        section_name="gameprog",
        default_filename="gameprog-discovery-cache.json",
        expected_signature=_gameprog_cache_signature(cfg),
        fetcher=fetcher,
        default_fetcher=fetch_text,
    )


def _write_gameprog_cache(
    config: dict[str, Any] | None,
    cfg: dict[str, Any],
    *,
    provider_candidates: list[dict[str, Any]],
    static_candidates: list[dict[str, Any]],
    failures: list[dict[str, Any]],
) -> None:
    write_adapter_directory_cache(
        config,
        section_name="gameprog",
        default_filename="gameprog-discovery-cache.json",
        signature=_gameprog_cache_signature(cfg),
        provider_candidates=provider_candidates,
        static_candidates=static_candidates,
        failures=failures,
    )


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
    return build_directory_static_candidate(
        studio=studio,
        target_url=target_url,
        nl_priority=nl_priority,
        website_only=website_only,
        name_suffix="Gameprog",
        discovery_method="gameprog",
        evidence_source="gameprog",
        evidence_types=["gameprog_directory"],
        source_directory="gameprog",
        source_directory_url=GAMEPROG_BASE_URL,
        source_directory_entry_url=detail_url,
        source_directory_location=location,
        manual_only=manual_only,
        weak_signal=weak_signal,
        website_evidence_types=[
            "gameprog_website",
            "gameprog_website_only",
            *(["gameprog_manual_website_only"] if manual_only else []),
        ],
        careers_evidence_type="gameprog_careers_url",
        location_evidence_type="gameprog_location",
        weak_signal_evidence_type="gameprog_no_current_openings",
    )


def _apply_gameprog_static_page_provenance(
    candidate: dict[str, Any],
    *,
    website_url: str,
    location: str,
) -> dict[str, Any]:
    evidence_types = ["gameprog_directory", "gameprog_website_fetch"]
    if location:
        evidence_types.append("gameprog_location")
    return apply_directory_provenance(
        candidate,
        evidence_source="gameprog",
        evidence_types=evidence_types,
        source_directory="gameprog",
        source_directory_url=GAMEPROG_BASE_URL,
        source_directory_entry_url=website_url,
        source_directory_location=location,
        careers_url_fallback=website_url,
        name_suffix="Gameprog",
    )


def _empty_gameprog_summary() -> dict[str, int]:
    return {
        "teamsRows": 0,
        "parsedRows": 0,
        "eligibleRows": 0,
        "websiteFetchJobs": 0,
        "websiteFetchFailures": 0,
        "recoveryFetchAttempts": 0,
        "recoveryPagesFetched": 0,
        "recoveredProviderCandidates": 0,
        "recoveredStaticCandidates": 0,
        "recoveryFailures": 0,
        "browserRecoveryCandidates": 0,
        "badProviderInferences": 0,
    }


def _apply_gameprog_provider_provenance(
    providers: list[dict[str, Any]],
    *,
    website_url: str,
    location: str,
) -> list[dict[str, Any]]:
    for inferred in providers:
        enriched = apply_directory_provenance(
            inferred,
            evidence_source="gameprog",
            evidence_types=["gameprog_directory", "gameprog_website_fetch"],
            source_directory="gameprog",
            source_directory_url=GAMEPROG_BASE_URL,
            source_directory_entry_url=website_url,
            source_directory_location=location,
            evidence_score_floor=44,
        )
        inferred.clear()
        inferred.update(enriched)
    return providers


def _gameprog_source_page_url(context: FetchedPageContext) -> str:
    return str(context.payload.get("sourcePageUrl") or context.page_url).strip()


def _gameprog_location(context: FetchedPageContext) -> str:
    return str(context.payload.get("place") or "").strip()


def _gameprog_provider_rows(
    providers: list[dict[str, Any]],
    context: FetchedPageContext,
) -> list[dict[str, Any]]:
    return _apply_gameprog_provider_provenance(
        providers,
        website_url=_gameprog_source_page_url(context),
        location=_gameprog_location(context),
    )


def _gameprog_explicit_static(
    explicit_careers_url: str,
    context: FetchedPageContext,
) -> dict[str, Any]:
    return build_gameprog_static_candidate(
        studio=context.studio,
        target_url=explicit_careers_url,
        nl_priority=context.nl_priority,
        website_only=False,
        detail_url=_gameprog_source_page_url(context),
        location=_gameprog_location(context),
        manual_only=False,
        weak_signal=False,
    )


def _gameprog_generic_static(
    candidate: dict[str, Any],
    context: FetchedPageContext,
) -> dict[str, Any]:
    return _apply_gameprog_static_page_provenance(
        candidate,
        website_url=_gameprog_source_page_url(context),
        location=_gameprog_location(context),
    )


def _gameprog_recovery_request(context: FetchedPageContext) -> DirectoryRecoveryRequest:
    return DirectoryRecoveryRequest(
        key=context.recovery_key or context.page_url,
        adapter="gameprog",
        discovery_method=context.discovery_method,
        name=context.studio or context.page_url,
        studio=context.studio,
        page_url=context.page_url,
        html=context.html,
        payload=dict(context.payload),
    )


def _gameprog_rows_from_outcome(outcome: PageOutcome) -> dict[str, Any]:
    return {
        "providerCandidates": outcome.provider_candidates,
        "staticCandidates": outcome.static_candidates,
        "failures": [],
        "fetchFailed": False,
        "recoveryRequests": outcome.recovery_requests,
        "fallbackStaticCandidates": outcome.fallback_static_candidates,
        "badProviderInferences": outcome.bad_provider_inferences,
    }


def _gameprog_fetch_result_candidates(
    result: dict[str, Any],
    *,
    website_only_fallback: bool,
    enable_recovery: bool = False,
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

    context = FetchedPageContext(
        page_url=website_url,
        html=str(result.get("text") or ""),
        studio=studio,
        nl_priority=nl_priority,
        discovery_method="gameprog",
        payload=entry,
        recovery_key=website_url,
    )

    def _fallback_static(fallback_context: FetchedPageContext) -> dict[str, Any] | None:
        careers_urls = common_recovery_urls(website_url, tuple(COMMON_CAREERS_PATTERNS[:3]))
        careers_url = careers_urls[0] if careers_urls else ""
        if not careers_url and not website_only_fallback:
            return None
        return build_gameprog_static_candidate(
            studio=fallback_context.studio,
            target_url=careers_url or website_url,
            nl_priority=fallback_context.nl_priority,
            website_only=not bool(careers_url),
            detail_url=website_url,
            location=location,
            manual_only=False,
            weak_signal=True,
        )

    strategy = PageOutcomeStrategy(
        provider_rows=_gameprog_provider_rows,
        explicit_static=_gameprog_explicit_static,
        generic_static=_gameprog_generic_static,
        fallback_static=_fallback_static,
        recovery_request=_gameprog_recovery_request,
        filter_bad_providers=True,
    )
    return _gameprog_rows_from_outcome(
        classify_fetched_page_with_strategy(
            context,
            strategy,
            enable_recovery=enable_recovery and bool(website_url),
        )
    )


def _gameprog_recovery_result_candidates(
    result: dict[str, Any],
    request: DirectoryRecoveryRequest,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    entry = dict(request.payload or {})
    recovery_url = str(result.get("url") or request.page_url or "").strip()
    context = FetchedPageContext(
        page_url=recovery_url,
        html=str(result.get("text") or ""),
        studio=request.studio,
        nl_priority=False,
        discovery_method="gameprog",
        payload={**entry, "sourcePageUrl": request.page_url},
        recovery_key=request.key,
    )
    outcome = classify_recovery_page_with_strategy(
        context,
        PageOutcomeStrategy(
            provider_rows=_gameprog_provider_rows,
            explicit_static=_gameprog_explicit_static,
            generic_static=_gameprog_generic_static,
            filter_bad_providers=True,
        ),
    )
    return outcome.provider_candidates, outcome.static_candidates


def _gameprog_scan(
    timeout_s: int,
    *,
    cfg: dict[str, Any],
    fetcher: Any,
    emit_log: Any,
    enable_recovery: bool = False,
    recovery_url_limit: int = DEFAULT_RECOVERY_URL_LIMIT,
) -> dict[str, Any]:
    teams_url = str(cfg.get("teamsUrl") or GAMEPROG_TEAMS_URL).strip()
    website_only_fallback = bool(cfg.get("websiteOnlyFallback", True))
    max_studios = max(0, int(cfg.get("maxStudios") or 0))

    failures: list[dict[str, Any]] = []
    batch_timing: dict[str, Any] = {
        "teamsUrl": teams_url,
        "maxStudios": max_studios,
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
        return empty_directory_scan_result(
            failures=failures,
            summary=_empty_gameprog_summary(),
            batch_timing=batch_timing,
            write_cache=False,
        )
    batch_timing["teamsFetchMs"] = audit_ledger.duration_ms(started)

    started = time.perf_counter()
    entries = parse_gameprog_teams_json(teams_json)
    batch_timing["parseMs"] = audit_ledger.duration_ms(started)
    parsed_count = len(entries)

    def empty_result() -> dict[str, Any]:
        failures.append(
            {
                "name": "gameprog_teams",
                "adapter": "gameprog",
                "error": "no entries parsed from teams.json",
                "stage": "teams_json_parse",
            }
        )
        return empty_directory_scan_result(
            failures=failures,
            summary=_empty_gameprog_summary(),
            batch_timing=batch_timing,
            write_cache=False,
        )

    def select_entries(parsed_entries: list[dict[str, Any]]) -> dict[str, Any]:
        selected_entries = parsed_entries[:max_studios] if max_studios else list(parsed_entries)
        return {
            "homepageEntries": selected_entries,
            "eligibleEntries": len(selected_entries),
        }

    return run_directory_entry_selection_scan(
        timeout_s,
        cfg=cfg,
        parsed_entries=entries,
        empty_result=empty_result,
        select_entries=select_entries,
        emit_selection_log=lambda selected: emit_log(
            f"Gameprog directory entries: {int(selected.get('eligibleEntries') or 0)}"
        ),
        url_field="url",
        adapter="gameprog",
        failure_stage="website_fetch",
        fetcher=fetcher,
        fetch_pages=fetch_directory_pages,
        resolve_fetch_limits=resolve_directory_fetch_limits,
        progress_label="Gameprog website fetch",
        required_fields=("studio",),
        analyze_result=lambda result: _gameprog_fetch_result_candidates(
            result,
            website_only_fallback=website_only_fallback,
            enable_recovery=enable_recovery,
        ),
        enable_recovery=enable_recovery,
        recovery_analyze_result=_gameprog_recovery_result_candidates,
        recovery_progress_label="Gameprog",
        unique_sources_fn=unique_sources,
        batch_timing=batch_timing,
        summary={
            "teamsRows": parsed_count,
            "parsedRows": parsed_count,
        },
        selection_timing_key="",
        recovery_url_limit=recovery_url_limit,
        initial_failures=failures,
    )


def run_gameprog_directory_audit(
    timeout_s: int,
    *,
    config: dict[str, Any] | None = None,
    fetcher: Any = None,
) -> tuple[dict[str, Any], bool]:
    from .reporting import emit_log

    fetcher = fetcher or fetch_text
    cfg = _gameprog_config_section(config)
    fetch_concurrency, per_host_concurrency = resolve_directory_fetch_limits(cfg)
    recovery_url_limit = resolve_recovery_url_limit(cfg)
    return run_directory_audit_spec(
        DirectoryAuditRunSpec(
            adapter="gameprog",
            schema_version=GAMEPROG_AUDIT_SCHEMA_VERSION,
            output_path=_gameprog_audit_path(config),
            ttl_minutes=_gameprog_audit_ttl_minutes(config),
            signature=_gameprog_cache_signature(cfg),
            timeout_s=timeout_s,
            scan=lambda scan_timeout_s: _gameprog_scan(
                scan_timeout_s,
                cfg=cfg,
                fetcher=fetcher,
                emit_log=emit_log,
                enable_recovery=bool(cfg.get("activeAuditRecoveryEnabled", True)),
                recovery_url_limit=recovery_url_limit,
            ),
            runtime={
                "fetchConcurrency": fetch_concurrency,
                "perHostConcurrency": per_host_concurrency,
                "teamsUrl": str(cfg.get("teamsUrl") or GAMEPROG_TEAMS_URL).strip(),
            },
            summary={
                "teamsRows": 0,
                "parsedRows": 0,
                "eligibleRows": 0,
                "websiteFetchJobs": 0,
                "websiteFetchFailures": 0,
                "recoveryFetchAttempts": 0,
                "recoveryPagesFetched": 0,
                "recoveredProviderCandidates": 0,
                "recoveredStaticCandidates": 0,
                "recoveryFailures": 0,
                "browserRecoveryCandidates": 0,
                "badProviderInferences": 0,
            },
            sample_limit=GAMEPROG_AUDIT_FAILURE_SAMPLE_LIMIT,
            emit_log=emit_log,
        )
    )


def discover_gameprog_candidates(
    timeout_s: int,
    *,
    config: dict[str, Any] | None = None,
    fetcher=None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    from .reporting import emit_log

    fetcher = fetcher or fetch_text
    cfg = _gameprog_config_section(config)
    return discover_directory_adapter_candidates(
        timeout_s,
        enabled=_gameprog_enabled(config),
        disabled_log="Gameprog directory disabled, skipping.",
        audit_enabled=_gameprog_audit_enabled(config),
        run_audit=lambda: run_gameprog_directory_audit(
            timeout_s,
            config=config,
            fetcher=fetcher,
        ),
        load_cache=lambda: _load_gameprog_cache(config, cfg, fetcher=fetcher),
        scan=lambda scan_timeout_s: _gameprog_scan(
            scan_timeout_s,
            cfg=cfg,
            fetcher=fetcher,
            emit_log=emit_log,
            enable_recovery=False,
        ),
        write_cache=lambda provider_candidates, static_candidates, failures: _write_gameprog_cache(
            config,
            cfg,
            provider_candidates=provider_candidates,
            static_candidates=static_candidates,
            failures=failures,
        ),
        emit_log=emit_log,
        scan_summary_log=lambda provider_candidates, static_candidates, failures: (
            "Gameprog candidates: "
            f"provider={len(provider_candidates)}, "
            f"static={len(static_candidates)}, "
            f"failures={len(failures)}."
        ),
    )
