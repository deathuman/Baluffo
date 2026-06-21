"""Google Sheets directory discovery helpers.

AI boundary owns: sheet directory row loading, candidate extraction, and directory source evidence.
AI boundary implement in: this file for sheet-directory discovery; global candidate orchestration stays in orchestrator_generation.
AI boundary search before contracts: candidate collections, source registry rows, and sheet directory tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused sheet directory tests.
"""

from __future__ import annotations

import csv
import os
from io import StringIO
from typing import Any
from urllib.parse import urlparse

from src.source_registry import unique_sources

from .audit_config import audit_artifact_path, audit_ttl_minutes, config_section
from .config import (
    DEFAULT_DISCOVERY_CONFIG,
    GAME_STUDIOS_SHEET_GID,
    GAME_STUDIOS_SHEET_ID,
    GAME_STUDIOS_SHEET_URL,
)
from .directory_adapter_templates import (
    apply_directory_provenance,
    build_known_directory_entry_candidate,
)
from .directory_audit import (
    DirectoryAuditRunSpec,
    directory_audit_rows,
    run_directory_audit_spec,
)
from .directory_index_scan import run_directory_index_scan
from .directory_page_recovery import (
    DEFAULT_RECOVERY_URL_LIMIT,
    RECOVERY_LOGIC_VERSION,
    DirectoryRecoveryRequest,
    apply_recovery_to_scan_result,
    http_recovery_request_from_context,
    recovery_result_candidates_from_strategy,
    resolve_recovery_url_limit,
    run_recovery_for_requests,
)
from .io_runtime import collapse_competing_candidates
from .multi_source_text import fetch_first_nonempty_text
from .page_outcomes import (
    FetchedPageContext,
    PageOutcomeStrategy,
)
from .web_search import infer_web_candidate

SHEET_DIRECTORY_AUDIT_SCHEMA_VERSION = 1
SHEET_DIRECTORY_AUDIT_FAILURE_SAMPLE_LIMIT = 10_000
SHEET_DIRECTORY_RECOVERY_FETCH_CONCURRENCY = 24
SHEET_DIRECTORY_RECOVERY_PER_HOST_CONCURRENCY = 3


def game_studios_sheet_candidate_urls(sheet_id: str, gid: str) -> list[str]:
    csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
    gviz_csv_url = (
        f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&gid={gid}"
    )
    pub_csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/pub?output=csv"
    return [gviz_csv_url, pub_csv_url, csv_url]


def _sheet_csv_rows(csv_text: str) -> list[list[str]]:
    return list(csv.reader(StringIO(str(csv_text or ""))))


def _normalized_sheet_headers(row: list[str]) -> list[str]:
    from .scoring import _norm_header

    return [_norm_header(cell) for cell in row]


def _sheet_row_is_header(row: list[str]) -> bool:
    normalized_full = _normalized_sheet_headers(row)
    normalized = [cell for cell in normalized_full if cell]
    if not normalized:
        return False
    has_studio = "studio" in normalized_full or "company" in normalized_full
    has_link = "link" in normalized_full or "url" in normalized_full
    has_roles = any("roles" in cell or "hiring" in cell for cell in normalized_full if cell)
    return has_studio and has_link and has_roles


def _sheet_header_index(rows: list[list[str]]) -> int:
    for idx, row in enumerate(rows[:250]):
        if _sheet_row_is_header(row):
            return idx
    return -1


def _sheet_column_indices(headers: list[str]) -> tuple[int, int, int]:
    studio_idx = -1
    link_idx = -1
    openings_idx = -1
    for i, h in enumerate(headers):
        if studio_idx < 0 and (h == "studio" or "studio" in h or "company" in h):
            studio_idx = i
        if link_idx < 0 and (h == "link" or "link" in h or h == "url" or "website" in h):
            link_idx = i
        if openings_idx < 0 and (
            "roles open" in h or "roles" == h or "openings" in h or h == "open"
        ):
            openings_idx = i
    return studio_idx, link_idx, openings_idx


def _sheet_cell(row: list[str], index: int) -> str:
    return str(row[index]).strip() if 0 <= index < len(row) else ""


def _sheet_row_entry(
    row: list[str],
    *,
    studio_idx: int,
    link_idx: int,
    openings_idx: int,
) -> dict[str, Any] | None:
    from .scoring import _norm_header, _parse_sheet_openings_flag

    studio = _sheet_cell(row, studio_idx)
    link = _sheet_cell(row, link_idx)
    if not studio or not link:
        return None
    if _norm_header(studio) in {"studio", "studios", "company"}:
        return None
    if not (link.startswith("http://") or link.startswith("https://")):
        return None
    openings_flag = (
        _parse_sheet_openings_flag(row[openings_idx]) if 0 <= openings_idx < len(row) else "unknown"
    )
    return {"studio": studio, "careersUrl": link, "openingsFlag": openings_flag}


def parse_game_studio_sheet_csv(csv_text: str) -> list[dict[str, Any]]:
    rows = _sheet_csv_rows(csv_text)
    if len(rows) < 2:
        return []

    header_idx = _sheet_header_index(rows)
    if header_idx < 0:
        return []

    headers = _normalized_sheet_headers(rows[header_idx])
    studio_idx, link_idx, openings_idx = _sheet_column_indices(headers)
    if studio_idx < 0 or link_idx < 0:
        return []

    out: list[dict[str, Any]] = []
    seen = set()
    for row in rows[header_idx + 1 :]:
        entry = _sheet_row_entry(
            row,
            studio_idx=studio_idx,
            link_idx=link_idx,
            openings_idx=openings_idx,
        )
        if entry is None:
            continue
        key = f"{entry['studio']}|{entry['careersUrl']}".lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(entry)
    return out


def _sheet_directory_config_section(config: dict[str, Any] | None) -> dict[str, Any]:
    return config_section(
        config,
        "sheetDirectory",
        defaults=dict(DEFAULT_DISCOVERY_CONFIG.get("sheetDirectory") or {}),
    )


def _sheet_directory_audit_signature(
    *,
    sheet_id: str,
    gid: str,
    recovery_enabled: bool,
    recovery_url_limit: int,
) -> dict[str, Any]:
    return {
        "parserVersion": SHEET_DIRECTORY_AUDIT_SCHEMA_VERSION,
        "sheetId": str(sheet_id),
        "gid": str(gid),
        "maxRows": str(os.getenv("BALUFFO_SHEET_DIRECTORY_MAX_ROWS") or "").strip(),
        "activeAuditRecoveryEnabled": bool(recovery_enabled),
        "activeAuditRecoveryUrlLimit": int(recovery_url_limit),
        "recoveryLogicVersion": RECOVERY_LOGIC_VERSION,
    }


def _fetch_sheet_csv(
    *,
    timeout_s: int,
    sheet_id: str,
    gid: str,
    fetcher: Any,
) -> tuple[str, str, list[str], str, int]:
    result = fetch_first_nonempty_text(
        game_studios_sheet_candidate_urls(sheet_id, gid),
        timeout_s=timeout_s,
        fetcher=fetcher,
    )
    return (
        result.text,
        result.last_error,
        result.attempted_urls,
        result.selected_url,
        result.duration_ms,
    )


def _sheet_max_rows() -> int | None:
    limit_raw = os.getenv("BALUFFO_SHEET_DIRECTORY_MAX_ROWS")
    if not limit_raw:
        return None
    try:
        return max(1, int(limit_raw))
    except ValueError:
        return None


def _selected_sheet_entries(raw_entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    entries = sorted(
        raw_entries,
        key=lambda row: 0 if str(row.get("openingsFlag") or "") == "yes" else 1,
    )
    max_rows = _sheet_max_rows()
    return entries[:max_rows] if max_rows is not None else entries


def _sheet_opening_counts(entries: list[dict[str, Any]]) -> dict[str, int]:
    return {
        "yesRows": sum(1 for row in entries if str(row.get("openingsFlag") or "") == "yes"),
        "speculativeRows": sum(
            1 for row in entries if str(row.get("openingsFlag") or "") == "speculative"
        ),
        "noRows": sum(1 for row in entries if str(row.get("openingsFlag") or "") == "no"),
        "unknownRows": sum(
            1
            for row in entries
            if str(row.get("openingsFlag") or "") not in ("yes", "speculative", "no")
        ),
    }


def _sheet_evidence_for_openings_flag(openings_flag: str) -> tuple[list[str], int, bool]:
    evidence_types = ["sheet_directory", "sheet_row"]
    if openings_flag == "yes":
        return [*evidence_types, "sheet_roles_open_yes"], 46, False
    if openings_flag == "speculative":
        return [*evidence_types, "sheet_roles_open_speculative"], 18, True
    if openings_flag == "no":
        return [*evidence_types, "sheet_roles_open_no"], 12, True
    return [*evidence_types, "sheet_roles_open_unknown"], 16, True


def _append_sheet_entry_candidate(
    entry: dict[str, Any],
    *,
    provider_candidates: list[dict[str, Any]],
    static_candidates: list[dict[str, Any]],
    failures: list[dict[str, Any]],
) -> bool:
    studio = str(entry.get("studio") or "").strip()
    careers_url = str(entry.get("careersUrl") or "").strip()
    openings_flag = str(entry.get("openingsFlag") or "unknown")
    if not studio or not careers_url:
        return False
    try:
        _ = urlparse(careers_url)
    except ValueError as exc:
        failures.append(
            {
                "name": careers_url,
                "adapter": "sheet_directory",
                "error": f"invalid careers url: {exc}",
                "stage": "directory_detail_parse",
            }
        )
        return True

    evidence_types, evidence_score, weak_signal = _sheet_evidence_for_openings_flag(openings_flag)
    row = build_known_directory_entry_candidate(
        target_url=careers_url,
        studio=studio,
        nl_priority=False,
        discovery_method="sheet_directory",
        discovery_stage="sheet_directory",
        evidence_source="game_studios_sheet",
        evidence_types=evidence_types,
        evidence_score=evidence_score,
        name_suffix="Sheet",
        enabled_by_default=None,
        weak_signal=bool(weak_signal),
        extra_fields={
            "sourceDirectory": "game_studios_sheet",
            "sourceDirectoryUrl": GAME_STUDIOS_SHEET_URL,
            "sourceDirectoryEntryUrl": careers_url,
        },
        infer_provider=infer_web_candidate,
    )
    if str(row.get("adapter") or "") == "static":
        static_candidates.append(row)
    else:
        provider_candidates.append(row)
    return False


def _sheet_static_row_key(row: dict[str, Any]) -> str:
    return str(
        row.get("sourceDirectoryEntryUrl") or row.get("careersUrl") or row.get("listing_url") or ""
    ).strip()


def _sheet_recovery_request(row: dict[str, Any]) -> DirectoryRecoveryRequest | None:
    page_url = _sheet_static_row_key(row)
    studio = str(row.get("studio") or row.get("company") or row.get("name") or "").strip()
    return http_recovery_request_from_context(
        FetchedPageContext(
            page_url=page_url,
            html="",
            studio=studio,
            nl_priority=bool(row.get("nlPriority")),
            discovery_method="sheet_directory",
            payload=dict(row),
            recovery_key=page_url,
        ),
        adapter="sheet_directory",
    )


def _sheet_recovery_evidence(payload: dict[str, Any]) -> tuple[list[str], int, bool]:
    raw_evidence = payload.get("evidenceTypes")
    evidence_types = (
        [str(value) for value in raw_evidence if str(value or "").strip()]
        if isinstance(raw_evidence, list)
        else ["sheet_directory", "sheet_row"]
    )
    try:
        evidence_score = int(payload.get("evidenceScore") or 0)
    except (TypeError, ValueError):
        evidence_score = 0
    return evidence_types, evidence_score, bool(payload.get("weakSignal"))


def _sheet_recovery_extra_fields(context: FetchedPageContext) -> dict[str, Any]:
    source_entry_url = str(
        context.payload.get("sourceDirectoryEntryUrl") or context.recovery_key or context.page_url
    ).strip()
    return {
        "sourceDirectory": "game_studios_sheet",
        "sourceDirectoryUrl": GAME_STUDIOS_SHEET_URL,
        "sourceDirectoryEntryUrl": source_entry_url,
    }


def _sheet_recovery_provider_rows(
    providers: list[dict[str, Any]],
    context: FetchedPageContext,
) -> list[dict[str, Any]]:
    evidence_types, evidence_score, weak_signal = _sheet_recovery_evidence(context.payload)
    source_entry_url = str(
        context.payload.get("sourceDirectoryEntryUrl") or context.recovery_key or context.page_url
    ).strip()
    rows: list[dict[str, Any]] = []
    for provider in providers:
        row = apply_directory_provenance(
            provider,
            evidence_source="game_studios_sheet",
            evidence_types=evidence_types,
            source_directory="game_studios_sheet",
            source_directory_url=GAME_STUDIOS_SHEET_URL,
            source_directory_entry_url=source_entry_url,
            careers_url_fallback=context.page_url,
            evidence_score_floor=evidence_score,
        )
        row["discoveryMethod"] = "sheet_directory"
        row["discoveryStage"] = "sheet_directory"
        row["weakSignal"] = bool(row.get("weakSignal")) or weak_signal
        rows.append(row)
    return rows


def _sheet_recovery_explicit_static(
    explicit_careers_url: str,
    context: FetchedPageContext,
) -> dict[str, Any]:
    evidence_types, evidence_score, weak_signal = _sheet_recovery_evidence(context.payload)
    return build_known_directory_entry_candidate(
        target_url=explicit_careers_url,
        studio=context.studio,
        nl_priority=context.nl_priority,
        discovery_method="sheet_directory",
        discovery_stage="sheet_directory",
        evidence_source="game_studios_sheet",
        evidence_types=evidence_types,
        evidence_score=evidence_score,
        name_suffix="Sheet",
        enabled_by_default=None,
        weak_signal=weak_signal,
        extra_fields=_sheet_recovery_extra_fields(context),
    )


def _sheet_recovery_generic_static(
    candidate: dict[str, Any],
    context: FetchedPageContext,
) -> dict[str, Any]:
    evidence_types, evidence_score, weak_signal = _sheet_recovery_evidence(context.payload)
    row = apply_directory_provenance(
        candidate,
        evidence_source="game_studios_sheet",
        evidence_types=evidence_types,
        source_directory="game_studios_sheet",
        source_directory_url=GAME_STUDIOS_SHEET_URL,
        source_directory_entry_url=str(
            context.payload.get("sourceDirectoryEntryUrl")
            or context.recovery_key
            or context.page_url
        ).strip(),
        careers_url_fallback=context.page_url,
        evidence_score_floor=evidence_score,
    )
    row["discoveryMethod"] = "sheet_directory"
    row["discoveryStage"] = "sheet_directory"
    row["weakSignal"] = bool(row.get("weakSignal")) or weak_signal
    return row


def _sheet_recovery_result_candidates(
    result: dict[str, Any],
    request: DirectoryRecoveryRequest,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    return recovery_result_candidates_from_strategy(
        result,
        request,
        strategy=PageOutcomeStrategy(
            provider_rows=_sheet_recovery_provider_rows,
            explicit_static=_sheet_recovery_explicit_static,
            generic_static=_sheet_recovery_generic_static,
        ),
        discovery_method="sheet_directory",
        nl_priority=bool((request.payload or {}).get("nlPriority")),
        include_source_page_url=False,
    )


def _apply_sheet_directory_recovery(
    scan_result: dict[str, Any],
    *,
    timeout_s: int,
    fetcher: Any,
    recovery_url_limit: int,
) -> dict[str, Any]:
    static_candidates = list(scan_result.get("staticCandidates") or [])
    requests = [
        request
        for row in static_candidates
        if (request := _sheet_recovery_request(row)) is not None
    ]
    if not requests:
        return scan_result

    recovery = run_recovery_for_requests(
        timeout_s,
        requests,
        fetcher=fetcher,
        total_concurrency=SHEET_DIRECTORY_RECOVERY_FETCH_CONCURRENCY,
        per_host_concurrency=SHEET_DIRECTORY_RECOVERY_PER_HOST_CONCURRENCY,
        analyze_result=_sheet_recovery_result_candidates,
        progress_label="Sheet directory",
        url_limit=recovery_url_limit,
    )
    recovery_scan_result = dict(scan_result)
    recovery_scan_result["staticCandidates"] = []
    return apply_recovery_to_scan_result(
        recovery_scan_result,
        recovery,
        provider_dedupe=collapse_competing_candidates,
        static_dedupe=unique_sources,
    )


def _empty_sheet_summary(
    *,
    attempted_urls: list[str],
    selected_csv_url: str,
    csv_fetch_failures: int,
    parse_failures: int = 0,
) -> dict[str, Any]:
    summary = {
        "csvUrlAttempts": len(attempted_urls),
        "selectedCsvUrl": selected_csv_url,
        "rawRows": 0,
        "eligibleRows": 0,
        "invalidUrls": 0,
        "csvFetchFailures": csv_fetch_failures,
    }
    if parse_failures:
        summary["parseFailures"] = parse_failures
    return summary


def _sheet_directory_scan(
    timeout_s: int,
    *,
    sheet_id: str,
    gid: str,
    fetcher: Any,
    emit_log: Any,
    enable_recovery: bool = False,
    recovery_url_limit: int = DEFAULT_RECOVERY_URL_LIMIT,
) -> dict[str, Any]:
    batch_timing: dict[str, Any] = {"sheetId": sheet_id, "gid": gid}

    csv_text, last_error, attempted_urls, selected_csv_url, csv_fetch_ms = _fetch_sheet_csv(
        timeout_s=timeout_s,
        sheet_id=sheet_id,
        gid=gid,
        fetcher=fetcher,
    )
    batch_timing["csvFetchMs"] = csv_fetch_ms

    def build_empty_summary(csv_fetch_failures: int, parse_failures: int) -> dict[str, Any]:
        return _empty_sheet_summary(
            attempted_urls=attempted_urls,
            selected_csv_url=selected_csv_url,
            csv_fetch_failures=csv_fetch_failures,
            parse_failures=parse_failures,
        )

    def build_summary(
        raw_entries: list[dict[str, Any]],
        entries: list[dict[str, Any]],
        invalid_url_count: int,
    ) -> dict[str, Any]:
        return {
            "csvUrlAttempts": len(attempted_urls),
            "selectedCsvUrl": selected_csv_url,
            "rawRows": len(raw_entries),
            "eligibleRows": len(entries),
            **_sheet_opening_counts(entries),
            "invalidUrls": invalid_url_count,
            "csvFetchFailures": 0,
            "parseFailures": 0,
        }

    def parsed_log(
        raw_entries: list[dict[str, Any]],
        entries: list[dict[str, Any]],
        summary: dict[str, Any],
    ) -> None:
        emit_log(
            "Game studios sheet directory rows parsed: "
            f"raw={len(raw_entries)}, usable={len(entries)}, "
            f"openings=yes/{summary['yesRows']}, "
            f"speculative/{summary['speculativeRows']}, "
            f"no/{summary['noRows']}, unknown/{summary['unknownRows']}."
        )

    def candidate_log(
        provider_candidates: list[dict[str, Any]],
        static_candidates: list[dict[str, Any]],
        invalid_url_count: int,
    ) -> None:
        emit_log(
            "Game studios sheet directory candidates after validation: "
            f"provider={len(provider_candidates)}, static={len(static_candidates)}, "
            f"invalid_urls={invalid_url_count}."
        )

    scan_result = run_directory_index_scan(
        source_text=csv_text,
        fetch_error=last_error or "sheet CSV fetch failed",
        parse_entries=parse_game_studio_sheet_csv,
        select_entries=_selected_sheet_entries,
        append_entry=lambda entry, provider_rows, static_rows, failures: (
            _append_sheet_entry_candidate(
                entry,
                provider_candidates=provider_rows,
                static_candidates=static_rows,
                failures=failures,
            )
        ),
        dedupe_provider_candidates=collapse_competing_candidates,
        dedupe_static_candidates=unique_sources,
        build_empty_summary=build_empty_summary,
        build_summary=build_summary,
        index_fetch_failure=lambda error: {
            "name": "game_studios_sheet",
            "adapter": "sheet_directory",
            "error": error,
            "stage": "directory_index_fetch",
        },
        parse_failure=lambda: {
            "name": "game_studios_sheet",
            "adapter": "sheet_directory",
            "error": "no rows parsed (check sheet header/columns)",
            "stage": "directory_parse",
        },
        completed_identity=lambda entry: str(entry.get("careersUrl") or "").strip(),
        batch_timing=batch_timing,
        parsed_callback=parsed_log,
        candidates_callback=candidate_log,
    )
    if not enable_recovery:
        return scan_result
    return _apply_sheet_directory_recovery(
        scan_result,
        timeout_s=timeout_s,
        fetcher=fetcher,
        recovery_url_limit=recovery_url_limit,
    )


def run_sheet_directory_audit(
    timeout_s: int,
    *,
    sheet_id: str | None = None,
    gid: str | None = None,
    config: dict[str, Any] | None = None,
    fetcher=None,
) -> tuple[dict[str, Any], bool]:
    from .reporting import emit_log
    from .web_search import fetch_text

    fetcher = fetcher or fetch_text
    sheet_id = str(sheet_id or GAME_STUDIOS_SHEET_ID)
    gid = str(gid or GAME_STUDIOS_SHEET_GID)
    cfg = _sheet_directory_config_section(config)
    recovery_enabled = bool(cfg.get("activeAuditRecoveryEnabled", True))
    recovery_url_limit = resolve_recovery_url_limit(cfg)
    return run_directory_audit_spec(
        DirectoryAuditRunSpec(
            adapter="sheet_directory",
            schema_version=SHEET_DIRECTORY_AUDIT_SCHEMA_VERSION,
            output_path=audit_artifact_path(
                cfg,
                default_filename="sheet-directory-discovery-audit.json",
            ),
            ttl_minutes=audit_ttl_minutes(cfg),
            signature=_sheet_directory_audit_signature(
                sheet_id=sheet_id,
                gid=gid,
                recovery_enabled=recovery_enabled,
                recovery_url_limit=recovery_url_limit,
            ),
            timeout_s=timeout_s,
            scan=lambda scan_timeout_s: _sheet_directory_scan(
                scan_timeout_s,
                sheet_id=sheet_id,
                gid=gid,
                fetcher=fetcher,
                emit_log=emit_log,
                enable_recovery=recovery_enabled,
                recovery_url_limit=recovery_url_limit,
            ),
            runtime={
                "sheetId": sheet_id,
                "gid": gid,
                "sheetUrl": GAME_STUDIOS_SHEET_URL,
            },
            summary={
                "csvUrlAttempts": 0,
                "selectedCsvUrl": "",
                "rawRows": 0,
                "eligibleRows": 0,
                "invalidUrls": 0,
                "csvFetchFailures": 0,
                "parseFailures": 0,
            },
            sample_limit=SHEET_DIRECTORY_AUDIT_FAILURE_SAMPLE_LIMIT,
            emit_log=emit_log,
        )
    )


def discover_game_studio_sheet_candidates(
    timeout_s: int,
    *,
    sheet_id: str | None = None,
    gid: str | None = None,
    config: dict[str, Any] | None = None,
    fetcher=None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    from .web_search import fetch_text

    fetcher = fetcher or fetch_text
    artifact, _cache_hit = run_sheet_directory_audit(
        timeout_s,
        sheet_id=sheet_id,
        gid=gid,
        config=config,
        fetcher=fetcher,
    )
    return directory_audit_rows(artifact)
