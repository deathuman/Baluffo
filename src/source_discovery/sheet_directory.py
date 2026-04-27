from __future__ import annotations

import csv
import os
import time
from io import StringIO
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from src.source_registry import unique_sources

from . import audit_ledger
from .audit_config import audit_artifact_path, audit_ttl_minutes, config_section
from .config import (
    DEFAULT_DISCOVERY_CONFIG,
    GAME_STUDIOS_SHEET_GID,
    GAME_STUDIOS_SHEET_ID,
    GAME_STUDIOS_SHEET_URL,
)
from .directory_audit import run_directory_audit
from .io_runtime import collapse_competing_candidates
from .scoring import unique_string_list
from .static_candidates import build_known_careers_url_candidate
from .web_search import infer_web_candidate

SHEET_DIRECTORY_AUDIT_SCHEMA_VERSION = 1
SHEET_DIRECTORY_AUDIT_FAILURE_SAMPLE_LIMIT = 10_000


def game_studios_sheet_candidate_urls(sheet_id: str, gid: str) -> list[str]:
    csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
    gviz_csv_url = (
        f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&gid={gid}"
    )
    pub_csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/pub?output=csv"
    return [gviz_csv_url, pub_csv_url, csv_url]


def parse_game_studio_sheet_csv(csv_text: str) -> list[dict[str, Any]]:
    from .scoring import _norm_header, _parse_sheet_openings_flag

    rows = list(csv.reader(StringIO(str(csv_text or ""))))
    if len(rows) < 2:
        return []

    header_idx = -1
    for idx, row in enumerate(rows[:250]):
        normalized_full = [_norm_header(cell) for cell in row]
        normalized = [cell for cell in normalized_full if cell]
        if not normalized:
            continue
        has_studio = "studio" in normalized_full or "company" in normalized_full
        has_link = "link" in normalized_full or "url" in normalized_full
        has_roles = any("roles" in cell or "hiring" in cell for cell in normalized_full if cell)
        if has_studio and has_link and has_roles:
            header_idx = idx
            break
    if header_idx < 0:
        return []

    headers = [_norm_header(cell) for cell in rows[header_idx]]
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
    if studio_idx < 0 or link_idx < 0:
        return []

    out: list[dict[str, Any]] = []
    seen = set()
    for row in rows[header_idx + 1 :]:
        studio = str(row[studio_idx]).strip() if studio_idx < len(row) else ""
        link = str(row[link_idx]).strip() if link_idx < len(row) else ""
        if not studio or not link:
            continue
        if _norm_header(studio) in {"studio", "studios", "company"}:
            continue
        if not (link.startswith("http://") or link.startswith("https://")):
            continue
        openings_flag = (
            _parse_sheet_openings_flag(row[openings_idx])
            if 0 <= openings_idx < len(row)
            else "unknown"
        )
        key = f"{studio}|{link}".lower()
        if key in seen:
            continue
        seen.add(key)
        out.append({"studio": studio, "careersUrl": link, "openingsFlag": openings_flag})
    return out


def _sheet_directory_config_section(config: dict[str, Any] | None) -> dict[str, Any]:
    return config_section(
        config,
        "sheetDirectory",
        defaults=dict(DEFAULT_DISCOVERY_CONFIG.get("sheetDirectory") or {}),
    )


def _sheet_directory_audit_path(config: dict[str, Any] | None) -> Path:
    cfg = _sheet_directory_config_section(config)
    return audit_artifact_path(
        cfg,
        default_filename="sheet-directory-discovery-audit.json",
    )


def _sheet_directory_audit_ttl_minutes(config: dict[str, Any] | None) -> int:
    return audit_ttl_minutes(_sheet_directory_config_section(config))


def _sheet_directory_audit_signature(*, sheet_id: str, gid: str) -> dict[str, Any]:
    return {
        "parserVersion": SHEET_DIRECTORY_AUDIT_SCHEMA_VERSION,
        "sheetId": str(sheet_id),
        "gid": str(gid),
        "maxRows": str(os.getenv("BALUFFO_SHEET_DIRECTORY_MAX_ROWS") or "").strip(),
    }


def _fetch_sheet_csv(
    *,
    timeout_s: int,
    sheet_id: str,
    gid: str,
    fetcher: Any,
) -> tuple[str, str, list[str], str, int]:
    csv_text = ""
    last_error = ""
    attempted_urls: list[str] = []
    selected_csv_url = ""
    started = time.perf_counter()
    for url in game_studios_sheet_candidate_urls(sheet_id, gid):
        attempted_urls.append(url)
        try:
            csv_text = fetcher(url, timeout_s)
            if str(csv_text or "").strip():
                selected_csv_url = url
                break
        except Exception as exc:  # noqa: BLE001
            last_error = str(exc)
            continue
    return csv_text, last_error, attempted_urls, selected_csv_url, audit_ledger.duration_ms(started)


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
    except Exception as exc:  # noqa: BLE001
        failures.append(
            {
                "name": careers_url or studio or "unknown",
                "adapter": "sheet_directory",
                "error": f"unexpected error validating careers url: {exc}",
                "stage": "directory_detail_parse",
            }
        )
        return True

    evidence_types, evidence_score, weak_signal = _sheet_evidence_for_openings_flag(openings_flag)
    inferred = infer_web_candidate(
        careers_url, studio, nl_priority=False, discovery_method="sheet_directory"
    )
    if inferred:
        inferred["discoveryStage"] = "sheet_directory"
        inferred["discoveryMethod"] = "sheet_directory"
        inferred["sourceDirectory"] = "game_studios_sheet"
        inferred["sourceDirectoryUrl"] = GAME_STUDIOS_SHEET_URL
        inferred["sourceDirectoryEntryUrl"] = careers_url
        inferred["evidenceTypes"] = unique_string_list(
            [*(inferred.get("evidenceTypes") or []), *evidence_types]
        )
        inferred["evidenceScore"] = max(int(inferred.get("evidenceScore") or 0), evidence_score)
        inferred["weakSignal"] = bool(inferred.get("weakSignal")) or weak_signal
        inferred["careersUrl"] = careers_url
        provider_candidates.append(inferred)
        return False

    static_candidates.append(
        build_known_careers_url_candidate(
            careers_url,
            studio=studio,
            name_suffix="Sheet",
            nl_priority=False,
            discovery_method="sheet_directory",
            discovery_stage="sheet_directory",
            evidence_source="game_studios_sheet",
            evidence_types=evidence_types,
            evidence_score=int(evidence_score),
            enabled_by_default=None,
            weak_signal=bool(weak_signal),
            extra_fields={
                "sourceDirectory": "game_studios_sheet",
                "sourceDirectoryUrl": GAME_STUDIOS_SHEET_URL,
                "sourceDirectoryEntryUrl": careers_url,
            },
        )
    )
    return False


def _empty_sheet_scan_result(
    *,
    failures: list[dict[str, Any]],
    batch_timing: dict[str, Any],
    attempted_urls: list[str],
    selected_csv_url: str = "",
) -> dict[str, Any]:
    return {
        "providerCandidates": [],
        "staticCandidates": [],
        "failures": failures,
        "summary": {
            "csvUrlAttempts": len(attempted_urls),
            "selectedCsvUrl": selected_csv_url,
            "rawRows": 0,
            "eligibleRows": 0,
            "invalidUrls": 0,
            "csvFetchFailures": len(failures),
        },
        "progress": {"complete": True, "cursor": 0, "completedUrlIdentities": []},
        "batchTiming": batch_timing,
    }


def _sheet_directory_scan(
    timeout_s: int,
    *,
    sheet_id: str,
    gid: str,
    fetcher: Any,
    emit_log: Any,
) -> dict[str, Any]:
    provider_candidates: list[dict[str, Any]] = []
    static_candidates: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    batch_timing: dict[str, Any] = {"sheetId": sheet_id, "gid": gid}

    csv_text, last_error, attempted_urls, selected_csv_url, csv_fetch_ms = _fetch_sheet_csv(
        timeout_s=timeout_s,
        sheet_id=sheet_id,
        gid=gid,
        fetcher=fetcher,
    )
    batch_timing["csvFetchMs"] = csv_fetch_ms
    if not str(csv_text or "").strip():
        failures.append(
            {
                "name": "game_studios_sheet",
                "adapter": "sheet_directory",
                "error": last_error or "sheet CSV fetch failed",
                "stage": "directory_index_fetch",
            }
        )
        return _empty_sheet_scan_result(
            failures=failures,
            batch_timing=batch_timing,
            attempted_urls=attempted_urls,
            selected_csv_url=selected_csv_url,
        )

    started = time.perf_counter()
    raw_entries = parse_game_studio_sheet_csv(csv_text)
    batch_timing["parseMs"] = audit_ledger.duration_ms(started)
    if not raw_entries and str(csv_text or "").strip():
        failures.append(
            {
                "name": "game_studios_sheet",
                "adapter": "sheet_directory",
                "error": "no rows parsed (check sheet header/columns)",
                "stage": "directory_parse",
            }
        )
        parse_result = _empty_sheet_scan_result(
            failures=failures,
            batch_timing=batch_timing,
            attempted_urls=attempted_urls,
            selected_csv_url=selected_csv_url,
        )
        parse_result["summary"] = {
            "csvUrlAttempts": len(attempted_urls),
            "selectedCsvUrl": selected_csv_url,
            "rawRows": 0,
            "eligibleRows": 0,
            "invalidUrls": 0,
            "csvFetchFailures": 0,
            "parseFailures": 1,
        }
        return parse_result

    total_raw = len(raw_entries)
    entries = _selected_sheet_entries(raw_entries)
    opening_counts = _sheet_opening_counts(entries)
    emit_log(
        "Game studios sheet directory rows parsed: "
        f"raw={total_raw}, usable={len(entries)}, "
        f"openings=yes/{opening_counts['yesRows']}, "
        f"speculative/{opening_counts['speculativeRows']}, "
        f"no/{opening_counts['noRows']}, unknown/{opening_counts['unknownRows']}."
    )

    invalid_url_count = 0
    started = time.perf_counter()
    for entry in entries:
        if _append_sheet_entry_candidate(
            entry,
            provider_candidates=provider_candidates,
            static_candidates=static_candidates,
            failures=failures,
        ):
            invalid_url_count += 1
    batch_timing["candidateAnalysisMs"] = audit_ledger.duration_ms(started)

    provider_candidates = collapse_competing_candidates(provider_candidates)
    static_candidates = unique_sources(static_candidates)
    emit_log(
        "Game studios sheet directory candidates after validation: "
        f"provider={len(provider_candidates)}, static={len(static_candidates)}, "
        f"invalid_urls={invalid_url_count}."
    )

    return {
        "providerCandidates": provider_candidates,
        "staticCandidates": static_candidates,
        "failures": failures,
        "summary": {
            "csvUrlAttempts": len(attempted_urls),
            "selectedCsvUrl": selected_csv_url,
            "rawRows": total_raw,
            "eligibleRows": len(entries),
            **opening_counts,
            "invalidUrls": invalid_url_count,
            "csvFetchFailures": 0,
            "parseFailures": 0,
        },
        "progress": {
            "complete": True,
            "cursor": len(entries),
            "completedUrlIdentities": [
                str(row.get("careersUrl") or "").strip()
                for row in entries
                if isinstance(row, dict) and str(row.get("careersUrl") or "").strip()
            ],
        },
        "batchTiming": batch_timing,
    }


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
    return run_directory_audit(
        adapter="sheet_directory",
        schema_version=SHEET_DIRECTORY_AUDIT_SCHEMA_VERSION,
        output_path=_sheet_directory_audit_path(config),
        ttl_minutes=_sheet_directory_audit_ttl_minutes(config),
        signature=_sheet_directory_audit_signature(sheet_id=sheet_id, gid=gid),
        timeout_s=timeout_s,
        scan=lambda scan_timeout_s: _sheet_directory_scan(
            scan_timeout_s,
            sheet_id=sheet_id,
            gid=gid,
            fetcher=fetcher,
            emit_log=emit_log,
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


def discover_game_studio_sheet_candidates(
    timeout_s: int,
    *,
    sheet_id: str | None = None,
    gid: str | None = None,
    fetcher=None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    from .reporting import emit_log
    from .web_search import fetch_text

    fetcher = fetcher or fetch_text
    sheet_id = str(sheet_id or GAME_STUDIOS_SHEET_ID)
    gid = str(gid or GAME_STUDIOS_SHEET_GID)
    scan = _sheet_directory_scan(
        timeout_s,
        sheet_id=sheet_id,
        gid=gid,
        fetcher=fetcher,
        emit_log=emit_log,
    )
    return (
        list(scan.get("providerCandidates") or []),
        list(scan.get("staticCandidates") or []),
        list(scan.get("failures") or []),
    )
