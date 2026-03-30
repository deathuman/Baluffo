from __future__ import annotations

import csv
import os
from io import StringIO
from typing import Any
from urllib.parse import urlparse

from src.source_registry import unique_sources

from .config import (
    GAME_STUDIOS_SHEET_GID,
    GAME_STUDIOS_SHEET_ID,
    GAME_STUDIOS_SHEET_URL,
)
from .io_runtime import collapse_competing_candidates
from .scoring import unique_string_list
from .web_search import infer_web_candidate


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
    provider_candidates: list[dict[str, Any]] = []
    static_candidates: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []

    csv_text = ""
    last_error = ""
    for url in game_studios_sheet_candidate_urls(sheet_id, gid):
        try:
            csv_text = fetcher(url, timeout_s)
            if str(csv_text or "").strip():
                break
        except Exception as exc:  # noqa: BLE001
            last_error = str(exc)
            continue
    if not str(csv_text or "").strip():
        failures.append(
            {
                "name": "game_studios_sheet",
                "adapter": "sheet_directory",
                "error": last_error or "sheet CSV fetch failed",
                "stage": "directory_index_fetch",
            }
        )
        return [], [], failures

    raw_entries = parse_game_studio_sheet_csv(csv_text)
    if not raw_entries and str(csv_text or "").strip():
        failures.append(
            {
                "name": "game_studios_sheet",
                "adapter": "sheet_directory",
                "error": "no rows parsed (check sheet header/columns)",
                "stage": "directory_parse",
            }
        )
        return [], [], failures

    total_raw = len(raw_entries)

    def _entry_priority(row: dict[str, Any]) -> int:
        return 0 if str(row.get("openingsFlag") or "") == "yes" else 1

    limit_raw = os.getenv("BALUFFO_SHEET_DIRECTORY_MAX_ROWS")
    if limit_raw:
        try:
            max_rows = max(1, int(limit_raw))
        except ValueError:
            max_rows = None
    else:
        max_rows = None

    entries_unsliced = sorted(raw_entries, key=_entry_priority)
    entries = entries_unsliced[:max_rows] if max_rows is not None else entries_unsliced

    yes_count = sum(1 for row in entries if str(row.get("openingsFlag") or "") == "yes")
    speculative_count = sum(
        1 for row in entries if str(row.get("openingsFlag") or "") == "speculative"
    )
    no_count = sum(1 for row in entries if str(row.get("openingsFlag") or "") == "no")
    unknown_count = sum(
        1
        for row in entries
        if str(row.get("openingsFlag") or "") not in ("yes", "speculative", "no")
    )
    emit_log(
        "Game studios sheet directory rows parsed: "
        f"raw={total_raw}, usable={len(entries)}, "
        f"openings=yes/{yes_count}, speculative/{speculative_count}, "
        f"no/{no_count}, unknown/{unknown_count}."
    )

    invalid_url_count = 0
    for entry in entries:
        studio = str(entry.get("studio") or "").strip()
        careers_url = str(entry.get("careersUrl") or "").strip()
        openings_flag = str(entry.get("openingsFlag") or "unknown")
        if not studio or not careers_url:
            continue
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
            invalid_url_count += 1
            continue
        except Exception as exc:  # noqa: BLE001
            failures.append(
                {
                    "name": careers_url or studio or "unknown",
                    "adapter": "sheet_directory",
                    "error": f"unexpected error validating careers url: {exc}",
                    "stage": "directory_detail_parse",
                }
            )
            invalid_url_count += 1
            continue

        evidence_types = ["sheet_directory", "sheet_row"]
        evidence_score = 18
        weak_signal = False
        if openings_flag == "yes":
            evidence_types.append("sheet_roles_open_yes")
            evidence_score = 46
        elif openings_flag == "speculative":
            evidence_types.append("sheet_roles_open_speculative")
            evidence_score = 18
            weak_signal = True
        elif openings_flag == "no":
            evidence_types.append("sheet_roles_open_no")
            evidence_score = 12
            weak_signal = True
        else:
            evidence_types.append("sheet_roles_open_unknown")
            evidence_score = 16
            weak_signal = True

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
            continue
        static_candidates.append(
            {
                "name": f"{studio} (Sheet)",
                "studio": studio,
                "company": studio,
                "adapter": "static",
                "pages": [careers_url],
                "listing_url": careers_url,
                "nlPriority": False,
                "discoveryMethod": "sheet_directory",
                "discoveryStage": "sheet_directory",
                "careersUrl": careers_url,
                "evidenceSource": "game_studios_sheet",
                "evidenceTypes": unique_string_list(evidence_types),
                "evidenceScore": int(evidence_score),
                "weakSignal": bool(weak_signal),
                "sourceDirectory": "game_studios_sheet",
                "sourceDirectoryUrl": GAME_STUDIOS_SHEET_URL,
                "sourceDirectoryEntryUrl": careers_url,
            }
        )

    emit_log(
        "Game studios sheet directory candidates after validation: "
        f"provider={len(provider_candidates)}, static={len(static_candidates)}, invalid_urls={invalid_url_count}."
    )

    return (
        collapse_competing_candidates(provider_candidates),
        unique_sources(static_candidates),
        failures,
    )
