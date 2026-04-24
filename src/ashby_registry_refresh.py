from __future__ import annotations

import json
import re
import sys
from collections.abc import Iterable
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.bridge.registry_tombstones import filter_tombstoned_rows, load_tombstones
from src.jobs.text_utils import clean_text
from src.shared.utils import now_iso
from src.source_registry import ACTIVE_PATH, ensure_source_id, load_json_array, save_json_atomic

ASHBY_REFRESH_REPORT_PATH = Path("data/ashby-registry-refresh-report.json")

CURATED_ASHBY_ROWS: list[dict[str, Any]] = [
    {
        "name": "Bigger Games (Ashby)",
        "studio": "Bigger Games",
        "board_url": "https://jobs.ashbyhq.com/biggergames",
        "careersUrl": "https://jobs.ashbyhq.com/biggergames",
    },
    {
        "name": "Battle Creek Games (Ashby)",
        "studio": "Battle Creek Games",
        "board_url": "https://jobs.ashbyhq.com/battle-creek-games",
        "careersUrl": "https://jobs.ashbyhq.com/battle-creek-games",
    },
    {
        "name": "Day[9]'s Game Studio (Ashby)",
        "studio": "Day[9]'s Game Studio",
        "board_url": "https://jobs.ashbyhq.com/day9",
        "careersUrl": "https://jobs.ashbyhq.com/day9",
    },
    {
        "name": "Endgame (Ashby)",
        "studio": "Endgame",
        "board_url": "https://jobs.ashbyhq.com/Endgame",
        "careersUrl": "https://jobs.ashbyhq.com/Endgame",
    },
    {
        "name": "FS Studio (Ashby)",
        "studio": "FS Studio",
        "board_url": "https://jobs.ashbyhq.com/fs-studio",
        "careersUrl": "https://jobs.ashbyhq.com/fs-studio",
    },
    {
        "name": "Improbable (Ashby)",
        "studio": "Improbable",
        "board_url": "https://jobs.ashbyhq.com/improbable",
        "careersUrl": "https://jobs.ashbyhq.com/improbable",
    },
    {
        "name": "Joyteractive (Ashby)",
        "studio": "Joyteractive",
        "board_url": "https://jobs.ashbyhq.com/Joyteractive",
        "careersUrl": "https://jobs.ashbyhq.com/Joyteractive",
    },
    {
        "name": "TapBlaze (Ashby)",
        "studio": "TapBlaze",
        "board_url": "https://jobs.ashbyhq.com/tapblaze",
        "careersUrl": "https://jobs.ashbyhq.com/tapblaze",
    },
    {
        "name": "Voldex Games (Ashby)",
        "studio": "Voldex Games",
        "board_url": "https://jobs.ashbyhq.com/voldex",
        "careersUrl": "https://jobs.ashbyhq.com/voldex",
    },
]

DISCOVERY_ASHBY_ROWS: list[dict[str, Any]] = [
    {
        "name": "GameChanger (Ashby)",
        "studio": "GameChanger",
        "board_url": "https://jobs.ashbyhq.com/gamechanger",
        "careersUrl": "https://jobs.ashbyhq.com/gamechanger",
        "relevanceHint": "sports-tech",
    },
    {
        "name": "Level (Ashby)",
        "studio": "Level",
        "board_url": "https://jobs.ashbyhq.com/level",
        "careersUrl": "https://jobs.ashbyhq.com/level",
    },
    {
        "name": "Unblocked (Ashby)",
        "studio": "Unblocked",
        "board_url": "https://jobs.ashbyhq.com/Unblocked",
        "careersUrl": "https://jobs.ashbyhq.com/Unblocked",
    },
    {
        "name": "Sim (Ashby)",
        "studio": "Sim",
        "board_url": "https://jobs.ashbyhq.com/sim",
        "careersUrl": "https://jobs.ashbyhq.com/sim",
    },
    {
        "name": "The Utopia Venture Platform (Ashby)",
        "studio": "The Utopia Venture Platform",
        "board_url": "https://jobs.ashbyhq.com/the-studio",
        "careersUrl": "https://jobs.ashbyhq.com/the-studio",
    },
]

GAME_RELEVANCE_TOKENS = (
    "game",
    "games",
    "gaming",
    "studio",
    "studios",
    "sports",
    "esports",
    "fantasy",
)


def _normalize_board_url(url: str) -> str:
    text = clean_text(url)
    if not text:
        return ""
    parsed = urlparse(text)
    path = parsed.path.rstrip("/")
    if path.lower().endswith("/jobs"):
        path = path[:-5] or "/"
    normalized = parsed._replace(path=path or "/", query="", fragment="")
    return normalized.geturl().rstrip("/")


def _default_fetch_text(url: str, timeout_s: int) -> str:
    request = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(request, timeout=timeout_s) as response:
        return str(response.read().decode("utf-8", errors="replace"))


def _probe_ashby_board(
    row: dict[str, Any], *, fetch_text=_default_fetch_text, timeout_s: int = 15
) -> dict[str, Any]:
    board_url = _normalize_board_url(
        clean_text(row.get("board_url")) or clean_text(row.get("careersUrl"))
    )
    if not board_url:
        return {
            "status": "invalid",
            "board_url": "",
            "postingsCount": 0,
            "organizationName": "",
            "error": "missing board_url",
        }
    try:
        html = fetch_text(board_url, timeout_s)
    except (HTTPError, URLError, TimeoutError, OSError) as exc:
        return {
            "status": "error",
            "board_url": board_url,
            "postingsCount": 0,
            "organizationName": "",
            "error": str(exc),
        }
    match = re.search(r"window\.__appData\s*=\s*(\{.*?\});", html, re.S)
    if not match:
        return {
            "status": "dead",
            "board_url": board_url,
            "postingsCount": 0,
            "organizationName": "",
            "error": "missing embedded app data",
        }
    try:
        payload = json.loads(match.group(1))
    except json.JSONDecodeError as exc:
        return {
            "status": "error",
            "board_url": board_url,
            "postingsCount": 0,
            "organizationName": "",
            "error": str(exc),
        }
    organization = payload.get("organization") if isinstance(payload, dict) else {}
    job_board = payload.get("jobBoard") if isinstance(payload, dict) else {}
    postings = job_board.get("jobPostings") if isinstance(job_board, dict) else []
    if not isinstance(postings, list):
        postings = []
    organization_name = clean_text((organization or {}).get("name"))
    status = "ok_with_jobs" if postings else "empty"
    return {
        "status": status,
        "board_url": board_url,
        "postingsCount": len(postings),
        "organizationName": organization_name,
        "jobTitles": [clean_text(item.get("title")) for item in postings if isinstance(item, dict)],
        "error": "",
    }


def _is_relevant_ashby_candidate(row: dict[str, Any], validation: dict[str, Any]) -> bool:
    hint = clean_text(row.get("relevanceHint")).lower()
    if hint in {"game", "games", "gaming", "sports-tech", "esports"}:
        return True
    fields = [
        clean_text(row.get("name")),
        clean_text(row.get("studio")),
        clean_text(validation.get("organizationName")),
        " ".join(clean_text(title) for title in (validation.get("jobTitles") or [])[:12]),
    ]
    haystack = " ".join(fields).lower()
    return any(token in haystack for token in GAME_RELEVANCE_TOKENS)


def _ashby_row_from_validation(row: dict[str, Any], validation: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(row)
    normalized["adapter"] = "ashby"
    normalized["name"] = (
        clean_text(normalized.get("name")) or f"{clean_text(normalized.get('studio'))} (Ashby)"
    )
    normalized["studio"] = (
        clean_text(normalized.get("studio"))
        or clean_text(validation.get("organizationName"))
        or normalized["name"].replace(" (Ashby)", "")
    )
    normalized["board_url"] = clean_text(validation.get("board_url"))
    normalized["careersUrl"] = clean_text(normalized.get("careersUrl")) or clean_text(
        validation.get("board_url")
    )
    normalized["remoteFriendly"] = bool(normalized.get("remoteFriendly", True))
    normalized["nlPriority"] = bool(normalized.get("nlPriority", False))
    normalized["enabledByDefault"] = bool(normalized.get("enabledByDefault", True))
    normalized["jobsFound"] = int(validation.get("postingsCount") or 0)
    normalized["sampleCount"] = int(validation.get("postingsCount") or 0)
    normalized["confidence"] = clean_text(normalized.get("confidence")) or "high"
    normalized["lastProbedAt"] = now_iso()
    normalized["lastValidationStatus"] = clean_text(validation.get("status")) or "ok_with_jobs"
    normalized["reasons"] = list(normalized.get("reasons") or [])
    if "live_jobs_detected" not in normalized["reasons"]:
        normalized["reasons"].append("live_jobs_detected")
    if "ashby_curated_validated" not in normalized["reasons"]:
        normalized["reasons"].append("ashby_curated_validated")
    return ensure_source_id(normalized)


def refresh_active_ashby_registry(
    *,
    active_path: Path = ACTIVE_PATH,
    report_path: Path = ASHBY_REFRESH_REPORT_PATH,
    curated_rows: Iterable[dict[str, Any]] = CURATED_ASHBY_ROWS,
    discovery_rows: Iterable[dict[str, Any]] = DISCOVERY_ASHBY_ROWS,
    fetch_text=_default_fetch_text,
    timeout_s: int = 15,
) -> dict[str, Any]:
    active_rows = load_json_array(active_path, [])
    tombstones = load_tombstones()
    active_rows = filter_tombstoned_rows(active_rows, tombstones)
    non_ashby_rows = [
        dict(row) for row in active_rows if clean_text(row.get("adapter")).lower() != "ashby"
    ]
    existing_ashby_rows = [
        dict(row) for row in active_rows if clean_text(row.get("adapter")).lower() == "ashby"
    ]

    candidates_by_key: dict[str, dict[str, Any]] = {}
    curated_names = {clean_text(row.get("name")) for row in curated_rows}
    for row in [*existing_ashby_rows, *list(curated_rows), *list(discovery_rows)]:
        key = _normalize_board_url(
            clean_text(row.get("board_url"))
            or clean_text(row.get("careersUrl"))
            or clean_text(row.get("name"))
        )
        if not key:
            continue
        if key not in candidates_by_key:
            candidates_by_key[key] = dict(row)
        else:
            merged = dict(candidates_by_key[key])
            merged.update({k: v for k, v in dict(row).items() if clean_text(v)})
            candidates_by_key[key] = merged

    kept_rows: list[dict[str, Any]] = []
    removed_rows: list[dict[str, Any]] = []
    rejected_candidates: list[dict[str, Any]] = []
    added_count = 0
    normalized_existing_keys = {
        _normalize_board_url(clean_text(row.get("board_url")) or clean_text(row.get("careersUrl")))
        for row in existing_ashby_rows
    }

    for key, row in sorted(
        candidates_by_key.items(), key=lambda item: clean_text(item[1].get("name")).lower()
    ):
        validation = _probe_ashby_board(row, fetch_text=fetch_text, timeout_s=timeout_s)
        if clean_text(validation.get("status")) == "ok_with_jobs":
            is_existing = key in normalized_existing_keys
            is_curated = clean_text(row.get("name")) in curated_names
            if (
                not is_existing
                and not is_curated
                and not _is_relevant_ashby_candidate(row, validation)
            ):
                rejected_candidates.append(
                    {
                        "name": clean_text(row.get("name")) or clean_text(row.get("studio")) or key,
                        "boardUrl": clean_text(validation.get("board_url")) or key,
                        "status": "rejected_irrelevant",
                        "postingsCount": int(validation.get("postingsCount") or 0),
                    }
                )
                continue
            kept_rows.append(_ashby_row_from_validation(row, validation))
            if key not in normalized_existing_keys:
                added_count += 1
            continue
        removed_rows.append(
            {
                "name": clean_text(row.get("name")) or clean_text(row.get("studio")) or key,
                "boardUrl": clean_text(validation.get("board_url")) or key,
                "status": clean_text(validation.get("status")) or "removed",
                "postingsCount": int(validation.get("postingsCount") or 0),
                "error": clean_text(validation.get("error")),
            }
        )

    next_rows = filter_tombstoned_rows([*non_ashby_rows, *kept_rows], tombstones)
    save_json_atomic(active_path, next_rows)

    report = {
        "generatedAt": now_iso(),
        "configuredBefore": len(existing_ashby_rows),
        "configuredAfter": len(kept_rows),
        "addedCount": int(added_count),
        "removedCount": len(removed_rows),
        "rejectedCount": len(rejected_candidates),
        "keptRows": [
            {
                "name": clean_text(row.get("name")),
                "boardUrl": clean_text(row.get("board_url")),
                "jobsFound": int(row.get("jobsFound") or 0),
            }
            for row in kept_rows
        ],
        "removedRows": removed_rows,
        "rejectedCandidates": rejected_candidates,
    }
    save_json_atomic(report_path, report)
    return report


def main() -> int:
    report = refresh_active_ashby_registry()
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
