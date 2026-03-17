from __future__ import annotations

from collections import Counter
from typing import Any, Dict, Iterable, List, Tuple

from src.shared.utils import now_iso

from .io_runtime import endpoint_url
from .scoring import unique_string_list


def emit_log(message: str) -> None:
    line = f"[{now_iso()}] {str(message or '').strip()}"
    print(line, flush=True)


def summarize_failures(failures: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    counter: Counter[str] = Counter()
    for row in failures:
        key = str(row.get("key") or row.get("adapter") or "unknown")
        counter[key] += 1
    return [{"key": key, "count": count} for key, count in counter.most_common(5)]


def stage_curated_seed_candidates() -> List[Dict[str, Any]]:
    from src.source_registry import unique_sources
    import src.source_discovery as sd

    rows: List[Dict[str, Any]] = []
    for raw in getattr(sd, "STATIC_DISCOVERY_CANDIDATES", []):
        if not isinstance(raw, dict):
            continue
        row = dict(raw)
        row["discoveryMethod"] = str(row.get("discoveryMethod") or "seed")
        row["discoveryStage"] = "curated_seed"
        row["evidenceScore"] = int(row.get("evidenceScore") or 52)
        row["evidenceTypes"] = list(row.get("evidenceTypes") or ["curated_seed"])
        row["evidenceSource"] = str(row.get("evidenceSource") or "seed")
        row["careersUrl"] = str(row.get("careersUrl") or endpoint_url(row) or "")
        rows.append(row)
    return unique_sources(rows)


def merge_candidate_streams(
    streams: Iterable[Tuple[str, List[Dict[str, Any]]]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for stage, items in streams:
        for raw in items:
            if not isinstance(raw, dict):
                continue
            row = dict(raw)
            row["discoveryStage"] = str(row.get("discoveryStage") or stage)
            row["discoveryMethod"] = str(row.get("discoveryMethod") or ("seed" if stage == "curated_seed" else "pattern"))
            row["discoveredAt"] = str(row.get("discoveredAt") or now_iso())
            row["evidenceTypes"] = unique_string_list(row.get("evidenceTypes") or [])
            row["evidenceScore"] = int(row.get("evidenceScore") or 0)
            rows.append(row)
    return rows

