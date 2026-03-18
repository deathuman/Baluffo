from __future__ import annotations

from collections import Counter
from typing import Any, Dict, Iterable, List, Optional, Tuple

from src.shared.utils import now_iso

from .config import EVIDENCE_TYPES_SET
from .io_runtime import endpoint_url
from .scoring import unique_string_list


def build_stage_summary(
    current_candidates: List[Dict[str, Any]],
    *,
    found_endpoint_count: int,
    generated_count_by_stage: Dict[str, int],
    survived_dedupe_count_by_stage: Dict[str, int],
    probed_count_by_stage: Dict[str, int],
    queued_count_by_stage: Dict[str, int],
    probed: int,
    healthy: int,
    failures: List[Dict[str, Any]],
    skipped_duplicate_count: int,
    skipped_invalid: int,
    skipped_low_evidence_probe_count: int,
    validation_skipped_count: int,
    probe_failed_count: int,
    queue_filtered_count: int,
    adapter_counter: Counter[str],
    method_counter: Counter[str],
    duplicate_reasons: Dict[str, int],
    deferred_counts: Optional[Dict[str, int]] = None,
    thresholds: Dict[str, Any],
    phase: str = "",
    phase_label: str = "",
) -> Dict[str, Any]:
    """Build the discovery report summary dict. Pure function; all inputs passed explicitly."""
    deferred_reason_rows = deferred_counts or {}
    deferred_by_cap = int(sum(int(value or 0) for value in deferred_reason_rows.values()))
    return {
        "phase": str(phase or ""),
        "phaseLabel": str(phase_label or ""),
        "probedCount": probed,
        "healthyCount": healthy,
        "newCandidateCount": len(current_candidates),
        "taEnvCandidateCount": sum(
            1 for row in current_candidates if "target_role_signal" in row.get("reasons", [])
        ),
        "nlCandidateCount": sum(1 for row in current_candidates if bool(row.get("nlPriority"))),
        "failedProbeCount": len([row for row in failures if str(row.get("stage")) == "probe"]),
        "probeMissCount": len([row for row in failures if str(row.get("stage")) == "probe_miss"]),
        "foundEndpointCount": found_endpoint_count,
        "probedCandidateCount": probed,
        "queuedCandidateCount": len([row for row in current_candidates if not bool(row.get("deferred"))]),
        "discoverableButDeferredCount": len([row for row in current_candidates if bool(row.get("deferred"))]),
        "skippedDuplicateCount": skipped_duplicate_count,
        "skippedInvalidCount": skipped_invalid,
        "skippedLowEvidenceProbeCount": skipped_low_evidence_probe_count,
        "adapterCounts": dict(adapter_counter),
        "methodCounts": dict(method_counter),
        "generatedCountByStage": dict(generated_count_by_stage),
        "survivedDedupeCountByStage": dict(survived_dedupe_count_by_stage),
        "probedCountByStage": dict(probed_count_by_stage),
        "queuedCountByStage": dict(queued_count_by_stage),
        "duplicateReasons": dict(duplicate_reasons),
        "deferredReasons": dict(deferred_reason_rows),
        "thresholds": dict(thresholds),
        "lossAccounting": {
            "generated": int(found_endpoint_count),
            "dedupSkipped": int(skipped_duplicate_count),
            "dedupSkippedReasons": dict(duplicate_reasons),
            "validationSkipped": int(validation_skipped_count),
            "lowEvidenceSkipped": int(skipped_low_evidence_probe_count),
            "probeFailed": int(probe_failed_count),
            "queueFiltered": int(queue_filtered_count),
            "deferredByCap": deferred_by_cap,
            "queued": int(len([row for row in current_candidates if not bool(row.get("deferred"))])),
        },
    }


def emit_log(message: str) -> None:
    line = f"[{now_iso()}] {str(message or '').strip()}"
    print(line, flush=True)


def _validate_evidence_types(values: List[str], *, context: str) -> List[str]:
    cleaned = unique_string_list([str(item or "").strip() for item in (values or []) if str(item or "").strip()])
    unknown = [item for item in cleaned if item not in EVIDENCE_TYPES_SET]
    if unknown:
        emit_log(f"Warning: dropping unknown evidenceTypes in {context}: {unknown}")
    return [item for item in cleaned if item in EVIDENCE_TYPES_SET]


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
        row["evidenceTypes"] = _validate_evidence_types(
            list(row.get("evidenceTypes") or ["seed_curated"]),
            context="stage_curated_seed_candidates",
        )
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
            row["evidenceTypes"] = _validate_evidence_types(
                list(row.get("evidenceTypes") or []),
                context="merge_candidate_streams",
            )
            row["evidenceScore"] = int(row.get("evidenceScore") or 0)
            rows.append(row)
    return rows

