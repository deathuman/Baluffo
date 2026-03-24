from __future__ import annotations

from collections import Counter
from collections.abc import Iterable
from typing import Any

from src.shared.utils import now_iso

from .config import EVIDENCE_TYPES_SET
from .io_runtime import endpoint_url
from .scoring import unique_string_list


def build_stage_summary(
    current_candidates: list[dict[str, Any]],
    *,
    found_endpoint_count: int,
    generated_count_by_stage: dict[str, int],
    survived_dedupe_count_by_stage: dict[str, int],
    probed_count_by_stage: dict[str, int],
    queued_count_by_stage: dict[str, int],
    probed: int,
    healthy: int,
    failures: list[dict[str, Any]],
    skipped_duplicate_count: int,
    skipped_invalid: int,
    skipped_low_evidence_probe_count: int,
    validation_skipped_count: int,
    probe_failed_count: int,
    queue_filtered_count: int,
    adapter_counter: Counter[str],
    method_counter: Counter[str],
    duplicate_reasons: dict[str, int],
    deferred_counts: dict[str, int] | None = None,
    queued_by_adapter: dict[str, int] | None = None,
    deferred_by_adapter: dict[str, int] | None = None,
    healthy_but_deferred_by_adapter: dict[str, int] | None = None,
    suppressed_static_count: int = 0,
    suppressed_static_by_reason: dict[str, int] | None = None,
    suppressed_static_by_stage: dict[str, int] | None = None,
    thresholds: dict[str, Any],
    phase: str = "",
    phase_label: str = "",
) -> dict[str, Any]:
    """Build the discovery report summary dict. Pure function; all inputs passed explicitly."""
    deferred_reason_rows = deferred_counts or {}
    deferred_by_cap = int(sum(int(value or 0) for value in deferred_reason_rows.values()))
    failed_probe_count_final = len([row for row in failures if str(row.get("stage")) == "probe"])
    probe_miss_count_final = len([row for row in failures if str(row.get("stage")) == "probe_miss"])
    return {
        "phase": str(phase or ""),
        "phaseKey": str(phase or ""),
        "phaseLabel": str(phase_label or ""),
        "probedCount": probed,
        "healthyCount": healthy,
        "newCandidateCount": len(current_candidates),
        "taEnvCandidateCount": sum(
            1 for row in current_candidates if "target_role_signal" in row.get("reasons", [])
        ),
        "nlCandidateCount": sum(1 for row in current_candidates if bool(row.get("nlPriority"))),
        "failedProbeCount": failed_probe_count_final,
        "probeMissCount": probe_miss_count_final,
        "foundEndpointCount": found_endpoint_count,
        "probedCandidateCount": probed,
        "queuedCandidateCount": len(
            [row for row in current_candidates if not bool(row.get("deferred"))]
        ),
        "discoverableButDeferredCount": len(
            [row for row in current_candidates if bool(row.get("deferred"))]
        ),
        "suppressedStaticCount": int(suppressed_static_count),
        "skippedDuplicateCount": skipped_duplicate_count,
        "skippedInvalidCount": skipped_invalid,
        "skippedLowEvidenceProbeCount": skipped_low_evidence_probe_count,
        "adapterCounts": dict(adapter_counter),
        "queuedByAdapter": dict(queued_by_adapter or {}),
        "deferredByAdapter": dict(deferred_by_adapter or {}),
        "healthyButDeferredByAdapter": dict(healthy_but_deferred_by_adapter or {}),
        "suppressedStaticByReason": dict(suppressed_static_by_reason or {}),
        "suppressedStaticByStage": dict(suppressed_static_by_stage or {}),
        "queuedProviderCount": int(
            sum(
                value
                for key, value in dict(queued_by_adapter or {}).items()
                if str(key) != "static"
            )
        ),
        "queuedStaticCount": int(dict(queued_by_adapter or {}).get("static") or 0),
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
            "probeFailed": int(failed_probe_count_final + probe_miss_count_final),
            "queueFiltered": int(queue_filtered_count),
            "deferredByCap": deferred_by_cap,
            "queued": int(
                len([row for row in current_candidates if not bool(row.get("deferred"))])
            ),
        },
    }


def build_discovery_task_progress(
    *,
    summary: dict[str, Any],
    finished: bool,
) -> dict[str, Any]:
    phase_key = str(summary.get("phaseKey") or summary.get("phase") or "").strip() or (
        "completed" if finished else "starting"
    )
    phase_label = str(summary.get("phaseLabel") or "").strip() or (
        "Discovery completed" if finished else "Initializing scan"
    )
    found_count = int(summary.get("foundEndpointCount") or 0)
    probed_count = int(summary.get("probedCandidateCount") or summary.get("probedCount") or 0)
    queued_count = int(summary.get("queuedCandidateCount") or 0)
    deferred_count = int(summary.get("discoverableButDeferredCount") or 0)
    failed_count = int(summary.get("failedProbeCount") or 0)
    loss = summary.get("lossAccounting") if isinstance(summary.get("lossAccounting"), dict) else {}
    probe_total = max(
        0,
        int(loss.get("generated") or 0)
        - int(loss.get("dedupSkipped") or 0)
        - int(loss.get("validationSkipped") or 0)
        - int(loss.get("lowEvidenceSkipped") or 0)
        - int(summary.get("suppressedStaticCount") or 0),
    ) or max(probed_count, failed_count, queued_count)
    mode = "indeterminate"
    ratio = 0.0
    if finished:
        mode = "determinate"
        ratio = 1.0
        phase_key = "completed"
        phase_label = "Discovery completed"
    elif phase_key == "probing_candidates" and probe_total > 0:
        mode = "determinate"
        ratio = max(0.0, min(1.0, probed_count / max(1, probe_total)))
    return {
        "active": not bool(finished),
        "phaseKey": phase_key,
        "phaseLabel": phase_label,
        "mode": mode,
        "ratio": ratio,
        "counts": {
            "foundEndpoints": found_count,
            "probedCandidates": probed_count,
            "probeTotal": probe_total,
            "queuedCandidates": queued_count,
            "deferredCandidates": deferred_count,
            "failedProbes": failed_count,
        },
    }


def emit_log(message: str) -> None:
    line = f"[{now_iso()}] {str(message or '').strip()}"
    print(line, flush=True)


def _validate_evidence_types(values: list[str], *, context: str) -> list[str]:
    cleaned = unique_string_list(
        [str(item or "").strip() for item in (values or []) if str(item or "").strip()]
    )
    unknown = [item for item in cleaned if item not in EVIDENCE_TYPES_SET]
    if unknown:
        emit_log(f"Warning: dropping unknown evidenceTypes in {context}: {unknown}")
    return [item for item in cleaned if item in EVIDENCE_TYPES_SET]


def summarize_failures(failures: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    counter: Counter[str] = Counter()
    for row in failures:
        key = str(row.get("key") or row.get("adapter") or "unknown")
        counter[key] += 1
    return [{"key": key, "count": count} for key, count in counter.most_common(5)]


def stage_curated_seed_candidates() -> list[dict[str, Any]]:
    import src.source_discovery as sd
    from src.source_registry import unique_sources

    rows: list[dict[str, Any]] = []
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
    streams: Iterable[tuple[str, list[dict[str, Any]]]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for stage, items in streams:
        for raw in items:
            if not isinstance(raw, dict):
                continue
            row = dict(raw)
            row["discoveryStage"] = str(row.get("discoveryStage") or stage)
            row["discoveryMethod"] = str(
                row.get("discoveryMethod") or ("seed" if stage == "curated_seed" else "pattern")
            )
            row["discoveredAt"] = str(row.get("discoveredAt") or now_iso())
            row["evidenceTypes"] = _validate_evidence_types(
                list(row.get("evidenceTypes") or []),
                context="merge_candidate_streams",
            )
            row["evidenceScore"] = int(row.get("evidenceScore") or 0)
            rows.append(row)
    return rows
