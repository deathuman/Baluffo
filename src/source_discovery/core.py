from __future__ import annotations

"""Compatibility surface for discovery scoring, queueing, and normalization."""

from .core_identity import adapter_domain_fingerprint, queue_family_key, root_domain
from .core_queue import (
    _queue_balancing_order,
    _sort_candidate_key,
    apply_queue_balancing,
    apply_sheet_directory_static_probe_cap,
    is_google_sheet_candidate,
    provider_queue_target,
    sheet_directory_static_probe_cap,
)
from .core_scoring import (
    STRUCTURED_BATCH_ADAPTERS,
    _parse_iso_datetime,
    classify_probe_failure_stage,
    compute_candidate_rank,
    compute_candidate_score,
    compute_confidence,
    init_stage_counter,
    normalize_candidate,
    probe_bucket_for,
    probe_concurrency_defaults,
)
from .core_thresholds import (
    STATIC_STRONG_EVIDENCE_TYPES,
    _evidence_threshold_for_probe,
    _evidence_threshold_for_queue,
    classify_static_suppression,
    estimate_probe_priority,
    should_queue_candidate,
)

__all__ = [
    "STATIC_STRONG_EVIDENCE_TYPES",
    "STRUCTURED_BATCH_ADAPTERS",
    "_evidence_threshold_for_probe",
    "_evidence_threshold_for_queue",
    "_parse_iso_datetime",
    "_queue_balancing_order",
    "_sort_candidate_key",
    "adapter_domain_fingerprint",
    "apply_queue_balancing",
    "apply_sheet_directory_static_probe_cap",
    "classify_probe_failure_stage",
    "classify_static_suppression",
    "compute_candidate_rank",
    "compute_candidate_score",
    "compute_confidence",
    "estimate_probe_priority",
    "init_stage_counter",
    "is_google_sheet_candidate",
    "normalize_candidate",
    "probe_bucket_for",
    "probe_concurrency_defaults",
    "provider_queue_target",
    "queue_family_key",
    "root_domain",
    "sheet_directory_static_probe_cap",
    "should_queue_candidate",
]
