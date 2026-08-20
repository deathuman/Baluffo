#!/usr/bin/env python3
"""Source registry utilities for discovery/approval workflows."""

from __future__ import annotations

from collections.abc import Callable
from importlib import reload as _reload
from pathlib import Path
from typing import Any

from src import source_registry_auto_approval as _auto
from src import source_registry_canonicalize as _canonicalize
from src import source_registry_identity as _identity
from src import source_registry_io as _io
from src import source_registry_policy as _policy
from src import source_registry_state as _state
from src.shared.utils import now_iso

_io = _reload(_io)

ACTIVE_PATH = _io.ACTIVE_PATH
ACTIVE_SEED_PATH = _io.ACTIVE_SEED_PATH
APPROVAL_STATE_PATH = _io.APPROVAL_STATE_PATH
DATA_DIR = _io.DATA_DIR
DEFAULTS_DIR = _io.DEFAULTS_DIR
DISCOVERY_CANDIDATES_PATH = _io.DISCOVERY_CANDIDATES_PATH
DISCOVERY_REPORT_PATH = _io.DISCOVERY_REPORT_PATH
M5_STRATEGIC_BACKLOG_PATH = _io.M5_STRATEGIC_BACKLOG_PATH
PENDING_PATH = _io.PENDING_PATH
PENDING_SEED_PATH = _io.PENDING_SEED_PATH
REJECTED_PATH = _io.REJECTED_PATH
TOMBSTONES_PATH = _io.TOMBSTONES_PATH
URL_PATCH_MANIFEST_PATH = _io.URL_PATCH_MANIFEST_PATH
load_json_array = _io.load_json_array
load_json_object = _io.load_json_object
load_runtime_evidence = _io.load_runtime_evidence
load_runtime_evidence_array = _io.load_runtime_evidence_array
registry_seed_path_for = _io.registry_seed_path_for
summarize_json_array_storage = _io.summarize_json_array_storage

AUTO_APPROVAL_CAP_DEFER_REASONS = _auto.AUTO_APPROVAL_CAP_DEFER_REASONS
AUTO_APPROVAL_EXISTING_MATCH_REASONS = _auto.AUTO_APPROVAL_EXISTING_MATCH_REASONS
AUTO_APPROVAL_SECONDARY_ADAPTERS = _auto.AUTO_APPROVAL_SECONDARY_ADAPTERS
AUTO_APPROVAL_STRONG_ADAPTERS = _auto.AUTO_APPROVAL_STRONG_ADAPTERS
_cap_deferred_candidate_is_auto_approvable = _auto._cap_deferred_candidate_is_auto_approvable
_discovery_jobs_count = _auto._discovery_jobs_count
_discovery_row_has_blocking_error = _auto._discovery_row_has_blocking_error
_discovery_row_has_blocking_state = _auto._discovery_row_has_blocking_state
_normalize_discovery_health_status = _auto._normalize_discovery_health_status
_pending_row_is_auto_approvable = _auto._pending_row_is_auto_approvable
_promotion_reason_for_candidate = _auto._promotion_reason_for_candidate
_rank_reason_tokens = _auto._rank_reason_tokens
_stamp_live_transition = _auto._stamp_live_transition

canonicalize_registry_row = _canonicalize.canonicalize_registry_row
sort_sources_by_identity = _canonicalize.sort_sources_by_identity

_clean_family_token = _identity._clean_family_token
ensure_source_id = _identity.ensure_source_id
normalize_source_url = _identity.normalize_source_url
provider_fields_from_row_identity = _identity.provider_fields_from_row_identity
provider_fields_from_source_id = _identity.provider_fields_from_source_id
source_endpoint_url = _identity.source_endpoint_url
source_family_key = _identity.source_family_key
source_identity = _identity.source_identity
source_url_fingerprint = _identity.source_url_fingerprint
static_listing_url_alias = _identity.static_listing_url_alias
static_listing_url_aliases = _identity.static_listing_url_aliases
unique_sources = _identity.unique_sources

_adapter_priority = _policy._adapter_priority
_demote_duplicate_variant = _policy._demote_duplicate_variant
_duplicate_winner_score = _policy._duplicate_winner_score
_metadata_score = _policy._metadata_score
_source_state_for_row = _policy._source_state_for_row
_state_rows_by_key = _policy._state_rows_by_key
demote_duplicate_active_variants = _policy.demote_duplicate_active_variants

REGISTRY_MIGRATION_V2 = _state.REGISTRY_MIGRATION_V2
REGISTRY_REASON_APPROVE = _state.REGISTRY_REASON_APPROVE
REGISTRY_REASON_DELETE = _state.REGISTRY_REASON_DELETE
REGISTRY_REASON_DISCOVERY_AUTO_APPROVE = _state.REGISTRY_REASON_DISCOVERY_AUTO_APPROVE
REGISTRY_REASON_DUPLICATE_FAMILY = _state.REGISTRY_REASON_DUPLICATE_FAMILY
REGISTRY_REASON_FETCH_EMPTY_DEMOTE = _state.REGISTRY_REASON_FETCH_EMPTY_DEMOTE
REGISTRY_REASON_FETCH_FAILURE_DEMOTE = _state.REGISTRY_REASON_FETCH_FAILURE_DEMOTE
REGISTRY_REASON_MANUAL_SOURCE = _state.REGISTRY_REASON_MANUAL_SOURCE
REGISTRY_REASON_MANUAL_SOURCE_VARIANT = _state.REGISTRY_REASON_MANUAL_SOURCE_VARIANT
REGISTRY_REASON_PENDING_DEFAULT = _state.REGISTRY_REASON_PENDING_DEFAULT
REGISTRY_REASON_REJECT = _state.REGISTRY_REASON_REJECT
REGISTRY_REASON_REPEATED_ZERO_JOBS = _state.REGISTRY_REASON_REPEATED_ZERO_JOBS
REGISTRY_REASON_RESTORE_REJECTED = _state.REGISTRY_REASON_RESTORE_REJECTED
REGISTRY_REASON_ROLLBACK = _state.REGISTRY_REASON_ROLLBACK
REGISTRY_STATE_ACTIVE = _state.REGISTRY_STATE_ACTIVE
REGISTRY_STATE_PENDING = _state.REGISTRY_STATE_PENDING
REGISTRY_STATE_REJECTED = _state.REGISTRY_STATE_REJECTED
REGISTRY_STATES = _state.REGISTRY_STATES
ZERO_JOB_HIDDEN_DEFER_THRESHOLD = _state.ZERO_JOB_HIDDEN_DEFER_THRESHOLD
_apply_registry_legacy_fields = _state._apply_registry_legacy_fields
_coerce_int = _state._coerce_int
_coerce_state = _state._coerce_state
_first_text = _state._first_text
_infer_pending_reason = _state._infer_pending_reason
_infer_registry_state = _state._infer_registry_state
_infer_state_changed_at = _state._infer_state_changed_at
_infer_state_changed_by = _state._infer_state_changed_by
_transition_state_metadata = _state._transition_state_metadata
hide_repeated_zero_job_pending = _state.hide_repeated_zero_job_pending
is_hidden_from_default = _state.is_hidden_from_default
transition_registry_to_active = _state.transition_registry_to_active
transition_registry_to_pending = _state.transition_registry_to_pending
transition_registry_to_rejected = _state.transition_registry_to_rejected


def _sync_io_paths() -> None:
    _io.DATA_DIR = DATA_DIR
    _io.DEFAULTS_DIR = DATA_DIR / "defaults"
    _io.ACTIVE_SEED_PATH = _io.DEFAULTS_DIR / "source-registry-active.seed.json"
    _io.PENDING_SEED_PATH = _io.DEFAULTS_DIR / "source-registry-pending.seed.json"


def ensure_data_dir() -> None:
    _sync_io_paths()
    _io.ensure_data_dir()


def cleanup_runtime_evidence_journals(data_dir: Path | None = None) -> dict[str, Any]:
    _sync_io_paths()
    return _io.cleanup_runtime_evidence_journals(DATA_DIR if data_dir is None else data_dir)


def save_json_atomic(path: Path, payload: Any) -> None:
    _sync_io_paths()
    _io.save_json_atomic(path, payload)


def save_registry_state_atomic(
    active_path: Path,
    pending_path: Path,
    rejected_path: Path,
    state: dict[str, list[dict[str, Any]]],
) -> None:
    _sync_io_paths()
    _io.save_registry_state_atomic(active_path, pending_path, rejected_path, state)


def apply_discovery_auto_approval(
    state: dict[str, list[dict[str, Any]]],
    report: dict[str, Any],
    *,
    auto_approve_enabled: bool,
    approval_state_path: Path | None = None,
    record_approval_state: bool = True,
    now_iso_fn: Callable[[], str] | None = now_iso,
) -> tuple[dict[str, list[dict[str, Any]]], int]:
    return _auto.apply_discovery_auto_approval(
        state,
        report,
        auto_approve_enabled=auto_approve_enabled,
        approval_state_path=Path(approval_state_path or APPROVAL_STATE_PATH),
        record_approval_state=record_approval_state,
        now_iso_fn=now_iso_fn,
    )


__all__ = [
    name
    for name in globals()
    if not name.startswith("__")
    and name not in {"_auto", "_canonicalize", "_identity", "_io", "_policy", "_reload", "_state"}
]
