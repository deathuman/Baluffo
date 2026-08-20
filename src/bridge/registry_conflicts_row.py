"""Registry conflict row construction helpers — thin coordinator.

AI boundary owns: admin-facing registry conflict row derivation, diff fields, and action labels;
the SAFE_AUTO_* constants and the full re-export surface stay here so the automation-family modules
and the registry_conflicts coordinator import from this module unchanged.
AI boundary implement in: this coordinator re-exports the six implementation leaves
(registry_conflicts_row_{core,identity,path,source_state,adjudication,audit}.py).
AI boundary search before contracts: registry conflict routes, registry_conflicts coordinator, and frontend registry conflict callers.
AI boundary verify: `npm run lint:repo-guardrails` plus focused registry conflict row tests.
"""

from __future__ import annotations

from src.bridge.registry_conflicts_row_adjudication import (
    _active_same_adapter_provider_rows,
    _adjudicated_independent_provider_loser_ids,
    _adjudication_families_by_key,
    _adjudication_proves_independent_provider_boards,
    _current_jobs_prove_independent_provider_boards,
    _independent_provider_board_audit_row,
    _with_live_adjudication_card,
)
from src.bridge.registry_conflicts_row_audit import (
    _build_independent_provider_board_audit,
    _build_pending_conflict_audit,
    _compare_registry_rows,
    _join_source_health_aliases,
    _row_has_fresh_count_evidence,
    _safe_auto_demoted_pending_audit_row,
    _source_identity_counts,
    _unique_registry_rows,
)
from src.bridge.registry_conflicts_row_core import (
    PROVIDER_ADAPTERS,
    _as_dict,
    _as_list,
    _clean_text,
    _effective_provider_adapter,
    _has_fresh_or_healthy_signal,
    _int_value,
    _is_provider_like_row,
    _is_provider_row,
    _is_static_row,
    _jobs_found_count,
    _positive_evidence_score,
    _row_adapter,
    _row_has_weak_job_signal,
    _row_identity,
    _row_jobs_evidence,
    _row_state,
    _static_row_current_jobs,
)
from src.bridge.registry_conflicts_row_identity import (
    _normalized_static_url_aliases,
    _normalized_url_for_comparison,
    _provider_endpoint_shape,
    _row_live_final_url,
    _row_primary_url,
    _source_job_identity_index,
    _static_url_has_job_fragment,
)
from src.bridge.registry_conflicts_row_path import (
    _has_homepage_to_career_site_path,
    _has_parent_child_listing_path,
    _host_matches_family,
    _is_careerish_path,
    _is_homepage_path,
    _single_static_host_path,
    _static_url_host_paths,
)
from src.bridge.registry_conflicts_row_source_state import (
    _ambiguous_registry_row_names,
    _merge_fetch_report_source_details,
    _source_state_rows_by_name,
)
from src.source_registry import source_identity

SAFE_AUTO_DEMOTE_ACTION = "auto_demote_same_adapter_provider_alias"

SAFE_AUTO_DEMOTE_LABEL = "Auto-demote safe duplicate"

SAFE_AUTO_DEMOTE_STATIC_URL_ALIAS_ACTION = "auto_demote_static_normalized_url_alias"

SAFE_AUTO_DEMOTE_STATIC_URL_ALIAS_LABEL = "Auto-demote static URL alias"

SAFE_AUTO_DEMOTE_STATIC_LISTING_VARIANT_ACTION = "auto_demote_static_same_host_listing_variant"

SAFE_AUTO_DEMOTE_STATIC_LISTING_VARIANT_LABEL = "Auto-demote static listing variant"

SAFE_AUTO_DEMOTE_STATIC_GENERATED_VARIANTS_ACTION = "auto_demote_static_generated_listing_variants"

SAFE_AUTO_DEMOTE_STATIC_GENERATED_VARIANTS_LABEL = "Auto-demote generated static listing variants"

SAFE_AUTO_DEMOTE_PROVIDER_STATIC_ACTION = "auto_demote_provider_static_weaker_source"

SAFE_AUTO_DEMOTE_PROVIDER_STATIC_LABEL = "Auto-demote weaker static source"

SAFE_AUTO_DEMOTE_PROVIDER_REDIRECT_ALIAS_ACTION = "auto_demote_provider_redirect_static_aliases"

SAFE_AUTO_DEMOTE_PROVIDER_REDIRECT_ALIAS_LABEL = "Auto-demote redirect/static aliases"

SAFE_AUTO_PROMOTE_PENDING_STATIC_FRAGMENT_ACTION = "auto_promote_pending_static_jobs_fragment"

SAFE_AUTO_PROMOTE_PENDING_STATIC_FRAGMENT_LABEL = "Auto-promote static jobs-section alias"

SAFE_AUTO_REJECT_PENDING_STATIC_BARE_ALIAS_ACTION = "auto_reject_pending_static_bare_alias"

SAFE_AUTO_REJECT_PENDING_STATIC_BARE_ALIAS_LABEL = "Auto-reject pending bare static alias"

SAFE_AUTO_PROMOTE_PENDING_PROVIDER_ACTION = "auto_promote_pending_provider_higher_jobs"

SAFE_AUTO_PROMOTE_PENDING_PROVIDER_LABEL = "Auto-promote higher-yield provider"

SAFE_AUTO_DEMOTE_ACTIONS = {
    SAFE_AUTO_DEMOTE_ACTION,
    SAFE_AUTO_DEMOTE_STATIC_URL_ALIAS_ACTION,
    SAFE_AUTO_DEMOTE_STATIC_LISTING_VARIANT_ACTION,
    SAFE_AUTO_DEMOTE_STATIC_GENERATED_VARIANTS_ACTION,
    SAFE_AUTO_DEMOTE_PROVIDER_STATIC_ACTION,
    SAFE_AUTO_DEMOTE_PROVIDER_REDIRECT_ALIAS_ACTION,
    SAFE_AUTO_PROMOTE_PENDING_STATIC_FRAGMENT_ACTION,
    SAFE_AUTO_REJECT_PENDING_STATIC_BARE_ALIAS_ACTION,
    SAFE_AUTO_PROMOTE_PENDING_PROVIDER_ACTION,
}

SAFE_AUTO_DEMOTE_ROUTE = "/registry/conflicts/auto-demote-safe"

SAFE_AUTO_DEMOTE_REASON = "registry_conflict_safe_auto_demote"

ADJUDICATION_AUTO_DEMOTE_REASON = "registry_conflict_adjudication_auto_demote"

RESOLVED_PENDING_DEMOTION_REASONS = frozenset(
    {
        SAFE_AUTO_DEMOTE_REASON,
        ADJUDICATION_AUTO_DEMOTE_REASON,
    }
)
