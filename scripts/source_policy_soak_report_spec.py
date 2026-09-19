#!/usr/bin/env python3
"""Schema constants and import surface for the soak report.

The pre-split module defined its repo root and its constant block before any
function, and the fidelity checker treats a unit's body as running up to the
next unit. The import preamble therefore travels with those constants in this
leaf instead of sitting in the coordinator header.
"""

from __future__ import annotations

import sys
from pathlib import Path

_repo_root = Path(__file__).resolve().parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))
del _repo_root


__all__ = [
    "ARTIFACT_PATHS",
    "CONSERVATIVE_CLEANUP_EXAMPLE_LIMIT",
    "CONSERVATIVE_CLEANUP_MIN_SAFE_RUNS",
    "CONSERVATIVE_CLEANUP_PROPOSAL_STALE_AFTER_SECONDS",
    "JSON_REPORT_NAME",
    "LEAN_REGISTRY_ARTIFACT_NAMES",
    "MARKDOWN_REPORT_NAME",
    "PROVIDER_ADAPTER_SOURCE_LOADERS",
    "PROVIDER_COVERAGE_GAP_BUCKETS",
    "PROVIDER_COVERAGE_GAP_EXAMPLE_LIMIT",
    "PROVIDER_COVERAGE_LINK_BACKFILL_EXAMPLE_LIMIT",
    "PROVIDER_COVERAGE_NEXT_ACTION_PRIORITY",
    "PROVIDER_COVERAGE_REVIEW_BLOCKING_DISAMBIGUATION_REASONS",
    "PROVIDER_ID_FIELDS",
    "PROVIDER_MIGRATION_ACTIONS",
    "PROVIDER_STAGING_DIAGNOSTIC_COUNT_KEYS",
    "PROVIDER_VALIDATION_DIAGNOSTIC_CAUSES",
    "REDUNDANT_STATIC_IF_PROVIDER",
    "REGISTRY_SEED_PATHS",
    "ROOT",
    "SCHEMA_VERSION",
    "SOURCE_SYNC_ALLOWED_KEYS",
    "SOURCE_SYNC_FORBIDDEN_TOKENS",
    "STATIC_LIKE_ADAPTERS",
    "STATIC_LIKE_STAGES",
    "STATIC_SCOPE_APPLY_AUDIT_NAME",
    "SUPPORTED_PROVIDERS",
    "_int_value",
    "as_json_list",
    "as_json_object",
    "build_provider_migration_payload",
    "clean_text",
    "common_registry_entries",
    "enrich_provider_migration_rows",
    "json_object_rows",
    "load_registry_json_array",
    "norm_text",
    "normalize_provider_coverage_payload",
    "normalize_provider_static_overlap_payload",
    "normalize_redundant_static_proposals_payload",
    "normalize_source_policy_recommendations_artifact",
    "normalize_source_policy_review_state_artifact",
    "normalize_static_suppression_policy_payload",
    "now_iso",
    "read_json",
    "runtime_static_source_name_for_registry_row",
    "source_identity",
]


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.jobs.adapters.static_sources import (
    static_source_name_for_registry_row as runtime_static_source_name_for_registry_row,
)
from src.jobs.common.contracts_provider_coverage import normalize_provider_coverage_payload
from src.jobs.common.contracts_provider_static_overlap import (
    normalize_provider_static_overlap_payload,
)
from src.jobs.common.contracts_redundant_static_proposals import (
    normalize_redundant_static_proposals_payload,
)
from src.jobs.common.contracts_source_policy_recommendations import (
    normalize_source_policy_recommendations_artifact,
)
from src.jobs.common.contracts_source_policy_review_state import (
    normalize_source_policy_review_state_artifact,
)
from src.jobs.common.contracts_static_suppression_policy import (
    normalize_static_suppression_policy_payload,
)
from src.jobs.common.registry import registry_entries as common_registry_entries
from src.jobs.common.registry_defaults import REDUNDANT_STATIC_IF_PROVIDER
from src.jobs.text_utils import clean_text, norm_text
from src.shared.json_io import read_json
from src.shared.json_shapes import as_json_list, as_json_object, json_object_rows
from src.shared.utils import int_or_default as _int_value
from src.shared.utils import now_iso
from src.source_discovery.config import SUPPORTED_PROVIDERS
from src.source_discovery.provider_migration_advisory import (
    build_provider_migration_payload,
    enrich_provider_migration_rows,
)
from src.source_registry_identity import source_identity
from src.source_registry_io import load_json_array as load_registry_json_array

SCHEMA_VERSION = "1.0"
JSON_REPORT_NAME = "source-policy-soak-report.json"
MARKDOWN_REPORT_NAME = "source-policy-soak-report.md"
STATIC_SCOPE_APPLY_AUDIT_NAME = "static-scope-apply-audit.json"

ARTIFACT_PATHS = {
    "sourceDiscoveryReport": "source-discovery-report.json",
    "sourceDiscoveryCandidates": "source-discovery-candidates.json",
    "jobsFetchReport": "jobs-fetch-report.json",
    "jobsSourceState": "jobs-source-state.json",
    "sourcePolicyRecommendations": "source-policy-recommendations.json",
    "sourcePolicyReviewState": "source-policy-review-state.json",
    "sourceRegistryActive": "source-registry-active.json",
    "sourceRegistryPending": "source-registry-pending.json",
    "sourceRegistryRejected": "source-registry-rejected.json",
    "sourceRegistryTombstones": "source-registry-tombstones.json",
    "sourceSync": "source-sync.json",
    "jobsUnified": "jobs-unified.json",
}
REGISTRY_SEED_PATHS = {
    "source-registry-active.json": "defaults/source-registry-active.seed.json",
    "source-registry-pending.json": "defaults/source-registry-pending.seed.json",
}
LEAN_REGISTRY_ARTIFACT_NAMES = {
    "source-registry-active.json",
    "source-registry-pending.json",
}

SOURCE_SYNC_ALLOWED_KEYS = {"schemaVersion", "generatedAt", "source", "active", "pending"}
SOURCE_SYNC_FORBIDDEN_TOKENS = {
    "sourcePolicy",
    "sourcePolicyReviewState",
    "sourcePolicyRecommendations",
    "reviewState",
    "manualSuppressionOverride",
    "force_pause",
    "recommendations",
    "redundantStaticProposals",
}
PROVIDER_MIGRATION_ACTIONS = {
    "add_provider_source",
    "review_provider_migration",
    "already_covered_by_provider",
    "unsupported_provider",
    "needs_probe",
    "keep_static",
    "insufficient_evidence",
}
STATIC_LIKE_ADAPTERS = {"static", "scrapy_static"}
STATIC_LIKE_STAGES = {"generic_static", "seed_careers_page", "sheet_directory"}
CONSERVATIVE_CLEANUP_MIN_SAFE_RUNS = 3
CONSERVATIVE_CLEANUP_EXAMPLE_LIMIT = 5
CONSERVATIVE_CLEANUP_PROPOSAL_STALE_AFTER_SECONDS = 24 * 60 * 60
PROVIDER_COVERAGE_LINK_BACKFILL_EXAMPLE_LIMIT = 5
PROVIDER_COVERAGE_GAP_EXAMPLE_LIMIT = 5
PROVIDER_COVERAGE_GAP_BUCKETS = (
    "unsupportedProviderDetected",
    "providerDetectedNeedsProbe",
    "stagedProviderNotFetched",
    "fetchedButNotValidated",
    "validatedProviderMissingMigrationSourceIdentity",
    "staticStillActiveDespiteValidatedProvider",
)
PROVIDER_VALIDATION_DIAGNOSTIC_CAUSES = (
    "zeroKeptFetched",
    "fetchError",
    "notFetched",
    "missingDetailEvidence",
    "validated",
)
PROVIDER_ID_FIELDS = (
    "slug",
    "account",
    "company_id",
    "subdomain",
    "api_url",
    "feed_url",
    "board_url",
    "site_path",
    "listing_url",
    "base_url",
)
PROVIDER_STAGING_DIAGNOSTIC_COUNT_KEYS = (
    "stageableProviderCandidateCount",
    "stagedProviderCandidateCount",
    "stagingSkippedCount",
    "stagingBlockedByDuplicateActiveCount",
    "stagingBlockedByDuplicatePendingCount",
    "stagingBlockedByUnsupportedProviderCount",
    "stagingBlockedByInsufficientEvidenceCount",
    "stagingBlockedByNeedsProbeCount",
    "stagingBlockedByProviderRowBuildFailureCount",
    "stagingBlockedByIdentityCollisionCount",
    "stagingBlockedByAdapterMismatchCount",
)
PROVIDER_ADAPTER_SOURCE_LOADERS = {
    "ashby": "ashby_sources",
    "bamboohr": "bamboohr_sources",
    "breezy": "breezy_sources",
    "greenhouse": "greenhouse_boards",
    "jazzhr": "jazzhr_sources",
    "lever": "lever_sources",
    "oracle_hcm": "oracle_hcm_sources",
    "personio": "personio_sources",
    "pinpoint": "pinpoint_sources",
    "recruitee": "recruitee_sources",
    "smartrecruiters": "smartrecruiters_sources",
    "teamtailor": "teamtailor_sources",
    "workable": "workable_sources",
    "workday": "workday_sources",
}
PROVIDER_COVERAGE_NEXT_ACTION_PRIORITY = {
    "refresh_discovery_staging_evidence": 1,
    "fetch_staged_provider_candidates": 2,
    "debug_provider_validation": 3,
    "review_one_migration_link": 4,
    "plan_unsupported_provider_family": 5,
    "resolve_link_ambiguity": 6,
    "none": 0,
}
PROVIDER_COVERAGE_REVIEW_BLOCKING_DISAMBIGUATION_REASONS = frozenset(
    {
        "insufficient_provider_success_history",
        "source_state_not_ok",
    }
)
