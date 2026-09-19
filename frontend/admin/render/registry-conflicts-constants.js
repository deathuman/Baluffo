/**
 * Registry conflict renderer — DOM selector tokens and triage/review fallback tables.
 *
 * Split out of ``registry-conflicts.js``; that module stays the thin
 * re-export surface for ``renderAdminRegistryConflicts``.
 *
 * @module registry-conflicts-constants
 */

import { UI_TOKENS } from "../../shared/ui/selectors.js";

const ACTION_TOKEN = UI_TOKENS.admin.registryConflictActionBtn;
const CHECK_TOKEN = UI_TOKENS.admin.registryConflictCheckBtn;
const TRIAGE_FILTER_SELECTOR = ".admin-registry-conflict-filter-btn";
const REVIEW_FILTER_SELECTOR = ".admin-registry-conflict-review-filter-btn";
const TRIAGE_FILTER_SELECT_SELECTOR = ".admin-registry-conflict-filter-select";
const REVIEW_FILTER_SELECT_SELECTOR = ".admin-registry-conflict-review-filter-select";
const SEARCH_INPUT_SELECTOR = ".admin-registry-conflict-search-input";
const LOAD_MORE_BUTTON_SELECTOR = ".admin-registry-conflict-load-more-btn";
const SAFE_AUTOMATION_SELECTOR = ".admin-registry-conflict-safe-automation-btn";
const TRIAGE_BUCKET_FALLBACKS = [
  {
    bucket: "exact_duplicate_auto_healable",
    label: "Exact duplicate",
    risk: "low",
    description: "Rows share the same canonical source identity."
  },
  {
    bucket: "active_active_likely_duplicate",
    label: "Active-active",
    risk: "high",
    description: "More than one active row exists for this source family."
  },
  {
    bucket: "pending_duplicate_of_active",
    label: "Pending duplicate",
    risk: "medium",
    description: "A pending row matches a family with one active source."
  },
  {
    bucket: "rejected_historical_noise",
    label: "Rejected noise",
    risk: "low",
    description: "Rejected rows are retained as historical registry noise."
  },
  {
    bucket: "ambiguous_manual_review",
    label: "Manual review",
    risk: "medium",
    description: "The conflict needs operator review."
  }
];
const REVIEW_QUEUE_FALLBACKS = [
  {
    queue: "p0_multi_active_provider",
    priority: 0,
    label: "Multiple active providers",
    description: "Multiple active API/provider rows exist for one source family."
  },
  {
    queue: "p1_active_provider_static",
    priority: 1,
    label: "Active provider + static",
    description: "Active provider rows coexist with active static rows."
  },
  {
    queue: "p1_pending_provider_against_active",
    priority: 1,
    label: "Pending provider vs active",
    description: "A pending API/provider candidate is competing with one active source."
  },
  {
    queue: "p2_same_adapter_active_variant",
    priority: 2,
    label: "Same-adapter active variant",
    description: "Multiple active rows use the same non-static source type."
  },
  {
    queue: "p2_static_url_variant_active",
    priority: 2,
    label: "Active static URL variants",
    description: "Multiple active static rows look like URL variants."
  },
  {
    queue: "p2_pending_static_variant",
    priority: 2,
    label: "Pending static variant",
    description: "Pending static rows compete with one active source."
  },
  {
    queue: "p3_pending_only_intake",
    priority: 3,
    label: "Pending-only intake",
    description: "Duplicate candidates are pending only."
  },
  {
    queue: "p3_low_signal_manual",
    priority: 3,
    label: "Low-signal manual review",
    description: "The conflict needs manual review."
  }
];

export {
  ACTION_TOKEN,
  CHECK_TOKEN,
  TRIAGE_FILTER_SELECTOR,
  REVIEW_FILTER_SELECTOR,
  TRIAGE_FILTER_SELECT_SELECTOR,
  REVIEW_FILTER_SELECT_SELECTOR,
  SEARCH_INPUT_SELECTOR,
  LOAD_MORE_BUTTON_SELECTOR,
  SAFE_AUTOMATION_SELECTOR,
  TRIAGE_BUCKET_FALLBACKS,
  REVIEW_QUEUE_FALLBACKS
};
