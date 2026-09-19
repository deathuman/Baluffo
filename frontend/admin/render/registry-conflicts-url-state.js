/**
 * Registry conflict renderer — Hash-parameter seeding and syncing for the conflict filters.
 *
 * Split out of ``registry-conflicts.js``; that module stays the thin
 * re-export surface for ``renderAdminRegistryConflicts``.
 *
 * @module registry-conflicts-url-state
 */

import { stringValue } from "../../shared/format-utils.js";

function conflictHashParams() {
  try {
    return new URLSearchParams((globalThis.location?.hash || "").replace(/^#\??/, ""));
  } catch {
    return new URLSearchParams("");
  }
}

function seedConflictFiltersFromHash(reviewEl) {
  const dataset = reviewEl?.dataset;
  if (!dataset) return;
  let params;
  try {
    params = conflictHashParams();
  } catch {
    return;
  }
  if (dataset.registryConflictTriageFilter === undefined && params.get("conflict-triage") !== null) {
    dataset.registryConflictTriageFilter = params.get("conflict-triage") || "all";
  }
  if (dataset.registryConflictReviewFilter === undefined && params.get("conflict-queue") !== null) {
    dataset.registryConflictReviewFilter = params.get("conflict-queue") || "all";
  }
  if (dataset.registryConflictSearchQuery === undefined && params.get("conflict-q") !== null) {
    dataset.registryConflictSearchQuery = params.get("conflict-q") || "";
  }
}

function syncConflictFiltersToHash(reviewEl) {
  try {
    if (!globalThis.history?.replaceState || !globalThis.location) return;
    const params = conflictHashParams();
    const setOrDelete = (key, value) => {
      if (value && value !== "all") params.set(key, value);
      else params.delete(key);
    };
    setOrDelete("conflict-triage", stringValue(reviewEl?.dataset?.registryConflictTriageFilter));
    setOrDelete("conflict-queue", stringValue(reviewEl?.dataset?.registryConflictReviewFilter));
    const search = stringValue(reviewEl?.dataset?.registryConflictSearchQuery);
    if (search) params.set("conflict-q", search);
    else params.delete("conflict-q");
    const qs = params.toString();
    const base = `${globalThis.location.pathname || ""}${globalThis.location.search || ""}`;
    globalThis.history.replaceState(null, "", qs ? `#${qs}` : base);
  } catch {
    // Stub environments without location/history.
  }
}

export {
  seedConflictFiltersFromHash,
  syncConflictFiltersToHash
};
