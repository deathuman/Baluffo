/**
 * Admin registry-conflicts renderer — thin coordinator.
 *
 * Owns exactly one public entrypoint, ``renderAdminRegistryConflicts``, which
 * composes the sibling leaves:
 * - registry-conflicts-constants.js  selector tokens and fallback tables
 * - registry-conflicts-model.js      payload normalization, sorting, classification
 * - registry-conflicts-view.js       toolbar, cards, groups, load-more footer
 * - registry-conflicts-actions.js    adjudication / safe-automation toolbars
 * - registry-conflicts-url-state.js  hash filter seeding and syncing
 * - registry-conflicts-wiring.js     DOM event wiring
 *
 * No leaf module imports from here.
 *
 * @module registry-conflicts
 */

import { stringValue } from "../../shared/format-utils.js";
import { escapeHtml } from "../../shared/ui/index.js";
import { stableOpsSignature } from "./ops-shared.js";
import {
  renderRegistryConflictActionStrip,
  renderSuppressedIndependentProviderBoards
} from "./registry-conflicts-actions.js";
import {
  adjudicationValue,
  conflictMatchesSearch,
  getConflictCards,
  getReviewPayload,
  getTriagePayload,
  objectValue,
  sortedConflictCards
} from "./registry-conflicts-model.js";
import {
  renderConflictFilterToolbar,
  renderConflictGroups,
  renderLoadMoreFooter
} from "./registry-conflicts-view.js";
import { seedConflictFiltersFromHash } from "./registry-conflicts-url-state.js";
import { wireRegistryConflictInteractions } from "./registry-conflicts-wiring.js";

export function renderAdminRegistryConflicts(reviewEl, payload, options = {}) {
    if (!reviewEl) return;
    const conflicts = sortedConflictCards(getConflictCards(payload));
    const summary = objectValue(payload?.summary);
    const triage = getTriagePayload(payload, conflicts);
    const review = getReviewPayload(payload, conflicts);
    const suppressedIndependentProviderBoards = objectValue(payload?.suppressedIndependentProviderBoards);
    const canPatchInPlace = Boolean(reviewEl && reviewEl.dataset);
    seedConflictFiltersFromHash(reviewEl);
    const activeTriageFilter = stringValue(reviewEl?.dataset?.registryConflictTriageFilter, "all");
    const activeReviewFilter = stringValue(reviewEl?.dataset?.registryConflictReviewFilter, "all");
    const activeSearchQuery = stringValue(reviewEl?.dataset?.registryConflictSearchQuery);
    const triageFilteredConflicts = activeTriageFilter === "all"
      ? conflicts
      : conflicts.filter(card => stringValue(card?.triageBucket, "ambiguous_manual_review") === activeTriageFilter);
    const reviewFilteredConflicts = activeReviewFilter === "all"
      ? triageFilteredConflicts
      : triageFilteredConflicts.filter(card => stringValue(card?.reviewQueue, "p3_low_signal_manual") === activeReviewFilter);
    const visibleConflicts = reviewFilteredConflicts.filter(card => conflictMatchesSearch(card, activeSearchQuery));
    const adjudication = adjudicationValue(payload);
    const checkingConflicts = Boolean(options?.checkingConflicts)
      || stringValue(adjudication?.status) === "running";
    const signature = stableOpsSignature({
      summary,
      triage,
      review,
      adjudication,
      activeTriageFilter,
      activeReviewFilter,
      activeSearchQuery,
      checkingConflicts,
      conflicts,
      suppressedIndependentProviderBoards
    });
    if (canPatchInPlace && reviewEl.dataset.registryConflictsSig === signature) return;
    if (canPatchInPlace) reviewEl.dataset.registryConflictsSig = signature;

    const conflictCount = Number(summary?.conflictCount || conflicts.length || 0);
    reviewEl.innerHTML = `
      ${renderConflictFilterToolbar(triage, review, activeTriageFilter, activeReviewFilter, activeSearchQuery)}
      ${renderRegistryConflictActionStrip(payload, visibleConflicts, checkingConflicts)}
      ${renderSuppressedIndependentProviderBoards(payload)}
      <div class="admin-registry-conflicts-list">
        ${visibleConflicts.length
          ? renderConflictGroups(visibleConflicts, review, { disableSafeAutomation: checkingConflicts })
          : `<div class="muted">${escapeHtml(
              conflictCount
                ? "No registry conflict cards match the selected triage, review queue, or search."
                : "No duplicate-family registry conflicts are currently queued."
            )}</div>`}
      </div>
      ${renderLoadMoreFooter(visibleConflicts.length, conflicts.length, conflictCount)}
    `;

  if (typeof reviewEl.querySelectorAll !== "function") return;
  wireRegistryConflictInteractions(reviewEl, options, {
    visibleConflicts,
    canPatchInPlace,
    rerender: () => renderAdminRegistryConflicts(reviewEl, payload, options)
  });
}
