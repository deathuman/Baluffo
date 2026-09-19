/**
 * Registry conflict renderer — interaction wiring.
 *
 * Split out of ``registry-conflicts.js``; that module stays the thin
 * re-export surface for ``renderAdminRegistryConflicts``.
 *
 * @module registry-conflicts-wiring
 */

import { stringValue } from "../../shared/format-utils.js";
import { ui } from "../../shared/ui/selectors.js";
import {
  ACTION_TOKEN,
  CHECK_TOKEN,
  LOAD_MORE_BUTTON_SELECTOR,
  REVIEW_FILTER_SELECTOR,
  REVIEW_FILTER_SELECT_SELECTOR,
  SAFE_AUTOMATION_SELECTOR,
  SEARCH_INPUT_SELECTOR,
  TRIAGE_FILTER_SELECTOR,
  TRIAGE_FILTER_SELECT_SELECTOR
} from "./registry-conflicts-constants.js";
import { safeAutomationValue } from "./registry-conflicts-model.js";
import { syncConflictFiltersToHash } from "./registry-conflicts-url-state.js";

/**
 * Wire the filter, paging, safe-automation, check, and per-row action handlers.
 *
 * Filter mutations clear the render signature and re-enter the coordinator via
 * the injected ``rerender`` callback, so this module never imports the
 * coordinator itself.
 *
 * @param {HTMLElement} reviewEl
 * @param {Object} options
 * @param {{ visibleConflicts: Array, canPatchInPlace: boolean, rerender: () => void }} context
 */
function wireRegistryConflictInteractions(reviewEl, options, { visibleConflicts, canPatchInPlace, rerender }) {
  const applyTriageFilter = bucket => {
      if (canPatchInPlace) {
        reviewEl.dataset.registryConflictTriageFilter = bucket;
        reviewEl.dataset.registryConflictsSig = "";
      }
      syncConflictFiltersToHash(reviewEl);
      rerender();
    };
    const applyReviewFilter = queue => {
      if (canPatchInPlace) {
        reviewEl.dataset.registryConflictReviewFilter = queue;
        reviewEl.dataset.registryConflictsSig = "";
      }
      syncConflictFiltersToHash(reviewEl);
      rerender();
    };
    const applySearch = query => {
      if (canPatchInPlace) {
        reviewEl.dataset.registryConflictSearchQuery = stringValue(query);
        reviewEl.dataset.registryConflictsSig = "";
      }
      syncConflictFiltersToHash(reviewEl);
      rerender();
    };
    reviewEl.querySelectorAll(SEARCH_INPUT_SELECTOR).forEach(input => {
      input.addEventListener("input", () => {
        const query = stringValue(input.value).trim();
        if (stringValue(reviewEl?.dataset?.registryConflictSearchQuery) === query) return;
        applySearch(query);
        const fresh = typeof reviewEl.querySelector === "function"
          ? reviewEl.querySelector(SEARCH_INPUT_SELECTOR)
          : null;
        if (fresh) {
          fresh.focus();
          const end = fresh.value.length;
          fresh.setSelectionRange?.(end, end);
        }
      });
    });
    reviewEl.querySelectorAll(LOAD_MORE_BUTTON_SELECTOR).forEach(button => {
      button.addEventListener("click", () => {
        if (typeof options.onRegistryConflictsLoadMore === "function") {
          options.onRegistryConflictsLoadMore();
        }
      });
    });
    reviewEl.querySelectorAll(TRIAGE_FILTER_SELECT_SELECTOR).forEach(select => {
      select.addEventListener("change", () => {
        applyTriageFilter(stringValue(select.value, "all"));
      });
    });
    reviewEl.querySelectorAll(REVIEW_FILTER_SELECT_SELECTOR).forEach(select => {
      select.addEventListener("change", () => {
        applyReviewFilter(stringValue(select.value, "all"));
      });
    });
    reviewEl.querySelectorAll(TRIAGE_FILTER_SELECTOR).forEach(button => {
      button.addEventListener("click", () => {
        const bucket = stringValue(button.dataset?.registryConflictFilterBucket, "all");
        applyTriageFilter(bucket);
      });
    });
    reviewEl.querySelectorAll(REVIEW_FILTER_SELECTOR).forEach(button => {
      button.addEventListener("click", () => {
        const queue = stringValue(button.dataset?.registryConflictReviewFilterQueue, "all");
        applyReviewFilter(queue);
      });
    });
    reviewEl.querySelectorAll(SAFE_AUTOMATION_SELECTOR).forEach(button => {
      button.addEventListener("click", () => {
        const cardIndex = Number(button.dataset.registryConflictSafeAutomationCardIndex || -1);
        const ids = stringValue(button.dataset.registryConflictSafeAutomationIds)
          .split(",")
          .map(id => id.trim())
          .filter(Boolean);
        const card = cardIndex >= 0 ? visibleConflicts[cardIndex] : null;
        const safeAutomation = card
          ? safeAutomationValue(card)
          : {
              eligible: true,
              action: stringValue(button.dataset.registryConflictSafeAutomationAction, "auto_demote_same_adapter_provider_alias"),
              label: "Apply safe demotions",
              route: stringValue(button.dataset.registryConflictSafeAutomationRoute, "/registry/conflicts/auto-demote-safe"),
              targetIds: ids,
              blockedReasons: []
            };
        if (typeof options.onRegistryConflictSafeAutomation === "function") {
          options.onRegistryConflictSafeAutomation(
            {
              ...safeAutomation,
              targetIds: ids.length ? ids : safeAutomation.targetIds
            },
            card
          );
        }
      });
    });
    reviewEl.querySelectorAll(ui(CHECK_TOKEN)).forEach(button => {
      button.addEventListener("click", () => {
        const applyAutopilot = String(button.dataset.registryConflictApplyAutopilot || "false") === "true";
        if (typeof options.onRegistryConflictCheck === "function") {
          options.onRegistryConflictCheck({ applyAutopilot });
        }
      });
    });
    reviewEl.querySelectorAll(ui(ACTION_TOKEN)).forEach(button => {
      button.addEventListener("click", () => {
        const cardIndex = Number(button.dataset.registryConflictCardIndex || -1);
        const rowIndex = Number(button.dataset.registryConflictRowIndex || -1);
        const actionIndex = Number(button.dataset.registryConflictActionIndex || -1);
        const card = visibleConflicts[cardIndex];
        const row = card?.rows?.[rowIndex];
        const action = row?.actions?.[actionIndex];
        if (row && action && typeof options.onRegistryConflictAction === "function") {
          options.onRegistryConflictAction(row, action, card);
        }
      });
    });
}

export { wireRegistryConflictInteractions };
