/**
 * Admin Ops summary rendering — fetcher-metrics DOM event wiring.
 *
 * Split out of ``ops-summary.js``; that module stays the thin coordinator
 * owning the five public render entrypoints.
 *
 * @module ops-summary-fetcher-wiring
 */

import { visibleProviderStaticRows } from "./ops-summary-provider-static.js";

/**
 * Wire the dedup-review, diagnostics-copy, and refresh handlers on the
 * already-rendered fetcher-metrics panel.
 *
 * @param {HTMLElement} metricsEl
 * @param {Object} options
 * @param {Object} context
 * @param {Object} context.diagnosticsByKey
 * @param {Array} context.providerStaticDisagreementRows
 * @param {Array} context.providerStaticTitleCompanyCollisionRows
 */
function wireOpsFetcherMetricsInteractions(
  metricsEl,
  options,
  { diagnosticsByKey, providerStaticDisagreementRows, providerStaticTitleCompanyCollisionRows }
) {
  if (typeof options?.onDedupReviewAction === "function") {
      const rowGroups = {
        providerStatic: visibleProviderStaticRows(providerStaticDisagreementRows),
        providerStaticTitleCompany: visibleProviderStaticRows(providerStaticTitleCompanyCollisionRows)
      };
      metricsEl.querySelectorAll("[data-dedup-review-action]").forEach(button => {
        button.addEventListener("click", () => {
          const action = String(button.getAttribute("data-dedup-review-action") || "");
          const tableKey = String(button.getAttribute("data-dedup-review-table") || "");
          const rowIndex = Number(button.getAttribute("data-dedup-review-row") || -1);
          const row = Array.isArray(rowGroups?.[tableKey]) ? rowGroups[tableKey][rowIndex] : null;
          if (!row || !action) return;
          options.onDedupReviewAction(row, action);
        });
      });
    }
    if (typeof options?.onCopySectionDiagnostics === "function") {
      metricsEl.querySelectorAll("[data-ops-diagnostics-copy]").forEach(button => {
        button.addEventListener("click", () => {
          const key = String(button.getAttribute("data-ops-diagnostics-copy") || "");
          const section = diagnosticsByKey[key];
          if (!section) return;
          options.onCopySectionDiagnostics(section);
        });
      });
    }
    if (typeof options?.onLoadDebugDiagnostics === "function") {
      metricsEl.querySelectorAll('[data-action="load-debug-diagnostics"]').forEach(button => {
        button.addEventListener("click", () => {
          options.onLoadDebugDiagnostics();
        });
      });
    }
    if (typeof options?.onRefreshAuditArtifacts === "function") {
      metricsEl.querySelectorAll('[data-action="refresh-discovery-audit-artifacts"]').forEach(button => {
        button.addEventListener("click", () => {
          options.onRefreshAuditArtifacts();
        });
      });
    }
    if (typeof options?.onRefreshTaskFailureAttempts === "function") {
      metricsEl.querySelectorAll('[data-action="refresh-task-failure-attempts"]').forEach(button => {
        button.addEventListener("click", () => {
          options.onRefreshTaskFailureAttempts();
        });
      });
    }
    if (typeof options?.onRefreshPerformanceProfile === "function") {
      metricsEl.querySelectorAll('[data-action="refresh-performance-profile"]').forEach(button => {
        button.addEventListener("click", () => {
          options.onRefreshPerformanceProfile();
        });
      });
    }
}

export { wireOpsFetcherMetricsInteractions };
