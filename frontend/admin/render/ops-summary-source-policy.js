/**
 * Source-health, provider-coverage, static-overlap, and conservative-cleanup
 * row formatters extracted from ``ops-summary.js``.
 *
 * No coordinator import.  Every function is a pure string renderer that
 * takes the relevant payload slice and returns HTML.
 *
 * @module ops-summary-source-policy
 */

import { escapeHtml } from "../../shared/ui/index.js";
import { formatDuration, sanitizeSlowSourceName } from "./ops-shared.js";

function formatSourceHealthRows(rows, emptyText, { includeDuration = false } = {}) {
  const sourceRows = Array.isArray(rows) ? rows : [];
  if (!sourceRows.length) return escapeHtml(emptyText);
  return sourceRows
    .slice(0, 5)
    .map(row => {
      const name = sanitizeSlowSourceName(row?.name);
      const status = String(row?.status || "unknown");
      const kept = Number(row?.keptCount || 0);
      const reason = String(
        row?.failureBucket
        || row?.classification
        || row?.zeroKeptClassification
        || row?.exclusionReason
        || row?.error
        || ""
      );
      const duration = includeDuration ? `, ${formatDuration(Number(row?.durationMs || 0))}` : "";
      const reasonSuffix = reason ? `, ${reason.replaceAll("_", " ")}` : "";
      return escapeHtml(`${name} (${status}, kept ${kept}${duration}${reasonSuffix})`);
    })
    .join("");
}

function formatProviderCoverageRows(rows, emptyText) {
  const providerRows = Array.isArray(rows) ? rows : [];
  if (!providerRows.length) return escapeHtml(emptyText);
  return providerRows
    .slice(0, 5)
    .map(row => {
      const name = sanitizeSlowSourceName(row?.name);
      const status = String(row?.providerCoverageStatus || "unknown").replaceAll("_", " ");
      const readiness = String(row?.providerReplacementReadiness || "none").replaceAll("_", " ");
      const kept = Number(row?.providerCoverageLatestKeptCount || 0);
      const successes = Number(row?.providerCoverageConsecutiveSuccesses || 0);
      return escapeHtml(`${name} (${status}, kept ${kept}, successes ${successes}, ${readiness})`);
    })
    .join("");
}

function formatDynamicRedundantStaticRows(rows, emptyText) {
  const sourceRows = Array.isArray(rows) ? rows : [];
  if (!sourceRows.length) return escapeHtml(emptyText);
  return sourceRows
    .slice(0, 5)
    .map(row => {
      const name = sanitizeSlowSourceName(row?.name);
      const provider = String(row?.coveredByProviderSourceId || "provider");
      const adapter = String(row?.coveredByProviderAdapter || "provider");
      const successes = Number(row?.providerCoverageConsecutiveSuccesses || 0);
      return escapeHtml(`${name} (covered by ${provider}, ${adapter}, successes ${successes})`);
    })
    .join(" | ");
}

function formatProviderStaticOverlapRows(rows, emptyText) {
  const pairRows = Array.isArray(rows) ? rows : [];
  if (!pairRows.length) return escapeHtml(emptyText);
  return pairRows
    .slice(0, 5)
    .map(row => {
      const staticName = sanitizeSlowSourceName(row?.staticSourceName || row?.staticSourceId);
      const provider = sanitizeSlowSourceName(row?.providerSourceName || row?.providerSourceId);
      const status = String(row?.auditStatus || "unknown").replaceAll("_", " ");
      const overlap = Number(row?.overlapCount || 0);
      const staticOnly = Number(row?.staticOnlyCount || 0);
      return escapeHtml(`${staticName} covered by ${provider} (${status}, overlap ${overlap}, static-only ${staticOnly})`);
    })
    .join(" | ");
}

function formatStaticSuppressionPolicyRows(rows, emptyText) {
  const pairRows = Array.isArray(rows) ? rows : [];
  if (!pairRows.length) return escapeHtml(emptyText);
  return pairRows
    .slice(0, 5)
    .map(row => {
      const staticName = sanitizeSlowSourceName(row?.staticSourceName || row?.staticSourceId);
      const provider = sanitizeSlowSourceName(row?.providerSourceName || row?.providerSourceId);
      const reason = String(row?.reason || row?.lastAuditStatus || "policy").replaceAll("_", " ");
      const status = String(row?.lastAuditStatus || "unknown").replaceAll("_", " ");
      return escapeHtml(`${staticName} -> ${provider} (${status}, ${reason})`);
    })
    .join(" | ");
}

function formatRedundantStaticProposalRows(rows, emptyText) {
  const proposalRows = Array.isArray(rows) ? rows : [];
  if (!proposalRows.length) return escapeHtml(emptyText);
  return proposalRows
    .slice(0, 5)
    .map(row => {
      const staticName = sanitizeSlowSourceName(row?.staticSourceName || row?.staticSourceId);
      const provider = sanitizeSlowSourceName(row?.providerSourceName || row?.providerSourceId);
      const proposal = String(row?.proposal || "proposal").replaceAll("_", " ");
      const action = String(row?.recommendedAction || "review_pair").replaceAll("_", " ");
      const status = String(row?.lastAuditStatus || "unknown").replaceAll("_", " ");
      return escapeHtml(`${staticName} -> ${provider} (${proposal}, ${action}, ${status})`);
    })
    .join(" | ");
}

function formatConservativeCleanupReasonCounts(reasonCounts) {
  const counts = reasonCounts && typeof reasonCounts === "object" ? reasonCounts : {};
  const entries = Object.entries(counts)
    .filter(([, value]) => Number(value || 0) > 0)
    .sort((a, b) => {
      const countDelta = Number(b[1] || 0) - Number(a[1] || 0);
      return countDelta || String(a[0] || "").localeCompare(String(b[0] || ""));
    });
  if (!entries.length) return "No blocked candidate reasons.";
  return entries
    .slice(0, 5)
    .map(([reason, count]) => `${String(reason || "unknown").replaceAll("_", " ")} ${Number(count || 0).toLocaleString()}`)
    .join(", ");
}

function formatConservativeCleanupFreshnessSummary(cleanup) {
  const freshnessStatus = String(cleanup?.proposalFreshnessStatus || "fresh").replaceAll("_", " ");
  const ageSeconds = Number(cleanup?.proposalFreshnessAgeSeconds);
  const age = Number.isFinite(ageSeconds) ? formatDuration(Math.max(0, ageSeconds) * 1000) : "unknown";
  const generatedAt = String(cleanup?.proposalGeneratedAt || "");
  const runId = String(cleanup?.proposalReportRunId || "");
  const staleAfterSeconds = Number(cleanup?.proposalStaleThresholdSeconds || 0);
  const staleAfter = staleAfterSeconds > 0 ? formatDuration(staleAfterSeconds * 1000) : "";
  return [
    `status ${freshnessStatus}`,
    `age ${age}`,
    generatedAt ? `generated ${generatedAt}` : "",
    runId ? `run ${runId}` : "",
    staleAfter ? `stale after ${staleAfter}` : ""
  ]
    .filter(Boolean)
    .join(", ");
}

function formatConservativeCleanupProposalRows(rows, emptyText) {
  const proposalRows = Array.isArray(rows) ? rows : [];
  if (!proposalRows.length) return escapeHtml(emptyText);
  return proposalRows
    .slice(0, 5)
    .map(row => {
      const staticName = sanitizeSlowSourceName(row?.staticSourceName || row?.staticSourceId);
      const provider = sanitizeSlowSourceName(row?.providerSourceName || row?.providerSourceId);
      const action = String(row?.recommendedAction || "move_static_to_hidden_pending").replaceAll("_", " ");
      const cleanRuns = Number(row?.cleanRunEvidenceCount || 0);
      const suppression = String(row?.suppressionEvidenceStatus || "unknown").replaceAll("_", " ");
      const readiness = String(row?.proposalReadiness || "actionable").replaceAll("_", " ");
      const freshness = String(row?.proposalFreshnessStatus || "fresh").replaceAll("_", " ");
      const readinessReason = String(row?.proposalReadinessReason || "");
      return escapeHtml(
        `${staticName} -> ${provider} (${action}, readiness ${readiness}${
          readinessReason ? `, ${readinessReason}` : ""
        }, freshness ${freshness}, clean runs ${cleanRuns}, suppression ${suppression})`
      );
    })
    .join(" | ");
}

function formatConservativeCleanupBlockedRows(rows, emptyText) {
  const blockedRows = Array.isArray(rows) ? rows : [];
  if (!blockedRows.length) return escapeHtml(emptyText);
  return blockedRows
    .slice(0, 5)
    .map(row => {
      const staticName = sanitizeSlowSourceName(row?.staticSourceName || row?.staticSourceId);
      const provider = sanitizeSlowSourceName(row?.providerSourceName || row?.providerSourceId);
      const blockers = Array.isArray(row?.blockers) && row.blockers.length
        ? row.blockers.slice(0, 3).map(item => String(item || "").replaceAll("_", " ")).join(", ")
        : "blocked";
      const readiness = String(row?.proposalReadiness || "blocked").replaceAll("_", " ");
      const freshness = String(row?.proposalFreshnessStatus || "fresh").replaceAll("_", " ");
      const readinessReason = String(row?.proposalReadinessReason || "");
      return escapeHtml(
        `${staticName} -> ${provider} (${readiness}${
          readinessReason ? `, ${readinessReason}` : ""
        }, freshness ${freshness}, ${blockers})`
      );
    })
    .join(" | ");
}

function formatDedupSourceClasses(sourceClasses) {
  const classes = sourceClasses && typeof sourceClasses === "object" ? sourceClasses : {};
  return [
    `provider ${Number(classes?.provider || 0).toLocaleString()}`,
    `static ${Number(classes?.static || 0).toLocaleString()}`,
    `social ${Number(classes?.social || 0).toLocaleString()}`,
    `other ${Number(classes?.other || 0).toLocaleString()}`
  ].join(", ");
}

export {
  formatSourceHealthRows,
  formatProviderCoverageRows,
  formatDynamicRedundantStaticRows,
  formatProviderStaticOverlapRows,
  formatStaticSuppressionPolicyRows,
  formatRedundantStaticProposalRows,
  formatConservativeCleanupReasonCounts,
  formatConservativeCleanupFreshnessSummary,
  formatConservativeCleanupProposalRows,
  formatConservativeCleanupBlockedRows,
  formatDedupSourceClasses
};
