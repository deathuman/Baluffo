import { setTooltip } from "../../../shared/ui/index.js";
import {
  filterSourcePolicyReviewPairs,
  getMigrationLinkLinkedActions,
  getMigrationLinkReviewActions
} from "../../render/source-policy-review.js";
import { getObjectValue } from "../../domain/ops-shape-utils.js";
import { ACTIVE_PIPELINE_KPI_DELAYED_LABEL } from "../../domain/ops-fetch-kpi-model.js";

export const OPS_TAB_KEYS = new Set(["overview", "discovery", "source-policy", "registry-conflicts", "dedup"]);

const OPS_TAB_BADGE_PENDING_TEXT = "...";
const OPS_TAB_BADGE_DELAYED_TEXT = "-";
const OPS_TAB_COUNTS_UNAVAILABLE_LABEL = "Count temporarily unavailable; retrying.";

function formatBadgeTitle(count, singular, plural = `${singular}s`) {
  const total = Math.max(0, Number(count) || 0);
  return total ? `${total.toLocaleString()} ${total === 1 ? singular : plural}` : `No ${plural}`;
}

function toAlertBadgeState(alerts = []) {
  const rows = Array.isArray(alerts) ? alerts : [];
  const criticalCount = rows.filter(alert => String(alert?.severity || "").toLowerCase() === "critical").length;
  const count = rows.length;
  return {
    count,
    tone: count > 0 ? (criticalCount > 0 ? "critical" : "warning") : "neutral",
    title: count > 0 ? formatBadgeTitle(count, "active alert", "active alerts") : "No active alerts"
  };
}

export function toDiscoveryBadgeState(report = {}) {
  const review = getObjectValue(report?.candidateReview);
  const summary = getObjectValue(report?.summary);
  const counts = getObjectValue(report?.counts);
  const count = Number(
    review?.totalCandidates
    || summary?.queuedCandidateCount
    || summary?.candidateCount
    || summary?.newCandidateCount
    || counts?.candidateCount
    || counts?.queuedCandidates
    || 0
  );
  return {
    count,
    tone: count > 0 ? "warning" : "neutral",
    title: count > 0 ? formatBadgeTitle(count, "discovery review item", "discovery review items") : "No discovery review items"
  };
}

function toSourcePolicyBadgeState(payload = {}) {
  const rows = Array.isArray(payload?.recommendations?.pairs) ? payload.recommendations.pairs : [];
  const needsActionCount = filterSourcePolicyReviewPairs(rows, "needs_action").length;
  const linkBackfill = getObjectValue(payload?.providerCoverageLinkBackfill);
  const reviewCandidates = Array.isArray(linkBackfill.reviewCandidates) ? linkBackfill.reviewCandidates : [];
  const linkedCandidates = Array.isArray(linkBackfill.linkedCandidates) ? linkBackfill.linkedCandidates : [];
  const blockedCandidates = Array.isArray(linkBackfill.blockedCandidates) ? linkBackfill.blockedCandidates : [];
  const actionableMigrationCount = reviewCandidates.filter(candidate => getMigrationLinkReviewActions(candidate).length > 0).length
    + linkedCandidates.filter(candidate => getMigrationLinkLinkedActions(candidate).length > 0).length;
  const count = needsActionCount + actionableMigrationCount + blockedCandidates.length;
  return {
    count,
    tone: blockedCandidates.length > 0 ? "critical" : count > 0 ? "warning" : "neutral",
    title: count > 0
      ? formatBadgeTitle(count, "source policy review item", "source policy review items")
      : "No source policy review items"
  };
}

function toDedupBadgeState(dedupEvidence = {}) {
  const gate = getObjectValue(dedupEvidence?.dedupAuditGate);
  const providerStaticRows = Array.isArray(dedupEvidence?.providerStaticDisagreementExamples) ? dedupEvidence.providerStaticDisagreementExamples : [];
  const titleCompanyRows = Array.isArray(dedupEvidence?.providerStaticTitleCompanyCollisionExamples) ? dedupEvidence.providerStaticTitleCompanyCollisionExamples : [];
  const reviewQueueRows = Array.isArray(dedupEvidence?.reviewQueue) ? dedupEvidence.reviewQueue : [];
  const reviewCount = providerStaticRows.length + titleCompanyRows.length + reviewQueueRows.length;
  const nonPrimaryMergeCounts = getObjectValue(gate?.currentRunNonPrimaryMergeCounts);
  const blockingCount = Math.max(0, Number(gate?.currentRunBlockingReviewQueueCount || 0))
    + Math.max(0, Number(gate?.carriedBlockingReviewQueueCount || 0))
    + Math.max(0, Number(gate?.providerStaticDisagreementBlockedCount || 0))
    + Math.max(0, Number(nonPrimaryMergeCounts?.blocking || 0));
  const monitorCount = Math.max(0, Number(gate?.currentRunMonitorReviewQueueCount || 0))
    + Math.max(0, Number(gate?.carriedMonitorReviewQueueCount || 0));
  const gateCount = Number(gate?.blockers?.length || 0) + Number(gate?.warnings?.length || 0);
  const count = Math.max(reviewCount, blockingCount, gateCount);
  return {
    count,
    tone: String(gate?.status || "").toLowerCase() === "blocked" || Number(gate?.blockers?.length || 0) > 0
      ? "critical"
      : count > 0 || monitorCount > 0
        ? "warning"
        : "neutral",
    title: count > 0
      ? formatBadgeTitle(count, "dedup blocker", "dedup blockers")
      : monitorCount > 0
        ? formatBadgeTitle(monitorCount, "dedup diagnostic", "dedup diagnostics")
        : "No dedup blockers"
  };
}

function pendingBadgeState(title = "Loading count", options = {}) {
  return {
    count: 0,
    tone: String(options?.tone || "pending"),
    title,
    loaded: false,
    pendingText: String(options?.pendingText || OPS_TAB_BADGE_PENDING_TEXT)
  };
}

function delayedBadgeState(title = ACTIVE_PIPELINE_KPI_DELAYED_LABEL) {
  return pendingBadgeState(title, {
    pendingText: OPS_TAB_BADGE_DELAYED_TEXT
  });
}

function normalizeBadgeState(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  if (value.loaded === false) {
    return pendingBadgeState(String(value.title || "Loading count"), {
      tone: value.tone,
      pendingText: value.pendingText
    });
  }
  return {
    count: Math.max(0, Number(value.count || 0)),
    tone: String(value.tone || "neutral"),
    title: String(value.title || "No items"),
    loaded: true
  };
}

function getSummaryBadgeState(tabCountsPayload, key) {
  const badges = getObjectValue(tabCountsPayload?.badges);
  return normalizeBadgeState(badges[key]);
}

function isLoadedOverviewHealth(health = {}) {
  if (!health || typeof health !== "object") return false;
  if (health.alertsEvaluated === true) return true;
  if (health.summaryView === true) return false;
  return Array.isArray(health.alerts) || Boolean(health.status);
}

export function isLoadedDiscoveryReport(report = {}) {
  if (!report || typeof report !== "object") return false;
  const count = toDiscoveryBadgeState(report).count;
  return count > 0
    || Boolean(report.candidateReview)
    || Array.isArray(report.candidates)
    || Array.isArray(report.failures)
    || (
      report.summaryView === true
      && Boolean(report.runId || report.startedAt || report.finishedAt || report.status)
    );
}

function isLoadedSourcePolicyPayload(payload = {}) {
  if (!payload || typeof payload !== "object") return false;
  return Boolean(payload.recommendations)
    || Boolean(payload.providerCoverageLinkBackfill)
    || Array.isArray(payload.warnings);
}

function isLoadedRegistryConflictsPayload(payload = {}) {
  if (!payload || typeof payload !== "object") return false;
  const status = String(payload.summaryStatus || "").toLowerCase();
  if (status === "pending") return false;
  return Boolean(status)
    || Boolean(payload.summary)
    || Array.isArray(payload.conflicts);
}

export function isLoadedDedupPayload(payload = {}) {
  const dedupEvidence = getObjectValue(payload?.latestRun?.dedupEvidence);
  return Object.keys(dedupEvidence).length > 0;
}

const REGISTRY_SYNC_DETAIL_FIELDS = [
  "activeCount",
  "pendingCount",
  "hiddenPendingCount",
  "deferredPendingCount",
  "ignoredRejectedCount",
  "ignoredTombstonedCount",
  "lastSyncAt",
  "lastSyncStatus",
  "pulledCount",
  "pushedCount",
  "conflictCount",
  "invalidRowsCount"
];

export function hasRegistrySyncDetails(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  return REGISTRY_SYNC_DETAIL_FIELDS.every(field => (
    Object.prototype.hasOwnProperty.call(value, field)
  ));
}

export function renderOpsTabBadges(refs, {
  health = {},
  discoveryReport = null,
  sourcePolicyRecommendations = {},
  registryConflictsPayload = {},
  fetcherMetricsPayload = {},
  tabCountsPayload = null,
  activePipelineOrFetch = false,
  tabCountsUnavailable = false
} = {}) {
  const badges = Array.isArray(refs?.adminOpsTabBadgeEls) ? refs.adminOpsTabBadgeEls : [];
  if (!badges.length) return;
  const discovery = discoveryReport && typeof discoveryReport === "object"
    ? discoveryReport
    : {};
  const dedupEvidence = getObjectValue(fetcherMetricsPayload?.latestRun?.dedupEvidence);
  const registryConflictsSummary = getObjectValue(registryConflictsPayload?.summary);
  const registrySummaryStatus = String(registryConflictsPayload?.summaryStatus || "").toLowerCase();
  const registryConflictCount = Number(registryConflictsSummary?.conflictCount || 0);
  const registryConflictBadgeState = registrySummaryStatus === "pending"
    ? {
        count: 0,
        tone: "pending",
        title: "Registry conflict summary pending",
        loaded: false
      }
    : registrySummaryStatus === "unavailable"
      ? {
        count: 0,
        tone: "neutral",
        title: "Registry conflict summary unavailable",
        loaded: true
      }
    : {
        count: registryConflictCount,
        tone: registryConflictCount > 0 ? "warning" : "neutral",
        title: registryConflictCount > 0
          ? formatBadgeTitle(registryConflictCount, "registry conflict", "registry conflicts")
          : "No registry conflicts"
    };
  const pendingBadge = title => (
    activePipelineOrFetch
      ? delayedBadgeState()
      : tabCountsUnavailable
        ? pendingBadgeState(OPS_TAB_COUNTS_UNAVAILABLE_LABEL, {
            pendingText: OPS_TAB_BADGE_DELAYED_TEXT
          })
        : pendingBadgeState(title)
  );
  const localBadgeStates = {
    overview: isLoadedOverviewHealth(health)
      ? toAlertBadgeState(health?.alerts || [])
      : pendingBadge("Loading Overview count"),
    discovery: isLoadedDiscoveryReport(discovery)
      ? toDiscoveryBadgeState(discovery)
      : pendingBadge("Loading Discovery Review count"),
    "source-policy": isLoadedSourcePolicyPayload(sourcePolicyRecommendations)
      ? toSourcePolicyBadgeState(sourcePolicyRecommendations || {})
      : pendingBadge("Loading Source Policy Review count"),
    "registry-conflicts": isLoadedRegistryConflictsPayload(registryConflictsPayload)
      ? registryConflictBadgeState
      : pendingBadge("Loading Registry Conflicts count"),
    dedup: isLoadedDedupPayload(fetcherMetricsPayload)
      ? toDedupBadgeState(dedupEvidence)
      : pendingBadge("Loading Dedup Lists count")
  };
  badges.forEach(badge => {
    const key = String(badge?.dataset?.opsTab || badge?.getAttribute?.("data-ops-tab") || "");
    const summaryState = getSummaryBadgeState(tabCountsPayload, key);
    const localState = localBadgeStates[key];
    const localLoaded = localState ? localState.loaded !== false : false;
    const state = summaryState && (summaryState.loaded || !localLoaded)
      ? summaryState
      : (localState || summaryState || pendingBadgeState());
    if (badge) {
      badge.textContent = state.loaded === false
        ? String(state.pendingText || OPS_TAB_BADGE_PENDING_TEXT)
        : Number(state.count || 0).toLocaleString();
      badge.setAttribute?.("data-badge-tone", state.tone);
      setTooltip(badge, state.title);
    }
  });
}
