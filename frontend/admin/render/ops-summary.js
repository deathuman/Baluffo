import { escapeHtml } from "../../shared/ui/index.js";
import {
  FETCHER_FAILURE_BUCKET_LABELS,
  formatDuration,
  formatDateTime,
  formatLastRunCell,
  formatScheduleCell,
  sanitizeSlowSourceName,
  stableOpsSignature
} from "./ops-shared.js";

export function renderAdminOpsAlerts(alertsEl, alerts, handlers = {}) {
  if (!alertsEl) return;
  const canPatchInPlace = Boolean(alertsEl && alertsEl.dataset);
  const rows = Array.isArray(alerts) ? alerts : [];
  const signature = stableOpsSignature(rows.map(alert => ({
    id: String(alert?.id || ""),
    severity: String(alert?.severity || ""),
    message: String(alert?.message || ""),
    dismissible: alert?.dismissible !== false
  })));
  if (canPatchInPlace && alertsEl.dataset.opsAlertsSig === signature) return;
  if (canPatchInPlace) alertsEl.dataset.opsAlertsSig = signature;
  if (!rows.length) {
    alertsEl.innerHTML = '<div class="admin-alert-banner healthy">No active alerts.</div>';
    return;
  }
  alertsEl.innerHTML = rows.map(alert => {
    const id = escapeHtml(String(alert?.id || ""));
    const severity = String(alert?.severity || "warning").toLowerCase();
    const cls = severity === "critical" ? "critical" : "warning";
    const dismissible = alert?.dismissible !== false;
    return `
      <div class="admin-alert-banner ${cls}">
        <div class="admin-alert-message">${escapeHtml(String(alert?.message || id))}</div>
        ${dismissible
          ? `<button class="btn back-btn admin-alert-ack-btn" data-ui="admin-alert-ack-btn" data-alert-id="${id}">Dismiss</button>`
          : ""}
      </div>
    `;
  }).join("");

  alertsEl.querySelectorAll(".admin-alert-ack-btn").forEach(btn => {
    btn.addEventListener("click", () => {
      if (typeof handlers.onAck === "function") {
        handlers.onAck(String(btn.dataset.alertId || ""));
      }
    });
  });
}

export function renderAdminOpsKpis(kpisEl, kpis, status) {
  if (!kpisEl) return;
  const canPatchInPlace = Boolean(kpisEl && kpisEl.dataset);
  const signature = stableOpsSignature({
    status: String(status || ""),
    sevenDayFetchSuccessRate: Number(kpis?.sevenDayFetchSuccessRate || 0),
    failedSourceRatioLatest: Number(kpis?.failedSourceRatioLatest || 0),
    pendingApprovalsCount: Number(kpis?.pendingApprovalsCount || 0),
    avgFetchDurationMs7d: Number(kpis?.avgFetchDurationMs7d || 0),
    lastSuccessfulFetchAge: String(kpis?.lastSuccessfulFetchAge || ""),
    registrySync: kpis?.registrySync || {},
    providerCoverage: kpis?.providerCoverage || {}
  });
  if (canPatchInPlace && kpisEl.dataset.opsKpisSig === signature) return;
  if (canPatchInPlace) kpisEl.dataset.opsKpisSig = signature;
  const successRate = Number(kpis?.sevenDayFetchSuccessRate || 0);
  const failedRatio = Number(kpis?.failedSourceRatioLatest || 0);
  const pending = Number(kpis?.pendingApprovalsCount || 0);
  const avgMs = Number(kpis?.avgFetchDurationMs7d || 0);
  const registrySync = kpis?.registrySync && typeof kpis.registrySync === "object"
    ? kpis.registrySync
    : {};
  const providerCoverage = kpis?.providerCoverage && typeof kpis.providerCoverage === "object"
    ? kpis.providerCoverage
    : {};
  const statusClass = status === "critical" ? "critical" : status === "warning" ? "warning" : "healthy";
  const lastSyncAt = String(registrySync?.lastSyncAt || "");
  const lastSyncLabel = lastSyncAt ? formatDateTime(lastSyncAt) : "Never";
  kpisEl.innerHTML = `
    <div class="admin-total-card">
      <div class="admin-total-label">Ops Status</div>
      <div class="admin-total-value"><span class="admin-status-chip ${statusClass}">${escapeHtml(status)}</span></div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Last Successful Fetch</div>
      <div class="admin-total-value">${escapeHtml(String(kpis?.lastSuccessfulFetchAge || "unknown"))}</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Fetch Success (7d)</div>
      <div class="admin-total-value">${(successRate * 100).toFixed(1)}%</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Avg Fetch Duration (7d)</div>
      <div class="admin-total-value">${formatDuration(avgMs)}</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Failed Source Ratio</div>
      <div class="admin-total-value">${(failedRatio * 100).toFixed(1)}%</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Pending Approvals</div>
      <div class="admin-total-value">${pending.toLocaleString()}</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Active Sources</div>
      <div class="admin-total-value">${Number(registrySync?.activeCount || 0).toLocaleString()}</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Pending Review</div>
      <div class="admin-total-value">${Number(registrySync?.pendingCount || 0).toLocaleString()}</div>
    </div>
    <div class="admin-ops-schedule-item admin-ops-full-row">
      <strong>Registry &amp; Sync</strong>:
      hidden ${Number(registrySync?.hiddenPendingCount || 0).toLocaleString()},
      deferred ${Number(registrySync?.deferredPendingCount || 0).toLocaleString()},
      rejected local-only ${Number(registrySync?.ignoredRejectedCount || 0).toLocaleString()},
      tombstones local-only ${Number(registrySync?.ignoredTombstonedCount || 0).toLocaleString()}.
    </div>
    <div class="admin-ops-schedule-item admin-ops-full-row">
      <strong>Last sync</strong>:
      ${escapeHtml(String(registrySync?.lastSyncStatus || "never"))} @ ${escapeHtml(lastSyncLabel)};
      pull ${Number(registrySync?.pulledCount || 0).toLocaleString()},
      push ${Number(registrySync?.pushedCount || 0).toLocaleString()},
      conflicts ${Number(registrySync?.conflictCount || 0).toLocaleString()},
      invalid rows ${Number(registrySync?.invalidRowsCount || 0).toLocaleString()}.
    </div>
    <div class="admin-ops-schedule-item admin-ops-full-row">
      <strong>Provider coverage</strong>:
      validated ${Number(providerCoverage?.statusCounts?.validated_provider || 0).toLocaleString()},
      probing ${Number((providerCoverage?.statusCounts?.probing || 0) + (providerCoverage?.statusCounts?.untested || 0)).toLocaleString()},
      failed/unstable ${Number((providerCoverage?.statusCounts?.failed_provider || 0) + (providerCoverage?.statusCounts?.unstable_provider || 0)).toLocaleString()},
      ready later ${Number((providerCoverage?.readyLaterProviders || []).length || 0).toLocaleString()}.
      Static sources are retained.
    </div>
  `;
}

export function renderAdminOpsSchedule(scheduleEl, schedule, latestOpsHealthCache) {
  if (!scheduleEl) return;
  const canPatchInPlace = Boolean(scheduleEl && scheduleEl.dataset);
  const signature = stableOpsSignature({
    schedule: schedule || {},
    lastRunResult: latestOpsHealthCache?.kpis?.lastRunResult || {}
  });
  if (canPatchInPlace && scheduleEl.dataset.opsScheduleSig === signature) return;
  if (canPatchInPlace) scheduleEl.dataset.opsScheduleSig = signature;
  const fetcher = schedule?.fetcher || {};
  const discovery = schedule?.discovery || {};
  scheduleEl.innerHTML = `
    <div class="admin-ops-schedule-item"><strong>Fetcher</strong>: ${formatScheduleCell(fetcher)}</div>
    <div class="admin-ops-schedule-item"><strong>Discovery</strong>: ${formatScheduleCell(discovery)}</div>
    <div class="admin-ops-schedule-item"><strong>Last Run</strong>: ${formatLastRunCell(latestOpsHealthCache?.kpis?.lastRunResult || {})}</div>
  `;
}

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

function formatDedupSourceClasses(sourceClasses) {
  const classes = sourceClasses && typeof sourceClasses === "object" ? sourceClasses : {};
  return [
    `provider ${Number(classes?.provider || 0).toLocaleString()}`,
    `static ${Number(classes?.static || 0).toLocaleString()}`,
    `social ${Number(classes?.social || 0).toLocaleString()}`,
    `other ${Number(classes?.other || 0).toLocaleString()}`
  ].join(", ");
}

function formatDedupRiskReasonCounts(reasonCounts) {
  const counts = reasonCounts && typeof reasonCounts === "object" ? reasonCounts : {};
  return [
    `location ${Number(counts?.same_title_company_different_location || 0).toLocaleString()}`,
    `provider/static ${Number(counts?.provider_static_duplicate_disagreement || 0).toLocaleString()}`,
    `missing provider IDs ${Number(counts?.missing_provider_ids || 0).toLocaleString()}`,
    `weak title/company ${Number(counts?.weak_title_company_only_evidence || 0).toLocaleString()}`
  ].join(", ");
}

function formatDedupOutlierReasonCounts(reasonCounts) {
  const counts = reasonCounts && typeof reasonCounts === "object" ? reasonCounts : {};
  return [
    `multi-location strong ${Number(counts?.multi_location_strong_identity || 0).toLocaleString()}`,
    `location weak ${Number(counts?.location_divergence_without_strong_identity || 0).toLocaleString()}`,
    `provider/static ${Number(counts?.provider_static_disagreement || 0).toLocaleString()}`,
    `large other ${Number(counts?.large_other_source_bundle || 0).toLocaleString()}`,
    `sparse ${Number(counts?.sparse_title_company_bundle || 0).toLocaleString()}`,
    `unknown ${Number(counts?.unknown || 0).toLocaleString()}`
  ].join(", ");
}

function formatDedupIdentityShapeCounts(shapeCounts) {
  const counts = shapeCounts && typeof shapeCounts === "object" ? shapeCounts : {};
  return [
    `detail URL ${Number(counts?.shared_job_detail_url || 0).toLocaleString()}`,
    `listing/category URL ${Number(counts?.shared_listing_or_category_url || 0).toLocaleString()}`,
    `many URLs ${Number(counts?.many_unique_urls_same_title || 0).toLocaleString()}`,
    `provider ID ${Number(counts?.provider_id_backed || 0).toLocaleString()}`,
    `missing URL/IDs ${Number(counts?.missing_url_and_ids || 0).toLocaleString()}`,
    `mixed/unknown ${Number(counts?.mixed_or_unknown_identity || 0).toLocaleString()}`
  ].join(", ");
}

function formatDedupReviewQueueCounts(queueCounts) {
  const counts = queueCounts && typeof queueCounts === "object" ? queueCounts : {};
  return [
    `many URLs ${Number(counts?.review_many_urls_same_title || 0).toLocaleString()}`,
    `listing URL ${Number(counts?.review_listing_url_bundle || 0).toLocaleString()}`,
    `category title ${Number(counts?.review_category_title_bundle || 0).toLocaleString()}`,
    `open application ${Number(counts?.review_open_application_bundle || 0).toLocaleString()}`,
    `provider/static ${Number(counts?.review_provider_static_disagreement || 0).toLocaleString()}`,
    `monitor ${Number(counts?.monitor || 0).toLocaleString()}`
  ].join(", ");
}

function formatDedupMergedRows(rows, emptyText) {
  const mergedRows = Array.isArray(rows) ? rows : [];
  if (!mergedRows.length) return escapeHtml(emptyText);
  const body = mergedRows
    .slice(0, 5)
    .map(row => {
      const title = String(row?.title || "Untitled");
      const company = String(row?.company || "Unknown company");
      const count = Number(row?.sourceBundleCount || 0);
      const classes = formatDedupSourceClasses(row?.sourceClasses);
      return `
        <tr>
          <td>${escapeHtml(title)}</td>
          <td>${escapeHtml(company)}</td>
          <td>${count.toLocaleString()}</td>
          <td>${escapeHtml(classes)}</td>
        </tr>
      `;
    })
    .join("");
  return `
    <table class="admin-dedup-evidence-table">
      <thead><tr><th>Title</th><th>Company</th><th>Sources</th><th>Classes</th></tr></thead>
      <tbody>${body}</tbody>
    </table>
  `;
}

function formatDedupOutlierRows(rows, emptyText) {
  const outlierRows = Array.isArray(rows) ? rows : [];
  if (!outlierRows.length) return escapeHtml(emptyText);
  const body = outlierRows
    .slice(0, 5)
    .map(row => {
      const title = String(row?.title || "Untitled");
      const company = String(row?.company || "Unknown company");
      const count = Number(row?.sourceBundleCount || 0);
      const classes = formatDedupSourceClasses(row?.sourceClasses);
      const reason = String(row?.outlierReason || "unknown").replaceAll("_", " ");
      const locations = Number(row?.distinctLocationCount || 0);
      const links = Number(row?.uniqueJobLinkCount || 0);
      const providerIds = Number(row?.providerSourceJobIdCount || 0);
      const dominant = String(row?.dominantSourceClass || "unknown");
      const strong = row?.hasStrongIdentity ? "strong identity" : "weak identity";
      const shared = row?.sharedPrimaryUrl ? ", shared URL" : "";
      const identityShape = String(row?.identityShape || "mixed_or_unknown_identity").replaceAll("_", " ");
      const titleShape = String(row?.titleShape || "empty_or_unknown").replaceAll("_", " ");
      const caveats = Array.isArray(row?.identityCaveats) ? row.identityCaveats : [];
      const caveatText = caveats.length
        ? `; caveats ${caveats.join(", ").replaceAll("_", " ")}`
        : "";
      const sharedUrl = row?.sharedUrlHost || row?.sharedUrlPath
        ? `; shared ${String(row?.sharedUrlHost || "")}${String(row?.sharedUrlPath || "")}`
        : "";
      const urlShape = `${identityShape}; title ${titleShape}; hosts ${Number(row?.uniqueUrlHostCount || 0).toLocaleString()}, prefixes ${Number(row?.uniqueUrlPathPrefixCount || 0).toLocaleString()}`;
      const detail = `${reason}; ${locations.toLocaleString()} locations, ${links.toLocaleString()} links, ${providerIds.toLocaleString()} provider IDs, ${dominant} dominant, ${strong}${shared}; ${urlShape}${sharedUrl}${caveatText}`;
      return `
        <tr>
          <td>${escapeHtml(title)}</td>
          <td>${escapeHtml(company)}</td>
          <td>${count.toLocaleString()}</td>
          <td>${escapeHtml(classes)}</td>
          <td>${escapeHtml(detail)}</td>
        </tr>
      `;
    })
    .join("");
  return `
    <table class="admin-dedup-evidence-table">
      <thead><tr><th>Title</th><th>Company</th><th>Sources</th><th>Classes</th><th>Outlier evidence</th></tr></thead>
      <tbody>${body}</tbody>
    </table>
  `;
}

function formatDedupRiskRows(rows, emptyText) {
  const riskRows = Array.isArray(rows) ? rows : [];
  if (!riskRows.length) return escapeHtml(emptyText);
  const body = riskRows
    .slice(0, 5)
    .map(row => {
      const title = String(row?.title || "Untitled");
      const company = String(row?.company || "Unknown company");
      const reasons = Array.isArray(row?.riskReasons) ? row.riskReasons : [];
      const reasonText = reasons.length ? reasons.join(", ").replaceAll("_", " ") : "review";
      return `
        <tr>
          <td>${escapeHtml(title)}</td>
          <td>${escapeHtml(company)}</td>
          <td>${escapeHtml(reasonText)}</td>
        </tr>
      `;
    })
    .join("");
  return `
    <table class="admin-dedup-evidence-table">
      <thead><tr><th>Title</th><th>Company</th><th>Reason</th></tr></thead>
      <tbody>${body}</tbody>
    </table>
  `;
}

function formatDedupReviewQueueRows(rows, emptyText) {
  const queueRows = Array.isArray(rows) ? rows : [];
  if (!queueRows.length) return escapeHtml(emptyText);
  const body = queueRows
    .slice(0, 5)
    .map(row => {
      const title = String(row?.title || "Untitled");
      const company = String(row?.company || "Unknown company");
      const count = Number(row?.sourceBundleCount || 0);
      const action = String(row?.recommendedReviewAction || "monitor").replaceAll("_", " ");
      const identityShape = String(row?.identityShape || "mixed_or_unknown_identity").replaceAll("_", " ");
      const outlierReason = String(row?.outlierReason || "unknown").replaceAll("_", " ");
      const caveats = Array.isArray(row?.identityCaveats) ? row.identityCaveats : [];
      const caveatText = caveats.length ? caveats.join(", ").replaceAll("_", " ") : "none";
      const sources = Array.isArray(row?.sampleSources) ? row.sampleSources : Array.isArray(row?.sources) ? row.sources : [];
      const sourceText = sources.length ? sources.slice(0, 3).join(" | ") : "none";
      const detail = `${identityShape}; ${outlierReason}; caveats ${caveatText}; sources ${sourceText}`;
      return `
        <tr>
          <td>${escapeHtml(action)}</td>
          <td>${escapeHtml(title)}</td>
          <td>${escapeHtml(company)}</td>
          <td>${count.toLocaleString()}</td>
          <td>${escapeHtml(detail)}</td>
        </tr>
      `;
    })
    .join("");
  return `
    <table class="admin-dedup-evidence-table">
      <thead><tr><th>Action</th><th>Title</th><th>Company</th><th>Sources</th><th>Evidence</th></tr></thead>
      <tbody>${body}</tbody>
    </table>
  `;
}

export function renderAdminOpsFetcherMetrics(metricsEl, metrics, failureSummary = null) {
  if (!metricsEl) return;
  const latest = metrics?.latestRun || {};
  const history = metrics?.history || {};
  const summary = failureSummary && typeof failureSummary === "object"
    ? failureSummary
    : { topLevelFailedSources: 0, detailFailureCount: 0, buckets: [] };
  const canPatchInPlace = Boolean(metricsEl && metricsEl.dataset);
  const signature = stableOpsSignature({
    latestRun: {
      inputCount: Number(latest?.inputCount || 0),
      outputCount: Number(latest?.outputCount || 0),
      duplicateRate: Number(latest?.duplicateRate || 0),
      sourceFailureRate: Number(latest?.sourceFailureRate || 0),
      failedSources: Number(latest?.failedSources || 0),
      sourceCount: Number(latest?.sourceCount || 0),
      durationMs: Number(latest?.durationMs || 0),
      medianSourceDurationMs: Number(latest?.medianSourceDurationMs || 0),
      p95SourceDurationMs: Number(latest?.p95SourceDurationMs || 0)
    },
    history: {
      windowRuns: Number(history?.windowRuns || 0),
      medianDurationMs: Number(history?.medianDurationMs || 0),
      averageDurationMs: Number(history?.averageDurationMs || 0)
    },
    slowestSources: Array.isArray(latest?.slowestSources) ? latest.slowestSources : [],
    stageTop: Array.isArray(latest?.stageTop) ? latest.stageTop : [],
    sourceHealth: latest?.sourceHealth || {},
    dedupEvidence: latest?.dedupEvidence || {},
    providerCoverage: latest?.providerCoverage || {},
    providerStaticOverlap: latest?.providerStaticOverlap || {},
    staticSuppressionPolicy: latest?.staticSuppressionPolicy || {},
    redundantStaticProposals: latest?.redundantStaticProposals || {},
    sourcePolicyRecommendationExport: latest?.sourcePolicyRecommendationExport || {},
    failureSummary: summary
  });
  if (canPatchInPlace && metricsEl.dataset.opsFetcherMetricsSig === signature) return;
  if (canPatchInPlace) metricsEl.dataset.opsFetcherMetricsSig = signature;

  const failed = Number(latest?.failedSources || 0);
  const sourceCount = Math.max(0, Number(latest?.sourceCount || 0));
  const duplicateRate = Math.max(0, Number(latest?.duplicateRate || 0));
  const outputYieldRate = Math.max(0, Number(latest?.outputYieldRate || 0));
  const failureRate = Math.max(0, Number(latest?.sourceFailureRate || 0));
  const slowest = Array.isArray(latest?.slowestSources) ? latest.slowestSources : [];
  const stageTop = Array.isArray(latest?.stageTop) ? latest.stageTop : [];
  const highCostLowYield = Array.isArray(latest?.highCostLowYieldSources) ? latest.highCostLowYieldSources : [];
  const sourceHealth = latest?.sourceHealth && typeof latest.sourceHealth === "object" ? latest.sourceHealth : {};
  const dedupEvidence = latest?.dedupEvidence && typeof latest.dedupEvidence === "object" ? latest.dedupEvidence : {};
  const providerCoverage = latest?.providerCoverage && typeof latest.providerCoverage === "object" ? latest.providerCoverage : {};
  const providerStaticOverlap = latest?.providerStaticOverlap && typeof latest.providerStaticOverlap === "object" ? latest.providerStaticOverlap : {};
  const staticSuppressionPolicy = latest?.staticSuppressionPolicy && typeof latest.staticSuppressionPolicy === "object" ? latest.staticSuppressionPolicy : {};
  const redundantStaticProposals = latest?.redundantStaticProposals && typeof latest.redundantStaticProposals === "object" ? latest.redundantStaticProposals : {};
  const sourcePolicyRecommendationExport = latest?.sourcePolicyRecommendationExport && typeof latest.sourcePolicyRecommendationExport === "object" ? latest.sourcePolicyRecommendationExport : {};
  const slowestSummary = slowest.length
    ? slowest
      .slice(0, 3)
      .map(row => `${sanitizeSlowSourceName(row?.name)} (${formatDuration(Number(row?.durationMs || 0))})`)
      .filter(Boolean)
      .join(" | ")
    : "No source timing data yet.";
  const slowestStageSummary = stageTop.length
    ? stageTop
      .slice(0, 3)
      .map(row => `${String(row?.stage || "unknown")} (${formatDuration(Number(row?.durationMs || 0))})`)
      .join(" | ")
    : "No stage timing data yet.";
  const highCostSummary = highCostLowYield.length
    ? highCostLowYield
      .slice(0, 3)
      .map(row => `${sanitizeSlowSourceName(row?.name)} (${formatDuration(Number(row?.durationMs || 0))}, kept ${Number(row?.keptCount || 0)})`)
      .join(" | ")
    : "No high-cost low-yield sources.";
  const bucketRows = Array.isArray(summary?.buckets) ? summary.buckets : [];
  const attentionSummary = formatSourceHealthRows(
    sourceHealth?.sourcesNeedingAttention,
    "No sources need attention.",
    { includeDuration: true }
  );
  const zeroReviewSummary = formatSourceHealthRows(
    sourceHealth?.zeroKeptNeedsReview,
    "No zero-kept sources need review.",
    { includeDuration: true }
  );
  const browserSummary = formatSourceHealthRows(
    sourceHealth?.browserFallbackRecommended,
    "No browser fallback recommendations.",
    { includeDuration: true }
  );
  const productiveSummary = formatSourceHealthRows(
    sourceHealth?.topProductiveSources,
    "No productive source ranking yet."
  );
  const dynamicRedundantSummary = formatDynamicRedundantStaticRows(
    sourceHealth?.dynamicRedundantStatic,
    "No runtime-only static suppression."
  );
  const validatedProviderSummary = formatProviderCoverageRows(
    providerCoverage?.validatedProviders,
    "No validated staged providers yet."
  );
  const failedProviderSummary = formatProviderCoverageRows(
    providerCoverage?.unstableOrFailedProviders,
    "No unstable or failed staged providers."
  );
  const reviewProviderSummary = formatProviderCoverageRows(
    providerCoverage?.needsReviewProviders,
    "No provider coverage rows need review."
  );
  const readyLaterProviderSummary = formatProviderCoverageRows(
    providerCoverage?.readyLaterProviders,
    "No providers are replacement-ready for a later slice."
  );
  const overlapAuditSummary = formatProviderStaticOverlapRows(
    providerStaticOverlap?.pairs,
    "No provider/static overlap audit pairs."
  );
  const suppressedPolicySummary = formatStaticSuppressionPolicyRows(
    staticSuppressionPolicy?.suppressedPairs,
    "No policy-suppressed pairs."
  );
  const pausedPolicySummary = formatStaticSuppressionPolicyRows(
    staticSuppressionPolicy?.pausedPairs,
    "No policy-paused pairs."
  );
  const warningPolicySummary = formatStaticSuppressionPolicyRows(
    staticSuppressionPolicy?.warningPairs,
    "No warning-suppressed pairs."
  );
  const proposalRows = Array.isArray(redundantStaticProposals?.proposals) ? redundantStaticProposals.proposals : [];
  const safeRedundantProposalSummary = formatRedundantStaticProposalRows(
    proposalRows.filter(row => row?.proposal === "safe_redundant_static"),
    "No safe redundant static proposals."
  );
  const keepStaticProposalSummary = formatRedundantStaticProposalRows(
    proposalRows.filter(row => row?.proposal === "keep_static"),
    "No keep-static proposals."
  );
  const moreHistoryProposalSummary = formatRedundantStaticProposalRows(
    proposalRows.filter(row => row?.proposal === "needs_more_history"),
    "No more-history proposals."
  );
  const reviewProposalSummary = formatRedundantStaticProposalRows(
    proposalRows.filter(row => row?.proposal === "needs_review" || row?.proposal === "provider_unstable"),
    "No review/provider-unstable proposals."
  );
  const staticOnlyProposalSummary = formatRedundantStaticProposalRows(
    proposalRows.filter(row => row?.proposal === "static_only_jobs_detected"),
    "No static-only proposals."
  );
  const mergeReasonCounts = dedupEvidence?.mergeReasonCounts && typeof dedupEvidence.mergeReasonCounts === "object"
    ? dedupEvidence.mergeReasonCounts
    : {};
  const sourceBundleComposition = dedupEvidence?.sourceBundleComposition && typeof dedupEvidence.sourceBundleComposition === "object"
    ? dedupEvidence.sourceBundleComposition
    : {};
  const riskReasonCounts = dedupEvidence?.riskReasonCounts && typeof dedupEvidence.riskReasonCounts === "object"
    ? dedupEvidence.riskReasonCounts
    : {};
  const outlierReasonCounts = dedupEvidence?.outlierReasonCounts && typeof dedupEvidence.outlierReasonCounts === "object"
    ? dedupEvidence.outlierReasonCounts
    : {};
  const identityShapeCounts = dedupEvidence?.identityShapeCounts && typeof dedupEvidence.identityShapeCounts === "object"
    ? dedupEvidence.identityShapeCounts
    : {};
  const reviewQueueCounts = dedupEvidence?.reviewQueueCounts && typeof dedupEvidence.reviewQueueCounts === "object"
    ? dedupEvidence.reviewQueueCounts
    : {};
  const topMergedSummary = formatDedupMergedRows(
    dedupEvidence?.topMergedJobs,
    "No merged canonical jobs in the latest fetch report."
  );
  const topOutlierSummary = formatDedupOutlierRows(
    dedupEvidence?.topSourceBundleOutliers,
    "No carried source-bundle collision outliers in the latest fetch report."
  );
  const riskyMergeSummary = formatDedupRiskRows(
    dedupEvidence?.riskyMergeExamples,
    "No risky merge examples in the latest fetch report."
  );
  const reviewQueueSummary = formatDedupReviewQueueRows(
    dedupEvidence?.reviewQueue,
    "No dedup review queue examples in the latest fetch report."
  );
  const bucketSummaryHtml = bucketRows.length
    ? bucketRows.map(bucket => `
      <div class="admin-ops-schedule-item admin-ops-full-row">
        <strong>${escapeHtml(FETCHER_FAILURE_BUCKET_LABELS[bucket.key] || bucket.key)}</strong>
        : ${Number(bucket.count || 0).toLocaleString()}
        ${bucket.examples?.length ? ` (${escapeHtml(bucket.examples.join(" | "))})` : ""}
      </div>
    `).join("")
    : `
      <div class="admin-ops-schedule-item admin-ops-full-row">
        <strong>Failure buckets</strong>: No classified failures in the latest fetch report.
      </div>
    `;

  metricsEl.innerHTML = `
    <div class="admin-total-card">
      <div class="admin-total-label">Latest Runtime</div>
      <div class="admin-total-value">${formatDuration(Number(latest?.durationMs || 0))}</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Median Runtime</div>
      <div class="admin-total-value">${formatDuration(Number(history?.medianDurationMs || 0))}</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Average Runtime</div>
      <div class="admin-total-value">${formatDuration(Number(history?.averageDurationMs || 0))}</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Window Runs</div>
      <div class="admin-total-value">${Number(history?.windowRuns || 0).toLocaleString()}</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Duplicate Rate</div>
      <div class="admin-total-value">${(duplicateRate * 100).toFixed(1)}%</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Output Yield</div>
      <div class="admin-total-value">${(outputYieldRate * 100).toFixed(1)}%</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Current Run Merges</div>
      <div class="admin-total-value">${Number(dedupEvidence?.mergedCount || latest?.mergedCount || 0).toLocaleString()}</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Bundle Collisions</div>
      <div class="admin-total-value">${Number(dedupEvidence?.sourceBundleCollisionCount || 0).toLocaleString()}</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Risky Merges</div>
      <div class="admin-total-value">${Number(dedupEvidence?.riskyMergeExampleCount || 0).toLocaleString()}</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Median Source Time</div>
      <div class="admin-total-value">${formatDuration(Number(latest?.medianSourceDurationMs || 0))}</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">P95 Source Time</div>
      <div class="admin-total-value">${formatDuration(Number(latest?.p95SourceDurationMs || 0))}</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Source Failures</div>
      <div class="admin-total-value">${failed.toLocaleString()} / ${sourceCount.toLocaleString()} (${(failureRate * 100).toFixed(1)}%)</div>
    </div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Top-level failed sources</strong>: ${Number(summary?.topLevelFailedSources || 0).toLocaleString()}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Grouped detail failures</strong>: ${Number(summary?.detailFailureCount || 0).toLocaleString()}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Failure buckets</strong></div>
    ${bucketSummaryHtml}
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Slowest sources</strong>: ${escapeHtml(slowestSummary)}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Slowest stages</strong>: ${escapeHtml(slowestStageSummary)}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>High-cost low-yield</strong>: ${escapeHtml(highCostSummary)}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup evidence</strong>: read-only diagnostics. Current-run merges by reason: primary URL ${Number(mergeReasonCounts?.primaryUrl || 0).toLocaleString()}, secondary key ${Number(mergeReasonCounts?.secondaryKey || 0).toLocaleString()}, social key ${Number(mergeReasonCounts?.socialKey || 0).toLocaleString()}, sparse identity ${Number(mergeReasonCounts?.sparseIdentity || 0).toLocaleString()}, unknown ${Number(mergeReasonCounts?.unknown || 0).toLocaleString()}. Carried source-bundle collision rows: ${Number(dedupEvidence?.sourceBundleCollisionCount || 0).toLocaleString()}.</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup source composition</strong>: ${escapeHtml(formatDedupSourceClasses(sourceBundleComposition))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup risk reasons</strong>: ${escapeHtml(formatDedupRiskReasonCounts(riskReasonCounts))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup outlier reasons</strong>: ${escapeHtml(formatDedupOutlierReasonCounts(outlierReasonCounts))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup identity shapes</strong>: ${escapeHtml(formatDedupIdentityShapeCounts(identityShapeCounts))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup review queue</strong>: ${escapeHtml(formatDedupReviewQueueCounts(reviewQueueCounts))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Top merged jobs</strong>: ${topMergedSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Top source-bundle outliers</strong>: ${topOutlierSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup review examples</strong>: ${reviewQueueSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Risky merge examples</strong>: ${riskyMergeSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Sources needing attention</strong>: ${attentionSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Zero kept / needs review</strong>: ${zeroReviewSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Browser fallback recommended</strong>: ${browserSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Top productive sources</strong>: ${productiveSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Runtime-suppressed static sources</strong>: ${dynamicRedundantSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Validated staged providers</strong>: ${validatedProviderSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Provider coverage needs review</strong>: ${reviewProviderSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Unstable / failed providers</strong>: ${failedProviderSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Ready later (no static mutation)</strong>: ${readyLaterProviderSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Provider/static overlap audit</strong>: safe ${Number(providerStaticOverlap?.safePairCount || 0).toLocaleString()}, needs review ${Number(providerStaticOverlap?.needsReviewPairCount || 0).toLocaleString()}, insufficient history ${Number(providerStaticOverlap?.insufficientHistoryPairCount || 0).toLocaleString()}. ${overlapAuditSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Static suppression policy</strong>: suppressed ${Number(staticSuppressionPolicy?.suppressedCount || 0).toLocaleString()}, paused ${Number(staticSuppressionPolicy?.pausedCount || 0).toLocaleString()}, warnings ${Number(staticSuppressionPolicy?.warningCount || 0).toLocaleString()}. Suppressed: ${suppressedPolicySummary} Paused: ${pausedPolicySummary} Warnings: ${warningPolicySummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Redundant static proposals</strong>: safe ${Number(redundantStaticProposals?.safeRedundantCount || 0).toLocaleString()}, keep static ${Number(redundantStaticProposals?.keepStaticCount || 0).toLocaleString()}, more history ${Number(redundantStaticProposals?.needsMoreHistoryCount || 0).toLocaleString()}, review/unstable ${Number((redundantStaticProposals?.needsReviewCount || 0) + (redundantStaticProposals?.providerUnstableCount || 0)).toLocaleString()}, static-only ${Number(redundantStaticProposals?.staticOnlyDetectedCount || 0).toLocaleString()}. Safe: ${safeRedundantProposalSummary} Keep: ${keepStaticProposalSummary} History: ${moreHistoryProposalSummary} Review: ${reviewProposalSummary} Static-only: ${staticOnlyProposalSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Source-policy review</strong>: local review pairs ${Number(sourcePolicyRecommendationExport?.reviewStatePairCount || 0).toLocaleString()}, force-paused ${Number(sourcePolicyRecommendationExport?.manualForcePausedCount || 0).toLocaleString()}. Use the Source Policy Review queue for local, reversible actions.</div>
  `;
}
