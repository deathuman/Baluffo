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
    .join(" | ");
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
    .join(" | ");
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
    providerCoverage: latest?.providerCoverage || {},
    providerStaticOverlap: latest?.providerStaticOverlap || {},
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
  const providerCoverage = latest?.providerCoverage && typeof latest.providerCoverage === "object" ? latest.providerCoverage : {};
  const providerStaticOverlap = latest?.providerStaticOverlap && typeof latest.providerStaticOverlap === "object" ? latest.providerStaticOverlap : {};
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
  `;
}
