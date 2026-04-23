import { escapeHtml } from "../../shared/ui/index.js";
import {
  FETCHER_FAILURE_BUCKET_LABELS,
  formatDuration,
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
    lastSuccessfulFetchAge: String(kpis?.lastSuccessfulFetchAge || "")
  });
  if (canPatchInPlace && kpisEl.dataset.opsKpisSig === signature) return;
  if (canPatchInPlace) kpisEl.dataset.opsKpisSig = signature;
  const successRate = Number(kpis?.sevenDayFetchSuccessRate || 0);
  const failedRatio = Number(kpis?.failedSourceRatioLatest || 0);
  const pending = Number(kpis?.pendingApprovalsCount || 0);
  const avgMs = Number(kpis?.avgFetchDurationMs7d || 0);
  const statusClass = status === "critical" ? "critical" : status === "warning" ? "warning" : "healthy";
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
  `;
}
