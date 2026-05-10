import { escapeHtml } from "../../shared/ui/index.js";
import {
  buildOpsFetcherDiagnosticsSections,
  buildOpsFetcherMetricSections,
  buildOpsTaskLaneRows
} from "../domain/ops-health-view-model.js";
import {
  FETCHER_FAILURE_BUCKET_LABELS,
  formatDuration,
  formatDateTime,
  formatLastRunCell,
  formatScheduleCell,
  getRunStatusChipClass,
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
    alertsEl.innerHTML = "";
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
    providerCoverage: kpis?.providerCoverage || {},
    dedupReviewState: kpis?.dedupReviewState || {}
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
  const dedupReviewState = kpis?.dedupReviewState && typeof kpis.dedupReviewState === "object"
    ? kpis.dedupReviewState
    : {};
  const statusClass = status === "critical" ? "critical" : status === "warning" ? "warning" : "healthy";
  const lastSyncAt = String(registrySync?.lastSyncAt || "");
  const lastSyncLabel = lastSyncAt ? formatDateTime(lastSyncAt) : "Never";
  const registryDiagnosticsHtml = `
    <details class="admin-ops-metrics-details admin-ops-registry-sync-details admin-ops-full-row">
      <summary>Registry and sync diagnostics</summary>
      <div class="admin-ops-metrics-details-body">
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
        <div class="admin-ops-schedule-item admin-ops-full-row">
          <strong>Dedup review-state</strong>: ${escapeHtml(formatDedupReviewStateSummary(dedupReviewState))}
        </div>
      </div>
    </details>
  `;
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
    ${registryDiagnosticsHtml}
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

function formatDedupReviewQueueCauseCounts(causeCounts) {
  const counts = causeCounts && typeof causeCounts === "object" ? causeCounts : {};
  return [
    `category ${Number(counts?.category_or_department_bucket || 0).toLocaleString()}`,
    `open application ${Number(counts?.open_application_family || 0).toLocaleString()}`,
    `listing page ${Number(counts?.listing_page_bundle || 0).toLocaleString()}`,
    `spreadsheet role ${Number(counts?.spreadsheet_role_bucket_needs_review || 0).toLocaleString()}`,
    `sheets role audit ${Number(counts?.google_sheets_role_bucket_needs_review || 0).toLocaleString()}`,
    `non-provider URL ${Number(counts?.non_provider_url_identity_needs_review || 0).toLocaleString()}`,
    `parser/text ${Number(counts?.parser_or_directory_text_pollution || 0).toLocaleString()}`,
    `provider/static ${Number(counts?.provider_static_disagreement || 0).toLocaleString()}`,
    `likely legitimate ${Number(counts?.likely_legitimate_multi_role_family || 0).toLocaleString()}`,
    `unknown ${Number(counts?.unknown || 0).toLocaleString()}`
  ].join(", ");
}

function formatDedupAuditGate(gate) {
  const auditGate = gate && typeof gate === "object" ? gate : {};
  const status = String(auditGate?.status || "unknown").replaceAll("_", " ");
  const ready = auditGate?.lifecycleUxReady === true ? "yes" : "no";
  const blockers = Array.isArray(auditGate?.blockers) ? auditGate.blockers : [];
  const warnings = Array.isArray(auditGate?.warnings) ? auditGate.warnings : [];
  const blockerText = blockers.length ? blockers.slice(0, 4).join(", ").replaceAll("_", " ") : "none";
  const warningText = warnings.length ? warnings.slice(0, 4).join(", ").replaceAll("_", " ") : "none";
  const guard = auditGate?.googleSheetsGenericRoleGuardActive === true ? "active" : "unknown";
  return [
    `status ${status}`,
    `lifecycle UX ready ${ready}`,
    `current-run merges ${Number(auditGate?.currentRunMergedCount || 0).toLocaleString()}`,
    `current-run collisions ${Number(auditGate?.currentRunSourceBundleCollisionCount || 0).toLocaleString()}`,
    `carried collisions ${Number(auditGate?.carriedSourceBundleCollisionCount || auditGate?.sourceBundleCollisionCount || 0).toLocaleString()}`,
    `historical-like ${Number(auditGate?.carriedCollisionLikelyHistoricalCount || 0).toLocaleString()}`,
    `raw high-risk diagnostics ${Number(auditGate?.highRiskReviewQueueCount || 0).toLocaleString()}`,
    `current high-risk ${Number(auditGate?.currentRunHighRiskReviewQueueCount || 0).toLocaleString()}`,
    `carried high-risk ${Number(auditGate?.carriedHighRiskReviewQueueCount || 0).toLocaleString()}`,
    `blocking review ${Number(auditGate?.blockingReviewQueueCount || 0).toLocaleString()}`,
    `current blocking ${Number(auditGate?.currentRunBlockingReviewQueueCount || 0).toLocaleString()}`,
    `carried blocking ${Number(auditGate?.carriedBlockingReviewQueueCount || 0).toLocaleString()}`,
    `monitor diagnostics ${Number(auditGate?.monitorReviewQueueCount || 0).toLocaleString()}`,
    `current monitor ${Number(auditGate?.currentRunMonitorReviewQueueCount || 0).toLocaleString()}`,
    `carried monitor ${Number(auditGate?.carriedMonitorReviewQueueCount || 0).toLocaleString()}`,
    `provider/static ${Number(auditGate?.providerStaticDisagreementCount || 0).toLocaleString()}`,
    `provider/static current ${Number(auditGate?.providerStaticDisagreementCurrentRunCount || 0).toLocaleString()}`,
    `provider/static carried ${Number(auditGate?.providerStaticDisagreementCarriedCount || 0).toLocaleString()}`,
    `Google Sheets guard ${guard}`,
    `Sheets role unresolved ${Number(auditGate?.googleSheetsRoleBucketUnresolvedCount || 0).toLocaleString()}`,
    `Sheets guard-blocked ${Number(auditGate?.googleSheetsRoleBucketGuardBlockedCount || 0).toLocaleString()}`,
    `Sheets historical ${Number(auditGate?.googleSheetsRoleBucketHistoricalCount || 0).toLocaleString()}`,
    `blockers ${blockerText}`,
    `warnings ${warningText}`
  ].join("; ");
}

function formatDedupAuditGateExampleDetails(row) {
  const title = String(row?.title || "Untitled");
  const company = String(row?.company || "Unknown company");
  const classification = String(row?.disagreementClassification || row?.suspectedCause || "unknown").replaceAll("_", " ");
  const action = String(row?.recommendedReviewAction || "monitor").replaceAll("_", " ");
  const status = String(row?.dedupReviewStatus || row?.disagreementGateDisposition || "unreviewed").replaceAll("_", " ");
  const reviewUpdatedBy = String(row?.dedupReviewUpdatedBy || "");
  const reviewUpdatedAt = String(row?.dedupReviewUpdatedAt || "");
  const origin = String(row?.bundleEvidenceOrigin || "unknown").replaceAll("_", " ");
  const gateDisposition = String(row?.disagreementGateDisposition || "").replaceAll("_", " ");
  const sourceBundleCount = Number(row?.sourceBundleCount || 0).toLocaleString();
  const causeEvidence = Array.isArray(row?.disagreementClassificationEvidence)
    ? row.disagreementClassificationEvidence
    : Array.isArray(row?.causeEvidence)
      ? row.causeEvidence
      : [];
  const gateEvidence = Array.isArray(row?.disagreementGateEvidence)
    ? row.disagreementGateEvidence
    : Array.isArray(row?.reviewEvidence)
      ? row.reviewEvidence
      : [];
  const summary = `${title} @ ${company} — ${classification}, ${status}`;
  const metaChips = [
    `classification ${classification}`,
    `action ${action}`,
    `review ${status}${reviewUpdatedBy ? ` by ${reviewUpdatedBy}` : ""}${reviewUpdatedAt ? ` at ${reviewUpdatedAt}` : ""}`,
    `origin ${origin}`,
    gateDisposition ? `gate ${gateDisposition}` : "",
    `sources ${sourceBundleCount}`
  ].filter(Boolean);
  return `
    <details class="admin-dedup-audit-gate-example">
      <summary><span class="admin-dedup-audit-gate-example-summary">${escapeHtml(summary)}</span></summary>
      <div class="admin-dedup-audit-gate-example-body">
        <div class="admin-dedup-audit-gate-example-chips">
          ${metaChips.map(label => `<span class="admin-dedup-audit-gate-chip">${escapeHtml(label)}</span>`).join("")}
        </div>
        <div class="admin-dedup-audit-gate-example-section">
          <div class="admin-dedup-audit-gate-example-label">Classification evidence</div>
          <div>${escapeHtml(causeEvidence.slice(0, 5).join(", ").replaceAll("_", " ") || "none")}</div>
        </div>
        <div class="admin-dedup-audit-gate-example-section">
          <div class="admin-dedup-audit-gate-example-label">Gate evidence</div>
          <div>${escapeHtml(gateEvidence.slice(0, 5).join(", ").replaceAll("_", " ") || "none")}</div>
        </div>
      </div>
    </details>
  `;
}

function formatDedupAuditGateCard(gate) {
  const auditGate = gate && typeof gate === "object" ? gate : {};
  const status = String(auditGate?.status || "unknown").replaceAll("_", " ");
  const ready = auditGate?.lifecycleUxReady === true ? "yes" : "no";
  const blockers = Array.isArray(auditGate?.blockers) ? auditGate.blockers : [];
  const warnings = Array.isArray(auditGate?.warnings) ? auditGate.warnings : [];
  const blockerText = blockers.length ? blockers.slice(0, 4).join(", ").replaceAll("_", " ") : "none";
  const warningText = warnings.length ? warnings.slice(0, 4).join(", ").replaceAll("_", " ") : "none";
  const gateChips = [
    `status ${status}`,
    `lifecycle UX ready ${ready}`,
    `current-run merges ${Number(auditGate?.currentRunMergedCount || 0).toLocaleString()}`,
    `current-run collisions ${Number(auditGate?.currentRunSourceBundleCollisionCount || 0).toLocaleString()}`,
    `carried collisions ${Number(auditGate?.carriedSourceBundleCollisionCount || auditGate?.sourceBundleCollisionCount || 0).toLocaleString()}`,
    `historical-like ${Number(auditGate?.carriedCollisionLikelyHistoricalCount || 0).toLocaleString()}`,
    `raw high-risk diagnostics ${Number(auditGate?.highRiskReviewQueueCount || 0).toLocaleString()}`,
    `current high-risk ${Number(auditGate?.currentRunHighRiskReviewQueueCount || 0).toLocaleString()}`,
    `carried high-risk ${Number(auditGate?.carriedHighRiskReviewQueueCount || 0).toLocaleString()}`,
    `blocking review ${Number(auditGate?.blockingReviewQueueCount || 0).toLocaleString()}`,
    `current blocking ${Number(auditGate?.currentRunBlockingReviewQueueCount || 0).toLocaleString()}`,
    `carried blocking ${Number(auditGate?.carriedBlockingReviewQueueCount || 0).toLocaleString()}`,
    `monitor diagnostics ${Number(auditGate?.monitorReviewQueueCount || 0).toLocaleString()}`,
    `current monitor ${Number(auditGate?.currentRunMonitorReviewQueueCount || 0).toLocaleString()}`,
    `carried monitor ${Number(auditGate?.carriedMonitorReviewQueueCount || 0).toLocaleString()}`,
    `provider/static ${Number(auditGate?.providerStaticDisagreementCount || 0).toLocaleString()}`,
    `provider/static current ${Number(auditGate?.providerStaticDisagreementCurrentRunCount || 0).toLocaleString()}`,
    `provider/static carried ${Number(auditGate?.providerStaticDisagreementCarriedCount || 0).toLocaleString()}`,
    `Google Sheets guard ${auditGate?.googleSheetsGenericRoleGuardActive === true ? "active" : "unknown"}`,
    `Sheets role unresolved ${Number(auditGate?.googleSheetsRoleBucketUnresolvedCount || 0).toLocaleString()}`,
    `Sheets guard-blocked ${Number(auditGate?.googleSheetsRoleBucketGuardBlockedCount || 0).toLocaleString()}`,
    `Sheets historical ${Number(auditGate?.googleSheetsRoleBucketHistoricalCount || 0).toLocaleString()}`
  ];
  const examples = Array.isArray(auditGate?.examples) ? auditGate.examples.slice(0, 5) : [];
  return `
    <section class="admin-ops-schedule-item admin-ops-full-row admin-dedup-audit-gate-card">
      <div class="admin-dedup-audit-gate-header">
        <div class="admin-dedup-audit-gate-title">
          <strong>Dedup Audit Gate</strong>
          <span class="admin-dedup-audit-gate-status">status ${escapeHtml(status)}</span>
          <span class="admin-dedup-audit-gate-ready">lifecycle UX ready ${escapeHtml(ready)}</span>
        </div>
        <div class="admin-dedup-audit-gate-summary">${escapeHtml(formatDedupAuditGate(auditGate))}</div>
      </div>
      <div class="admin-dedup-audit-gate-flags">
        <div class="admin-dedup-audit-gate-flag">
          <span class="admin-dedup-audit-gate-flag-label">Blockers</span>
          <span class="admin-dedup-audit-gate-flag-value">${escapeHtml(blockerText)}</span>
        </div>
        <div class="admin-dedup-audit-gate-flag">
          <span class="admin-dedup-audit-gate-flag-label">Warnings</span>
          <span class="admin-dedup-audit-gate-flag-value">${escapeHtml(warningText)}</span>
        </div>
      </div>
      <div class="admin-dedup-audit-gate-chips">
        ${gateChips.map(label => `<span class="admin-dedup-audit-gate-chip">${escapeHtml(label)}</span>`).join("")}
      </div>
      <div class="admin-dedup-audit-gate-examples">
        <div><strong>Examples</strong></div>
        ${examples.length ? examples.map(row => formatDedupAuditGateExampleDetails(row)).join("") : `<div class="admin-dedup-audit-gate-empty">${escapeHtml("No gate examples.")}</div>`}
      </div>
    </section>
  `;
}

function formatDedupAuditGateExamples(rows, emptyText) {
  const examples = Array.isArray(rows) ? rows : [];
  if (!examples.length) return escapeHtml(emptyText);
  return examples
    .slice(0, 5)
    .map(row => {
      const title = String(row?.title || "Untitled");
      const company = String(row?.company || "Unknown company");
      const cause = String(row?.suspectedCause || "unknown").replaceAll("_", " ");
      const quality = String(row?.identityQuality || "unknown").replaceAll("_", " ");
      const action = String(row?.recommendedReviewAction || "monitor").replaceAll("_", " ");
      const origin = String(row?.bundleEvidenceOrigin || "").replaceAll("_", " ");
      const originText = origin ? `, ${origin}` : "";
      return escapeHtml(`${title} @ ${company} (${cause}, ${quality}, ${action}${originText})`);
    })
    .join(" | ");
}

function formatDiagnosticsCopyButton(key) {
  return `
    <button
      class="btn clear-filters-btn admin-ops-diagnostics-copy-btn"
      type="button"
      data-ops-diagnostics-copy="${escapeHtml(key)}"
      title="Copy bounded diagnostics for this section"
    >Copy diagnostics</button>
  `;
}

function formatOpsFetcherMetricSection(section) {
  return `
    <section class="admin-ops-metrics-section admin-ops-metrics-section-${escapeHtml(section.key)}">
      <div class="admin-ops-metrics-section-head">
        <div>
          <h4>${escapeHtml(section.title)}</h4>
          <p>${escapeHtml(section.description)}</p>
        </div>
        ${section.diagnostics ? formatDiagnosticsCopyButton(section.key) : ""}
      </div>
      <div class="admin-ops-metrics-section-body">
        ${section.html}
      </div>
    </section>
  `;
}

function formatOpsMetricsDetails(summary, html, className = "") {
  return `
    <details class="admin-ops-metrics-details ${escapeHtml(className)}">
      <summary>${escapeHtml(summary)}</summary>
      <div class="admin-ops-metrics-details-body">
        ${html}
      </div>
    </details>
  `;
}

function formatOpsTaskLane(rows, diagnostics = null) {
  const laneRows = Array.isArray(rows) ? rows : [];
  const body = laneRows.map(row => {
    const status = String(row.lifecycleStatus || row.status || "unknown").trim();
    return `
      <div class="admin-ops-task-lane-card admin-ops-task-lane-card-${escapeHtml(row.type)}">
        <div class="admin-ops-task-lane-card-head">
          <strong>${escapeHtml(row.label)}</strong>
          <span class="admin-status-chip ${getRunStatusChipClass(status)}">${escapeHtml(status.replaceAll("_", " "))}</span>
        </div>
        <div class="admin-ops-task-lane-meta">${escapeHtml(row.hasRun ? (row.isLive ? `Running ${formatDuration(Number(row.elapsedMs || 0))}` : `Last ${formatDuration(Number(row.elapsedMs || 0))}`) : "No run yet")}</div>
        <div class="admin-ops-task-lane-summary">${escapeHtml(row.summary)}</div>
      </div>
    `;
  }).join("");
  return `
    <section class="admin-ops-task-lane" aria-label="Operations task status">
      <div class="admin-ops-task-lane-head">
        <div>
          <h4>Task Status</h4>
          <p>Compact read-only status for discovery, fetch, and sync.</p>
        </div>
        ${diagnostics ? formatDiagnosticsCopyButton("taskStatus") : ""}
      </div>
      <div class="admin-ops-task-lane-grid">${body}</div>
    </section>
  `;
}

function formatDedupReviewStateSummary(summary, readWarning = "") {
  const state = summary && typeof summary === "object" ? summary : {};
  const artifactPath = String(state?.artifactPath || "data/dedup-review-state.json");
  const status = String(state?.status || (readWarning ? "warning" : "ok")).replaceAll("_", " ");
  const warning = String(state?.readWarning || readWarning || "").replaceAll("_", " ");
  const reviewedPairs = Number(state?.reviewedPairCount || 0);
  const reviewedSafe = Number(state?.reviewedSafeCount || 0);
  const confirmedBlocking = Number(state?.confirmedBlockingCount || 0);
  const unresolvedBlocking = Number(state?.unresolvedBlockingCount || 0);
  return [
    `path ${artifactPath}`,
    `status ${status}`,
    `reviewed pairs ${reviewedPairs.toLocaleString()}`,
    `reviewed safe ${reviewedSafe.toLocaleString()}`,
    `confirmed blocking ${confirmedBlocking.toLocaleString()}`,
    `unresolved blocking ${unresolvedBlocking.toLocaleString()}`,
    warning ? `warning ${warning}` : ""
  ].filter(Boolean).join(", ");
}

function formatCurrentRunMergeExamples(rows, emptyText) {
  const examples = Array.isArray(rows) ? rows : [];
  if (!examples.length) return escapeHtml(emptyText);
  return examples
    .slice(0, 5)
    .map(row => {
      const title = String(row?.title || "Untitled");
      const company = String(row?.company || "Unknown company");
      const source = String(row?.incomingSource || "unknown");
      const reason = String(row?.mergeReason || "unknown").replaceAll("_", " ");
      const review = String(row?.recommendedReviewAction || "monitor").replaceAll("_", " ");
      const nonBlockingReason = String(row?.nonBlockingReason || "").replaceAll("_", " ");
      const suffix = nonBlockingReason ? `, ${nonBlockingReason}` : "";
      return escapeHtml(`${title} @ ${company} (${reason}, ${source}, ${review}${suffix})`);
    })
    .join(" | ");
}

function formatProviderStaticDisagreementCounts(disagreementCounts) {
  const counts = disagreementCounts && typeof disagreementCounts === "object" ? disagreementCounts : {};
  return [
    `total ${Number(counts?.total || 0).toLocaleString()}`,
    `current ${Number(counts?.currentRun || 0).toLocaleString()}`,
    `carried ${Number(counts?.carried || 0).toLocaleString()}`
  ].join(", ");
}

function formatProviderStaticDisagreementGateCounts(gateCounts) {
  const counts = gateCounts && typeof gateCounts === "object" ? gateCounts : {};
  return [
    `blocked ${Number(counts?.blocked || 0).toLocaleString()}`,
    `warning ${Number(counts?.warning || 0).toLocaleString()}`,
    `current blocked ${Number(counts?.currentRunBlocked || 0).toLocaleString()}`,
    `carried blocked ${Number(counts?.carriedBlocked || 0).toLocaleString()}`,
    `carried warning ${Number(counts?.carriedWarning || 0).toLocaleString()}`,
    `auto-safe ${Number(counts?.autoSafeWarning || 0).toLocaleString()}`,
    `reviewed safe ${Number(counts?.reviewedSafeWarning || 0).toLocaleString()}`,
    `confirmed blocking ${Number(counts?.confirmedBlocking || 0).toLocaleString()}`
  ].join(", ");
}

function formatProviderStaticTitleCompanyCollisionCounts(collisionCounts) {
  const counts = collisionCounts && typeof collisionCounts === "object" ? collisionCounts : {};
  return [
    `total ${Number(counts?.total || 0).toLocaleString()}`,
    `current ${Number(counts?.currentRun || 0).toLocaleString()}`,
    `carried ${Number(counts?.carried || 0).toLocaleString()}`
  ].join(", ");
}

function formatProviderStaticTitleCompanyCollisionAuditCounts(auditCounts) {
  const counts = auditCounts && typeof auditCounts === "object" ? auditCounts : {};
  return [
    `location pollution ${Number(counts?.carried_location_pollution || 0).toLocaleString()}`,
    `location variants ${Number(counts?.carried_location_variant || 0).toLocaleString()}`,
    `provider identity location conflicts ${Number(counts?.carried_provider_identity_location_conflict || 0).toLocaleString()}`,
    `possible real conflict ${Number(counts?.possible_real_multi_location_conflict || 0).toLocaleString()}`,
    `not carried ${Number(counts?.not_carried || 0).toLocaleString()}`,
    `unknown ${Number(counts?.unknown || 0).toLocaleString()}`
  ].join(", ");
}

function formatProviderStaticDisagreementClassificationCounts(classificationCounts) {
  const counts = classificationCounts && typeof classificationCounts === "object" ? classificationCounts : {};
  return [
    `same job/different URLs ${Number(counts?.same_job_different_urls || 0).toLocaleString()}`,
    `canonical/redirect ${Number(counts?.provider_redirect_or_canonical_url || 0).toLocaleString()}`,
    `static URL variant ${Number(counts?.static_parser_url_variant || 0).toLocaleString()}`,
    `title/company collision ${Number(counts?.title_company_collision || 0).toLocaleString()}`,
    `stale carried ${Number(counts?.stale_carried_bundle || 0).toLocaleString()}`,
    `manual review ${Number(counts?.needs_manual_review || 0).toLocaleString()}`
  ].join(", ");
}

function renderDedupReviewActionButtons(tableKey, rowIndex, showActions) {
  if (!showActions) return "";
  return `
    <div class="admin-inline-actions" data-dedup-review-actions="${escapeHtml(tableKey)}:${Number(rowIndex)}">
      <button type="button" class="admin-pill-button" title="Downgrade this exact disagreement from blocker to warning." data-dedup-review-action="reviewed_safe" data-dedup-review-table="${escapeHtml(tableKey)}" data-dedup-review-row="${Number(rowIndex)}">Safe duplicate</button>
      <button type="button" class="admin-pill-button" title="Keep this exact disagreement blocking and record that it was reviewed." data-dedup-review-action="confirmed_blocking" data-dedup-review-table="${escapeHtml(tableKey)}" data-dedup-review-row="${Number(rowIndex)}">Real blocker</button>
      <button type="button" class="admin-pill-button" title="Remove the manual decision and let the report classify it again." data-dedup-review-action="clear_review" data-dedup-review-table="${escapeHtml(tableKey)}" data-dedup-review-row="${Number(rowIndex)}">Reset review</button>
      <span class="admin-muted">Local review only: no merge, registry, source, or job data is changed.</span>
    </div>
  `;
}

function humanizeProviderStaticValue(value, fallback = "unknown") {
  const text = String(value || "").trim();
  return text ? text.replaceAll("_", " ") : fallback;
}

function formatProviderStaticList(values, limit = 2) {
  const items = Array.isArray(values) ? values.filter(Boolean).slice(0, limit) : [];
  return items.length ? items.join(" | ") : "none";
}

function providerStaticRecommendationLabel(row) {
  const reviewStatus = String(row?.dedupReviewStatus || "");
  if (reviewStatus === "reviewed_safe") return "Safe duplicate";
  if (reviewStatus === "confirmed_blocking") return "Real blocker";
  const recommendation = String(row?.operatorReviewRecommendation || "");
  if (recommendation === "safe_duplicate") return "Safe duplicate";
  if (recommendation === "real_blocker") return "Real blocker";
  if (recommendation === "needs_review") return "Needs review";
  const disposition = String(row?.disagreementGateDisposition || "");
  if (disposition === "warning") return "Safe duplicate";
  return "Needs review";
}

function providerStaticReasonLabel(row) {
  const reviewStatus = String(row?.dedupReviewStatus || "");
  if (reviewStatus === "reviewed_safe") return "Manually reviewed as safe.";
  if (reviewStatus === "confirmed_blocking") return "Manually confirmed as a real blocker.";
  const gateEvidence = Array.isArray(row?.disagreementGateEvidence) ? row.disagreementGateEvidence : [];
  if (gateEvidence.some(item => String(item || "").startsWith("auto_safe_"))) {
    return "Strong provider/static identity; safe URL variant.";
  }
  if (gateEvidence.some(item => String(item || "") === "carried_location_pollution")) {
    return "Historical location text pollution; warning only.";
  }
  const reason = String(row?.operatorReviewReason || "");
  const labels = {
    auto_safe_provider_static_variant: "Strong provider/static identity; safe URL variant.",
    carried_location_pollution_warning: "Historical location text pollution; warning only.",
    different_locations_same_title_company: "Same title/company appears across different locations.",
    manual_confirmed_blocking: "Manually confirmed as a real blocker.",
    manual_reviewed_safe: "Manually reviewed as safe.",
    warning_not_blocking: "Already warning-only.",
    static_parser_url_variant_blocked: "Looks like a static URL variant but lacks concrete shared job identity.",
    provider_redirect_or_canonical_url_blocked: "Looks like a canonical URL variant but lacks concrete shared job identity.",
    same_job_different_urls_blocked: "Provider and static have different URLs and need human review.",
    title_company_collision_blocked: "Title/company collision needs review.",
    needs_manual_review_blocked: "Evidence is incomplete or ambiguous."
  };
  return labels[reason] || humanizeProviderStaticValue(reason, "Needs human review.");
}

function providerStaticReviewStatus(row) {
  const status = humanizeProviderStaticValue(row?.dedupReviewStatus, "unreviewed");
  const updatedBy = String(row?.dedupReviewUpdatedBy || "");
  const updatedAt = String(row?.dedupReviewUpdatedAt || "");
  return `${status}${updatedBy ? ` by ${updatedBy}` : ""}${updatedAt ? ` at ${updatedAt}` : ""}`;
}

function formatProviderStaticRawEvidence(row) {
  const classificationEvidence = Array.isArray(row?.disagreementClassificationEvidence)
    ? row.disagreementClassificationEvidence
    : [];
  const gateEvidence = Array.isArray(row?.disagreementGateEvidence)
    ? row.disagreementGateEvidence
    : [];
  const disagreementEvidence = Array.isArray(row?.disagreementEvidence)
    ? row.disagreementEvidence
    : [];
  const auditEvidence = Array.isArray(row?.carriedLocationPollutionEvidence)
    ? row.carriedLocationPollutionEvidence
    : [];
  const raw = [
    `classification evidence ${classificationEvidence.slice(0, 8).join(", ").replaceAll("_", " ") || "none"}`,
    `gate evidence ${gateEvidence.slice(0, 8).join(", ").replaceAll("_", " ") || "none"}`,
    `disagreement evidence ${disagreementEvidence.slice(0, 8).join(", ").replaceAll("_", " ") || "none"}`,
    auditEvidence.length ? `audit evidence ${auditEvidence.slice(0, 8).join(", ").replaceAll("_", " ")}` : ""
  ].filter(Boolean).join("; ");
  return `
    <details class="admin-dedup-raw-evidence">
      <summary>Raw evidence</summary>
      <div>${escapeHtml(raw)}</div>
    </details>
  `;
}

function formatProviderStaticEvidenceBlock(label, sources, ids, urls) {
  const rows = [
    ["Source", formatProviderStaticList(sources)],
    ["Job IDs", formatProviderStaticList(ids)],
    ["URLs", formatProviderStaticList(urls)]
  ];
  return `
    <section class="admin-dedup-provider-static-evidence-block">
      <h5>${escapeHtml(label)}</h5>
      ${rows.map(([rowLabel, value]) => `
        <div class="admin-dedup-provider-static-evidence-row">
          <span>${escapeHtml(rowLabel)}</span>
          <code>${escapeHtml(value)}</code>
        </div>
      `).join("")}
    </section>
  `;
}

function isProviderStaticBlockedRow(row) {
  return String(row?.disagreementGateDisposition || "").toLowerCase() === "blocked";
}

function isProviderStaticAutoSafeVariantRow(row) {
  const disposition = String(row?.disagreementGateDisposition || "").toLowerCase();
  if (disposition !== "warning") return false;
  const reviewStatus = String(row?.dedupReviewStatus || "").toLowerCase();
  if (reviewStatus === "confirmed_blocking") return false;
  const gateEvidence = Array.isArray(row?.disagreementGateEvidence) ? row.disagreementGateEvidence : [];
  const hasAutoSafeEvidence = gateEvidence.some(item => String(item || "").startsWith("auto_safe_"));
  const recommendation = String(row?.operatorReviewRecommendation || "").toLowerCase();
  const reason = String(row?.operatorReviewReason || "").toLowerCase();
  return hasAutoSafeEvidence || recommendation === "safe_duplicate" || reason === "auto_safe_provider_static_variant";
}

function visibleProviderStaticRows(rows, limit = 5) {
  const allRows = Array.isArray(rows) ? rows : [];
  const candidateRows = allRows.filter(row => !isProviderStaticAutoSafeVariantRow(row));
  const cappedWarningSlots = Math.max(0, Number(limit) || 0);
  const blockedCount = candidateRows.filter(isProviderStaticBlockedRow).length;
  const warningLimit = Math.max(0, cappedWarningSlots - blockedCount);
  let warningsAdded = 0;
  return candidateRows.filter(row => {
    if (isProviderStaticBlockedRow(row)) return true;
    if (warningsAdded >= warningLimit) return false;
    warningsAdded += 1;
    return true;
  });
}

function formatProviderStaticGuidedRows(rows, emptyText, options = {}) {
  const disagreementRows = Array.isArray(rows) ? rows : [];
  if (!disagreementRows.length) return escapeHtml(emptyText);
  const showActions = typeof options?.onReviewAction === "function";
  const tableKey = String(options?.tableKey || "providerStatic");
  const visibleRows = visibleProviderStaticRows(disagreementRows);
  const hiddenSafeCount = disagreementRows.filter(isProviderStaticAutoSafeVariantRow).length;
  const hiddenSafeSummary = hiddenSafeCount
    ? `<div class="admin-muted">Hidden safe provider/static URL variants: ${Number(hiddenSafeCount).toLocaleString()}.</div>`
    : "";
  if (!visibleRows.length) {
    return `${escapeHtml(emptyText)}${hiddenSafeSummary ? ` ${hiddenSafeSummary}` : ""}`;
  }
  const cards = visibleRows
    .map((row, rowIndex) => {
      const title = String(row?.title || "Untitled");
      const company = String(row?.company || "Unknown company");
      const origin = humanizeProviderStaticValue(row?.bundleEvidenceOrigin);
      const quality = humanizeProviderStaticValue(row?.identityQuality);
      const disposition = humanizeProviderStaticValue(row?.disagreementGateDisposition, "blocked");
      const providerSources = Array.isArray(row?.providerSources) ? row.providerSources : [];
      const staticSources = Array.isArray(row?.staticSources) ? row.staticSources : [];
      const providerUrls = Array.isArray(row?.providerUrls) ? row.providerUrls : [];
      const staticUrls = Array.isArray(row?.staticUrls) ? row.staticUrls : [];
      const providerIds = Array.isArray(row?.providerSourceJobIds) ? row.providerSourceJobIds : [];
      const staticIds = Array.isArray(row?.staticSourceJobIds) ? row.staticSourceJobIds : [];
      const tokens = Array.isArray(row?.concreteSharedIdentifierTokens)
        ? row.concreteSharedIdentifierTokens
        : Array.isArray(row?.sharedIdentifierTokens)
          ? row.sharedIdentifierTokens
          : [];
      const locations = Array.isArray(row?.sampleLocations) ? row.sampleLocations : [];
      const classification = humanizeProviderStaticValue(row?.disagreementClassification, "needs manual review");
      const hint = humanizeProviderStaticValue(row?.collisionReviewHint, "");
      const audit = humanizeProviderStaticValue(row?.carriedLocationPollutionAudit, "");
      const statusChips = [
        `gate ${disposition}`,
        `review ${providerStaticReviewStatus(row)}`,
        `origin ${origin}`,
        `classification ${classification}`,
        hint ? `hint ${hint}` : "",
        audit ? `audit ${audit}` : "",
        `quality ${quality}`,
        `sources ${Number(row?.sourceBundleCount || 0).toLocaleString()}`,
        `locations ${Number(row?.distinctLocationCount || 0).toLocaleString()}${locations.length ? ` (${locations.slice(0, 2).join(" | ")})` : ""}`,
        tokens.length ? `shared job token ${tokens.slice(0, 2).join(", ")}` : "shared job token none"
      ].filter(Boolean);
      return `
        <article class="admin-dedup-provider-static-card">
          <div class="admin-dedup-provider-static-card-head">
            <div>
              <h5>${escapeHtml(title)}</h5>
              <div class="admin-dedup-provider-static-company">${escapeHtml(company)}</div>
            </div>
            <div class="admin-dedup-provider-static-recommendation">
              <span>${escapeHtml(providerStaticRecommendationLabel(row))}</span>
              <p>${escapeHtml(providerStaticReasonLabel(row))}</p>
            </div>
          </div>
          <div class="admin-dedup-provider-static-chips">
            ${statusChips.map(chip => `<span>${escapeHtml(chip)}</span>`).join("")}
          </div>
          <div class="admin-dedup-provider-static-evidence-grid">
            ${formatProviderStaticEvidenceBlock("Provider evidence", providerSources, providerIds, providerUrls)}
            ${formatProviderStaticEvidenceBlock("Static evidence", staticSources, staticIds, staticUrls)}
          </div>
          ${formatProviderStaticRawEvidence(row)}
          ${renderDedupReviewActionButtons(tableKey, rowIndex, showActions)}
        </article>
      `;
    })
    .join("");
  return `
    ${hiddenSafeSummary}
    <div class="admin-dedup-provider-static-list">${cards}</div>
  `;
}

function formatProviderStaticDisagreementRows(rows, emptyText, options = {}) {
  return formatProviderStaticGuidedRows(rows, emptyText, {
    ...options,
    tableKey: String(options?.tableKey || "providerStatic")
  });
}

function formatProviderStaticTitleCompanyCollisionRows(rows, emptyText, options = {}) {
  return formatProviderStaticGuidedRows(rows, emptyText, {
    ...options,
    tableKey: String(options?.tableKey || "providerStaticTitleCompany")
  });
}

function formatDedupIdentityQualityCounts(qualityCounts) {
  const counts = qualityCounts && typeof qualityCounts === "object" ? qualityCounts : {};
  return [
    `provider ID ${Number(counts?.provider_id_strong || 0).toLocaleString()}`,
    `detail URL ${Number(counts?.shared_detail_url_strong || 0).toLocaleString()}`,
    `listing URL weak ${Number(counts?.shared_listing_url_weak || 0).toLocaleString()}`,
    `same-host URLs weak ${Number(counts?.many_urls_same_host_weak || 0).toLocaleString()}`,
    `many-host URLs weak ${Number(counts?.many_urls_many_hosts_weak || 0).toLocaleString()}`,
    `other source ID ${Number(counts?.other_source_id_untrusted || 0).toLocaleString()}`,
    `missing ${Number(counts?.missing_identity || 0).toLocaleString()}`,
    `unknown ${Number(counts?.unknown || 0).toLocaleString()}`
  ].join(", ");
}

function formatDedupNonProviderIdentityProvenanceCounts(provenanceCounts) {
  const counts = provenanceCounts && typeof provenanceCounts === "object" ? provenanceCounts : {};
  return [
    `google sheets ${Number(counts?.google_sheets_row_identity || 0).toLocaleString()}`,
    `URL-derived ${Number(counts?.url_derived_identity || 0).toLocaleString()}`,
    `category/directory ${Number(counts?.category_or_directory_identity || 0).toLocaleString()}`,
    `opaque other ${Number(counts?.opaque_other_source_identity || 0).toLocaleString()}`,
    `mixed ${Number(counts?.mixed_non_provider_identity || 0).toLocaleString()}`,
    `none ${Number(counts?.none || 0).toLocaleString()}`,
    `unknown ${Number(counts?.unknown || 0).toLocaleString()}`
  ].join(", ");
}

function formatDedupGoogleSheetsBundleShapeCounts(shapeCounts) {
  const counts = shapeCounts && typeof shapeCounts === "object" ? shapeCounts : {};
  return [
    `role/category ${Number(counts?.role_category_bucket || 0).toLocaleString()}`,
    `company role family ${Number(counts?.company_role_family || 0).toLocaleString()}`,
    `single-location URLs ${Number(counts?.single_location_many_urls || 0).toLocaleString()}`,
    `multi-location URLs ${Number(counts?.multi_location_many_urls || 0).toLocaleString()}`,
    `row collision ${Number(counts?.spreadsheet_row_collision || 0).toLocaleString()}`,
    `not sheets ${Number(counts?.not_google_sheets || 0).toLocaleString()}`,
    `unknown ${Number(counts?.unknown || 0).toLocaleString()}`
  ].join(", ");
}

function formatDedupGoogleSheetsRoleBucketAuditCounts(auditCounts) {
  const counts = auditCounts && typeof auditCounts === "object" ? auditCounts : {};
  return [
    `spreadsheet category ${Number(counts?.likely_spreadsheet_category_bucket || 0).toLocaleString()}`,
    `manual role review ${Number(counts?.role_family_needs_manual_review || 0).toLocaleString()}`,
    `detail URLs ${Number(counts?.job_detail_urls_same_role || 0).toLocaleString()}`,
    `listing/search ${Number(counts?.listing_or_search_url_bucket || 0).toLocaleString()}`,
    `parser normalized ${Number(counts?.parser_normalized_role_title || 0).toLocaleString()}`,
    `not sheets role ${Number(counts?.not_google_sheets_role_bucket || 0).toLocaleString()}`,
    `unknown ${Number(counts?.unknown || 0).toLocaleString()}`
  ].join(", ");
}

function formatDedupGoogleSheetsRoleBucketAuditSummary(audit) {
  const summary = audit && typeof audit === "object" ? audit : {};
  const classificationCounts = summary?.classificationCounts && typeof summary.classificationCounts === "object"
    ? summary.classificationCounts
    : {};
  const countText = [
    `total ${Number(summary?.totalRoleBucketCount || 0).toLocaleString()}`,
    `current-run ${Number(summary?.currentRunRoleBucketCount || 0).toLocaleString()}`,
    `carried ${Number(summary?.carriedHistoricalRoleBucketCount || 0).toLocaleString()}`,
    `guard-blocked different URL ${Number(summary?.blockedByDifferentPrimaryUrlCount || 0).toLocaleString()}`,
    `allowed same URL ${Number(summary?.allowedSamePrimaryUrlCount || 0).toLocaleString()}`,
    `historical ${Number(summary?.likelyHistoricalCollisionCount || 0).toLocaleString()}`,
    `parser/category ${Number(summary?.likelyParserCategoryBucketCount || 0).toLocaleString()}`,
    `unresolved ${Number(summary?.unresolvedRoleBucketCount || 0).toLocaleString()}`
  ].join(", ");
  const classificationText = [
    `fixed by guard ${Number(classificationCounts?.fixed_by_generic_role_guard || 0).toLocaleString()}`,
    `allowed same URL ${Number(classificationCounts?.allowed_same_primary_url || 0).toLocaleString()}`,
    `historical carried ${Number(classificationCounts?.historical_carried_bundle || 0).toLocaleString()}`,
    `unresolved current-run ${Number(classificationCounts?.unresolved_current_run_role_bucket || 0).toLocaleString()}`,
    `parser/category noise ${Number(classificationCounts?.parser_or_sheet_category_noise || 0).toLocaleString()}`,
    `needs narrow guard ${Number(classificationCounts?.needs_narrow_dedup_guard || 0).toLocaleString()}`
  ].join(", ");
  const examples = Array.isArray(summary?.examples) ? summary.examples.slice(0, 5) : [];
  const exampleHtml = examples.length
    ? `
      <table class="admin-dedup-evidence-table">
        <thead><tr><th>Class</th><th>Title</th><th>Company</th><th>Origin</th><th>Evidence</th></tr></thead>
        <tbody>${examples.map(row => {
          const classification = String(row?.classification || "unknown").replaceAll("_", " ");
          const title = String(row?.title || row?.targetTitle || "Untitled");
          const company = String(row?.company || row?.targetCompany || "Unknown company");
          const origin = String(row?.bundleEvidenceOrigin || "unknown").replaceAll("_", " ");
          const evidence = Array.isArray(row?.evidence) ? row.evidence.slice(0, 6).join(", ").replaceAll("_", " ") : "none";
          return `
            <tr>
              <td>${escapeHtml(classification)}</td>
              <td>${escapeHtml(title)}</td>
              <td>${escapeHtml(company)}</td>
              <td>${escapeHtml(origin)}</td>
              <td>${escapeHtml(evidence)}</td>
            </tr>
          `;
        }).join("")}</tbody>
      </table>
    `
    : escapeHtml("No Google Sheets role-bucket audit examples.");
  return `
    <div><strong>Summary</strong>: ${escapeHtml(countText)}</div>
    <div><strong>Classifications</strong>: ${escapeHtml(classificationText)}</div>
    ${exampleHtml}
  `;
}

function formatDedupGoogleSheetsBucketIntentCounts(intentCounts) {
  const counts = intentCounts && typeof intentCounts === "object" ? intentCounts : {};
  return [
    `taxonomy bucket ${Number(counts?.likely_spreadsheet_taxonomy_bucket || 0).toLocaleString()}`,
    `possible role family ${Number(counts?.possible_role_family || 0).toLocaleString()}`,
    `weak title/company ${Number(counts?.weak_title_company_grouping || 0).toLocaleString()}`,
    `listing/search ${Number(counts?.listing_or_search_bucket || 0).toLocaleString()}`,
    `parser normalized ${Number(counts?.parser_normalized_bucket || 0).toLocaleString()}`,
    `not sheets ${Number(counts?.not_google_sheets_bucket || 0).toLocaleString()}`,
    `unknown ${Number(counts?.unknown || 0).toLocaleString()}`
  ].join(", ");
}

function formatDedupGoogleSheetsWeakGroupingAuditCounts(auditCounts) {
  const counts = auditCounts && typeof auditCounts === "object" ? auditCounts : {};
  return [
    `role detail URLs ${Number(counts?.role_bucket_detail_url_grouping || 0).toLocaleString()}`,
    `role listing/search ${Number(counts?.role_bucket_listing_grouping || 0).toLocaleString()}`,
    `single-token title ${Number(counts?.single_token_title_many_urls || 0).toLocaleString()}`,
    `two-token title ${Number(counts?.two_token_title_many_urls || 0).toLocaleString()}`,
    `concrete title ${Number(counts?.concrete_title_many_urls || 0).toLocaleString()}`,
    `parser pollution ${Number(counts?.parser_pollution_grouping || 0).toLocaleString()}`,
    `not weak sheets ${Number(counts?.not_weak_google_sheets_grouping || 0).toLocaleString()}`,
    `unknown ${Number(counts?.unknown || 0).toLocaleString()}`
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
      const identityQuality = String(row?.identityQuality || "unknown").replaceAll("_", " ");
      const nonProviderProvenance = String(row?.nonProviderIdentityProvenance || "unknown").replaceAll("_", " ");
      const googleSheetsShape = String(row?.googleSheetsBundleShape || "unknown").replaceAll("_", " ");
      const googleSheetsAudit = String(row?.googleSheetsRoleBucketAudit || "unknown").replaceAll("_", " ");
      const googleSheetsIntent = String(row?.googleSheetsBucketIntent || "unknown").replaceAll("_", " ");
      const googleSheetsWeakAudit = String(row?.googleSheetsWeakGroupingAudit || "unknown").replaceAll("_", " ");
      const outlierReason = String(row?.outlierReason || "unknown").replaceAll("_", " ");
      const suspectedCause = String(row?.suspectedCause || "unknown").replaceAll("_", " ");
      const caveats = Array.isArray(row?.identityCaveats) ? row.identityCaveats : [];
      const caveatText = caveats.length ? caveats.join(", ").replaceAll("_", " ") : "none";
      const causeEvidence = Array.isArray(row?.causeEvidence) ? row.causeEvidence : [];
      const causeText = causeEvidence.length ? causeEvidence.slice(0, 5).join(", ").replaceAll("_", " ") : "none";
      const qualityEvidence = Array.isArray(row?.identityQualityEvidence) ? row.identityQualityEvidence : [];
      const qualityText = qualityEvidence.length ? qualityEvidence.slice(0, 5).join(", ").replaceAll("_", " ") : "none";
      const provenanceEvidence = Array.isArray(row?.nonProviderIdentityEvidence) ? row.nonProviderIdentityEvidence : [];
      const provenanceText = provenanceEvidence.length ? provenanceEvidence.slice(0, 5).join(", ").replaceAll("_", " ") : "none";
      const googleSheetsEvidence = Array.isArray(row?.googleSheetsBundleEvidence) ? row.googleSheetsBundleEvidence : [];
      const googleSheetsText = googleSheetsEvidence.length ? googleSheetsEvidence.slice(0, 5).join(", ").replaceAll("_", " ") : "none";
      const googleSheetsAuditEvidence = Array.isArray(row?.googleSheetsRoleBucketAuditEvidence) ? row.googleSheetsRoleBucketAuditEvidence : [];
      const googleSheetsAuditText = googleSheetsAuditEvidence.length ? googleSheetsAuditEvidence.slice(0, 5).join(", ").replaceAll("_", " ") : "none";
      const googleSheetsIntentEvidence = Array.isArray(row?.googleSheetsBucketIntentEvidence) ? row.googleSheetsBucketIntentEvidence : [];
      const googleSheetsIntentText = googleSheetsIntentEvidence.length ? googleSheetsIntentEvidence.slice(0, 5).join(", ").replaceAll("_", " ") : "none";
      const googleSheetsWeakEvidence = Array.isArray(row?.googleSheetsWeakGroupingEvidence) ? row.googleSheetsWeakGroupingEvidence : [];
      const googleSheetsWeakText = googleSheetsWeakEvidence.length ? googleSheetsWeakEvidence.slice(0, 5).join(", ").replaceAll("_", " ") : "none";
      const sources = Array.isArray(row?.sampleSources) ? row.sampleSources : Array.isArray(row?.sources) ? row.sources : [];
      const sourceText = sources.length ? sources.slice(0, 3).join(" | ") : "none";
      const detail = `${suspectedCause}; ${identityShape}; quality ${identityQuality}; provenance ${nonProviderProvenance}; sheets ${googleSheetsShape}; sheets audit ${googleSheetsAudit}; sheets intent ${googleSheetsIntent}; sheets weak audit ${googleSheetsWeakAudit}; ${outlierReason}; caveats ${caveatText}; cause evidence ${causeText}; identity evidence ${qualityText}; provenance evidence ${provenanceText}; sheets evidence ${googleSheetsText}; sheets audit evidence ${googleSheetsAuditText}; sheets intent evidence ${googleSheetsIntentText}; sheets weak evidence ${googleSheetsWeakText}; sources ${sourceText}`;
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

function buildDedupListsContent(metrics, options = {}) {
  const latest = metrics?.latestRun || {};
  const dedupEvidence = latest?.dedupEvidence && typeof latest.dedupEvidence === "object" ? latest.dedupEvidence : {};
  const dedupReviewStateSummary = latest?.dedupReviewStateSummary && typeof latest.dedupReviewStateSummary === "object"
    ? latest.dedupReviewStateSummary
    : {};
  const dedupReviewStateReadWarning = String(latest?.dedupReviewStateReadWarning || "");
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
  const identityQualityCounts = dedupEvidence?.identityQualityCounts && typeof dedupEvidence.identityQualityCounts === "object"
    ? dedupEvidence.identityQualityCounts
    : {};
  const nonProviderIdentityProvenanceCounts = dedupEvidence?.nonProviderIdentityProvenanceCounts && typeof dedupEvidence.nonProviderIdentityProvenanceCounts === "object"
    ? dedupEvidence.nonProviderIdentityProvenanceCounts
    : {};
  const googleSheetsBundleShapeCounts = dedupEvidence?.googleSheetsBundleShapeCounts && typeof dedupEvidence.googleSheetsBundleShapeCounts === "object"
    ? dedupEvidence.googleSheetsBundleShapeCounts
    : {};
  const googleSheetsRoleBucketAuditCounts = dedupEvidence?.googleSheetsRoleBucketAuditCounts && typeof dedupEvidence.googleSheetsRoleBucketAuditCounts === "object"
    ? dedupEvidence.googleSheetsRoleBucketAuditCounts
    : {};
  const googleSheetsRoleBucketAudit = dedupEvidence?.googleSheetsRoleBucketAudit && typeof dedupEvidence.googleSheetsRoleBucketAudit === "object"
    ? dedupEvidence.googleSheetsRoleBucketAudit
    : {};
  const googleSheetsBucketIntentCounts = dedupEvidence?.googleSheetsBucketIntentCounts && typeof dedupEvidence.googleSheetsBucketIntentCounts === "object"
    ? dedupEvidence.googleSheetsBucketIntentCounts
    : {};
  const googleSheetsWeakGroupingAuditCounts = dedupEvidence?.googleSheetsWeakGroupingAuditCounts && typeof dedupEvidence.googleSheetsWeakGroupingAuditCounts === "object"
    ? dedupEvidence.googleSheetsWeakGroupingAuditCounts
    : {};
  const reviewQueueCounts = dedupEvidence?.reviewQueueCounts && typeof dedupEvidence.reviewQueueCounts === "object"
    ? dedupEvidence.reviewQueueCounts
    : {};
  const reviewQueueCauseCounts = dedupEvidence?.reviewQueueCauseCounts && typeof dedupEvidence.reviewQueueCauseCounts === "object"
    ? dedupEvidence.reviewQueueCauseCounts
    : {};
  const dedupAuditGate = dedupEvidence?.dedupAuditGate && typeof dedupEvidence.dedupAuditGate === "object"
    ? dedupEvidence.dedupAuditGate
    : {};
  const providerStaticDisagreementCounts = dedupEvidence?.providerStaticDisagreementCounts && typeof dedupEvidence.providerStaticDisagreementCounts === "object"
    ? dedupEvidence.providerStaticDisagreementCounts
    : {};
  const providerStaticDisagreementGateCounts = dedupEvidence?.providerStaticDisagreementGateCounts && typeof dedupEvidence.providerStaticDisagreementGateCounts === "object"
    ? dedupEvidence.providerStaticDisagreementGateCounts
    : {};
  const providerStaticDisagreementClassificationCounts = dedupEvidence?.providerStaticDisagreementClassificationCounts && typeof dedupEvidence.providerStaticDisagreementClassificationCounts === "object"
    ? dedupEvidence.providerStaticDisagreementClassificationCounts
    : {};
  const providerStaticTitleCompanyCollisionCounts = dedupEvidence?.providerStaticTitleCompanyCollisionCounts && typeof dedupEvidence.providerStaticTitleCompanyCollisionCounts === "object"
    ? dedupEvidence.providerStaticTitleCompanyCollisionCounts
    : {};
  const providerStaticTitleCompanyCollisionAuditCounts = dedupEvidence?.providerStaticTitleCompanyCollisionAuditCounts && typeof dedupEvidence.providerStaticTitleCompanyCollisionAuditCounts === "object"
    ? dedupEvidence.providerStaticTitleCompanyCollisionAuditCounts
    : {};
  const providerStaticDisagreementRows = Array.isArray(dedupEvidence?.providerStaticDisagreementExamples)
    ? dedupEvidence.providerStaticDisagreementExamples
    : [];
  const providerStaticTitleCompanyCollisionRows = Array.isArray(dedupEvidence?.providerStaticTitleCompanyCollisionExamples)
    ? dedupEvidence.providerStaticTitleCompanyCollisionExamples
    : [];
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
  const providerStaticDisagreementSummary = formatProviderStaticDisagreementRows(
    providerStaticDisagreementRows,
    "No provider/static disagreement examples in the latest fetch report.",
    { onReviewAction: options?.onDedupReviewAction, tableKey: "providerStatic" }
  );
  const providerStaticTitleCompanyCollisionSummary = formatProviderStaticTitleCompanyCollisionRows(
    providerStaticTitleCompanyCollisionRows,
    "No provider/static title/company collision examples in the latest fetch report.",
    { onReviewAction: options?.onDedupReviewAction, tableKey: "providerStaticTitleCompany" }
  );
  const supportingHtml = `
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup current-run merge examples</strong>: ${formatCurrentRunMergeExamples(dedupEvidence?.currentRunMergeExamples, "No current-run merge examples.")}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup provider/static disagreements</strong>: ${escapeHtml(formatProviderStaticDisagreementCounts(providerStaticDisagreementCounts))}. Gate: ${escapeHtml(formatProviderStaticDisagreementGateCounts(providerStaticDisagreementGateCounts))}. Classifications: ${escapeHtml(formatProviderStaticDisagreementClassificationCounts(providerStaticDisagreementClassificationCounts))}. ${providerStaticDisagreementSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup provider/static title-company collisions</strong>: ${escapeHtml(formatProviderStaticTitleCompanyCollisionCounts(providerStaticTitleCompanyCollisionCounts))}. Audit: ${escapeHtml(formatProviderStaticTitleCompanyCollisionAuditCounts(providerStaticTitleCompanyCollisionAuditCounts))}. ${providerStaticTitleCompanyCollisionSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup carried bundle examples</strong>: ${formatDedupAuditGateExamples(dedupEvidence?.carriedBundleExamples, "No carried bundle examples.")}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup source composition</strong>: ${escapeHtml(formatDedupSourceClasses(sourceBundleComposition))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup risk reasons</strong>: ${escapeHtml(formatDedupRiskReasonCounts(riskReasonCounts))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup outlier reasons</strong>: ${escapeHtml(formatDedupOutlierReasonCounts(outlierReasonCounts))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup identity shapes</strong>: ${escapeHtml(formatDedupIdentityShapeCounts(identityShapeCounts))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup identity quality</strong>: ${escapeHtml(formatDedupIdentityQualityCounts(identityQualityCounts))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup non-provider provenance</strong>: ${escapeHtml(formatDedupNonProviderIdentityProvenanceCounts(nonProviderIdentityProvenanceCounts))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup Google Sheets bundle shapes</strong>: ${escapeHtml(formatDedupGoogleSheetsBundleShapeCounts(googleSheetsBundleShapeCounts))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup Google Sheets role-bucket audit</strong>: ${escapeHtml(formatDedupGoogleSheetsRoleBucketAuditCounts(googleSheetsRoleBucketAuditCounts))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup Google Sheets role-bucket audit summary</strong>: ${formatDedupGoogleSheetsRoleBucketAuditSummary(googleSheetsRoleBucketAudit)}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup Google Sheets bucket intent</strong>: ${escapeHtml(formatDedupGoogleSheetsBucketIntentCounts(googleSheetsBucketIntentCounts))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup Google Sheets weak grouping audit</strong>: ${escapeHtml(formatDedupGoogleSheetsWeakGroupingAuditCounts(googleSheetsWeakGroupingAuditCounts))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup action queue</strong>: ${escapeHtml(formatDedupReviewQueueCounts(reviewQueueCounts))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup diagnostic causes</strong>: ${escapeHtml(formatDedupReviewQueueCauseCounts(reviewQueueCauseCounts))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Top merged jobs</strong>: ${topMergedSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Top source-bundle outliers</strong>: ${topOutlierSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup review examples</strong>: ${reviewQueueSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Risky merge examples</strong>: ${riskyMergeSummary}</div>
  `;
  return {
    html: `
      <section class="admin-ops-metrics-section admin-ops-metrics-section-dedup">
        <div class="admin-ops-metrics-section-head">
          <div>
            <h4>Dedup Lists</h4>
            <p>Read-only gate, review-state, and blocker evidence before lifecycle UX.</p>
          </div>
        </div>
        <div class="admin-ops-metrics-section-body">
          <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup evidence</strong>: read-only diagnostics. Current-run merges by reason: primary URL ${Number(mergeReasonCounts?.primaryUrl || 0).toLocaleString()}, secondary key ${Number(mergeReasonCounts?.secondaryKey || 0).toLocaleString()}, known mirror pair ${Number(mergeReasonCounts?.knownMirrorPair || 0).toLocaleString()}, social key ${Number(mergeReasonCounts?.socialKey || 0).toLocaleString()}, sparse identity ${Number(mergeReasonCounts?.sparseIdentity || 0).toLocaleString()}, unknown ${Number(mergeReasonCounts?.unknown || 0).toLocaleString()}. Carried source-bundle collision rows: ${Number(dedupEvidence?.sourceBundleCollisionCount || 0).toLocaleString()}.</div>
          ${formatDedupAuditGateCard(dedupAuditGate)}
          <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup review-state</strong>: ${escapeHtml(formatDedupReviewStateSummary(dedupReviewStateSummary, dedupReviewStateReadWarning))}</div>
          ${formatOpsMetricsDetails("Dedup supporting diagnostics", supportingHtml, "admin-ops-dedup-details")}
        </div>
      </section>
    `,
    rowGroups: {
      providerStatic: visibleProviderStaticRows(providerStaticDisagreementRows),
      providerStaticTitleCompany: visibleProviderStaticRows(providerStaticTitleCompanyCollisionRows)
    }
  };
}

function wireDedupReviewActions(container, rowGroups, onDedupReviewAction) {
  if (typeof onDedupReviewAction !== "function") return;
  container.querySelectorAll("[data-dedup-review-action]").forEach(button => {
    button.addEventListener("click", () => {
      const action = String(button.getAttribute("data-dedup-review-action") || "");
      const tableKey = String(button.getAttribute("data-dedup-review-table") || "");
      const rowIndex = Number(button.getAttribute("data-dedup-review-row") || -1);
      const row = Array.isArray(rowGroups?.[tableKey]) ? rowGroups[tableKey][rowIndex] : null;
      if (!row || !action) return;
      onDedupReviewAction(row, action);
    });
  });
}

export function renderAdminOpsDedupLists(dedupEl, metrics, options = {}) {
  if (!dedupEl) return;
  const latest = metrics?.latestRun || {};
  const canPatchInPlace = Boolean(dedupEl && dedupEl.dataset);
  const signature = stableOpsSignature({
    dedupEvidence: latest?.dedupEvidence || {},
    dedupReviewStateSummary: latest?.dedupReviewStateSummary || {},
    dedupReviewStateReadWarning: String(latest?.dedupReviewStateReadWarning || "")
  });
  if (canPatchInPlace && dedupEl.dataset.opsDedupListsSig === signature) return;
  if (canPatchInPlace) dedupEl.dataset.opsDedupListsSig = signature;
  const content = buildDedupListsContent(metrics, options);
  dedupEl.innerHTML = content.html;
  wireDedupReviewActions(dedupEl, content.rowGroups, options?.onDedupReviewAction);
}

export function renderAdminOpsFetcherMetrics(metricsEl, metrics, failureSummary = null, options = {}) {
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
    dedupReviewStateSummary: latest?.dedupReviewStateSummary || {},
    dedupReviewStateReadWarning: String(latest?.dedupReviewStateReadWarning || ""),
    providerCoverage: latest?.providerCoverage || {},
    providerStaticOverlap: latest?.providerStaticOverlap || {},
    staticSuppressionPolicy: latest?.staticSuppressionPolicy || {},
    redundantStaticProposals: latest?.redundantStaticProposals || {},
    conservativeStaticCleanupProposals: latest?.conservativeStaticCleanupProposals || {},
    sourcePolicyRecommendationExport: latest?.sourcePolicyRecommendationExport || {},
    frontendPerfCounters: metrics?.frontendPerfCounters || {},
    runModel: options?.runModel || {},
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
  const dedupReviewStateSummary = latest?.dedupReviewStateSummary && typeof latest.dedupReviewStateSummary === "object"
    ? latest.dedupReviewStateSummary
    : {};
  const dedupReviewStateReadWarning = String(latest?.dedupReviewStateReadWarning || "");
  const providerCoverage = latest?.providerCoverage && typeof latest.providerCoverage === "object" ? latest.providerCoverage : {};
  const providerStaticOverlap = latest?.providerStaticOverlap && typeof latest.providerStaticOverlap === "object" ? latest.providerStaticOverlap : {};
  const staticSuppressionPolicy = latest?.staticSuppressionPolicy && typeof latest.staticSuppressionPolicy === "object" ? latest.staticSuppressionPolicy : {};
  const redundantStaticProposals = latest?.redundantStaticProposals && typeof latest.redundantStaticProposals === "object" ? latest.redundantStaticProposals : {};
  const conservativeStaticCleanupProposals = latest?.conservativeStaticCleanupProposals && typeof latest.conservativeStaticCleanupProposals === "object" ? latest.conservativeStaticCleanupProposals : {};
  const sourcePolicyRecommendationExport = latest?.sourcePolicyRecommendationExport && typeof latest.sourcePolicyRecommendationExport === "object" ? latest.sourcePolicyRecommendationExport : {};
  const frontendPerfCounters = metrics?.frontendPerfCounters && typeof metrics.frontendPerfCounters === "object"
    ? metrics.frontendPerfCounters
    : {};
  const frontendPerfCounterRows = Object.entries(frontendPerfCounters)
    .filter(([, value]) => value && typeof value === "object")
    .sort((left, right) => Number(right[1]?.p95Ms || 0) - Number(left[1]?.p95Ms || 0))
    .slice(0, 8);
  const frontendPerfSummary = frontendPerfCounterRows.length
    ? frontendPerfCounterRows.map(([key, row]) => (
      `${escapeHtml(key)}: p95 ${formatDuration(Number(row?.p95Ms || 0))}, p50 ${formatDuration(Number(row?.p50Ms || 0))}, count ${Number(row?.count || 0).toLocaleString()}`
    )).join("; ")
    : "No frontend fetch/render counter samples yet.";
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
  const cleanupProposalReadySummary = formatConservativeCleanupProposalRows(
    conservativeStaticCleanupProposals?.proposalReadyExamples || conservativeStaticCleanupProposals?.proposals,
    "No proposal-ready cleanup pairs yet."
  );
  const cleanupBlockedSummary = formatConservativeCleanupBlockedRows(
    conservativeStaticCleanupProposals?.blockedExamples || conservativeStaticCleanupProposals?.blockedCandidates,
    "No blocked cleanup candidates."
  );
  const cleanupBlockedReasonSummary = formatConservativeCleanupReasonCounts(
    conservativeStaticCleanupProposals?.blockedReasonCounts
  );
  const cleanupFreshnessSummary = formatConservativeCleanupFreshnessSummary(
    conservativeStaticCleanupProposals
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
  const identityQualityCounts = dedupEvidence?.identityQualityCounts && typeof dedupEvidence.identityQualityCounts === "object"
    ? dedupEvidence.identityQualityCounts
    : {};
  const nonProviderIdentityProvenanceCounts = dedupEvidence?.nonProviderIdentityProvenanceCounts && typeof dedupEvidence.nonProviderIdentityProvenanceCounts === "object"
    ? dedupEvidence.nonProviderIdentityProvenanceCounts
    : {};
  const googleSheetsBundleShapeCounts = dedupEvidence?.googleSheetsBundleShapeCounts && typeof dedupEvidence.googleSheetsBundleShapeCounts === "object"
    ? dedupEvidence.googleSheetsBundleShapeCounts
    : {};
  const googleSheetsRoleBucketAuditCounts = dedupEvidence?.googleSheetsRoleBucketAuditCounts && typeof dedupEvidence.googleSheetsRoleBucketAuditCounts === "object"
    ? dedupEvidence.googleSheetsRoleBucketAuditCounts
    : {};
  const googleSheetsRoleBucketAudit = dedupEvidence?.googleSheetsRoleBucketAudit && typeof dedupEvidence.googleSheetsRoleBucketAudit === "object"
    ? dedupEvidence.googleSheetsRoleBucketAudit
    : {};
  const googleSheetsBucketIntentCounts = dedupEvidence?.googleSheetsBucketIntentCounts && typeof dedupEvidence.googleSheetsBucketIntentCounts === "object"
    ? dedupEvidence.googleSheetsBucketIntentCounts
    : {};
  const googleSheetsWeakGroupingAuditCounts = dedupEvidence?.googleSheetsWeakGroupingAuditCounts && typeof dedupEvidence.googleSheetsWeakGroupingAuditCounts === "object"
    ? dedupEvidence.googleSheetsWeakGroupingAuditCounts
    : {};
  const reviewQueueCounts = dedupEvidence?.reviewQueueCounts && typeof dedupEvidence.reviewQueueCounts === "object"
    ? dedupEvidence.reviewQueueCounts
    : {};
  const reviewQueueCauseCounts = dedupEvidence?.reviewQueueCauseCounts && typeof dedupEvidence.reviewQueueCauseCounts === "object"
    ? dedupEvidence.reviewQueueCauseCounts
    : {};
  const dedupAuditGate = dedupEvidence?.dedupAuditGate && typeof dedupEvidence.dedupAuditGate === "object"
    ? dedupEvidence.dedupAuditGate
    : {};
  const providerStaticDisagreementCounts = dedupEvidence?.providerStaticDisagreementCounts && typeof dedupEvidence.providerStaticDisagreementCounts === "object"
    ? dedupEvidence.providerStaticDisagreementCounts
    : {};
  const providerStaticDisagreementGateCounts = dedupEvidence?.providerStaticDisagreementGateCounts && typeof dedupEvidence.providerStaticDisagreementGateCounts === "object"
    ? dedupEvidence.providerStaticDisagreementGateCounts
    : {};
  const providerStaticDisagreementClassificationCounts = dedupEvidence?.providerStaticDisagreementClassificationCounts && typeof dedupEvidence.providerStaticDisagreementClassificationCounts === "object"
    ? dedupEvidence.providerStaticDisagreementClassificationCounts
    : {};
  const providerStaticTitleCompanyCollisionCounts = dedupEvidence?.providerStaticTitleCompanyCollisionCounts && typeof dedupEvidence.providerStaticTitleCompanyCollisionCounts === "object"
    ? dedupEvidence.providerStaticTitleCompanyCollisionCounts
    : {};
  const providerStaticTitleCompanyCollisionAuditCounts = dedupEvidence?.providerStaticTitleCompanyCollisionAuditCounts && typeof dedupEvidence.providerStaticTitleCompanyCollisionAuditCounts === "object"
    ? dedupEvidence.providerStaticTitleCompanyCollisionAuditCounts
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
  const providerStaticDisagreementRows = Array.isArray(dedupEvidence?.providerStaticDisagreementExamples)
    ? dedupEvidence.providerStaticDisagreementExamples
    : [];
  const providerStaticTitleCompanyCollisionRows = Array.isArray(dedupEvidence?.providerStaticTitleCompanyCollisionExamples)
    ? dedupEvidence.providerStaticTitleCompanyCollisionExamples
    : [];
  const providerStaticDisagreementSummary = formatProviderStaticDisagreementRows(
    providerStaticDisagreementRows,
    "No provider/static disagreement examples in the latest fetch report.",
    { onReviewAction: options?.onDedupReviewAction, tableKey: "providerStatic" }
  );
  const providerStaticTitleCompanyCollisionSummary = formatProviderStaticTitleCompanyCollisionRows(
    providerStaticTitleCompanyCollisionRows,
    "No provider/static title/company collision examples in the latest fetch report.",
    { onReviewAction: options?.onDedupReviewAction, tableKey: "providerStaticTitleCompany" }
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

  const runtimeSecondaryHtml = `
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Slowest sources</strong>: ${escapeHtml(slowestSummary)}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Slowest stages</strong>: ${escapeHtml(slowestStageSummary)}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>High-cost low-yield</strong>: ${escapeHtml(highCostSummary)}</div>
  `;
  const runtimeSectionHtml = `
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
    ${formatOpsMetricsDetails("Runtime diagnostics", runtimeSecondaryHtml, "admin-ops-runtime-details")}
  `;

  const failureBucketDetailsHtml = `
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Failure buckets</strong></div>
    ${bucketSummaryHtml}
  `;
  const failuresSectionHtml = `
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Top-level failed sources</strong>: ${Number(summary?.topLevelFailedSources || 0).toLocaleString()}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Grouped detail failures</strong>: ${Number(summary?.detailFailureCount || 0).toLocaleString()}</div>
    ${formatOpsMetricsDetails("Failure bucket details", failureBucketDetailsHtml, "admin-ops-failures-details")}
  `;

  const dedupSecondaryHtml = `
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup current-run merge examples</strong>: ${formatCurrentRunMergeExamples(dedupEvidence?.currentRunMergeExamples, "No current-run merge examples.")}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup provider/static disagreements</strong>: ${escapeHtml(formatProviderStaticDisagreementCounts(providerStaticDisagreementCounts))}. Gate: ${escapeHtml(formatProviderStaticDisagreementGateCounts(providerStaticDisagreementGateCounts))}. Classifications: ${escapeHtml(formatProviderStaticDisagreementClassificationCounts(providerStaticDisagreementClassificationCounts))}. ${providerStaticDisagreementSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup provider/static title-company collisions</strong>: ${escapeHtml(formatProviderStaticTitleCompanyCollisionCounts(providerStaticTitleCompanyCollisionCounts))}. Audit: ${escapeHtml(formatProviderStaticTitleCompanyCollisionAuditCounts(providerStaticTitleCompanyCollisionAuditCounts))}. ${providerStaticTitleCompanyCollisionSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup carried bundle examples</strong>: ${formatDedupAuditGateExamples(dedupEvidence?.carriedBundleExamples, "No carried bundle examples.")}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup source composition</strong>: ${escapeHtml(formatDedupSourceClasses(sourceBundleComposition))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup risk reasons</strong>: ${escapeHtml(formatDedupRiskReasonCounts(riskReasonCounts))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup outlier reasons</strong>: ${escapeHtml(formatDedupOutlierReasonCounts(outlierReasonCounts))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup identity shapes</strong>: ${escapeHtml(formatDedupIdentityShapeCounts(identityShapeCounts))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup identity quality</strong>: ${escapeHtml(formatDedupIdentityQualityCounts(identityQualityCounts))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup non-provider provenance</strong>: ${escapeHtml(formatDedupNonProviderIdentityProvenanceCounts(nonProviderIdentityProvenanceCounts))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup Google Sheets bundle shapes</strong>: ${escapeHtml(formatDedupGoogleSheetsBundleShapeCounts(googleSheetsBundleShapeCounts))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup Google Sheets role-bucket audit</strong>: ${escapeHtml(formatDedupGoogleSheetsRoleBucketAuditCounts(googleSheetsRoleBucketAuditCounts))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup Google Sheets role-bucket audit summary</strong>: ${formatDedupGoogleSheetsRoleBucketAuditSummary(googleSheetsRoleBucketAudit)}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup Google Sheets bucket intent</strong>: ${escapeHtml(formatDedupGoogleSheetsBucketIntentCounts(googleSheetsBucketIntentCounts))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup Google Sheets weak grouping audit</strong>: ${escapeHtml(formatDedupGoogleSheetsWeakGroupingAuditCounts(googleSheetsWeakGroupingAuditCounts))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup action queue</strong>: ${escapeHtml(formatDedupReviewQueueCounts(reviewQueueCounts))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup diagnostic causes</strong>: ${escapeHtml(formatDedupReviewQueueCauseCounts(reviewQueueCauseCounts))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Top merged jobs</strong>: ${topMergedSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Top source-bundle outliers</strong>: ${topOutlierSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup review examples</strong>: ${reviewQueueSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Risky merge examples</strong>: ${riskyMergeSummary}</div>
  `;
  const dedupSectionHtml = `
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup evidence</strong>: read-only diagnostics. Current-run merges by reason: primary URL ${Number(mergeReasonCounts?.primaryUrl || 0).toLocaleString()}, secondary key ${Number(mergeReasonCounts?.secondaryKey || 0).toLocaleString()}, known mirror pair ${Number(mergeReasonCounts?.knownMirrorPair || 0).toLocaleString()}, social key ${Number(mergeReasonCounts?.socialKey || 0).toLocaleString()}, sparse identity ${Number(mergeReasonCounts?.sparseIdentity || 0).toLocaleString()}, unknown ${Number(mergeReasonCounts?.unknown || 0).toLocaleString()}. Carried source-bundle collision rows: ${Number(dedupEvidence?.sourceBundleCollisionCount || 0).toLocaleString()}.</div>
    ${formatDedupAuditGateCard(dedupAuditGate)}
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup review-state</strong>: ${escapeHtml(formatDedupReviewStateSummary(dedupReviewStateSummary, dedupReviewStateReadWarning))}</div>
    ${formatOpsMetricsDetails("Dedup supporting diagnostics", dedupSecondaryHtml, "admin-ops-dedup-details")}
  `;

  const sourceHealthSecondaryHtml = `
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Zero kept / needs review</strong>: ${zeroReviewSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Browser fallback recommended</strong>: ${browserSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Top productive sources</strong>: ${productiveSummary}</div>
  `;
  const sourceHealthSectionHtml = `
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Sources needing attention</strong>: ${attentionSummary}</div>
    ${formatOpsMetricsDetails("Source health supporting diagnostics", sourceHealthSecondaryHtml, "admin-ops-source-health-details")}
  `;

  const sourcePolicySecondaryHtml = `
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Runtime-suppressed static sources</strong>: ${dynamicRedundantSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Validated staged providers</strong>: ${validatedProviderSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Provider coverage needs review</strong>: ${reviewProviderSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Unstable / failed providers</strong>: ${failedProviderSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Ready later (no static mutation)</strong>: ${readyLaterProviderSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Provider/static overlap audit</strong>: safe ${Number(providerStaticOverlap?.safePairCount || 0).toLocaleString()}, needs review ${Number(providerStaticOverlap?.needsReviewPairCount || 0).toLocaleString()}, insufficient history ${Number(providerStaticOverlap?.insufficientHistoryPairCount || 0).toLocaleString()}. ${overlapAuditSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Static suppression policy</strong>: suppressed ${Number(staticSuppressionPolicy?.suppressedCount || 0).toLocaleString()}, paused ${Number(staticSuppressionPolicy?.pausedCount || 0).toLocaleString()}, warnings ${Number(staticSuppressionPolicy?.warningCount || 0).toLocaleString()}. Suppressed: ${suppressedPolicySummary} Paused: ${pausedPolicySummary} Warnings: ${warningPolicySummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Redundant static proposals</strong>: safe ${Number(redundantStaticProposals?.safeRedundantCount || 0).toLocaleString()}, keep static ${Number(redundantStaticProposals?.keepStaticCount || 0).toLocaleString()}, more history ${Number(redundantStaticProposals?.needsMoreHistoryCount || 0).toLocaleString()}, review/unstable ${Number((redundantStaticProposals?.needsReviewCount || 0) + (redundantStaticProposals?.providerUnstableCount || 0)).toLocaleString()}, static-only ${Number(redundantStaticProposals?.staticOnlyDetectedCount || 0).toLocaleString()}. Safe: ${safeRedundantProposalSummary} Keep: ${keepStaticProposalSummary} History: ${moreHistoryProposalSummary} Review: ${reviewProposalSummary} Static-only: ${staticOnlyProposalSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Conservative static cleanup proposals</strong>: total candidates ${Number(conservativeStaticCleanupProposals?.totalCandidateCount || 0).toLocaleString()}, proposal-ready ${Number(conservativeStaticCleanupProposals?.proposalCount || 0).toLocaleString()}, stale ${Number(conservativeStaticCleanupProposals?.staleCount || 0).toLocaleString()}, blocked ${Number(conservativeStaticCleanupProposals?.blockedCount || 0).toLocaleString()}. Freshness: ${escapeHtml(cleanupFreshnessSummary)}. Blockers: ${escapeHtml(cleanupBlockedReasonSummary)} Ready: ${cleanupProposalReadySummary} Blocked: ${cleanupBlockedSummary}</div>
  `;
  const sourcePolicySectionHtml = `
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Source-policy review</strong>: local review pairs ${Number(sourcePolicyRecommendationExport?.reviewStatePairCount || 0).toLocaleString()}, force-paused ${Number(sourcePolicyRecommendationExport?.manualForcePausedCount || 0).toLocaleString()}. Use the Source Policy Review queue for local, reversible actions.</div>
    ${formatOpsMetricsDetails("Source policy supporting diagnostics", sourcePolicySecondaryHtml, "admin-ops-source-policy-details")}
  `;
  const frontendPerfSectionHtml = `
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Frontend fetch/render counters</strong>: ${frontendPerfSummary}</div>
  `;

  const taskLaneRows = buildOpsTaskLaneRows(options?.runModel || {});
  const diagnosticsByKey = buildOpsFetcherDiagnosticsSections({
    latest,
    history,
    failureSummary: summary,
    taskLaneRows
  });
  const taskLaneHtml = formatOpsTaskLane(taskLaneRows, diagnosticsByKey.taskStatus);
  const sectionHtmlByKey = {
    runtime: runtimeSectionHtml,
    failures: failuresSectionHtml,
    frontendPerf: frontendPerfSectionHtml,
    sourceHealth: sourceHealthSectionHtml,
    sourcePolicy: sourcePolicySectionHtml
  };
  if (options?.includeDedupSection === true) {
    sectionHtmlByKey.dedup = dedupSectionHtml;
  }
  const sectionHtml = `${taskLaneHtml}${buildOpsFetcherMetricSections(
    sectionHtmlByKey,
    diagnosticsByKey
  ).map(formatOpsFetcherMetricSection).join("")}`;
  metricsEl.innerHTML = `
    <details class="admin-ops-metrics-details admin-ops-fetcher-diagnostics admin-ops-full-row">
      <summary>Fetcher diagnostics</summary>
      <div class="admin-ops-metrics-details-body admin-ops-fetcher-diagnostics-body">
        ${sectionHtml}
      </div>
    </details>
  `;

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
}
