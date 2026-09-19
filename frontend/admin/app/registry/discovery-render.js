/**
 * Admin registry — discovery lane fetch and render.
 *
 * One pass of ``loadDiscoveryData``'s body: fetch the registry/report/
 * candidate lanes concurrently, then render the source tables and discovery
 * candidate review panel.
 *
 * Split out of ``discovery-load.js``; that module owns the entrypoint and the
 * endpoint/retry helpers.
 *
 * @module discovery-render
 */

import { deriveDiscoveryLifecycleCounts, deriveDiscoveryQueuedCount } from "../../domain.js";
import { renderDiscoveryCandidateReviewHtml } from "../../render.js";

const FULL_REGISTRY_LOAD_TIMEOUT_MS = 60000;
const ACTIVE_REGISTRY_LOAD_TIMEOUT_MS = 10000;
const ADMIN_REGISTRY_TABLE_LIMIT_PER_BUCKET = 250;
const ACTIVE_ADMIN_REGISTRY_TABLE_LIMIT_PER_BUCKET = 25;

const CAP_DEFER_REASONS = new Set(["adapter_cap", "domain_cap", "top_n_cap"]);

function getDiscoveryCandidatesRows(payload) {
  return Array.isArray(payload?.candidates) ? payload.candidates : [];
}

function countCapDeferredCandidates(rows) {
  return rows.filter(row => row?.deferred && CAP_DEFER_REASONS.has(String(row?.deferReason || row?.dropReason || ""))).length;
}

function countJobPositiveDeferredCandidates(rows) {
  return rows.filter(row => row?.deferred && Number(row?.jobsFound ?? row?.sampleCount ?? 0) > 0).length;
}

function formatRegistryCountBasis(summary) {
  if (summary?.summaryExact === true || String(summary?.countBasis || "").toLowerCase() === "normalized") {
    return "normalized counts";
  }
  if (summary?.summaryExact === false || String(summary?.countBasis || "").toLowerCase() === "storage") {
    return "storage snapshot counts";
  }
  return "loaded counts";
}

function formatPendingApprovalBreakdown(summary) {
  const pendingApproval = summary?.pendingApproval && typeof summary.pendingApproval === "object"
    ? summary.pendingApproval
    : null;
  if (!pendingApproval) return "";
  const buckets = pendingApproval.reviewBucketCounts && typeof pendingApproval.reviewBucketCounts === "object"
    ? pendingApproval.reviewBucketCounts
    : {};
  const blockers = pendingApproval.blockerCounts && typeof pendingApproval.blockerCounts === "object"
    ? pendingApproval.blockerCounts
    : {};
  const autoEligible = Number(pendingApproval.autoApprovalEligibleCount || buckets.auto_approvable || 0);
  const weakSignal = Number(buckets.weak_signal || blockers.weak_signal || 0);
  const zeroJobs = Number(buckets.zero_jobs || blockers.zero_jobs || 0);
  const conflictDemoted = Number(buckets.conflict_demoted || blockers.conflict_demoted || 0);
  const existingMatch = Number(buckets.existing_match || blockers.existing_match || 0);
  const deferred = Number(buckets.deferred || blockers.deferred || 0);
  const parts = [
    `auto-eligible ${autoEligible.toLocaleString()}`,
    `weak ${weakSignal.toLocaleString()}`,
    `zero jobs ${zeroJobs.toLocaleString()}`,
    `conflict-demoted ${conflictDemoted.toLocaleString()}`,
    `existing-match ${existingMatch.toLocaleString()}`,
    `deferred ${deferred.toLocaleString()}`
  ];
  return parts.join(", ");
}

/**
 * Run one discovery load pass.
 *
 * @param {Object} ctx everything the pass closes over in ``createDiscoveryLoad``
 * @returns {Promise<Object|null>} the load result
 */
async function runDiscoveryLoad(ctx) {
  const {
    state,
    refs,
    getBridge,
    options,
    background,
    activeContext,
    activeCompactSourceTables,
    renderToken,
    registryRenderToken,
    toAdminFilterState,
    resolveLatestFetchReport,
    loadDiscoveryEndpoint,
    buildDiscoveryRegistrySignature,
    resetRegistryRefreshRetryDelay,
    scheduleRegistryRefreshRetry,
    appendDiscoveryLog,
    adminDispatch,
    adminActions,
    getErrorMessage,
    setBusyFlag,
    renderSourcesTable,
    mergeSourceDiscoveryCandidates,
    mergeSourceStatusFromReport,
    applySourceFilter,
    getSourceDiscoveryJobsCount,
    sourcePayloadIsDegradedEmpty,
    setSourceTableRefreshingPlaceholder,
    setSourceTableUnavailablePlaceholder,
    setSourceTablesLoadState,
    renderSourceTablesDelayed,
    markSourceTablesDelayedForActiveWork,
    scheduleDeferredRender
  } = ctx;

try {
    const filterState = toAdminFilterState();
    const sourceTablesOnly = Boolean(options?.sourceTablesOnly || activeCompactSourceTables);
    const reportPromise = sourceTablesOnly
      ? Promise.resolve(null)
      : loadDiscoveryEndpoint(
        "source discovery report",
        getBridge("/discovery/report"),
        state.latestDiscoveryReportCache || { summary: {}, candidates: [], failures: [] },
        { background }
      );
    const loadRegistrySummary = Boolean(options?.completionRefresh && !activeCompactSourceTables);
    const registrySummaryPromise = loadRegistrySummary
      ? loadDiscoveryEndpoint(
        "Admin registry summary",
        getBridge("/registry/summary"),
        { ok: true, summary: {} },
        { background }
      )
      : Promise.resolve({ ok: true, summary: {} });
    const discoveryCandidatesPromise = sourceTablesOnly
      ? Promise.resolve({ candidates: [] })
      : loadDiscoveryEndpoint(
        "source discovery candidates",
        getBridge("/discovery/candidates"),
        { candidates: [] }
      );
    const latestFetchReportPromise = sourceTablesOnly
      ? Promise.resolve(state.latestFetcherReportCache || {})
      : loadDiscoveryEndpoint(
        "latest fetch report",
        resolveLatestFetchReport(options),
        state.latestFetcherReportCache || {}
      );
    const registryLimitPerBucket = activeCompactSourceTables
      ? ACTIVE_ADMIN_REGISTRY_TABLE_LIMIT_PER_BUCKET
      : ADMIN_REGISTRY_TABLE_LIMIT_PER_BUCKET;
    // Single compact-table lane: detail/annotation modes were removed
    // server-side (2026-08-24); the endpoint now serves this lane by default.
    const registrySourcesPath = `/registry/sources?view=table&buckets=pending,active,rejected&includeHiddenPending=${filterState.showZeroJobs ? "1" : "0"}&limitPerBucket=${registryLimitPerBucket}`;
    const registrySourcesTimeoutMs = activeCompactSourceTables
      ? ACTIVE_REGISTRY_LOAD_TIMEOUT_MS
      : FULL_REGISTRY_LOAD_TIMEOUT_MS;
    const registrySourcesPromise = registrySummaryPromise
      .then(registrySummary => loadDiscoveryEndpoint(
        "Admin registry source tables",
        getBridge(registrySourcesPath, { timeoutMs: registrySourcesTimeoutMs }),
        {
          ok: false,
          sources: { pending: [], active: [], rejected: [] },
          summary: registrySummary?.summary || {}
        },
        { registryRefresh: true, background }
      ));
    registrySourcesPromise.then(payload => {
      if (!payload?.__loadFailed && !sourcePayloadIsDegradedEmpty(payload)) {
        state.sourceTablesBridgeDegraded = false;
        state.adminBridgeHeavyRouteDegradedUntilMs = 0;
      }
    }).catch(() => {});

    const pendingRowsPromise = Promise.all([registrySourcesPromise, discoveryCandidatesPromise, latestFetchReportPromise])
      .then(([registrySources, discoveryCandidates, latestFetchReport]) => {
        const pending = {
          sources: Array.isArray(registrySources?.sources?.pending)
            ? registrySources.sources.pending
            : [],
          summary: registrySources?.summary || {},
          __loadFailed: Boolean(registrySources?.__loadFailed),
          __delayedDuringActiveRun: Boolean(registrySources?.__delayedDuringActiveRun)
        };
        const degradedEmpty = sourcePayloadIsDegradedEmpty(registrySources);
        const loadFailed = Boolean(pending?.__loadFailed || degradedEmpty);
        const rows = mergeSourceStatusFromReport(
          mergeSourceDiscoveryCandidates(Array.isArray(pending?.sources) ? pending.sources : [], discoveryCandidates),
          latestFetchReport,
          "pending"
        );
        const hiddenZeroJobsCount = Math.max(
          Number(pending?.summary?.hiddenPendingCount || 0),
          rows.filter(row => getSourceDiscoveryJobsCount(row) === 0).length
        );
        const visibleRows = applySourceFilter(
          filterState.showZeroJobs ? rows : rows.filter(row => getSourceDiscoveryJobsCount(row) !== 0)
        );
        scheduleDeferredRender(() => {
          if (background || renderToken !== registryRenderToken) return;
          if (loadFailed) {
            if (degradedEmpty) {
              setSourceTableRefreshingPlaceholder(refs.adminPendingSourcesEl, pending.summary);
            } else if (pending.__delayedDuringActiveRun) {
              renderSourceTablesDelayed({ onlyIfPlaceholder: true });
            } else {
              setSourceTableUnavailablePlaceholder(refs.adminPendingSourcesEl, "pending");
            }
            return;
          }
          renderSourcesTable(refs.adminPendingSourcesEl, visibleRows, "pending");
          if (
            refs.adminPendingSourcesEl
            && !filterState.showZeroJobs
            && visibleRows.length === 0
            && hiddenZeroJobsCount > 0
          ) {
            refs.adminPendingSourcesEl.innerHTML = `<div class="no-results">${hiddenZeroJobsCount.toLocaleString()} pending sources have 0 discovery jobs and are hidden. Enable "Show zero-jobs pending sources" to view them.</div>`;
          }
        });
        return {
          payload: pending,
          rows,
          hiddenZeroJobsCount,
          visibleRows,
          loadFailed,
          delayedDuringActiveRun: Boolean(pending?.__delayedDuringActiveRun || degradedEmpty)
        };
      });
    const activeRowsPromise = Promise.all([registrySourcesPromise, discoveryCandidatesPromise, latestFetchReportPromise])
      .then(([registrySources, discoveryCandidates, latestFetchReport]) => {
        const active = {
          sources: Array.isArray(registrySources?.sources?.active)
            ? registrySources.sources.active
            : [],
          summary: registrySources?.summary || {},
          __loadFailed: Boolean(registrySources?.__loadFailed),
          __delayedDuringActiveRun: Boolean(registrySources?.__delayedDuringActiveRun)
        };
        const degradedEmpty = sourcePayloadIsDegradedEmpty(registrySources);
        const loadFailed = Boolean(active?.__loadFailed || degradedEmpty);
        const rows = mergeSourceStatusFromReport(
          mergeSourceDiscoveryCandidates(Array.isArray(active?.sources) ? active.sources : [], discoveryCandidates),
          latestFetchReport,
          "active"
        );
        const visibleRows = applySourceFilter(rows);
        scheduleDeferredRender(() => {
          if (background || renderToken !== registryRenderToken) return;
          if (loadFailed) {
            if (degradedEmpty) {
              setSourceTableRefreshingPlaceholder(refs.adminActiveSourcesEl, active.summary);
            } else if (active.__delayedDuringActiveRun) {
              renderSourceTablesDelayed({ onlyIfPlaceholder: true });
            } else {
              setSourceTableUnavailablePlaceholder(refs.adminActiveSourcesEl, "active");
            }
            return;
          }
          renderSourcesTable(refs.adminActiveSourcesEl, visibleRows, "active");
        });
        return {
          payload: active,
          rows,
          visibleRows,
          loadFailed,
          delayedDuringActiveRun: Boolean(active?.__delayedDuringActiveRun || degradedEmpty)
        };
      });
    const rejectedRowsPromise = Promise.all([registrySourcesPromise, discoveryCandidatesPromise, latestFetchReportPromise])
      .then(([registrySources, discoveryCandidates, latestFetchReport]) => {
        const rejected = {
          sources: Array.isArray(registrySources?.sources?.rejected)
            ? registrySources.sources.rejected
            : [],
          summary: registrySources?.summary || {},
          __loadFailed: Boolean(registrySources?.__loadFailed),
          __delayedDuringActiveRun: Boolean(registrySources?.__delayedDuringActiveRun)
        };
        const degradedEmpty = sourcePayloadIsDegradedEmpty(registrySources);
        const loadFailed = Boolean(rejected?.__loadFailed || degradedEmpty);
        const rows = mergeSourceStatusFromReport(
          mergeSourceDiscoveryCandidates(Array.isArray(rejected?.sources) ? rejected.sources : [], discoveryCandidates),
          latestFetchReport,
          "rejected"
        );
        const visibleRows = applySourceFilter(rows);
        scheduleDeferredRender(() => {
          if (background || renderToken !== registryRenderToken) return;
          if (loadFailed) {
            if (degradedEmpty) {
              setSourceTableRefreshingPlaceholder(refs.adminRejectedSourcesEl, rejected.summary);
            } else if (rejected.__delayedDuringActiveRun) {
              renderSourceTablesDelayed({ onlyIfPlaceholder: true });
            } else {
              setSourceTableUnavailablePlaceholder(refs.adminRejectedSourcesEl, "rejected");
            }
            return;
          }
          renderSourcesTable(refs.adminRejectedSourcesEl, visibleRows, "rejected");
        });
        return {
          payload: rejected,
          rows,
          visibleRows,
          loadFailed,
          delayedDuringActiveRun: Boolean(rejected?.__delayedDuringActiveRun || degradedEmpty)
        };
      });
    const [report, discoveryCandidates, pendingResult, activeResult, rejectedResult] = await Promise.all([
      reportPromise,
      discoveryCandidatesPromise,
      pendingRowsPromise,
      activeRowsPromise,
      rejectedRowsPromise
    ]);
    const pending = pendingResult.payload;
    const active = activeResult.payload;
    const rejected = rejectedResult.payload;
    if (report && typeof report === "object" && !Array.isArray(report)) {
      state.latestDiscoveryReportCache = report;
    }
    const summary = report?.summary || {};
    const foundCount = Number(summary.foundEndpointCount ?? summary.probedCount ?? 0);
    const probedCount = Number(summary.probedCandidateCount ?? summary.probedCount ?? 0);
    const queuedCount = deriveDiscoveryQueuedCount(report);
    const deferredCount = Number(summary.discoverableButDeferredCount ?? 0);
    const lifecycleCounts = deriveDiscoveryLifecycleCounts(report);
    const skippedCount = Number(summary.skippedDuplicateCount || 0);
    const failedCount = Number(summary.failedProbeCount || 0);
    const discoveryCandidateRows = getDiscoveryCandidatesRows(discoveryCandidates);
    const capDeferredCount = countCapDeferredCandidates(discoveryCandidateRows);
    const jobPositiveDeferredCount = countJobPositiveDeferredCandidates(discoveryCandidateRows);
    const runtimeAutoApproval = report?.runtime?.autoApproval && typeof report.runtime.autoApproval === "object"
      ? report.runtime.autoApproval
      : {};
    const autoApprovedCount = Number(summary.approvedCandidateCount ?? runtimeAutoApproval.approvedCount ?? 0);
    const activeRegistryCount = Number(active?.summary?.activeCount || 0);
    const registryCountBasisLabel = formatRegistryCountBasis(
      active?.summary || pending?.summary || rejected?.summary || {}
    );
    const pendingRows = pendingResult.rows;
    const activeRows = activeResult.rows;
    const rejectedRows = rejectedResult.rows;
    const partialLoadFailed = Boolean(
      report?.__loadFailed
      || pendingResult.loadFailed
      || activeResult.loadFailed
      || rejectedResult.loadFailed
    );
    const registryDelayedDuringActiveRun = Boolean(
      pending?.__delayedDuringActiveRun
      || active?.__delayedDuringActiveRun
      || rejected?.__delayedDuringActiveRun
      || pendingResult.delayedDuringActiveRun
      || activeResult.delayedDuringActiveRun
      || rejectedResult.delayedDuringActiveRun
    );
    const registrySignature = buildDiscoveryRegistrySignature({
      pending: pendingRows,
      active: activeRows,
      rejected: rejectedRows
    });
    const hiddenZeroJobsCount = pendingResult.hiddenZeroJobsCount;

    if (!sourceTablesOnly && refs.adminDiscoverySummaryEl) {
      const pendingApprovalBreakdown = formatPendingApprovalBreakdown(pending?.summary);
      const pendingApprovalText = pendingApprovalBreakdown
        ? ` | Pending source blockers: ${pendingApprovalBreakdown}`
        : "";
      const summaryText = `Found ${foundCount} | Probed ${probedCount} | Review queue ${queuedCount} | Deferred review ${deferredCount} | Deferred by caps ${capDeferredCount} | Job-positive deferred ${jobPositiveDeferredCount} | Validated ${lifecycleCounts.validated} | Auto-approved this run ${autoApprovedCount} | Active registry ${activeRegistryCount} (${registryCountBasisLabel}) | Failed ${failedCount} | Skipped dupes ${skippedCount} | Pending sources ${Number(pending?.summary?.pendingCount || 0)}${pendingApprovalText} | Rejected ${Number(rejected?.summary?.rejectedCount || 0)} | Hidden zero-jobs ${hiddenZeroJobsCount}`;
      refs.adminDiscoverySummaryEl.textContent = summaryText;
      refs.adminDiscoverySummaryEl.innerHTML = `<div>${summaryText}</div>`;
    }
    if (refs.discoveryPendingBadgeEl) {
      const pendingCount = Number(pending?.summary?.pendingCount || 0);
      if (pendingCount > 0) {
        refs.discoveryPendingBadgeEl.textContent = pendingCount > 999 ? "999+" : pendingCount.toLocaleString();
        refs.discoveryPendingBadgeEl.classList.remove("hidden");
      } else {
        refs.discoveryPendingBadgeEl.classList.add("hidden");
      }
    }
    if (!sourceTablesOnly && refs.adminDiscoveryReviewEl) {
      refs.adminDiscoveryReviewEl.innerHTML = renderDiscoveryCandidateReviewHtml(
        report?.candidateReview,
        { showEmpty: true }
      );
    }
    const registryChanged = registrySignature !== String(state.discoveryRegistrySignature || "");
    if (!partialLoadFailed) {
      state.discoveryRegistrySignature = registrySignature;
    }
    const shouldRenderTables = Boolean(
      !partialLoadFailed
      && (
        options?.forceRender
        || registryChanged
        || !state.discoveryTablesRendered
      )
    );
    if (background && shouldRenderTables) {
      scheduleDeferredRender(() => {
        if (renderToken !== registryRenderToken) return;
        renderSourcesTable(refs.adminPendingSourcesEl, pendingResult.visibleRows || [], "pending");
        if (
          refs.adminPendingSourcesEl
          && !filterState.showZeroJobs
          && (pendingResult.visibleRows || []).length === 0
          && hiddenZeroJobsCount > 0
        ) {
          refs.adminPendingSourcesEl.innerHTML = `<div class="no-results">${hiddenZeroJobsCount.toLocaleString()} pending sources have 0 discovery jobs and are hidden. Enable "Show zero-jobs pending sources" to view them.</div>`;
        }
        renderSourcesTable(refs.adminActiveSourcesEl, activeResult.visibleRows || [], "active");
        renderSourcesTable(refs.adminRejectedSourcesEl, rejectedResult.visibleRows || [], "rejected");
        state.discoveryTablesRendered = true;
      });
    } else if (!background) {
      state.discoveryTablesRendered = true;
    }
    if (!partialLoadFailed && registryChanged && options?.logChanges !== false) {
      if (sourceTablesOnly) {
        appendDiscoveryLog("Source registry tables loaded.", "success");
      } else {
        appendDiscoveryLog("Loading source discovery report and registries...");
        appendDiscoveryLog(
          `Discovery summary: found ${foundCount}, probed ${probedCount}, review queue ${queuedCount}, auto-approved ${autoApprovedCount}, failed ${failedCount}, skipped duplicates ${skippedCount}.`,
          "info"
        );
        const topFailures = Array.isArray(report?.topFailures) ? report.topFailures : [];
        if (topFailures.length) {
          const line = topFailures
            .slice(0, 3)
            .map(item => `${String(item?.key || "unknown")} (${Number(item?.count || 0)})`)
            .join(", ");
          appendDiscoveryLog(`Top failures: ${line}`, "warn");
        }
        appendDiscoveryLog("Source discovery data loaded.", "success");
      }
    }
    adminDispatch.dispatch({ type: adminActions.DISCOVERY_REFRESHED, payload: { at: new Date().toISOString() } });
    if (!partialLoadFailed) {
      state.sourceTablesDelayedDuringActiveRun = false;
      state.discoveryLastLoadSucceededAtMs = Date.now();
      setSourceTablesLoadState("loaded", activeContext.reason);
      resetRegistryRefreshRetryDelay();
    } else if (registryDelayedDuringActiveRun) {
      markSourceTablesDelayedForActiveWork("active_registry_timeout", { onlyIfPlaceholder: true });
      if (options?.suppressRegistryRetry !== true) {
        scheduleRegistryRefreshRetry({ forcePipelinePreflight: true, fetchReport: options?.fetchReport || null });
      }
    } else {
      setSourceTablesLoadState("unavailable", activeContext.reason);
    }
    return {
      report,
      pendingRows,
      activeRows,
      rejectedRows,
      partialLoadFailed
    };
  } catch (err) {
    setSourceTablesLoadState("unavailable", activeContext.reason);
    appendDiscoveryLog(`Could not load source discovery data: ${getErrorMessage(err)}`, "error");
    if (refs.adminDiscoverySummaryEl) {
      const message = getErrorMessage(err);
      if (String(message || "").includes("bridge unreachable")) {
        refs.adminDiscoverySummaryEl.textContent = "Source discovery bridge unavailable. Start `Run admin bridge` task.";
      }
    }
    if (refs.adminDiscoveryReviewEl) {
      refs.adminDiscoveryReviewEl.innerHTML = '<div class="no-results">Discovery review unavailable.</div>';
    }
    return null;
  } finally {
    state.discoveryLoadPromise = null;
    state.discoveryLastLoadCompletedAtMs = Date.now();
    setBusyFlag("discoveryLoad", false);
  }
}

export { runDiscoveryLoad };
