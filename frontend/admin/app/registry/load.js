import { deriveDiscoveryLifecycleCounts, deriveDiscoveryQueuedCount } from "../../domain.js";
import { renderDiscoveryCandidateReviewHtml } from "../../render.js?v=16";

const ADMIN_SHOW_ZERO_JOBS_KEY = "baluffo_admin_show_zero_jobs_sources";
const CAP_DEFER_REASONS = new Set(["adapter_cap", "domain_cap", "top_n_cap"]);
const FULL_REGISTRY_LOAD_TIMEOUT_MS = 60000;
const PIPELINE_STATUS_PREFLIGHT_TIMEOUT_MS = 3000;
const REGISTRY_REFRESH_RETRY_DELAY_MS = 5000;
const ACTIVE_PIPELINE_SOURCE_TABLES_DELAYED_LABEL = "Source tables delayed while job update is running.";
const JOBS_PIPELINE_STATUS_PATH = "/tasks/run-jobs-pipeline-status";
const INACTIVE_PIPELINE_STAGES = new Set(["idle", "complete", "completed", "error", "failed", "canceled", "cancelled", "aborted"]);

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

function fnv1a32(value, seed = 0x811c9dc5) {
  let hash = seed >>> 0;
  const text = String(value ?? "");
  for (let index = 0; index < text.length; index += 1) {
    hash ^= text.charCodeAt(index);
    hash = Math.imul(hash, 0x01000193) >>> 0;
  }
  return hash >>> 0;
}

function toDigestHex(value) {
  return (value >>> 0).toString(16).padStart(8, "0");
}

export function createRegistryLoadController({
  state,
  refs,
  getBridge,
  fetchJobsFetchReportJson,
  mergeSourceDiscoveryCandidates = rows => rows,
  mergeSourceStatusFromReport,
  applySourceFilter,
  getSourceJobsFoundCount,
  getSourceDiscoveryJobsCount = getSourceJobsFoundCount,
  normalizeSourceFilter,
  readShowZeroJobs,
  adminDispatch,
  adminActions,
  appendDiscoveryLog,
  getErrorMessage,
  setBusyFlag,
  renderSourcesTable,
  renderScheduler
}) {
  let registryRenderToken = 0;

  function resolveLatestFetchReport(options = {}) {
    const providedReport = options?.fetchReport;
    if (providedReport && typeof providedReport === "object" && !Array.isArray(providedReport)) {
      state.latestFetcherReportCache = providedReport;
      return Promise.resolve(providedReport);
    }
    if (!options?.forceFetchReport && state.latestFetcherReportCache) {
      return Promise.resolve(state.latestFetcherReportCache);
    }
    return Promise.resolve(fetchJobsFetchReportJson())
      .then(report => {
        if (report && typeof report === "object" && !Array.isArray(report)) {
          state.latestFetcherReportCache = report;
        }
        return report || state.latestFetcherReportCache || null;
      });
  }

  function toAdminFilterState() {
    return {
      activeSourceFilter: normalizeSourceFilter(state.activeSourceFilter),
      showZeroJobs: readShowZeroJobs(ADMIN_SHOW_ZERO_JOBS_KEY)
    };
  }

  function buildDiscoveryRegistrySignature(rowsByBucket) {
    const buckets = ["pending", "active", "rejected"];
    return buckets.map(bucket => {
      const rows = Array.isArray(rowsByBucket?.[bucket]) ? rowsByBucket[bucket] : [];
      let count = 0;
      let xorHash = 0;
      let sumHash = 0;
      let lengthHash = 0;
      rows.forEach(row => {
        const rowText = [
          String(row?.id || row?.sourceId || row?.name || ""),
          String(row?.name || ""),
          String(row?.adapter || ""),
          String(row?.studio || ""),
          String(row?.status || ""),
          String(Number(getSourceJobsFoundCount(row) || 0)),
          String(row?.sourceId || ""),
          String(row?.url || row?.sourceUrl || "")
        ].join("\u001f");
        const rowHash = fnv1a32(rowText);
        count += 1;
        xorHash = (xorHash ^ rowHash) >>> 0;
        sumHash = (sumHash + rowHash) >>> 0;
        lengthHash = (lengthHash + fnv1a32(rowText.length, rowHash)) >>> 0;
      });
      return `${bucket}:${count}:${toDigestHex(xorHash)}:${toDigestHex(sumHash)}:${toDigestHex(lengthHash)}`;
    }).join("|");
  }

  function setSourceTablePlaceholder(container, bucketLabel) {
    if (!container) return;
    container.innerHTML = `<div class="muted">Loading ${bucketLabel} sources...</div>`;
  }

  function setSourceTableDelayedPlaceholder(container) {
    if (!container) return;
    container.innerHTML = `<div class="muted">${ACTIVE_PIPELINE_SOURCE_TABLES_DELAYED_LABEL}</div>`;
  }

  function setSourceTableUnavailablePlaceholder(container, bucketLabel) {
    if (!container) return;
    container.innerHTML = `<div class="no-results">Could not load ${bucketLabel} sources. Retry after the running job update finishes.</div>`;
  }

  function sourceTableNeedsDelayedPlaceholder(container) {
    if (!container) return false;
    const currentText = String(container.textContent || container.innerHTML || "").trim();
    return !currentText
      || /Loading (pending|active|rejected) sources/i.test(currentText)
      || currentText.includes(ACTIVE_PIPELINE_SOURCE_TABLES_DELAYED_LABEL);
  }

  function activePipelineOrFetchRunning() {
    if (state.adminBusyState?.livePipelineRunning || state.adminBusyState?.liveFetchRunning) {
      return true;
    }
    const rows = Array.isArray(state.latestOpsTaskStatePayload?.tasks)
      ? state.latestOpsTaskStatePayload.tasks
      : [];
    return rows.some(row => {
      const type = String(row?.taskType || row?.type || "").trim().toLowerCase();
      return row?.active !== false && !row?.finishedAt && (type === "pipeline" || type === "fetch");
    });
  }

  function pipelineStatusIndicatesActive(payload = {}) {
    if (!payload || typeof payload !== "object" || Array.isArray(payload)) return false;
    if (payload.active === true) return true;
    const activeChildren = Array.isArray(payload.activeChildren) ? payload.activeChildren : [];
    if (activeChildren.some(row => row && typeof row === "object" && row.active !== false)) {
      return true;
    }
    const stage = String(payload.stage || payload?.progress?.phaseKey || "").trim().toLowerCase();
    if (!stage || INACTIVE_PIPELINE_STAGES.has(stage)) return false;
    if (payload.active === false) return false;
    return true;
  }

  function rememberPipelineStatusActivity(payload = {}) {
    if (!pipelineStatusIndicatesActive(payload)) return;
    const activeChildren = Array.isArray(payload.activeChildren)
      ? payload.activeChildren
          .filter(row => row && typeof row === "object" && row.active !== false)
          .slice(0, 3)
          .map(row => ({
            ...row,
            taskType: String(row.taskType || row.type || "").trim().toLowerCase(),
            type: String(row.type || row.taskType || "").trim().toLowerCase(),
            active: true
          }))
      : [];
    const stage = String(payload.stage || payload?.progress?.phaseKey || "pipeline").trim().toLowerCase();
    setBusyFlag("livePipelineRunning", true);
    setBusyFlag(
      "liveFetchRunning",
      activeChildren.some(row => String(row?.taskType || row?.type || "").trim().toLowerCase() === "fetch") || stage === "fetch"
    );
    state.discoveryPipelineStatusLastActiveAtMs = Date.now();
  }

  async function refreshActivePipelineStatus({ force = false } = {}) {
    if (!force && activePipelineOrFetchRunning()) return true;
    try {
      const payload = await getBridge(JOBS_PIPELINE_STATUS_PATH, { timeoutMs: PIPELINE_STATUS_PREFLIGHT_TIMEOUT_MS });
      if (pipelineStatusIndicatesActive(payload)) {
        rememberPipelineStatusActivity(payload);
        return true;
      }
      state.discoveryPipelineStatusLastActiveAtMs = 0;
      if (force) {
        setBusyFlag("livePipelineRunning", false);
        setBusyFlag("liveFetchRunning", false);
      }
    } catch {
      // Source tables should remain available when the fast control-plane preflight is unavailable.
    }
    return activePipelineOrFetchRunning();
  }

  function recentlyDetectedActivePipeline() {
    const lastActiveAtMs = Number(state.discoveryPipelineStatusLastActiveAtMs || 0);
    return lastActiveAtMs > 0 && Date.now() - lastActiveAtMs < 120000;
  }

  function renderSourceTablesDelayed({ onlyIfPlaceholder = false } = {}) {
    if (!onlyIfPlaceholder || sourceTableNeedsDelayedPlaceholder(refs.adminPendingSourcesEl)) {
      setSourceTableDelayedPlaceholder(refs.adminPendingSourcesEl);
    }
    if (!onlyIfPlaceholder || sourceTableNeedsDelayedPlaceholder(refs.adminActiveSourcesEl)) {
      setSourceTableDelayedPlaceholder(refs.adminActiveSourcesEl);
    }
    if (!onlyIfPlaceholder || sourceTableNeedsDelayedPlaceholder(refs.adminRejectedSourcesEl)) {
      setSourceTableDelayedPlaceholder(refs.adminRejectedSourcesEl);
    }
  }

  function scheduleDeferredRender(callback) {
    const scheduleRender = typeof renderScheduler === "function"
      ? renderScheduler
      : renderCallback => {
        renderCallback();
        return () => {};
      };
    scheduleRender(callback);
  }

  async function loadDiscoveryEndpoint(label, promise, fallback, options = {}) {
    try {
      return await promise;
    } catch (err) {
      const message = getErrorMessage(err);
      const registryRefreshDelayedByPipeline = Boolean(
        options?.registryRefresh
        && (
          activePipelineOrFetchRunning()
          || recentlyDetectedActivePipeline()
        )
        && /(timed out|timeout|HTTP 504|\b504\b)/i.test(message)
      );
      if (registryRefreshDelayedByPipeline) {
        state.sourceTablesDelayedDuringActiveRun = true;
        renderSourceTablesDelayed({ onlyIfPlaceholder: true });
        appendDiscoveryLog(ACTIVE_PIPELINE_SOURCE_TABLES_DELAYED_LABEL, "warn");
      } else if (options?.registryRefresh && options?.background && /timed out/i.test(message)) {
        appendDiscoveryLog("Source table refresh delayed; retrying.", "warn");
      } else {
        appendDiscoveryLog(`Could not load ${label}: ${message}`, "error");
      }
      return {
        ...(fallback && typeof fallback === "object" && !Array.isArray(fallback) ? fallback : {}),
        __loadFailed: true
      };
    }
  }

  function scheduleRegistryRefreshRetry(options = {}) {
    if (state.discoveryRegistryRefreshRetryTimer || typeof globalThis.setTimeout !== "function") {
      return;
    }
    state.discoveryRegistryRefreshRetryTimer = globalThis.setTimeout(() => {
      state.discoveryRegistryRefreshRetryTimer = null;
      loadDiscoveryData({
        background: true,
        completionRefresh: true,
        suppressPlaceholders: true,
        logChanges: false,
        fetchReport: options?.fetchReport || null,
        forceFetchReport: Boolean(options?.fetchReport)
      }).catch(() => {});
    }, REGISTRY_REFRESH_RETRY_DELAY_MS);
    if (typeof state.discoveryRegistryRefreshRetryTimer?.unref === "function") {
      state.discoveryRegistryRefreshRetryTimer.unref();
    }
  }

  async function loadDiscoveryData(options = {}) {
    if (state.adminBusyState.discoveryLoad) return state.discoveryLoadPromise || null;
    const nowMs = Date.now();
    const liveDiscoveryRunning = Boolean(
      state.adminBusyState?.liveDiscoveryRunning
      || state.adminBusyState?.discoveryWatch
      || state.discoveryLiveProgressState
    );
    const allowDuringLiveDiscovery = Boolean(
      options?.allowDuringLiveDiscovery
      || options?.completionRefresh
      || options?.forceDuringLiveDiscovery
    );
    if (liveDiscoveryRunning && !allowDuringLiveDiscovery) {
      const background = Boolean(options?.background);
      const lastNoticeAtMs = Number(state.discoveryDeferredLoadNoticeAtMs || 0);
      if (!background && nowMs - lastNoticeAtMs > 5000) {
        state.discoveryDeferredLoadNoticeAtMs = nowMs;
        appendDiscoveryLog(
          "Discovery is running; source tables will refresh after this run completes.",
          "info"
        );
      }
      return {
        skipped: true,
        reason: "discovery_running",
        report: state.latestDiscoveryReportCache || null,
        pendingRows: [],
        activeRows: [],
        rejectedRows: [],
        partialLoadFailed: false
      };
    }
    const livePipelineOrFetchRunning = await refreshActivePipelineStatus();
    const allowDuringActivePipeline = Boolean(options?.forceDuringActivePipeline);
    if (livePipelineOrFetchRunning && !allowDuringActivePipeline) {
      const background = Boolean(options?.background);
      state.sourceTablesDelayedDuringActiveRun = true;
      if (options?.suppressPlaceholders !== true) {
        renderSourceTablesDelayed({ onlyIfPlaceholder: true });
      }
      const lastNoticeAtMs = Number(state.discoveryPipelineDeferredLoadNoticeAtMs || 0);
      if (!background && nowMs - lastNoticeAtMs > 5000) {
        state.discoveryPipelineDeferredLoadNoticeAtMs = nowMs;
        appendDiscoveryLog(ACTIVE_PIPELINE_SOURCE_TABLES_DELAYED_LABEL, "warn");
      }
      return {
        skipped: true,
        reason: "pipeline_running",
        sourceTablesDelayed: true,
        report: state.latestDiscoveryReportCache || null,
        pendingRows: [],
        activeRows: [],
        rejectedRows: [],
        partialLoadFailed: false
      };
    }
    const background = Boolean(options?.background);
    const showPlaceholders = !background && options?.suppressPlaceholders !== true;
    if (showPlaceholders) {
      setSourceTablePlaceholder(refs.adminPendingSourcesEl, "pending");
      setSourceTablePlaceholder(refs.adminActiveSourcesEl, "active");
      setSourceTablePlaceholder(refs.adminRejectedSourcesEl, "rejected");
    }
    const skipIfFreshMs = Math.max(0, Number(options?.skipIfFreshMs || 0));
    const lastLoadAtMs = Number(state.discoveryLastLoadSucceededAtMs || 0);
    if (skipIfFreshMs > 0 && lastLoadAtMs > 0 && nowMs - lastLoadAtMs < skipIfFreshMs) {
      return state.discoveryLoadPromise || null;
    }
    state.discoveryLastLoadStartedAtMs = nowMs;
    const renderToken = ++registryRenderToken;
    setBusyFlag("discoveryLoad", true);
    state.discoveryLoadPromise = (async () => {
      try {
        const filterState = toAdminFilterState();
        const sourceTablesOnly = Boolean(options?.sourceTablesOnly);
        const reportPromise = sourceTablesOnly
          ? Promise.resolve(null)
          : loadDiscoveryEndpoint(
            "source discovery report",
            getBridge("/discovery/report"),
            state.latestDiscoveryReportCache || { summary: {}, candidates: [], failures: [] },
            { background }
          );
        const registrySummaryPromise = options?.completionRefresh
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
        const registrySourcesPath = `/registry/sources?view=table&buckets=pending,active,rejected&includeHiddenPending=${filterState.showZeroJobs ? "1" : "0"}`;
        const registrySourcesPromise = registrySummaryPromise
          .then(registrySummary => loadDiscoveryEndpoint(
            "Admin registry source tables",
            getBridge(registrySourcesPath, { timeoutMs: FULL_REGISTRY_LOAD_TIMEOUT_MS }),
            {
              ok: false,
              sources: { pending: [], active: [], rejected: [] },
              summary: registrySummary?.summary || {}
            },
            { registryRefresh: true, background }
          ));

        const pendingRowsPromise = Promise.all([registrySourcesPromise, discoveryCandidatesPromise, latestFetchReportPromise])
          .then(([registrySources, discoveryCandidates, latestFetchReport]) => {
            const pending = {
              sources: Array.isArray(registrySources?.sources?.pending)
                ? registrySources.sources.pending
                : [],
              summary: registrySources?.summary || {},
              __loadFailed: Boolean(registrySources?.__loadFailed)
            };
            const loadFailed = Boolean(pending?.__loadFailed);
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
                setSourceTableUnavailablePlaceholder(refs.adminPendingSourcesEl, "pending");
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
            return { payload: pending, rows, hiddenZeroJobsCount, visibleRows, loadFailed };
          });
        const activeRowsPromise = Promise.all([registrySourcesPromise, discoveryCandidatesPromise, latestFetchReportPromise])
          .then(([registrySources, discoveryCandidates, latestFetchReport]) => {
            const active = {
              sources: Array.isArray(registrySources?.sources?.active)
                ? registrySources.sources.active
                : [],
              summary: registrySources?.summary || {},
              __loadFailed: Boolean(registrySources?.__loadFailed)
            };
            const loadFailed = Boolean(active?.__loadFailed);
            const rows = mergeSourceStatusFromReport(
              mergeSourceDiscoveryCandidates(Array.isArray(active?.sources) ? active.sources : [], discoveryCandidates),
              latestFetchReport,
              "active"
            );
            const visibleRows = applySourceFilter(rows);
            scheduleDeferredRender(() => {
              if (background || renderToken !== registryRenderToken) return;
              if (loadFailed) {
                setSourceTableUnavailablePlaceholder(refs.adminActiveSourcesEl, "active");
                return;
              }
              renderSourcesTable(refs.adminActiveSourcesEl, visibleRows, "active");
            });
            return { payload: active, rows, visibleRows, loadFailed };
          });
        const rejectedRowsPromise = Promise.all([registrySourcesPromise, discoveryCandidatesPromise, latestFetchReportPromise])
          .then(([registrySources, discoveryCandidates, latestFetchReport]) => {
            const rejected = {
              sources: Array.isArray(registrySources?.sources?.rejected)
                ? registrySources.sources.rejected
                : [],
              summary: registrySources?.summary || {},
              __loadFailed: Boolean(registrySources?.__loadFailed)
            };
            const loadFailed = Boolean(rejected?.__loadFailed);
            const rows = mergeSourceStatusFromReport(
              mergeSourceDiscoveryCandidates(Array.isArray(rejected?.sources) ? rejected.sources : [], discoveryCandidates),
              latestFetchReport,
              "rejected"
            );
            const visibleRows = applySourceFilter(rows);
            scheduleDeferredRender(() => {
              if (background || renderToken !== registryRenderToken) return;
              if (loadFailed) {
                setSourceTableUnavailablePlaceholder(refs.adminRejectedSourcesEl, "rejected");
                return;
              }
              renderSourcesTable(refs.adminRejectedSourcesEl, visibleRows, "rejected");
            });
            return { payload: rejected, rows, visibleRows, loadFailed };
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
        const registrySignature = buildDiscoveryRegistrySignature({
          pending: pendingRows,
          active: activeRows,
          rejected: rejectedRows
        });
        const hiddenZeroJobsCount = pendingResult.hiddenZeroJobsCount;

        if (!sourceTablesOnly && refs.adminDiscoverySummaryEl) {
          const summaryText = `Found ${foundCount} | Probed ${probedCount} | Review queue ${queuedCount} | Deferred review ${deferredCount} | Deferred by caps ${capDeferredCount} | Job-positive deferred ${jobPositiveDeferredCount} | Validated ${lifecycleCounts.validated} | Auto-approved this run ${autoApprovedCount} | Active registry ${activeRegistryCount} (${registryCountBasisLabel}) | Failed ${failedCount} | Skipped dupes ${skippedCount} | Pending ${Number(pending?.summary?.pendingCount || 0)} | Rejected ${Number(rejected?.summary?.rejectedCount || 0)} | Hidden zero-jobs ${hiddenZeroJobsCount}`;
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
        }
        return {
          report,
          pendingRows,
          activeRows,
          rejectedRows,
          partialLoadFailed
        };
      } catch (err) {
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
    })();
    return state.discoveryLoadPromise;
  }

  async function syncSourceTablesAfterTaskCompletion({
    taskType,
    completionSignature,
    fetchReport = null
  } = {}) {
    const normalizedTaskType = String(taskType || "").trim().toLowerCase();
    const signature = String(completionSignature || "").trim();
    if (!normalizedTaskType || !signature) return null;
    const signatureKey = normalizedTaskType === "fetch"
      ? "fetcherSourceSyncSignature"
      : "discoverySourceSyncSignature";
    if (String(state[signatureKey] || "") === signature) {
      return null;
    }
    const result = await loadDiscoveryData({
      background: true,
      fetchReport,
      forceFetchReport: Boolean(fetchReport),
      logChanges: false,
      completionRefresh: true,
      suppressPlaceholders: true
    });
    if (result) {
      if (result.partialLoadFailed) {
        state[signatureKey] = "";
        scheduleRegistryRefreshRetry({ fetchReport });
      } else {
        state[signatureKey] = signature;
      }
    }
    return result;
  }

  async function refreshSourceTablesAfterActiveRunIdle(options = {}) {
    if (!state.sourceTablesDelayedDuringActiveRun) return null;
    if (state.adminBusyState?.discoveryLoad) return state.discoveryLoadPromise || null;
    const stillActive = await refreshActivePipelineStatus({ force: true });
    if (stillActive) return null;
    const result = await loadDiscoveryData({
      sourceTablesOnly: true,
      background: true,
      completionRefresh: true,
      suppressPlaceholders: true,
      logChanges: false,
      fetchReport: options?.fetchReport || null,
      forceFetchReport: Boolean(options?.fetchReport)
    });
    if (result && !result.partialLoadFailed && !result.skipped) {
      state.sourceTablesDelayedDuringActiveRun = false;
    }
    return result;
  }

  return {
    loadDiscoveryData,
    syncSourceTablesAfterTaskCompletion,
    refreshSourceTablesAfterActiveRunIdle
  };
}
