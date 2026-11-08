export function createOverviewHydration({
  state,
  refs,
  getBridge,
  getErrorMessage,
  showToast,
  escapeHtml,
  getObjectValue,
  isLoadedDiscoveryReport,
  isLoadedDedupPayload,
  maybeUnrefTimer,
  loadLatestDiscoveryReport,
  getCachedSourcePolicyPayload,
  getCachedRegistryConflictsPayload,
  renderDeferredOverviewDetails,
  renderSourcePolicyReviewQueue,
  renderRegistryConflictsQueue,
  renderDiscoveryReviewPanel,
  renderAdminOpsDedupListsImpl,
  buildFetcherMetricsPayload,
  handleDedupReviewAction,
  rerenderOpsTabBadges,
  currentRenderToken,
  isStaleRenderToken,
  OPS_HISTORY_DETAIL_PATH,
  OPS_FETCHER_METRICS_DETAIL_PATH,
  OPS_DISCOVERY_AUDIT_ARTIFACTS_PATH,
  OPS_TASK_FAILURE_ATTEMPTS_PATH,
  OPS_BROWSER_FALLBACK_QUEUE_PATH,
  OPS_PERFORMANCE_PROFILE_PATH,
  SOURCE_POLICY_DETAIL_PATH,
  REGISTRY_CONFLICTS_DETAIL_PATH
}) {
  let opsOverviewDetailLoad = null;
  let opsOverviewDetailLoadToken = 0;
  let fetcherMetricsDetailLoad = null;
  let fetcherDiagnosticsLoad = null;
  let sourcePolicyDetailLoad = null;
  let registryConflictsDetailLoad = null;
  let dedupListsDetailLoad = null;
  // ponytail: fixed 50-row pages; tune if conflict payloads grow past ~5k cards
  const REGISTRY_CONFLICTS_PAGE_SIZE = 50;
  // The gateway allows heavy routes 60s; the bridge rebuilds a cold conflict
  // cache in the background, so this budget is only a safety net.
  const REGISTRY_CONFLICTS_DETAIL_TIMEOUT_MS = 60000;
  // Source Policy enriches recommendations against the full registry and its
  // evidence; on a large registry it can exceed the gateway's default 8s
  // proxy budget.
  const SOURCE_POLICY_DETAIL_TIMEOUT_MS = 60000;
  const REGISTRY_CONFLICTS_BUILD_RETRY_MS = 3000;
  const REGISTRY_CONFLICTS_BUILD_MAX_RETRIES = 20;
  const REGISTRY_CONFLICTS_STALE_REFRESH_MS = 5000;
  const REGISTRY_CONFLICTS_STALE_MAX_REFRESHES = 12;

  // Dedup, the diagnostics disclosure, and debug hydration all read the same
  // route; share one request while it is in flight.
  function loadFetcherMetricsDetail() {
    if (fetcherMetricsDetailLoad) return fetcherMetricsDetailLoad;
    fetcherMetricsDetailLoad = getBridge(OPS_FETCHER_METRICS_DETAIL_PATH).finally(() => {
      fetcherMetricsDetailLoad = null;
    });
    return fetcherMetricsDetailLoad;
  }

  function loadOpsOverviewDetailData(renderToken = currentRenderToken()) {
    if (opsOverviewDetailLoad && opsOverviewDetailLoadToken === renderToken) return opsOverviewDetailLoad;
    state.opsDebugDiagnosticsLoading = true;
    opsOverviewDetailLoadToken = renderToken;
    renderDeferredOverviewDetails(renderToken);
    const detailLoad = (async () => {
      const [
        historyResult,
        fetcherMetricsResult,
        auditArtifactsResult,
        taskFailureAttemptsResult,
        browserFallbackQueueResult,
        performanceProfileResult
      ] = await Promise.allSettled([
        getBridge(OPS_HISTORY_DETAIL_PATH),
        loadFetcherMetricsDetail(),
        getBridge(OPS_DISCOVERY_AUDIT_ARTIFACTS_PATH),
        getBridge(OPS_TASK_FAILURE_ATTEMPTS_PATH),
        getBridge(OPS_BROWSER_FALLBACK_QUEUE_PATH),
        getBridge(OPS_PERFORMANCE_PROFILE_PATH)
      ]);
      if (isStaleRenderToken(renderToken)) return;
      let changed = false;
      if (
        historyResult.status === "fulfilled"
        && historyResult.value
        && typeof historyResult.value === "object"
        && !Array.isArray(historyResult.value)
      ) {
        state.latestOpsHistoryPayload = historyResult.value;
        state.opsHistoryLoaded = true;
        state.opsHistoryFullLoaded = true;
        changed = true;
      }
      if (
        fetcherMetricsResult.status === "fulfilled"
        && fetcherMetricsResult.value
        && typeof fetcherMetricsResult.value === "object"
        && !Array.isArray(fetcherMetricsResult.value)
      ) {
        state.latestOpsFetcherMetricsPayload = fetcherMetricsResult.value;
        state.opsFetcherDiagnosticsLoaded = true;
        changed = true;
      }
      if (
        auditArtifactsResult.status === "fulfilled"
        && auditArtifactsResult.value
        && typeof auditArtifactsResult.value === "object"
        && !Array.isArray(auditArtifactsResult.value)
      ) {
        state.latestDiscoveryAuditArtifactsPayload = auditArtifactsResult.value;
        changed = true;
      }
      if (
        taskFailureAttemptsResult.status === "fulfilled"
        && taskFailureAttemptsResult.value
        && typeof taskFailureAttemptsResult.value === "object"
        && !Array.isArray(taskFailureAttemptsResult.value)
      ) {
        state.latestTaskFailureAttemptsPayload = taskFailureAttemptsResult.value;
        changed = true;
      }
      if (
        browserFallbackQueueResult.status === "fulfilled"
        && browserFallbackQueueResult.value
        && typeof browserFallbackQueueResult.value === "object"
        && !Array.isArray(browserFallbackQueueResult.value)
      ) {
        state.latestBrowserFallbackQueuePayload = browserFallbackQueueResult.value;
        changed = true;
      }
      if (
        performanceProfileResult.status === "fulfilled"
        && performanceProfileResult.value
        && typeof performanceProfileResult.value === "object"
        && !Array.isArray(performanceProfileResult.value)
      ) {
        state.latestOpsPerformanceProfilePayload = performanceProfileResult.value;
        changed = true;
      }
      if (changed) {
        state.opsDebugDiagnosticsLoaded = true;
        renderDeferredOverviewDetails(renderToken);
      }
    })().finally(() => {
      if (opsOverviewDetailLoad === detailLoad) {
        opsOverviewDetailLoad = null;
        opsOverviewDetailLoadToken = 0;
        state.opsDebugDiagnosticsLoading = false;
        renderDeferredOverviewDetails(renderToken);
      }
    });
    opsOverviewDetailLoad = detailLoad;
    return opsOverviewDetailLoad;
  }

  function setFetcherDiagnosticsStatus(message) {
    const body = refs?.adminOpsFetcherMetricsEl?.querySelector?.(".admin-ops-fetcher-diagnostics-body");
    if (!body) return;
    body.innerHTML = `<div class="muted">${escapeHtml(message)}</div>`;
  }

  function loadFetcherDiagnosticsData() {
    if (state.opsFetcherDiagnosticsLoaded) {
      renderDeferredOverviewDetails(currentRenderToken());
      return Promise.resolve(state.latestOpsFetcherMetricsPayload || null);
    }
    if (fetcherDiagnosticsLoad) return fetcherDiagnosticsLoad;
    const renderToken = currentRenderToken();
    state.opsFetcherDiagnosticsLoading = true;
    setFetcherDiagnosticsStatus("Loading fetcher diagnostics...");
    fetcherDiagnosticsLoad = (async () => {
      try {
        const [payload, browserFallbackQueue] = await Promise.all([
          loadFetcherMetricsDetail(),
          getBridge(OPS_BROWSER_FALLBACK_QUEUE_PATH).catch(() => null)
        ]);
        if (isStaleRenderToken(renderToken)) return null;
        if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
          throw new Error("fetcher metrics payload was not an object");
        }
        state.latestOpsFetcherMetricsPayload = payload;
        if (
          browserFallbackQueue
          && typeof browserFallbackQueue === "object"
          && !Array.isArray(browserFallbackQueue)
        ) {
          state.latestBrowserFallbackQueuePayload = browserFallbackQueue;
        }
        state.opsFetcherDiagnosticsLoaded = true;
        renderDeferredOverviewDetails(renderToken);
        return payload;
      } catch (err) {
        if (!isStaleRenderToken(renderToken)) {
          setFetcherDiagnosticsStatus(`Could not load fetcher diagnostics: ${getErrorMessage(err)}`);
        }
        return null;
      } finally {
        state.opsFetcherDiagnosticsLoading = false;
        fetcherDiagnosticsLoad = null;
      }
    })();
    return fetcherDiagnosticsLoad;
  }

  function handleLoadDebugDiagnostics() {
    return loadOpsOverviewDetailData(currentRenderToken()).catch(err => {
      showToast(`Could not load debug diagnostics: ${getErrorMessage(err)}`, "error");
    });
  }

  function scheduleOpsOverviewDetailData(renderToken = currentRenderToken()) {
    maybeUnrefTimer(setTimeout(() => {
      loadOpsOverviewDetailData(renderToken).catch(() => {});
    }, 0));
  }

  async function loadSourcePolicyDetail({ force = false } = {}) {
    if (!force && state.sourcePolicyRecommendationsDetailLoaded) {
      return getCachedSourcePolicyPayload();
    }
    if (sourcePolicyDetailLoad) return sourcePolicyDetailLoad;
    sourcePolicyDetailLoad = (async () => {
      try {
        // A panel read must not be reported as the bridge going offline.
        const payload = await getBridge(SOURCE_POLICY_DETAIL_PATH, {
          timeoutMs: SOURCE_POLICY_DETAIL_TIMEOUT_MS,
          affectsReachability: false
        });
        const sourcePolicyRecommendations = payload
          && typeof payload === "object"
          && !Array.isArray(payload)
          ? payload
          : { recommendations: { pairs: [] } };
        state.latestSourcePolicyRecommendationsPayload = sourcePolicyRecommendations;
        state.sourcePolicyRecommendationsDetailLoaded = true;
        renderSourcePolicyReviewQueue(sourcePolicyRecommendations);
        rerenderOpsTabBadges();
        return sourcePolicyRecommendations;
      } catch (err) {
        if (refs.adminSourcePolicyReviewEl) {
          refs.adminSourcePolicyReviewEl.innerHTML = `<div class="muted">${escapeHtml(`Could not load source policy details: ${getErrorMessage(err)}`)}</div>`;
        }
        return null;
      }
    })().finally(() => {
      sourcePolicyDetailLoad = null;
    });
    return sourcePolicyDetailLoad;
  }

  function scheduleRegistryConflictsReload(delayMs) {
    if (state.registryConflictsReloadTimer) return;
    state.registryConflictsReloadTimer = maybeUnrefTimer(setTimeout(() => {
      state.registryConflictsReloadTimer = null;
      if (state.adminOpsActiveTab !== "registry-conflicts") return;
      loadRegistryConflictsDetail({ force: true }).catch(() => {});
    }, delayMs));
  }

  async function loadRegistryConflictsDetail({ force = false } = {}) {
    if (!force && state.registryConflictsDetailLoaded) {
      return getCachedRegistryConflictsPayload();
    }
    if (registryConflictsDetailLoad) return registryConflictsDetailLoad;
    registryConflictsDetailLoad = (async () => {
      try {
        const payload = await getBridge(
          `${REGISTRY_CONFLICTS_DETAIL_PATH}?limit=${REGISTRY_CONFLICTS_PAGE_SIZE}`,
          { timeoutMs: REGISTRY_CONFLICTS_DETAIL_TIMEOUT_MS }
        );
        const registryConflictsPayload = payload
          && typeof payload === "object"
          && !Array.isArray(payload)
          ? payload
          : { summary: { conflictCount: 0 }, conflicts: [] };
        const detailStatus = String(registryConflictsPayload?.detailStatus || "").toLowerCase();
        const fullCacheStatus = String(registryConflictsPayload?.fullCacheStatus || "").toLowerCase();
        state.latestRegistryConflictsPayload = registryConflictsPayload;
        state.registryConflictCheckRunning = String(registryConflictsPayload?.adjudication?.status || "") === "running";
        if (detailStatus === "building") {
          state.registryConflictsDetailLoaded = false;
          state.registryConflictsDetailRetryCount = Number(state.registryConflictsDetailRetryCount || 0) + 1;
          if (refs.adminRegistryConflictsReviewEl) {
            refs.adminRegistryConflictsReviewEl.innerHTML = `<div class="muted">${escapeHtml(
              "Building the registry conflict list on the device. This can take up to a minute on slower hardware."
            )}</div>`;
          }
          rerenderOpsTabBadges();
          if (state.registryConflictsDetailRetryCount <= REGISTRY_CONFLICTS_BUILD_MAX_RETRIES) {
            scheduleRegistryConflictsReload(REGISTRY_CONFLICTS_BUILD_RETRY_MS);
          }
          return registryConflictsPayload;
        }
        state.registryConflictsDetailLoaded = true;
        state.registryConflictsDetailRetryCount = 0;
        if (fullCacheStatus === "stale" && registryConflictsPayload?.refreshInFlight) {
          state.registryConflictsStaleRefreshCount = Number(state.registryConflictsStaleRefreshCount || 0) + 1;
          if (state.registryConflictsStaleRefreshCount <= REGISTRY_CONFLICTS_STALE_MAX_REFRESHES) {
            scheduleRegistryConflictsReload(REGISTRY_CONFLICTS_STALE_REFRESH_MS);
          }
        } else {
          state.registryConflictsStaleRefreshCount = 0;
        }
        renderRegistryConflictsQueue(registryConflictsPayload);
        rerenderOpsTabBadges();
        return registryConflictsPayload;
      } catch (err) {
        if (refs.adminRegistryConflictsReviewEl) {
          refs.adminRegistryConflictsReviewEl.innerHTML = `<div class="muted">${escapeHtml(`Could not load registry conflict details: ${getErrorMessage(err)}`)}</div>`;
        }
        return null;
      }
    })().finally(() => {
      registryConflictsDetailLoad = null;
    });
    return registryConflictsDetailLoad;
  }

  async function loadRegistryConflictsMore() {
    const payload = getObjectValue(state.latestRegistryConflictsPayload);
    if (!payload || payload.summaryView) return null;
    const loaded = Array.isArray(payload.conflicts) ? payload.conflicts.length : 0;
    const total = Number(payload?.summary?.conflictCount || 0);
    if (!total || loaded >= total) return null;
    if (registryConflictsDetailLoad) return registryConflictsDetailLoad;
    registryConflictsDetailLoad = (async () => {
      const next = await getBridge(
        `${REGISTRY_CONFLICTS_DETAIL_PATH}?limit=${REGISTRY_CONFLICTS_PAGE_SIZE}&offset=${loaded}`,
        { timeoutMs: REGISTRY_CONFLICTS_DETAIL_TIMEOUT_MS }
      );
      if (String(next?.detailStatus || "") === "building") return null;
      const cards = Array.isArray(next?.conflicts) ? next.conflicts : [];
      const current = getObjectValue(state.latestRegistryConflictsPayload);
      if (cards.length && current === payload) {
        payload.conflicts = [...(payload.conflicts || []), ...cards];
        payload.returnedCount = payload.conflicts.length;
        renderRegistryConflictsQueue(payload);
      }
      return next;
    })().finally(() => {
      registryConflictsDetailLoad = null;
    });
    return registryConflictsDetailLoad;
  }

  async function loadDiscoveryReviewDetail({ force = false } = {}) {
    const cachedReport = getObjectValue(state.latestDiscoveryReportCache);
    if (!force && isLoadedDiscoveryReport(cachedReport) && cachedReport.candidateReview) {
      renderDiscoveryReviewPanel(cachedReport);
      return cachedReport;
    }
    if (refs.adminDiscoveryReviewEl) {
      refs.adminDiscoveryReviewEl.innerHTML = '<div class="muted">Loading Discovery Review...</div>';
    }
    try {
      const report = typeof loadLatestDiscoveryReport === "function"
        ? await loadLatestDiscoveryReport({ silent: true })
        : await getBridge("/discovery/report");
      if (report && typeof report === "object" && !Array.isArray(report)) {
        state.latestDiscoveryReportCache = report;
        renderDiscoveryReviewPanel(report);
        rerenderOpsTabBadges();
        return report;
      }
      renderDiscoveryReviewPanel({});
      return null;
    } catch (err) {
      if (refs.adminDiscoveryReviewEl) {
        refs.adminDiscoveryReviewEl.innerHTML = `<div class="muted">${escapeHtml(`Could not load Discovery Review: ${getErrorMessage(err)}`)}</div>`;
      }
      return null;
    }
  }

  async function loadDedupListsDetail({ force = false } = {}) {
    const cachedMetrics = getObjectValue(state.latestOpsFetcherMetricsPayload);
    if (!force && isLoadedDedupPayload(cachedMetrics)) {
      renderAdminOpsDedupListsImpl(refs.adminOpsDedupListsEl, buildFetcherMetricsPayload(), {
        onDedupReviewAction: handleDedupReviewAction
      });
      return cachedMetrics;
    }
    if (dedupListsDetailLoad) return dedupListsDetailLoad;
    if (refs.adminOpsDedupListsEl) {
      refs.adminOpsDedupListsEl.innerHTML = '<div class="muted">Loading Dedup Lists...</div>';
    }
    dedupListsDetailLoad = (async () => {
      try {
        const payload = await loadFetcherMetricsDetail();
        const fetcherMetricsPayload = payload && typeof payload === "object" && !Array.isArray(payload)
          ? payload
          : { latestRun: {} };
        state.latestOpsFetcherMetricsPayload = fetcherMetricsPayload;
        state.opsFetcherDiagnosticsLoaded = true;
        renderAdminOpsDedupListsImpl(refs.adminOpsDedupListsEl, buildFetcherMetricsPayload(), {
          onDedupReviewAction: handleDedupReviewAction
        });
        rerenderOpsTabBadges();
        return fetcherMetricsPayload;
      } catch (err) {
        if (refs.adminOpsDedupListsEl) {
          refs.adminOpsDedupListsEl.innerHTML = `<div class="muted">${escapeHtml(`Could not load Dedup Lists: ${getErrorMessage(err)}`)}</div>`;
        }
        return null;
      }
    })().finally(() => {
      dedupListsDetailLoad = null;
    });
    return dedupListsDetailLoad;
  }

  function loadActiveOpsTabDetail(tabKey = state.adminOpsActiveTab || "overview", { force = false } = {}) {
    if (tabKey === "discovery") return loadDiscoveryReviewDetail({ force });
    if (tabKey === "source-policy") return loadSourcePolicyDetail({ force });
    if (tabKey === "registry-conflicts") return loadRegistryConflictsDetail({ force });
    if (tabKey === "dedup") return loadDedupListsDetail({ force });
    return Promise.resolve(null);
  }

  return {
    loadOpsOverviewDetailData,
    loadFetcherDiagnosticsData,
    handleLoadDebugDiagnostics,
    scheduleOpsOverviewDetailData,
    loadSourcePolicyDetail,
    loadRegistryConflictsDetail,
    loadRegistryConflictsMore,
    loadDiscoveryReviewDetail,
    loadDedupListsDetail,
    loadActiveOpsTabDetail
  };
}
