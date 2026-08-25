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
  OPS_PERFORMANCE_PROFILE_PATH,
  SOURCE_POLICY_DETAIL_PATH,
  REGISTRY_CONFLICTS_DETAIL_PATH
}) {
  let opsOverviewDetailLoad = null;
  let opsOverviewDetailLoadToken = 0;
  let sourcePolicyDetailLoad = null;
  let registryConflictsDetailLoad = null;
  let dedupListsDetailLoad = null;
  // ponytail: fixed 50-row pages; tune if conflict payloads grow past ~5k cards
  const REGISTRY_CONFLICTS_PAGE_SIZE = 50;

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
        performanceProfileResult
      ] = await Promise.allSettled([
        getBridge(OPS_HISTORY_DETAIL_PATH),
        getBridge(OPS_FETCHER_METRICS_DETAIL_PATH),
        getBridge(OPS_DISCOVERY_AUDIT_ARTIFACTS_PATH),
        getBridge(OPS_TASK_FAILURE_ATTEMPTS_PATH),
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
        const payload = await getBridge(SOURCE_POLICY_DETAIL_PATH);
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

  async function loadRegistryConflictsDetail({ force = false } = {}) {
    if (!force && state.registryConflictsDetailLoaded) {
      return getCachedRegistryConflictsPayload();
    }
    if (registryConflictsDetailLoad) return registryConflictsDetailLoad;
    registryConflictsDetailLoad = (async () => {
      try {
        const payload = await getBridge(`${REGISTRY_CONFLICTS_DETAIL_PATH}?limit=${REGISTRY_CONFLICTS_PAGE_SIZE}`);
        const registryConflictsPayload = payload
          && typeof payload === "object"
          && !Array.isArray(payload)
          ? payload
          : { summary: { conflictCount: 0 }, conflicts: [] };
        state.latestRegistryConflictsPayload = registryConflictsPayload;
        state.registryConflictsDetailLoaded = true;
        state.registryConflictCheckRunning = String(registryConflictsPayload?.adjudication?.status || "") === "running";
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
        `${REGISTRY_CONFLICTS_DETAIL_PATH}?limit=${REGISTRY_CONFLICTS_PAGE_SIZE}&offset=${loaded}`
      );
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
        const payload = await getBridge(OPS_FETCHER_METRICS_DETAIL_PATH);
        const fetcherMetricsPayload = payload && typeof payload === "object" && !Array.isArray(payload)
          ? payload
          : { latestRun: {} };
        state.latestOpsFetcherMetricsPayload = fetcherMetricsPayload;
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
