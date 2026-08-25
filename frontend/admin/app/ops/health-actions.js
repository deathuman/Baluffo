export function createOpsActions({
  state,
  refs,
  postBridge,
  showToast,
  getErrorMessage,
  escapeHtml,
  getObjectValue,
  loadOpsHealthData,
  loadSourcePolicyDetail,
  loadRegistryConflictsMore,
  loadOpsOverviewDetailData,
  loadActiveOpsSummaryData,
  applyOptimisticAbortRow,
  setPendingOpsAbort,
  clearPendingOpsAbort,
  hasPendingOpsAbort,
  isAbortAcceptedResult,
  hasActivePipelineOrFetchRows,
  renderAdminRegistryConflictsImpl,
  renderDiscoveryCandidateReviewHtml,
  toDiscoveryBadgeState,
  renderAdminSourcePolicyReviewImpl,
  currentRenderToken
}) {
  function buildSourcePolicyActionPayload(row, action) {
    const payload = {
      action,
      staticSourceId: String(row?.staticSourceId || ""),
      staticSourceName: String(row?.staticSourceName || ""),
      providerSourceId: String(row?.providerSourceId || ""),
      providerSourceName: String(row?.providerSourceName || "")
    };
    if (action === "snooze") {
      payload.snoozedUntil = new Date(Date.now() + 7 * 24 * 60 * 60 * 1000).toISOString();
    }
    return payload;
  }

  function buildMigrationLinkActionPayload(candidate, action) {
    if (action === "apply_migration_identity_link") {
      return { ...(candidate?.recommendedApiPayload || {}) };
    }
    if (action === "clear_migration_identity_link") {
      return {
        action,
        providerSourceId: String(candidate?.providerSourceId || candidate?.recommendedApiPayload?.providerSourceId || ""),
        staticSourceId: String(
          candidate?.selectedStaticSourceId
          || candidate?.staticSourceId
          || candidate?.migrationSourceIdentity
          || candidate?.recommendedApiPayload?.staticSourceId
          || ""
        )
      };
    }
    return { action };
  }

  function buildDedupReviewActionPayload(row, action) {
    return {
      action,
      title: String(row?.title || ""),
      company: String(row?.company || ""),
      dedupKey: String(row?.dedupKey || ""),
      bundleEvidenceOrigin: String(row?.bundleEvidenceOrigin || ""),
      disagreementClassification: String(row?.disagreementClassification || ""),
      providerSourceJobIds: Array.isArray(row?.providerSourceJobIds) ? row.providerSourceJobIds : [],
      staticSourceJobIds: Array.isArray(row?.staticSourceJobIds) ? row.staticSourceJobIds : [],
      providerSources: Array.isArray(row?.providerSources) ? row.providerSources : [],
      staticSources: Array.isArray(row?.staticSources) ? row.staticSources : [],
      providerUrls: Array.isArray(row?.providerUrls) ? row.providerUrls : [],
      staticUrls: Array.isArray(row?.staticUrls) ? row.staticUrls : [],
      sharedIdentifierTokens: Array.isArray(row?.sharedIdentifierTokens) ? row.sharedIdentifierTokens : [],
      distinctLocationCount: Number(row?.distinctLocationCount || 0),
      sampleLocations: Array.isArray(row?.sampleLocations) ? row.sampleLocations : [],
      identityQuality: String(row?.identityQuality || ""),
      carriedLocationPollutionAudit: String(row?.carriedLocationPollutionAudit || "")
    };
  }

  async function handleSourcePolicyAction(row, action) {
    if (!row || !action) return;
    try {
      await postBridge("/source-policy/review-action", buildSourcePolicyActionPayload(row, action));
      await loadOpsHealthData();
      await loadSourcePolicyDetail({ force: true });
    } catch (err) {
      showToast(`Could not update source policy review: ${getErrorMessage(err)}`, "error");
    }
  }

  // ponytail: sequential per-row POSTs; batch endpoint only if bulk runs feel slow
  let sourcePolicyBulkInFlight = false;
  async function handleSourcePolicyBulkAction(action, selectedRows = []) {
    if (!action || !selectedRows.length) return;
    if (sourcePolicyBulkInFlight) {
      showToast("A bulk review action is already running.", "warn");
      return;
    }
    sourcePolicyBulkInFlight = true;
    let ok = 0;
    let failed = 0;
    try {
      for (const row of selectedRows) {
        try {
          await postBridge("/source-policy/review-action", buildSourcePolicyActionPayload(row, action));
          ok += 1;
        } catch {
          failed += 1;
        }
      }
    } finally {
      sourcePolicyBulkInFlight = false;
    }
    showToast(
      failed
        ? `Bulk ${action}: ${ok.toLocaleString()} applied, ${failed.toLocaleString()} failed.`
        : `Bulk ${action}: ${ok.toLocaleString()} pair${ok === 1 ? "" : "s"} updated.`,
      failed ? "warn" : "success"
    );
    if (ok > 0 && refs?.adminSourcePolicyReviewEl?.dataset) {
      refs.adminSourcePolicyReviewEl.dataset.sourcePolicySelected = "[]";
    }
    await loadOpsHealthData();
    await loadSourcePolicyDetail({ force: true });
  }

  async function handleDedupReviewAction(row, action) {
    if (!row || !action) return;
    try {
      await postBridge("/dedup/review-action", buildDedupReviewActionPayload(row, action));
      await loadOpsHealthData();
    } catch (err) {
      showToast(`Could not update dedup review: ${getErrorMessage(err)}`, "error");
    }
  }

  async function handleCopySectionDiagnostics(section) {
    if (!section || typeof section !== "object" || Array.isArray(section)) return;
    const title = String(section?.title || section?.key || "section");
    const payload = JSON.stringify(section, null, 2);
    if (globalThis.navigator?.clipboard?.writeText) {
      try {
        await globalThis.navigator.clipboard.writeText(payload);
        showToast(`${title} diagnostics copied.`, "success");
        return;
      } catch {
        // Fall through to toast-only failure below.
      }
    }
    showToast(`Could not copy ${title} diagnostics.`, "warn");
  }

  async function handleCopyRunDiagnostics(payload) {
    if (!payload || typeof payload !== "object" || Array.isArray(payload)) return;
    const title = String(payload?.title || payload?.taskType || "Run");
    const serialized = JSON.stringify(payload, null, 2);
    if (globalThis.navigator?.clipboard?.writeText) {
      try {
        await globalThis.navigator.clipboard.writeText(serialized);
        showToast(`${title} run diagnostics copied.`, "success");
        return;
      } catch {
        // Fall through to toast-only failure below.
      }
    }
    showToast(`Could not copy ${title} run diagnostics.`, "warn");
  }

  async function handleRefreshAuditArtifacts() {
    state.latestDiscoveryAuditArtifactsPayload = { ok: true, artifacts: [] };
    try {
      await loadOpsOverviewDetailData(currentRenderToken());
      showToast("Discovery audit artifacts refreshed.", "success");
    } catch (err) {
      showToast(`Could not refresh discovery audit artifacts: ${getErrorMessage(err)}`, "warn");
    }
  }

  async function handleRefreshTaskFailureAttempts() {
    state.latestTaskFailureAttemptsPayload = { ok: true, fetch: {}, discovery: {}, warnings: [] };
    try {
      await loadOpsOverviewDetailData(currentRenderToken());
      showToast("Task failure-attempt diagnostics refreshed.", "success");
    } catch (err) {
      showToast(`Could not refresh task failure-attempt diagnostics: ${getErrorMessage(err)}`, "warn");
    }
  }

  async function handleRefreshPerformanceProfile() {
    state.latestOpsPerformanceProfilePayload = { ok: true, routeTimings: { routes: [] }, operationTimings: { operations: [] } };
    try {
      await loadOpsOverviewDetailData(currentRenderToken());
      showToast("Performance diagnostics refreshed.", "success");
    } catch (err) {
      showToast(`Could not refresh performance diagnostics: ${getErrorMessage(err)}`, "warn");
    }
  }

  async function handleAbortRun(row) {
    const taskType = String(row?.taskType || "").trim().toLowerCase();
    const runId = String(row?.runId || "").trim();
    if (!taskType || !runId) return;
    if (hasPendingOpsAbort(taskType, runId)) {
      showToast("Task abort is already queued.", "info");
      return;
    }
    const confirmed = typeof globalThis.confirm === "function"
      ? globalThis.confirm(`Abort ${taskType} task ${runId}?`)
      : true;
    if (!confirmed) return;
    const pendingAbort = setPendingOpsAbort(taskType, runId, {
      startedAt: String(row?.startedAt || "")
    });
    applyOptimisticAbortRow(taskType, runId, pendingAbort);
    try {
      const result = await postBridge("/tasks/abort", {
        taskType,
        runId,
        reason: "admin_ops_abort",
      });
      if (!isAbortAcceptedResult(result)) {
        throw new Error(String(result?.error || "abort failed"));
      }
      showToast(result?.gatewayAccepted ? "Task abort queued." : "Task abort requested.", "success");
      await loadActiveOpsSummaryData(currentRenderToken(), { fromPoll: false });
    } catch (err) {
      clearPendingOpsAbort(taskType, runId);
      showToast(`Could not abort task: ${getErrorMessage(err)}`, "error");
      if (hasActivePipelineOrFetchRows()) {
        await loadActiveOpsSummaryData(currentRenderToken(), { fromPoll: false });
      }
    }
  }

  async function handleMigrationLinkAction(candidate, action) {
    if (!candidate || !action) return;
    try {
      await postBridge("/source-policy/migration-link-action", buildMigrationLinkActionPayload(candidate, action));
      showToast(
        action === "clear_migration_identity_link"
          ? "Migration identity link cleared."
          : "Migration identity link applied.",
        "success"
      );
      await loadOpsHealthData();
      await loadSourcePolicyDetail({ force: true });
    } catch (err) {
      showToast(`Could not update migration identity link: ${getErrorMessage(err)}`, "error");
    }
  }

  function renderRegistryConflictsQueue(payload = state.latestRegistryConflictsPayload || {}) {
    if (payload?.summaryView && refs.adminRegistryConflictsReviewEl) {
      const conflictCount = Number(payload?.summary?.conflictCount || 0);
      const summaryStatus = String(payload?.summaryStatus || "").toLowerCase();
      refs.adminRegistryConflictsReviewEl.innerHTML = summaryStatus === "pending"
        ? '<div class="muted">Registry conflict summary is loading in the background. Details load when this panel is opened.</div>'
        : summaryStatus === "unavailable"
          ? '<div class="muted">Registry conflict summary is unavailable. Details load when this panel is opened.</div>'
          : conflictCount > 0
        ? `<div class="muted">${escapeHtml(`${conflictCount.toLocaleString()} registry conflict(s) detected. Details load when this panel is opened.`)}</div>`
        : '<div class="muted">No registry conflicts detected.</div>';
      return;
    }
    const adjudication = getObjectValue(payload?.adjudication);
    const conflictCheckRunning = Boolean(state.registryConflictCheckRunning)
      || String(adjudication?.status || "") === "running";
    renderAdminRegistryConflictsImpl(refs.adminRegistryConflictsReviewEl, payload || {}, {
      onRegistryConflictAction: handleRegistryConflictAction,
      onRegistryConflictSafeAutomation: handleRegistryConflictSafeAutomation,
      onRegistryConflictCheck: handleRegistryConflictCheck,
      onRegistryConflictsLoadMore: handleRegistryConflictsLoadMore,
      checkingConflicts: conflictCheckRunning
    });
  }

  async function handleRegistryConflictsLoadMore() {
    try {
      await loadRegistryConflictsMore();
    } catch (err) {
      showToast(`Could not load more registry conflicts: ${getErrorMessage(err)}`, "error");
    }
  }

  function renderDiscoveryReviewPanel(report = state.latestDiscoveryReportCache || {}) {
    if (!refs.adminDiscoveryReviewEl) return;
    const candidateReview = getObjectValue(report?.candidateReview);
    let laneLimits = {};
    try {
      laneLimits = JSON.parse(refs.adminDiscoveryReviewEl?.dataset?.discoveryLaneLimits || "{}") || {};
    } catch {
      laneLimits = {};
    }
    refs.adminDiscoveryReviewEl.innerHTML = renderDiscoveryCandidateReviewHtml(
      candidateReview,
      { showEmpty: true, laneLimits, expandableLanes: true }
    );
    if (!Object.keys(candidateReview).length) {
      const count = toDiscoveryBadgeState(report).count;
      if (count > 0) {
        const suffix = count === 1 ? "" : "s";
        refs.adminDiscoveryReviewEl.innerHTML = `<div class="no-results">${escapeHtml(`${count.toLocaleString()} discovery review item${suffix} counted, but detailed review lanes are not loaded in the latest report.`)}</div>`;
      }
      return;
    }
    refs.adminDiscoveryReviewEl.querySelectorAll?.(".admin-discovery-lane-more-btn").forEach(btn => {
      btn.addEventListener("click", () => {
        const key = String(btn.dataset.discoveryLaneKey || "");
        if (!key) return;
        try {
          laneLimits = JSON.parse(refs.adminDiscoveryReviewEl.dataset.discoveryLaneLimits || "{}") || {};
        } catch {
          laneLimits = {};
        }
        laneLimits[key] = Number(laneLimits[key] || 5) + 10;
        refs.adminDiscoveryReviewEl.dataset.discoveryLaneLimits = JSON.stringify(laneLimits);
        renderDiscoveryReviewPanel(state.latestDiscoveryReportCache || report);
      });
    });
  }

  async function handleRegistryConflictCheck(options = {}) {
    if (state.registryConflictCheckRunning) return;
    state.registryConflictCheckRunning = true;
    renderRegistryConflictsQueue(state.latestRegistryConflictsPayload || {});
    try {
      const result = await postBridge("/registry/conflicts/check-sources", {
        applyAutopilot: Boolean(options?.applyAutopilot)
      });
      const started = Boolean(result?.started);
      const alreadyRunning = Boolean(result?.alreadyRunning);
      const demoted = Number(result?.demoted || 0);
      const checked = Number(result?.checkedSourceCount || 0);
      if (started || alreadyRunning || String(result?.status || "") === "running") {
        showToast(
          alreadyRunning
            ? "Conflict source check is already running."
            : (
              options?.applyAutopilot
                ? "Conflict source check started; high-confidence recommendations will apply when probes finish."
                : "Conflict source check started."
            ),
          "success"
        );
      } else {
        showToast(
          options?.applyAutopilot
            ? `Conflict source check finished: ${demoted} demoted, ${checked} checked.`
            : `Conflict source check finished: ${checked} checked.`,
          "success"
        );
      }
      await loadOpsHealthData();
    } catch (err) {
      showToast(`Could not check conflicting sources: ${getErrorMessage(err)}`, "error");
    } finally {
      state.registryConflictCheckRunning = false;
      renderRegistryConflictsQueue(state.latestRegistryConflictsPayload || {});
    }
  }

  async function handleRegistryConflictSafeAutomation(safeAutomation) {
    if (!safeAutomation || !safeAutomation.action) return;
    const route = String(safeAutomation?.route || "/registry/conflicts/auto-demote-safe").trim();
    const ids = Array.isArray(safeAutomation?.targetIds)
      ? safeAutomation.targetIds.map(id => String(id).trim()).filter(Boolean)
      : [];
    if (!route) return;
    try {
      const result = await postBridge(route, {
        action: String(safeAutomation.action || "auto_demote_same_adapter_provider_alias"),
        ids
      });
      const demoted = Number(result?.demoted || 0);
      const skipped = Number(result?.skipped || 0);
      showToast(`Safe auto-demotion applied: ${demoted} demoted, ${skipped} skipped.`, "success");
      await loadOpsHealthData();
    } catch (err) {
      showToast(`Could not apply safe registry automation: ${getErrorMessage(err)}`, "error");
    }
  }

  async function handleRegistryConflictAction(row, action) {
    if (!row || !action) return;
    const route = String(action?.route || "").trim();
    const ids = Array.isArray(action?.ids) && action.ids.length > 0
      ? action.ids.map(id => String(id).trim()).filter(Boolean)
      : [row?.id, row?.sourceId]
          .map(id => String(id || "").trim())
          .filter(Boolean);
    if (!route || !ids.length) return;
    try {
      const result = await postBridge(route, { ids });
      const count = Number(
        result?.approved
        ?? result?.rejected
        ?? result?.demoted
        ?? result?.restored
        ?? ids.length
      );
      const actionKey = String(action?.action || "").trim().toLowerCase();
      const noun = count === 1 ? "source" : "sources";
      const message = actionKey === "approve"
        ? `Promoted ${count} ${noun}.`
        : actionKey === "reject"
          ? `Rejected ${count} ${noun}.`
          : actionKey === "demote-active"
            ? `Demoted ${count} ${noun}.`
            : actionKey === "restore-rejected"
              ? `Restored ${count} ${noun}.`
              : `${String(action?.label || "Action")} applied to ${count} ${noun}.`;
      showToast(message, "success");
      await loadOpsHealthData();
    } catch (err) {
      showToast(`Could not update registry conflict review: ${getErrorMessage(err)}`, "error");
    }
  }

  function renderSourcePolicyReviewQueue(payload = state.latestSourcePolicyRecommendationsPayload || {}) {
    renderAdminSourcePolicyReviewImpl(refs.adminSourcePolicyReviewEl, payload || {}, {
      selectedFilter: state.sourcePolicyReviewFilter || "all",
      onSourcePolicyFilter: filter => {
        state.sourcePolicyReviewFilter = filter || "all";
        renderSourcePolicyReviewQueue(state.latestSourcePolicyRecommendationsPayload || payload || {});
      },
      onSourcePolicyAction: handleSourcePolicyAction,
      onSourcePolicyBulkAction: handleSourcePolicyBulkAction,
      onMigrationLinkAction: handleMigrationLinkAction
    });
  }

  return {
    handleSourcePolicyAction,
    handleDedupReviewAction,
    handleCopySectionDiagnostics,
    handleCopyRunDiagnostics,
    handleRefreshAuditArtifacts,
    handleRefreshTaskFailureAttempts,
    handleRefreshPerformanceProfile,
    handleAbortRun,
    handleMigrationLinkAction,
    handleRegistryConflictCheck,
    handleRegistryConflictSafeAutomation,
    handleRegistryConflictAction,
    renderRegistryConflictsQueue,
    renderDiscoveryReviewPanel,
    renderSourcePolicyReviewQueue
  };
}
