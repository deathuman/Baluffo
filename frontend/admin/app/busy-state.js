function isAdminBusy(busyState, keys) {
  return keys.some(key => Boolean(busyState?.[key]));
}

function isFetcherBusy(busyState) {
  return isAdminBusy(busyState, ["fetcherRun", "fetcherWatch", "fetcherReportLoad", "liveFetchRunning"]);
}

function isDiscoveryBusy(busyState) {
  return isAdminBusy(busyState, [
    "discoveryRun",
    "discoveryWatch",
    "discoveryLoad",
    "discoveryWrite",
    "manualAdd",
    "manualCheck",
    "liveDiscoveryRunning"
  ]);
}

function isOpsBusy(busyState) {
  return Boolean(busyState?.opsLoad);
}

export function isSyncBusy(busyState) {
  return isAdminBusy(busyState, ["syncRun", "liveSyncRunning"]);
}

function isPipelineBusy(busyState) {
  return Boolean(busyState?.livePipelineRunning);
}

function setBusyBadge(el, state, text) {
  if (!el) return;
  const normalized = String(state || "idle").toLowerCase();
  el.classList.remove("idle", "running");
  el.classList.add(normalized === "running" ? "running" : "idle");
  el.textContent = String(text || "");
}

function setButtonBusy(el, busy, busyText, fallbackIdleText = "") {
  if (!el) return;
  if (!el.dataset.idleLabel) {
    el.dataset.idleLabel = String(el.textContent || fallbackIdleText || "").trim();
  }
  if (!el.dataset.idleLabel && fallbackIdleText) {
    el.dataset.idleLabel = String(fallbackIdleText).trim();
  }
  el.disabled = Boolean(busy);
  el.setAttribute("aria-disabled", busy ? "true" : "false");
  if (busy && busyText) {
    el.textContent = String(busyText);
  } else if (!busy && typeof el.dataset.idleLabel === "string") {
    el.textContent = el.dataset.idleLabel;
  }
}

function setPreviewBulkBusyMessage(el, busy) {
  if (!el) return;
  el.textContent = busy
    ? "Source registry actions are paused while Admin work is running."
    : "";
  el.classList?.toggle("hidden", !busy);
}

export function toAdminViewState(busyState, { isUnlocked } = {}) {
  return {
    isUnlocked: Boolean(isUnlocked),
    pipelineBusy: isPipelineBusy(busyState),
    fetcherBusy: isFetcherBusy(busyState),
    discoveryBusy: isDiscoveryBusy(busyState),
    syncBusy: isSyncBusy(busyState)
  };
}

export function syncAdminBusyUi({
  busyState,
  viewState,
  fetcherPresetMeta,
  refs,
  onSyncDiscoveryLogDisclosure
}) {
  const fetcherBusy = viewState.fetcherBusy;
  const discoveryBusy = viewState.discoveryBusy;
  const opsBusy = isOpsBusy(busyState);
  const syncBusy = viewState.syncBusy;
  const pipelineBusy = viewState.pipelineBusy;
  const lockBusy = viewState.pipelineBusy;

  setButtonBusy(refs.adminRunFetcherBtnEl, fetcherBusy || lockBusy, fetcherPresetMeta.default.busyLabel);
  setButtonBusy(refs.adminRunFetcherIncrementalBtnEl, fetcherBusy || lockBusy, fetcherPresetMeta.incremental.busyLabel);
  setButtonBusy(refs.adminRunFetcherUncappedBtnEl, fetcherBusy || lockBusy, fetcherPresetMeta.uncapped.busyLabel);
  setButtonBusy(refs.adminRunFetcherForceBtnEl, fetcherBusy || lockBusy, fetcherPresetMeta.force_full.busyLabel);
  setButtonBusy(refs.adminRetryFailedBtnEl, fetcherBusy || lockBusy, fetcherPresetMeta.retry_failed.busyLabel);
  setButtonBusy(refs.adminRefreshReportBtnEl, Boolean(busyState.fetcherReportLoad), "Loading Report...");
  setButtonBusy(refs.adminRefreshBtnEl, false);
  setButtonBusy(refs.adminRefreshOpsBtnEl, opsBusy, "Refreshing...");
  setButtonBusy(refs.adminSyncTestBtnEl, syncBusy || lockBusy, "Testing...");
  setButtonBusy(refs.adminSyncPullBtnEl, syncBusy || lockBusy, "Pull Running...");
  setButtonBusy(refs.adminSyncPushBtnEl, syncBusy || lockBusy, "Push Running...");
  [refs.adminSyncEnabledEl].forEach(el => {
    if (!el) return;
    el.disabled = syncBusy || lockBusy;
    el.setAttribute("aria-disabled", (syncBusy || lockBusy) ? "true" : "false");
  });

  setButtonBusy(refs.adminRunDiscoveryBtnEl, discoveryBusy || lockBusy, "Discovery Running...", "Run Discovery");
  setButtonBusy(refs.adminRunDiscoveryUncappedBtnEl, discoveryBusy || lockBusy, "Uncapped Discovery Running...", "Run Uncapped Discovery");
  setButtonBusy(refs.adminLoadDiscoveryBtnEl, discoveryBusy || lockBusy, "Loading Discovery...", "Load Discovery");
  const registryBusy = discoveryBusy || lockBusy;
  setPreviewBulkBusyMessage(refs.adminBulkBusyMessageEl, registryBusy);
  setButtonBusy(refs.adminApproveSourcesBtnEl, registryBusy, "", "Approve Sources");
  setButtonBusy(refs.adminRejectSourcesBtnEl, registryBusy, "", "Reject Sources");
  setButtonBusy(refs.adminDeleteSourcesBtnEl, registryBusy, "", "Delete Sources");
  setButtonBusy(refs.adminRestoreRejectedBtnEl, registryBusy, "", "Restore Rejected");
  setButtonBusy(refs.adminDemoteActiveBtnEl, registryBusy, "", "Demote Active");
  setButtonBusy(refs.adminAddManualSourceBtnEl, discoveryBusy || lockBusy, "Adding Source...", "Add Source");
  if (refs.adminManualSourceUrlEl) {
    refs.adminManualSourceUrlEl.disabled = discoveryBusy || lockBusy;
    refs.adminManualSourceUrlEl.setAttribute("aria-disabled", (discoveryBusy || lockBusy) ? "true" : "false");
  }
  refs.adminSourceFilterBtnEls.forEach(btn => {
    btn.disabled = discoveryBusy || lockBusy;
    btn.setAttribute("aria-disabled", (discoveryBusy || lockBusy) ? "true" : "false");
  });

  const fetcherLabel = busyState.fetcherWatch
    ? "Fetcher Running"
    : busyState.liveFetchRunning
      ? "Fetcher Running"
      : busyState.fetcherReportLoad
        ? "Loading Report"
        : "Fetcher Idle";
  const discoveryLabel = busyState.liveDiscoveryRunning ? "Discovery Running" : (discoveryBusy ? "Discovery Busy" : "Discovery Idle");
  const opsLabel = pipelineBusy ? "Pipeline Running" : (opsBusy ? "Ops Refreshing" : "Ops Idle");
  setBusyBadge(refs.adminFetcherProgressBadgeEl, fetcherBusy ? "running" : "idle", fetcherLabel);
  setBusyBadge(refs.adminDiscoveryProgressBadgeEl, discoveryBusy ? "running" : "idle", discoveryLabel);
  setBusyBadge(refs.adminOpsProgressBadgeEl, (opsBusy || pipelineBusy) ? "running" : "idle", opsLabel);
  if (refs.adminContentEl) refs.adminContentEl.classList.toggle("admin-pipeline-locked", viewState.pipelineBusy);
  onSyncDiscoveryLogDisclosure();
}

export function setBusyFlag(busyState, key, value) {
  if (!Object.prototype.hasOwnProperty.call(busyState, key)) return;
  busyState[key] = Boolean(value);
}

export function resetBusyFlags(busyState) {
  Object.keys(busyState).forEach(key => {
    busyState[key] = false;
  });
}
