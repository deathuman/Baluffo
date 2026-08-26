import { bindAsyncClick, bindUi } from "../../../shared/ui/index.js";
import { navigateDesktopPage } from "../../../shared/local-data/desktop-client.js";
import { createAdminSectionLoadCoordinator } from "./section-loader.js";

function bindWindowResize(handler) {
  window.addEventListener("resize", handler);
}

export function bindAdminRuntimeEvents({
  state,
  refs,
  onRestoreActiveRunWatches,
  getLastJobsUrl,
  fetcherController,
  discoveryController,
  registryController,
  opsController,
  syncController,
  readShowZeroJobs,
  writeShowZeroJobs,
  showZeroJobsKey,
  onSyncDiscoveryLogDisclosure,
  onSetSourceFilter
}) {
  const restoreWatch = () => Promise.resolve(onRestoreActiveRunWatches?.()).catch(() => {});
  [
    ["pageshow", event => {
      if (event?.persisted) restoreWatch();
    }, window]
  ].forEach(([eventName, handler, target]) => target.addEventListener(eventName, handler));

  [
    [refs.adminJobsBtnEl, () => { navigateDesktopPage(getLastJobsUrl()); }],
    [refs.adminSavedBtnEl, () => { navigateDesktopPage("saved.html"); }],
    [refs.adminClearLogBtnEl, () => { fetcherController.setFetcherLogPlaceholder("Output log cleared."); }],
    [refs.adminClearDiscoveryLogBtnEl, event => {
      event.preventDefault();
      event.stopPropagation();
      discoveryController.setDiscoveryLogPlaceholder("Discovery log cleared.");
    }]
  ].forEach(([el, handler]) => bindUi(el, "click", handler));

  [
    [refs.adminRunFetcherBtnEl, () => fetcherController.triggerJobsFetcherTask({ preset: "default" })],
    [refs.adminRunFetcherIncrementalBtnEl, () => fetcherController.triggerJobsFetcherTask({ preset: "incremental" })],
    [refs.adminRunFetcherUncappedBtnEl, () => fetcherController.triggerJobsFetcherTask({ preset: "uncapped" })],
    [refs.adminRunFetcherForceBtnEl, () => fetcherController.triggerJobsFetcherTask({ preset: "force_full" })],
    [refs.adminRefreshReportBtnEl, () => fetcherController.loadLatestFetcherReport()],
    [refs.adminRetryFailedBtnEl, async () => {
      fetcherController.appendFetcherLog(fetcherController.getFetcherPresetMeta("retry_failed").requestedLog, "warn");
      await fetcherController.triggerJobsFetcherTask({ preset: "retry_failed" });
    }],
    [refs.adminCopyFailuresBtnEl, () => fetcherController.copyLatestFailureSummary()],
    [refs.adminRunDiscoveryBtnEl, () => discoveryController.runDiscoveryTask()],
    [refs.adminRunDiscoveryUncappedBtnEl, () => discoveryController.runDiscoveryTask({ preset: "uncapped" })],
    [refs.adminLoadDiscoveryBtnEl, async () => {
      await registryController.loadDiscoveryData();
      await discoveryController.loadDiscoveryLogChunk?.({
        reset: true,
        guarded: false,
        view: "tail",
        limitChars: 65536
      });
    }],
    [refs.adminApproveSourcesBtnEl, () => registryController.approveSelectedSources()],
    [refs.adminRejectSourcesBtnEl, () => registryController.rejectSelectedSources()],
    [refs.adminDeleteSourcesBtnEl, () => registryController.deleteSelectedSources()],
    [refs.adminRestoreRejectedBtnEl, () => registryController.restoreRejectedSources()],
    [refs.adminDemoteActiveBtnEl, () => registryController.demoteActiveSources()],
    [refs.adminAddManualSourceBtnEl, () => registryController.addManualSource()],
    [refs.adminSyncTestBtnEl, () => syncController.testSyncConfig()],
    [refs.adminSyncPullBtnEl, () => syncController.pullSourcesSync()],
    [refs.adminSyncPushBtnEl, () => syncController.pushSourcesSync()]
  ].forEach(([el, handler]) => bindAsyncClick(el, handler));

  [
    [refs.adminPendingSourcesSelectAllEl, "pending"],
    [refs.adminActiveSourcesSelectAllEl, "active"],
    [refs.adminRejectedSourcesSelectAllEl, "rejected"]
  ].forEach(([checkboxEl, type]) => {
    if (!checkboxEl) return;
    checkboxEl.addEventListener("change", () => {
      registryController.toggleSelectAllSources(type, checkboxEl.checked);
    });
  });

  if (refs.adminDiscoveryLogDetailsEl) {
    refs.adminDiscoveryLogDetailsEl.addEventListener("toggle", () => {
      if (state.discoveryLogDetailsSyncing) return;
      state.discoveryLogUserToggled = true;
      state.discoveryLogPreferredOpen = Boolean(refs.adminDiscoveryLogDetailsEl.open);
    });
  }

  if (refs.adminOpsKpisEl) {
    refs.adminOpsKpisEl.addEventListener("toggle", event => {
      const target = event?.target;
      if (!target?.matches?.(".admin-ops-registry-sync-details") || !target.open) return;
      opsController.loadRegistrySyncDiagnosticsData?.({ silent: false }).catch(() => {});
    }, true);
  }

  bindWindowResize(() => {
    onSyncDiscoveryLogDisclosure();
  });

  if (refs.adminManualSourceUrlEl) {
    refs.adminManualSourceUrlEl.addEventListener("keydown", event => {
      if (event.key === "Enter") {
        event.preventDefault();
        registryController.addManualSource().catch(() => {});
      }
    });
  }

  if (refs.adminShowZeroJobsToggleEl) {
    refs.adminShowZeroJobsToggleEl.checked = readShowZeroJobs(showZeroJobsKey);
    refs.adminShowZeroJobsToggleEl.addEventListener("change", () => {
      writeShowZeroJobs(showZeroJobsKey, Boolean(refs.adminShowZeroJobsToggleEl.checked));
      registryController.loadDiscoveryData().catch(() => {});
    });
  }

  if (refs.adminDiscoveryAutoApproveToggleEl) {
    refs.adminDiscoveryAutoApproveToggleEl.addEventListener("input", () => {
      state.discoveryConfigDirty = true;
    });
    refs.adminDiscoveryAutoApproveToggleEl.addEventListener("change", () => {
      state.discoveryConfigDirty = true;
      discoveryController.saveDiscoveryConfig().catch(() => {});
    });
  }

  if (refs.adminSyncEnabledEl) {
    refs.adminSyncEnabledEl.addEventListener("input", () => {
      state.syncConfigDirty = true;
    });
    refs.adminSyncEnabledEl.addEventListener("change", () => {
      state.syncConfigDirty = true;
      syncController.saveSyncConfig().catch(() => {});
    });
  }

  (refs.adminSourceFilterBtnEls || []).forEach(btn => {
    btn.addEventListener("click", () => {
      onSetSourceFilter(String(btn.dataset.sourceFilter || "all").toLowerCase());
      registryController.loadDiscoveryData().catch(() => {});
    });
  });

  createAdminSectionLoadCoordinator({
    state,
    refs,
    opsController,
    fetcherController,
    discoveryController,
    registryController,
    syncController
  }).start();
}
