import { createRegistryLoadController } from "./registry/load.js";
import { createRegistryMutationController } from "./registry/mutations.js";
import { createRegistryUi } from "./registry/ui.js";

export function createAdminRegistryController({
  state,
  refs,
  getBridge,
  postBridge,
  fetchJobsFetchReportJson,
  mergeSourceStatusFromReport,
  applySourceFilter,
  getSourceJobsFoundCount,
  deriveSourceStatus,
  renderSourcesTableHtml: renderSourcesTableHtmlImpl,
  readShowZeroJobs,
  normalizeSourceFilter,
  adminDispatch,
  adminActions,
  appendDiscoveryLog,
  formatManualCheckFailureMessage,
  loadOpsHealthData,
  setBusyFlag,
  showToast,
  getErrorMessage
}) {
  const ui = createRegistryUi({
    refs,
    getSourceJobsFoundCount,
    deriveSourceStatus,
    renderSourcesTableHtml: renderSourcesTableHtmlImpl
  });

  const loadController = createRegistryLoadController({
    state,
    refs,
    getBridge,
    fetchJobsFetchReportJson,
    mergeSourceStatusFromReport,
    applySourceFilter,
    getSourceJobsFoundCount,
    normalizeSourceFilter,
    readShowZeroJobs,
    adminDispatch,
    adminActions,
    appendDiscoveryLog,
    getErrorMessage,
    setBusyFlag,
    renderSourcesTable: ui.renderSourcesTable
  });

  const mutationController = createRegistryMutationController({
    state,
    refs,
    postBridge,
    formatManualCheckFailureMessage,
    loadDiscoveryData: loadController.loadDiscoveryData,
    loadOpsHealthData,
    setBusyFlag,
    showToast,
    appendDiscoveryLog,
    getErrorMessage,
    setManualSourceFeedback: ui.setManualSourceFeedback,
    getBucketContainer: ui.getBucketContainer,
    selectedIds: ui.selectedIds,
    selectedSourcesAcrossDiscoveryBuckets: ui.selectedSourcesAcrossDiscoveryBuckets
  });

  return {
    setManualSourceFeedback: ui.setManualSourceFeedback,
    loadDiscoveryData: loadController.loadDiscoveryData,
    syncSourceTablesAfterTaskCompletion: loadController.syncSourceTablesAfterTaskCompletion,
    addManualSource: mutationController.addManualSource,
    approveSelectedSources: mutationController.approveSelectedSources,
    rejectSelectedSources: mutationController.rejectSelectedSources,
    restoreRejectedSources: mutationController.restoreRejectedSources,
    demoteActiveSources: mutationController.demoteActiveSources,
    deleteSelectedSources: mutationController.deleteSelectedSources,
    toggleSelectAllSources: ui.toggleSelectAllSources
  };
}
