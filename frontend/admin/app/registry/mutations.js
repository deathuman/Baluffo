import { requestConfirmationDialog } from "../../../local-data/profile-name-dialog.js";

const DISCOVERY_OPERATION_BLOCKED_MESSAGE = "Another discovery operation is running.";

function isDiscoveryOperationBlocked(state) {
  return Boolean(
    state.adminBusyState.discoveryRun
    || state.adminBusyState.discoveryWatch
    || state.adminBusyState.discoveryLoad
    || state.adminBusyState.discoveryWrite
    || state.adminBusyState.manualAdd
    || state.adminBusyState.manualCheck
    || state.adminBusyState.liveDiscoveryRunning
  );
}

async function runRegistryMutation({
  state,
  busyKey,
  setBusyFlag,
  showToast,
  execute,
  onError
}) {
  if (isDiscoveryOperationBlocked(state)) {
    showToast(DISCOVERY_OPERATION_BLOCKED_MESSAGE, "info");
    return null;
  }
  if (state.adminBusyState[busyKey]) {
    showToast("This registry action is already in progress.", "info");
    return null;
  }
  setBusyFlag(busyKey, true);
  try {
    return await execute();
  } catch (err) {
    if (typeof onError === "function") {
      return onError(err);
    }
    throw err;
  } finally {
    setBusyFlag(busyKey, false);
  }
}

export function createRegistryMutationController({
  state,
  refs,
  postBridge,
  formatManualCheckFailureMessage,
  loadDiscoveryData,
  loadOpsHealthData,
  setBusyFlag,
  showToast,
  appendDiscoveryLog,
  getErrorMessage,
  setManualSourceFeedback,
  getBucketContainer,
  selectedIds,
  selectedSourcesAcrossDiscoveryBuckets
}) {
  async function addManualSource() {
    return runRegistryMutation({
      state,
      busyKey: "manualAdd",
      setBusyFlag,
      showToast,
      onError: err => {
        appendDiscoveryLog(`Manual source add failed: ${getErrorMessage(err)}`, "error");
        showToast("Could not add manual source.", "error");
        return null;
      },
      execute: async () => {
        const url = String(refs.adminManualSourceUrlEl?.value || "").trim();
        if (!url) {
          setManualSourceFeedback("invalid URL", "error");
          showToast("Enter a source URL.", "error");
          return null;
        }

        const addResult = await postBridge("/sources/manual", { url });
        const status = String(addResult?.status || "").toLowerCase();

        if (status === "invalid") {
          setManualSourceFeedback("invalid URL", "error");
          appendDiscoveryLog(`Manual source invalid: ${String(addResult?.message || "invalid URL")}`, "error");
          showToast(String(addResult?.message || "Invalid source URL."), "error");
          return null;
        }
        if (status === "duplicate") {
          setManualSourceFeedback("duplicate skipped", "warn");
          appendDiscoveryLog("Manual source duplicate skipped.", "warn");
          showToast("Source already exists. Skipped duplicate.", "info");
          return null;
        }
        if (status !== "added") {
          setManualSourceFeedback("check failed", "error");
          showToast("Could not add manual source.", "error");
          return null;
        }

        if (refs.adminManualSourceUrlEl) refs.adminManualSourceUrlEl.value = "";
        setManualSourceFeedback("added", "success");
        if (String(addResult?.source?.adapter || "").toLowerCase() === "static") {
          appendDiscoveryLog("No known provider detected, using generic website scraping.", "warn");
        }
        appendDiscoveryLog("Manual source added.", "success");

        const sourceId = String(addResult?.sourceId || "");
        if (sourceId) {
          setBusyFlag("manualCheck", true);
          try {
            setManualSourceFeedback("check started", "muted");
            const checkResult = await postBridge("/discovery/check-source", { sourceId });
            if (!checkResult?.started || checkResult?.ok === false) {
              setManualSourceFeedback("check failed", "error");
              appendDiscoveryLog(`Manual source check failed: ${String(checkResult?.error || "unknown error")}`, "error");
              if (Array.isArray(checkResult?.suggestedUrls) && checkResult.suggestedUrls.length) {
                appendDiscoveryLog(`Try alternate URL(s): ${checkResult.suggestedUrls.join(" | ")}`, "warn");
              }
              if (checkResult?.browserFallbackAttempted) {
                appendDiscoveryLog("Browser fallback was attempted during this check.", "muted");
              }
              showToast(formatManualCheckFailureMessage(checkResult), "error");
            } else {
              appendDiscoveryLog(
                `Manual source check completed (jobs found: ${Number(checkResult?.jobsFound || 0)}${checkResult?.weakSignal ? ", weak signal" : ""}).`,
                "success"
              );
              if (checkResult?.browserFallbackUsed) {
                appendDiscoveryLog("Generic browser fallback was used to bypass a blocked page.", "warn");
              }
              showToast("Manual source added and checked.", "success");
            }
          } finally {
            setBusyFlag("manualCheck", false);
          }
        }

        await loadDiscoveryData();
        await loadOpsHealthData();
        return addResult || null;
      }
    });
  }

  async function approveSelectedSources() {
    return runRegistryMutation({
      state,
      busyKey: "discoveryWrite",
      setBusyFlag,
      showToast,
      onError: err => {
        appendDiscoveryLog(`Approve failed: ${getErrorMessage(err)}`, "error");
        showToast("Could not approve sources.", "error");
        return null;
      },
      execute: async () => {
        const ids = selectedIds(getBucketContainer("pending"), ".pending-source-checkbox");
        if (!ids.length) {
          showToast("Select pending sources to approve.", "info");
          return null;
        }
        const result = await postBridge("/registry/approve", { ids });
        appendDiscoveryLog(`Approved ${Number(result?.approved || 0)} source(s).`, "success");
        showToast("Sources approved.", "success");
        await loadDiscoveryData();
        await loadOpsHealthData();
        return result || null;
      }
    });
  }

  async function rejectSelectedSources() {
    return runRegistryMutation({
      state,
      busyKey: "discoveryWrite",
      setBusyFlag,
      showToast,
      onError: err => {
        appendDiscoveryLog(`Reject failed: ${getErrorMessage(err)}`, "error");
        showToast("Could not reject sources.", "error");
        return null;
      },
      execute: async () => {
        const ids = selectedIds(getBucketContainer("pending"), ".pending-source-checkbox");
        if (!ids.length) {
          showToast("Select pending sources to reject.", "info");
          return null;
        }
        const result = await postBridge("/registry/reject", { ids });
        appendDiscoveryLog(`Rejected ${Number(result?.rejected || 0)} source(s).`, "warn");
        showToast("Sources rejected.", "success");
        await loadDiscoveryData();
        await loadOpsHealthData();
        return result || null;
      }
    });
  }

  async function restoreRejectedSources() {
    return runRegistryMutation({
      state,
      busyKey: "discoveryWrite",
      setBusyFlag,
      showToast,
      onError: err => {
        appendDiscoveryLog(`Restore failed: ${getErrorMessage(err)}`, "error");
        showToast("Could not restore rejected sources.", "error");
        return null;
      },
      execute: async () => {
        const ids = selectedIds(getBucketContainer("rejected"), ".rejected-source-checkbox");
        if (!ids.length) {
          showToast("Select rejected sources to restore.", "info");
          return null;
        }
        const result = await postBridge("/registry/restore-rejected", { ids });
        appendDiscoveryLog(`Restored ${Number(result?.restored || 0)} rejected source(s) to pending.`, "success");
        showToast("Rejected sources restored to pending.", "success");
        await loadDiscoveryData();
        await loadOpsHealthData();
        return result || null;
      }
    });
  }

  async function demoteActiveSources() {
    return runRegistryMutation({
      state,
      busyKey: "discoveryWrite",
      setBusyFlag,
      showToast,
      onError: err => {
        appendDiscoveryLog(`Demote failed: ${getErrorMessage(err)}`, "error");
        showToast("Could not demote sources.", "error");
        return null;
      },
      execute: async () => {
        const ids = selectedIds(getBucketContainer("active"), ".active-source-checkbox");
        const result = await postBridge("/registry/demote-active", { ids: ids.length ? ids : [] });
        appendDiscoveryLog(`Demoted ${Number(result?.demoted || 0)} zero-job source(s) to pending.`, "success");
        showToast("Sources demoted to pending.", "success");
        await loadDiscoveryData();
        await loadOpsHealthData();
        return result || null;
      }
    });
  }

  async function deleteSelectedSources() {
    return runRegistryMutation({
      state,
      busyKey: "discoveryWrite",
      setBusyFlag,
      showToast,
      onError: err => {
        appendDiscoveryLog(`Delete failed: ${getErrorMessage(err)}`, "error");
        showToast("Could not delete selected sources.", "error");
        return null;
      },
      execute: async () => {
        const sources = selectedSourcesAcrossDiscoveryBuckets();
        const ids = Array.from(new Set(sources.map(item => item.id).filter(Boolean)));
        const urls = Array.from(new Set(sources.map(item => item.url).filter(Boolean)));
        if (!ids.length && !urls.length) {
          showToast("Select sources to delete.", "info");
          return null;
        }
        const confirmed = await requestConfirmationDialog({
          title: "Delete selected sources?",
          description: `Delete ${sources.length} selected source(s) from registry? This cannot be undone.`,
          confirmLabel: "Delete sources"
        });
        if (!confirmed) {
          return null;
        }
        const result = await postBridge("/registry/delete", { ids, urls });
        appendDiscoveryLog(`Deleted ${Number(result?.deleted || 0)} source(s).`, "warn");
        showToast("Selected sources deleted.", "success");
        await loadDiscoveryData();
        await loadOpsHealthData();
        return result || null;
      }
    });
  }

  return {
    addManualSource,
    approveSelectedSources,
    rejectSelectedSources,
    restoreRejectedSources,
    demoteActiveSources,
    deleteSelectedSources
  };
}
