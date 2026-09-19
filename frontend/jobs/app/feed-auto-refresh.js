/**
 * Jobs feed — admin auto-refresh signal entrypoints.
 *
 * Split out of ``feed.js``; that module stays the thin coordinator owning
 * the public entrypoints.
 *
 * @module feed-auto-refresh
 */

function handleJobsAutoRefreshSignalValue(rawValue, deps) {
  const {
    parseAutoRefreshSignal,
    getLastHandledAutoRefreshSignalId,
    getHasInitializedJobsFeed,
    setPendingAutoRefreshSignal,
    triggerAutoRefreshFromSignal,
    logError
  } = deps;

  const signal = parseAutoRefreshSignal(rawValue);
  if (!signal) return;
  if (signal.id === getLastHandledAutoRefreshSignalId()) return;

  if (!getHasInitializedJobsFeed()) {
    setPendingAutoRefreshSignal(signal);
    return;
  }

  setPendingAutoRefreshSignal(null);
  triggerAutoRefreshFromSignal(signal).catch(err => {
    logError("Auto-refresh from admin signal failed", err);
  });
}

async function applyPendingJobsAutoRefreshSignal(deps) {
  const {
    getPendingAutoRefreshSignal,
    setPendingAutoRefreshSignal,
    readAutoRefreshSignal,
    autoRefreshSignalKey,
    handleAutoRefreshSignalValue,
    triggerAutoRefreshFromSignal
  } = deps;

  const pendingAutoRefreshSignal = getPendingAutoRefreshSignal();
  if (pendingAutoRefreshSignal) {
    setPendingAutoRefreshSignal(null);
    await triggerAutoRefreshFromSignal(pendingAutoRefreshSignal);
    return;
  }

  const latestRaw = readAutoRefreshSignal(autoRefreshSignalKey);
  handleAutoRefreshSignalValue(latestRaw);
}

async function triggerJobsAutoRefreshFromSignal(signal, deps) {
  const {
    getLastHandledAutoRefreshSignalId,
    setSourceStatus,
    getAutoRefreshStatusText,
    refreshJobsNow,
    markAutoRefreshSignalHandled,
    showToast
  } = deps;

  if (!signal?.id) return;
  if (signal.id === getLastHandledAutoRefreshSignalId()) return;
  setSourceStatus(getAutoRefreshStatusText(signal));

  const ok = await refreshJobsNow({ manual: false });
  markAutoRefreshSignalHandled(signal.id);
  if (ok) {
    showToast("Jobs auto-refreshed from latest fetcher run.", "success");
  }
}

export {
  handleJobsAutoRefreshSignalValue,
  applyPendingJobsAutoRefreshSignal,
  triggerJobsAutoRefreshFromSignal
};
