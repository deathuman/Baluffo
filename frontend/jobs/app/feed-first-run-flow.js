/**
 * Jobs feed — first-run bootstrap flow.
 *
 * Split out of ``feed.js``; that module stays the thin coordinator owning
 * the public entrypoints.
 * *
 * The reattach/start/confirm/poll/retry sequence `initJobsFeed` runs on a cold
 * desktop launch, extracted verbatim so the entrypoint stays a coordinator.
 * @module feed-first-run-flow
 */

import { jobsFirstRunBootstrapNumberOverride } from "./feed-filter-match.js";
import {
  FIRST_RUN_BOOTSTRAP_CONFIRMING_STATUS,
  FIRST_RUN_BOOTSTRAP_PROGRESS_STALE_MS,
  FIRST_RUN_BOOTSTRAP_STATUS,
  LOCAL_FEED_MISSING_MESSAGE
} from "./feed-constants.js";
import {
  bootstrapStartHasRunningEvidence,
  bootstrapStartUnconfirmedError,
  isActiveBootstrapReport,
  isFreshBootstrapProgress,
  isSuccessfulJobsFetchReport,
  isTerminalFailedJobsFetchReport,
  isUncertainBootstrapStartError,
  reportRunId
} from "./feed-report-probe.js";
import {
  bootstrapRetryMessage,
  clearBootstrapAutoStart,
  markBootstrapFailed,
  markBootstrapRunning,
  notifyFirstRunBootstrap,
  sleep
} from "./feed-bootstrap-marker.js";

/**
 * Build the first-run bootstrap flow for one ``initJobsFeed`` call.
 *
 * @param {Object} deps the ``initJobsFeed`` dependency bag
 * @returns {Object} the flow steps the entrypoint drives
 */
function createFirstRunBootstrapFlow(deps) {
  const {
    emitMetric,
    recalculateItemsPerPage,
    updateFilterOptions,
    applyStateToFilters,
    applyFiltersAndRender,
    setSourceStatus,
    refreshJobsNow,
    fetchJobsReport,
    fetchJobsTaskLive,
    startJobsBootstrap,
    windowObject,
    setProgress,
    setJobsStartupState,
    bootstrapStartTimeoutMs = 30000,
    bootstrapConfirmTimeoutMs = 20000,
    bootstrapConfirmIntervalMs = 1000,
    bootstrapPollIntervalMs = 1500,
    bootstrapTimeoutMs = jobsFirstRunBootstrapNumberOverride(
      windowObject,
      "jobsFirstRunBootstrapTimeoutMs",
      5 * 60 * 1000
    ),
    bootstrapProgressStaleMs = jobsFirstRunBootstrapNumberOverride(
      windowObject,
      "jobsFirstRunBootstrapProgressStaleMs",
      FIRST_RUN_BOOTSTRAP_PROGRESS_STALE_MS
    ),
    showError,
    setAllJobs,
    showFirstRunBootstrapNotice
  } = deps;

  let firstRunBootstrapNoticeShown = false;

function showBootstrapNoticeOnce(reason) {
    if (firstRunBootstrapNoticeShown) return;
    firstRunBootstrapNoticeShown = true;
    notifyFirstRunBootstrap(showFirstRunBootstrapNotice, reason);
  }

  function setFirstRunStartupState(detail = "first_run_bootstrap") {
    if (typeof setJobsStartupState === "function") {
      setJobsStartupState("interactive", detail);
    }
  }

function renderFirstRunBootstrapState() {
      if (typeof setAllJobs === "function") setAllJobs([]);
      recalculateItemsPerPage();
    updateFilterOptions();
    applyStateToFilters();
    applyFiltersAndRender({
      resetPage: false,
        emptyStateReason: "first_run_bootstrap"
      });
    }

    async function loadCompletedFirstRunFeed(report = null) {
      const candidate = report || (
        typeof fetchJobsReport === "function"
          ? await fetchJobsReport({ timeoutMs: 1500 }).catch(() => null)
          : null
      );
      if (!isSuccessfulJobsFetchReport(candidate)) return false;
      const loaded = await refreshJobsNow({ manual: false, firstLoad: true });
      if (loaded) {
        clearBootstrapAutoStart(windowObject);
        if (typeof setProgress === "function") setProgress(false);
        return true;
      }
      return false;
    }

async function startBootstrapAndLoad({ explicit = false } = {}) {
      if (explicit && await loadCompletedFirstRunFeed()) return true;
      if (explicit) clearBootstrapAutoStart(windowObject);
      markBootstrapRunning(windowObject);
      setFirstRunStartupState();
      if (typeof setProgress === "function") setProgress(true);
      setSourceStatus(FIRST_RUN_BOOTSTRAP_STATUS);
      try {
        if (typeof startJobsBootstrap !== "function") {
          throw new Error("bootstrap route unavailable");
        }
        emitMetric("jobs_first_run_bootstrap_start_requested", { explicit });
        const startedPayload = await startBootstrapWithConfirmation({ explicit });
        markBootstrapRunning(windowObject, { runId: startedPayload?.runId });
        if (startedPayload?.alreadyCompleted) {
          if (typeof setProgress === "function") setProgress(false);
          const loaded = await refreshJobsNow({ manual: false, firstLoad: true });
          if (loaded) {
            clearBootstrapAutoStart(windowObject);
            return true;
          }
          throw new Error(LOCAL_FEED_MISSING_MESSAGE);
        }
        if (!startedPayload?.started && !startedPayload?.alreadyRunning) {
          throw new Error(String(startedPayload?.error || "bootstrap did not start"));
        }
        const pollInterval = Math.max(0, Number(bootstrapPollIntervalMs) || 0);
        const deadline = Date.now() + Math.max(1000, Number(bootstrapTimeoutMs) || 120000);
        let latestFreshProgressAt = 0;
        for (;;) {
          await new Promise(resolve => setTimeout(resolve, pollInterval));
          const now = Date.now();
          const nextReport = await fetchJobsReport({ timeoutMs: 1500 }).catch(() => null);
          if (isSuccessfulJobsFetchReport(nextReport)) {
            if (await loadCompletedFirstRunFeed(nextReport)) return true;
            throw new Error(LOCAL_FEED_MISSING_MESSAGE);
          }
          if (isTerminalFailedJobsFetchReport(nextReport)) {
            throw new Error(bootstrapRetryMessage(nextReport));
          }
          const taskLive = typeof fetchJobsTaskLive === "function"
            ? await fetchJobsTaskLive({ timeoutMs: 1500 }).catch(() => null)
            : null;
          if (
            isFreshBootstrapProgress(taskLive, { now, staleMs: bootstrapProgressStaleMs })
            || isFreshBootstrapProgress(nextReport, { now, staleMs: bootstrapProgressStaleMs })
          ) {
            latestFreshProgressAt = now;
          }
          if (now < deadline) {
            continue;
          }
          const finalReport = await fetchJobsReport({ timeoutMs: 1500 }).catch(() => null);
          if (isSuccessfulJobsFetchReport(finalReport)) {
            if (await loadCompletedFirstRunFeed(finalReport)) return true;
            throw new Error(LOCAL_FEED_MISSING_MESSAGE);
          }
          const finalTaskLive = typeof fetchJobsTaskLive === "function"
            ? await fetchJobsTaskLive({ timeoutMs: 1500 }).catch(() => null)
            : null;
          if (isFreshBootstrapProgress(finalTaskLive, { now: Date.now(), staleMs: bootstrapProgressStaleMs })) {
            latestFreshProgressAt = Date.now();
            continue;
          }
          if (
            latestFreshProgressAt
            && now - latestFreshProgressAt <= Math.max(
              1000,
              Number(bootstrapProgressStaleMs) || FIRST_RUN_BOOTSTRAP_PROGRESS_STALE_MS
            )
          ) {
            continue;
          }
          break;
        }
        throw new Error("first-run sheet refresh timed out");
      } catch (err) {
        if (err?.bootstrapStartUnconfirmed) {
          clearBootstrapAutoStart(windowObject);
        } else {
          markBootstrapFailed(windowObject, String(err?.message || err || ""));
        }
        throw err;
      } finally {
        if (typeof setProgress === "function") setProgress(false);
      }
    }

    async function startBootstrapWithConfirmation({ explicit = false } = {}) {
      try {
        const payload = await startJobsBootstrap({ timeoutMs: bootstrapStartTimeoutMs });
        if (bootstrapStartHasRunningEvidence(payload)) return payload;
        return payload;
      } catch (err) {
        if (!isUncertainBootstrapStartError(err)) throw err;
        emitMetric("jobs_first_run_bootstrap_start_uncertain", {
          explicit,
          error: String(err?.message || err || "")
        });
        setSourceStatus(FIRST_RUN_BOOTSTRAP_CONFIRMING_STATUS);
        const confirmedPayload = await confirmBootstrapStart({ explicit });
        if (confirmedPayload) return confirmedPayload;
        throw bootstrapStartUnconfirmedError();
      }
    }

    async function confirmBootstrapStart({ explicit = false } = {}) {
      const deadline = Date.now() + Math.max(0, Number(bootstrapConfirmTimeoutMs) || 0);
      const interval = Math.max(0, Number(bootstrapConfirmIntervalMs) || 0);
      let retriedStart = false;
      for (;;) {
        const report = typeof fetchJobsReport === "function"
          ? await fetchJobsReport({ timeoutMs: 1500 }).catch(() => null)
          : null;
        if (isSuccessfulJobsFetchReport(report)) {
          return { alreadyCompleted: true, runId: reportRunId(report) };
        }
        if (isTerminalFailedJobsFetchReport(report)) {
          throw new Error(bootstrapRetryMessage(report));
        }
        if (isActiveBootstrapReport(report)) {
          emitMetric("jobs_first_run_bootstrap_start_confirmed", {
            explicit,
            evidence: "report",
            runId: reportRunId(report)
          });
          return { alreadyRunning: true, runId: reportRunId(report) };
        }

        if (!retriedStart) {
          retriedStart = true;
          try {
            const retryPayload = await startJobsBootstrap({ timeoutMs: bootstrapStartTimeoutMs });
            if (bootstrapStartHasRunningEvidence(retryPayload)) {
              emitMetric("jobs_first_run_bootstrap_start_confirmed", {
                explicit,
                evidence: retryPayload?.alreadyRunning ? "already_running" : "retry_start",
                runId: String(retryPayload?.runId || "")
              });
              return retryPayload;
            }
          } catch (retryErr) {
            if (!isUncertainBootstrapStartError(retryErr)) throw retryErr;
            emitMetric("jobs_first_run_bootstrap_start_retry_uncertain", {
              explicit,
              error: String(retryErr?.message || retryErr || "")
            });
          }
        }

        if (Date.now() >= deadline) return null;
        await sleep(interval);
      }
    }

    let retryBootstrapInFlight = false;
    async function retryBootstrap(event) {
      if (retryBootstrapInFlight) return;
      retryBootstrapInFlight = true;
      const retryButton = event?.currentTarget;
      if (retryButton) {
        retryButton.disabled = true;
        retryButton.setAttribute("aria-busy", "true");
      }
      try {
        setFirstRunStartupState("first_run_bootstrap_retry");
        renderFirstRunBootstrapState();
        const ok = await startBootstrapAndLoad({ explicit: true });
        if (!ok) {
          throw new Error("unable to load promoted sheet jobs");
        }
      } catch (err) {
        showError(String(err?.message || "Unable to refresh first-run jobs."), retryBootstrap);
      } finally {
        retryBootstrapInFlight = false;
        if (retryButton?.isConnected) {
          retryButton.disabled = false;
          retryButton.removeAttribute("aria-busy");
        }
      }
    }

  return {
    showBootstrapNoticeOnce,
    setFirstRunStartupState,
    renderFirstRunBootstrapState,
    loadCompletedFirstRunFeed,
    startBootstrapAndLoad,
    startBootstrapWithConfirmation,
    confirmBootstrapStart,
    retryBootstrap
  };
}

export {
  createFirstRunBootstrapFlow
};
