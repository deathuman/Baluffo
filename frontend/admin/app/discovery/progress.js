import {
  appendLiveTaskActivity,
  buildTaskWorkItemActivitySignature,
  markLiveTaskActivity
} from "../live-task.js";
import {
  deriveDiscoveryProgressModel,
  deriveDiscoveryQueuedCount,
  deriveDiscoveryTaskProgress
} from "../../domain/progress.js";
import { applyAdminTaskProgress } from "../progress-ui.js";

export function createAdminDiscoveryProgressController({
  state,
  refs,
  getBridge,
  postBridge,
  setBusyFlag,
  getErrorMessage,
  logAdminError,
  showToast,
  loadDiscoveryData,
  appendDiscoveryLog,
  appendDiscoveryLogEvent
}) {
  function populateDiscoveryConfigForm(savedConfig = {}, { force = false } = {}) {
    if (!refs.adminDiscoveryAutoApproveToggleEl) return;
    if (state.discoveryConfigDirty && !force) return;
    refs.adminDiscoveryAutoApproveToggleEl.checked = savedConfig.autoApproveHealthyPendingOnComplete !== false;
  }

  function collectDiscoveryConfigPayload() {
    return {
      autoApproveHealthyPendingOnComplete: Boolean(refs.adminDiscoveryAutoApproveToggleEl?.checked)
    };
  }

  async function loadDiscoveryConfig(options = {}) {
    const silent = Boolean(options?.silent);
    const forceForm = Boolean(options?.forceForm);
    try {
      const payload = await getBridge("/discovery/config");
      state.latestDiscoveryConfigCache = payload || null;
      populateDiscoveryConfigForm((payload || {}).savedConfig || {}, { force: forceForm });
      return payload || null;
    } catch (err) {
      if (!silent) showToast(`Could not load discovery settings: ${getErrorMessage(err)}`, "error");
      throw err;
    }
  }

  async function loadLatestDiscoveryReport(options = {}) {
    const silent = Boolean(options.silent);
    try {
      const report = await getBridge("/discovery/report");
      if (report && typeof report === "object" && !Array.isArray(report)) {
        state.latestDiscoveryReportCache = report;
      }
      return report || null;
    } catch (err) {
      if (!silent) {
        logAdminError("Failed to load discovery report", err);
      }
      return null;
    }
  }

  async function saveDiscoveryConfig() {
    setBusyFlag("discoveryWrite", true);
    try {
      const result = await postBridge("/discovery/config", collectDiscoveryConfigPayload());
      state.latestDiscoveryConfigCache = result || null;
      state.discoveryConfigDirty = false;
      populateDiscoveryConfigForm((result || {}).savedConfig || {}, { force: true });
      showToast("Discovery auto-approve preference updated.", "success");
      return result || null;
    } catch (err) {
      showToast(`Could not save discovery settings: ${getErrorMessage(err)}`, "error");
      throw err;
    } finally {
      setBusyFlag("discoveryWrite", false);
    }
  }

  function setDiscoveryProgress(view) {
    if (!refs.adminDiscoveryProgressEl || !refs.adminDiscoveryProgressBarEl || !refs.adminDiscoveryProgressLabelEl) {
      return;
    }

    applyAdminTaskProgress(
      refs.adminDiscoveryProgressEl,
      refs.adminDiscoveryProgressBarEl,
      refs.adminDiscoveryProgressLabelEl,
      view
    );
  }

  function getDiscoveryProgressPhaseHint() {
    return String(state.discoveryLiveProgressState?.serverPhaseLabel || "").trim();
  }

  function updateDiscoveryProgressFromReport(report, { running = false } = {}) {
    setDiscoveryProgress(deriveDiscoveryProgressModel(report, {
      running,
      phaseHint: getDiscoveryProgressPhaseHint()
    }));
  }

  function runProgressAppend(report, nowMs) {
    const liveState = state.discoveryLiveProgressState;
    if (!liveState) return;
    updateDiscoveryProgressFromReport(report, { running: true });
    const summary = report?.summary || {};
    const progress = deriveDiscoveryTaskProgress(report, {
      running: true,
      phaseHint: getDiscoveryProgressPhaseHint()
    });
    const phaseLabel = String(progress?.phaseLabel || summary.phaseLabel || summary.phase || "").trim();
    const counts = progress?.counts && typeof progress.counts === "object" ? progress.counts : {};
    const foundCount = Number(counts.foundEndpoints ?? summary.foundEndpointCount ?? 0);
    const probedCount = Number(counts.probedCandidates ?? summary.probedCandidateCount ?? summary.probedCount ?? 0);
    const queuedCount = Number(counts.queuedCandidates ?? deriveDiscoveryQueuedCount(report));
    const deferredCount = Number(counts.deferredCandidates ?? summary.discoverableButDeferredCount ?? 0);
    const failedCount = Number(counts.failedProbes ?? summary.failedProbeCount ?? 0);
    const skippedCount = Number(summary.skippedDuplicateCount || 0);
    const invalidCount = Number(summary.skippedInvalidCount || 0);
    let sawLocalActivity = false;

    const summarySignature = [foundCount, probedCount, queuedCount, deferredCount, failedCount, skippedCount, invalidCount].join("|");
    if (phaseLabel && phaseLabel !== liveState.phaseLabel) {
      liveState.phaseLabel = phaseLabel;
      sawLocalActivity = true;
      appendDiscoveryLog(`Discovery phase: ${phaseLabel}.`, "muted");
    }

    const candidates = Array.isArray(report?.candidates) ? report.candidates : [];
    if (candidates.length > liveState.candidateCount) {
      const nextRows = candidates.slice(liveState.candidateCount, candidates.length);
      const adapterCounts = new Map();
      nextRows.forEach(row => {
        const adapter = String(row?.adapter || "unknown");
        adapterCounts.set(adapter, Number(adapterCounts.get(adapter) || 0) + 1);
      });
      const burstSummary = Array.from(adapterCounts.entries())
        .sort((a, b) => b[1] - a[1])
        .slice(0, 2)
        .map(([adapter, count]) => `${adapter} ${count}`)
        .join(" | ");
      appendDiscoveryLog(
        `New queue burst: +${nextRows.length} candidate${nextRows.length === 1 ? "" : "s"}${burstSummary ? ` (${burstSummary})` : ""}.`,
        "muted"
      );
      liveState.candidateCount = candidates.length;
      sawLocalActivity = true;
    } else {
      liveState.candidateCount = candidates.length;
    }

    const failures = Array.isArray(report?.failures) ? report.failures : [];
    if (failures.length > liveState.failureCount) {
      const nextFailures = failures.slice(liveState.failureCount, failures.length);
      const grouped = new Map();
      nextFailures.forEach(item => {
        const key = String(item?.stage || item?.errorCode || item?.error || "unknown").trim() || "unknown";
        grouped.set(key, Number(grouped.get(key) || 0) + 1);
      });
      const cluster = Array.from(grouped.entries())
        .sort((a, b) => b[1] - a[1])
        .slice(0, 3)
        .map(([label, count]) => `${label} x${count}`)
        .join(" | ");
      appendDiscoveryLog(`Failure cluster: ${cluster}`, "warn");
      liveState.failureCount = failures.length;
      sawLocalActivity = true;
    } else {
      liveState.failureCount = failures.length;
    }

    if (sawLocalActivity) {
      markLiveTaskActivity(liveState, nowMs);
    }

    appendLiveTaskActivity({
      payload: report,
      liveState,
      nowMs,
      appendEvent: event => appendDiscoveryLogEvent(event, "muted"),
      scope: "discovery",
      summarySignature,
      workItemSignature: buildTaskWorkItemActivitySignature(report),
      onSummaryChange: () => {
        appendDiscoveryLog(
          `Discovery: endpoints ${foundCount}, probed ${probedCount}, queued ${queuedCount}, deferred ${deferredCount}, failed ${failedCount}, skipped dupes ${skippedCount}, invalid ${invalidCount}.`,
          failedCount > 0 ? "warn" : "info"
        );
      },
      onHeartbeat: () => {
        appendDiscoveryLog(
          `Discovery active${phaseLabel ? ` (${phaseLabel})` : ""}: endpoints ${foundCount}, probed ${probedCount}, queued ${queuedCount}, deferred ${deferredCount}.`,
          "muted"
        );
      }
    });
  }

  function refreshDiscoveryDataIfNeeded(report) {
    if (typeof loadDiscoveryData !== "function") return;
    const liveState = state.discoveryLiveProgressState;
    if (!liveState) return;
    const summary = report?.summary || {};
    const signature = [
      String(report?.runId || ""),
      String(report?.startedAt || ""),
      String(report?.finishedAt || ""),
      Number(summary.foundEndpointCount || 0),
      Number(summary.probedCandidateCount ?? summary.probedCount ?? 0),
      Number(summary.queuedCandidateCount ?? 0),
      Number(summary.failedProbeCount || 0),
      Number(summary.skippedDuplicateCount || 0)
    ].join("|");
    if (signature === liveState.registryRefreshSignature) return;
    liveState.registryRefreshSignature = signature;
    loadDiscoveryData().catch(() => {});
  }

  return {
    populateDiscoveryConfigForm,
    collectDiscoveryConfigPayload,
    loadDiscoveryConfig,
    loadLatestDiscoveryReport,
    saveDiscoveryConfig,
    setDiscoveryProgress,
    updateDiscoveryProgressFromReport,
    runProgressAppend,
    refreshDiscoveryDataIfNeeded
  };
}
