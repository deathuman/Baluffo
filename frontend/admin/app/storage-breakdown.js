import {
  renderStorageMaintenanceHtml,
  renderStorageMaintenancePreviewHtml,
  renderStorageRebuildHtml,
  renderStorageTablesHtml,
  renderStorageTablesMessageHtml,
  renderStorageWriteLockHtml,
  storageMaintenanceFollowUpScheduled,
  storageMaintenancePhaseLabel,
  storageMaintenanceRetryScheduled
} from "../render/storage-tables.js";
import { renderStorageDiagnosticHtml } from "../render/storage-diagnostic.js";
import {
  downloadStorageDiagnosticJson,
  serializeStorageDiagnosticExport,
  storageDiagnosticExportFilename
} from "./storage-diagnostic-export.js";

const STORAGE_HEALTH_ROUTE = "/ops/storage-health";
const STORAGE_DIAGNOSTIC_ROUTE = "/ops/storage-diagnostic";
const STORAGE_MAINTENANCE_ROUTE = "/ops/storage-maintenance";
const STORAGE_MAINTENANCE_PREVIEW_ROUTE = "/ops/storage-maintenance/preview";
const STORAGE_REBUILD_ROUTE = "/ops/storage-rebuild";
const DEFAULT_STORAGE_HEALTH_TIMEOUT_MS = 5000;
// The preview reads cached generation stats and bounded 200-row samples, then
// stays under the container gateway's existing 60 s heavy-route budget.
const STORAGE_MAINTENANCE_PREVIEW_TIMEOUT_MS = 45 * 1000;
const STORAGE_DIAGNOSTIC_POLL_MS = 2000;
const MAINTENANCE_POLL_FAST_MS = 3000;
const MAINTENANCE_POLL_SLOW_MS = 15000;
const MAINTENANCE_POLL_FAST_WINDOW_MS = 120000;
const MAINTENANCE_POLL_MAX_MS = 30 * 60 * 1000;
// A full current-feed rebuild copies the whole live database and can run for
// hours on a slow disk; keep the panel polling until the run reports terminal.
const REBUILD_POLL_MAX_MS = 12 * 60 * 60 * 1000;

function stateShouldDefer(state, options = {}) {
  const busy = state?.adminBusyState || {};
  return Boolean(
    (!options.allowStartupBridgeLane && state?.adminStartupBridgeHydrationInFlight)
    || busy.discoveryLoad
    || state?.opsActiveAdminWorkLastActive
    || state?.opsActivePipelineOrFetchLastActive
    || busy.livePipelineRunning
    || busy.liveFetchRunning
    || busy.liveDiscoveryRunning
  );
}

/**
 * Owns the Runtime Database Storage panel beside Stored Profiles Overview.
 *
 * The panel reads once, refreshes manually, and accepts the Action Center's
 * storage payload. While an explicit maintenance pass is running it adds one
 * bounded poll: fast for the first two minutes, slower after that, stopped when
 * the pass ends or after 30 minutes, and never scheduled while the active-work
 * gate is held. This keeps maintenance visible without adding a general
 * storage-health poll during fetches or discovery.
 *
 * `load(options)` forwards its options to `shouldDefer` so the startup read can
 * join the serial startup bridge lane (`allowStartupBridgeLane: true`) instead of
 * being deferred by it. Active work still defers unconditionally.
 */

export function createStorageBreakdownController({
  refs,
  getBridge,
  postBridge,
  formatBytes,
  state = {},
  shouldDefer = options => stateShouldDefer(state, options),
  timeoutMs = DEFAULT_STORAGE_HEALTH_TIMEOUT_MS,
  downloadJson = downloadStorageDiagnosticJson
}) {
  let inFlight = false;
  let refreshBound = false;
  let diagnosticInFlight = false;
  let diagnosticPollTimer = null;
  let diagnostic = null;
  let maintenanceInFlight = false;
  let maintenanceCancelInFlight = false;
  let maintenanceStatus = null;
  let maintenanceCompaction = null;
  let maintenancePreview = null;
  let maintenancePreviewInFlight = false;
  let maintenancePreviewRequestId = 0;
  let writeLock = null;
  let maintenancePollTimer = null;
  let maintenancePollStartedAt = 0;
  let maintenanceStatusInFlight = false;
  let rebuild = null;
  let rebuildInFlight = false;
  let rebuildCancelInFlight = false;
  let rebuildStatusInFlight = false;
  let rebuildPollTimer = null;
  let rebuildPollStartedAt = 0;

  function statusText(tableSizes) {
    if (!tableSizes || tableSizes.status === "unavailable") return "Unavailable";
    if (tableSizes.status === "partial" || tableSizes.complete === false) return "Partial snapshot";
    return tableSizes.estimated ? "Estimated snapshot" : "Measured";
  }

  function setStatus(text, tone = "") {
    const statusEl = refs.adminStorageTablesStatusEl;
    if (!statusEl) return;
    statusEl.textContent = text;
    statusEl.setAttribute?.("data-tone", tone);
  }

  function renderMessage(message) {
    if (refs.adminStorageTablesBodyEl) {
      refs.adminStorageTablesBodyEl.innerHTML = renderStorageTablesMessageHtml(message);
    }
  }

  function render(tableSizes) {
    if (!refs.adminStorageTablesBodyEl) return;
    refs.adminStorageTablesBodyEl.innerHTML = renderStorageTablesHtml(tableSizes, formatBytes);
  }

  function diagnosticStatusText(value) {
    if (!value) return { text: "Not run", tone: "" };
    const quick = value.mode === "quick";
    if (value.status === "running" || value.status === "cancelling") {
      return {
        text: value.status === "cancelling"
          ? "Cancelling…"
          : quick ? "Checking…" : "Auditing…",
        tone: ""
      };
    }
    if (value.status === "complete") {
      if (quick) {
        const partial = value.partial === true
          || value.exactContent === false
          || value.jobsByGeneration?.status === "partial"
          || value.rowBytesProbe?.status === "partial";
        return partial
          ? { text: "Partial results", tone: "warning" }
          : { text: "Check complete", tone: "" };
      }
      return value.tableSizes?.status === "unavailable"
        ? { text: "Audit complete — content only", tone: "warning" }
        : { text: "Audit complete", tone: "" };
    }
    if (value.status === "failed") {
      return { text: quick ? "Check failed" : "Audit failed", tone: "warning" };
    }
    if (value.status === "cancelled") return { text: "Cancelled", tone: "warning" };
    return { text: "Not run", tone: "" };
  }

  function setDiagnosticStatus(text, tone = "") {
    const statusEl = refs.adminStorageDiagnosticStatusEl;
    if (!statusEl) return;
    statusEl.textContent = text;
    statusEl.setAttribute?.("data-tone", tone);
  }

  function setExportAvailable(available) {
    const button = refs.adminStorageDiagnosticExportBtnEl;
    if (button) button.hidden = !available;
  }

  function setMaintenanceStatus(text, tone = "") {
    const statusEl = refs.adminStorageMaintenanceStatusEl;
    if (!statusEl) return;
    statusEl.textContent = text;
    statusEl.setAttribute?.("data-tone", tone);
  }

  function maintenanceRunning(status) {
    return Boolean(
      status
        && (
          status.running === true
          || status.status === "running"
          || status.status === "cancelling"
        )
    );
  }

  function compactionRunning(status = maintenanceCompaction) {
    return Boolean(
      status
        && (
          status.running === true
          || String(status.status || "") === "running"
        )
    );
  }

  function maintenanceActive() {
    return maintenanceRunning(maintenanceStatus)
      || compactionRunning()
      || storageMaintenanceFollowUpScheduled(maintenanceStatus)
      || storageMaintenanceRetryScheduled(maintenanceStatus);
  }

  function maintenanceHeadline(status) {
    if (!status) {
      return compactionRunning()
        ? { text: "Compaction is running and cannot be safely interrupted.", tone: "warning" }
        : { text: "", tone: "" };
    }
    const compactionNote = compactionRunning()
      ? " Compaction is running and cannot be safely interrupted."
      : "";
    if (status.status === "cancelling") {
      return {
        text: `${compactionRunning() ? "Feed cleanup stopping" : "Maintenance stopping"} after the current database step…${compactionNote}`,
        tone: "warning"
      };
    }
    if (maintenanceRunning(status)) {
      return {
        text: `Maintenance running — ${storageMaintenancePhaseLabel(status.phase)}${compactionNote}`,
        tone: ""
      };
    }
    if (status.status === "cancelled") {
      return {
        text: compactionRunning()
          ? "Feed cleanup stopped. Compaction is still running and cannot be safely interrupted."
          : "Maintenance stopped. Already removed data stays removed.",
        tone: "warning"
      };
    }
    if (status.status === "failed") {
      const retryScheduled = storageMaintenanceRetryScheduled(status);
      const retryInS = Math.max(0, Number(status.retryInS ?? status.nextRunInS));
      const retryText = retryScheduled
        ? ` Retry scheduled${retryInS > 0 ? ` in ${Math.max(1, Math.round(retryInS))}s` : ""}.`
        : "";
      return {
        text: `Maintenance failed: ${status.error || "unknown error"}.${retryText}${compactionNote}`,
        tone: "warning"
      };
    }
    if (status.status === "held-active-task") {
      return { text: "Maintenance paused while an active task runs.", tone: "warning" };
    }
    if (storageMaintenanceFollowUpScheduled(status)) {
      const followUpInS = Math.max(0, Number(status.followUpInS ?? status.nextRunInS));
      return {
        text: `Maintenance batch complete; follow-up scheduled${followUpInS > 0 ? ` in ${Math.max(1, Math.round(followUpInS))}s` : ""}.${compactionNote}`,
        tone: ""
      };
    }
    if (status.status === "ok" || status.status === "nothing-to-prune") {
      return {
        text: compactionRunning()
          ? "Feed cleanup complete. Compaction is running and cannot be safely interrupted."
          : "Maintenance complete.",
        tone: ""
      };
    }
    if (status.status === "not-run" && compactionRunning()) {
      return { text: "Compaction is running and cannot be safely interrupted.", tone: "warning" };
    }
    return { text: "", tone: "" };
  }

  function renderMaintenanceCancel() {
    const button = refs.adminStorageMaintenanceCancelBtnEl;
    if (!button) return;
    const active = maintenanceRunning(maintenanceStatus);
    const compaction = compactionRunning();
    button.hidden = !active;
    button.disabled = Boolean(
      maintenanceCancelInFlight || maintenanceStatus?.status === "cancelling"
    );
    button.textContent = compaction ? "Stop feed cleanup" : "Stop maintenance";
    button.setAttribute?.(
      "data-tooltip",
      compaction
        ? "Stop feed cleanup after the current batch. Compaction is running and cannot be safely interrupted."
        : "Stop maintenance after the current bounded database step. Already removed data stays removed."
    );
  }

  function renderMaintenanceProgress() {
    const progressEl = refs.adminStorageMaintenanceProgressEl;
    renderMaintenanceCancel();
    if (!progressEl) return;
    const html = renderStorageMaintenanceHtml(
      maintenanceStatus,
      maintenanceCompaction,
      formatBytes
    );
    progressEl.innerHTML = html;
    progressEl.hidden = !html;
  }

  function renderMaintenancePreview() {
    const previewEl = refs.adminStorageMaintenancePreviewEl;
    if (!previewEl) return;
    if (!maintenancePreview) {
      previewEl.hidden = true;
      previewEl.innerHTML = "";
      return;
    }
    previewEl.hidden = false;
    previewEl.innerHTML = renderStorageMaintenancePreviewHtml(maintenancePreview, formatBytes);
  }

  async function previewMaintenance() {
    if (maintenancePreviewInFlight || typeof getBridge !== "function") return null;
    maintenancePreviewInFlight = true;
    const requestId = ++maintenancePreviewRequestId;
    const button = refs.adminStorageMaintenancePreviewBtnEl;
    if (button) {
      button.disabled = true;
      button.setAttribute?.("aria-busy", "true");
    }
    maintenancePreview = { status: "loading" };
    renderMaintenancePreview();
    try {
      const payload = await getBridge(STORAGE_MAINTENANCE_PREVIEW_ROUTE, {
        timeoutMs: STORAGE_MAINTENANCE_PREVIEW_TIMEOUT_MS,
        affectsReachability: false
      });
      if (requestId !== maintenancePreviewRequestId) return null;
      const preview = payload?.maintenancePreview || null;
      maintenancePreview = preview || {
        status: "failed",
        error: payload?.error || "maintenance preview did not return a report"
      };
      renderMaintenancePreview();
      return maintenancePreview;
    } catch (err) {
      if (requestId !== maintenancePreviewRequestId) return null;
      maintenancePreview = {
        status: "failed",
        error: err?.message || err || "unknown error"
      };
      renderMaintenancePreview();
      return null;
    } finally {
      maintenancePreviewInFlight = false;
      if (button) {
        button.disabled = false;
        button.removeAttribute?.("aria-busy");
      }
    }
  }

  function renderWriteLock() {
    const lockEl = refs.adminStorageWriteLockEl;
    if (!lockEl) return;
    lockEl.innerHTML = renderStorageWriteLockHtml(writeLock);
    lockEl.hidden = false;
  }

  function stopMaintenancePoll() {
    if (maintenancePollTimer !== null) {
      clearTimeout(maintenancePollTimer);
      maintenancePollTimer = null;
    }
    maintenancePollStartedAt = 0;
  }

  function scheduleMaintenancePoll() {
    if (!maintenanceActive()) {
      stopMaintenancePoll();
      return;
    }
    if (maintenancePollTimer !== null) return;
    if (shouldDefer()) {
      setMaintenanceStatus(
        maintenanceRunning(maintenanceStatus)
          ? "Maintenance is running. Live updates pause while an active task runs."
          : storageMaintenanceFollowUpScheduled(maintenanceStatus)
            ? "Follow-up is scheduled. Live updates pause while an active task runs."
            : "Compaction is running. Live updates pause while an active task runs.",
        "warning"
      );
      return;
    }
    if (!maintenancePollStartedAt) maintenancePollStartedAt = Date.now();
    const elapsed = Date.now() - maintenancePollStartedAt;
    if (elapsed >= MAINTENANCE_POLL_MAX_MS) {
      setMaintenanceStatus(
        "Maintenance is still running. Live updates paused; use Refresh to resume.",
        "warning"
      );
      return;
    }
    const delay = elapsed < MAINTENANCE_POLL_FAST_WINDOW_MS
      ? MAINTENANCE_POLL_FAST_MS
      : MAINTENANCE_POLL_SLOW_MS;
    maintenancePollTimer = setTimeout(() => {
      maintenancePollTimer = null;
      void refreshMaintenanceStatus();
    }, delay);
    maintenancePollTimer?.unref?.();
  }

  function acceptMaintenanceStatus(storage) {
    if (storage?.runtimeRetention) maintenanceStatus = storage.runtimeRetention;
    if (storage?.compaction) maintenanceCompaction = storage.compaction;
    if (storage?.writeLock !== undefined) writeLock = storage.writeLock;
    renderWriteLock();
    if (!maintenanceStatus) {
      if (compactionRunning()) {
        setMaintenanceStatus("Compaction is running and cannot be safely interrupted.", "warning");
      } else {
        setMaintenanceStatus("", "");
      }
      renderMaintenanceProgress();
      if (maintenanceActive()) scheduleMaintenancePoll();
      else stopMaintenancePoll();
      return;
    }
    const headline = maintenanceHeadline(maintenanceStatus);
    if (headline.text) setMaintenanceStatus(headline.text, headline.tone);
    renderMaintenanceProgress();
    if (maintenanceActive()) scheduleMaintenancePoll();
    else {
      stopMaintenancePoll();
      if (maintenanceStatus.status === "not-run") setMaintenanceStatus("", "");
    }
  }

  async function refreshMaintenanceStatus() {
    if (maintenanceStatusInFlight) return null;
    maintenanceStatusInFlight = true;
    try {
      const payload = await getBridge(STORAGE_HEALTH_ROUTE, {
        timeoutMs: DEFAULT_STORAGE_HEALTH_TIMEOUT_MS,
        affectsReachability: false
      });
      acceptStorageHealth(payload);
      return payload?.storage?.runtimeRetention || null;
    } catch (err) {
      if (maintenanceActive()) {
        setMaintenanceStatus(
          `Maintenance status unavailable: ${err?.message || err || "unknown error"}. Retrying…`,
          "warning"
        );
        scheduleMaintenancePoll();
      }
      return null;
    } finally {
      maintenanceStatusInFlight = false;
    }
  }

  function renderDiagnostic() {
    const status = diagnosticStatusText(diagnostic);
    setDiagnosticStatus(status.text, status.tone);
    setExportAvailable(diagnostic?.status === "complete");
    if (refs.adminStorageDiagnosticCancelBtnEl) {
      refs.adminStorageDiagnosticCancelBtnEl.hidden = !(
        diagnostic?.status === "running" || diagnostic?.status === "cancelling"
      );
    }
    if (refs.adminStorageDiagnosticBodyEl) {
      refs.adminStorageDiagnosticBodyEl.innerHTML = renderStorageDiagnosticHtml(
        diagnostic,
        formatBytes
      );
    }
  }

  function clearDiagnosticPoll() {
    if (diagnosticPollTimer !== null) {
      clearTimeout(diagnosticPollTimer);
      diagnosticPollTimer = null;
    }
  }

  function scheduleDiagnosticPoll() {
    clearDiagnosticPoll();
    if (diagnostic?.status !== "running" && diagnostic?.status !== "cancelling") return;
    diagnosticPollTimer = setTimeout(() => {
      diagnosticPollTimer = null;
      void refreshDiagnostic();
    }, STORAGE_DIAGNOSTIC_POLL_MS);
  }

  function acceptDiagnostic(payload) {
    diagnostic = payload?.storageDiagnostic || null;
    renderDiagnostic();
    scheduleDiagnosticPoll();
    return diagnostic;
  }

  async function refreshDiagnostic() {
    if (diagnosticInFlight) return null;
    diagnosticInFlight = true;
    try {
      const payload = await getBridge(STORAGE_DIAGNOSTIC_ROUTE, {
        timeoutMs: DEFAULT_STORAGE_HEALTH_TIMEOUT_MS,
        affectsReachability: false
      });
      return acceptDiagnostic(payload);
    } catch (err) {
      clearDiagnosticPoll();
      setDiagnosticStatus("Unavailable", "warning");
      setExportAvailable(false);
      if (refs.adminStorageDiagnosticBodyEl) {
        refs.adminStorageDiagnosticBodyEl.innerHTML = renderStorageTablesMessageHtml(
          `Could not load the storage diagnostic: ${err?.message || err || "unknown error"}`
        );
      }
      return null;
    } finally {
      diagnosticInFlight = false;
    }
  }

  async function startStorageDiagnostic(mode = "exact") {
    if (typeof postBridge !== "function") return null;
    const quick = mode === "quick";
    clearDiagnosticPoll();
    setExportAvailable(false);
    setDiagnosticStatus(quick ? "Starting check…" : "Starting full audit…");
    if (refs.adminStorageDiagnosticBodyEl) {
      refs.adminStorageDiagnosticBodyEl.innerHTML = renderStorageTablesMessageHtml(
        quick
          ? "Starting the quick check…"
          : "Starting the full storage audit…"
      );
    }
    try {
      const payload = await postBridge(
        STORAGE_DIAGNOSTIC_ROUTE,
        quick ? { mode: "quick" } : {},
        { timeoutMs: DEFAULT_STORAGE_HEALTH_TIMEOUT_MS, affectsReachability: false }
      );
      return acceptDiagnostic(payload);
    } catch (err) {
      setExportAvailable(false);
      setDiagnosticStatus(quick ? "Check failed" : "Audit failed", "warning");
      if (refs.adminStorageDiagnosticBodyEl) {
        refs.adminStorageDiagnosticBodyEl.innerHTML = renderStorageTablesMessageHtml(
          `Could not start the ${quick ? "quick check" : "full audit"}: ${err?.message || err || "unknown error"}`
        );
      }
      return null;
    }
  }

  function startExactDiagnostic() {
    return startStorageDiagnostic("exact");
  }

  function startQuickCheck() {
    return startStorageDiagnostic("quick");
  }

  async function cancelExactDiagnostic() {
    if (typeof postBridge !== "function") return null;
    const runId = String(diagnostic?.runId || "").trim();
    if (!runId) {
      setDiagnosticStatus(
        "The audit stop needs the current run id; refresh the audit and try again.",
        "warning"
      );
      return null;
    }
    try {
      const payload = await postBridge(
        `${STORAGE_DIAGNOSTIC_ROUTE}/cancel`,
        { runId },
        {
          timeoutMs: DEFAULT_STORAGE_HEALTH_TIMEOUT_MS,
          affectsReachability: false,
          allowStatuses: [409]
        }
      );
      if (payload?.stale) {
        acceptDiagnostic(payload);
        setDiagnosticStatus(
          "Stop request was stale: the audit has already moved on. Refresh before trying again.",
          "warning"
        );
        return diagnostic;
      }
      return acceptDiagnostic(payload);
    } catch (err) {
      setExportAvailable(false);
      setDiagnosticStatus("Unavailable", "warning");
      if (refs.adminStorageDiagnosticBodyEl) {
        refs.adminStorageDiagnosticBodyEl.innerHTML = renderStorageTablesMessageHtml(
          `Could not cancel the full audit: ${err?.message || err || "unknown error"}`
        );
      }
      return null;
    }
  }

  async function cancelMaintenance() {
    if (
      maintenanceCancelInFlight
      || typeof postBridge !== "function"
      || !maintenanceRunning(maintenanceStatus)
    ) {
      return null;
    }
    const runId = String(maintenanceStatus?.runId || "").trim();
    if (!runId) {
      setMaintenanceStatus(
        "Maintenance stop needs the current run id; refresh the status and try again.",
        "warning"
      );
      return null;
    }
    maintenanceCancelInFlight = true;
    const previousStatus = maintenanceStatus;
    if (maintenanceStatus) {
      maintenanceStatus = {
        ...maintenanceStatus,
        status: "cancelling",
        phase: "cancelling",
        stage: "cancelling",
        cancelRequested: true
      };
    }
    setMaintenanceStatus(
      "Stopping maintenance after the current database step…",
      "warning"
    );
    renderMaintenanceProgress();
    try {
      const payload = await postBridge(
        `${STORAGE_MAINTENANCE_ROUTE}/cancel`,
        { runId },
        {
          timeoutMs: DEFAULT_STORAGE_HEALTH_TIMEOUT_MS,
          affectsReachability: false,
          allowStatuses: [409]
        }
      );
      const runtimeRetention = payload?.runtimeRetention || null;
      acceptMaintenanceStatus({ runtimeRetention, compaction: payload?.compaction });
      if (!runtimeRetention) scheduleMaintenancePoll();
      if (payload?.stale) {
        setMaintenanceStatus(
          "Stop request was stale: maintenance has already moved on. Refresh before trying again.",
          "warning"
        );
        scheduleMaintenancePoll();
        return payload;
      }
      if (payload?.ok === false) {
        maintenanceStatus = previousStatus;
        setMaintenanceStatus(
          `Could not stop maintenance: ${payload?.error || "unknown error"}`,
          "warning"
        );
        scheduleMaintenancePoll();
      }
      return payload;
    } catch (err) {
      maintenanceStatus = previousStatus;
      setMaintenanceStatus(
        `Could not stop maintenance: ${err?.message || err || "unknown error"}`,
        "warning"
      );
      scheduleMaintenancePoll();
      return null;
    } finally {
      maintenanceCancelInFlight = false;
      renderMaintenanceProgress();
    }
  }

  async function startMaintenance() {
    if (maintenanceInFlight || typeof postBridge !== "function") return null;
    maintenanceInFlight = true;
    maintenancePreviewRequestId += 1;
    maintenancePreview = null;
    renderMaintenancePreview();
    maintenancePollStartedAt = 0;
    const button = refs.adminStorageMaintenanceBtnEl;
    if (button) button.disabled = true;
    setMaintenanceStatus("Starting maintenance…");
    try {
      const payload = await postBridge(
        STORAGE_MAINTENANCE_ROUTE,
        {},
        { timeoutMs: DEFAULT_STORAGE_HEALTH_TIMEOUT_MS, affectsReachability: false }
      );
      const runtimeRetention = payload?.runtimeRetention || null;
      acceptMaintenanceStatus({ runtimeRetention, compaction: payload?.compaction });
      if (payload?.started) {
        if (!runtimeRetention || !maintenanceRunning(runtimeRetention)) {
          maintenanceStatus = {
            ...(runtimeRetention || {}),
            status: "running",
            running: true,
            phase: runtimeRetention?.phase || "starting",
            elapsedMs: runtimeRetention?.elapsedMs || 0
          };
          renderMaintenanceProgress();
          setMaintenanceStatus(
            "Maintenance started — older saved versions will be removed and the database compacted in the background."
          );
        }
        scheduleMaintenancePoll();
      } else if (payload?.alreadyRunning) {
        if (!runtimeRetention || !maintenanceRunning(runtimeRetention)) {
          maintenanceStatus = {
            ...(runtimeRetention || {}),
            status: "running",
            running: true,
            phase: runtimeRetention?.phase || "starting",
            elapsedMs: runtimeRetention?.elapsedMs || 0
          };
          renderMaintenanceProgress();
          setMaintenanceStatus("Maintenance is already running.", "warning");
        }
        scheduleMaintenancePoll();
      } else if (payload?.reason === "held-active-task") {
        setMaintenanceStatus(
          "Deferred while an active task is running; try again when it finishes.",
          "warning"
        );
      } else {
        setMaintenanceStatus(
          `Maintenance did not start: ${payload?.error || payload?.reason || "unknown reason"}`,
          "warning"
        );
      }
      return payload;
    } catch (err) {
      setMaintenanceStatus(
        `Could not start maintenance: ${err?.message || err || "unknown error"}`,
        "warning"
      );
      return null;
    } finally {
      maintenanceInFlight = false;
      if (button) button.disabled = false;
    }
  }

  function rebuildRunning(value = rebuild) {
    return Boolean(
      value
        && (
          value.running === true
          || String(value.status || "") === "running"
          || String(value.status || "") === "cancelling"
        )
    );
  }

  function rebuildStatusText(value) {
    if (!value) return { text: "Not run", tone: "" };
    const status = String(value.status || "idle");
    if (status === "running") return { text: "Rebuild running", tone: "" };
    if (status === "cancelling") return { text: "Stopping rebuild…", tone: "warning" };
    if (status === "complete") return { text: "Rebuild complete", tone: "" };
    if (status === "superseded") {
      return { text: "Rebuild superseded by a database write", tone: "warning" };
    }
    if (status === "interrupted") {
      return { text: "Rebuild interrupted by a restart", tone: "warning" };
    }
    if (status === "failed") return { text: "Rebuild failed", tone: "warning" };
    if (status === "cancelled") return { text: "Rebuild stopped", tone: "warning" };
    if (status === "rolled-back") return { text: "Old database restored", tone: "" };
    return { text: "Not run", tone: "" };
  }

  function setRebuildStatus(text, tone = "") {
    const statusEl = refs.adminStorageRebuildStatusEl;
    if (!statusEl) return;
    statusEl.textContent = text;
    statusEl.setAttribute?.("data-tone", tone);
  }

  function renderRebuild() {
    if (refs.adminStorageRebuildBodyEl) {
      refs.adminStorageRebuildBodyEl.innerHTML = renderStorageRebuildHtml(rebuild, formatBytes);
    }
    const status = rebuildStatusText(rebuild);
    setRebuildStatus(status.text, status.tone);
    const active = rebuildRunning();
    const runButton = refs.adminStorageRebuildBtnEl;
    if (runButton) {
      runButton.disabled = rebuildInFlight || active;
      runButton.hidden = active;
    }
    const cancelButton = refs.adminStorageRebuildCancelBtnEl;
    if (cancelButton) {
      cancelButton.hidden = !active;
      cancelButton.disabled = rebuildCancelInFlight || rebuild?.status === "cancelling";
    }
    const canRestore = rebuild?.canRollback === true && !active;
    if (refs.adminStorageRebuildRollbackBtnEl) {
      refs.adminStorageRebuildRollbackBtnEl.hidden = !canRestore;
      refs.adminStorageRebuildRollbackBtnEl.disabled = rebuildInFlight;
    }
    if (refs.adminStorageRebuildBackupDeleteBtnEl) {
      refs.adminStorageRebuildBackupDeleteBtnEl.hidden = !canRestore;
      refs.adminStorageRebuildBackupDeleteBtnEl.disabled = rebuildInFlight;
    }
  }

  function stopRebuildPoll() {
    if (rebuildPollTimer !== null) {
      clearTimeout(rebuildPollTimer);
      rebuildPollTimer = null;
    }
    rebuildPollStartedAt = 0;
  }

  function scheduleRebuildPoll() {
    if (!rebuildRunning()) {
      stopRebuildPoll();
      return;
    }
    if (rebuildPollTimer !== null) return;
    if (shouldDefer()) return;
    if (!rebuildPollStartedAt) rebuildPollStartedAt = Date.now();
    const elapsed = Date.now() - rebuildPollStartedAt;
    if (elapsed >= REBUILD_POLL_MAX_MS) return;
    const delay = elapsed < MAINTENANCE_POLL_FAST_WINDOW_MS
      ? MAINTENANCE_POLL_FAST_MS
      : MAINTENANCE_POLL_SLOW_MS;
    rebuildPollTimer = setTimeout(() => {
      rebuildPollTimer = null;
      void refreshRebuildStatus();
    }, delay);
    rebuildPollTimer?.unref?.();
  }

  function acceptRebuild(payload) {
    if (payload?.storageRebuild !== undefined) rebuild = payload.storageRebuild;
    renderRebuild();
    if (rebuildRunning()) scheduleRebuildPoll();
    else stopRebuildPoll();
    return rebuild;
  }

  async function refreshRebuildStatus() {
    if (rebuildStatusInFlight || typeof getBridge !== "function") return null;
    rebuildStatusInFlight = true;
    try {
      const payload = await getBridge(STORAGE_REBUILD_ROUTE, {
        timeoutMs: DEFAULT_STORAGE_HEALTH_TIMEOUT_MS,
        affectsReachability: false
      });
      return acceptRebuild(payload);
    } catch (err) {
      if (rebuildRunning()) {
        setRebuildStatus(
          `Rebuild status unavailable: ${err?.message || err || "unknown error"}. Retrying…`,
          "warning"
        );
        scheduleRebuildPoll();
      }
      return null;
    } finally {
      rebuildStatusInFlight = false;
    }
  }

  async function startRebuild() {
    if (rebuildInFlight || typeof postBridge !== "function") return null;
    rebuildInFlight = true;
    rebuildPollStartedAt = 0;
    setRebuildStatus("Starting rebuild…");
    try {
      const payload = await postBridge(
        STORAGE_REBUILD_ROUTE,
        {},
        { timeoutMs: DEFAULT_STORAGE_HEALTH_TIMEOUT_MS, affectsReachability: false }
      );
      acceptRebuild(payload);
      if (!payload?.started && !payload?.alreadyRunning) {
        const reason = String(payload?.reason || "");
        if (reason === "held-active-task") {
          setRebuildStatus(
            "Rebuild is deferred while an active task runs; try again when it finishes.",
            "warning"
          );
        } else if (reason === "backup-exists") {
          setRebuildStatus(
            "A rollback copy already exists. Roll back or delete it before rebuilding again.",
            "warning"
          );
        } else if (reason === "compaction-running") {
          setRebuildStatus("A compaction is running; wait for it to finish.", "warning");
        } else if (payload?.error) {
          setRebuildStatus(`Rebuild did not start: ${payload.error}`, "warning");
        }
      }
      if (payload?.started) scheduleRebuildPoll();
      return payload;
    } catch (err) {
      setRebuildStatus(
        `Could not start the rebuild: ${err?.message || err || "unknown error"}`,
        "warning"
      );
      return null;
    } finally {
      rebuildInFlight = false;
      renderRebuild();
    }
  }

  async function cancelRebuild() {
    if (
      rebuildCancelInFlight
      || typeof postBridge !== "function"
      || !rebuildRunning()
    ) {
      return null;
    }
    const runId = String(rebuild?.runId || "").trim();
    if (!runId) {
      setRebuildStatus(
        "Stopping the rebuild needs the current run id; refresh and try again.",
        "warning"
      );
      return null;
    }
    rebuildCancelInFlight = true;
    if (rebuild) rebuild = { ...rebuild, status: "cancelling", phase: "cancelling" };
    renderRebuild();
    try {
      const payload = await postBridge(
        `${STORAGE_REBUILD_ROUTE}/cancel`,
        { runId },
        {
          timeoutMs: DEFAULT_STORAGE_HEALTH_TIMEOUT_MS,
          affectsReachability: false,
          allowStatuses: [409]
        }
      );
      acceptRebuild(payload);
      if (payload?.stale) {
        setRebuildStatus(
          "Stop request was stale: the rebuild already moved on. Refresh before trying again.",
          "warning"
        );
      }
      return payload;
    } catch (err) {
      setRebuildStatus(
        `Could not stop the rebuild: ${err?.message || err || "unknown error"}`,
        "warning"
      );
      return null;
    } finally {
      rebuildCancelInFlight = false;
      renderRebuild();
    }
  }

  async function rollbackRebuild() {
    if (rebuildInFlight || typeof postBridge !== "function") return null;
    rebuildInFlight = true;
    setRebuildStatus("Restoring the old database…");
    try {
      const payload = await postBridge(
        `${STORAGE_REBUILD_ROUTE}/rollback`,
        {},
        {
          timeoutMs: DEFAULT_STORAGE_HEALTH_TIMEOUT_MS,
          affectsReachability: false,
          allowStatuses: [409]
        }
      );
      acceptRebuild(payload);
      if (!payload?.ok) {
        setRebuildStatus(
          `Could not restore the old database: ${payload?.error || "unknown error"}`,
          "warning"
        );
      }
      void load();
      return payload;
    } catch (err) {
      setRebuildStatus(
        `Could not restore the old database: ${err?.message || err || "unknown error"}`,
        "warning"
      );
      return null;
    } finally {
      rebuildInFlight = false;
      renderRebuild();
    }
  }

  async function deleteRebuildBackup() {
    if (rebuildInFlight || typeof postBridge !== "function") return null;
    rebuildInFlight = true;
    setRebuildStatus("Deleting the retained old database…");
    try {
      const payload = await postBridge(
        `${STORAGE_REBUILD_ROUTE}/backup-delete`,
        {},
        {
          timeoutMs: DEFAULT_STORAGE_HEALTH_TIMEOUT_MS,
          affectsReachability: false,
          allowStatuses: [409]
        }
      );
      acceptRebuild(payload);
      if (!payload?.ok) {
        setRebuildStatus(
          `Could not delete the old database: ${payload?.error || "unknown error"}`,
          "warning"
        );
      }
      return payload;
    } catch (err) {
      setRebuildStatus(
        `Could not delete the old database: ${err?.message || err || "unknown error"}`,
        "warning"
      );
      return null;
    } finally {
      rebuildInFlight = false;
      renderRebuild();
    }
  }

  function exportExactDiagnostic() {
    if (diagnostic?.status !== "complete") return null;
    const exportedAt = new Date();
    const filename = storageDiagnosticExportFilename(diagnostic, exportedAt);
    const content = serializeStorageDiagnosticExport(diagnostic, exportedAt);
    try {
      downloadJson(filename, content);
    } catch (err) {
      setDiagnosticStatus("Export failed", "warning");
      console.error("Exact storage diagnostic export failed:", err);
      return null;
    }
    return { filename, content };
  }

  function acceptStorageHealth(payload) {
    const storage = payload?.storage || {};
    acceptMaintenanceStatus(storage);
    const tableSizes = storage.tableSizes || null;
    if (!tableSizes) return null;
    render(tableSizes);
    setStatus(
      statusText(tableSizes),
      tableSizes.status === "partial" || tableSizes.status === "unavailable" ? "warning" : ""
    );
    return tableSizes;
  }

  async function load(options = {}) {
    if (inFlight) return null;
    if (shouldDefer(options)) {
      setStatus("Deferred", "warning");
      renderMessage(
        "Storage breakdown is deferred while an active run is using the bridge. Use Refresh when it finishes."
      );
      return null;
    }
    inFlight = true;
    maintenancePollStartedAt = 0;
    setStatus("Loading…");
    renderMessage("Loading database storage details…");
    try {
      const payload = await getBridge(STORAGE_HEALTH_ROUTE, {
        timeoutMs,
        affectsReachability: false
      });
      const tableSizes = acceptStorageHealth(payload);
      if (refs.adminStorageRebuildBodyEl) void refreshRebuildStatus();
      if (!tableSizes) {
        setStatus("Unavailable", "warning");
        renderMessage("Storage health did not include a table-size breakdown.");
      }
      return tableSizes;
    } catch (err) {
      setStatus("Unavailable", "warning");
      renderMessage(`Could not load storage health: ${err?.message || err || "unknown error"}`);
      return null;
    } finally {
      inFlight = false;
    }
  }

  function init() {
    if (refreshBound) return;
    refreshBound = true;
    refs.adminStorageTablesRefreshBtnEl?.addEventListener?.("click", () => load());
    refs.adminStorageQuickCheckBtnEl?.addEventListener?.("click", () => startQuickCheck());
    refs.adminStorageMaintenancePreviewBtnEl?.addEventListener?.("click", () =>
      previewMaintenance()
    );
    refs.adminStorageMaintenanceBtnEl?.addEventListener?.("click", () => startMaintenance());
    refs.adminStorageMaintenanceCancelBtnEl?.addEventListener?.("click", () =>
      cancelMaintenance()
    );
    refs.adminStorageRebuildBtnEl?.addEventListener?.("click", () => startRebuild());
    refs.adminStorageRebuildCancelBtnEl?.addEventListener?.("click", () => cancelRebuild());
    refs.adminStorageRebuildRollbackBtnEl?.addEventListener?.("click", () => rollbackRebuild());
    refs.adminStorageRebuildBackupDeleteBtnEl?.addEventListener?.("click", () =>
      deleteRebuildBackup()
    );
    refs.adminStorageDiagnosticBtnEl?.addEventListener?.("click", () => startExactDiagnostic());
    refs.adminStorageDiagnosticCancelBtnEl?.addEventListener?.("click", () =>
      cancelExactDiagnostic()
    );
    refs.adminStorageDiagnosticExportBtnEl?.addEventListener?.("click", () =>
      exportExactDiagnostic()
    );
  }

  return {
    init,
    load,
    acceptStorageHealth,
    refreshMaintenanceStatus,
    stopMaintenancePoll,
    refreshDiagnostic,
    startStorageDiagnostic,
    previewMaintenance,
    startMaintenance,
    cancelMaintenance,
    acceptRebuild,
    refreshRebuildStatus,
    stopRebuildPoll,
    startRebuild,
    cancelRebuild,
    rollbackRebuild,
    deleteRebuildBackup,
    startQuickCheck,
    startExactDiagnostic,
    cancelExactDiagnostic,
    exportExactDiagnostic
  };
}
