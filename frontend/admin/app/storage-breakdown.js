import {
  renderStorageTablesHtml,
  renderStorageTablesMessageHtml
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
const DEFAULT_STORAGE_HEALTH_TIMEOUT_MS = 5000;
const STORAGE_DIAGNOSTIC_POLL_MS = 2000;

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
 * The panel is deliberately read-once plus manual refresh: Admin must not add a
 * storage-health poll during active work. The Action Center already fetches the
 * same route on its own cadence and hands the payload in through
 * `acceptStorageHealth`, so the panel stays current without a second poll.
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
    try {
      const payload = await postBridge(
        `${STORAGE_DIAGNOSTIC_ROUTE}/cancel`,
        {},
        { timeoutMs: DEFAULT_STORAGE_HEALTH_TIMEOUT_MS, affectsReachability: false }
      );
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

  async function startMaintenance() {
    if (maintenanceInFlight || typeof postBridge !== "function") return null;
    maintenanceInFlight = true;
    const button = refs.adminStorageMaintenanceBtnEl;
    if (button) button.disabled = true;
    setMaintenanceStatus("Starting maintenance…");
    try {
      const payload = await postBridge(
        STORAGE_MAINTENANCE_ROUTE,
        {},
        { timeoutMs: DEFAULT_STORAGE_HEALTH_TIMEOUT_MS, affectsReachability: false }
      );
      if (payload?.started) {
        setMaintenanceStatus(
          "Maintenance started — older saved versions will be removed and the database compacted in the background."
        );
      } else if (payload?.alreadyRunning) {
        setMaintenanceStatus("Maintenance is already running.", "warning");
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
    const tableSizes = payload?.storage?.tableSizes || null;
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
    setStatus("Loading…");
    renderMessage("Loading database storage details…");
    try {
      const payload = await getBridge(STORAGE_HEALTH_ROUTE, {
        timeoutMs,
        affectsReachability: false
      });
      const tableSizes = acceptStorageHealth(payload);
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
    refs.adminStorageMaintenanceBtnEl?.addEventListener?.("click", () => startMaintenance());
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
    refreshDiagnostic,
    startStorageDiagnostic,
    startMaintenance,
    startQuickCheck,
    startExactDiagnostic,
    cancelExactDiagnostic,
    exportExactDiagnostic
  };
}
