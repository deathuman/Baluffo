const STORAGE_HEALTH_ROUTE = "/ops/storage-health";
const DEFAULT_STORAGE_HEALTH_TIMEOUT_MS = 5000;

/**
 * Owns the Runtime Database Storage panel beside Stored Profiles Overview.
 *
 * The panel is deliberately read-once plus manual refresh: Admin must not add a
 * storage-health poll during active work. The Action Center already fetches the
 * same route on its own cadence and hands the payload in through
 * `acceptStorageHealth`, so the panel stays current without a second poll.
 */
export function createStorageBreakdownController({
  refs,
  getBridge,
  formatBytes,
  renderStorageTablesHtml,
  renderStorageTablesMessageHtml,
  shouldDefer = () => false,
  timeoutMs = DEFAULT_STORAGE_HEALTH_TIMEOUT_MS
}) {
  let inFlight = false;
  let refreshBound = false;

  function statusText(tableSizes) {
    if (!tableSizes || tableSizes.status === "unavailable") return "Unavailable";
    if (tableSizes.status === "partial" || tableSizes.complete === false) return "Partial estimate";
    return tableSizes.estimated ? "Estimated" : "Exact";
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

  async function load() {
    if (inFlight) return null;
    if (shouldDefer()) {
      setStatus("Deferred", "warning");
      renderMessage(
        "Storage breakdown is deferred while an active run is using the bridge. Use Refresh when it finishes."
      );
      return null;
    }
    inFlight = true;
    setStatus("Loading…");
    renderMessage("Loading runtime database table sizes…");
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
  }

  return {
    init,
    load,
    acceptStorageHealth
  };
}
