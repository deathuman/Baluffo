/**
 * @fileoverview Shared admin bridge button status watcher.
 * Polls admin bridge health endpoint and updates button state.
 * Keeps poll logic shared while allowing page-specific presentation via applyState callback.
 */

/**
 * Creates an admin bridge button watcher.
 * @param {Object} options
 * @param {HTMLElement} options.buttonEl - The button element to update
 * @param {string} options.baseUrl - Admin bridge base URL
 * @param {Function} options.fetchJson - fetchJson utility function
 * @param {Function} options.applyState - Callback to apply page-specific presentation: ({ state, label, title, activeAlerts }) => void
 * @param {number} [options.intervalMs] - Polling interval in milliseconds (default: 5000)
 * @param {Function} [options.awaitBridgeReady] - Optional startup gate before the first bridge poll
 * @returns {{ setAdminPageButtonState, pollAdminBridgeButtonState, startAdminBridgeButtonWatch, stopAdminBridgeButtonWatch }}
 */
export function createAdminBridgeButtonWatcher({
  buttonEl,
  baseUrl,
  fetchJson,
  applyState,
  intervalMs = 5000,
  awaitBridgeReady = async () => true
}) {
  let currentState = "checking";
  let pollTimer = null;
  let initialBridgeReadyResolved = false;

  function getRuntimeBridgeBaseFromLocation() {
    try {
      const url = new URL(window.location.href);
      const bridgePort = String(url.searchParams.get("bridgePort") || "").trim();
      const bridgeHost = String(url.searchParams.get("bridgeHost") || "").trim() || "127.0.0.1";
      if (/^\d+$/.test(bridgePort)) {
        return `http://${bridgeHost}:${bridgePort}`;
      }
    } catch {
      // Ignore URL parsing failures and use the configured bridge base.
    }
    return "";
  }

  function getRuntimeBridgeBaseFromSession() {
    try {
      return String(window.sessionStorage.getItem("baluffo_runtime_bridge_base") || "").trim();
    } catch {
      return "";
    }
  }

  function getBridgeBaseCandidates() {
    const seen = new Set();
    return [
      baseUrl,
      getRuntimeBridgeBaseFromLocation(),
      getRuntimeBridgeBaseFromSession()
    ]
      .map(value => String(value ?? "").trim())
      .filter(value => {
        if (seen.has(value)) return false;
        seen.add(value);
        return true;
      });
  }

  async function fetchHealth() {
    let lastError = null;
    for (const candidateBase of getBridgeBaseCandidates()) {
      try {
        return await fetchJson(candidateBase, "/ops/health?view=ready");
      } catch (error) {
        lastError = error;
      }
    }
    throw lastError || new Error("Admin bridge is offline");
  }

  /**
   * Sets the admin bridge button state.
   * @param {string} stateValue - "online", "offline", or "checking"
   * @param {string} [label] - Button text
   * @param {string} [title] - Button title/tooltip
   * @param {number} [activeAlerts] - Number of active alerts (for online state)
   */
  function setAdminPageButtonState(stateValue, label, title, activeAlerts = 0) {
    const normalized = String(stateValue || "checking").toLowerCase();
    currentState = normalized;
    applyState({ buttonEl, state: normalized, label, title, activeAlerts });
  }

  /**
   * Polls admin bridge health and updates button state.
   * @returns {Promise<void>}
   */
  async function pollAdminBridgeButtonState() {
    if (!buttonEl) return;
    if (!initialBridgeReadyResolved) {
      initialBridgeReadyResolved = true;
      if (!(await awaitBridgeReady())) {
        setAdminPageButtonState("offline", "Admin Offline", "Admin bridge is offline");
        return;
      }
    }
    if (currentState !== "online") {
      setAdminPageButtonState("checking", "Admin", "Checking admin bridge status");
    }
    try {
      const payload = await fetchHealth();
      const summary = payload?.summary || {};
      const activeAlertCount = Number(summary?.activeAlertCount || 0);
      const label = activeAlertCount > 0
        ? `Admin Online (${activeAlertCount} alert${activeAlertCount !== 1 ? "s" : ""})`
        : "Admin Online";
      setAdminPageButtonState("online", label, "Open admin panel", activeAlertCount);
    } catch {
      setAdminPageButtonState("offline", "Admin Offline", "Admin bridge is offline");
    }
  }

  /**
   * Starts polling admin bridge status at regular intervals.
   */
  function startAdminBridgeButtonWatch() {
    if (!buttonEl || pollTimer) return;
    setAdminPageButtonState("checking", "Admin", "Checking admin bridge status");
    pollAdminBridgeButtonState().catch(() => { });
    pollTimer = window.setInterval(() => {
      pollAdminBridgeButtonState().catch(() => { });
    }, intervalMs);
  }

  /**
   * Stops polling admin bridge status.
   */
  function stopAdminBridgeButtonWatch() {
    if (!pollTimer) return;
    clearInterval(pollTimer);
    pollTimer = null;
  }

  return {
    setAdminPageButtonState,
    pollAdminBridgeButtonState,
    startAdminBridgeButtonWatch,
    stopAdminBridgeButtonWatch
  };
}
