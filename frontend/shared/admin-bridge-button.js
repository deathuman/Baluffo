/**
 * @fileoverview Shared admin bridge button status watcher.
 * Polls admin bridge health endpoint and updates button state.
 * Keeps poll logic shared while allowing page-specific presentation via applyState callback.
 */

import { createVisibilityPausedInterval } from "./visibility-poll.js?v=1";

/**
 * Creates an admin bridge button watcher.
 * @param {Object} options
 * @param {HTMLElement} options.buttonEl - The button element to update
 * @param {string} options.baseUrl - Admin bridge base URL
 * @param {Function} options.fetchJson - fetchJson utility function
 * @param {Function} options.applyState - Callback to apply page-specific presentation: ({ state, label, title, activeAlerts }) => void
 * @param {number} [options.intervalMs] - Polling interval in milliseconds (default: 5000)
 * @param {Function} [options.awaitBridgeReady] - Optional startup gate before the first bridge poll
 * @param {boolean} [options.degradeOnFailure] - Keep navigation available when health is delayed
 * @param {boolean} [options.degradeWhenBridgeNotReady] - Degrade instead of offline when startup gate fails
 * @param {string} [options.statusPath] - Lightweight status path used for polling
 * @returns {{ setAdminPageButtonState, pollAdminBridgeButtonState, startAdminBridgeButtonWatch, stopAdminBridgeButtonWatch }}
 */
export function createAdminBridgeButtonWatcher({
  buttonEl,
  baseUrl,
  fetchJson,
  applyState,
  intervalMs = 5000,
  awaitBridgeReady = async () => true,
  degradeOnFailure = false,
  degradeWhenBridgeNotReady = false,
  statusPath = "/app/ready"
}) {
  let currentState = "checking";
  let pollTimer = null;
  let pollInFlight = null;
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
    const path = String(statusPath || "/app/ready");
    for (const candidateBase of getBridgeBaseCandidates()) {
      try {
        return await fetchJson(candidateBase, path);
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

  function setDelayedStatus() {
    setAdminPageButtonState(
      "degraded",
      "Admin",
      "Admin status delayed; open Admin anyway"
    );
  }

  /**
   * Polls admin bridge health and updates button state.
   * @returns {Promise<void>}
   */
  async function pollAdminBridgeButtonState() {
    if (pollInFlight) return pollInFlight;
    pollInFlight = pollAdminBridgeButtonStateOnce().finally(() => {
      pollInFlight = null;
    });
    return pollInFlight;
  }

  async function pollAdminBridgeButtonStateOnce() {
    if (!buttonEl) return;
    if (!initialBridgeReadyResolved) {
      initialBridgeReadyResolved = true;
      if (!(await awaitBridgeReady())) {
        if (degradeOnFailure && degradeWhenBridgeNotReady) setDelayedStatus();
        else setAdminPageButtonState("offline", "Admin Offline", "Admin bridge is offline");
        return;
      }
    }
    if (currentState !== "online") {
      if (degradeOnFailure) {
        setAdminPageButtonState("degraded", "Admin", "Checking Admin status; open Admin anyway");
      } else {
        setAdminPageButtonState("checking", "Admin", "Checking admin bridge status");
      }
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
      if (degradeOnFailure) setDelayedStatus();
      else setAdminPageButtonState("offline", "Admin Offline", "Admin bridge is offline");
    }
  }

  /**
   * Starts polling admin bridge status at regular intervals.
   */
  function startAdminBridgeButtonWatch() {
    if (!buttonEl || pollTimer) return;
    if (degradeOnFailure) {
      setAdminPageButtonState("degraded", "Admin", "Checking Admin status; open Admin anyway");
    } else {
      setAdminPageButtonState("checking", "Admin", "Checking admin bridge status");
    }
    pollAdminBridgeButtonState().catch(() => { });
    pollTimer = createVisibilityPausedInterval(
      () => { pollAdminBridgeButtonState().catch(() => { }); },
      intervalMs,
      window
    );
  }

  /**
   * Stops polling admin bridge status.
   */
  function stopAdminBridgeButtonWatch() {
    if (!pollTimer) return;
    pollTimer.stop();
    pollTimer = null;
  }

  return {
    setAdminPageButtonState,
    pollAdminBridgeButtonState,
    startAdminBridgeButtonWatch,
    stopAdminBridgeButtonWatch
  };
}
