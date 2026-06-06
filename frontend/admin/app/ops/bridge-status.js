import { setTooltip } from "../../../shared/ui/index.js?v=6";

function maybeUnrefTimer(timer) {
  timer?.unref?.();
  return timer;
}

export function createOpsBridgeStatusController({
  state,
  refs,
  getBridge,
  onBridgeStatusChange,
  bridgeStatusPollIntervalMs
}) {
  let lastBridgeStatus = "checking";
  let bridgeStatusFailureCount = 0;
  let bridgeStatusInitialPollTimer = null;

  function getBridgeStatus() {
    return lastBridgeStatus;
  }

  function setBridgeStatusBadge(stateValue, label) {
    if (!refs.adminBridgeStatusBadgeEl) return;
    const normalized = String(stateValue || "checking").toLowerCase();
    refs.adminBridgeStatusBadgeEl.classList.remove("online", "offline", "checking");
    refs.adminBridgeStatusBadgeEl.classList.add(
      normalized === "online" ? "online" : normalized === "offline" ? "offline" : "checking"
    );
    refs.adminBridgeStatusBadgeEl.textContent = label || "Bridge Checking";
    setTooltip(refs.adminBridgeStatusBadgeEl, label || "Local admin bridge status");
    refs.adminBridgeStatusBadgeEl.classList.remove("refresh-pulse");
    void refs.adminBridgeStatusBadgeEl.offsetWidth;
    refs.adminBridgeStatusBadgeEl.classList.add("refresh-pulse");
  }

  function startBridgeStatusWatch(options = {}) {
    stopBridgeStatusWatch();
    const deferInitial = Boolean(options?.deferInitial);
    if (deferInitial) {
      lastBridgeStatus = "checking";
      onBridgeStatusChange?.("checking");
      setBridgeStatusBadge("checking", "Bridge Checking");
      const initialDelayMs = Math.max(600, Number(options?.initialDelayMs) || bridgeStatusPollIntervalMs);
      bridgeStatusInitialPollTimer = maybeUnrefTimer(setTimeout(() => {
        bridgeStatusInitialPollTimer = null;
        pollBridgeStatus().catch(() => {});
      }, initialDelayMs));
    } else {
      pollBridgeStatus({ forceChecking: true }).catch(() => {});
    }
    state.bridgeStatusPollTimer = maybeUnrefTimer(setInterval(() => {
      pollBridgeStatus().catch(() => {});
    }, bridgeStatusPollIntervalMs));
  }

  function stopBridgeStatusWatch() {
    if (bridgeStatusInitialPollTimer) {
      clearTimeout(bridgeStatusInitialPollTimer);
      bridgeStatusInitialPollTimer = null;
    }
    if (!state.bridgeStatusPollTimer) return;
    clearInterval(state.bridgeStatusPollTimer);
    state.bridgeStatusPollTimer = null;
  }

  async function pollBridgeStatus(options = {}) {
    if (options.forceChecking) {
      if (lastBridgeStatus !== "checking") {
        lastBridgeStatus = "checking";
        onBridgeStatusChange?.("checking");
      }
      setBridgeStatusBadge("checking", "Bridge Checking");
    }
    try {
      const healthPayload = await getBridge("/ops/health?view=ready", { timeoutMs: 5000 });
      const serviceName = String(healthPayload?.service || "").trim();
      if (serviceName && serviceName !== "baluffo-bridge") {
        throw new Error("Bridge health response mismatch");
      }
      bridgeStatusFailureCount = 0;
      if (lastBridgeStatus !== "online") {
        lastBridgeStatus = "online";
        onBridgeStatusChange?.("online");
      }
      setBridgeStatusBadge("online", "Bridge Online");
    } catch {
      bridgeStatusFailureCount += 1;
      if (lastBridgeStatus === "online" && bridgeStatusFailureCount < 2) {
        setBridgeStatusBadge("checking", "Bridge Checking");
        return;
      }
      if (lastBridgeStatus !== "offline") {
        lastBridgeStatus = "offline";
        onBridgeStatusChange?.("offline");
      }
      setBridgeStatusBadge("offline", "Bridge Offline");
    }
  }

  return {
    getBridgeStatus,
    setBridgeStatusBadge,
    startBridgeStatusWatch,
    stopBridgeStatusWatch,
    pollBridgeStatus
  };
}
