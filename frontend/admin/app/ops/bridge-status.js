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
  let bridgeStatusPollInFlight = null;

  function getBridgeStatus() {
    return lastBridgeStatus;
  }

  function setBridgeStatusBadge(stateValue, label) {
    if (!refs.adminBridgeStatusBadgeEl) return;
    const normalized = String(stateValue || "checking").toLowerCase();
    refs.adminBridgeStatusBadgeEl.classList.remove("online", "offline", "checking", "degraded");
    refs.adminBridgeStatusBadgeEl.classList.add(
      normalized === "online"
        ? "online"
        : normalized === "offline"
          ? "offline"
          : normalized === "degraded"
            ? "degraded"
            : "checking"
    );
    refs.adminBridgeStatusBadgeEl.textContent = label || "Bridge Checking";
    setTooltip(refs.adminBridgeStatusBadgeEl, label || "Local admin bridge status");
    refs.adminBridgeStatusBadgeEl.classList.remove("refresh-pulse");
  }

  function startBridgeStatusWatch(options = {}) {
    stopBridgeStatusWatch();
    const deferInitial = Boolean(options?.deferInitial);
    const startInterval = () => {
      if (state.bridgeStatusPollTimer) return;
      state.bridgeStatusPollTimer = maybeUnrefTimer(setInterval(() => {
        pollBridgeStatus().catch(() => {});
      }, bridgeStatusPollIntervalMs));
    };
    if (deferInitial) {
      lastBridgeStatus = "checking";
      onBridgeStatusChange?.("checking");
      setBridgeStatusBadge("checking", "Bridge Checking");
      const initialDelayMs = Math.max(600, Number(options?.initialDelayMs) || bridgeStatusPollIntervalMs);
      bridgeStatusInitialPollTimer = maybeUnrefTimer(setTimeout(() => {
        bridgeStatusInitialPollTimer = null;
        pollBridgeStatus().catch(() => {}).finally(startInterval);
      }, initialDelayMs));
    } else {
      pollBridgeStatus({ forceChecking: true }).catch(() => {}).finally(startInterval);
    }
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
    if (bridgeStatusPollInFlight && !options.forceChecking) {
      return bridgeStatusPollInFlight;
    }
    bridgeStatusPollInFlight = (async () => {
    if (options.forceChecking) {
      if (lastBridgeStatus !== "checking") {
        lastBridgeStatus = "checking";
        onBridgeStatusChange?.("checking");
      }
      setBridgeStatusBadge("checking", "Bridge Checking");
    }
    try {
      const healthPayload = await getBridge("/app/ready", { timeoutMs: 3000 });
      const serviceName = String(healthPayload?.service || "").trim();
      if (serviceName && !["baluffo-bridge", "baluffo-container-gateway"].includes(serviceName)) {
        throw new Error("Bridge health response mismatch");
      }
      bridgeStatusFailureCount = 0;
      const readyStatus = String(healthPayload?.status || "").trim().toLowerCase();
      const nextStatus = readyStatus === "degraded" ? "degraded" : "online";
      if (lastBridgeStatus !== nextStatus) {
        lastBridgeStatus = nextStatus;
        onBridgeStatusChange?.(nextStatus);
      }
      setBridgeStatusBadge(nextStatus, nextStatus === "degraded" ? "Bridge Degraded" : "Bridge Online");
    } catch {
      try {
        const pipelinePayload = await getBridge("/tasks/run-jobs-pipeline-status", { timeoutMs: 5000 });
        if (pipelinePayload && typeof pipelinePayload === "object") {
          bridgeStatusFailureCount = 0;
          if (lastBridgeStatus !== "degraded") {
            lastBridgeStatus = "degraded";
            onBridgeStatusChange?.("degraded");
          }
          setBridgeStatusBadge("degraded", "Bridge Degraded");
          return;
        }
      } catch {
        // Fall through to normal offline handling.
      }
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
    })();
    try {
      return await bridgeStatusPollInFlight;
    } finally {
      bridgeStatusPollInFlight = null;
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
