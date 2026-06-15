import { hasActiveTaskStateRows } from "../../live-task.js";
import {
  armDesktopReloadBypass,
  consumeDesktopNavigationBypass,
  hasDesktopNavigationBypass
} from "./navigation.js";
import {
  DESKTOP_BOOTSTRAP_RETRY_INTERVAL_MS,
  DESKTOP_BOOTSTRAP_RETRY_WINDOW_MS,
  DESKTOP_LIFECYCLE_HEARTBEAT_MS,
  DESKTOP_SESSION_LIFECYCLE_URL,
  TASKS_URL,
  UPDATE_STATUS_URL,
  desktopState
} from "./state.js";

export function normalizeDesktopSession(payload) {
  const session = payload && typeof payload === "object" ? payload : {};
  const sessionId = String(session.sessionId || "").trim();
  const ownerToken = String(session.ownerToken || "").trim();
  if (!sessionId || !ownerToken) {
    return null;
  }
  return {
    sessionId,
    ownerToken,
    lastActivityAt: String(session.lastActivityAt || "")
  };
}

function generateDesktopPageId() {
  if (globalThis.crypto?.randomUUID) {
    return globalThis.crypto.randomUUID();
  }
  return `desktop-page-${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

function hasActiveDesktopWork() {
  return Boolean(
    desktopState.desktopActiveWorkSnapshot.hasActiveTask
    || desktopState.desktopActiveWorkSnapshot.hasActiveUpdate
  );
}

const CONFIRMED_ACTIVE_WORK_CLOSE_REASON = "confirmed_active_work_close";
const ACTIVE_WORK_CLOSE_ATTEMPT_REASON = "active_work_close_attempt";

function isDesktopReloadShortcut(event) {
  const key = String(event?.key || "").toLowerCase();
  const code = String(event?.code || "").toLowerCase();
  const isFunctionReload = key === "f5" || code === "f5";
  const isKeyboardReload = key === "r" && Boolean(event?.ctrlKey || event?.metaKey);
  return Boolean(isFunctionReload || isKeyboardReload);
}

function isActiveUpdatePayload(payload) {
  const status = payload && typeof payload === "object" ? payload : {};
  const downloadState = String(status.downloadState || "").toLowerCase();
  const installState = String(status.installState || "").toLowerCase();
  if (downloadState === "downloading") {
    return true;
  }
  return new Set(["handoff_requested", "waiting_for_exit", "installing", "verifying"]).has(installState);
}

async function fetchJsonWithOk(url) {
  const response = await fetch(url, { cache: "no-store" });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(String(payload?.error || response.statusText || "Request failed."));
  }
  return payload;
}

function waitForDelay(delayMs) {
  return new Promise(resolve => {
    globalThis.setTimeout(resolve, Math.max(0, Number(delayMs) || 0));
  });
}

let desktopBootstrapStats = null;

export function getDesktopBootstrapStats() {
  return desktopBootstrapStats ? { ...desktopBootstrapStats } : null;
}

async function refreshDesktopActiveWorkSnapshot() {
  if (!desktopState.desktopLifecycleEnabled) {
    return;
  }
  const [taskState, updateState] = await Promise.allSettled([
    fetchJsonWithOk(TASKS_URL),
    fetchJsonWithOk(UPDATE_STATUS_URL)
  ]);
  if (taskState.status === "fulfilled") {
    desktopState.desktopActiveWorkSnapshot.hasActiveTask = hasActiveTaskStateRows(taskState.value);
  }
  if (updateState.status === "fulfilled") {
    desktopState.desktopActiveWorkSnapshot.hasActiveUpdate = isActiveUpdatePayload(updateState.value);
  }
}

export function stopDesktopLifecycle() {
  if (desktopState.desktopLifecycleHeartbeatTimer) {
    window.clearInterval?.(desktopState.desktopLifecycleHeartbeatTimer);
    desktopState.desktopLifecycleHeartbeatTimer = 0;
  }
  if (desktopState.desktopActiveWorkTimer) {
    window.clearInterval?.(desktopState.desktopActiveWorkTimer);
    desktopState.desktopActiveWorkTimer = 0;
  }
}

async function postDesktopLifecycle(state, { keepalive = false, reason = "" } = {}) {
  if (!desktopState.desktopLifecycleEnabled) {
    return null;
  }
  if (!desktopState.desktopSession || !desktopState.desktopPageId) {
    return null;
  }
  const requestOptions = {
    method: "POST",
    cache: "no-store",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      ownerToken: desktopState.desktopSession.ownerToken,
      sessionId: desktopState.desktopSession.sessionId,
      pageId: desktopState.desktopPageId,
      state,
      reason: String(reason || "")
    })
  };
  if (keepalive) {
    requestOptions.keepalive = true;
  }
  return fetch(DESKTOP_SESSION_LIFECYCLE_URL, requestOptions);
}

function sendDesktopClosingSignal(reason) {
  if (!desktopState.desktopLifecycleEnabled) {
    return false;
  }
  if (desktopState.desktopClosingSignaled || !desktopState.desktopSession || !desktopState.desktopPageId) {
    return false;
  }
  desktopState.desktopClosingSignaled = true;
  stopDesktopLifecycle();
  const body = JSON.stringify({
    ownerToken: desktopState.desktopSession.ownerToken,
    sessionId: desktopState.desktopSession.sessionId,
    pageId: desktopState.desktopPageId,
    state: "closing",
    reason: String(reason || "")
  });
  try {
    if (globalThis.navigator?.sendBeacon) {
      const blob = new Blob([body], { type: "application/json" });
      globalThis.navigator.sendBeacon(DESKTOP_SESSION_LIFECYCLE_URL, blob);
    }
  } catch {
    // Ignore beacon errors and fall back to fetch keepalive.
  }
  fetch(DESKTOP_SESSION_LIFECYCLE_URL, {
    method: "POST",
    cache: "no-store",
    keepalive: true,
    headers: {
      "Content-Type": "application/json"
    },
    body
  }).catch(() => {});
  return true;
}

function bindDesktopLifecycleEvents(clearDesktopNavigationBypass) {
  if (!desktopState.desktopLifecycleEnabled) {
    return;
  }
  if (window.__baluffoDesktopLifecycleBound) {
    return;
  }
  window.__baluffoDesktopLifecycleBound = true;
  window.addEventListener?.("keydown", event => {
    if (isDesktopReloadShortcut(event)) {
      armDesktopReloadBypass();
    }
  });
  window.addEventListener?.("beforeunload", event => {
    const bypassDesktopNavigation = hasDesktopNavigationBypass();
    desktopState.desktopCloseAttemptPending = true;
    if (bypassDesktopNavigation) {
      desktopState.desktopCloseAttemptPending = false;
      return undefined;
    }
    if (hasActiveDesktopWork()) {
      postDesktopLifecycle("closing", {
        keepalive: true,
        reason: ACTIVE_WORK_CLOSE_ATTEMPT_REASON
      }).catch(() => {});
      event.preventDefault();
      event.returnValue = "";
      return "";
    }
    sendDesktopClosingSignal("beforeunload");
    return undefined;
  });
  window.addEventListener?.("pagehide", () => {
    if (consumeDesktopNavigationBypass()) {
      desktopState.desktopCloseAttemptPending = false;
      return;
    }
    const hadCloseAttemptPending = Boolean(desktopState.desktopCloseAttemptPending);
    if (!desktopState.desktopCloseAttemptPending && !desktopState.desktopClosingSignaled) {
      desktopState.desktopCloseAttemptPending = true;
    }
    sendDesktopClosingSignal(
      hadCloseAttemptPending && hasActiveDesktopWork()
        ? CONFIRMED_ACTIVE_WORK_CLOSE_REASON
        : "pagehide"
    );
  });
  window.addEventListener?.("focus", () => {
    desktopState.desktopCloseAttemptPending = false;
    clearDesktopNavigationBypass();
    if (!desktopState.desktopClosingSignaled && desktopState.desktopSession) {
      postDesktopLifecycle("alive", { keepalive: true }).catch(() => {});
    }
    if (
      !desktopState.desktopClosingSignaled
      && desktopState.desktopSession
      && !desktopState.desktopLifecycleHeartbeatTimer
    ) {
      startDesktopLifecycle(clearDesktopNavigationBypass);
    }
  });
}

function startDesktopLifecycle(clearDesktopNavigationBypass) {
  if (!desktopState.desktopLifecycleEnabled) {
    return;
  }
  if (!desktopState.desktopSession) {
    return;
  }
  if (!desktopState.desktopPageId) {
    desktopState.desktopPageId = generateDesktopPageId();
  }
  bindDesktopLifecycleEvents(clearDesktopNavigationBypass);
  const sendAlive = () => {
    if (desktopState.desktopClosingSignaled) {
      return;
    }
    postDesktopLifecycle("alive").catch(() => {});
  };
  sendAlive();
  if (!desktopState.desktopLifecycleHeartbeatTimer) {
    desktopState.desktopLifecycleHeartbeatTimer = window.setInterval(
      sendAlive,
      DESKTOP_LIFECYCLE_HEARTBEAT_MS
    );
  }
  refreshDesktopActiveWorkSnapshot().catch(() => {});
  if (!desktopState.desktopActiveWorkTimer) {
    desktopState.desktopActiveWorkTimer = window.setInterval(() => {
      refreshDesktopActiveWorkSnapshot().catch(() => {});
    }, DESKTOP_LIFECYCLE_HEARTBEAT_MS);
  }
}

export async function waitForDesktopBootstrap() {
  if (desktopState.desktopBootstrapPromise) {
    return desktopState.desktopBootstrapPromise;
  }
  return desktopState.desktopBootstrapStatus === "ready";
}

export async function bootstrapDesktopApi({
  refreshCurrentUser,
  commitAuthState,
  clearDesktopNavigationBypass,
  toErrorMessage,
  nowFn = Date.now,
  waitFn = waitForDelay,
  retryWindowMs = DESKTOP_BOOTSTRAP_RETRY_WINDOW_MS,
  retryIntervalMs = DESKTOP_BOOTSTRAP_RETRY_INTERVAL_MS,
  enableLifecycle = true
}) {
  desktopState.desktopLifecycleEnabled = Boolean(enableLifecycle);
  if (desktopState.desktopBootstrapPromise) {
    return desktopState.desktopBootstrapPromise;
  }
  if (desktopState.desktopBootstrapStatus === "ready") {
    return true;
  }
  if (desktopState.desktopBootstrapStatus === "failed") {
    return false;
  }
  const bootstrapRevision = desktopState.authStateRevision;
  const startedAtMs = Number(nowFn());
  const deadlineMs = startedAtMs + Math.max(0, Number(retryWindowMs) || 0);
  desktopBootstrapStats = {
    startedAtMs,
    firstSuccessfulAttemptMs: null,
    completedAtMs: null,
    attemptCount: 0,
    failureCount: 0
  };
  desktopState.desktopBootstrapStatus = "pending";
  desktopState.desktopBootstrapPromise = (async () => {
    while (true) {
      try {
        desktopBootstrapStats.attemptCount += 1;
        await refreshCurrentUser({ revision: bootstrapRevision });
        const completedAtMs = Number(nowFn());
        desktopBootstrapStats.firstSuccessfulAttemptMs = Math.max(
          0,
          Math.round(completedAtMs - startedAtMs)
        );
        desktopBootstrapStats.completedAtMs = completedAtMs;
        desktopState.desktopClosingSignaled = false;
        desktopState.desktopCloseAttemptPending = false;
        desktopState.desktopBootstrapStatus = "ready";
        clearDesktopNavigationBypass();
        if (desktopState.desktopLifecycleEnabled) {
          startDesktopLifecycle(clearDesktopNavigationBypass);
        }
        return true;
      } catch (error) {
        desktopBootstrapStats.failureCount += 1;
        if (Number(nowFn()) >= deadlineMs) {
          desktopState.desktopBootstrapStatus = "failed";
          console.error(
            "[desktop-local-data] bootstrap failed:",
            toErrorMessage(error, "bootstrap failed")
          );
          commitAuthState(null, bootstrapRevision);
          return false;
        }
        await waitFn(Math.max(0, Number(retryIntervalMs) || 0));
      }
    }
  })().finally(() => {
    desktopState.desktopBootstrapPromise = null;
  });
  return desktopState.desktopBootstrapPromise;
}
