import { hasActiveTaskStateRows } from "../../live-task.js";
import { consumeDesktopNavigationBypass } from "./navigation.js";
import {
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

async function refreshDesktopActiveWorkSnapshot() {
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

async function postDesktopLifecycle(state, { keepalive = false } = {}) {
  if (!desktopState.desktopSession || !desktopState.desktopPageId) {
    return null;
  }
  return fetch(DESKTOP_SESSION_LIFECYCLE_URL, {
    method: "POST",
    cache: "no-store",
    keepalive,
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      ownerToken: desktopState.desktopSession.ownerToken,
      sessionId: desktopState.desktopSession.sessionId,
      pageId: desktopState.desktopPageId,
      state
    })
  });
}

function sendDesktopClosingSignal(reason) {
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
      if (globalThis.navigator.sendBeacon(DESKTOP_SESSION_LIFECYCLE_URL, blob)) {
        return true;
      }
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
  if (window.__baluffoDesktopLifecycleBound) {
    return;
  }
  window.__baluffoDesktopLifecycleBound = true;
  window.addEventListener?.("beforeunload", event => {
    desktopState.desktopCloseAttemptPending = true;
    if (hasActiveDesktopWork() && !consumeDesktopNavigationBypass()) {
      event.preventDefault();
      event.returnValue = "";
      return "";
    }
    sendDesktopClosingSignal("beforeunload");
    return undefined;
  });
  window.addEventListener?.("pagehide", () => {
    if (!desktopState.desktopCloseAttemptPending && !desktopState.desktopClosingSignaled) {
      desktopState.desktopCloseAttemptPending = true;
    }
    sendDesktopClosingSignal("pagehide");
  });
  window.addEventListener?.("focus", () => {
    desktopState.desktopCloseAttemptPending = false;
    clearDesktopNavigationBypass();
    if (
      !desktopState.desktopClosingSignaled
      && desktopState.desktopSession
      && !desktopState.desktopLifecycleHeartbeatTimer
    ) {
      startDesktopLifecycle(clearDesktopNavigationBypass);
    }
  });
}

export function startDesktopLifecycle(clearDesktopNavigationBypass) {
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
    postDesktopLifecycle("alive", { keepalive: true }).catch(() => {});
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

export async function bootstrapDesktopApi({
  refreshCurrentUser,
  commitAuthState,
  clearDesktopNavigationBypass,
  toErrorMessage
}) {
  const bootstrapRevision = desktopState.authStateRevision;
  try {
    await refreshCurrentUser({ revision: bootstrapRevision });
    desktopState.desktopClosingSignaled = false;
    desktopState.desktopCloseAttemptPending = false;
    clearDesktopNavigationBypass();
    startDesktopLifecycle(clearDesktopNavigationBypass);
  } catch (error) {
    console.error(
      "[desktop-local-data] bootstrap failed:",
      toErrorMessage(error, "bootstrap failed")
    );
    commitAuthState(null, bootstrapRevision);
  }
}
