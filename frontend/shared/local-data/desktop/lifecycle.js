import {
  DESKTOP_BOOTSTRAP_RETRY_INTERVAL_MS,
  DESKTOP_BOOTSTRAP_RETRY_WINDOW_MS,
  desktopState
} from "./state.js";

// What every page needs from the local-data client -- hosted pages included:
// resolve the session, retry while the server is still booting, and publish the
// auth state the app surfaces read. The desktop window's session lifecycle
// (heartbeat, close signals, active-work close veto) lives in
// `lifecycle-session.js` and is handed in by the desktop composition only, so a
// hosted page never imports it.
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

function waitForDelay(delayMs) {
  return new Promise(resolve => {
    globalThis.setTimeout(resolve, Math.max(0, Number(delayMs) || 0));
  });
}

let desktopBootstrapStats = null;

export function getDesktopBootstrapStats() {
  return desktopBootstrapStats ? { ...desktopBootstrapStats } : null;
}

// "idle" | "pending" | "ready" | "failed". The Admin reconnect banner reads this
// so it can show a wait for the bridge instead of rendering an empty page while
// the bootstrap retry window is still open.
export function getDesktopBootstrapStatus() {
  return desktopState.desktopBootstrapStatus;
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
  enableLifecycle = false,
  startLifecycle = null
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
        if (desktopState.desktopLifecycleEnabled && startLifecycle) {
          startLifecycle(clearDesktopNavigationBypass);
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
