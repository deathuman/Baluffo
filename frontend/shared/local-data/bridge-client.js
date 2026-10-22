import {
  commitAuthState,
  createDesktopLocalDataApi,
  refreshCurrentUser,
  toErrorMessage
} from "./desktop/api.js";
import {
  bootstrapDesktopApi,
  getDesktopBootstrapStats,
  getDesktopBootstrapStatus,
  waitForDesktopBootstrap
} from "./desktop/lifecycle.js";
import { clearDesktopNavigationBypass } from "./desktop/navigation.js";
import { desktopState } from "./desktop/state.js";

// The bridge local-data composition: the two runtimes whose data lives outside
// the page (a container deployment and the packaged desktop app) share it. It
// deliberately does not import the desktop session lifecycle -- a hosted page
// has no window heartbeat, close signal or active-work close veto -- so the
// desktop composition (`desktop-client.js`) hands those in as callbacks.
export {
  getDesktopBootstrapStats as getBridgeBootstrapStats,
  getDesktopBootstrapStatus as getBridgeBootstrapStatus
};

// Defaults are the hosted behaviour: no lifecycle. The desktop composition
// passes `enableLifecycle: true` plus its `startLifecycle`/`onSessionLost`.
export function initBridgeLocalDataClient({
  enableLifecycle = false,
  startLifecycle = null,
  onDesktopSessionLost = null
} = {}) {
  const windowChanged = desktopState.desktopBoundWindow && desktopState.desktopBoundWindow !== window;
  if (windowChanged) {
    desktopState.desktopBootstrapPromise = null;
    desktopState.desktopBootstrapStatus = "idle";
    desktopState.desktopSession = null;
    desktopState.desktopLifecycleHeartbeatTimer = 0;
    desktopState.desktopActiveWorkTimer = 0;
    desktopState.desktopClosingSignaled = false;
    desktopState.desktopCloseAttemptPending = false;
    desktopState.desktopPageId = "";
    desktopState.desktopActiveWorkSnapshot.hasActiveTask = false;
    desktopState.desktopActiveWorkSnapshot.hasActiveUpdate = false;
  }
  const needsBootstrap = !desktopState.desktopApiInitialized || windowChanged;
  const desktopApi = createDesktopLocalDataApi({ onDesktopSessionLost });
  if (needsBootstrap) {
    desktopState.desktopLifecycleEnabled = Boolean(enableLifecycle);
  } else if (enableLifecycle) {
    desktopState.desktopLifecycleEnabled = true;
  }
  desktopState.desktopApiInitialized = true;
  desktopState.desktopBoundWindow = window;
  window.JobAppLocalData = desktopApi;
  if (!needsBootstrap) {
    return desktopApi;
  }
  desktopState.currentUser = null;
  void bootstrapDesktopApi({
    refreshCurrentUser,
    commitAuthState,
    clearDesktopNavigationBypass,
    toErrorMessage,
    enableLifecycle: desktopState.desktopLifecycleEnabled,
    startLifecycle
  });
  return desktopApi;
}

export async function awaitBridgeBootstrap(options = {}) {
  if (!desktopState.desktopApiInitialized || desktopState.desktopBoundWindow !== window) {
    initBridgeLocalDataClient(options);
  } else if (options.enableLifecycle) {
    desktopState.desktopLifecycleEnabled = true;
  }
  return waitForDesktopBootstrap();
}
