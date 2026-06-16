import { createDesktopLocalDataApi, commitAuthState, refreshCurrentUser, toErrorMessage } from "./desktop/api.js";
import {
  bootstrapDesktopApi,
  getDesktopBootstrapStats,
  stopDesktopLifecycle,
  waitForDesktopBootstrap
} from "./desktop/lifecycle.js";
import { clearDesktopNavigationBypass, navigateDesktopPage } from "./desktop/navigation.js";
import { desktopState } from "./desktop/state.js";
const desktopApi = createDesktopLocalDataApi();
export { navigateDesktopPage };
export { getDesktopBootstrapStats };
export async function awaitDesktopBootstrap({ enableLifecycle = true } = {}) {
  if (!desktopState.desktopApiInitialized || desktopState.desktopBoundWindow !== window) {
    initDesktopLocalDataClient({ enableLifecycle });
  } else if (enableLifecycle) {
    desktopState.desktopLifecycleEnabled = true;
  }
  return waitForDesktopBootstrap();
}
export function initDesktopLocalDataClient({ enableLifecycle = true } = {}) {
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
  if (needsBootstrap) {
    desktopState.desktopLifecycleEnabled = Boolean(enableLifecycle);
    if (!desktopState.desktopLifecycleEnabled) {
      stopDesktopLifecycle();
    }
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
    enableLifecycle
  });
  return desktopApi;
}
