import { createDesktopLocalDataApi, commitAuthState, refreshCurrentUser, toErrorMessage } from "./desktop/api.js";
import {
  bootstrapDesktopApi,
  getDesktopBootstrapStats,
  waitForDesktopBootstrap
} from "./desktop/lifecycle.js";
import { clearDesktopNavigationBypass, navigateDesktopPage } from "./desktop/navigation.js";
import { desktopState } from "./desktop/state.js";

const desktopApi = createDesktopLocalDataApi();

export { navigateDesktopPage };
export { getDesktopBootstrapStats };

export async function awaitDesktopBootstrap() {
  if (!desktopState.desktopApiInitialized || desktopState.desktopBoundWindow !== window) {
    initDesktopLocalDataClient();
  }
  return waitForDesktopBootstrap();
}

export function initDesktopLocalDataClient() {
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
  desktopState.desktopApiInitialized = true;
  desktopState.desktopBoundWindow = window;
  window.JobAppLocalData = desktopApi;
  if (!needsBootstrap) {
    return desktopApi;
  }
  // Keep the persisted session hint until the bridge session refresh resolves.
  desktopState.currentUser = null;
  void bootstrapDesktopApi({
    refreshCurrentUser,
    commitAuthState,
    clearDesktopNavigationBypass,
    toErrorMessage
  });
  return desktopApi;
}
