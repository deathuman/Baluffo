import { createDesktopLocalDataApi, commitAuthState, refreshCurrentUser, toErrorMessage } from "./desktop/api.js";
import { bootstrapDesktopApi } from "./desktop/lifecycle.js";
import { clearDesktopNavigationBypass, navigateDesktopPage } from "./desktop/navigation.js";
import { desktopState } from "./desktop/state.js";

const desktopApi = createDesktopLocalDataApi();

export { navigateDesktopPage };

export function initDesktopLocalDataClient() {
  const windowChanged = desktopState.desktopBoundWindow && desktopState.desktopBoundWindow !== window;
  if (windowChanged) {
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
  commitAuthState(null);
  bootstrapDesktopApi({
    refreshCurrentUser,
    commitAuthState,
    clearDesktopNavigationBypass,
    toErrorMessage
  }).catch(() => {
    // Startup fetch errors are already logged in bootstrapDesktopApi.
  });
  return desktopApi;
}
