import {
  awaitBridgeBootstrap,
  getBridgeBootstrapStats,
  initBridgeLocalDataClient
} from "./bridge-client.js";
import { startDesktopLifecycle, stopDesktopLifecycle } from "./desktop/lifecycle-session.js";
import { desktopState } from "./desktop/state.js";

// The desktop composition: the bridge local-data client plus the packaged
// window's session lifecycle. Only the desktop build's pages and the development
// dispatcher (`app-client.js`) import this module; a hosted page imports
// `bridge-client.js` instead, so it never carries the heartbeat and close-signal
// code.
const DESKTOP_SESSION_CALLBACKS = Object.freeze({
  enableLifecycle: true,
  startLifecycle: startDesktopLifecycle,
  onDesktopSessionLost: stopDesktopLifecycle
});

export { getBridgeBootstrapStats };

export async function awaitDesktopBootstrap(options = {}) {
  return awaitBridgeBootstrap({ ...DESKTOP_SESSION_CALLBACKS, ...options });
}

export function initDesktopLocalDataClient(options = {}) {
  const desktopApi = initBridgeLocalDataClient({ ...DESKTOP_SESSION_CALLBACKS, ...options });
  // A page that boots without the lifecycle must not keep a heartbeat it
  // inherited from an earlier init in the same document.
  if (!desktopState.desktopLifecycleEnabled) {
    stopDesktopLifecycle();
  }
  return desktopApi;
}
