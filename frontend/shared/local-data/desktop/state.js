import { AdminConfig } from "../../config/admin-config.js";

export const BASE_URL = `${AdminConfig.ADMIN_BRIDGE_BASE}/desktop-local-data`;
export const TASKS_URL = `${AdminConfig.ADMIN_BRIDGE_BASE}/ops/task-state?view=summary`;
export const UPDATE_STATUS_URL = `${AdminConfig.ADMIN_BRIDGE_BASE}/app/update-status`;
export const DESKTOP_SESSION_LIFECYCLE_URL = `${AdminConfig.ADMIN_BRIDGE_BASE}/app/desktop-session-lifecycle`;
export const SESSION_KEY = "baluffo_current_profile_id";
export const DESKTOP_LIFECYCLE_HEARTBEAT_MS = 5000;
export const DESKTOP_BOOTSTRAP_RETRY_WINDOW_MS = 10_000;
export const DESKTOP_BOOTSTRAP_RETRY_INTERVAL_MS = 250;
export const DESKTOP_NAVIGATION_BYPASS_WINDOW_MS = 2000;
export const APPROVED_DESKTOP_PAGE_PATHS = new Set(["/", "/index.html", "/jobs.html", "/saved.html", "/admin.html"]);
export const AUTH_LISTENERS = new Set();
export const SAVED_SUBSCRIPTIONS = new Set();

export const desktopState = {
  currentUser: null,
  pollingStarted: false,
  authStateRevision: 0,
  desktopApiInitialized: false,
  desktopApi: null,
  desktopBoundWindow: null,
  desktopBootstrapPromise: null,
  desktopBootstrapStatus: "idle",
  desktopSession: null,
  desktopPageId: "",
  desktopLifecycleHeartbeatTimer: 0,
  desktopActiveWorkTimer: 0,
  desktopClosingSignaled: false,
  desktopCloseAttemptPending: false,
  desktopActiveWorkSnapshot: {
    hasActiveTask: false,
    hasActiveUpdate: false
  },
  desktopNavigationBypassExpiresAt: 0
};
