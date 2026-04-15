import { AdminConfig } from "../config/admin-config.js";
import { APPLICATION_STATUSES } from "../../local-data/constants.js";
import { requestProfileName } from "../../local-data/profile-name-dialog.js";
import { createLocalDataRuntime } from "../../local-data/runtime-contract.js";
import { buildAttachmentPath, generateJobKey } from "../../local-data/job-utils.js";
import { canTransitionPhase, normalizeApplicationStatus } from "../../local-data/phase.js";

const BASE_URL = `${AdminConfig.ADMIN_BRIDGE_BASE}/desktop-local-data`;
const TASKS_URL = `${AdminConfig.ADMIN_BRIDGE_BASE}/ops/task-state`;
const UPDATE_STATUS_URL = `${AdminConfig.ADMIN_BRIDGE_BASE}/app/update-status`;
const DESKTOP_SESSION_LIFECYCLE_URL = `${AdminConfig.ADMIN_BRIDGE_BASE}/app/desktop-session-lifecycle`;
const AUTH_LISTENERS = new Set();
const SAVED_SUBSCRIPTIONS = new Set();
const SESSION_KEY = "baluffo_current_profile_id";
const DESKTOP_LIFECYCLE_HEARTBEAT_MS = 5000;
let currentUser = null;
let pollingStarted = false;
let authStateRevision = 0;
let desktopApiInitialized = false;
let desktopSession = null;
let desktopPageId = "";
let desktopLifecycleHeartbeatTimer = 0;
let desktopActiveWorkTimer = 0;
let desktopClosingSignaled = false;
let desktopCloseAttemptPending = false;
let desktopActiveWorkSnapshot = {
  hasActiveTask: false,
  hasActiveUpdate: false
};

function toErrorMessage(error, fallback) {
  return error?.message || String(error || "") || fallback;
}

function normalizeDesktopSession(payload) {
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
  return Boolean(desktopActiveWorkSnapshot.hasActiveTask || desktopActiveWorkSnapshot.hasActiveUpdate);
}

function isActiveTaskPayload(payload) {
  const tasks = Array.isArray(payload?.tasks) ? payload.tasks : [];
  return tasks.some(task => Boolean(task?.active));
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
    desktopActiveWorkSnapshot.hasActiveTask = isActiveTaskPayload(taskState.value);
  }
  if (updateState.status === "fulfilled") {
    desktopActiveWorkSnapshot.hasActiveUpdate = isActiveUpdatePayload(updateState.value);
  }
}

function stopDesktopLifecycle() {
  if (desktopLifecycleHeartbeatTimer) {
    window.clearInterval?.(desktopLifecycleHeartbeatTimer);
    desktopLifecycleHeartbeatTimer = 0;
  }
  if (desktopActiveWorkTimer) {
    window.clearInterval?.(desktopActiveWorkTimer);
    desktopActiveWorkTimer = 0;
  }
}

async function postDesktopLifecycle(state, { keepalive = false } = {}) {
  if (!desktopSession || !desktopPageId) {
    return null;
  }
  const response = await fetch(DESKTOP_SESSION_LIFECYCLE_URL, {
    method: "POST",
    cache: "no-store",
    keepalive,
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      ownerToken: desktopSession.ownerToken,
      sessionId: desktopSession.sessionId,
      pageId: desktopPageId,
      state
    })
  });
  return response;
}

function sendDesktopClosingSignal(reason) {
  if (desktopClosingSignaled || !desktopSession || !desktopPageId) {
    return false;
  }
  desktopClosingSignaled = true;
  stopDesktopLifecycle();
  const body = JSON.stringify({
    ownerToken: desktopSession.ownerToken,
    sessionId: desktopSession.sessionId,
    pageId: desktopPageId,
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

function bindDesktopLifecycleEvents() {
  if (window.__baluffoDesktopLifecycleBound) {
    return;
  }
  window.__baluffoDesktopLifecycleBound = true;
  window.addEventListener?.("beforeunload", event => {
    desktopCloseAttemptPending = true;
    if (hasActiveDesktopWork()) {
      event.preventDefault();
      event.returnValue = "";
      return "";
    }
    sendDesktopClosingSignal("beforeunload");
    return undefined;
  });
  window.addEventListener?.("pagehide", () => {
    if (!desktopCloseAttemptPending && !desktopClosingSignaled) {
      desktopCloseAttemptPending = true;
    }
    sendDesktopClosingSignal("pagehide");
  });
  window.addEventListener?.("unload", () => {
    sendDesktopClosingSignal("unload");
  });
  window.addEventListener?.("focus", () => {
    desktopCloseAttemptPending = false;
    if (!desktopClosingSignaled && desktopSession && !desktopLifecycleHeartbeatTimer) {
      startDesktopLifecycle();
    }
  });
}

function startDesktopLifecycle() {
  if (!desktopSession) {
    return;
  }
  if (!desktopPageId) {
    desktopPageId = generateDesktopPageId();
  }
  bindDesktopLifecycleEvents();
  const sendAlive = () => {
    if (desktopClosingSignaled) {
      return;
    }
    postDesktopLifecycle("alive", { keepalive: true }).catch(() => {});
  };
  sendAlive();
  if (!desktopLifecycleHeartbeatTimer) {
    desktopLifecycleHeartbeatTimer = window.setInterval(sendAlive, DESKTOP_LIFECYCLE_HEARTBEAT_MS);
  }
  refreshDesktopActiveWorkSnapshot().catch(() => {});
  if (!desktopActiveWorkTimer) {
    desktopActiveWorkTimer = window.setInterval(() => {
      refreshDesktopActiveWorkSnapshot().catch(() => {});
    }, DESKTOP_LIFECYCLE_HEARTBEAT_MS);
  }
}

function buildAttachmentContentUrl(uid, jobKey, attachmentId, options = {}) {
  const includeDownload = Boolean(options.download);
  const query = new URLSearchParams({
    uid: String(uid || ""),
    jobKey: String(jobKey || ""),
    attachmentId: String(attachmentId || "")
  });
  if (includeDownload) {
    query.set("download", "1");
  }
  return `${BASE_URL}/attachments/content?${query.toString()}`;
}

function parseFilenameFromContentDisposition(value) {
  const text = String(value || "");
  if (!text) return "";
  const utfMatch = text.match(/filename\*\s*=\s*UTF-8''([^;]+)/i);
  if (utfMatch && utfMatch[1]) {
    try {
      return decodeURIComponent(String(utfMatch[1]).trim());
    } catch {
      return String(utfMatch[1]).trim();
    }
  }
  const quotedMatch = text.match(/filename\s*=\s*"([^"]+)"/i);
  if (quotedMatch && quotedMatch[1]) return String(quotedMatch[1]).trim();
  const plainMatch = text.match(/filename\s*=\s*([^;]+)/i);
  return plainMatch && plainMatch[1] ? String(plainMatch[1]).trim() : "";
}

function getFileExtension(name) {
  const idx = String(name || "").lastIndexOf(".");
  if (idx === -1) return "";
  return String(name).slice(idx + 1).toLowerCase();
}

function isImageAttachmentMeta(attachment) {
  const type = String(attachment?.type || "").toLowerCase();
  if (type === "image/png" || type === "image/jpeg") return true;
  const ext = getFileExtension(attachment?.name || "");
  return ext === "png" || ext === "jpg" || ext === "jpeg";
}

async function requestJson(path, options = {}) {
  const response = await fetch(`${BASE_URL}${path}`, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {})
    }
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok || payload?.ok === false) {
    throw new Error(String(payload?.error || response.statusText || "Request failed."));
  }
  return payload;
}

async function fetchAttachmentBlob(uid, jobKey, attachmentId) {
  const response = await fetch(buildAttachmentContentUrl(uid, jobKey, attachmentId));
  if (!response.ok) {
    let errorMessage = "Could not read attachment.";
    try {
      const payload = await response.json();
      errorMessage = String(payload?.error || errorMessage);
    } catch {
      // ignore
    }
    throw new Error(errorMessage);
  }
  const blob = await response.blob();
  const headerName = parseFilenameFromContentDisposition(response.headers.get("Content-Disposition"));
  return {
    blob,
    filename: headerName,
    contentType: response.headers.get("Content-Type") || blob.type || ""
  };
}

function notifyAuthChanged() {
  try {
    window.localStorage.setItem(SESSION_KEY, currentUser?.uid || "");
  } catch {
    // no-op
  }
  AUTH_LISTENERS.forEach(listener => {
    listener(currentUser);
  });
}

async function fetchDesktopSessionPayload() {
  return requestJson("/session");
}

async function fetchCurrentUser() {
  const payload = await fetchDesktopSessionPayload();
  desktopSession = normalizeDesktopSession(payload.desktopSession);
  if (!desktopSession) {
    stopDesktopLifecycle();
  }
  return payload.user || null;
}

function commitAuthState(user, revision = null) {
  if (revision !== null && Number(revision) !== authStateRevision) {
    return currentUser;
  }
  currentUser = user || null;
  notifyAuthChanged();
  return currentUser;
}

async function refreshCurrentUser(options = {}) {
  const revision = Object.prototype.hasOwnProperty.call(options, "revision")
    ? Number(options.revision)
    : null;
  const user = await fetchCurrentUser();
  return commitAuthState(user, revision);
}

async function listSavedJobs(uid) {
  const payload = await requestJson(`/saved-jobs?uid=${encodeURIComponent(String(uid || ""))}`);
  return Array.isArray(payload.rows) ? payload.rows : [];
}

async function pollSavedSubscriptions() {
  for (const subscription of Array.from(SAVED_SUBSCRIPTIONS)) {
    try {
      const rows = await listSavedJobs(subscription.uid);
      const serialized = JSON.stringify(rows);
      if (serialized === subscription.lastPayload) {
        continue;
      }
      subscription.lastPayload = serialized;
      subscription.onChange(rows);
    } catch (error) {
      if (typeof subscription.onError === "function") {
        subscription.onError(error);
      }
    }
  }
}

function ensurePolling() {
  if (pollingStarted) {
    return;
  }
  pollingStarted = true;
  window.setInterval(() => {
    pollSavedSubscriptions().catch(() => {});
  }, 1500);
}

function fileToDataUrl(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result || ""));
    reader.onerror = () => reject(reader.error || new Error("Could not read file."));
    reader.readAsDataURL(file);
  });
}

const desktopApi = createLocalDataRuntime({
  APPLICATION_STATUSES,
  isReady() {
    return true;
  },
  getCurrentUser() {
    return currentUser;
  },
  onAuthStateChanged(callback) {
    AUTH_LISTENERS.add(callback);
    callback(currentUser);
    return () => AUTH_LISTENERS.delete(callback);
  },
  async signIn() {
    const name = await requestProfileName({
      title: "Sign in",
      description: "Enter a profile name to sign in or create a local desktop profile.",
      defaultValue: currentUser?.displayName || ""
    });
    if (!String(name || "").trim()) {
      throw new Error("Sign-in cancelled.");
    }
    const payload = await requestJson("/sign-in", {
      method: "POST",
      body: JSON.stringify({ name })
    });
    authStateRevision += 1;
    const user = commitAuthState(payload.user || null);
    return { user };
  },
  async signOut() {
    await requestJson("/sign-out", { method: "POST", body: "{}" });
    authStateRevision += 1;
    commitAuthState(null);
  },
  async saveJobForUser(uid, job, options = {}) {
    const payload = await requestJson("/saved-jobs/save", {
      method: "POST",
      body: JSON.stringify({ uid, job, options })
    });
    await pollSavedSubscriptions();
    return String(payload.jobKey || generateJobKey(job));
  },
  async removeSavedJobForUser(uid, jobKey) {
    await requestJson("/saved-jobs/remove", {
      method: "POST",
      body: JSON.stringify({ uid, jobKey })
    });
    await pollSavedSubscriptions();
  },
  async getSavedJobKeys(uid) {
    const payload = await requestJson(`/saved-job-keys?uid=${encodeURIComponent(String(uid || ""))}`);
    return Array.isArray(payload.keys) ? payload.keys : [];
  },
  subscribeSavedJobs(uid, onChange, onError) {
    ensurePolling();
    const subscription = {
      uid: String(uid || ""),
      onChange,
      onError,
      lastPayload: ""
    };
    SAVED_SUBSCRIPTIONS.add(subscription);
    listSavedJobs(uid).then(rows => {
      subscription.lastPayload = JSON.stringify(rows);
      onChange(rows);
    }).catch(error => {
      if (typeof onError === "function") {
        onError(error);
      }
    });
    return () => SAVED_SUBSCRIPTIONS.delete(subscription);
  },
  generateJobKey,
  buildAttachmentPath,
  canTransitionPhase,
  async updateApplicationStatus(uid, jobKey, status, options = {}) {
    await requestJson("/saved-jobs/status", {
      method: "POST",
      body: JSON.stringify({ uid, jobKey, status: normalizeApplicationStatus(status), options })
    });
    await pollSavedSubscriptions();
  },
  async updateJobNotes(uid, jobKey, notes) {
    await requestJson("/saved-jobs/notes", {
      method: "POST",
      body: JSON.stringify({ uid, jobKey, notes })
    });
    await pollSavedSubscriptions();
  },
  async listAttachmentsForJob(uid, jobKey) {
    const payload = await requestJson(`/attachments?uid=${encodeURIComponent(String(uid || ""))}&jobKey=${encodeURIComponent(String(jobKey || ""))}`);
    const rows = Array.isArray(payload.rows) ? payload.rows : [];
    const hydrated = await Promise.all(
      rows.map(async row => {
        if (!isImageAttachmentMeta(row)) {
          return row;
        }
        try {
          const attachmentId = String(row?.id || "");
          if (!attachmentId) return row;
          const data = await fetchAttachmentBlob(uid, jobKey, attachmentId);
          return {
            ...row,
            blob: data.blob
          };
        } catch {
          return row;
        }
      })
    );
    return hydrated;
  },
  async addAttachmentForJob(uid, jobKey, fileMeta, blob) {
    const blobDataUrl = await fileToDataUrl(blob);
    const payload = await requestJson("/attachments/add", {
      method: "POST",
      body: JSON.stringify({ uid, jobKey, fileMeta, blobDataUrl })
    });
    await pollSavedSubscriptions();
    return String(payload.attachmentId || "");
  },
  async getAttachmentBlob(uid, jobKey, attachmentId) {
    return fetchAttachmentBlob(uid, jobKey, attachmentId);
  },
  getAttachmentDownloadUrl(uid, jobKey, attachmentId) {
    return buildAttachmentContentUrl(uid, jobKey, attachmentId, { download: true });
  },
  getAttachmentOpenUrl(uid, jobKey, attachmentId) {
    return buildAttachmentContentUrl(uid, jobKey, attachmentId);
  },
  async deleteAttachmentForJob(uid, jobKey, attachmentId) {
    await requestJson("/attachments/delete", {
      method: "POST",
      body: JSON.stringify({ uid, jobKey, attachmentId })
    });
    await pollSavedSubscriptions();
  },
  async listActivityForUser(uid, limit = 300) {
    const payload = await requestJson(`/activity?uid=${encodeURIComponent(String(uid || ""))}&limit=${encodeURIComponent(String(limit || 300))}`);
    return Array.isArray(payload.rows) ? payload.rows : [];
  },
  async exportProfileData(uid, options = {}) {
    const payload = await requestJson("/backup/export", {
      method: "POST",
      body: JSON.stringify({ uid, options })
    });
    return payload.payload || {};
  },
  getBackupExportUrl(uid, options = {}) {
    const query = new URLSearchParams({
      uid: String(uid || ""),
      includeFiles: options?.includeFiles ? "1" : "0"
    });
    return `${BASE_URL}/backup/export-file?${query.toString()}`;
  },
  async importProfileData(uid, payload) {
    const response = await requestJson("/backup/import", {
      method: "POST",
      body: JSON.stringify({ uid, payload })
    });
    await pollSavedSubscriptions();
    return response.result || {};
  },
  async getAdminOverview() {
    const payload = await requestJson("/admin/overview", {
      method: "POST",
      body: JSON.stringify({})
    });
    return payload.overview || { users: [], totals: {} };
  },
  async wipeAccountAdmin(uid) {
    const payload = await requestJson("/admin/wipe", {
      method: "POST",
      body: JSON.stringify({ uid })
    });
    authStateRevision += 1;
    commitAuthState(payload.user || null);
    await pollSavedSubscriptions();
  }
}, "desktop local data runtime");

async function bootstrapDesktopApi() {
  const bootstrapRevision = authStateRevision;
  try {
    await refreshCurrentUser({ revision: bootstrapRevision });
    desktopClosingSignaled = false;
    desktopCloseAttemptPending = false;
    startDesktopLifecycle();
  } catch (error) {
    console.error("[desktop-local-data] bootstrap failed:", toErrorMessage(error, "bootstrap failed"));
    commitAuthState(null, bootstrapRevision);
    return;
  }
}

export function initDesktopLocalDataClient() {
  if (desktopApiInitialized) {
    return desktopApi;
  }
  desktopApiInitialized = true;
  window.JobAppLocalData = desktopApi;
  commitAuthState(null);
  bootstrapDesktopApi().catch(() => {
    // Startup fetch errors are already logged in bootstrapDesktopApi.
  });
  return desktopApi;
}
