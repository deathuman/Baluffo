import { AdminConfig } from "./admin-config.js";
import { buildAttachmentPath, generateJobKey } from "./frontend/local-data/job-utils.js";
import { canTransitionPhase, normalizeApplicationStatus } from "./frontend/local-data/phase.js";

const BASE_URL = `${AdminConfig.ADMIN_BRIDGE_BASE}/desktop-local-data`;
const AUTH_LISTENERS = new Set();
const SAVED_SUBSCRIPTIONS = new Set();
const SESSION_KEY = "baluffo_current_profile_id";
let currentUser = null;
let pollingStarted = false;

function toErrorMessage(error, fallback) {
  return error?.message || String(error || "") || fallback;
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

async function refreshCurrentUser() {
  const payload = await requestJson("/session");
  currentUser = payload.user || null;
  return currentUser;
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

const desktopApi = {
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
    const name = window.prompt("Enter profile name to sign in or create a local desktop profile:", currentUser?.displayName || "");
    if (!String(name || "").trim()) {
      throw new Error("Sign-in cancelled.");
    }
    const payload = await requestJson("/sign-in", {
      method: "POST",
      body: JSON.stringify({ name })
    });
    currentUser = payload.user || null;
    notifyAuthChanged();
    return { user: currentUser };
  },
  async signOut() {
    await requestJson("/sign-out", { method: "POST", body: "{}" });
    currentUser = null;
    notifyAuthChanged();
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
    return Array.isArray(payload.rows) ? payload.rows : [];
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
  verifyAdminPin(pin) {
    return String(pin || "") === "1234";
  },
  async getAdminOverview(pin) {
    const payload = await requestJson("/admin/overview", {
      method: "POST",
      body: JSON.stringify({ pin })
    });
    return payload.overview || { users: [], totals: {} };
  },
  async wipeAccountAdmin(pin, uid) {
    const payload = await requestJson("/admin/wipe", {
      method: "POST",
      body: JSON.stringify({ pin, uid })
    });
    currentUser = payload.user || null;
    notifyAuthChanged();
    await pollSavedSubscriptions();
  }
};

async function bootstrapDesktopApi() {
  try {
    await refreshCurrentUser();
  } catch (error) {
    console.error("[desktop-local-data] bootstrap failed:", toErrorMessage(error, "bootstrap failed"));
    currentUser = null;
  }
  notifyAuthChanged();
}

// Expose API immediately so page boot is never blocked by bridge/session fetch latency.
window.JobAppLocalData = desktopApi;
notifyAuthChanged();
bootstrapDesktopApi().catch(() => {
  // Startup fetch errors are already logged in bootstrapDesktopApi.
});
